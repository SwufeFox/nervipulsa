"""Model adapters.

ScriptedBackend exercises the protocol without credentials. OpenAICompatibleBackend
sends Chat Completions requests to OpenAI-compatible HTTP endpoints.
Neither adapter waits for Python execution.
"""

from __future__ import annotations

import asyncio
import json
import os
import urllib.error
import urllib.request
from collections.abc import Mapping
from typing import Any, Callable

from .events import SYSTEM_PROMPT

from .ports import ModelRequest, ModelResponse, ToolCall


from .ports import ProviderFailure as ProviderError





def tool_schema(workspace: str):
    """Backward-compatible lazy export of the built-in Python tool schema."""
    from .tools import python_exec_schema
    return python_exec_schema(workspace)


def environment_tool_schema():
    """Backward-compatible lazy export of environment discovery schema."""
    from .tools import python_environment_schema
    return python_environment_schema()


def __getattr__(name: str):
    if name == "TOOL_DESCRIPTION":
        from .tools import TOOL_DESCRIPTION
        return TOOL_DESCRIPTION
    raise AttributeError(name)

def text_response(text: str, *, usage: dict[str, Any] | None = None) -> ModelResponse:
    return ModelResponse(content=text, usage=usage, model="scripted")


def tool_response(*calls: tuple[str, str, float | None], text: str = "") -> ModelResponse:
    """Build tool calls as (id, code, timeout-or-None)."""
    tool_calls = []
    for call_id, code, timeout in calls:
        arguments: dict[str, Any] = {"code": code}
        if timeout is not None:
            arguments["timeout"] = timeout
        tool_calls.append(ToolCall(id=call_id, name="python_exec", arguments=arguments))
    return ModelResponse(content=text, tool_calls=tool_calls, model="scripted")


class ScriptedBackend:
    """Deterministic stand-in. `calls` counts model requests, not tool runs."""

    def __init__(self, responder: Callable[[ModelRequest], Any]) -> None:
        self._responder = responder
        self.calls = 0
        self.requests: list[ModelRequest] = []

    async def complete(self, request: ModelRequest) -> ModelResponse:
        self.calls += 1
        self.requests.append(request)
        result = self._responder(request)
        if hasattr(result, "__await__"):
            result = await result
        if not isinstance(result, ModelResponse):
            raise ProviderError("response_invalid", "scripted responder returned a non-response")
        return result


DEFAULT_OPENAI_BASE_URL = "https://api.openai.com/v1"


class OpenAICompatibleBackend:
    """Adapter for the OpenAI Chat Completions HTTP protocol."""

    def __init__(self, *, provider: str, base_url: str, api_key: str, model: str, timeout: float = 120, adapter: str = "openai-chat-completions") -> None:
        self.provider = provider.strip().lower() or "openai"
        self.adapter = adapter
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model
        self.timeout = timeout

    async def complete(self, request: ModelRequest) -> ModelResponse:
        if self.adapter != "openai-chat-completions":
            raise ProviderError("request_not_sent", f"unsupported protocol adapter: {self.adapter}")
        model = self.model or request.model

        body: dict[str, Any] = {"model": model, "messages": request.messages}
        if request.tools:
            body["tools"] = request.tools
            body["tool_choice"] = "auto"
        headers = {"Content-Type": "application/json"}
        key_env = {
            "openai": "OPENAI_API_KEY",
            "deepseek": "DEEPSEEK_API_KEY",
            "openrouter": "OPENROUTER_API_KEY",
        }.get(self.provider)
        key = self.api_key or (os.environ.get(key_env, "") if key_env else "")
        required_key_providers = {"openai", "deepseek", "openrouter"}
        if self.provider in required_key_providers and not key:
            raise ProviderError(
                "request_not_sent",
                f"missing API key for {self.provider}; configure it in /config or set {key_env}",
            )
        if key:
            headers["Authorization"] = f"Bearer {key}"
        http_request = urllib.request.Request(
            f"{self.base_url}/chat/completions", data=json.dumps(body).encode("utf-8"),
            headers=headers, method="POST",
        )
        try:
            raw = await asyncio.to_thread(_http_post, http_request, self.timeout)
            response = json.loads(raw)
            return _parse_openai_response(response, model)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            if key:
                detail = detail.replace(key, "[redacted]")
            kind = "request_not_sent" if exc.code in {400, 401, 403, 404, 422} else "response_unknown"
            raise ProviderError(kind, f"provider HTTP {exc.code}: {detail[:500]}") from exc
        except (json.JSONDecodeError, UnicodeError) as exc:
            raise ProviderError("response_invalid", f"provider response invalid: {str(exc)[:500]}") from exc
        except (OSError, TimeoutError) as exc:
            detail = str(exc).replace(key, "[redacted]") if key else str(exc)
            raise ProviderError("response_unknown", f"provider response unknown: {detail[:500]}") from exc


def _http_post(request: urllib.request.Request, timeout: float) -> bytes:
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()



def _as_mapping(value: Any) -> dict[str, Any] | None:
    if isinstance(value, Mapping):
        return dict(value)
    dump = getattr(value, "model_dump", None)
    if not callable(dump):
        return None
    try:
        mapped = dump(exclude_none=True)
    except TypeError:
        mapped = dump()
    if not isinstance(mapped, Mapping):
        return None
    result = dict(mapped)
    extra = getattr(value, "model_extra", None)
    if isinstance(extra, Mapping):
        result.update(extra)
    return result


def _parse_openai_response(response: Any, fallback_model: str) -> ModelResponse:
    body = response if isinstance(response, dict) else None
    if body is None:
        raise ProviderError("response_invalid", "provider response was not an object")
    choices = body.get("choices")
    if not isinstance(choices, list) or not choices:
        raise ProviderError("response_invalid", "provider response has no message")
    choice = _as_mapping(choices[0])
    message = _as_mapping(choice.get("message")) if choice else None
    if message is None:
        raise ProviderError("response_invalid", "provider response has no message")
    parsed = _parse_message(message)
    model = body.get("model")
    parsed.model = model if isinstance(model, str) else fallback_model
    parsed.usage = _as_mapping(body.get("usage"))
    return parsed


def _parse_message(message: dict[str, Any]) -> ModelResponse:
    content = message.get("content")
    if content is None:
        content = ""
    elif not isinstance(content, str):
        content = str(content)
    reasoning = message.get("reasoning_content")
    if reasoning is not None and not isinstance(reasoning, str):
        reasoning = str(reasoning)
    tool_calls: list[ToolCall] = []
    raw_calls = message.get("tool_calls") or []
    if not isinstance(raw_calls, list):
        raw_calls = []
    for index, raw in enumerate(raw_calls):
        if not isinstance(raw, dict):
            tool_calls.append(ToolCall(id=f"invalid_{index}", name="", arguments=None, invalid="invalid_payload"))
            continue
        function = raw.get("function") if isinstance(raw.get("function"), dict) else {}
        name = function.get("name")
        arguments, error = _parse_arguments(function.get("arguments"))
        call_id = raw.get("id")
        tool_calls.append(
            ToolCall(
                id=str(call_id) if call_id else f"call_{index}",
                name=str(name) if isinstance(name, str) else "",
                arguments=arguments,
                invalid=error,
            )
        )
    return ModelResponse(content=content, tool_calls=tool_calls, reasoning_content=reasoning)


def _parse_arguments(arguments: Any) -> tuple[dict[str, Any] | None, str | None]:
    if isinstance(arguments, dict):
        return arguments, None
    if isinstance(arguments, str):
        try:
            value = json.loads(arguments)
        except json.JSONDecodeError:
            return None, "invalid_payload"
        if not isinstance(value, dict):
            return None, "invalid_payload"
        return value, None
    return None, "invalid_payload"


__all__ = [
    "SYSTEM_PROMPT",
    "TOOL_DESCRIPTION",
    "ModelRequest",
    "ModelResponse",
    "OpenAICompatibleBackend",
    "ProviderError",
    "ScriptedBackend",
    "ToolCall",
    "text_response",
    "tool_response",
    "tool_schema",
]
