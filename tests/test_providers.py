"""OpenAI-compatible HTTP adapter behavior against a local endpoint."""

from __future__ import annotations

import asyncio
import json
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest

from nervipulsa.providers import OpenAICompatibleBackend, ModelRequest, ProviderError, tool_schema


def test_tool_schema_exposes_worker_handler_api() -> None:
    description = tool_schema("/workspace")["function"]["description"]

    for detail in (
        "on_finished(callback)",
        "off_finished(handle)",
        "single\nobservation dict",
        "request_id",
        "status",
        "stdout",
        "including the\nexecution that registers the callbacks",
        "frozen handler snapshot",
        "only later executions",
        "At most\n16 handlers",
        "A timeout, cancellation, or worker restart clears registrations",
        "agent.handler_fired",
        "expected_handler_count",
        "handler_result_status",
        "missing_handler_ids",
        "Compare the expected count",
        "Do not\nassume missing results will arrive later",
        "handler_id",
        "worker_epoch",
    ):
        assert detail in description


@contextmanager
def _provider_server(status: int, response: dict[str, Any]) -> Iterator[tuple[str, list[tuple[str, dict[str, Any]]]]]:
    requests: list[tuple[str, dict[str, Any]]] = []
    payload = json.dumps(response).encode("utf-8")

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            size = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(size))
            requests.append((self.path, body))
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, _format: str, *_args: Any) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield f"http://{host}:{port}/v1", requests
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def _request() -> ModelRequest:
    return ModelRequest(
        messages=[{"role": "user", "content": "run the code"}],
        tools=[
            {
                "type": "function",
                "function": {
                    "name": "python_exec",
                    "description": "Execute Python code",
                    "parameters": {"type": "object", "properties": {"code": {"type": "string"}}},
                },
            }
        ],
        activation_id="activation-1",
        model="request-model",
    )


def test_openai_normalizes_local_tool_call_and_reasoning_fields() -> None:
    body = {
        "id": "chatcmpl-local",
        "object": "chat.completion",
        "created": 1,
        "model": "deepseek-flash",
        "choices": [
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": None,
                    "reasoning_content": "reasoning text",
                    "tool_calls": [
                        {
                            "id": "call-local",
                            "type": "function",
                            "function": {
                                "name": "python_exec",
                                "arguments": "{\"code\": \"print(1 + 1)\"}",
                            },
                        }
                    ],
                },
                "finish_reason": "tool_calls",
            }
        ],
        "usage": {"prompt_tokens": 3, "completion_tokens": 4, "total_tokens": 7},
    }
    with _provider_server(200, body) as (base_url, requests):
        backend = OpenAICompatibleBackend(
            provider="openai",
            base_url=base_url,
            api_key="local-test-key",
            model="deepseek-flash",
        )
        result = asyncio.run(backend.complete(_request()))

    assert result.content == ""
    assert result.reasoning_content == "reasoning text"
    assert result.model == "deepseek-flash"
    assert result.usage == {"prompt_tokens": 3, "completion_tokens": 4, "total_tokens": 7}
    assert len(result.tool_calls) == 1
    assert result.tool_calls[0].id == "call-local"
    assert result.tool_calls[0].name == "python_exec"
    assert result.tool_calls[0].arguments == {"code": "print(1 + 1)"}
    assert requests[0][0] == "/v1/chat/completions"
    assert requests[0][1]["model"] == "deepseek-flash"


def test_openai_error_mapping_redacts_key_without_retrying() -> None:
    secret = "local-secret-key"
    with _provider_server(500, {"error": {"message": f"upstream rejected {secret}"}}) as (base_url, requests):
        backend = OpenAICompatibleBackend(
            provider="openai",
            base_url=base_url,
            api_key=secret,
            model="deepseek-flash",
        )
        with pytest.raises(ProviderError) as raised:
            asyncio.run(backend.complete(_request()))

    assert raised.value.kind == "response_unknown"
    assert secret not in str(raised.value)
    assert "500" in str(raised.value)
    assert len(requests) == 1


def test_openai_rejects_malformed_completion_response() -> None:
    with _provider_server(200, {"model": "deepseek-flash", "choices": []}) as (base_url, _requests):
        backend = OpenAICompatibleBackend(
            provider="openai",
            base_url=base_url,
            api_key="local-test-key",
            model="deepseek-flash",
        )
        with pytest.raises(ProviderError) as raised:
            asyncio.run(backend.complete(_request()))

    assert raised.value.kind == "response_invalid"


def test_openai_auth_rejections_are_known_not_sent_errors() -> None:
    with _provider_server(401, {"error": {"message": "invalid API key"}}) as (base_url, requests):
        backend = OpenAICompatibleBackend(
            provider="openai",
            base_url=base_url,
            api_key="local-test-key",
            model="deepseek-flash",
        )
        with pytest.raises(ProviderError) as raised:
            asyncio.run(backend.complete(_request()))

    assert raised.value.kind == "request_not_sent"
    assert "401" in str(raised.value)
    assert len(requests) == 1


def test_missing_openai_key_fails_before_sending(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    with _provider_server(200, {}) as (base_url, requests):
        backend = OpenAICompatibleBackend(
            provider="openai",
            base_url=base_url,
            api_key="",
            model="deepseek-flash",
        )
        with pytest.raises(ProviderError) as raised:
            asyncio.run(backend.complete(_request()))

    assert raised.value.kind == "request_not_sent"
    assert not requests


def test_unsupported_provider_fails_before_sending() -> None:
    backend = OpenAICompatibleBackend(
        provider="anthropic", base_url="https://example.test/v1",
        api_key="local-test-key", model="provider/model-name",
    )
    with pytest.raises(ProviderError, match="unsupported provider"):
        asyncio.run(backend.complete(_request()))
