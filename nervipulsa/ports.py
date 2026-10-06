"""Neutral contracts shared by runtimes, tools, and provider adapters."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


class EventSink(Protocol):
    """Minimal event emission surface available to tool adapters."""

    def call(
        self,
        event_type: str,
        payload: dict[str, Any],
        *,
        reply_to: str | None = None,
        reserve_result_for: str | None = None,
        reserve_handler_slots: int = 0,
    ) -> Any: ...


class ToolContext(Protocol):
    workspace: str
    default_timeout: float
    max_timeout: float
    event_sink: EventSink


@dataclass(frozen=True)
class ToolExecutionContext:
    workspace: str
    default_timeout: float
    max_timeout: float
    event_sink: EventSink


@dataclass
class ToolCall:
    id: str
    name: str
    arguments: dict[str, Any] | None
    invalid: str | None = None


@dataclass
class ModelRequest:
    messages: list[dict[str, Any]]
    tools: list[dict[str, Any]]
    activation_id: str
    model: str
    purpose: str = "activation"


@dataclass
class ModelResponse:
    content: str | None = None
    tool_calls: list[ToolCall] = field(default_factory=list)
    reasoning_content: str | None = None
    usage: dict[str, Any] | None = None
    model: str | None = None


class ProviderFailure(Exception):
    def __init__(self, kind: str, message: str) -> None:
        super().__init__(message)
        self.kind = kind
        self.message = message


class ModelBackend(Protocol):
    async def complete(self, request: ModelRequest) -> ModelResponse: ...


class Tool(Protocol):
    name: str
    def schema(self, *, workspace: str) -> dict[str, Any]: ...
    def validate(self, arguments: dict[str, Any], *, max_timeout: float) -> str | None: ...
    async def invoke(self, arguments: dict[str, Any], *, context: ToolContext, activation: Any, call: ToolCall) -> tuple[dict[str, Any], str]: ...


class ToolCatalog(Protocol):
    def schemas(self, *, workspace: str) -> list[dict[str, Any]]: ...
    def get(self, name: str) -> Tool | None: ...


class ProviderRegistry(Protocol):
    def create(self, settings: Any) -> ModelBackend: ...


class ToolRegistry(Protocol):
    def create(self, workspace: str) -> ToolCatalog: ...
