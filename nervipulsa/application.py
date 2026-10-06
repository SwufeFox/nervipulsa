"""Application composition: select adapters and assemble a session runtime."""
from __future__ import annotations

from pathlib import Path
import inspect
from typing import Callable

from .config import Settings
from .ports import ProviderRegistry, ToolRegistry
from .providers import OpenAICompatibleBackend
from .runtime import Runtime
from .tools import DefaultToolCatalog


class DefaultProviderRegistry:
    def create(self, settings: Settings):
        return OpenAICompatibleBackend(provider=settings.provider, adapter=settings.adapter, base_url=settings.base_url, api_key=settings.api_key, model=settings.model)


class DefaultToolRegistry:
    def __init__(self, tools=()):
        self.tools = tuple(tools)

    def create(self, workspace: str):
        return DefaultToolCatalog(workspace, self.tools)


def create_backend(settings: Settings, provider_registry: ProviderRegistry | Callable[[Settings], object] | None = None):
    registry = provider_registry or DefaultProviderRegistry()
    return registry.create(settings) if hasattr(registry, "create") else registry(settings)


def create_runtime(settings: Settings, workspace: Path, *, echo: Callable[[str], None] | None = None, runtime_factory=Runtime, provider_registry: ProviderRegistry | Callable[[Settings], object] | None = None, tool_registry: ToolRegistry | Callable[[str], object] | None = None) -> Runtime:
    backend = create_backend(settings, provider_registry)
    registry = tool_registry or DefaultToolRegistry()
    catalog = registry.create(str(workspace.resolve())) if hasattr(registry, "create") else registry(str(workspace.resolve()))
    kwargs = {"echo": echo}
    if "tool_catalog" in inspect.signature(runtime_factory).parameters:
        kwargs["tool_catalog"] = catalog
    return runtime_factory(settings, backend, workspace, **kwargs)
