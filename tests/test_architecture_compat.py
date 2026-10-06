from nervipulsa.application import create_backend, create_runtime
from nervipulsa.config import Settings
from nervipulsa.providers import OpenAICompatibleBackend, tool_schema
from nervipulsa.tools import python_exec_schema


def test_application_factory_selects_legacy_provider_adapter(tmp_path):
    settings = Settings(provider="ollama", base_url="http://localhost:11434/v1", model="test")
    backend = create_backend(settings)
    assert isinstance(backend, OpenAICompatibleBackend)
    assert backend.provider == "ollama"
    assert backend.model == "test"


def test_tool_schema_legacy_export_matches_new_port():
    assert tool_schema("/workspace") == python_exec_schema("/workspace")


def test_application_runtime_uses_selected_backend(tmp_path):
    settings = Settings(provider="ollama", base_url="http://localhost:11434/v1", model="test")
    runtime = create_runtime(settings, tmp_path)
    try:
        assert isinstance(runtime.backend, OpenAICompatibleBackend)
        assert runtime.actor.backend is runtime.backend
    finally:
        runtime.journal.close()


def test_custom_backend_and_tool_are_injected(tmp_path):
    from nervipulsa.application import DefaultToolRegistry
    from nervipulsa.ports import ModelResponse, ToolCall
    from nervipulsa.providers import text_response

    class Backend:
        async def complete(self, request):
            return ModelResponse(tool_calls=[ToolCall("custom-call", "custom_tool", {"value": 7})])

    class ProviderFactory:
        def create(self, settings):
            return backend

    class CustomTool:
        name = "custom_tool"
        invoked = False

        def schema(self, *, workspace):
            return {"type": "function", "function": {"name": self.name, "parameters": {"type": "object"}}}

        def validate(self, arguments, *, max_timeout):
            return None if arguments.get("value") == 7 else "invalid_payload"

        async def invoke(self, arguments, *, context, activation, call):
            self.invoked = True
            return {"status": "accepted", "custom": arguments["value"]}, "accepted"

    backend = Backend()
    tool = CustomTool()
    runtime = create_runtime(Settings(model="custom"), tmp_path, provider_registry=ProviderFactory(), tool_registry=DefaultToolRegistry([tool]))

    async def exercise():
        try:
            await runtime.start()
            runtime.submit_text("invoke custom tool")
            assert await runtime.wait_until_idle(5)
            assert runtime.backend is backend
            assert tool.invoked
            assert any("custom_tool" in item["function"]["name"] for item in backend_request_tools)
        finally:
            await runtime.shutdown()

    backend_request_tools = []
    original_complete = backend.complete
    async def record_request(request):
        backend_request_tools.extend(request.tools)
        return await original_complete(request)
    backend.complete = record_request
    import asyncio
    asyncio.run(exercise())


def test_dependency_direction_keeps_contracts_neutral():
    import ast
    from pathlib import Path

    ports = ast.parse(Path("nervipulsa/ports.py").read_text(encoding="utf-8"))
    assert not any(isinstance(node, ast.ImportFrom) and node.module == "providers" for node in ast.walk(ports))
    providers = ast.parse(Path("nervipulsa/providers.py").read_text(encoding="utf-8"))
    assert not any(isinstance(node, ast.ImportFrom) and node.module in {"llm", "runtime", "application"} for node in ast.walk(providers))
    llm = ast.parse(Path("nervipulsa/llm.py").read_text(encoding="utf-8"))
    imported = {alias.name for node in ast.walk(llm) if isinstance(node, ast.ImportFrom) and node.module == "providers" for alias in node.names}
    assert not imported.intersection({"ModelRequest", "ModelResponse", "ToolCall", "ProviderError"})
