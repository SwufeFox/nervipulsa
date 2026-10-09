from __future__ import annotations

import asyncio
import sqlite3
import threading
from pathlib import Path
from types import SimpleNamespace

from prompt_toolkit.document import Document

import nervipulsa.cli as cli
from nervipulsa.cli import _COMMAND_COMPLETER, handle_line
from nervipulsa.config import Settings, save_settings
from nervipulsa.events import Event
from nervipulsa.journal import Journal
from nervipulsa.providers import (
    OpenAICompatibleBackend,
    ScriptedBackend,
    text_response,
    tool_response,
)
from nervipulsa.runtime import Runtime


def _completions(text: str) -> set[str]:
    return {item.text for item in _COMMAND_COMPLETER.get_completions(Document(text), None)}


def test_non_tty_reads_lines_off_event_loop_and_waits_for_idle(tmp_path: Path, monkeypatch) -> None:
    main_thread = threading.get_ident()
    read_thread_ids: list[int] = []
    submitted: list[str] = []
    idle_timeouts: list[float | None] = []

    class Stdin:
        def isatty(self) -> bool:
            return False

        def readline(self) -> str:
            read_thread_ids.append(threading.get_ident())
            return "hello\n" if len(read_thread_ids) == 1 else ""

    class FakeRuntime:
        def __init__(self, _settings, _backend, _workspace, echo=None) -> None:
            self.echo = echo
            self.host = SimpleNamespace(cleanup_failed=False)
            self.journal = SimpleNamespace(incomplete=False)

        async def start(self) -> None:
            return None

        def submit_text(self, text: str) -> SimpleNamespace:
            submitted.append(text)
            return SimpleNamespace(accepted=True, reason=None)

        async def wait_until_idle(self, timeout: float | None) -> bool:
            idle_timeouts.append(timeout)
            return True

        async def shutdown(self) -> None:
            return None

    monkeypatch.setattr(cli, "Runtime", FakeRuntime)
    monkeypatch.setattr(cli.sys, "stdin", Stdin())

    assert asyncio.run(cli.run_cli(Settings(model="scripted"), tmp_path)) == 0
    assert read_thread_ids and all(thread_id != main_thread for thread_id in read_thread_ids)
    assert submitted == ["hello"]
    assert idle_timeouts == [None]


    assert _completions("/config ") == {"show", "set", "profiles", "use"}
    assert _completions("/config set ") == {"provider", "base-url", "model", "api-key"}


def test_uninstall_command_completes_at_root() -> None:
    assert _completions("/un") == {"install"}


def test_uninstall_requires_interactive_terminal() -> None:
    output: list[str] = []
    runtime = SimpleNamespace(echo=output.append)

    assert handle_line(runtime, "/uninstall") is False
    assert output == ["/uninstall requires an interactive terminal."]


def test_uninstall_removes_only_nervipulsa_saved_data(tmp_path, monkeypatch) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    project_file = workspace / "main.py"
    project_file.write_text("print('keep')", encoding="utf-8")
    workspace_data = workspace / ".nervipulsa"
    output_dir = workspace_data / "outputs"
    output_dir.mkdir(parents=True)
    (workspace_data / "journal.sqlite").write_text("state", encoding="utf-8")
    (output_dir / "artifact.txt").write_text("artifact", encoding="utf-8")

    config_dir = tmp_path / "config"
    config_dir.mkdir()
    settings_file = config_dir / "config.json"
    settings_file.write_text("{}", encoding="utf-8")
    temporary_file = settings_file.with_suffix(".json.tmp")
    temporary_file.write_text("partial", encoding="utf-8")
    unrelated_file = config_dir / "keep.json"
    unrelated_file.write_text("keep", encoding="utf-8")
    monkeypatch.setattr(cli, "config_path", lambda: settings_file)

    cli._delete_saved_data(workspace)

    assert not workspace_data.exists()
    assert not settings_file.exists()
    assert not temporary_file.exists()
    assert project_file.exists()
    assert unrelated_file.exists()


def test_config_show_reports_key_state_without_exposing_key() -> None:
    secret = "never-print-this-key"
    output: list[str] = []
    runtime = SimpleNamespace(
        settings=Settings(
            provider="openai",
            base_url="https://example.test/v1",
            model="example-model",
            api_key=secret,
        ),
        echo=output.append,
    )

    assert handle_line(runtime, "/config show") is False

    rendered = "\n".join(output)
    assert "provider: openai" in rendered
    assert "model: example-model" in rendered
    assert "api-key: set" in rendered
    assert secret not in rendered


def test_output_command_displays_requested_streams() -> None:
    output: list[str] = []
    runtime = SimpleNamespace(
        echo=output.append,
        execution_output=lambda execution_id, stream: f"{execution_id}:{stream}",
    )

    assert handle_line(runtime, "/output evt_123 both") is False

    assert output == [
        "--- stdout ---",
        "evt_123:stdout",
        "--- stderr ---",
        "evt_123:stderr",
    ]




def test_finished_event_displays_status_outputs_and_exception(tmp_path: Path) -> None:
    output: list[str] = []
    runtime = Runtime(
        Settings(model="scripted"),
        object(),
        tmp_path,
        echo=output.append,
    )
    event = Event(
        session_id=runtime.bus.session_id,
        id="finished_123",
        seq=1,
        type="python.finished",
        source="python_host",
        target="llm",
        reply_to="exec_123",
        payload={
            "status": "failed",
            "stdout": "before failure\n",
            "stderr": "traceback preview\n",
            "stdout_bytes": 15,
            "stderr_bytes": 18,
            "duration_ms": 125,
            "exception": "ValueError: expected failure",
        },
        accepted_at=1.0,
    )

    runtime._observe(event)

    assert output.count("python exec_123  failed  0.12s") == 1
    assert any("before failure" in line for line in output)
    assert any("traceback preview" in line for line in output)
    assert any("ValueError: expected failure" in line for line in output)
    assert any(line.startswith("  context") for line in output)
    assert not any("/output" in line for line in output)


def test_runtime_reads_full_output_from_retained_journal_artifact(tmp_path: Path) -> None:
    runtime = Runtime(Settings(model="scripted"), object(), tmp_path)
    artifact = runtime.output_dir / "exec_123.stdout.log"
    artifact.parent.mkdir(parents=True)
    artifact.write_text("first line\n" + "x" * 10000, encoding="utf-8")
    event = Event(
        session_id=runtime.bus.session_id,
        id="finished_123",
        seq=1,
        type="python.finished",
        source="python_host",
        target="llm",
        reply_to="exec_123",
        payload={
            "status": "succeeded",
            "stdout": "preview",
            "stderr": "",
            "stdout_bytes": artifact.stat().st_size,
            "stderr_bytes": 0,
            "stdout_artifact_path": str(artifact),
        },
        accepted_at=1.0,
    )
    runtime.journal.start()
    try:
        runtime.journal.observe_event(event)
        assert runtime.journal.flush()
        assert runtime.execution_output("exec_123", "stdout") == artifact.read_text(encoding="utf-8")
        assert runtime.execution_output("exec_123", "stderr") == ""
    finally:
        runtime.journal.close()




def test_large_output_artifacts_survive_a_later_session(tmp_path: Path) -> None:
    async def run_one(workspace: Path, letter: str) -> tuple[Runtime, str, str]:
        def respond(request):
            if any(
                '"kind":"runtime_event"' in str(message.get("content") or "")
                for message in request.messages
                if message.get("role") == "user"
            ):
                return text_response("output received")
            return tool_response(("call-0", f"print('{letter}' * 5000)", None))

        runtime = Runtime(
            Settings(model="scripted", max_timeout=30, default_timeout=30),
            ScriptedBackend(respond),
            workspace,
            echo=lambda _text: None,
        )
        await runtime.start()
        runtime.submit_text("produce large output")
        for _ in range(400):
            finished = next(
                (event for event in runtime.trace if event.type == "python.finished"),
                None,
            )
            if finished is not None:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("Python execution did not finish")
        assert await runtime.wait_until_idle()
        assert finished is not None
        return runtime, finished.reply_to or "", str(finished.payload["stdout_artifact_path"])

    async def body() -> None:
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        first: Runtime | None = None
        second: Runtime | None = None
        try:
            first, first_id, first_artifact = await run_one(workspace, "A")
            await first.shutdown()
            second, second_id, second_artifact = await run_one(workspace, "B")

            assert first_id == second_id
            assert first_artifact != second_artifact
            assert first.execution_output(first_id, "stdout") == "A" * 5000 + "\n"
            assert second.execution_output(second_id, "stdout") == "B" * 5000 + "\n"
        finally:
            if first is not None:
                await first.shutdown()
            if second is not None:
                await second.shutdown()

    asyncio.run(body())


def test_provider_switch_applies_now_and_clears_previous_key(
    tmp_path: Path, monkeypatch
) -> None:
    output: list[str] = []
    settings = Settings(
        provider="openai",
        base_url="https://custom-openai-gateway.example/v1",
        model="old-model",
        api_key="old-provider-secret",
    )
    backend = OpenAICompatibleBackend(
        provider=settings.provider,
        base_url=settings.base_url,
        api_key=settings.api_key,
        model=settings.model,
    )
    runtime = Runtime(settings, backend, tmp_path, echo=output.append)
    monkeypatch.setattr(cli, "save_settings", lambda _settings: tmp_path / "config.json")

    cli._save_config_changes(runtime, {"provider": "anthropic", "model": "new-model"})

    assert runtime.settings.provider == "anthropic"
    assert runtime.settings.api_key == ""
    assert runtime.settings.base_url == Settings().base_url
    assert any("Base URL was reset" in line for line in output)
    assert runtime.backend is runtime.actor.backend
    assert runtime.backend.provider == "anthropic"
    assert runtime.actor.model == "new-model"
    assert "old-provider-secret" not in runtime.backend.api_key
    assert any("cleared" in line for line in output)
    output.clear()
    cli._save_config_changes(
        runtime,
        {
            "provider": "openai",
            "model": "next-model",
            "base_url": "https://openai-compatible.example/v1",
            "api_key": "destination-provider-secret",
        },
    )
    assert runtime.settings.api_key == "destination-provider-secret"
    assert runtime.backend.api_key == "destination-provider-secret"
    assert runtime.settings.base_url == "https://openai-compatible.example/v1"
    assert any("explicitly supplied API key" in line for line in output)
    assert not any("cleared" in line for line in output)


def test_profile_switch_restores_saved_provider_settings(tmp_path: Path, monkeypatch) -> None:
    settings = Settings()
    settings.profiles["openai"] = {
        "adapter": "openai-chat-completions",
        "base_url": "https://api.openai.com/v1",
        "model": "gpt-test",
        "api_key": "openai-profile-key",
    }
    backend = OpenAICompatibleBackend(
        provider=settings.provider,
        adapter=settings.adapter,
        base_url=settings.base_url,
        api_key=settings.api_key,
        model=settings.model,
    )
    output: list[str] = []
    runtime = Runtime(settings, backend, tmp_path, echo=output.append)
    config_file = tmp_path / "config.json"
    monkeypatch.setattr(cli, "save_settings", lambda updated: save_settings(updated, config_file))

    asyncio.run(cli._handle_config_command(runtime, "/config use openai"))
    assert runtime.settings.provider == "openai"
    assert runtime.settings.base_url == "https://api.openai.com/v1"
    assert runtime.settings.model == "gpt-test"
    assert runtime.settings.api_key == "openai-profile-key"

    asyncio.run(cli._handle_config_command(runtime, "/config use magpie"))
    assert runtime.settings.provider == "magpie"
    assert runtime.settings.base_url == "http://127.0.0.1:3425/v1"
    assert runtime.settings.api_key == "magpie"
    assert "openai-profile-key" not in runtime.backend.api_key


    settings = Settings(
        provider="openai",
        base_url="https://api.openai.com/v1",
        model="example-model",
        api_key="previous-endpoint-secret",
    )
    backend = OpenAICompatibleBackend(
        provider=settings.provider,
        base_url=settings.base_url,
        api_key=settings.api_key,
        model=settings.model,
    )
    output: list[str] = []
    runtime = Runtime(settings, backend, tmp_path, echo=output.append)
    monkeypatch.setattr(cli, "save_settings", lambda _settings: tmp_path / "config.json")

    cli._save_config_changes(runtime, {"base_url": "https://gateway.example/v1"})

    assert runtime.settings.base_url == "https://gateway.example/v1"
    assert runtime.settings.api_key == ""
    assert "previous-endpoint-secret" not in runtime.backend.api_key
    assert any("cleared" in line for line in output)


def test_existing_journal_migrates_activation_context_column(tmp_path: Path) -> None:
    path = tmp_path / "journal.sqlite"
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE activations (
                id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                input_event_ids_json TEXT NOT NULL,
                transcript_version INTEGER NOT NULL,
                status TEXT NOT NULL,
                started_at REAL NOT NULL,
                ended_at REAL,
                model TEXT,
                usage_json TEXT,
                error_json TEXT
            )
            """
        )

    journal = Journal(path)
    journal.start()
    try:
        assert journal.flush()
        assert journal.recent("existing-session")["activations"] == []
    finally:
        assert journal.close()

    with sqlite3.connect(path) as connection:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(activations)")}
    assert "context_json" in columns
