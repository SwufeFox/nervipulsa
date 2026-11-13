from __future__ import annotations

import asyncio
import json
from io import StringIO
import sqlite3
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest
from prompt_toolkit.document import Document

import nervipulsa.cli as cli
from nervipulsa.cli import _COMMAND_COMPLETER, handle_line
from nervipulsa.config import Settings, save_settings
from nervipulsa.events import Event
from nervipulsa.journal import Journal
from nervipulsa.ports import ModelResponse, ProviderFailure
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

        def status(self) -> dict[str, object]:
            return {"actor": "waiting", "pause_reason": None, "runtime": "RUNNING"}

        async def shutdown(self) -> None:
            return None

    monkeypatch.setattr(cli, "Runtime", FakeRuntime)
    monkeypatch.setattr(cli.sys, "stdin", Stdin())

    assert asyncio.run(cli.run_cli(Settings(model="scripted"), tmp_path)) == 0
    assert read_thread_ids and all(thread_id != main_thread for thread_id in read_thread_ids)
    assert submitted == ["hello"]
    assert len(idle_timeouts) == 1 and 0 < (idle_timeouts[0] or 0) <= 0.5


    assert _completions("/config ") == {"show", "set", "profiles", "use"}
    assert _completions("/config set ") == {"provider", "base-url", "model", "api-key"}


def test_inspect_is_discoverable_from_help_and_completion() -> None:
    output: list[str] = []
    runtime = SimpleNamespace(echo=output.append)

    assert handle_line(runtime, "/help") is False
    assert any(line.startswith("/inspect") for line in output)
    assert _completions("/ins") == {"pect"}


def test_startup_hints_report_missing_model_and_key(monkeypatch) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    settings = Settings(
        provider="openai",
        model="",
        api_key="",
        base_url="https://api.openai.com/v1",
    )

    hints = cli._startup_hints(settings)

    assert any("/config set model" in hint for hint in hints)
    assert any("OPENAI_API_KEY" in hint for hint in hints)


def test_startup_hints_accept_provider_env_key(monkeypatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "secret")
    settings = Settings(
        provider="openai",
        model="gpt-4o",
        api_key="",
        base_url="https://api.openai.com/v1",
    )

    assert cli._startup_hints(settings) == []


def test_startup_hints_allow_local_endpoint_without_key() -> None:
    settings = Settings(
        provider="ollama",
        model="llama3",
        api_key="",
        base_url="http://localhost:11434/v1",
    )

    assert cli._startup_hints(settings) == []


def test_startup_hints_reject_base_url_without_scheme() -> None:
    settings = Settings(
        provider="ollama",
        model="llama3",
        api_key="",
        base_url="localhost:11434/v1",
    )

    hints = cli._startup_hints(settings)

    assert any("http://" in hint and "https://" in hint for hint in hints)


class _NoTools:
    def schemas(self, *, workspace=None):
        return []

    def get(self, name):
        return None


def _install_scripted_runtime(monkeypatch, backend, **runtime_options) -> None:
    def fake_create(settings, workspace, *, echo=None, runtime_factory=Runtime):
        return runtime_factory(
            settings,
            backend,
            workspace,
            echo=echo,
            python_host=False,
            tool_catalog=_NoTools(),
            **runtime_options,
        )

    monkeypatch.setattr(cli, "create_runtime", fake_create)


def test_one_shot_json_reports_success(monkeypatch, tmp_path, capsys) -> None:
    backend = ScriptedBackend(
        lambda _request: ModelResponse(content="ready", model="scripted")
    )
    _install_scripted_runtime(monkeypatch, backend)

    code = asyncio.run(
        cli.run_cli(
            Settings(model="scripted"),
            tmp_path,
            prompt="hello",
            json_output=True,
            run_timeout=10,
            session_id="cli-test",
        )
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["status"] == "succeeded"
    assert payload["exit_code"] == 0
    assert payload["session_id"] == "cli-test"
    assert payload["final_message"] == "ready"
    assert payload["last_message"] == {
        "activation_id": payload["activation"]["id"], "text": "ready",
    }
    assert payload["activation"]["status"] == "committed"
    assert payload["delivery"]["accepted"] is True


def test_one_shot_json_reports_pause_and_nonzero_exit(monkeypatch, tmp_path, capsys) -> None:
    def fail(_request):
        raise ProviderFailure("response_unknown", "socket dropped")

    backend = ScriptedBackend(fail)
    _install_scripted_runtime(monkeypatch, backend)

    code = asyncio.run(
        cli.run_cli(
            Settings(model="scripted"),
            tmp_path,
            prompt="hello",
            json_output=True,
            run_timeout=10,
        )
    )

    payload = json.loads(capsys.readouterr().out)
    assert code == 1
    assert payload["status"] == "paused"
    assert payload["exit_code"] == 1
    assert payload["pause"]["kind"] == "response_unknown"


def test_one_shot_json_reports_timeout(monkeypatch, tmp_path, capsys) -> None:
    async def slow(_request):
        await asyncio.sleep(5)
        return ModelResponse(content="late", model="scripted")

    backend = ScriptedBackend(slow)
    _install_scripted_runtime(monkeypatch, backend)

    code = asyncio.run(
        cli.run_cli(
            Settings(model="scripted"),
            tmp_path,
            prompt="hello",
            json_output=True,
            run_timeout=0.1,
        )
    )

    payload = json.loads(capsys.readouterr().out)
    assert code == 124
    assert payload["status"] == "timeout"
    assert payload["exit_code"] == 124
    assert payload["final_message"] is None
    assert payload["last_message"] is None


def test_piped_input_deadline_returns_timeout_exit(monkeypatch, tmp_path):
    async def slow(_request):
        await asyncio.Event().wait()

    backend = ScriptedBackend(slow)
    _install_scripted_runtime(monkeypatch, backend)
    monkeypatch.setattr(cli.sys, "stdin", StringIO("hello\n"))
    assert asyncio.run(cli.run_cli(
        Settings(model="scripted"), tmp_path, run_timeout=0.1,
    )) == 124


def test_session_id_reuse_requires_resume(monkeypatch, tmp_path, capsys) -> None:
    backend = ScriptedBackend(
        lambda _request: ModelResponse(content="ready", model="scripted")
    )
    _install_scripted_runtime(monkeypatch, backend)

    first = asyncio.run(
        cli.run_cli(
            Settings(model="scripted"),
            tmp_path,
            prompt="one",
            json_output=True,
            run_timeout=10,
            session_id="duplicate",
        )
    )
    assert first == 0
    capsys.readouterr()

    second = asyncio.run(
        cli.run_cli(
            Settings(model="scripted"),
            tmp_path,
            prompt="two",
            json_output=True,
            run_timeout=10,
            session_id="duplicate",
        )
    )
    payload = json.loads(capsys.readouterr().out)
    assert second == 1
    assert payload["status"] == "rejected"
    assert payload["delivery"]["reason"] == "session_exists"
    assert backend.calls == 1


def test_one_shot_rejects_active_session_before_resume(monkeypatch, tmp_path, capsys):
    backend = ScriptedBackend(lambda _request: ModelResponse(content="saved"))
    _install_scripted_runtime(monkeypatch, backend)

    async def body():
        holder = Runtime(
            Settings(model="scripted"), backend, tmp_path, session_id="busy",
            python_host=False, tool_catalog=_NoTools(),
        )
        await holder.start()
        try:
            assert holder.submit_text("remember").accepted
            assert await holder.wait_until_idle(timeout=10)
            code = await cli.run_cli(
                Settings(model="scripted"), tmp_path, prompt="competing input",
                session_id="busy", resume=True, json_output=True, run_timeout=10,
            )
            payload = json.loads(capsys.readouterr().out)
            assert code == payload["exit_code"] == 1
            assert payload["status"] == "rejected"
            assert payload["delivery"]["reason"] == "session_busy"
            assert payload["delivery"]["accepted"] is False
            assert payload["activation"] is None
            assert payload["final_message"] is None
            assert payload["last_message"] is None
            assert backend.calls == 1
            assert holder.submit_text("holder still owns the session").accepted
            assert await holder.wait_until_idle(timeout=10)
            assert backend.calls == 2
        finally:
            await holder.shutdown()

    asyncio.run(body())


def test_one_shot_does_not_publish_response_before_commit_hook(monkeypatch, tmp_path, capsys):
    async def fail_commit(_activation, _response):
        raise RuntimeError("storage unavailable")

    backend = ScriptedBackend(lambda _request: ModelResponse(content="uncommitted answer"))
    _install_scripted_runtime(monkeypatch, backend, response_commit_hook=fail_commit)
    code = asyncio.run(cli.run_cli(
        Settings(model="scripted"), tmp_path, prompt="hello", json_output=True,
        run_timeout=10,
    ))
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 1
    assert payload["pause"]["kind"] == "response_commit_failed"
    assert payload["final_message"] is None
    assert "uncommitted answer" not in captured.out + captured.err


@pytest.mark.parametrize("terminal", ["success", "commit_failure", "unknown", "timeout", "budget"])
def test_one_shot_distinguishes_tool_progress_from_final_answer(
    terminal, monkeypatch, tmp_path, capsys,
):
    async def respond(_request):
        if backend.calls == 1:
            return tool_response((
                "write-effect",
                "from pathlib import Path\n"
                "Path('effect.txt').write_text('executed', encoding='utf-8')\n"
                "print('effect recorded')",
                None,
            ), text="I will run that tool.")
        if terminal == "unknown":
            raise ProviderFailure("response_unknown", "connection lost")
        if terminal == "timeout":
            await asyncio.Event().wait()
        return ModelResponse(content="final answer")

    async def commit(_activation, response):
        if terminal == "commit_failure" and response.content == "final answer":
            raise RuntimeError("storage unavailable")

    backend = ScriptedBackend(respond)

    def create(settings, workspace, *, echo=None, runtime_factory=Runtime):
        return runtime_factory(
            settings, backend, workspace, echo=echo, response_commit_hook=commit,
        )

    monkeypatch.setattr(cli, "create_runtime", create)
    code = asyncio.run(cli.run_cli(
        Settings(model="scripted", max_activations=1 if terminal == "budget" else 10),
        tmp_path, prompt="perform the task", json_output=True,
        run_timeout=5 if terminal == "timeout" else 20,
    ))
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert (tmp_path / "effect.txt").read_text(encoding="utf-8") == "executed"
    assert code == payload["exit_code"]
    if terminal == "success":
        assert code == 0
        assert payload["status"] == "succeeded"
        assert payload["final_message"] == "final answer"
        assert payload["last_message"] == {
            "activation_id": payload["activation"]["id"], "text": "final answer",
        }
    else:
        assert code == (124 if terminal == "timeout" else 1)
        assert payload["status"] == ("timeout" if terminal == "timeout" else "paused")
        assert payload["final_message"] is None
        assert payload["last_message"] == {
            "activation_id": backend.requests[0].activation_id,
            "text": "I will run that tool.",
        }
        if terminal == "commit_failure":
            assert payload["pause"]["kind"] == "response_commit_failed"
            assert "final answer" not in captured.out + captured.err
        elif terminal == "unknown":
            assert payload["pause"]["kind"] == "response_unknown"
        elif terminal == "budget":
            assert payload["pause"]["kind"] == "budget"


def test_one_shot_cleanup_failure_cannot_report_success(monkeypatch, tmp_path, capsys):
    backend = ScriptedBackend(lambda _request: ModelResponse(content="ready"))
    _install_scripted_runtime(monkeypatch, backend)
    shutdown = Runtime.shutdown

    async def failed_persistence(self):
        await shutdown(self)
        self.journal._error = "injected journal close failure"

    monkeypatch.setattr(Runtime, "shutdown", failed_persistence)
    code = asyncio.run(cli.run_cli(
        Settings(model="scripted"), tmp_path, prompt="hello", json_output=True,
        run_timeout=10,
    ))
    payload = json.loads(capsys.readouterr().out)
    assert code == payload["exit_code"] == 1
    assert payload["status"] == "failed"
    assert payload["ok"] is False
    assert payload["cleanup_incomplete"] is True
    assert payload["final_message"] is None
    assert payload["last_message"]["text"] == "ready"


def test_one_shot_resume_restores_prior_messages(monkeypatch, tmp_path, capsys):
    backend = ScriptedBackend(lambda _request: ModelResponse(content="saved"))
    _install_scripted_runtime(monkeypatch, backend)
    settings = Settings(model="scripted")
    assert asyncio.run(cli.run_cli(
        settings, tmp_path, prompt="color: cobalt", session_id="remember",
        json_output=True, run_timeout=10,
    )) == 0
    first = json.loads(capsys.readouterr().out)
    assert asyncio.run(cli.run_cli(
        settings, tmp_path, prompt="recall the color", session_id="remember",
        resume=True, json_output=True, run_timeout=10,
    )) == 0
    second = json.loads(capsys.readouterr().out)
    messages = backend.requests[-1].messages
    assert {"role": "user", "content": "color: cobalt"} in messages
    assert {"role": "assistant", "content": "saved"} in messages
    assert second["resume"]["restored"] is True
    assert second["delivery"]["event_id"] != first["delivery"]["event_id"]


def test_one_shot_corrupt_resume_fails_before_request(monkeypatch, tmp_path, capsys):
    backend = ScriptedBackend(lambda _request: ModelResponse(content="saved"))
    _install_scripted_runtime(monkeypatch, backend)
    settings = Settings(model="scripted")
    assert asyncio.run(cli.run_cli(
        settings, tmp_path, prompt="remember", session_id="corrupt",
        json_output=True, run_timeout=10,
    )) == 0
    capsys.readouterr()
    connection = sqlite3.connect(tmp_path / ".nervipulsa" / "journal.sqlite")
    try:
        connection.execute(
            "UPDATE durable_transcript_snapshots SET messages_json=? WHERE session_id=?",
            ("invalid-json", "corrupt"),
        )
        connection.commit()
    finally:
        connection.close()
    code = asyncio.run(cli.run_cli(
        settings, tmp_path, prompt="continue", session_id="corrupt", resume=True,
        json_output=True, run_timeout=10,
    ))
    payload = json.loads(capsys.readouterr().out)
    assert code == 1
    assert payload["status"] == "failed"
    assert payload["delivery"]["reason"] == "initialization_failed"
    assert payload["final_message"] is None
    assert payload["last_message"] is None
    assert backend.calls == 1


@pytest.mark.parametrize("value", ["nan", "inf", "-inf", "-1"])
def test_main_rejects_unbounded_or_negative_run_deadlines(value):
    with pytest.raises(SystemExit) as exc:
        cli.main([f"--run-timeout={value}", "--prompt", "hello"])
    assert exc.value.code == 2


def test_main_rejects_json_without_prompt(capsys) -> None:
    with pytest.raises(SystemExit) as exc:
        cli.main(["--json"])
    assert exc.value.code == 2


def test_main_rejects_resume_without_session(capsys) -> None:
    with pytest.raises(SystemExit) as exc:
        cli.main(["--resume"])
    assert exc.value.code == 2


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


def test_effects_command_lists_unresolved_records() -> None:
    class RuntimeStub:
        def __init__(self) -> None:
            self.output: list[str] = []
            self.echo = self.output.append

        def unresolved_effects(self):
            return [{
                "execution_id": "exec-1",
                "session_id": "session-2",
                "state": "dispatching",
                "intent_at": 12.5,
                "error": None,
                "latest_assessment": {
                    "assessment": "occurred",
                    "operator": "Lolin",
                    "assessed_at": 13.5,
                    "evidence": "Target record 42 exists.",
                },
            }]

    runtime = RuntimeStub()
    assert handle_line(runtime, "/effects") is False
    assert runtime.output == [
        "effect session=session-2 execution=exec-1 dispatching intent_at=12.5 error=-",
        "  operator assessment: occurred by Lolin at=13.5 evidence=Target record 42 exists.",
        "  Do not automatically replay: execution may already have taken effect.",
    ]


def test_effects_decide_preserves_quoted_windows_evidence() -> None:
    class RuntimeStub:
        def __init__(self) -> None:
            self.output: list[str] = []
            self.echo = self.output.append
            self.recorded = None

        def record_effect_assessment(self, *args):
            self.recorded = args
            return {
                "id": 7,
                "assessment": args[2],
                "operator": args[3],
                "evidence": args[4],
            }

    runtime = RuntimeStub()
    command = r'/effects decide session-2 exec-1 not_occurred --by "Lolin" --evidence "Checked C:\workspace\results\effect.txt and found no entry."'
    assert handle_line(runtime, command) is False
    assert runtime.recorded == (
        "session-2",
        "exec-1",
        "not_occurred",
        "Lolin",
        r"Checked C:\workspace\results\effect.txt and found no entry.",
    )
    assert "This is an operator attestation; the target was not independently verified." in runtime.output
    assert "Worker state was not changed and Python was not replayed." in runtime.output
    assert "This assessment does not authorize or perform a retry." in runtime.output


def test_effects_history_defaults_limit_and_shows_all_records() -> None:
    class RuntimeStub:
        def __init__(self) -> None:
            self.output: list[str] = []
            self.echo = self.output.append
            self.limit = None

        def effect_assessments(self, limit):
            self.limit = limit
            return [{
                "id": 2,
                "assessment": "not_occurred",
                "session_id": "session-2",
                "execution_id": "exec-1",
                "operator": "Lolin",
                "assessed_at": 12.5,
                "evidence": r"Checked C:\workspace\result.txt.",
            }]

    runtime = RuntimeStub()
    assert handle_line(runtime, "/effects history") is False
    assert runtime.limit == 100
    assert "operator attestation #2 not_occurred session=session-2 execution=exec-1 by=Lolin at=12.5: " + r"Checked C:\workspace\result.txt." in runtime.output
