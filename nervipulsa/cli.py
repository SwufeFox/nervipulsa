"""Interactive CLI with an editable prompt and async-safe output."""

from __future__ import annotations

import argparse
import asyncio
import json
import shutil
import sys
from dataclasses import replace
from pathlib import Path

from prompt_toolkit import PromptSession
from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.history import InMemoryHistory
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.patch_stdout import patch_stdout

from .config import PROVIDER_DEFAULTS, Settings, config_path, load_settings, save_settings
from .application import create_backend, create_runtime
from .runtime import Runtime


_CONFIG_USAGE = "/config [show | set <provider|base-url|model|api-key> | profiles | use <name>]"
_CONFIG_FIELDS = {
    "provider": ("provider", "Provider"),
    "base-url": ("base_url", "Base URL"),
    "model": ("model", "Model"),
    "api-key": ("api_key", "API key"),
}

_TOP_LEVEL_COMMANDS = (
    "/help",
    "/config",
    "/status",
    "/cancel",
    "/retry",
    "/logs",
    "/output",
    "/exit",
    "/uninstall",
)


class _CommandCompleter(Completer):
    def get_completions(self, document, complete_event):
        text = document.text_before_cursor
        if not text.startswith("/"):
            return
        if " " not in text:
            for command in _TOP_LEVEL_COMMANDS:
                if command.startswith(text) and command != text:
                    yield Completion(command[len(text) :], display=command)
            return

        command, _, remainder = text.partition(" ")
        if command == "/output":
            pieces = remainder.rsplit(" ", 1)
            if len(pieces) == 2:
                prefix = pieces[1]
                for candidate in ("stdout", "stderr", "both"):
                    if candidate.startswith(prefix) and candidate != prefix:
                        yield Completion(candidate[len(prefix) :], display=candidate)
            return
        if command != "/config":
            return
        subcommand, separator, prefix = remainder.partition(" ")
        if separator and subcommand == "set" and " " not in prefix:
            candidates = _CONFIG_FIELDS
        elif not separator:
            candidates = ("show", "set", "profiles", "use")
            prefix = subcommand
        else:
            return
        for candidate in candidates:
            if candidate.startswith(prefix) and candidate != prefix:
                yield Completion(candidate[len(prefix) :], display=candidate)


_COMMAND_COMPLETER = _CommandCompleter()


class LineBuffer:
    def __init__(self) -> None:
        self._lines: list[str] = []
        self._block = False

    def feed(self, line: str) -> str | None:
        if self._block:
            if line.strip() == '"""':
                self._block = False
                text = "\n".join(self._lines)
                self._lines = []
                return text
            self._lines.append(line)
            return None
        if line.strip() == '"""':
            self._block = True
            return None
        if line.endswith("\\"):
            self._lines.append(line[:-1])
            return None
        if self._lines:
            self._lines.append(line)
            text = "\n".join(self._lines)
            self._lines = []
            return text
        return line


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="nervipulsa")
    parser.add_argument("--dir", default=None, help="Workspace directory for the whole session")
    parser.add_argument("--provider", default=None)
    parser.add_argument("--base-url", default=None)
    parser.add_argument("--model", default=None)
    parser.add_argument("--api-key", default=None)
    parser.add_argument("--max-activations", type=int, default=None)
    parser.add_argument("--timeout", type=float, default=None, help="Default python_exec timeout in seconds")
    parser.add_argument("--max-timeout", type=float, default=None)
    return parser


def settings_from_args(argv: list[str] | None = None) -> Settings:
    args = build_parser().parse_args(argv)
    cli: dict[str, object] = {}
    if args.dir:
        cli["workspace"] = args.dir
    if args.provider:
        cli["provider"] = args.provider
    if args.base_url:
        cli["base_url"] = args.base_url
    if args.model:
        cli["model"] = args.model
    if args.api_key:
        cli["api_key"] = args.api_key
    if args.max_activations is not None:
        cli["max_activations"] = args.max_activations
    if args.timeout is not None:
        cli["default_timeout"] = args.timeout
    if args.max_timeout is not None:
        cli["max_timeout"] = args.max_timeout
    settings = load_settings(cli)
    if not settings.workspace:
        settings.workspace = str(Path.cwd())
    return settings


def handle_line(runtime: Runtime, line: str) -> bool:
    """Apply one command or user message. Return True when the session should close."""
    stripped = line.strip()
    if not stripped:
        return False
    if stripped == "/exit":
        return True
    if stripped == "/help":
        runtime.echo("Enter sends. Ctrl+J adds a line. Tab completes. Up/Down recalls input.")
        commands = (
            ("/status", "model, Python, and context budget"),
            ("/logs", "event ids, delivery, and context changes"),
            ("/output <id> [stdout|stderr|both]", "full saved output"),
            ("/cancel <id>", "stop queued or running code"),
            ("/retry", "resume a paused model call"),
            ("/config", "provider, model, and API key"),
            ("/exit", "drain and close"),
        )
        width = max(len(name) for name, _description in commands) + 2
        for name, description in commands:
            runtime.echo(f"{name.ljust(width)}{description}")
        runtime.echo("/uninstall removes saved settings and this workspace's .nervipulsa data only.")
        return False
    if stripped == "/status":
        _show_status(runtime)
        return False
    if stripped == "/retry":
        delivery = runtime.retry()
        if not delivery.accepted:
            runtime.echo(f"[rejected] {delivery.reason}")
        return False
    if stripped == "/logs":
        recent = runtime.logs()
        if runtime.journal.incomplete:
            runtime.echo("[journal] incomplete")
        if recent["events"]:
            runtime.echo("events")
        for event in reversed(recent["events"]):
            runtime.echo(
                f"  {event['seq']}  {event['type']}  {event['id']}  "
                f"{event.get('source') or '-'} → {event.get('target') or '-'}  "
                f"reply_to={event['reply_to'] or '-'}"
            )
        for activation in recent["activations"]:
            try:
                context = json.loads(activation.get("context_json") or "{}")
            except json.JSONDecodeError:
                context = {}
            if not isinstance(context, dict):
                context = {}
            request_chars = context.get(
                "request_transcript_chars",
                context.get("transcript_chars_after_compaction", context.get("transcript_chars_after_inputs")),
            )
            limit_chars = context.get("configured_limit_chars")
            delta = context.get("input_added_chars")
            growth = context.get("growth_since_previous_request_chars")
            response_added = context.get("response_and_receipt_added_chars")
            usage = context.get("provider_usage")
            usage_text = (
                json.dumps(usage, ensure_ascii=False, separators=(",", ":"))
                if usage is not None
                else "unavailable"
            )
            runtime.echo(
                f"activation {activation['id']} {activation['status']} "
                f"context={request_chars}/{limit_chars} JSON chars added={delta} "
                f"growth={growth} response+receipts={response_added} chars "
                f"usage={usage_text} inputs={activation['input_event_ids_json']}"
            )
            for item in context.get("input_events", []):
                if not isinstance(item, dict):
                    continue
                marker = " duplicate" if item.get("duplicate") else ""
                runtime.echo(
                    f"  input {item.get('type')} {item.get('id')}: "
                    f"+{item.get('added_chars', 0)} JSON chars{marker}"
                )
                if item.get("type") == "python.finished":
                    runtime.echo(
                        f"    output preview={item.get('output_preview_chars')} chars "
                        f"stdout={item.get('stdout_bytes')} bytes "
                        f"stderr={item.get('stderr_bytes')} bytes "
                        f"truncated={item.get('truncated')}"
                    )
            compaction = context.get("compaction")
            if isinstance(compaction, dict) and compaction.get("chars_recovered"):
                runtime.echo(
                    f"  compacted {compaction.get('chars_recovered')} chars; "
                    f"transcript now {context.get('transcript_chars_after_compaction')}"
                )
            elif isinstance(compaction, dict) and compaction.get("status") in {"kept_original", "skipped"}:
                runtime.echo(
                    f"  context {compaction.get('status')} ({compaction.get('reason') or 'unchanged'})"
                )
            for attempt in context.get("attempts", []):
                if isinstance(attempt, dict):
                    runtime.echo(
                        f"  request {attempt.get('number')} {attempt.get('status')} "
                        f"transcript={attempt.get('transcript_chars')} chars "
                        f"tools={attempt.get('tool_schema_chars')} chars "
                        f"usage={json.dumps(attempt.get('usage'), ensure_ascii=False, separators=(',', ':'))}"
                    )
        for execution in recent["executions"]:
            runtime.echo(
                f"execution {execution['execution_id']} {execution['status']} epoch={execution['worker_epoch']}"
            )
        return False
    if stripped.startswith("/output"):
        parts = stripped.split()
        if len(parts) not in {2, 3}:
            runtime.echo("usage: /output <execution_id> [stdout|stderr|both]")
            return False
        execution_id = parts[1]
        stream = parts[2] if len(parts) == 3 else "both"
        streams = ("stdout", "stderr") if stream == "both" else (stream,)
        if any(item not in {"stdout", "stderr"} for item in streams):
            runtime.echo("usage: /output <execution_id> [stdout|stderr|both]")
            return False
        try:
            outputs = [(item, runtime.execution_output(execution_id, item)) for item in streams]
        except ValueError as exc:
            runtime.echo(f"[output error] {exc}")
            return False
        for item, content in outputs:
            runtime.echo(f"--- {item} ---")
            runtime.echo(content.rstrip("\n") if content else "(empty)")
        return False
    if stripped.startswith("/cancel"):
        parts = stripped.split(maxsplit=1)
        if len(parts) != 2 or not parts[1].strip():
            runtime.echo("usage: /cancel <execution_id>")
            return False
        delivery = runtime.cancel(parts[1].strip())
        if not delivery.accepted:
            runtime.echo(f"[rejected] {delivery.reason}")
        return False
    if stripped == "/uninstall":
        runtime.echo("/uninstall requires an interactive terminal.")
        return False
    if stripped == "/config show":
        _show_config(runtime)
        return False
    if stripped == "/config":
        runtime.echo("/config requires an interactive terminal.")
        return False
    if stripped.startswith("/config "):
        runtime.echo("/config edits require an interactive terminal.")
        return False
    if stripped.startswith("/"):
        runtime.echo(f"unknown command: {stripped.split()[0]}")
        return False
    delivery = runtime.submit_text(line)
    if not delivery.accepted:
        runtime.echo(f"[rejected] {delivery.reason}")
    return False


def _show_status(runtime: Runtime) -> None:
    status = runtime.status()
    actor = status["actor"]
    if status["pause_reason"]:
        actor = f"paused ({status['pause_reason']})"
    python_state = status["python"]
    used = int(status["transcript_chars"])
    limit = int(status["context_limit_chars"])
    percent = 0 if limit <= 0 else min(100, round(100 * used / limit))
    compressing = "  compressing" if status["context_compressing"] else ""
    journal = "journal incomplete" if status["journal_incomplete"] else "journal ok"
    if status["journal_incomplete"]:
        details = [f"dropped={status['journal_dropped']}"]
        if status["journal_error"]:
            details.append(str(status["journal_error"])[:160])
        journal += f" ({'; '.join(details)})"
    runtime.echo(f"{status['provider']}  {status['model']}")
    runtime.echo(str(status["workspace"]))
    runtime.echo(f"{actor}  python {python_state}  queue {status['python_queue']}")
    runtime.echo(f"context  {used:,}/{limit:,}  {percent}%{compressing}")
    runtime.echo(f"worker epoch {status['worker_epoch']}  {journal}  api key {status['api_key']}")


def _show_config(runtime: Runtime) -> None:
    settings = runtime.settings
    runtime.echo("Active settings (saved changes apply to new model requests immediately):")
    runtime.echo(f"profile: {settings.active_profile}")
    runtime.echo(f"adapter: {settings.adapter}")
    for field, (attribute, _) in _CONFIG_FIELDS.items():
        value = getattr(settings, attribute)
        if attribute == "api_key":
            value = "set" if value else "missing"
        runtime.echo(f"{field}: {value}")


async def _prompt_config_value(settings: Settings, field: str) -> str:
    attribute, label = _CONFIG_FIELDS[field]
    current = getattr(settings, attribute)
    secret = attribute == "api_key"
    shown = ("set" if current else "missing") if secret else (current or "(unset)")
    prompt = f"{label} [{shown}]: "
    prompt_session = PromptSession(multiline=False, is_password=secret)
    return (await prompt_session.prompt_async(prompt)).strip()


def _save_config_changes(runtime: Runtime, changes: dict[str, str]) -> None:
    if "active_profile" not in changes and "provider" in changes:
        provider_name = str(changes["provider"]).strip()
        changes = {**changes, "active_profile": provider_name}
        if provider_name != runtime.settings.provider:
            destination = runtime.settings.profiles.get(provider_name) or PROVIDER_DEFAULTS.get(provider_name, {})
            changes = {
                **{key: value for key, value in destination.items() if key not in changes},
                **changes,
            }
    provider_changed = (
        "provider" in changes
        and str(changes["provider"]).strip() != runtime.settings.provider
    )
    base_url_reset = False
    if provider_changed and "base_url" not in changes:
        default_base_url = Settings().base_url
        base_url_reset = (
            runtime.settings.base_url.rstrip("/") != default_base_url.rstrip("/")
        )
        if base_url_reset:
            changes = {**changes, "base_url": default_base_url}
    base_url_changed = (
        "base_url" in changes
        and str(changes["base_url"]).rstrip("/") != runtime.settings.base_url.rstrip("/")
    )
    endpoint_changed = provider_changed or base_url_changed
    api_key_cleared = endpoint_changed and "api_key" not in changes
    if api_key_cleared:
        changes = {**changes, "api_key": ""}
    updated = replace(runtime.settings, **changes)
    path = save_settings(updated)
    backend = create_backend(updated)
    runtime.apply_provider_settings(updated, backend)
    runtime.echo(
        f"saved {path}; new model requests now use "
        f"{updated.provider}/{updated.model or '(unset)'}."
    )
    if base_url_reset:
        runtime.echo(
            "The custom Base URL was reset; set a replacement if the new provider "
            "uses a compatible gateway."
        )
    if api_key_cleared:
        runtime.echo(
            "The saved API key was cleared after the provider/endpoint change; "
            "set the destination key or use its environment variable."
        )
    elif endpoint_changed and updated.api_key:
        runtime.echo("The explicitly supplied API key will be used for new requests.")
    runtime.echo("API key is stored outside the workspace and is not written to the journal.")

async def _edit_config_async(runtime: Runtime) -> None:
    changes: dict[str, str] = {}
    for field, (attribute, _) in _CONFIG_FIELDS.items():
        value = await _prompt_config_value(runtime.settings, field)
        if value:
            changes[attribute] = value
    _save_config_changes(runtime, changes)


async def _set_config_value(runtime: Runtime, field: str) -> None:
    if field not in _CONFIG_FIELDS:
        runtime.echo(f"Unknown config field: {field}")
        runtime.echo(_CONFIG_USAGE)
        return
    attribute, _ = _CONFIG_FIELDS[field]
    value = await _prompt_config_value(runtime.settings, field)
    if not value:
        runtime.echo("No change; settings unchanged.")
        return
    _save_config_changes(runtime, {attribute: value})


async def _handle_config_command(runtime: Runtime, line: str) -> None:
    parts = line.split()
    if parts == ["/config", "profiles"]:
        runtime.echo("Profiles: " + ", ".join(sorted(runtime.settings.profiles)))
    elif len(parts) == 3 and parts[1] == "use":
        name = parts[2]
        profile = runtime.settings.profiles.get(name)
        if profile is None:
            runtime.echo(f"Unknown profile: {name}")
            return
        changes = {"provider": name, "active_profile": name}
        changes.update({key: profile[key] for key in ("adapter", "base_url", "model", "api_key") if key in profile})
        _save_config_changes(runtime, changes)
    elif len(parts) == 1:
        await _edit_config_async(runtime)
    elif len(parts) == 2 and parts[1] == "show":
        _show_config(runtime)
    elif len(parts) == 3 and parts[1] == "set":
        await _set_config_value(runtime, parts[2])
    else:
        runtime.echo(f"Usage: {_CONFIG_USAGE}")


async def _confirm_uninstall(runtime: Runtime, workspace: Path) -> bool:
    config_file = config_path()
    workspace_data = workspace / ".nervipulsa"
    runtime.echo("Uninstall will permanently remove:")
    runtime.echo(f"  user settings: {config_file}")
    runtime.echo(f"  current workspace data: {workspace_data}")
    runtime.echo("Project files and data in other workspaces will be kept.")
    confirmation = PromptSession(multiline=False)
    try:
        answer = await confirmation.prompt_async("Type DELETE to confirm: ")
    except (EOFError, KeyboardInterrupt):
        answer = ""
    if answer.strip() != "DELETE":
        runtime.echo("Uninstall cancelled; saved data was kept.")
        return False
    return True


def _delete_saved_data(workspace: Path) -> None:
    workspace_data = workspace / ".nervipulsa"
    if workspace_data.is_symlink():
        raise OSError(f"refusing to remove workspace data symlink: {workspace_data}")
    if workspace_data.exists():
        if not workspace_data.is_dir():
            raise OSError(f"workspace data path is not a directory: {workspace_data}")
        shutil.rmtree(workspace_data)
    settings_file = config_path()
    settings_file.unlink(missing_ok=True)
    settings_file.with_suffix(".json.tmp").unlink(missing_ok=True)


async def run_cli(settings: Settings, workspace: Path) -> int:
    def echo(text: str) -> None:
        print(text, flush=True)

    runtime = create_runtime(settings, workspace, echo=echo, runtime_factory=Runtime)
    await runtime.start()
    echo(f"nervipulsa  {settings.provider}  {settings.model or '(no model)'}  {workspace}")
    echo("Enter sends. /help lists commands.")
    if not settings.api_key:
        echo("No API key saved. /config can store one; NERVIPULSA_API_KEY or OPENAI_API_KEY can also provide it.")
    buffer = LineBuffer()
    uninstall_requested = False
    try:
        if not sys.stdin.isatty():
            while True:
                raw = await asyncio.to_thread(sys.stdin.readline)
                if raw == "":
                    break
                text = buffer.feed(raw.rstrip("\r\n"))
                if text is not None and handle_line(runtime, text):
                    break
            await runtime.wait_until_idle(timeout=30)
        else:
            bindings = KeyBindings()

            @bindings.add("enter")
            def _submit(event) -> None:
                event.current_buffer.validate_and_handle()

            @bindings.add("c-j")
            def _insert_newline(event) -> None:
                event.current_buffer.insert_text("\n")

            session = PromptSession(
                history=InMemoryHistory(),
                completer=_COMMAND_COMPLETER,
                key_bindings=bindings,
                multiline=True,
            )
            with patch_stdout():
                while True:
                    try:
                        line = await session.prompt_async("> ")
                    except EOFError:
                        break
                    except KeyboardInterrupt:
                        echo("Input cleared. Use /cancel <execution_id> to stop Python execution.")
                        continue
                    if line.strip() == "/config" or line.strip().startswith("/config "):
                        await _handle_config_command(runtime, line.strip())
                        continue
                    if line.strip() == "/uninstall":
                        if await _confirm_uninstall(runtime, workspace):
                            uninstall_requested = True
                            break
                        continue
                    if handle_line(runtime, line):
                        break
    finally:
        await runtime.shutdown()
    if uninstall_requested:
        if runtime.host.cleanup_failed or runtime.journal.incomplete:
            echo("[uninstall] runtime shutdown incomplete; saved data was kept.")
            return 1
        try:
            _delete_saved_data(workspace)
        except OSError as exc:
            echo(f"[uninstall] failed to remove saved data: {exc}")
            return 1
        echo("Saved config and current workspace data removed; project files were kept.")
        return 0
    if runtime.host.cleanup_failed or runtime.journal.incomplete:
        echo("[shutdown] cleanup or journal incomplete")
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, OSError):
        pass
    settings = settings_from_args(argv)
    workspace = Path(settings.workspace).resolve()
    try:
        return asyncio.run(run_cli(settings, workspace))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
