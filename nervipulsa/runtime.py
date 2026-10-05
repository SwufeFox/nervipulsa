"""Assemble the bus, host, model actor, journal, and UI for one session."""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any, Callable

from .config import Settings
from .events import Bus, Delivery, Event, Mailbox, RuntimeState
from .journal import Journal
from .llm import LLMActor
from .python_host import PythonHost


class Runtime:
    def __init__(
        self,
        settings: Settings,
        backend: Any,
        workspace: Path,
        *,
        journal_path: Path | None = None,
        echo: Callable[[str], None] | None = None,
    ) -> None:
        self.settings = settings
        self.backend = backend
        self.workspace = workspace.resolve()
        self.echo = echo or (lambda _text: None)
        self.journal_path = journal_path or (self.workspace / ".nervipulsa" / "journal.sqlite")
        self.output_dir = self.workspace / ".nervipulsa" / "outputs"
        self.journal = Journal(self.journal_path)
        self.bus = Bus()
        self.trace: list[Event] = []
        self.listeners: list[Callable[[Event], None]] = []
        self.llm_box = Mailbox(
            "llm",
            ordinary_limit=settings.ordinary_limit,
            result_limit=settings.result_limit,
            feedback_limit=1,
        )
        self.python_box = Mailbox("python_host", ordinary_limit=max(8, settings.result_limit))
        self.control_box = Mailbox("python_host.control", ordinary_limit=32)
        self.ui_box = Mailbox("ui", ordinary_limit=256)
        self.life_box = Mailbox("lifecycle", ordinary_limit=4)
        self._register()
        self.user_emitter = self.bus.emitter("cli")
        self.llm_emitter = self.bus.emitter("llm")
        self.host_emitter = self.bus.emitter("python_host")
        self.host = PythonHost(
            requests=self.python_box,
            control=self.control_box,
            ui=self.host_emitter,
            results=self.host_emitter,
            workspace=self.workspace,
            output_dir=self.output_dir / self.bus.session_id,
            max_timeout=settings.max_timeout,
            journal=self.journal,
            session_id=self.bus.session_id,
        )
        self.actor = LLMActor(
            bus_state=lambda: self.bus.state,
            bus_seq=lambda: self.bus.seq,
            inbox=self.llm_box,
            backend=backend,
            emitter=self.llm_emitter,
            ui=self.llm_emitter,
            workspace=str(self.workspace),
            model=settings.model,
            session_id=self.bus.session_id,
            journal=self.journal,
            max_activations=settings.max_activations,
            max_timeout=settings.max_timeout,
            default_timeout=settings.default_timeout,
            context_limit=settings.context_limit,
            batch_limit=settings.batch_limit,
            runtime_facts=self.host.runtime_facts,
        )
        self.actor.note = self.echo
        self._tasks: list[asyncio.Task[None]] = []
        self._shutting = False

    def _register(self) -> None:
        routes = {
            "user.message": self.llm_box,
            "assistant.message": self.ui_box,
            "python.requested": self.python_box,
            "python.started": self.ui_box,
            "python.finished": self.llm_box,
            "python.cancel": self.control_box,
            "python.cancel_result": self.ui_box,
            "agent.feedback": self.llm_box,
            "agent.handler_fired": self.llm_box,
            "agent.retry": self.llm_box,
            "python.environment_changed": self.llm_box,
            "session.shutdown": self.life_box,
        }
        for event_type, mailbox in routes.items():
            self.bus.register(event_type, mailbox)

    def add_listener(self, listener: Callable[[Event], None]) -> None:
        self.listeners.append(listener)

    async def start(self) -> None:
        self.workspace.mkdir(parents=True, exist_ok=True)
        self.journal_path.parent.mkdir(parents=True, exist_ok=True)
        self.journal.start()
        self.bus.add_observer(self._observe)
        self.bus.add_delivery_sink(self._observe_delivery)
        self.bus.start()
        self.host.start()
        self._tasks = [
            asyncio.create_task(self.actor.run(), name="llm-actor"),
            asyncio.create_task(self._ui_loop(), name="ui"),
            asyncio.create_task(self._lifecycle(), name="lifecycle"),
        ]

    def submit_text(self, text: str) -> Delivery:
        return self.user_emitter.call("user.message", {"text": text})

    def cancel(self, execution_id: str, reason: str = "cli") -> Delivery:
        return self.user_emitter.call(
            "python.cancel",
            {"execution_id": execution_id, "reason": reason},
        )

    def retry(self) -> Delivery:
        return self.user_emitter.call("agent.retry", {})

    def status(self) -> dict[str, Any]:
        paused = self.actor.paused
        context_chars = self.actor.projector.view_length()
        return {
            "model": self.settings.model or "(unset)",
            "provider": self.settings.provider,
            "workspace": str(self.workspace),
            "runtime": self.bus.state.value,
            "actor": "paused" if paused is not None else self.actor.phase,
            "pause_reason": None if paused is None else paused.error_kind,
            "python": self.host.current_execution_id or "idle",
            "python_queue": self.host.queue_size,
            "transcript_chars": context_chars,
            "context_limit_chars": self.actor.context_limit,
            "context_remaining_chars": max(0, self.actor.context_limit - context_chars),
            "context_compressing": self.actor.compacting,
            "worker_epoch": self.host.worker_epoch,
            "journal_incomplete": self.journal.incomplete,
            "api_key": "set" if self.settings.api_key else "missing",
        }

    def logs(self, limit: int = 20) -> dict[str, list[dict[str, Any]]]:
        self.journal.flush(timeout=2)
        return self.journal.recent(self.bus.session_id, limit)

    def apply_provider_settings(self, settings: Settings, backend: Any) -> None:
        self.settings = settings
        self.backend = backend
        self.actor.backend = backend
        self.actor.model = settings.model

    def execution_output(self, execution_id: str, stream: str) -> str:
        if stream not in {"stdout", "stderr"}:
            raise ValueError("stream must be stdout or stderr")
        event = next(
            (
                item
                for item in reversed(self.trace)
                if item.type == "python.finished" and item.reply_to == execution_id
            ),
            None,
        )
        if event is not None:
            payload = event.payload
        else:
            self.journal.flush(timeout=2)
            record = self.journal.execution_event(self.bus.session_id, execution_id)
            if record is None:
                raise ValueError(f"no Python output found for execution {execution_id}")
            try:
                payload = json.loads(str(record["payload_json"]))
            except (KeyError, json.JSONDecodeError) as exc:
                raise ValueError(f"stored output record for {execution_id} is invalid") from exc
        artifact = payload.get(f"{stream}_artifact_path")
        if isinstance(artifact, str):
            root = self.output_dir.resolve()
            path = Path(artifact).resolve()
            try:
                path.relative_to(root)
            except ValueError as exc:
                raise ValueError("output artifact path is outside the workspace output directory") from exc
            try:
                return path.read_text(encoding="utf-8", errors="replace")
            except OSError as exc:
                raise ValueError(f"cannot read saved {stream} output: {exc}") from exc
        text = payload.get(stream)
        if not isinstance(text, str):
            raise ValueError(f"complete {stream} output is unavailable")
        total_bytes = payload.get(f"{stream}_bytes")
        if isinstance(total_bytes, int) and total_bytes > len(text.encode("utf-8")):
            raise ValueError(f"complete {stream} output artifact is missing")
        return text

    async def wait_until_idle(self, timeout: float = 10) -> bool:
        deadline = time.monotonic() + timeout
        stable = 0
        while time.monotonic() < deadline:
            paused = self.actor.paused is not None
            idle = (
                self.actor.phase == "waiting"
                and not self.actor.busy
                and not paused
                and not self.actor.held
                and self.host.idle
                and self.llm_box.size == 0
                and self.bus.state is RuntimeState.RUNNING
            )
            if idle:
                stable += 1
                if stable >= 3:
                    return True
            else:
                stable = 0
            await asyncio.sleep(0.02)
        return False

    async def shutdown(self) -> None:
        if self._shutting:
            return
        self._shutting = True
        if self.bus.state is RuntimeState.RUNNING:
            self.bus.begin_draining()
        self.actor.request_stop()
        await self.host.close()
        if self.bus.state is not RuntimeState.CLOSED:
            self.bus.close()
        current = asyncio.current_task()
        tasks = [task for task in self._tasks if task is not current]
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self.journal.flush(timeout=5)
        self.journal.close(timeout=5)

    def _observe(self, event: Event) -> None:
        self.trace.append(event)
        self.journal.observe_event(event)
        if event.type == "python.requested":
            self.echo(f"python {event.id}")
            code = str(event.payload.get("code") or "").strip("\n")
            preview = self._excerpt(code, 240) if code else ""
            if preview:
                for line in preview.splitlines():
                    self.echo(f"  {line}")
            else:
                self.echo("  (no code)")
        elif event.type == "python.finished":
            self._render(event)
        for listener in tuple(self.listeners):
            try:
                listener(event)
            except Exception:
                pass

    def _observe_delivery(self, event: Event | None, receiver: str, delivery: Delivery) -> None:
        self.journal.observe_delivery(self.bus.session_id, event, receiver, delivery)

    async def _ui_loop(self) -> None:
        while True:
            batch = await self.ui_box.take_batch(32)
            if not batch:
                return
            for event in batch:
                self._render(event)

    async def _lifecycle(self) -> None:
        while not self._shutting:
            batch = await self.life_box.take_batch(4)
            if not batch:
                return
            if any(event.type == "session.shutdown" for event in batch):
                await self.shutdown()
                return

    def _render(self, event: Event) -> None:
        if event.type == "assistant.message":
            text = str(event.payload.get("text") or "")
            if text.strip():
                self.echo(text)
            return
        if event.type == "python.started":
            self.echo(f"python {event.payload.get('execution_id')}  running")
            return
        if event.type == "python.finished":
            execution_id = event.reply_to or event.payload.get("execution_id")
            status = str(event.payload.get("status") or "unknown")
            try:
                duration_ms = max(0, int(event.payload.get("duration_ms") or 0))
            except (TypeError, ValueError):
                duration_ms = 0
            self.echo(f"python {execution_id}  {status}  {duration_ms / 1000:.2f}s")
            streams: list[tuple[str, str, int, bool]] = []
            for stream in ("stdout", "stderr"):
                text = str(event.payload.get(stream) or "")
                preview_bytes = len(text.encode("utf-8"))
                total = event.payload.get(f"{stream}_bytes")
                if not isinstance(total, int) or isinstance(total, bool):
                    total = preview_bytes
                excerpt = self._excerpt(text, 280)
                stream_truncated = total > preview_bytes
                if excerpt or stream_truncated:
                    streams.append((stream, excerpt, total, stream_truncated))
            truncated = any(item[3] for item in streams) or bool(event.payload.get("truncated"))
            label_streams = len(streams) > 1 or any(item[0] == "stderr" or item[3] for item in streams)
            for stream, excerpt, total, stream_truncated in streams:
                if stream_truncated:
                    self.echo(f"  {stream}  {total:,} bytes, preview")
                elif label_streams:
                    self.echo(f"  {stream}")
                indent = "    " if label_streams or stream_truncated else "  "
                for line in excerpt.splitlines():
                    self.echo(f"{indent}{line}")
            if status == "failed":
                exception = event.payload.get("exception")
                if not isinstance(exception, str) or not exception:
                    exception = next(
                        (
                            line.strip()
                            for line in reversed(str(event.payload.get("stderr") or "").splitlines())
                            if line.strip()
                        ),
                        "",
                    )
                if exception:
                    already_shown = any(
                        exception.strip() == line.strip()
                        for _, excerpt, _, _ in streams
                        for line in excerpt.splitlines()
                    )
                    if not already_shown:
                        self.echo(f"  {self._excerpt(exception, 220)}")
            if truncated:
                self.echo(f"  /output {execution_id}")
            self._echo_context()
            return
        if event.type == "python.cancel_result":
            self.echo(
                f"python {event.payload.get('execution_id')}  cancel {event.payload.get('status')}"
            )
            return
        if event.type == "agent.error":
            self.echo(f"error  {event.payload.get('kind')}  {event.payload.get('message')}")

    def _echo_context(self) -> None:
        used = self.actor.projector.view_length()
        limit = max(1, self.actor.context_limit)
        percent = min(100, round(100 * used / limit))
        line = f"  context  {used:,}/{self.actor.context_limit:,}  {percent}%"
        if self.actor.compacting:
            line += "  compressing"
        self.echo(line)

    @staticmethod
    def _excerpt(value: str, limit: int) -> str:
        if len(value) <= limit:
            return value.rstrip()
        head = max(1, limit * 2 // 3)
        tail = max(1, limit - head - 24)
        omitted = len(value) - head - tail
        return f"{value[:head]}\n… [{omitted} chars omitted] …\n{value[-tail:]}"
