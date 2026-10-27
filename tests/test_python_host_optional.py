"""Contract: an application that exposes no tools can decline the Python worker.

Why this exists (evidence from a real application, not a hypothetical):

A Telegram group agent built on this runtime exposes an empty tool catalog — the
model must never be able to execute Python. `Runtime.start()` spawned a Python
interpreter anyway, measured at ~300 ms and one live process per runtime. Because
the application keeps one runtime per chat *and per forum topic*, ten active
topics meant ten resident interpreters that no model could ever reach:

    live worker processes now: 10
    live after 5s idle:       10

(`nervipulsa-telegram-chatbot`, experiments/milestone_c review, 2026-10-27.)

The cost is not the only problem. An application that never executes Python is
still carrying an arbitrary-code-execution surface, and `start()` fails on hosts
where no usable interpreter exists — for a bot that will never run a line of it.

The contract: `python_host=False` starts a runtime where

* no worker process is ever spawned;
* `python.requested` is not routed, so nothing can be submitted;
* the runtime still starts, runs, answers, and shuts down cleanly;
* every host-facing status field keeps working and reports "not available"
  rather than raising.

Default stays `True`; existing callers are unaffected.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from nervipulsa.config import Settings
from nervipulsa.ports import ModelResponse
from nervipulsa.providers import ScriptedBackend
from nervipulsa.runtime import Runtime


class _NoTools:
    def schemas(self, *, workspace: str | None = None):
        return []

    def get(self, name: str):
        return None


def _settings(workspace) -> Settings:
    return Settings(model="scripted", workspace=str(workspace), max_timeout=10,
                    default_timeout=10)


def _responder(request):
    return ModelResponse(content="ready", model="scripted")


def test_start_does_not_spawn_a_worker_when_python_is_disabled(workspace):
    async def body() -> None:
        runtime = Runtime(
            _settings(workspace),
            ScriptedBackend(_responder),
            workspace,
            tool_catalog=_NoTools(),
            python_host=False,
        )
        await runtime.start()
        try:
            # Give the scheduler a generous window: a lazy spawn would show here.
            for _ in range(25):
                await asyncio.sleep(0.02)
                assert runtime.host.managed_pids() == [], "a worker was spawned"
            assert runtime.status()["worker_epoch"] == 0
        finally:
            await runtime.shutdown()

    asyncio.run(body())


def test_the_runtime_still_works_without_python(workspace):
    async def body() -> None:
        backend = ScriptedBackend(_responder)
        runtime = Runtime(
            _settings(workspace),
            backend,
            workspace,
            tool_catalog=_NoTools(),
            python_host=False,
        )
        await runtime.start()
        try:
            delivery = runtime.submit_text("hello")
            assert delivery.accepted, delivery.reason
            assert await runtime.wait_until_idle(timeout=10)
            assert backend.calls >= 1
            assistant = [e for e in runtime.trace if e.type == "assistant.message"]
            assert assistant, "no assistant output without a python host"
            assert assistant[0].payload["text"] == "ready"
            assert runtime.actor.tools == []
        finally:
            await runtime.shutdown()

    asyncio.run(body())


def test_python_requests_are_refused_not_executed(workspace):
    """The route is absent, so nothing can enqueue an execution — even directly."""

    async def body() -> None:
        runtime = Runtime(
            _settings(workspace),
            ScriptedBackend(_responder),
            workspace,
            tool_catalog=_NoTools(),
            python_host=False,
        )
        await runtime.start()
        try:
            delivery = runtime.bus.call(
                "test", "python.requested",
                {"code": "print(1)", "timeout": 1.0},
            )
            assert not delivery.accepted
            assert delivery.reason == "unknown_route", delivery.reason
            assert runtime.host.managed_pids() == []
        finally:
            await runtime.shutdown()

    asyncio.run(body())


def test_status_and_facts_report_unavailability_without_raising(workspace):
    async def body() -> None:
        runtime = Runtime(
            _settings(workspace),
            ScriptedBackend(_responder),
            workspace,
            tool_catalog=_NoTools(),
            python_host=False,
        )
        await runtime.start()
        try:
            status = runtime.status()
            # "disabled" rather than "idle": a caller must be able to tell
            # "nothing is running right now" from "this runtime has no Python".
            assert status["python"] == "disabled"
            assert status["python_enabled"] is False
            assert status["python_queue"] == 0
            assert status["worker_epoch"] == 0
            assert status["python_active_execution"] is None
            assert runtime.host.runtime_facts()["namespace"] == "disabled"
            assert runtime.host.idle is True
            assert runtime.host.cleanup_failed is False
            # And a report is still serialisable, which is what callers do with it.
            json.dumps(status)
        finally:
            await runtime.shutdown()

    asyncio.run(body())


def test_shutdown_is_clean_without_a_worker(workspace):
    async def body() -> None:
        runtime = Runtime(
            _settings(workspace),
            ScriptedBackend(_responder),
            workspace,
            tool_catalog=_NoTools(),
            python_host=False,
        )
        await runtime.start()
        runtime.submit_text("one")
        await runtime.wait_until_idle(timeout=10)
        await runtime.shutdown()
        assert runtime.bus.state.value == "CLOSED"
        assert runtime.host.managed_pids() == []
        assert runtime.host.cleanup_failed is False

    asyncio.run(body())


def test_default_still_spawns_a_worker(workspace):
    """The contrast that makes the other tests meaningful."""

    async def body() -> None:
        runtime = Runtime(
            _settings(workspace),
            ScriptedBackend(_responder),
            workspace,
            tool_catalog=_NoTools(),
        )
        await runtime.start()
        try:
            for _ in range(150):
                if runtime.host.managed_pids():
                    break
                await asyncio.sleep(0.02)
            assert runtime.host.managed_pids(), "the default no longer starts a worker"
        finally:
            await runtime.shutdown()

    asyncio.run(body())


def test_the_flag_must_be_a_bool(workspace):
    with pytest.raises(TypeError):
        Runtime(
            _settings(workspace),
            ScriptedBackend(_responder),
            workspace,
            python_host="no",  # type: ignore[arg-type]
        )
