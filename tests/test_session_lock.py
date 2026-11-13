"""Real OS leases cover runtime startup, teardown, cancellation, and death."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
import queue
import subprocess
import sys
import threading

import pytest

from nervipulsa.config import Settings
from nervipulsa.events import RuntimeState
from nervipulsa.ports import ModelResponse
from nervipulsa.providers import ScriptedBackend
from nervipulsa.runtime import Runtime
from nervipulsa.session_lock import SessionBusyError, SessionLease


def runtime_at(workspace, session_id="shared", *, journal_path=None, python_host=False):
    backend = ScriptedBackend(lambda _request: ModelResponse(content="ready"))
    return Runtime(
        Settings(), backend, workspace, python_host=python_host,
        session_id=session_id, journal_path=journal_path,
    )


def test_competing_runtime_never_starts_or_publishes(tmp_path, monkeypatch):
    async def run():
        owner = runtime_at(tmp_path)
        contender = runtime_at(tmp_path)
        await owner.start()
        try:
            def forbidden(*_args, **_kwargs):
                pytest.fail("busy runtime reached startup behind the lease")

            monkeypatch.setattr(contender.journal, "start", forbidden)
            monkeypatch.setattr(contender.journal, "session_history_state", forbidden)
            monkeypatch.setattr(contender.journal, "latest_transcript_snapshot", forbidden)
            monkeypatch.setattr(contender.bus, "start", forbidden)
            monkeypatch.setattr(contender.host, "start", forbidden)
            with pytest.raises(SessionBusyError):
                await contender.start(restore_transcript=True)
            assert not contender.submit_text("must not publish").accepted
            assert not contender.submit_observation({"text": "must not publish"}).accepted
            assert contender.backend.calls == 0
            assert contender.trace == []
            assert contender.bus.seq == 0
            await contender.shutdown()
            # A failed contender's cleanup cannot unlock the owner's handle.
            with pytest.raises(SessionBusyError):
                await asyncio.to_thread(SessionLease.acquire, owner.journal_path, "shared")
            assert owner.submit_text("owner still works").accepted
            assert await owner.wait_until_idle()
            assert owner.backend.calls == 1
        finally:
            await owner.shutdown()
            await contender.shutdown()
        replacement = runtime_at(tmp_path)
        try:
            assert await replacement.start() is None
        finally:
            await replacement.shutdown()

    asyncio.run(run())


def test_independent_sessions_and_journals_can_run_together(tmp_path):
    async def run():
        first = runtime_at(tmp_path, "first")
        second = runtime_at(tmp_path, "second")
        other_journal = runtime_at(
            tmp_path, "first", journal_path=tmp_path / "other.sqlite"
        )
        runtimes = (first, second, other_journal)
        try:
            await asyncio.gather(*(runtime.start() for runtime in runtimes))
            for runtime in runtimes:
                assert runtime.submit_text("independent").accepted
            assert all(await asyncio.gather(*(
                runtime.wait_until_idle() for runtime in runtimes
            )))
            assert [runtime.backend.calls for runtime in runtimes] == [1, 1, 1]
        finally:
            await asyncio.gather(*(runtime.shutdown() for runtime in runtimes))

    asyncio.run(run())


def test_canonical_journal_and_hashed_session_names(tmp_path):
    journal = tmp_path / "journal.sqlite"
    session_id = "../outside/unsafe\\name:" + "x" * 400
    owner = SessionLease.acquire(journal, session_id)
    lock_path = owner.path
    try:
        assert lock_path.parent == tmp_path / ".nervipulsa-locks"
        assert len(lock_path.stem) == 64
        assert all(character in "0123456789abcdef" for character in lock_path.stem)
        alias = tmp_path / "missing" / ".." / "journal.sqlite"
        with pytest.raises(SessionBusyError):
            SessionLease.acquire(alias, session_id)
        if os.name == "nt":
            with pytest.raises(SessionBusyError):
                SessionLease.acquire(Path(str(journal).upper()), session_id)
    finally:
        owner.close()
    assert lock_path.exists()
    successor = SessionLease.acquire(journal, session_id)
    try:
        owner.close()
        with pytest.raises(SessionBusyError):
            SessionLease.acquire(journal, session_id)
        assert successor.path == lock_path
    finally:
        successor.close()
    assert lock_path.exists()


@pytest.mark.parametrize("failure_at", ["journal", "history", "restore", "host"])
def test_startup_failure_releases_lease(tmp_path, monkeypatch, failure_at):
    async def run():
        broken = runtime_at(tmp_path, python_host=failure_at == "host")

        def fail(*_args, **_kwargs):
            raise RuntimeError("startup fault")

        if failure_at == "journal":
            original_start = broken.journal.start

            def start_then_fail():
                original_start()
                fail()

            monkeypatch.setattr(broken.journal, "start", start_then_fail)
        elif failure_at == "history":
            monkeypatch.setattr(broken.journal, "session_history_state", fail)
        elif failure_at == "restore":
            monkeypatch.setattr(broken, "_apply_transcript_snapshot", fail)
        else:
            original_start = broken.host.start

            def start_host_then_fail():
                original_start()
                fail()

            monkeypatch.setattr(broken.host, "start", start_host_then_fail)
        with pytest.raises(RuntimeError, match="startup fault"):
            await broken.start(restore_transcript=True)
        assert broken.bus.state is RuntimeState.CLOSED
        assert broken.backend.calls == 0
        assert not broken.journal._thread.is_alive()
        if failure_at == "host":
            assert broken.host._accept_task.done()
            assert broken.host._control_task.done()
            assert broken.host._scheduler_task.done()
        replacement = runtime_at(tmp_path)
        try:
            report = await replacement.start(restore_transcript=True)
            assert report == {
                "restored": False, "reason": "no_snapshot", "session_id": "shared"
            }
            await broken.shutdown()
            with pytest.raises(SessionBusyError):
                await asyncio.to_thread(SessionLease.acquire, replacement.journal_path, "shared")
        finally:
            await replacement.shutdown()

    asyncio.run(run())


@pytest.mark.parametrize("acquire_before_cancel", [False, True])
def test_acquisition_cancellation_waits_for_thread_and_releases(
    tmp_path, monkeypatch, acquire_before_cancel
):
    async def run():
        runtime = runtime_at(tmp_path)
        entered = threading.Event()
        proceed = threading.Event()
        original_acquire = SessionLease.acquire
        loop_thread = threading.get_ident()

        def gated_acquire(cls, journal_path, session_id):
            assert threading.get_ident() != loop_thread
            lease = original_acquire(journal_path, session_id) if acquire_before_cancel else None
            entered.set()
            assert proceed.wait(10), "acquisition gate was not released"
            return lease if lease is not None else original_acquire(journal_path, session_id)

        monkeypatch.setattr(SessionLease, "acquire", classmethod(gated_acquire))
        starting = asyncio.create_task(runtime.start())
        closing = None
        try:
            assert await asyncio.to_thread(entered.wait, 10)
            starting.cancel()
            await asyncio.sleep(0)
            assert not starting.done()
            starting.cancel()
            await asyncio.sleep(0)
            assert not starting.done()
            assert runtime.backend.calls == 0
            assert not runtime.journal._started
            closing = asyncio.create_task(runtime.shutdown())
            await asyncio.sleep(0)
            assert not closing.done()
            if acquire_before_cancel:
                with pytest.raises(SessionBusyError):
                    await asyncio.to_thread(original_acquire, runtime.journal_path, "shared")
        finally:
            proceed.set()
            with pytest.raises(asyncio.CancelledError):
                await starting
            if closing is not None:
                await closing
            await runtime.shutdown()
        assert not runtime.journal._thread.is_alive()
        assert runtime.backend.calls == 0
        lease = await asyncio.to_thread(original_acquire, runtime.journal_path, "shared")
        await asyncio.to_thread(lease.close)

    asyncio.run(run())


def test_shutdown_cancellation_holds_lease_through_cleanup(tmp_path, monkeypatch):
    async def run():
        runtime = runtime_at(tmp_path)
        entered = asyncio.Event()
        proceed = asyncio.Event()
        original_close = runtime.host.close

        async def gated_close():
            entered.set()
            await proceed.wait()
            await original_close()

        monkeypatch.setattr(runtime.host, "close", gated_close)
        await runtime.start()
        closing = asyncio.create_task(runtime.shutdown())
        try:
            await asyncio.wait_for(entered.wait(), 10)
            closing.cancel()
            await asyncio.sleep(0)
            assert not closing.done()
            with pytest.raises(SessionBusyError):
                await asyncio.to_thread(SessionLease.acquire, runtime.journal_path, "shared")
        finally:
            proceed.set()
            with pytest.raises(asyncio.CancelledError):
                await closing
        lease = await asyncio.to_thread(SessionLease.acquire, runtime.journal_path, "shared")
        await asyncio.to_thread(lease.close)
        assert not runtime.journal._thread.is_alive()

    asyncio.run(run())


_CHILD = r"""
import asyncio
from pathlib import Path
import sys
from nervipulsa.config import Settings
from nervipulsa.ports import ModelResponse
from nervipulsa.providers import ScriptedBackend
from nervipulsa.runtime import Runtime

async def main():
    runtime = Runtime(
        Settings(), ScriptedBackend(lambda _: ModelResponse(content="unused")),
        Path(sys.argv[1]), python_host=False, session_id="shared",
    )
    await runtime.start()
    print("leased", flush=True)
    await asyncio.to_thread(sys.stdin.readline)
    await runtime.shutdown()

asyncio.run(main())
"""


@pytest.mark.parametrize("termination", ["shutdown", "death"])
def test_subprocess_lease_released_after_shutdown_or_death(tmp_path, termination):
    child = subprocess.Popen(
        [sys.executable, "-u", "-c", _CHILD, str(tmp_path)],
        cwd=Path(__file__).resolve().parents[1], text=True,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    ready = queue.Queue()
    reader = threading.Thread(target=lambda: ready.put(child.stdout.readline()), daemon=True)
    reader.start()
    try:
        assert ready.get(timeout=20).strip() == "leased"

        async def compete():
            contender = runtime_at(tmp_path)
            try:
                with pytest.raises(SessionBusyError):
                    await contender.start()
                assert contender.backend.calls == 0
                assert not contender.submit_text("blocked").accepted
            finally:
                await contender.shutdown()
            independent = runtime_at(tmp_path, "independent")
            try:
                await independent.start()
            finally:
                await independent.shutdown()

        asyncio.run(compete())
        if termination == "death":
            child.kill()
            child.wait(timeout=15)
        else:
            _, stderr = child.communicate("shutdown\n", timeout=15)
            assert child.returncode == 0, stderr

        async def restart():
            successor = runtime_at(tmp_path)
            try:
                await successor.start()
                assert successor.submit_text("after release").accepted
                assert await successor.wait_until_idle()
                assert successor.backend.calls == 1
            finally:
                await successor.shutdown()

        asyncio.run(restart())
    finally:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=15)
        reader.join(timeout=5)
        for stream in (child.stdin, child.stdout, child.stderr):
            stream.close()
