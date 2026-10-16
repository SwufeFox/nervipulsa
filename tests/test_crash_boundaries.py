from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest

from nervipulsa.journal import Journal
from tests.test_subprocess_recovery import _kill_tree, _wait_json


_CHILD = r'''
import asyncio, json, sys, time
from pathlib import Path
from nervipulsa.config import Settings
from nervipulsa.providers import ScriptedBackend, text_response
from nervipulsa.runtime import Runtime

root, fault, ready, gate = Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3]), Path(sys.argv[4])
async def main():
    backend_inputs = []
    runtime = Runtime(Settings(), ScriptedBackend(lambda messages: (backend_inputs.append(messages), text_response("fresh"))[1]), root)
    original_prepare = runtime.journal.prepare_effect
    original_dispatch = runtime.journal.mark_effect_dispatching
    original_observe = runtime.journal.observe_effect
    def wait_gate():
        ready.write_text(json.dumps({"session_id": runtime.bus.session_id, "worker_pid": getattr(getattr(runtime.host, "_managed", None), "pid", None), "request_id": request_id[0], "trace_types": [e.type for e in runtime.trace], "marker": (root / "crash-effect.marker").exists(), "execution_id": request_id[0], "llm_mailbox_size": runtime.llm_box.size}))
        while not gate.exists(): time.sleep(.01)
    request_id = [None]
    if fault == "T0":
        def prepare(s, e): original_prepare(s, e); wait_gate()
        runtime.journal.prepare_effect = prepare
    elif fault == "T1":
        def dispatch(s, e): original_dispatch(s, e); wait_gate()
        runtime.journal.mark_effect_dispatching = dispatch
    elif fault == "T3":
        def observe(s, e, o): wait_gate(); original_observe(s, e, o)
        runtime.journal.observe_effect = observe
    elif fault == "T4":
        original_runtime_observe = runtime._observe
        def observe(event):
            original_runtime_observe(event)
            if event.type == "python.finished": wait_gate()
        runtime._observe = observe
        async def hold(limit): await asyncio.Event().wait()
        runtime.llm_box.take_batch = hold
    await runtime.start()
    code = "from pathlib import Path\nimport time\np=Path('crash-effect.marker'); f=p.open('a', encoding='utf-8'); f.write('effect\\n'); f.close()\n"
    if fault == "T2": code += f"while not Path({str(gate)!r}).exists(): time.sleep(.01)\n"
    delivery = runtime.host_emitter.call("python.requested", {"code": code, "timeout": 30}, reserve_result_for="llm")
    request_id[0] = delivery.event_id
    assert delivery.accepted
    if fault == "T2":
        deadline = time.monotonic() + 20
        while not (root / "crash-effect.marker").exists() and time.monotonic() < deadline: await asyncio.sleep(.01)
        ready.write_text(json.dumps({"session_id": runtime.bus.session_id, "worker_pid": getattr(runtime.host._managed, "pid", None), "request_id": delivery.event_id, "trace_types": [e.type for e in runtime.trace], "marker": True}))
        while not gate.exists(): await asyncio.sleep(.01)
    if fault in ("T0", "T1", "T3", "T4"):
        while not ready.exists(): await asyncio.sleep(.01)
    while True: await asyncio.sleep(1)
asyncio.run(main())
'''

@pytest.mark.parametrize("fault,state,marker", [("T0", "intent", False), ("T1", "dispatching", False), ("T2", "dispatching", True), ("T3", "dispatching", True), ("T4", "observed", True)])
def test_t0_t4_crash_boundaries(workspace: Path, fault: str, state: str, marker: bool) -> None:
    ready, gate = workspace / "fault-ready.json", workspace / "parent.gate"
    child = subprocess.Popen([sys.executable, "-c", _CHILD, str(workspace), fault, str(ready), str(gate)], cwd=Path(__file__).resolve().parents[1], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **({"start_new_session": True} if os.name != "nt" else {}))
    restart = None
    worker_pid = None
    try:
        before = _wait_json(ready, child)
        worker_pid = before["worker_pid"]
        assert before["session_id"] and before["request_id"]
        assert before["marker"] is marker
        assert ("python.finished" in before["trace_types"]) is (fault == "T4")
        if fault == "T4": assert before["llm_mailbox_size"] >= 1
        with sqlite3.connect(workspace / ".nervipulsa" / "journal.sqlite") as db:
            row = db.execute("SELECT state FROM durable_effects WHERE session_id=? AND execution_id=?", (before["session_id"], before["request_id"])).fetchone()
        assert row == (state,)
        _kill_tree(child, worker_pid); worker_pid = None
        with sqlite3.connect(workspace / ".nervipulsa" / "journal.sqlite") as db:
            unresolved = db.execute("SELECT execution_id FROM durable_effects WHERE session_id=? AND state!='observed'", (before["session_id"],)).fetchall()
            observed = db.execute("SELECT state FROM durable_effects WHERE session_id=? AND execution_id=?", (before["session_id"], before["request_id"])).fetchone()
        pending = Journal(workspace / ".nervipulsa" / "journal.sqlite").unresolved_effects(session_id=before["session_id"])
        assert bool(pending) is (fault != "T4")
        if fault == "T4": assert observed == ("observed",)
        else: assert before["request_id"] in {item[0] for item in unresolved}
        restart_path = workspace / "restart.json"
        restart_code = r'''import asyncio,io,json,sys
from pathlib import Path
from nervipulsa.cli import handle_line
from nervipulsa.config import Settings
from nervipulsa.providers import ScriptedBackend,text_response
from nervipulsa.runtime import Runtime
async def m():
 backend=ScriptedBackend(lambda _: text_response("fresh")); r=Runtime(Settings(),backend,Path(sys.argv[1]))
 try:
  await r.start(); capture=io.StringIO(); r.echo=lambda text: print(text,file=capture)
  handle_line(r,"/effects"); effect_output=capture.getvalue()
  r.user_emitter.call("user.message",{"text":"activation probe"})
  while not backend.requests or not any(e.type == "assistant.message" for e in r.trace): await asyncio.sleep(.01)
  r.journal.flush(5)
  Path(sys.argv[2]).write_text(json.dumps({"session":r.bus.session_id,"trace":[e.type for e in r.trace],"effect_output":effect_output,"logs":r.journal.recent(r.bus.session_id,100),"inputs":backend.requests[0].messages}))
 finally: await r.shutdown()
asyncio.run(m())'''
        restart = subprocess.Popen([sys.executable, "-c", restart_code, str(workspace), str(restart_path)], cwd=Path(__file__).resolve().parents[1], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **({"start_new_session": True} if os.name != "nt" else {}))
        after = _wait_json(restart_path, restart); restart.wait(timeout=10)
        assert after["session"] != before["session_id"]
        assert "activation probe" in str(after["inputs"])
        assert "python.finished" not in str(after["inputs"])
        assert "python.finished" not in after["trace"]
        assert not any(e["type"] == "python.finished" for e in after["logs"]["events"])
        assert after["logs"]["activations"]
        if fault == "T4":
            assert "No unresolved durable effects." in after["effect_output"]
        else:
            assert before["request_id"] in after["effect_output"]
            assert f" {state} intent_at=" in after["effect_output"]
            assert "Do not automatically replay" in after["effect_output"]
        marker_path = workspace / "crash-effect.marker"
        if marker:
            assert marker_path.read_text(encoding="utf-8").splitlines().count("effect") == 1
        else:
            assert not marker_path.exists()
    finally:
        _kill_tree(child, worker_pid)
        if restart is not None and restart.poll() is None: _kill_tree(restart)
        gate.unlink(missing_ok=True); ready.unlink(missing_ok=True)
        for name in ("crash-effect.marker", "crash-parent.gate", "restart.json"): (workspace / name).unlink(missing_ok=True)
