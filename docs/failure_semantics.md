# Failure Semantics Baseline

- **Status:** baseline behavior measured before the effect ledger; current regression reports `effect_state=unknown` for both worker-exit cases. Combined current-main suite: 137 passed.
- **Baseline commit:** `41c83d0c3f2fbc81d49993072cbfa585d7047742`
- **Branch:** `research/fault-semantics`
- **Follow-up:** the current `main` adds the [Durable Effect Protocol](durable_effect_protocol.md) in commit `6484416`. This report remains pinned to the pre-protocol baseline above; its process-kill probes do not validate that ledger.

**Research question:** after a worker exits, can Nervipulsa distinguish "the external effect did not happen" from "the effect happened but no terminal was observed"?

## Scope and vocabulary

This baseline studies the committed Runtime without changing production code. It separates five facts that are easy to conflate:

- **Intent:** a model/tool call asks for work.
- **Admission:** the in-process bus accepts and queues the event.
- **Execution observation:** the host sees IPC such as `started`, `finished`, timeout, or worker exit.
- **External effect:** the environment actually changes.
- **Model-visible evidence:** an event is projected into the LLM actor's in-memory transcript and included in a provider request.

Admission, execution observation, external effect, and model-visible evidence are different facts. In particular, `started` is not `succeeded`; `python.finished.status == "failed"` after worker exit does not prove that no external effect occurred; and a completed tool call does not prove that the user's goal was achieved.

The controlled effect oracle is a file created by the worker. It is a deterministic local stand-in, not a network service. The experiment kills only the Python worker with `os._exit`; it does not kill the host/runtime process, simulate power loss, or measure model behavior.

## Baseline implementation facts

| Stage | Behavior at the baseline commit | Evidence |
|---|---|---|
| Admission | `Bus.call` validates route/payload and receiver capacity, reserves terminal and handler capacity, then offers the event. Rejection is synchronous and reservation/sequence state is rolled back on a failed offer. The event and reservations remain in process memory. | [Bus.call](../nervipulsa/events.py#L446), [Mailbox capacity and reservations](../nervipulsa/events.py#L185). Existing admission tests are in [test_events.py](../tests/test_events.py). |
| Host queue | PythonHost takes requests into a pending deque and runs one Python execution at a time. | [accept loop and enqueue](../nervipulsa/python_host.py#L233), [scheduler](../nervipulsa/python_host.py#L292). |
| Worker protocol | Host sends a framed `execute` request. A `started` frame is an IPC acknowledgement. It does not establish what the arbitrary Python code has done in the outside world. | [worker dispatch and frame handling](../nervipulsa/python_host.py#L603). |
| Worker exit | While the host remains alive, unexpected worker exit produces a `failed` terminal, marks the namespace reset, and starts a replacement worker. It does not roll back filesystem or remote effects. | [worker-exit handling](../nervipulsa/python_host.py#L672), existing [worker crash test](../tests/test_python_host.py#L275). |
| Terminal delivery | `python.finished` is offered to a reserved mailbox lane. A rejected terminal increments a counter and attempts an `agent.error`; there is no durable outbox or terminal replay loop. | [terminal emission](../nervipulsa/python_host.py#L932), [Bus delivery](../nervipulsa/events.py#L518). |
| Model consumption | The actor projects each event into an in-memory transcript and then marks the mailbox lease consumed. Normal-path tests inspect the actual scripted/provider request. | [LLM activation projection](../nervipulsa/llm.py#L921), [mailbox dequeue/consume](../nervipulsa/events.py#L321). |
| Journal | Journal submission is non-blocking and bounded; a full queue increments `dropped`. SQLite uses WAL and `synchronous=NORMAL`. Runtime startup starts the journal but does not rehydrate pending executions or LLM activations from it. | [Journal submit](../nervipulsa/journal.py#L47), [writer](../nervipulsa/journal.py#L128), [Runtime.start](../nervipulsa/runtime.py#L112). |
| Provider/commit interruptions | During the same live process, an unknown provider response pauses for explicit retry; an interrupted tool commit can continue from the retained activation state. These are in-process paths, not cross-process recovery. | [unknown response test](../tests/test_llm.py#L454), [commit interruption test](../tests/test_llm.py#L420). |

## Controlled worker-exit experiment

[`test_worker_exit_does_not_identify_external_effect`](../tests/test_fault_semantics.py#L86) runs two cases with the same failure point at the host boundary:

1. Worker exits before writing the marker.
2. Worker writes and closes the marker, then exits before sending a terminal frame.

The test independently checks the terminal (`status`, `reason`, `namespace_reset`) and the effect oracle (marker contents). At the pinned baseline commit, the terminal had no `effect_state` field. After the Durable Effect Protocol was merged, the same regression now asserts that current terminals report `effect_state=unknown` in both cases. It also checks that the live host does not replay the marker within a 150 ms observation window.

### Measured result

At the initial baseline checkpoint, both cases returned `status=failed`, `reason=worker_exit`, and `namespace_reset=true` with no `effect_state` field. The effect oracle differed: absent before the effect, present once after the effect. After the Durable Effect Protocol merge, both current terminals retain the same execution status/reason/reset fields and additionally report `effect_state=unknown`; the marker still differs between cases. Thus the terminal cannot tell whether the external effect occurred, and the ledger correctly labels the outcome as uncertain. The historical baseline probe passed **2 cases** and its then-related suite passed **56 tests**; expanded Handler results are listed below. Exact baseline commands and environment are recorded in [`failure_baseline.json`](../benchmarks/failure_baseline.json).

This establishes a narrow but useful result: at this fault point, the host's terminal does not identify whether the external effect occurred. It does **not** establish false-success behavior by a model, duplicate effects under automatic retry, or whole-runtime recovery behavior.

## Handler capacity and lifecycle fault matrix

The focused rig narrows the LLM mailbox handler budget to 16 slots (`result_limit=4 × handler_result_limit=4`). Each accepted Python request reserves all 16 per-execution slots, so the test can exercise full-budget admission and cleanup without relying on load timing.

- **Cancel at capacity:** while one execution is running with all handler slots reserved, a second request is rejected. After cancellation and terminal consumption, both terminal and handler reservations reach zero, and a follow-up request is admitted.
- **Timeout at capacity:** a registered completion callback blocks past the execution timeout. The terminal reports `timeout` and `handler_result_status=incomplete`; a post-terminal late handler result is rejected, reservations reach zero, and a follow-up request is admitted.
- **Host close at capacity:** closing the live host and bus clears the full reservation set; a later handler event is rejected as `runtime_closing`.
- **Duplicate handler result:** the test duplicates a parsed handler result at the host result-application boundary. The host emits one result, increments `late_frames` for the duplicate, and releases all slots after both the handler event and terminal are consumed.

`python -m pytest tests/test_fault_semantics.py -q` passed **11 cases in 18.06s** on the pre-protocol baseline: two worker-exit cases, four Handler cases, and five controlled crash-window probes. This covers deterministic in-process lifecycle paths and controlled Runtime shutdown/restart. It does not simulate duplicate IPC bytes, abrupt OS process death, power loss, or concurrent stress. The duplicate handler frame remains injected after parsing and before bus delivery.

After the Durable Effect Protocol merge, the updated module passed **11 cases in 27.09s** with both worker-exit variants reporting `effect_state=unknown`; the combined current-main suite passed **137 tests in 102.03s**.

## A0/A1/A2 scripted mechanism ablation

These are explicit benchmark operationalizations, not a claim to reproduce an unavailable historical implementation:

- **A0, blocking tool commit:** a benchmark-only `PythonExecTool` wrapper waits for the matching `python.finished` before returning from tool invocation. This blocks the actor around the unchanged asynchronous PythonHost; it is not a separate synchronous worker implementation. The normal terminal event remains routed to the actor.
- **A1, asynchronous tool:** the standard tool returns `accepted` immediately; no completion handler is registered.
- **A2, asynchronous tool with Handler:** the standard tool returns `accepted` immediately and registers a one-shot `on_finished` callback that removes itself with `off_finished`.

Five paired blocks ran each mode once in seeded random order (`seed=20261013`). Each fresh Runtime used a real serial PythonHost and `ScriptedBackend`, the same 220 ms worker task and marker, and the same status prompt submitted immediately after `python.started`. The measured response delay is from status submission until the scripted backend first sees that input; it is not provider or user-visible model latency. Each trial starts a fresh worker, so accepted-to-finished time includes process startup.

| Mode | Status prompt observed, mean / median | Python accepted → finished, mean | Handler events | Backend calls | Success |
|---|---:|---:|---:|---:|---:|
| A0 | 222.133 / 222.083 ms | 822.675 ms | 0 | 2.0 | 5/5 |
| A1 | 0.508 / 0.484 ms | 827.450 ms | 0 | 3.0 | 5/5 |
| A2 | 0.352 / 0.347 ms | 804.951 ms | 5 | 3.0 | 5/5 |

Paired status-latency deltas were **A1-A0: -221.625 ms** (median -221.589), **A2-A0: -221.781 ms**, and **A2-A1: -0.156 ms** across five pairs each. Under this scripted setup, blocking the actor delayed its next response to an already submitted user message by about the worker sleep duration. A1 and A2 behaved similarly on that measure; the small A2鈭扐1 difference is not evidence of a Handler latency benefit. A0 also used one fewer backend activation per trial because the queued status and terminal were observed together after the blocking call. Backend call count is not token cost. With five pairs and an immediate scripted responder, these results are mechanism evidence only, not a quality or performance claim for a real model.

The reproduction command, environment, per-trial values, and all paired differences are in [`handler_ablation.json`](../benchmarks/handler_ablation.json); the runnable script is [`bench_handler_ablation.py`](../benchmarks/bench_handler_ablation.py).

## Crash-window matrix

| Fault point | Current observable evidence | What is not justified | Recovery entry point at this commit |
|---|---|---|---|
| Before bus admission | Rejection or no accepted event. | No accepted task can be inferred. | None needed. |
| After admission, before host dequeue | Both the controlled gate and an abrupt OS-process probe leave an accepted request in memory. After pre-kill Journal flush, a new OS process sees the old request row but does not replay it; the marker stays absent. | Journal observation is not a recovery instruction. | No startup reconstruction. |
| Host pending, before IPC dispatch | Pending work is held in the host's memory deque. A live host may cancel it during shutdown or worker reset. | Process exit does not preserve the pending deque or its reservation owner. | No startup reconstruction. |
| IPC dispatch / worker `started` | A fault injected after the complete frame is delivered but before the host send call returns lets the worker write the marker; the host reports `cancelled / namespace_reset_before_start`. | A send exception at this boundary does not prove that Python did not start. This synthetic post-delivery exception does not establish how often a real socket raises after peer delivery. | No durable dispatch ledger or reconciliation. |
| Effect before terminal frame | The worker can change the environment and then exit. The test proves marker absence/presence can differ while the host terminal is identical. | A failed worker terminal cannot be treated as proof of "no effect"; blindly rerunning arbitrary code may duplicate an effect. | The inspected worker-exit path does not re-enqueue the failed execution or query an effect oracle. |
| Terminal offered, before actor consumes it | Controlled and abrupt-process gates leave the worker effect and accepted python.finished queued. After OS termination, a fresh Runtime does not replay the terminal row; the marker remains present. | Successful delivery to an in-memory mailbox is not durable actor consumption. | No startup mailbox reconstruction. |
| Delivery to a closed terminal mailbox | Closing the actor mailbox after `started` causes terminal delivery rejection, increments `undelivered_terminals`, and emits `agent.error` to UI. There is no terminal retry. | The rejected terminal is visible as a delivery failure but is not retained in a durable outbox. | No outbox or replay loop. |
| Event projected, before/while provider request | The scripted provider receives `python.finished` in the request and blocks; the mailbox lease/reservation is already consumed. After Runtime shutdown, a fresh Runtime has no prior transcript. | Consumption plus provider-request construction is not durable acknowledgement; restart does not resume the provider call or reconstruct transcript state from the Journal. | No cross-process activation resume. |

The five in-process probes are in [test_fault_semantics.py](../tests/test_fault_semantics.py): `test_accepted_request_waiting_before_host_dequeue_is_not_replayed`, `test_frame_delivered_before_send_error_can_run_despite_prestart_terminal`, `test_closed_terminal_mailbox_records_undelivered_result`, `test_queued_terminal_is_not_replayed_after_runtime_restart`, and `test_consumed_terminal_is_projected_before_interrupted_provider_call`. They use explicit asyncio gates or a post-send fault hook. Their restart cases perform a controlled Runtime shutdown and construct a fresh Runtime. The separate OS-process termination probe below uses a parent process to terminate a child Runtime. The IPC case deliberately delivers the whole frame before raising a synthetic local `OSError`, so it demonstrates semantic ambiguity at that boundary, not a measured property of ordinary socket failure rates.

The closed-mailbox probe closes the actor mailbox after the worker's `started` event but before completion. The worker's completion is rejected; the host increments `undelivered_terminals` and routes an `agent.error` to the UI, with no automatic retry. A separate actor gate lets a successful terminal offer remain queued and unconsumed while the worker effect is already present; shutdown plus a fresh Runtime does not replay the event. In the consumption probe, the scripted backend confirms that the terminal was included in its request while the LLM mailbox is empty; interrupting that provider call and starting a new Runtime loses the in-memory transcript, even though the Journal shares the workspace.

"No recovery entry point found" is a source-audit result for this commit, supported by the deterministic probes below. The two abrupt-process probes observed no replay at their specific fault points. They do not rule out recovery paths outside those points or replace a power-loss/storage-failure experiment.

## Abrupt OS-process recovery probe

[`test_os_process_death_does_not_replay_journaled_work`](../tests/test_subprocess_recovery.py) launches a Python child process that starts a real Runtime and PythonHost against the fixture workspace and SQLite Journal. Two sentinel-gated fault points are exercised:

- **Accepted `python.requested`, before PythonHost dequeue:** the child's `take_batch` waits on an asyncio gate. The request is accepted, the Journal writer is flushed and the row is read back, the worker PID is recorded, and the marker is absent before the pytest parent terminates the child OS process tree.
- **`python.finished` offered, before actor consumption:** the actor's mailbox take is gated. The Python worker writes the marker and offers its terminal, which is confirmed in the queued mailbox and flushed Journal before the parent terminates the OS process tree.

For both cases the parent launches a new OS process and a fresh Runtime using the same workspace and Journal file. The new Runtime has a different session ID, emits neither old `python.requested` nor old `python.finished`, and does not change the marker. The old request event is present in the old session's Journal after the pre-kill flush; for the terminal case, the old terminal row is also present. This demonstrates that these Journal rows are observations, not replay instructions. The evidence is scoped to the tested Windows subprocess termination and local marker oracle.

Reproduction command: python -m pytest tests/test_subprocess_recovery.py -q — 2 passed in 7.49s on the final cleanup revision. The combined command python -m pytest tests/test_subprocess_recovery.py tests/test_fault_semantics.py tests/test_events.py tests/test_python_host.py tests/test_providers.py tests/test_llm.py -q initially had 66 passed and 1 failed because test_python_environment_tool_runs_before_smoke_test exceeded its 8-second idle wait. That test passed alone in 2.37s; the combined rerun passed 67 tests in 132.35s.

The parent uses taskkill /T /F on Windows to terminate the Runtime child and worker descendants; POSIX uses a dedicated process group. A direct worker PID fallback is attempted only if tree termination fails while the Runtime parent was live, so successful tree termination is not followed by a kill against a possibly recycled PID. The parent reaps both processes and removes sentinel files. This is not a power-loss test: the Journal is explicitly flushed before termination, so it measures process loss after known durable observation writes, not loss during SQLite/WAL writes. It does not claim replay behavior for storage failure or arbitrary filesystem effects; the worker marker is only a local effect oracle. The child uses a scripted backend and does not test provider or model recovery.


**Supported by the inspected implementation and existing tests:** bounded in-process admission and rejection; terminal/handler capacity reservation; worker supervision; worker-exit, timeout, and cancellation reporting while the host remains alive; event projection into normal provider requests; explicit pause for an unknown provider response in the same process.

**Not provided by this baseline:** durable admission, cross-process task or transcript recovery, exactly-once effects, rollback of arbitrary Python side effects, automatic effect reconciliation, or a guarantee that a failed execution had no effect. The SQLite Journal is an asynchronous observation log, not an execution owner or recovery queue.

**Not measured here:** abrupt process death outside the two probed fault points, power loss, an exact close-vs-terminal-offer race, concurrent mailbox stress, task quality, false-success rate in a real model, duplicate effects under retry strategies, real-provider latency, or token cost.

## Evidence and reproduction

- Fixed source commit: `41c83d0c3f2fbc81d49993072cbfa585d7047742`; production files were not modified on this research branch.
- Expanded deterministic fault probe: python -m pytest tests/test_fault_semantics.py -q — **11 passed in 18.06s**.
- Abrupt process probe: python -m pytest tests/test_subprocess_recovery.py -q — **2 passed in 7.49s on the final cleanup revision**.
- Related suite: python -m pytest tests/test_subprocess_recovery.py tests/test_fault_semantics.py tests/test_events.py tests/test_python_host.py tests/test_providers.py tests/test_llm.py -q — **67 passed in 132.35s** after the transient idle timeout described above.
- Full suite after the cleanup revision: python -m pytest -q — **124 passed in 79.57s**.
- Scripted A0/A1/A2 ablation: `python benchmarks/bench_handler_ablation.py --blocks 5 --seed 20261013 --json benchmarks/handler_ablation.json` — **15/15 trials succeeded**, five randomized paired blocks.
- Environment: Windows 11 build `10.0.26300`, AMD64, Python `3.13.14`, pytest `9.1.1`; the ablation JSON captures its own environment metadata.
- Fault tests are deterministic in-process checks, not stress tests. The ablation is a small ScriptedBackend mechanism microbenchmark, not a real-model or production-task benchmark.

## Next research slices

1. **A — Journal observation durability:** the effect ledger now has injected SQLite commit-failure coverage. Still test bounded Journal enqueue/drop behavior, partial-batch flush, and termination before flush; keep power-loss claims separate from process-kill evidence.
2. **B — uncertain external operations:** inject process termination around the ledger's `dispatching` transition and the worker side effect, then confirm `/effects` exposes a stable unresolved row for manual reconciliation. Compare retry only where an idempotency contract makes it safe.
3. **C — agent responsiveness and quality:** measure task quality, response latency, completion time, token use, and duplicate/false-success behavior under matched tasks and provider settings.

Keep delivery correctness, recovery semantics, and model behavior supported by separate evidence.
