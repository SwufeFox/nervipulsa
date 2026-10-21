# Durable Effect Protocol

**Status:** merged to `main` in `6484416` on 2026-10-13 (Asia/Shanghai). This is a local execution ledger, not a durable workflow engine.

## Purpose

Python code may change files or contact external services before Nervipulsa receives a terminal frame. If the worker or host disappears in that interval, the runtime cannot infer that the effect did not happen. The protocol records this uncertainty before dispatch and keeps it visible for review.

The effect ledger is stored in SQLite table `durable_effects`, keyed by `(session_id, execution_id)`. Critical transitions use a synchronous connection with `PRAGMA synchronous=FULL`; the ordinary event Journal remains an asynchronous observation sidecar.

## State transitions

| State | Meaning | Durable boundary |
| --- | --- | --- |
| `intent` | The host is preparing dispatch for one execution; no execute frame has been sent. | The intent row commits before dispatch. If the following transition fails, no execute frame is sent and the row remains `intent`. |
| `dispatching` | The runtime committed its intent to send an execute frame. The worker may or may not have received all bytes. | Committed before writing the framed IPC request. A partial socket write is therefore treated as uncertain. |
| `observed` | The host received a worker terminal frame and committed the outcome. The outcome can be success or failure; this state does not claim exactly-once external effects. | Committed before `python.finished` is emitted. |
| `unknown` | A dispatched execution has no durable observed terminal, or the observed-outcome commit failed. The external effect may have happened. | The runtime attempts to persist `unknown`; if that write also fails, the row remains `dispatching` and still appears unresolved. |

Timeout, cancellation after dispatch, worker loss, and uncertain IPC writes are not automatically retried. A queued cancellation or a failure before dispatch reports `not_started`; it does not imply a dispatched effect.

## Operator visibility

Use `/effects` to list unresolved rows across sessions. Each entry includes session and execution identity, worker state, intent time, and the last persistence error when available. If an operator assessment exists, the latest assessment is shown in a separate field; the row remains listed until a worker terminal is durably observed.

Record an out-of-band assessment explicitly:

```text
/effects decide <session_id> <execution_id> <occurred|not_occurred|undetermined> --by "<operator>" --evidence "<what was checked and where>"
/effects history [limit]
```

Assessments append to SQLite table `effect_assessments`; repeated or contradictory assessments remain in history, with the newest shown alongside the unresolved row. The record does not mutate `durable_effects`, convert a human statement into `observed`, enqueue Python, or authorize a retry. Evidence and operator labels are free-text attestations: Nervipulsa does not authenticate the operator or query the target system to verify the claim. `not_occurred` therefore does not prove that replay is safe. No automatic reconciler or replay is provided. On 2026-10-16, focused coverage via `python -m pytest tests/test_journal.py tests/test_cli.py -q` passed **27 tests**; this does not cover live target verification. The new [CI workflow](../.github/workflows/tests.yml) configures the full suite on Ubuntu and Windows, but has not yet produced a hosted run.


## Failure-injection evidence

SQLite commit failures are injected through a proxy around a real SQLite connection, so the tests exercise `commit()` and rollback behavior rather than only replacing a Journal method:

- Intent commit failure leaves no effect row.
- Dispatch commit failure rolls back to the committed `intent` row and the host sends no execute frame.
- Observed commit failure rolls back to `dispatching`; the host emits a terminal with `effect_state=unknown`.
- If persistence of that unknown result also fails, the previously committed `dispatching` row remains unresolved rather than being represented as observed.

Relevant coverage lives in [`test_journal.py`](../tests/test_journal.py), [`test_python_host.py`](../tests/test_python_host.py), [`test_cli.py`](../tests/test_cli.py), and [`test_llm.py`](../tests/test_llm.py). The isolated protocol branch passed 29 focused effect tests and 124 tests in its full suite before integration. After merging the protocol and fault-semantics branches, `python -m pytest -q` passed **137 tests in 102.03s**; the integration run also updated the worker-exit regression to expect the current `unknown` state. The chronological failures and reruns are in the [architecture research log](runtime_harness_architecture_research.md).

## OS process-termination experiment (2026-10-14)

The new [`test_crash_boundaries.py`](../tests/test_crash_boundaries.py) probes five gated points by forcibly terminating the Runtime OS process tree, then starting a new Runtime against the same workspace and SQLite file. The worker writes an append-only local marker so a duplicate execution is distinguishable from the original side effect.

| Point | Kill boundary | Durable row after restart | Marker | `/effects` and replay observation |
| --- | --- | --- | --- | --- |
| T0 | After `intent` commit, before `dispatching` | `intent` | Absent | Listed unresolved; request is not replayed. |
| T1 | After `dispatching` commit, before the execute frame is written | `dispatching` | Absent | Listed unresolved; request is not replayed. |
| T2 | Worker wrote the marker and is held before returning a terminal | `dispatching` | Exactly one entry | Listed unresolved; the effect happened, but no terminal is inferred or replayed. |
| T3 | Host received the worker terminal, before committing `observed` | `dispatching` | Exactly one entry | Listed unresolved; the in-memory terminal is lost with the process and not replayed. |
| T4 | `observed` committed and `python.finished` entered the old LLM mailbox, with the actor held before consumption | `observed` | Exactly one entry | No longer unresolved; the old terminal is absent from the new session and its model input. |

The restart probe also sends a fresh user message through a `ScriptedBackend` activation. For T0–T3 it invokes the actual CLI `/effects` handler and checks that the original execution is listed; for T4 it checks the no-unresolved message. Session IDs change on restart. T4 therefore separates durable effect observation from transcript/model recovery: the ledger says `observed`, while the new Runtime does not receive the old terminal.

`python -m pytest tests/test_crash_boundaries.py -q` passed **5 tests in 44.96s** after the final `/effects` state and no-replay assertions were added. After the CLI assessment changes, the first repeat had four passes and a T3 child-startup gate timeout; isolated T3 passed in 11.71s, and the next full repeat passed **5 tests in 58.72s**. The broader regression command `python -m pytest tests/test_crash_boundaries.py tests/test_subprocess_recovery.py tests/test_python_host.py tests/test_journal.py -q` passed **36 tests in 187.57s** after the T4 gate correction and marker fix, before those final assertion-only additions. These are process-termination tests around completed SQLite commits, using a local file marker and scripted model backend. They do not test a kill during SQLite/WAL commit, power loss, storage-controller behavior, remote effects, general task recovery, or exactly-once execution. The ledger preserves uncertainty; it does not restore the prior task or transcript.



## Workspace fingerprint observations (2026-10-18, removed 2026-10-21)

The opt-in `--fingerprint-workspace` execution file-tree digest was removed on
2026-10-21 as a simplification cut: it scanned the whole workspace twice per
execution, was never validated by runtime experiments, and its history is kept
in git, not in this protocol document.

## Limits

The ledger does not make arbitrary Python effects transactional with SQLite. It does not replay Python, roll back file or remote changes, restore a transcript, or guarantee exactly-once behavior. `synchronous=FULL` is a commit policy, not evidence of recovery under power loss, storage-controller failure, or every crash boundary. The earlier [Failure Semantics Baseline](failure_semantics.md) remains pinned to commit `41c83d0`; its two process-kill probes predate this ledger and are not protocol evidence. The newer [T0–T4 process-termination experiment](#os-process-termination-experiment-2026-10-14) covers only the specified points after SQLite commits; it does not cover a kill during a commit or WAL write.
