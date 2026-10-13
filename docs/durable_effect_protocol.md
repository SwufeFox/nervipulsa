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

Use `/effects` to list unresolved rows across sessions. Each entry includes session and execution identity, state, intent time, and the last persistence error when available. The command explicitly tells the operator not to replay automatically. Resolution is a human reconciliation step; this implementation does not provide an automatic reconciler.

## Failure-injection evidence

SQLite commit failures are injected through a proxy around a real SQLite connection, so the tests exercise `commit()` and rollback behavior rather than only replacing a Journal method:

- Intent commit failure leaves no effect row.
- Dispatch commit failure rolls back to the committed `intent` row and the host sends no execute frame.
- Observed commit failure rolls back to `dispatching`; the host emits a terminal with `effect_state=unknown`.
- If persistence of that unknown result also fails, the previously committed `dispatching` row remains unresolved rather than being represented as observed.

Relevant coverage lives in [`test_journal.py`](../tests/test_journal.py), [`test_python_host.py`](../tests/test_python_host.py), [`test_cli.py`](../tests/test_cli.py), and [`test_llm.py`](../tests/test_llm.py). The isolated protocol branch passed 29 focused effect tests and 124 tests in its full suite before integration. After merging the protocol and fault-semantics branches, `python -m pytest -q` passed **137 tests in 102.03s**; the integration run also updated the worker-exit regression to expect the current `unknown` state. The chronological failures and reruns are in the [architecture research log](runtime_harness_architecture_research.md).

## Limits

The ledger does not make arbitrary Python effects transactional with SQLite. It does not replay Python, roll back file or remote changes, restore a transcript, or guarantee exactly-once behavior. `synchronous=FULL` is a commit policy, not evidence of recovery under power loss, storage-controller failure, or every crash boundary. The earlier [Failure Semantics Baseline](failure_semantics.md) is pinned to commit `41c83d0`; its two process-kill probes predate this ledger and must not be presented as tests of this protocol.
