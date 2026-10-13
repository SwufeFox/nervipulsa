# Research documents

This index separates the project’s chronological record, current synthesis, and focused evidence reports. Historical measurements stay attached to their original dates; a later green test run does not retroactively validate earlier claims.

| Document | Purpose | Evidence boundary |
| --- | --- | --- |
| [Runtime and harness architecture research](runtime_harness_architecture_research.md) | Append-only chronology of source reviews, experiments, failures, and open questions. | Read each dated entry in its own code and environment context. |
| [Event-native runtime paper](event_native_runtime_paper.md) | Current synthesis of the runtime model, results, discussion, and limits. | Claims cite the underlying research record; this is a research draft, not a production-readiness claim. |
| [Failure semantics baseline](failure_semantics.md) | Focused audit of admission, worker effects, handler capacity, and recovery windows. | Pinned to commit `41c83d0`; its process-kill evidence predates the durable effect ledger and is not a protocol recovery test. |
| [Durable Effect Protocol](durable_effect_protocol.md) | Current ledger states, synchronous commit boundaries, `/effects` review, and injected failures. | Tracks intent and uncertainty; no automatic replay, rollback, or exactly-once guarantee. |
| [User experience review](user_experience_review.md) | Code-based first-use journey, visible feedback, recovery friction, and prioritized improvements. | Source review only; no current live-provider run or first-time-user study. |
| [Worker handler bridge research](research_worker_handler_bridge.md) | Design and lifecycle notes for worker callbacks. | Source and test observations do not alone establish process recovery. |
| [Python environment discovery research](python_environment_discovery_research.md) | Static package and API discovery study. | CLI examples are individual observations, not general reliability claims. |

## Reproduction artifacts

- [`test_fault_semantics.py`](../tests/test_fault_semantics.py) — worker-exit, handler lifecycle, and controlled Runtime restart probes.
- [`test_subprocess_recovery.py`](../tests/test_subprocess_recovery.py) — parent-controlled OS process termination at two fault points.
- [`bench_handler_ablation.py`](../benchmarks/bench_handler_ablation.py) — scripted A0/A1/A2 mechanism comparison.
- [`failure_baseline.json`](../benchmarks/failure_baseline.json) and [`handler_ablation.json`](../benchmarks/handler_ablation.json) — machine-readable observations and benchmark records.

The project [README](../README.md) remains the usage and implementation entry point. The durable-effect protocol is now part of `main`; its state machine and fault-injection evidence are documented in [durable_effect_protocol.md](durable_effect_protocol.md). The failure-semantics report remains explicitly pinned to the pre-protocol baseline.
