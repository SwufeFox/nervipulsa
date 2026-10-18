# Documentation

Start with [Current implementation architecture](architecture.md) for the request-to-terminal flow, module responsibilities, state ownership, persistence, and security boundaries. It is the maintained architecture entry point for this checkout; the project [README](../README.md) covers usage and extension examples.

The architecture reference changes with implementation. The research log remains an append-only chronology; the paper is a research synthesis. Historical measurements stay attached to their original dates and revisions, and are not evidence that this round was tested.

| Document | Purpose | Evidence boundary |
| --- | --- | --- |
| [Current implementation architecture](architecture.md) | Authoritative onboarding map of the current control chain, module/state ownership, failure semantics, fingerprint assessment, and deferred decisions. | Source review with static checks only for this round; no new runtime validation. |
| [Runtime and harness architecture research](runtime_harness_architecture_research.md) | Append-only chronology of source reviews, experiments, failures, and open questions. | Read each dated entry in its own code and environment context. |
| [Event-native runtime paper](event_native_runtime_paper.md) | Current synthesis of the runtime model, results, discussion, and limits. | Claims cite the underlying research record; this is a research draft, not a production-readiness claim. |
| [Failure semantics baseline](failure_semantics.md) | Focused audit of admission, worker effects, handler capacity, and recovery windows. | Pinned to commit `41c83d0`; its process-kill evidence predates the durable effect ledger and is not a protocol recovery test. |
| [Durable Effect Protocol](durable_effect_protocol.md) | Current ledger states, synchronous commit boundaries, `/effects` operator assessments, SQLite failure injection, and T0–T4 process-termination results. | Tracks worker observation and operator attestations separately; no automatic replay, rollback, or exactly-once guarantee. |
| [Literature screening and protocol comparison](literature_screening_2026-10-16.md) | Pollard, KedgeFlow, Temporal, transactional outbox, idempotency, memory reuse, and the local T0–T4 evidence boundary. | Selected source sections were checked; one secondary article's author/date/source-count metadata remains unverified. |
| [LayerFS integration research](layerfs_research_2026-10-18.md) | Version-separated review of LayerFS v0.1.7/current `main`, storage and Workspace semantics, evidence limits, and a Nervipulsa prototype experiment design. | Upstream release/docs review; no LayerFS code was run and no integration claim is made. |
| [User experience review](user_experience_review.md) | Code-based first-use journey, visible feedback, recovery friction, and prioritized improvements. | Source review only; no current live-provider run or first-time-user study. |
| [Worker handler bridge research](research_worker_handler_bridge.md) | Design and lifecycle notes for worker callbacks. | Source and test observations do not alone establish process recovery. |
| [Python environment discovery research](python_environment_discovery_research.md) | Static package and API discovery study. | CLI examples are individual observations, not general reliability claims. |

## Reproduction artifacts

- [`test_fault_semantics.py`](../tests/test_fault_semantics.py) — worker-exit, handler lifecycle, and controlled Runtime restart probes.
- [`test_subprocess_recovery.py`](../tests/test_subprocess_recovery.py) — parent-controlled OS process termination at two pre-protocol fault points.
- [`test_crash_boundaries.py`](../tests/test_crash_boundaries.py) — T0–T4 Durable Effect Protocol process-termination boundaries, restart inspection, and no-replay checks.
- [`bench_handler_ablation.py`](../benchmarks/bench_handler_ablation.py) — scripted A0/A1/A2 mechanism comparison.
- [`failure_baseline.json`](../benchmarks/failure_baseline.json) and [`handler_ablation.json`](../benchmarks/handler_ablation.json) — machine-readable observations and benchmark records.

The [architecture reference](architecture.md) describes the current implementation. The durable-effect protocol's state machine and existing fault-injection evidence are documented in [durable_effect_protocol.md](durable_effect_protocol.md). The failure-semantics report remains explicitly pinned to the pre-protocol baseline.
