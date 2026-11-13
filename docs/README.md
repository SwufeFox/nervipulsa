# Documentation

Start with [Current implementation architecture](architecture.md) for the
request-to-terminal flow, module responsibilities, state ownership, persistence,
and security boundaries. It is the maintained architecture entry point for this
checkout; the project [README](../README.md) covers usage and extension
examples.

The architecture reference changes with implementation. Dated research files
keep their original dates, revisions, and measurements: they are evidence for
the version they studied, not descriptions of the current checkout. When a
research claim is used to justify current behavior, verify it against the
architecture reference and the tests in this tree.

## Current references

| Document | Purpose | Evidence boundary |
| --- | --- | --- |
| [Current implementation architecture](architecture.md) | Authoritative onboarding map of the control chain, module/state ownership, durable memory and effect boundaries, activation correlation, bounded diagnostics, failure semantics, and deferred decisions. | Maintained against this checkout; source review plus the local test suite. It does not claim live-provider, long-run, or hostile-workspace validation. |
| [Durable Effect Protocol](durable_effect_protocol.md) | Current ledger states, synchronous commit boundaries, `/effects` operator assessments, SQLite failure injection, and process-termination results. | Tracks worker observation and operator attestations separately; no automatic replay, rollback, or exactly-once guarantee. |
| [Design contract](../Nervipulsa_Design_v0.4.md) | Intended v0.4 behavior, including the invariants and required coverage in section 13. | Design intent and acceptance criteria, not implementation evidence. |
| [Recorded results](../TEST_RESULTS.md) | Historical benchmark and real-model measurements with their dates and adapter context. | Attached to the recorded revision and environment; earlier adapter runs do not validate the current adapter. |

## Research and historical records

| Document | Purpose | Evidence boundary |
| --- | --- | --- |
| [Failure semantics baseline](failure_semantics.md) | Focused audit of admission, worker effects, handler capacity, and recovery windows. | Pinned to commit `41c83d0`; its process-kill evidence predates the durable effect ledger and is not a protocol recovery test. |
| [Runtime and harness architecture research](runtime_harness_architecture_research.md) | Append-only chronology of source reviews, experiments, failures, and open questions. | Read each dated entry in its own code and environment context. |
| [Event-native runtime paper](event_native_runtime_paper.md) | Research synthesis of the runtime model, results, discussion, and limits. | Claims cite the underlying research record; this is a research draft, not a production-readiness claim. |
| [Literature screening and protocol comparison](literature_screening_2026-10-16.md) | Pollard, KedgeFlow, Temporal, transactional outbox, idempotency, memory reuse, and the local T0–T4 evidence boundary. | Selected source sections were checked; one secondary article's author/date/source-count metadata remains unverified. |
| [LayerFS integration research](layerfs_research_2026-10-18.md) | Version-separated review of LayerFS v0.1.7/current `main`, storage and Workspace semantics, evidence limits, and a Nervipulsa prototype experiment design. | Upstream release/docs review; no LayerFS code was run and no integration claim is made. |
| [User experience review](user_experience_review.md) | Code-based first-use journey, visible feedback, recovery friction, and prioritized improvements. | Source review only; no current live-provider run or first-time-user study. |
| [Worker handler bridge research](research_worker_handler_bridge.md) | Design and lifecycle notes for worker callbacks. | Source and test observations do not alone establish process recovery. |
| [Python environment discovery research](python_environment_discovery_research.md) | Static package and API discovery study. | CLI examples are individual observations, not general reliability claims. |

## Reproduction artifacts

- [`test_fault_semantics.py`](../tests/test_fault_semantics.py) — worker-exit,
  handler lifecycle, and controlled Runtime restart probes.
- [`test_subprocess_recovery.py`](../tests/test_subprocess_recovery.py) —
  parent-controlled OS process termination at two pre-protocol fault points.
- [`test_crash_boundaries.py`](../tests/test_crash_boundaries.py) — T0–T4
  Durable Effect Protocol process-termination boundaries, restart inspection,
  and no-replay checks.
- [`test_session_lock.py`](../tests/test_session_lock.py) — real OS contention,
  startup/shutdown cancellation, and ownership release on process death.
- [`subagent_acceptance.py`](../examples/subagent_acceptance.py) — opt-in real
  CLI task with isolated configuration, independent verification, transcript
  continuation, and session-reuse rejection; keeps separate run artifacts.
- [`bench_handler_ablation.py`](../benchmarks/bench_handler_ablation.py) —
  scripted A0/A1/A2 mechanism comparison.
- [`failure_baseline.json`](../benchmarks/failure_baseline.json) and
  [`handler_ablation.json`](../benchmarks/handler_ablation.json) —
  machine-readable observations and benchmark records.

The [architecture reference](architecture.md) describes the current
implementation. The durable-effect protocol's state machine and existing
fault-injection evidence are documented in
[durable_effect_protocol.md](durable_effect_protocol.md). The failure-semantics
report remains explicitly pinned to the pre-protocol baseline.
