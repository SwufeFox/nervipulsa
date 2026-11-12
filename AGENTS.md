# Repository Guidelines

## Project Overview

Nervipulsa is a local, event-driven coding-agent runtime (`nervipulsa`, Python
>=3.11, version `0.4.0.dev0` / v0.4-dev). User messages, model output, tool
receipts, and Python results travel as events on bounded mailboxes, so model
inference does not block Python execution. The package implements its own
OpenAI-compatible provider adapter, tool catalog, transcript/retry state,
SQLite journal, and persistent Python worker; it does not use a model SDK.

This is a development runtime, not a sandbox: `python_exec` runs with the host
user's operating-system permissions. Preserve the stated durability boundaries;
do not add hidden replay, rollback, or exactly-once claims.

## Architecture & Data Flow

- Entry: `nervipulsa/cli.py:main` (console script `nervipulsa`, also
  `python -m nervipulsa`) builds `Settings`, then calls
  `nervipulsa.application.create_runtime()`.
- Composition: `application.py` asks a `ProviderRegistry` for a backend and a
  `ToolRegistry` for a catalog, then constructs `Runtime`. `Runtime` owns one
  `Bus`, lane mailboxes, `LLMActor`, `PythonHost`/`NullPythonHost`, `Journal`,
  UI/lifecycle tasks, and the fixed route table.
- Flow: `submit_text()` / `submit_observation()` -> `Bus.call()` admission
  (JSON/size/capacity validation, seq/id assignment) -> LLM mailbox ->
  `LLMActor._activate()` projects input into `Transcript` ->
  `ModelBackend.complete()` -> validated tool calls -> tool receipts ->
  `python.requested` -> `PythonHost` queue -> worker IPC frames ->
  `python.finished` -> later activation.
- Concurrency: the host runs one asyncio loop. Blocking work stays off it:
  provider HTTP uses `asyncio.to_thread`, `PythonHost` moves process work and
  synchronous SQLite commits to threads, and `Journal` has a background writer
  thread. Worker reader threads hand frames back through a thread-safe queue and
  loop wakeup.
- Persistence split: ordinary event observations use the bounded async journal
  queue (drops set `journal.incomplete`). Durable effects and durable memory use
  separate synchronous SQLite connections with `PRAGMA synchronous=FULL`;
  activation + transcript snapshots use `BEGIN IMMEDIATE`.
- Effect ordering: before writing an execute frame, commit `intent` then
  `dispatching`; after a worker terminal, commit `observed` before publishing
  `python.finished`. If observation cannot be committed, report `unknown`;
  never infer "not executed".
- No replay: restart may call `Runtime.restore_transcript()` (model view only,
  newest committed snapshot) or inspect rows via `/inspect`, but never restores
  mailbox frames, the activation ledger, the Python namespace, or pending side
  effects. Timeout/cancel resets the worker epoch and namespace; it does not
  roll back files.
- Pauses: `response_unknown`, `commit_interrupted`, and
  `response_commit_failed` require an explicit retry. External
  `user.message` / `runtime.observation` inputs can recover ordinary pauses;
  internal tool results and handler feedback do not count as external input.

## Key Directories

| Path | Purpose |
| --- | --- |
| `nervipulsa/` | Runtime package: events, actor, providers, tools, worker, journal, config, CLI |
| `tests/` | Pytest suite organized by behavior and failure mode; real subprocess coverage |
| `docs/` | `architecture.md` (current reference), `README.md` (doc index), dated research records |
| `benchmarks/` | Credential-free mechanism benchmarks and retained JSON artifacts |
| `examples/` | Live-model experiment driver, manual pass criteria, and a bugfix fixture |
| `.github/workflows/` | CI: Ubuntu and Windows on Python 3.11 |

## Development Commands

```bash
# Editable install with test tooling
python -m pip install -e ".[dev]"

# Interactive CLI; first run: /config sets provider, model, and API key
nervipulsa --dir .
python -m nervipulsa --dir .

# Offline, credential-free scripted trace
python -m nervipulsa.demo --scripted

# Tests
python -m pytest -q
python -m pytest -q tests/test_llm.py
python -m pytest -q tests/test_crash_boundaries.py
python -m pytest -q tests/test_python_host.py::test_timeout_and_cancel_kill_the_process_tree

# Credential-free benchmarks
python benchmarks/bench_runtime.py --json benchmarks/result.json
python benchmarks/bench_handler_ablation.py --blocks 5 --seed 20261013 --json benchmarks/handler_ablation.json

# Live model checks (spend tokens; not part of CI)
NV_BASE_URL=... NV_API_KEY=... NV_MODEL=deepseek-flash \
  python examples/real_model_experiments.py 1 2 3
```

No lint, format, or type-check command is configured.

## Code Conventions & Common Patterns

- **Style:** four-space indent, `from __future__ import annotations` at the top
  of modules, type hints on public APIs, module/function docstrings that state
  the contract. No enforced formatter: match the surrounding file and avoid
  drive-by reflow.
- **Data records:** dataclasses for state and value objects; `frozen=True` /
  `slots=True` for events and delivery results. Use `StrEnum` for explicit
  states such as `RuntimeState` and `Lane`.
- **Async boundary:** never call SQLite, subprocess, socket, or blocking HTTP
  directly in the event loop; use the existing `asyncio.to_thread` or reader-thread
  paths.
- **Errors:** admission refusals return `Delivery(False, reason=...)`; invariant
  and persistence failures raise. Model/tool failures become activation pause
  states or typed provider failures, not process death.
- **Dependency injection:** depend on `ports.py` protocols. Registries accept an
  object with `.create(...)` or a callable. Tools receive a narrow `ToolContext`
  and `EventSink`; keep CLI/transport concerns out of core modules.
- **Events/state:** keep `session_id` scoping in SQLite; event ids and seq are
  per session. Use mailbox lanes and reservations rather than bypassing
  admission/capacity accounting. `agent.retry` is a control event, not
  transcript content.
- **Durability:** use the synchronous `FULL`-commit path for
  intent/dispatching/observed and durable memory. Do not replace those commits
  with the async observation queue or add automatic replay/resume.

## Important Files

| Path | Why it matters |
| --- | --- |
| `nervipulsa/cli.py` | `main`, argument parsing, interactive commands, config UI |
| `nervipulsa/application.py` | Composition root: registries -> backend/catalog -> Runtime |
| `nervipulsa/runtime.py` | Session ownership, public API, routing, restore/inspect |
| `nervipulsa/events.py` | Event validation, lanes, Bus, mailboxes, reservations |
| `nervipulsa/llm.py` | Actor, activation lifecycle, transcript, compaction, pause/retry |
| `nervipulsa/python_host.py`, `python_worker.py` | Execution queue, epochs, IPC, handlers, terminals |
| `nervipulsa/journal.py` | Async observations plus synchronous durable ledgers |
| `nervipulsa/providers.py`, `ports.py` | Transport adapters and extension contracts |
| `nervipulsa/config.py` | Precedence, profiles, redaction |
| `docs/architecture.md` | Current implementation reference; update when behavior changes |

## Runtime/Tooling Preferences

- CPython **3.11+**. CI runs Python 3.11 on `ubuntu-latest` and `windows-latest`.
- `pip` + `setuptools` (`>=68`); no lockfile or alternate package manager.
  Runtime dependency: `prompt-toolkit>=3.0.43`; dev extra: `pytest>=8`.
- No Ruff, Black, mypy, or pyright configuration. Do not introduce a formatter
  or type checker as part of an unrelated change.
- Generated/local state: `.nervipulsa/` (journal, outputs), `.scratch/` (tests,
  benchmarks, demo fallback), `.venv/`, `build/`, `dist/`, `*.egg-info/`; these
  are ignored or untracked.
- Windows specifics: process-tree cleanup uses a Job Object. The AppContainer
  worker is experimental and opt-in via `NERVIPULSA_WINDOWS_APPCONTAINER=1`;
  its tests skip on non-Windows hosts.

## Testing & QA

- Framework: `pytest` only (`pyproject.toml`: `testpaths = ["tests"]`,
  `addopts = "-ra -p no:cacheprovider"`). No `pytest-asyncio`; async scenarios
  commonly use `asyncio.run(...)` inside synchronous tests.
- Full suite: `python -m pytest -q`. Expect several minutes: worker and crash
  tests start real processes, sleep, and use deadlines up to about 70 seconds.
- Run the narrowest file or test first when changing a subsystem, for example
  `tests/test_events.py`, `tests/test_llm.py`, `tests/test_python_host.py`, or
  `tests/test_crash_boundaries.py`.
- Tests are behavior/integration contracts: event order and capacity,
  pause/retry, commit ordering, no replay, process-tree cleanup, and provider
  mapping against a local HTTP stub. Do not replace real process tests with
  mocks or loosen timeouts just to make a run green.
- No coverage configuration or threshold exists. Add regression tests for
  changed boundaries; use the `workspace` fixture (`tests/conftest.py`) or
  `tmp_path`. Provider tests never contact live endpoints; live experiments are
  opt-in and spend tokens.
