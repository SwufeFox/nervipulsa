# Recorded results

## Runtime mechanics benchmarks (2025 incremental-accounting change)

Measured by `benchmarks/bench_runtime.py` on Windows, CPython 3.13.14, before
and after two mechanism changes: (1) incremental transcript size accounting in
`nervipulsa/llm.py`, and (2) event-driven idle wakeups in
`nervipulsa/python_host.py`. All probes are credential-free.

| Probe | Before | After | Change |
| --- | --- | --- | --- |
| Context measurement, 400 messages / 859,333 chars | 37.44 ms per 4 measurements (full `json.dumps`) | 0.033 ms (incremental) | ~1100x, same measured path |
| Host idle wake: accepted → `python.started` | 44.8 ms mean, 50.9 ms max (12 rounds) | 1.9 ms mean, 5.0 ms max (12 rounds) | ~24x |
| Idle scheduler churn | one `asyncio.to_thread(_poll_idle)` dispatch every 0.2 s | 8 wakeups / 2 s, zero thread dispatches | polling thread removed |
| Transcript accounting invariant | exact | `chars_match_full_dump: True` | unchanged |

Two notes on the "before" column. The 37.44 ms legacy figure is measured on the
same transcript object as the 0.033 ms figure by running the old
`json.dumps(snapshot())` path alongside the new one, so the comparison is
like-for-like. The 44.8 ms idle-wake figure comes from a 12-round probe against
the pre-change host.

Behavior is preserved: `python -m pytest -q` -> 47 passed (the suite gained one
new test, `test_incremental_length_matches_a_full_serialization`, which asserts
the incremental counter stays bit-exact through appends, direct external list
mutation, and summary replacement); `python -m nervipulsa.demo --scripted` ->
`scripted contract: PASS`.

## Current HTTP adapter, context, and output verification

**Current HTTP adapter, context, and output verification:** `python -m pytest -q` ->
44 passed on Windows, CPython 3.13.14, pytest 9.1.1. Six
`tests/test_providers.py` cases use a local OpenAI-compatible HTTP stub; no
external provider credentials or endpoint were used. The scripted demo reported
`scripted contract: PASS`.

An interactive CLI smoke used a local provider stub. It displayed bounded code,
success status/duration, and a 5,021-byte stdout preview; a second execution
displayed `ValueError: SMOKE-EXCEPTION`. `/output evt_00000002 stdout` returned
the full result through `SMOKE-END`, and `/logs` showed transcript characters,
the configured limit, per-input additions, and provider-reported usage.
`/exit` returned code 0.

The 27-test result and real-model/interactive TTY runs below predate the
migration from the hand-written `urllib` adapter to the SDK adapter. They remain
historical evidence, not validation of the current adapter. The live experiment
driver now uses the direct HTTP adapter; no live provider was called after that migration.

Historical automated suite environment: Windows, CPython 3.13.14, pytest 9.1.1;
the 27-test suite used no provider credentials.

    python -m pytest -q
    -> 27 passed

    python -m nervipulsa.demo --scripted
    -> scripted contract: PASS
    -> real model: NOT VERIFIED by this command; see examples/real_model_experiments.py

Both commands exit non-zero on any failed assertion, so a green run is evidence
for exactly two claims: the scripted protocol and the real-process tests passed
on this machine. It is not evidence about model quality.

## Interactive CLI smoke

Using `deepseek-flash` at `http://127.0.0.1:8317/v1`, the initial TTY smoke
displayed `accepted`, `started`, and `finished succeeded`; `/status` showed
Python idle after completion, and `/exit` exited with code 0.

A later TTY smoke left a two-line draft unsubmitted while a 90 s Python job ran.
The completion output appeared with the draft still present; Enter then sent
the intact message and the model replied `ASYNC_DRAFT_ACK`. A separate `/config`
smoke saved settings in an isolated config directory and exited with code 0.

A separate keybinding smoke completed `/he` with Tab, submitted `/help`, and
recalled `/help` with Up; `/exit` exited with code 0.

The config regression smoke used Tab on `/config se` and showed the `set`
suggestion, then exercised `/config set api-key` and the full `/config`
wizard in an isolated config directory. A following `/config show` remained
a CLI command after password entry rather than becoming journaled user text;
the saved key was checked only for presence. `/exit` returned code 0.

A TTY uninstall smoke created user settings and workspace metadata. A `NO`
response preserved both and kept the CLI running. Confirming with `DELETE`
removed both after shutdown; a project file and an unrelated config-directory
file remained, and the CLI exited with code 0.

## What each file covers

| File | Tests | Verified behavior |
| --- | --- | --- |
| `tests/test_events.py` | 5 | Rejection reasons, unknown routes, payload limits, FIFO by `seq`, reserved terminals surviving a full ordinary lane, route freeze, closing states, observer failure not undoing delivery, no synchronous re-entry |
| `tests/test_python_host.py` | 4 | Two fast executions accepted immediately and delivered serially; cwd restored each call; variables survive an exception; stdout/stderr/traceback separation and exception summary; child-process output captured; timeout, queued cancel and running cancel each produce exactly one terminal; the worker's grandchild is dead after cancel; worker crash resets the namespace; idle crash restarts and raises `python.environment_changed`; large output has separate bounded previews and complete per-stream artifacts |
| `tests/test_llm.py` | 12 | Batching and no call while idle; a message arriving mid-inference waits for the next activation; invalid arguments reject the whole response without executing; a partially rejected multi-call keeps its one native receipt per call; a commit interrupted mid-delivery repeats only the undelivered call; `response_unknown` never auto-retries; a retry does not re-append the same input; serialized-character accounting and provider usage; budget/context pauses; compaction preserves current and pending tool calls, pairs receipts, and resumes without replaying completed code; shutdown and journal secrecy |
| `tests/test_config.py` | 2 | CLI beats environment beats file; the key round-trips but stays out of the public view |
| `tests/test_cli.py` | 12 | Contextual completion; redacted config display; piped uninstall guard and safe removal; execution summaries, full output retrieval, and artifact retention across sessions; provider switching applies immediately, clears stale keys, resets custom endpoints unless replaced explicitly; existing-journal migration |
| `tests/test_demo.py` | 3 | The scripted demo exits 0; the demo without `--scripted` refuses to imply a real run; live experiment workspaces preserve prior outputs |
| `tests/test_providers.py` | 6 | Local OpenAI-compatible endpoint; tool/reasoning normalization, malformed-response handling, auth/error classification, secret redaction, and no hidden retries |

The demo trace additionally asserts, from observed events: four model calls, two
executions, the interjection as its own activation ordered after the first
snapshot, the terminal result as the input of a later activation, one native
tool receipt per tool call (never containing final stdout), runtime observations
projected as a separate envelope, and no model call while idle.

## Real model, two recorded runs

The first run used model `deepseek-flash` through a local OpenAI-compatible
endpoint. The runnable driver is `examples/real_model_experiments.py`; it reads
credentials from `NV_BASE_URL` / `NV_API_KEY` / `NV_MODEL` and writes JSON and
text reports per experiment. The first run passed all three experiments:

| Experiment | Result | Model calls | Tokens (prompt/completion) |
| --- | --- | --- | --- |
| 1. Interject during a 20 s execution | PASS, 7/7 checks | 3 | 2580 / 583 |
| 2. Change the multiplier mid-execution | PASS, 5/5 checks | 4 | 3537 / 3049 |
| 3. Real bug fix with a compatibility note | PASS, 5/5 checks | 10 | 225868 / 8763 |

First-run observations, not asserted by the scripted suite:

- Experiment 1: the marker program was submitted once and never resubmitted. The
  reply to the interjection landed about 16 s before the marker finished, and a
  later activation reported the terminal result and the marker text.
- Experiment 2: step 1 slept and printed `7`; the multiplier change arrived
  during that sleep; the product then ran in a second execution queued behind
  the first and printed `21`. The activation that produced the product had the
  change message among its input events, so the new constraint reached the model
  before the arithmetic, not after it.
- Experiment 3: the model reproduced `assert -1 == 5`, changed `return a - b` to
  `return a + b`, kept the two-parameter signature, and a separate fresh test
  process then reported `2 passed`.

One failure in the first run is worth reading in full. In experiment 3 the model first asked for
`timeout=30` on a test run that takes 23-27 s on this machine (a 15 s slow test
plus interpreter and pytest startup). The host timed it out, reset the worker,
and cancelled the already-queued follow-up execution with
`namespace_reset_before_start` — exactly the sequence in section 7.3. The model
then measured the real elapsed time itself, raised its timeouts, and finished
correctly. So the timeout was a model budgeting error that the runtime contained,
not a runtime defect; no partial namespace leaked into the retry.

### Second recorded run

The second run used `deepseek-flash` at `http://127.0.0.1:8317/v1`:

| Experiment | Result | Model calls | Tokens (prompt/completion) |
| --- | --- | --- | --- |
| 1. Interject during a 20 s execution | PASS, 7/7 checks | 3 | 2421 / 418 |
| 2. Change the multiplier mid-execution | PASS, 5/5 checks | 4 | 5139 / 2124 |
| 3. Real bug fix with a compatibility note | PASS, 5/5 checks | 7 | 18449 / 3696 |
| **Total** | **PASS, 17/17 checks** | **14** | **26009 / 6238** |

The reports are retained under
`.scratch/authorized-real-model-dmwrmo1j/reports/`. Experiment 3 hit its
configured 60 s timeout on the initial test request; the worker reset and the
queued follow-up was cancelled. The model then completed the fix, and a fresh
pytest process reported `2 passed`. All three journals passed the API-key
exclusion check.

## Not verified

- These are two live runs of one model; they do not establish behavior across
  providers or model families, or general reliability. The automated test
  suite itself does not contact a provider.
- No crash-recovery, rollback, or sandbox guarantee. `python_exec` runs with the
  current user's permissions.
- The Windows path was exercised here; the POSIX process-group path was not run
  on this machine.
