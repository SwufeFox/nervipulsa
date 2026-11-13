# Recorded results

## Shared-budget source reads (2026-11-13)

Added `hashline_edit.view_files(requests)` for 1–8 source excerpts sharing one
3 KiB UTF-8 return budget, including headers, separators and per-excerpt
continuation calls. Each request supplies `path` and optional `start`, `end`
and `column`. Repeated paths use one frozen whole-file snapshot per batch;
combined snapshots must fit the 16 MiB cache so returned tags remain usable.
The single-file API remains available. The tool description now recommends
batching short operations that do not require an intermediate model decision.

Focused Windows / CPython 3.13.14 verification:

```bash
python -m pytest -q tests/test_hashline_edit.py
python -m pytest -q tests/test_python_host.py::test_worker_hashline_import_view_and_put_edit tests/test_python_host.py::test_worker_source_page_survives_output_preview
```

**23 passed in 0.77s** and **2 passed in 1.44s**. New behavioral coverage checks
whole-file edit identity from targeted batch excerpts, stale edits after an
unseen change, Unicode/long-line continuation reconstruction, empty files,
same-file consistency during an external change, cache-budget rejection and
invalid/escaping requests. Existing real-worker checks still exercise import,
editing, source paging and the actual output preview boundary.

A separate real-worker experiment read the same six selected source regions
from one external input, issuing each next tool on the previous terminal.
It used an already warmed persistent worker, default FULL commits, a synthetic
20 ms provider delay, and the same 5 ms bookkeeping and numbered marker writes
per excerpt. Two trials per grouping ran in order 1/3/6/6/3/1. Active timing
ends after the final tool-free response is durably committed; startup, warm-up,
shutdown and offline inspection are excluded.

| Excerpts per execution | Python executions | Model requests | Active seconds, two-trial range |
| ---: | ---: | ---: | ---: |
| 1 (`view_file`) | 6 | 7 | 2.164–2.751 |
| 3 (`view_files`) | 2 | 3 | 0.815–0.824 |
| 6 (`view_files`) | 1 | 2 | 0.491–0.513 |

All six cases passed. Every case returned all six requested regions without
truncation, preserved the input files, and verified the numbered operation log,
accepted receipts, observed/succeeded effects, activation/snapshot correspondence
and persisted final response. The full batch's two-trial median was 4.90 times
faster than separate calls. This measures the complete scripted path, not only
the reader function or a live model's willingness to use batching.

A separate CPU/I/O probe requested eight short regions from the same
128,893-byte file. Three alternating paired samples returned identical 682-byte
views. Separate reads made eight raw reads per sample; the batch made one.
The measured median fell from 435.11 ms to 44.60 ms, with ranges 283.72–484.51 ms
and 43.41–47.30 ms respectively. This isolates snapshot reuse within the parent
process and excludes model, worker transport and durable commits.

The [portable evidence JSON](benchmarks/source_batching_2026-11-13.json) retains
all timings, output sizes, source hashes, environment and verification results.
Complete local journals, fixture files and the raw report remain under
`.scratch/source-batching-00078cd94767462da49949937104913d/`.
Disposable probe code was removed. No full-suite, hosted CI, live-model adoption,
statistical-confidence or production-capacity result is claimed for this change.

## Durable runtime throughput study (2026-11-13)

This study measures finite runtime capacity with FULL commits and no automatic
effect replay. It establishes a large event-loop responsiveness improvement,
not a general throughput increase or a production rating. Single-session
throughput regressed, and shared-journal failures remain in both revisions.

### Method and reproduction

Four serial runs used the order **A1 baseline / B1 offload / B2 offload /
A2 baseline**, with two independent trials per workload/session/layout cell.
A is `465a034048cd88e944ae45c39b69b47f58544d93`; B is the working-tree actor
persistence change. The exact same load script was copied into both source
checkouts. Harness SHA-256:
`5cbf854f91d70f2ab2539c1ad3ee81802462a37fce96772e04326b57f39836af`.
Of the four fingerprinted runtime modules (`llm.py`, `journal.py`,
`python_host.py`, `events.py`), only `llm.py` differs. Imported source paths,
both source hashes and the runtime defaults are retained in the JSON evidence.

Environment: Windows 11 build 26300, CPython 3.13.14, SQLite 3.50.4, x64,
8 logical CPUs, AppContainer disabled. All four runs used these arguments,
with a distinct output file and workspace root for each label:

```bash
python benchmarks/bench_load.py --sessions 1 2 4 --journals shared separate \
  --mode all --jobs 12 --rate 500 --seconds 2 --provider-ms 20 --work-ms 5 \
  --repeats 1 --seed 20261113 --json .scratch/load-reproduction.json \
  --workspace-root .scratch/load-reproduction
```

The 20 ms provider delay is synthetic async sleep; no live HTTP or inference
is measured. Worker mode runs actual Python processes, one outstanding task
per session, including a 5 ms worker sleep, marker write, accepted receipt,
observed effect, transcript snapshot and committed follow-up. Admission mode
schedules 1,000 ordinary inputs at an aggregate 500/s by absolute clock time;
it retains normal batching and reports producer lateness and refusals.
Input settlements are not model calls or coding-task completions.

Defaults are unchanged: ordinary capacity 64, result capacity 4, batch limit
32, context limit 200,000 serialized characters and activation budget 100.
FULL activation/snapshot/receipt and effect commits stay enabled. Startup and
one warm-up job/input per session precede active timing. Goodput includes
accepted-input drain, excludes startup/warm-up/shutdown/offline reconciliation,
and counts only work in sessions passing independent exact-set checks.
Those excluded phases still have recorded timings, and startup failure fails
the cell. Percentiles use nearest rank and include sample counts; parent CPU
excludes worker CPU. Two trials do not establish statistical confidence or
sustained capacity, and latency quantiles are not pooled across trials.

### All measured cells

Worker rates are independently verified Python tool tasks/active second;
admission rates are independently verified ordinary input settlements/active
second. A dagger marks incomplete evidence: the numeric value is a conservative
verified subset, not a passing capacity result. Startup failure has no rate.

| Workload | Sessions | Journals | A1 baseline | B1 offload | B2 offload | A2 baseline |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| worker | 1 | shared | 2.83 | 1.38 | 1.08 | 2.60 |
| worker | 1 | separate | 2.15 | 0.72 | 1.92 | 2.52 |
| worker | 2 | shared | 1.96 | startup failed | 2.19 | startup failed |
| worker | 2 | separate | 2.50 | 3.28 | 2.25 | 3.46 |
| worker | 4 | shared | 1.65† | 2.16 | 1.80 | 1.99 |
| worker | 4 | separate | 3.05 | 4.91 | 3.38 | 3.24 |
| admission | 1 | shared | 197.26 | 280.42 | 215.19 | 161.44 |
| admission | 1 | separate | 212.89 | 95.24 | 154.20 | 184.82 |
| admission | 2 | shared | startup failed | 217.05 | 175.90 | startup failed |
| admission | 2 | separate | 24.54 | 365.93 | 257.27 | 26.06 |
| admission | 4 | shared | 1.59† | 223.89 | startup failed | 0.00† |
| admission | 4 | separate | 4.02 | 367.28 | 435.24 | 6.93 |

**All four complete matrix commands exited 1.** In total, 40/48 cells passed;
all 24 independent-journal cells passed. The eight failed shared-journal cells
comprise five initialization failures (`OperationalError: database is locked`)
and three cells with incomplete observations. A1's shared four-session worker
cell verified 36/48 jobs; its shared four-session admission cell verified
250/1,000 inputs. A2's shared four-session admission cell verified 0/1,000.
All accepted work in these cells reached observed settlement, but a session
with incomplete audit evidence contributes no verified goodput. Zero verified
goodput here does not mean no input was processed or no effect occurred.
Failures are preserved; no rejected inputs were automatically retried.

### Interpretation and production boundary

- Event-loop responsiveness is the strongest result. In the four-session
  independent-journal worker cell, loop-lag p95 fell from 299–347 ms to
  14.7–15.0 ms. For ordinary input it fell from 890–1,353 ms to 14.35–14.42 ms.
  These are observations from a 10 ms timer, not scheduling deadlines.
- Throughput is workload dependent. Single-session worker and admission rates
  both fell in the independent-journal cells; two-session worker ranges overlap.
  Four-session worker throughput rose. Moving the wait off the loop does not
  make a FULL transaction faster or remove SQLite's single-writer constraint.
- The baseline's four-session independent-journal producer needed 144–247 s
  for a nominal two-second arrival plan. Current runs needed about two seconds,
  but rejected 166/1,000 and 0/1,000 inputs respectively. Current planned-arrival
  to committed p95 was 413–1,059 ms. Baseline active model requests were 1,000
  versus 39 and 52 after offload: preserved batching explains much of the
  ordinary-input gain. It is not a comparable gain in per-request database
  service speed or verified coding-task throughput.
- Shared-journal startup is still unreliable under concurrent session startup.
  Current four-session shared worker p95 latency remained 5.22–5.28 s even
  with low loop lag. The async observation journal remains best effort; a
  missing observation does not invalidate a separately committed effect, but
  it prevents the stricter audit proof required by this experiment. Do not
  derive shared-database production capacity from its successful subsets.
- SQLite 3.50.4 is in the potentially affected range of the official
  [WAL-reset defect](https://www.sqlite.org/wal.html#walreset). Use a version
  containing the fix (3.51.3+ or the listed 3.50.7/3.44.6 backports) before
  production storage qualification and rerun the workload. This experiment
  did not reproduce corruption and does not attribute lock errors to that bug.
- Required production evidence remains: representative task mix and latency/
  refusal SLOs, patched SQLite, shared-writer/startup behavior, long-duration
  load and fault combinations, real provider limits/thread contention, and
  resource curves for memory, WAL/disk growth and all managed processes.

### Implementation and regression verification

The actor persistence changes retain FULL transactions and await the actual
commit before admitting the next model/tool step or publishing settlement.
Cancellation waits for a started SQLite call; explicit retry can repair an input
snapshot or retained receipt without invoking an accepted tool again.

Focused Windows / CPython 3.13.14 verification completed in separate groups:

| Targets | Result |
| --- | --- |
| `tests/test_actor_persistence.py` | 14 passed in 12.57s |
| `tests/test_llm.py` | 29 passed in 37.34s |
| Durable memory, invalid-response recovery, activation correlation, session locks, T0–T4 crash boundaries, transcript restore, CLI and runtime observations | 106 passed in 162.26s |

These are 149 passing tests, not a full-suite result. The first combined command
was terminated at its 600-second deadline without a completed result; its cause
was not established. Diagnostic logs remain in the study directory. Baseline
[GitHub Actions #10](https://github.com/SwufeFox/nervipulsa/actions/runs/43387063038)
passed Ubuntu and Windows Python 3.11 jobs at `465a034`; it does not cover these
working-tree changes. No live provider, POSIX run, long-duration result, disk
fault experiment or production acceptance is claimed for this revision.

### Retained evidence

The portable [comparison JSON](benchmarks/durable_throughput_2026-11-13.json)
contains all 48 cells, exact configurations and source hashes, rates, latency
sample counts/distributions, stage timings, failure counts and regression
results. Long session-error lists are bounded with their original counts.
Complete per-job/event/row evidence remains in the ignored local directory:
`.scratch/throughput-study-04y2422o/`, files `baseline-a1.json`,
`offload-b1.json`, `offload-b2.json`, `baseline-a2.json`. These raw local paths
and the isolated baseline checkout are not portable CI artifacts.

The initial mechanics probe is also retained in the comparison JSON: context
accounting matched the full serialization; its four-measurement mean was
0.0778 ms versus 46.77 ms for the legacy serialization path. Its 24-tool scripted
session took 14.2795 s including client polling/settling, so that probe is not
used as a saturation rate.

## Supervised process deadlines (2026-11-13)

Added opt-in `--process-timeout` for one-shot `--prompt` calls. A synchronous
parent owns the runtime child, holds stdout until full interpreter exit, and
terminates the managed tree at expiry. Timing begins after parent argument and
configuration resolution, before child launch; up to five further seconds are
reserved for OS cleanup/reaping. The existing `--run-timeout` still initiates
orderly shutdown after the completion-wait deadline.

Windows installs a strict Job Object before releasing the runtime startup
gate. POSIX workers register with the supervisor before creating independent
sessions, and `waitid(WNOWAIT)` retains leader identities until group cleanup
or acknowledged retirement. The host child's proxy/TLS environment is preserved;
ordinary Python workers still receive the filtered environment. Child settings
and prompts use a private socket, and held stdout uses a temporary file rather
than growing a list of output chunks in memory.

Focused Windows / CPython 3.13.14 verification:

    python -m pytest -q tests/test_supervision.py tests/test_process_tree.py tests/test_cli.py tests/test_python_host.py tests/test_session_lock.py tests/test_appcontainer.py

**95 passed in 116.92s.** New real-subprocess and loopback-HTTP cases prove:

- A normal supervised call returns one success JSON object with coherent
  process/JSON exit status and its final response.
- A shorter completion timeout cannot leave blocking HTTP teardown running
  beyond the outer deadline and cleanup window.
- A child that has already produced a successful receipt but remains alive
  in executor teardown yields only the parent's timeout receipt. The successful
  child receipt is observed separately by a disposable startup extension.
- A real running Python tool and its gated descendant are stopped; releasing
  the descendant's file-write gate after termination cannot produce the effect.
  The session lease is released, and transcript continuation does not replay
  the already-performed tool effect.
- A worker's own timeout can still reset its namespace, start a replacement
  worker, and complete a task while the outer supervisor remains active.
- Invalid numeric process deadlines and positive deadlines without `--prompt`
  are rejected before running the task. Existing CLI, worker, session-lock,
  process-tree, and AppContainer regressions also passed in this group.

The updated `examples/subagent_acceptance.py` then passed **37 required
criteria**, with no failures or dependent skips, against Magpie at
`http://127.0.0.1:3425/v1`, model `deepseek/deepseek-flash`. Its separate local
stalled-HTTP case used `--run-timeout 0.25 --process-timeout 3`: the outer CLI
exited 124 in **4.269s**, within the deadline plus cleanup allowance, with
`supervision.timed_out=true`, `tree_stopped=true`, and no final message.
The live-model cases fixed the copied module, preserved the original test,
passed independent verification (**1 passed in 0.01s**), recalled the marker
after a fresh-process resume without tools, and rejected reuse without resume.
Every CLI invocation used supervision. The final report independently records
`runtime_completed=true` and `verified_task_success=true`.

Local artifacts are retained under
`.scratch/subagent-acceptance-10d86614bfe848ada51bdbdddd571541/`, including
`report.json`, process receipts, stderr, verifier output, fixture hashes, and
journal snapshots. No full-suite, hosted-CI, POSIX execution, or long-duration
result is claimed for this revision.

OS scheduling and process creation are not real-time guarantees. Processes
deliberately detached from managed groups remain outside containment; a forced
supervisor kill on POSIX cannot execute its cleanup handler. Experimental
Windows AppContainer is rejected with process supervision because its ACL and
profile cleanup cannot honor the bounded teardown contract. Forced interruption
can lose queued journal observations and leave effects uncertain; unknown
admission/activation/text fields are null, and no replay or rollback is implied.

## Subagent result and session ownership acceptance (2026-11-13)

This revision separates runtime completion from progress text. `final_message`
is the successful terminal activation's recorded, tool-free response; it is
null for every non-success result, including cleanup failure. `last_message`
retains the latest recorded text and activation ID for diagnosis. A later
commit-hook failure can no longer present earlier tool progress as the final
answer, and uncommitted response text remains unpublished.

Each Runtime now acquires a nonblocking OS lock for its canonical journal
path/session before journal startup or history reads. A competing CLI returns
`rejected/session_busy` without publishing input or calling the model. Startup
failure/cancellation and shutdown cancellation retain ownership until pending
initialization and cleanup finish, including actual journal-writer termination.
OS process death releases the lock; its persistent file is not deleted.

Focused verification on Windows / CPython 3.13.14:

    python -m pytest -q tests/test_cli.py tests/test_session_lock.py tests/test_transcript_restore.py tests/test_python_host_optional.py tests/test_crash_boundaries.py

**83 passed in 59.94s.** Coverage includes multi-activation success, commit-hook
failure, response-unknown, timeout and budget pauses; cleanup-failure results;
same-process and real-subprocess contention; independent sessions; cancellation
and process-death release; transcript restoration and event-ID preservation;
and T0–T4 crash/restart inspection without replay. POSIX locking is implemented
but was not exercised on this Windows host. No full-suite or hosted-CI result
is claimed for this revision.

The new opt-in `examples/subagent_acceptance.py` was then run with Magpie at
`http://127.0.0.1:3425/v1`, model `deepseek/deepseek-flash`. Each invocation used
a fresh fixture workspace and empty configuration directory. The final run
passed all **27 required criteria**, with no dependent skips:

- The model changed `calc.py` from subtraction to addition; the original test
  bytes remained unchanged. A separate verifier ran pristine tests against
  the resulting module: **1 passed in 0.01s**.
- A new CLI process resumed the session and recalled the unique first-prompt
  marker. No tool calls, receipts, executions, or Python requests were added
  during this continuation, and all prior journal rows were preserved.
- Reuse without `--resume` returned `rejected/session_exists`, exit 1, while
  retaining prior history. JSON status and process exits agreed throughout.
- The final report records `ok=true`, `runtime_completed=true`, and
  `verified_task_success=true` separately.

Evidence is retained under
`.scratch/subagent-acceptance-d6b7038298cd43d0bf2044c4793d1310/`: `report.json`,
CLI stdout JSON/stderr, verifier output, fixture hashes, and journal snapshots.
The earlier run under
`.scratch/subagent-acceptance-3137a33cb0354e7facaea401616300f2/` is also retained:
the runtime scenarios succeeded, but the verifier's `python -I` hid user-site
pytest. The driver now uses `-P`, preserving installed dependencies while
keeping a separate pristine verifier directory; the later full acceptance run
exercises that correction.

These are local live-provider observations, not a model-quality benchmark or
general production-readiness claim. `--run-timeout` still bounds completion
waiting, not total process lifetime. Blocking HTTP, worker cleanup, and waiting
for a journal writer can extend exit; no outer process supervisor or hard
process deadline was added. Restore remains transcript-only, with no effect
replay or rollback. Long-duration and cross-platform live runs remain unverified.

## Repository investigation dogfooding (2026-11-13)

After committing `17b258d`, Nervipulsa reviewed its own CLI/session lifecycle in
an isolated worktree at that revision, using Magpie and
`deepseek/deepseek-flash`. The task required a deterministic reproduction and
a JSON findings report, with no tracked-file edits or full-suite runs.

- The initial run repeatedly read truncated file prefixes and exhausted its
  activation budget before writing a report. This exposed a missing bounded
  source-reading workflow.
- A resumed run tried the new `view_file(start=..., end=...)` API but received
  `TypeError`: the worker imported Nervipulsa from the older workspace checkout,
  while its host advertised the updated helper. Worker bootstrap now uses
  Python's `-P` option and the host source root, then enables workspace imports
  for user code after runtime initialization.
- Source views now fit within 3 KiB, carry executable continuation cursors,
  support line ranges and Unicode-safe long-line paging, and retain full-file
  snapshot tags. The tool description explains the output cap and how to read
  targeted source without repeatedly dumping entire files.
- With both fixes active, the same subagent finished the review and wrote its
  report. Bounded helper pages were usable; arbitrary large `print()` calls
  still truncated. The inherited conversation also needed model compaction.

| Run | Python executions | Truncated outputs | Outcome |
| --- | --- | --- | --- |
| Initial repository task | 14 | 7 | Activation-budget pause; no report |
| Resume before bootstrap fix | 14 | 5 | Old workspace helper rejected `start`; budget pause |
| Resume after bootstrap fix | 9 | 3 | Report produced; CLI succeeded |

The report labeled an earlier recorded assistant message in `final_message`
after a later commit-hook failure as a defect. The parent independently
reproduced the output: exit 1, `paused/response_commit_failed`, the prior
recorded message, and no uncommitted answer. This matched the documented
last-recorded-text contract at the time; the initial change only clarified the
README. The later result-contract change is recorded above. The worktree had
no tracked-file changes from the subagent.

Local evidence is retained under
`.scratch/subagent-review-17b258d/.scratch/`: `review-findings.json`,
`parent-verification.json`, and the disposable reproduction. These are ignored
local artifacts, not portable CI results. Run the reproduction through `runpy`
from the repository root; executing the nested script directly does not put
the repository package on Python's import path.


Verification: source-view, provider, and selected real-worker tests reported
**23 passed in 5.76s**; worker lifecycle, process-tree, and AppContainer tests
reported **32 passed in 38.10s**. The latter includes real workers started in
workspaces containing a conflicting `nervipulsa` package or `json.py`, while
ordinary workspace-module imports remain functional.

The sessions share prior conversation and use corrective prompts; this is a
workflow investigation, not a controlled model-quality or performance benchmark.


## One-shot and resume smoke (2026-11-13)

Environment: Windows, CPython 3.13.14, local Magpie gateway at
`http://127.0.0.1:3425/v1`, model `deepseek/deepseek-flash`. These are live
provider runs, not CI.

- `--prompt 'Reply with exactly: PONG' --json`: `succeeded`, exit 0,
  `final_message: PONG`, 1375 total tokens.
- Session `demo-cont`: the first prompt acknowledged "Ada" (1363 tokens); a
  second prompt with `--session-id demo-cont --resume` answered `Ada`
  (1380 tokens, 1152 prompt-cache hits), showing the restored transcript
  reached the model.
- Reusing `demo-cont` without `--resume` was rejected with `session_exists`,
  exit 1, before any model call.
- The first exploratory subagent run exposed a worker bug: user code ending
  with `raise SystemExit(0)` after a passing pytest was reported as `failed`.
  The worker now treats `SystemExit(None)` and integer-zero exit as success;
  other exit values remain failures without terminating the persistent worker.
  Regression coverage includes a broken exit-value formatter and namespace
  preservation (`test_system_exit_zero_succeeds_and_nonzero_fails`).
- A final live file task wrote and reread `result.json` with answer `42`, ended
  its single Python execution with `SystemExit(0)`, and returned
  `VERIFIED_COBALT_42`. A fresh process with `--resume` returned `42` without
  another Python execution. All prior SQLite event rows were preserved; the
  continued input was `evt_00000008`, above the previous sequence floor of 7.
- A deterministic restart probe exposed an additional bug: restoring only
  transcript event IDs reused an earlier assistant event ID, and the journal
  silently ignored the new input. Explicit sessions now reserve IDs from all
  persisted observation and durable input/effect/receipt metadata before
  starting actors. An effect-only session also preserves its IDs without
  replaying the unresolved operation.

The final live evidence is retained locally in
`.scratch/runtime-subagent-final-bp2s7ma8/evidence.json`; that directory is
ignored and is not a portable CI artifact.

Verification stages on the working tree based on `1e97974`:

- Full suite before the final session/result-boundary corrections:
  `python -m pytest -q` — **233 passed in 383.28s**.
- After those corrections: CLI, transcript restore, journal, optional-host,
  and the SystemExit regression group — **72 passed in 39.89s**.
- Final CLI run after aligning piped-input timeout exit codes:
  `python -m pytest -q tests/test_cli.py` — **35 passed in 10.95s**.
- A local HTTP stub delayed its response by 1.2 seconds. A `--run-timeout 0.1`
  invocation returned `status=timeout`, exit 124, and no late assistant answer;
  process exit followed the request by 1.26 seconds. The timeout bounds waiting
  for completion, not blocking-HTTP thread teardown or total process lifetime.


Bounded claim: these runs validate the local CLI one-shot/resume path against
one gateway and model on one machine; they do not establish provider/model
portability or production task quality.

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
