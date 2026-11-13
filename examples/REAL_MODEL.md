# Real-model checks

These three checks are not run by `python -m nervipulsa.demo --scripted` or by
`pytest`. That command only proves the scripted contract.

## Run them automatically

`real_model_experiments.py` drives all three, including the timed interjections,
and writes a JSON and text report per experiment:

    NV_BASE_URL=http://127.0.0.1:8317/v1 \
    NV_API_KEY=... NV_MODEL=deepseek-flash \
    python examples/real_model_experiments.py 1 2 3

By default, reports and workspaces land in `.scratch/real-model-<run-id>/`;
every invocation gets a fresh directory, so reruns do not delete previous
artifacts. `NV_REPORT_DIR` overrides only the report location; reusing that
directory replaces same-named reports. Credentials are read from the
environment only. Passing needs a model that accepts an execution, keeps
working, and reads a later message before acting on it.

Two recorded runs of all three experiments passed; see `TEST_RESULTS.md` for
the numbers, observed timings, and timeout the model had to correct.

## Repeatable CLI subagent acceptance

    NV_PROVIDER=magpie NV_BASE_URL=... NV_API_KEY=... NV_MODEL=... \
    python examples/subagent_acceptance.py

This separate driver creates a fresh workspace and empty user-configuration
directory. It uses only the supplied `NV_*` provider configuration;
`NV_PROVIDER` defaults to `openai`. Credentials are passed in the environment,
and captured text is redacted before being written to reports.

The required checks are:

- Fix a copy of `bugfix/calc.py`, preserving the original test file. Run pristine
  tests against the resulting module in a separate verifier directory, with
  model-added test/config files excluded.
- Start a new CLI process with `--resume`; recall a unique first-prompt marker
  from the restored transcript without tools, retaining all prior journal rows.
- Reuse the existing session without `--resume`; require `session_exists`,
  coherent process/JSON exit status, and retained history.

Each run keeps `.scratch/subagent-acceptance-<uuid>/report.json`, `workspace/`
with the SQLite journal, `verifier/`, and `artifacts/` with commands, stdout JSON,
stderr, fixture hashes, and journal snapshots. A failed or skipped required
check makes the driver exit nonzero. `runtime_completed` and
`verified_task_success` are reported separately; a successful model response
alone cannot pass the acceptance run. Failed runs are retained alongside later runs.

Options are `--max-activations` (12), `--run-timeout` (180 seconds per CLI
invocation), and `--pytest-timeout` (60 seconds). The CLI deadline bounds
completion waiting, not total process lifetime; the driver waits for orderly
shutdown. This check does not establish hard deadlines, effect replay,
long-running stability, or general task quality. Credential-free session
contention, cancellation, process-death release, and result-failure regressions
live in `tests/test_session_lock.py` and `tests/test_cli.py`.

## Or do them by hand

Start a session with a configured OpenAI-compatible endpoint:

    nervipulsa --dir examples/bugfix

## 1. Talk during a running execution

Ask for Python that waits about 20 seconds and then prints a marker. After
`python.started`, send: "Reply that you received this. Leave the original
execution running; do not start it again."

Pass only if the reply arrives before the marker, the marker is produced by
the original execution id exactly once, and a later activation reports that
result without a second copy of the same code.

## 2. A later message changes the next action

Ask the model to wait and print 7 in one execution, with the multiplication left
for a later step. While that code is running, send a message that the multiplier
is now 3. The later step should print 21. `/logs` must show the second user
message among the inputs of the activation that performs the multiplication,
not inside the still-running first execution.

## 3. Fix the sample bug

`examples/bugfix` has `add()` returning a difference and a test that expects
a sum. Ask the model to fix it and run the test. While the test runs, add a
compatibility note such as "also keep the two-argument signature." Check the
result with a fresh process:

    python -m pytest -q examples/bugfix/test_calc.py

Keep the diff, activation count, token usage, and event trace. A scripted
pass does not count as this experiment.
