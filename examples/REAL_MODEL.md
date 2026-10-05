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
