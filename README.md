# Nervipulsa v0.4-dev

Nervipulsa is a local, event-driven coding-agent runtime. User messages, model
actions, and Python results travel as events. Model inference never waits for
Python execution, so the CLI can accept another message while code is running.

This checkout contains a v0.4 development implementation. The scripted runtime
exercises the asynchronous event contract without credentials. Live model calls use the OpenAI Chat Completions HTTP protocol directly. Nervipulsa
keeps its own tool execution, transcript, and retry state; no model SDK is required.

## Install

Python 3.11 or newer is required. From this directory:

    python -m pip install -e .

Then start the interactive CLI:

    nervipulsa --dir .

`python -m nervipulsa --dir .` runs the same entry point without installing.

The first run can open /config to set a provider, base URL, model, and API key.
Configuration is saved under the platform user configuration directory,
outside the workspace. The key is hidden while entering and is never written to
the event journal or passed to the Python worker.

The supported `provider` value is `openai`, which selects the OpenAI Chat
Completions protocol. This also supports OpenAI-compatible services: set `base_url`
to the service's `/v1` root, set `model` to its model identifier, and provide the
API key it expects. Nervipulsa sends the configured model name unchanged; model
names are not compiled into the code. Other provider protocols such as native
Anthropic or Gemini are not currently supported. The API key is required before
sending a request and is sent as a Bearer token.

Provider and Base URL changes take effect for the next model request without
restarting. Changing either clears the saved API key unless a replacement key
is supplied in the same update; configure the destination key or its
provider-specific environment variable before making a live request.

Switching providers without an explicit replacement Base URL resets a custom
endpoint to the provider's native route. Supply a new Base URL in the same
configuration update to keep using a compatible gateway.

Configuration precedence is command line, NERVIPULSA_* environment variables,
user configuration, then defaults. --dir fixes the workspace for the whole
session. The API key can also be supplied as NERVIPULSA_API_KEY.

The interactive prompt supports in-session history (Up/Down), completion with
Tab (including /config subcommands and fields), Enter to submit, Ctrl+J for
multiline input, and safe async output while you are typing.

## Commands

- /config opens full setup; /config show lists active settings with the API
  key redacted.
- /config set <provider|base-url|model|api-key> securely prompts for one value.
- Configuration changes are saved and apply to the next model request.
- /status shows model, workspace, activation state, Python queue, and current
  transcript size/remaining configured character budget.
- /cancel <execution_id> cancels queued or running Python code.
- /retry explicitly retries a paused model activation.
- /logs shows recent events, context growth and limit, per-input JSON-character
  additions, provider-reported usage, and Python output sizes.
- /output <execution_id> [stdout|stderr|both] retrieves complete output.
- /exit drains and closes the runtime.
- /uninstall asks for DELETE, then removes user config and the current
  workspace's .nervipulsa data; project files and other workspaces are kept.

`NERVIPULSA_CONTEXT_LIMIT` sets a serialized-transcript JSON-character limit,
not a tokenizer-token limit or an inferred model context window. Diagnostics
show this estimate separately from provider-reported usage; the estimate
excludes tool-schema and provider framing. If an activation still exceeds the
limit after compaction, Nervipulsa reports the estimate, configured limit,
newly added input size, recovered characters, and latest provider usage, then
pauses without sending that request.

When the projected context nears its budget, the LLM actor asks the same model
to summarize older completed interactions. That summary replaces the prefix
only after it succeeds; a failed summary keeps the original context. Events
that arrive during the summary stay in the inbox and are read on a later turn.
Current user requirements, pending tool calls, and the most recent completed
fragment stay verbatim. Compaction does not re-execute code. Live worker epoch
and unfinished executions are read from the runtime on each request, not from
the summary. If the overrun comes from a duplicate history append or a budget
below the fixed prompt, Nervipulsa reports that and does not compress it away.
Original events stay in the journal.

Python submissions show the code, then the result: status, duration, output or
the exception, and the context budget. Compression is mentioned only while it
is happening. Event ids and delivery fields stay in `/logs`. Assistant text is
shown only when the model actually wrote it. Each stream is limited to a 4 KiB
preview in model history; larger streams are retained as full files under
`.nervipulsa/outputs/` and remain available with `/output`.

Run the no-credential end-to-end trace with:

    python -m nervipulsa.demo --scripted

The trace edits a file in a temporary workspace, accepts an inserted user
message while Python is running, and feeds the terminal result into a later
activation. This demonstrates the scripted protocol only; a real model/provider
has not been validated by that command.

## Tests

    pytest -q

Coverage spans the event kernel, real worker, actor, CLI, and provider boundary:

- event kernel: capacity, routing, FIFO order, reserved result slots, and
  closing states (no worker process involved);
- real worker process: persistent variables, cwd restoration, separate streams
  and exceptions, bounded previews with complete per-stream artifacts, timeout,
  cancellation, process-tree kill, worker crash, and epoch reset;
- scripted actor: batch projection, one native receipt per tool call,
  activation-ledger recovery, context accounting and provider-usage journaling,
  atomic history compaction, and no replay of completed executions;
- CLI: bounded code previews, status/duration/exception summaries, full output
  retrieval, context logs, provider switching, and journal migration;
- provider boundary: OpenAI-compatible HTTP requests against a local stub,
  including tool calls, reasoning fields, malformed responses, error
  classification, secret redaction, and single-attempt behavior.

Timeout and cancellation tests start real processes and assert that the worker
and its children are gone; they do not mock the kill path. On Windows the launch
path uses a job object and is exercised by the same tests. No external provider
is contacted; local adapter tests cover protocol mapping, not live provider
availability or model quality.

## Runtime mechanics benchmarks

`benchmarks/bench_runtime.py` measures mechanism cost, not model quality:

    python benchmarks/bench_runtime.py --json benchmarks/result.json

Four probes, all credential-free:

| Probe | Measures |
| --- | --- |
| `transcript_measurement` | Cost of one context-size measurement as history grows, plus a legacy full-`json.dumps` baseline on the identical data |
| `tool_session` | Wall and CPU cost of a scripted tool-calling session with a growing transcript |
| `host_idle_wake` | Accepted→`python.started` latency while the host is idle |
| `idle_overhead` | CPU and scheduler wakeups of an idle runtime |

The transcript probe asserts `chars_match_full_dump`; the incremental size
accounting in `nervipulsa/llm.py` must stay bit-exact with a full
`json.dumps` of the same snapshot.

## Real-model checks

The three experiments in `Nervipulsa_Design_v0.4.md` section 13.3 need
credentials and spend tokens, so they stay out of `pytest`:

    NV_BASE_URL=... NV_API_KEY=... NV_MODEL=... \
    python examples/real_model_experiments.py 1 2 3

Each run keeps its workspace, event trace, and token usage under `.scratch/`.
Two runs against `deepseek-flash` passed all three experiments using the
previous raw HTTP adapter; they are historical evidence, not validation of the
current HTTP adapter. See `TEST_RESULTS.md` for per-run metrics and the
timeout corrected in experiment 3. Provider tests use a local HTTP stub; they
make no live provider requests. After automated tests, configure the actual
provider, base URL, model, and key in the Magpie CLI for a separate live user test.

## Files

| Path | Responsibility |
| --- | --- |
| `nervipulsa/events.py` | Event, Delivery, Bus, mailboxes and capacity rules |
| `nervipulsa/llm.py` | LLM actor, batches, transcript, activation ledger |
| `nervipulsa/providers.py` | ScriptedBackend and OpenAI-compatible HTTP adapter |
| `nervipulsa/python_host.py` | Request queue, process management, terminals |
| `nervipulsa/python_worker.py` | IPC frames, persistent namespace, output |
| `nervipulsa/framing.py` | Length-prefixed JSON control frames |
| `nervipulsa/process_tree.py` | Filtered environment, spawn and tree cleanup |
| `nervipulsa/journal.py` | SQLite observation records |
| `nervipulsa/config.py` | Precedence, persistence, redaction |
| `nervipulsa/cli.py` | Input, display, commands, assembly |
| `examples/real_model_experiments.py` | Runnable driver for the three live checks |
| `examples/REAL_MODEL.md` | What each live check means and how to read it |
| `benchmarks/bench_runtime.py` | Credential-free mechanism benchmarks |
| `docs/research_worker_handler_bridge.md` | v0.5 research: programmable worker event handlers |
| `TEST_RESULTS.md` | Recorded scripted and real-model results |

## Execution boundary

python_exec runs arbitrary code with the current user's operating-system
permissions. --dir selects the default working directory; it is not a sandbox.
Use this only with trusted local workspaces. The worker keeps Python variables
and imports between calls. Timeout and running cancellation terminate the
worker and reset that namespace. File and external side effects are not rolled
back.

The SQLite journal is an asynchronous observer. Event acceptance does not mean
the journal has been flushed to disk. A visible incomplete-journal marker is
shown if its bounded observation queue fills or writing fails.

## Development status

This package stays labeled v0.4-dev. Three claims are kept separate:

- **Design promise.** The contract in `Nervipulsa_Design_v0.4.md` is the
  intended behavior, including the invariants in section 13.1.
- **Automated tests pass.** `pytest -q` and
  `python -m nervipulsa.demo --scripted` exit non-zero on any assertion
  failure, so a green run means the scripted protocol and the real process
  tests passed on this machine.
- **Historical real-model runs (previous adapter).** Two `deepseek-flash` runs passed
  all three experiments using the previous raw HTTP adapter; metrics are in
  `TEST_RESULTS.md`. They do not verify the current adapter; no live provider
  was called after migration.

Known boundaries: `python_exec` runs with the current user's permissions and is
not a sandbox; there is no cross-process crash recovery, no rollback of file
writes, and no exactly-once guarantee against a provider that may have already
accepted a request whose response was lost.
