# Nervipulsa Runtime / Harness Architecture Research

**Status:** Ongoing, long-running research task  
**Started:** 2026-10-04 11:45 Asia/Shanghai (from the user's explicit long-task instruction)  
**Scope:** Event-native agent runtime, worker-side handlers, live model experience, activation cost, lifecycle and audit semantics.

## Research question

Can Nervipulsa evolve from a model/tool-call loop into a reliable event-native harness where persistent workers handle selected events locally, while user messages, execution results, model activations, backpressure, and audit history remain understandable and correct?

## Evidence gathered

### Transport and live CLI

- LiteLLM was removed after the OpenAI-compatible local gateway worked with direct Chat Completions requests but the LiteLLM route stalled or rejected custom-model parameters.
- The native standard-library HTTP adapter and local HTTP-stub suite passed: 47 tests.
- A live `codex/gpt-6-luna` CLI task read and edited `sample.py`, executed two Python calls (inspection, then edit/assert), printed `CHECK PASS`, and returned a concise summary. The execution and assistant events were journaled; all three activations committed.
- A CLI bug that blocked the asyncio loop while reading piped stdin was corrected by reading stdin in a worker thread. CLI regression coverage passes.

### User input during execution

- A controlled live run accepted a second user message while a Python execution was running. The first execution finished, the additional message was processed, and the final task succeeded.
- That follow-up also caused an unnecessary no-op Python call. The model should be evaluated on whether it can answer or defer without generating redundant tool work.
- An earlier less constrained run exposed a model-level failure: it changed the function name against instructions, claimed the check passed before the result arrived, then corrected itself after the failure observation. This is a useful failure case, not a runtime success.

### Worker handler bridge

- `on_finished(callback)` runs in the persistent Python worker. The callback receives one observation dict (`request_id`, `status`, `stdout`); it fires after normal worker completion, including the execution that registers it, and registrations persist for the worker epoch. A timeout, cancellation, or restart can stop the worker before callbacks fire and clears registrations. The callback's stringified result is returned as `agent.handler_fired` with handler ID, trigger, and epoch. There is currently no unregister operation.
- The first real-model test found two defects: the handler event was routed to the LLM mailbox but not projected into the transcript, and its resulting activation was a silent no-op. The model-visible tool description also omitted the API, causing the model to define a local function named `on_finished` instead of registering a runtime handler.
- The fix projects handler events as runtime observations and coalesces them with matching `python.finished` events for up to 25 ms by `trigger.request_id == reply_to`. Events removed from the mailbox during that window are all included and globally sorted by `seq`, so interleaved user messages cannot be overtaken. Native events remain separately journaled. The model-visible tool description documents callback signature/lifecycle. Full suite after prompt-contract, epoch cleanup, ordering, and projection coverage: 51 passed.
- Live proof: Python generated a random 24-character nonce, persisted it without printing, and returned it through a handler. The model repeated the exact value; activation 2 consumed both terminal event IDs. One execution used two committed activations.
- Live multiple-handler proof: the model registered two one-argument callbacks. They returned two unpredictable nonces in registration order; both exact values appeared in the final answer. The journal showed `python.finished` plus both `agent.handler_fired` events in one activation.
- Persistent-epoch proof: one registration survived across two separate Python executions and fired once after each. Two independent nonces were returned exactly and in order. The journal showed each completion/result pair together; total was three activations for two executions.
- Epoch cleanup is now tested with a real worker timeout/restart: stale handler IDs are cleared, an old-epoch `handler.fired` frame is rejected, and a fresh registration routes normally in the new epoch.
- Error proof: when the model defined a zero-argument callback, both executions still completed; each handler failure became a visible error observation. This also showed that the callback signature must be explicit and discoverable.
- A no-handler one-execution control also used two activations and one execution. It reported 7,842 total tokens; the nonce handler run reported 8,224. Prompts and generated code differed, so the token difference is directional only. The current completion hook adds observable behavior but does not reduce model calls for a one-shot Python cycle.
- Remaining API limits: only `on_finished` is available; registrations persist without unregister/replace, and no arbitrary named-event subscription or registration count limit has been demonstrated.


### Verified control/data path

1. CLI input emits `user.message` to the LLM mailbox. A model `python_exec` call emits `python.requested`; admission reserves a terminal slot keyed to the new event ID before returning the accepted receipt.
2. `PythonHost` dequeues requests serially and sends an `execute` frame to the worker. The worker emits `started`/`finished`; registered callbacks emit `handler.register` during the run and `handler.fired` after the execution completes.
3. The host publishes `python.finished` on the reserved-result lane. It validates handler frames against the current worker epoch and emits `agent.handler_fired` through the event bus. Handler observations currently use the ordinary lane.

4. `LLMActor` reads `expected_handler_count` from `python.finished`: zero skips waiting; positive counts wait until matching `agent.handler_fired` results arrive or the 25 ms deadline expires. Historical events without the field keep a bounded 25 ms grace. Events drained during the wait are sorted by global sequence, projected individually, and recorded in activation input IDs. Consuming `python.finished` releases its terminal reservation.
5. Epoch restart clears host handler IDs. Old epoch frames are rejected. Timeout/restart may clear worker namespace and registrations; the new epoch starts without old callbacks.



- Default LLM mailbox capacity is 64 ordinary events; terminal execution results use separately reserved slots (default 4). `agent.handler_fired` currently uses the ordinary lane.
- Separate normal-lane probe accepted 64 `agent.handler_fired` events and rejected the 65th with `capacity_exceeded`. The bus journal records the rejection, but `PythonHost._route_handler_frame()` ignores `Delivery`, so the model receives no per-result failure signal.
- A reservation probe filled the ordinary lane to 64, then emitted `python.finished` and 65 `agent.handler_fired` events with `Lane.RESERVED_RESULT` keyed to the same execution reservation. All 66 reserved-lane events were accepted. This could protect correlated handlers from unrelated ordinary traffic, but the reservation bounds keys, not event count: mailbox total reached 130. Any such design still needs a per-execution handler/output budget, and late results after `python.finished` consumption need a defined fallback.
- The LLM actor uses `expected_handler_count`: zero skips waiting; known positive counts wait until matching handler events arrive or the 25 ms deadline; missing legacy counts retain bounded grace. Missing results remain detectable in the projected terminal event, but ordinary-lane rejection is not yet guaranteed to reach the model.
- Design options to compare: publish an expected callback count on the native terminal record and end coalescing as soon as correlated results arrive; give handler outputs a bounded/dedicated delivery budget or aggregate them per execution; cap registrations per worker epoch; and surface incomplete/rejected handler results explicitly. Preserve the reserved `python.finished` result regardless of handler pressure.

- An independent architecture review compared ordinary per-event delivery, aggregation, and expected-count delivery. Its recommended direction is per-execution expected count plus bounded capacity, while retaining one journal event per handler. A count field alone is insufficient if ordinary delivery can still reject results; registration or result budget must be reserved before execution, with explicit incomplete status on timeout.
- The coalescer now adds every event it removes from the mailbox during its wait window and sorts the combined inputs by global `seq`. A deterministic regression test proves `python.finished(10)`, `user.message(11)`, and `agent.handler_fired(12)` enter the activation in that order.

The event-driven execution spine is real and usable. Handler delivery is semantically verified against actual model context, including multiple callbacks and repeated calls within one worker epoch; epoch restarts clear old host registrations. The expected-count barrier avoids no-handler delay and can report missing results, but ordinary-lane pressure can still reject them and registration is unbounded. This remains a strong prototype rather than a complete harness; cancellation/shutdown, mid-execution user messaging, controlled cost measurement, and a bounded delivery contract remain open.

## Research plan

1. **Map the control/data path.** Document worker IPC frame order, host epoch registry, mailbox delivery, transcript projection, activation consumption, and shutdown behavior. Include rejection and late-frame paths.
2. **Initial live baseline completed.** One no-handler execution and one nonce-proven handler execution both used 2 committed activations and 1 execution. Raw usage was 7,842 vs 8,224 total tokens, but prompts differed; design a paired benchmark before drawing cost conclusions.
3. **Lifecycle results so far.** Repeated delivery across two executions in one epoch, multiple callbacks in one execution, callback exceptions, and worker restart/epoch invalidation are verified. Still pending: cancellation and shutdown semantics.
4. **Evaluate wider event handlers.** Decide whether the architecture should expose named event subscriptions or retain only `on_finished`; specify unregister/replace semantics and backpressure before adding event emit.
5. **Exercise mid-execution conversations.** Reduce redundant no-op calls and prevent claims about results before corresponding terminal observations arrive; preserve all user messages and demonstrate the behavior in live model runs.
6. **Make an architecture decision.** Keep, reshape, or remove the bridge based on measured cost and semantic clarity. Then update the design note and acceptance tests.

## Open questions

- Does each handler trigger add a distinct LLM request, or can the runtime safely batch related terminal observations without introducing an unacceptable delay?
- Should a handled completion still trigger normal LLM processing, or can the handler result be consumed as the completion observation while retaining the actual `python.finished` event in the journal and tool transcript?
- What API should replace the one-shot `on_finished` hook if handlers are intended to process named event types over a worker epoch?
- How should users observe registrations, replacements, errors, and dropped/late handler frames in `/logs`?
- Can the runtime expose in-flight execution state strongly enough that a model receiving a mid-execution message does not claim an unobserved result or emit a redundant no-op call?

## Candidate bounded delivery design

The expected-count barrier is in place, but results still use the ordinary lane. A bounded follow-up should preserve the current behavior where a handler registered inside a Python execution can fire for that same execution, while making fan-out and queue occupancy finite.

- Keep a hard active-handler limit of 16 per worker epoch and add `off_finished(handle)` so a long-lived worker can release registrations. `on_finished` returns the handle. The worker freezes the handler ID snapshot when it sends `finished`; registrations made during user code are included in that execution, while registration/removal inside a callback affects later executions. A callback already in the frozen snapshot still fires once for the current result.
- Remove per-registration IPC frames from the host path. The `finished` frame carries the bounded snapshot IDs and count; the host validates each `handler.fired` against that request's snapshot. This avoids maintaining a second registry that can drift. It also avoids synchronous Host ACK: the worker's main thread executes user code and does not read its control socket until execution returns, so a synchronous registration/result ACK would deadlock unless the worker gains a separate control-reader thread.
- At `python.requested` admission, reserve the terminal slot and up to 16 independent handler-result slots atomically in the LLM mailbox. The dedicated handler lane spends one reserved slot per accepted result. Slot accounting remains live while a result is queued or held in the actor's coalescing/activation batch; `mark_consumed(agent.handler_fired)` frees that result slot.
- When `python.finished` is accepted, shrink the unused part of the reservation to its `expected_handler_count`. When the actor consumes `python.finished`, close unfilled slots; keep already accepted result slots counted until their own consumption. A late result then attempts ordinary-lane fallback and is checked for rejection. The terminal reservation remains exclusive to `python.finished`.
- Bound total handler slots independently of ordinary events (initially `result_limit * 16`, or a separately configured ceiling). Admission rejects before worker execution if slots cannot be reserved. Timeout, cancellation, restart, and shutdown release unused slots; the projected expected count lets the model identify an incomplete result set if the 25 ms delivery window closes first.

This is a candidate for a small implementation slice, not yet a proven contract. Its key acceptance tests are ordinary-lane saturation with every reserved handler result still admitted; count/slot accounting through mailbox dequeue and activation consumption; same-execution registration; callback-time register/unregister; late fallback; and zeroed accounting after cancel/restart/shutdown.

### Lifecycle blocker found during protocol review

The Worker originally sent its `finished` frame before invoking callbacks. `PythonHost._execute_blocking()` returned as soon as it read that frame, so the host marked the execution idle and could schedule the next request while callbacks were still running. The original execution timeout did not cover callback time. A callback could therefore leave an apparently idle runtime with a blocked worker; a later request could hit the separate 10-second worker-start deadline and force a restart. This also meant callback work was absent from the terminal execution duration.

**Fixed:** the worker freezes its handler snapshot after user code, buffers each callback result, sends those result frames before the final `finished` frame, and calculates duration after callbacks and frame transmission. The Host buffers fired frames while the request is active, waits for that final frame, then publishes `python.finished` followed by the corresponding handler events. If timeout/cancel kills the worker before that frame, the partial handler buffer is discarded. A real subprocess regression verifies callback timeout and queued-request ordering. Full suite: **53 passed**.

This keeps public event ordering while making the existing execution timeout cover callbacks. No synchronous Host ACK is needed: the worker's main thread executes user code and does not read its control socket until execution returns, so a synchronous registration/result ACK would deadlock unless the worker gains a separate control-reader thread.

## Current audit: bounded handler delivery closure (2026-10-04)

This section records the current source review and regression run. It supersedes the older “Current next action” description where it says the bounded lane is still only a candidate.

### Verified by source inspection

- `LLMActor._execute_tool()` submits `python.requested` with a terminal reservation and 16 handler-result slots. `Bus.call()` checks receiver admission before allocating a sequence, then reserves downstream capacity before offering the request. If a reservation or offer fails, it rolls back the sequence and any acquired reservations; rejected provisional event IDs are not published.
- The LLM mailbox has a global handler-slot ceiling of `handler_result_limit * result_limit` and a per-execution ceiling of `MAX_HANDLER_RESULTS_PER_EXECUTION` (16). A fired result spends one slot; its slot remains counted while queued or leased, and `mark_consumed()` releases it. An accepted terminal shrinks unused slots to the worker-reported expected count. Consuming the terminal closes unfilled capacity while accepted results remain accounted for until consumed.
- `HANDLER_RESULT` requires an active reservation key and a matching `reply_to`; handler events on the ordinary lane also require a reserved per-execution slot and matching `reply_to`. Thus the host's ordinary fallback remains within the same budget. If both routes reject, the host increments the undelivered counter and emits a UI error.
- Terminal reservations are checked against the lane key. This audit found that the mailbox previously settled handler slots using `event.reply_to` without checking that it matched the reserved terminal key. A wrong-key terminal could therefore settle another execution's handler budget. `Mailbox.offer()` now rejects that mismatch before settling slots; a regression test preserves the unrelated reservation.
- Worker snapshot IDs and epoch are checked before routing; active execution frames are further checked against that request's snapshot, epoch, trigger request ID, and uniqueness before delivery. Timeout/cancel paths report known handler IDs as missing and discard buffered partial fired frames. A worker crash before a valid snapshot reports handler status as unknown. Worker restart clears host registrations and snapshots.
- Close clears queued and leased mailbox state, all terminal and handler reservations, and handler-event accounting. Actor coalescing projects every event it removes in global sequence order. With a known expected count it waits for matching handler arrivals up to the count; legacy terminals without a count retain a bounded 25 ms grace. If the terminal lists missing IDs, the projected terminal explicitly reports incomplete delivery even when coalescing proceeds without all results.

### Test evidence from this session

- The first full `python -m pytest` run completed with 58 passed and 1 failed. The failing coalescer ordering fixture attempted a direct ordinary-lane handler offer without reserving a handler slot. The offer was correctly rejected (`capacity_exceeded`); production admission reserves slots before execution. The fixture now reserves a slot before offering the handler event.
- A mailbox regression exercises a terminal whose reply key conflicts with its reserved lane key and checks that it cannot reduce another execution's handler budget. The source fix rejects the mismatch before settling slots.
- After these changes, full `python -m pytest` completed successfully: **59 passed in 71.59 seconds**. No local gateway CLI E2E was run during this audit; previously recorded live CLI evidence above belongs to earlier runs and is not evidence from this session.

### Remaining assumptions and limits

- Source and deterministic tests establish slot bookkeeping, but this session has not run a stress test across many simultaneous admissions to measure contention behavior. Bus/mailbox operations are synchronous on the owning asyncio loop, so their reservation mutation is serialized under the current architecture.
- The coalescer counts arrived handler events by trigger request ID rather than checking distinct handler IDs itself. The host currently filters duplicate handler IDs before delivery, and the terminal reports `missing_handler_ids`; a future alternate producer must preserve those host-side uniqueness checks or harden coalescing to compare distinct IDs.
- If handler delivery is rejected twice, the UI receives an error and terminal metadata can report missing results; this does not guarantee the LLM will receive that UI error. The terminal remains independently reserved and observable.

### Architecture decision

Keep per-execution expected counts with independently bounded handler slots, terminal reservation, and one journal/event record per delivered handler. Preserve ordinary-lane fallback only when it spends the same execution's reserved slot. Reject mismatched correlation keys before any accounting mutation. Keep timeout/cancel/restart cleanup and explicit incomplete/unknown terminal status. Do not replace this with an unbounded ordinary fan-out or treat the handler lane's bounded total as a global concurrency guarantee beyond the configured mailbox limits.


## Remaining research

1. Stress-test simultaneous admission at the global slot ceiling and verify release after close, timeout, cancellation, and restart under load.
2. Decide whether coalescing should independently deduplicate handler IDs even though the host currently enforces uniqueness.
3. Run a paired live benchmark if cost conclusions are needed; no local gateway CLI E2E was part of the current audit.

