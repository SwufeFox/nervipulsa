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
3. The host validates handler frames against the current worker epoch and the active request snapshot, then routes handler observations through their bounded reservation. It emits `python.finished` after those routing attempts so its missing IDs include rejected results. The LLM actor coalesces correlated observations and projects them in global sequence order; native events remain separately journaled.

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

**Fixed:** the worker freezes its handler snapshot after user code, buffers each callback result, sends those result frames before the final `finished` frame, and calculates duration after callbacks and frame transmission. The Host buffers fired frames while the request is active and waits for that final frame. As of the 2026-10-06 delivery-rejection fix, it routes each buffered handler result before publishing `python.finished`, so the terminal can report host-side delivery failures; the actor consumes the resulting events in global sequence order. If timeout/cancel kills the worker before that frame, the partial handler buffer is discarded. A real subprocess regression verifies callback timeout and queued-request ordering.

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



## Live CLI 复验（2026-10-05）

### 目的与执行范围

按用户既有授权，本次启动使用进程级 `NERVIPULSA_BASE_URL`、`NERVIPULSA_MODEL`、`NERVIPULSA_API_KEY` 覆盖；值从已配置设置载入，没有修改或保存持久配置，密钥未写入日志。CLI 工作目录指定为 `.scratch/live_cli_e2e`。任务要求创建含 `clamp(value, low, high)` 的简单 Python 模块，实际执行边界/范围 assertion，并打印 `CLAMP ASSERTIONS PASS`；同时要求尝试注册 `on_finished`，将执行状态和 marker 观测作为 handler observation 返回。

### 结果与证据

- CLI 进程退出码为 0；启动横幅显示 provider `openai`、模型 `deepseek-flash` 和指定临时工作区。CLI 捕获的 stdout 只有启动横幅与交互提示，stderr 为空。
- Journal：1 个 `user.message`；1 个 activation，状态 `paused`；provider attempt 状态 `unavailable`，错误为 `urlopen` 的 Windows `[WinError 10061]`（目标计算机主动拒绝连接）。这是本次失败证据；本轮没有 provider 回复。
- 执行数为 0、tool call 数为 0；没有生成 `math_ops.py`，断言没有运行，marker 没有出现。`on_finished` 注册及额外 observation 因模型调用未成功而未能验证。
- 因此本次只能证明 CLI 接受输入并将 gateway 连接失败记录到 journal；**当前版本的真实 CLI 任务未通过，不能声称当前代码 live 验证通过**。没有测得执行/handler activation、无关或 no-op 工具调用，也没有最终模型答复。
- 本轮 `.scratch/live_cli_e2e` 临时工作区（含 journal）已清理。

### 架构含义与待办

连接失败发生在任何工具执行之前，不提供 Python worker 或 handler 协议的正反证据。既有历史 CLI 记录仍是较早运行的证据，不能替代当前版本复验。待 gateway 可连接后，按同一授权重跑实际修改与 assertion，并检查 stdout/stderr、事件和 activation 数、工具调用相关性、handler observation 是否进入最终模型上下文及最终答复。

### 3425 gateway 重试（2026-10-05）

- **第一次重试的配置偏差：** 从用户配置加载的 base URL 仍为 `http://127.0.0.1:8317/v1`。CLI 进程以 provider `openai`、模型 `deepseek-flash` 启动，但请求仍遇到 Windows `WinError 10061`；该次 journal 新建了一个 paused activation，没有 Python execution。此结果不算对用户指出的 3425 监听端口的有效验证。API key 仅由子进程环境提供，未输出或落盘。
- **3425 有效重试：** 使用同一配置中的 provider/model/key，仅对该次进程设置 `NERVIPULSA_BASE_URL=http://127.0.0.1:3425/v1`。临时工作区为 `.scratch/live_cli_retry`。任务要求写入 `clamp_module.py`、运行三个边界 assertion 并打印 `CLAMP ASSERTIONS PASS`，同时在单轮内尝试注册一参数 `on_finished`，让模型最后确认执行和回调状态。
- **真实 CLI 与 journal 结果：** 3425 返回了可用模型响应。Journal 依序记录 `user.message`、`python.requested`、`python.started`、`python.finished`、`agent.handler_fired`、`assistant.message`。唯一 execution `evt_00000002` 状态 `succeeded`，worker epoch 1，耗时 332 ms，stdout 为 `CLAMP ASSERTIONS PASS`，stderr 为空；三项 assertion 均通过。模型代码正确调用运行时提供的 `on_finished`，没有定义本地替代函数。
- **Handler 验证：** terminal 报告 `expected_handler_count=1`、`handler_result_status=complete`、无 missing IDs。`agent.handler_fired` 的 observation 为 `status=succeeded` 和 `stdout_contains_marker=true`。第二个 committed activation 的 input IDs 同时包含 `python.finished` 与 `agent.handler_fired`；模型最终答复准确复述断言、marker、执行状态及 handler observation。因此此单轮任务适合并已完成 on_finished handler 验证，没有污染主任务证据。
- **Activation 与使用量：** 两个 committed activations，一次 Python execution，一次 tool call；第一 activation provider request 16.239 s，usage 4,493 tokens；第二 activation 2.058 s，usage 5,096 tokens。合计 9,589 tokens。此单次任务不构成成本基准。CLI 子进程以捕获输出运行时，Windows GBK 解码导致 stdout 捕获异常；journal 完整记录了最终 assistant.message，且 CLI 进程退出码为 0。验证结论以 journal 为准。
- **清理与证据边界：** `.scratch/live_cli_retry` 临时工作区及其中 journal 已在记录后清理。之前记录的 8317 连接拒绝与 2026-10-05 初次失败均保留；本次 3425 结果单独记录。该结果证明当前代码在此模型/gateway 的一轮安全任务中完成了理解、执行、handler 投递和模型观察，不证明并发压力、成本优势或生产可靠性。

## 架构含义与待办（更新）

3425 实验补齐了此前缺失的当前版本 live CLI 证据：模型能够理解 `on_finished` 的一参数 API，handler 能在同一执行完成后被触发，且 terminal 与 handler 两条事件都进入下一次 activation 并被最终回答正确使用。保留窄 handler bridge 的原型判断获得一项新的正向现场证据，但尚不能据此推断一般任务质量或可靠性。后续仍需容量上限压力、取消/关闭组合生命周期验证，以及有/无 handler 的配对成本实验。

## 有界 handler 预算与生命周期复验（2026-10-05）

### 本轮问题与代码审阅

继续核查全局 handler-result ceiling、每执行 reservation、终态收缩、结果消费释放及 close/cancel/timeout/restart 后的 late-frame 语义。保护并保留了上方完整的 2026-10-05 live CLI 3425 记录；本轮没有重写该记录，也没有进行新的 live CLI/model run。

- `Bus.call()` 在接受 `python.requested` 前同步检查目标 mailbox admission，再按 terminal、handler slots、请求 offer 的顺序取得容量；中途失败会 rollback 已取得的 reservation，并回滚 provisional sequence。worker 不会收到被拒绝的请求。
- Mailbox 的全局 handler ceiling 为 `handler_result_limit * result_limit`，单执行预留最多 16 个。handler 结果入队时把一个 available slot 转为 outstanding；dequeue/lease 不释放，`mark_consumed(agent.handler_fired)` 才释放 outstanding。收到 `python.finished` 会将 available 部分收缩到实际 expected count；消费 terminal 关闭未填部分，而已接收、未消费的 handler 结果继续计数。`close()` 清空队列、lease、terminal reservation 和 handler accounting。
- `PythonHost` 在 queued cancel、running cancel、timeout、worker restart 前后都通过唯一 terminal 结果结束该执行；worker epoch 清空 handler registry/snapshot，旧 epoch 或不属于当前 snapshot 的 frame 被计为 late 并丢弃。terminal 被 LLM 消费后，迟到 handler 使用已关闭的 reservation 会被拒绝。timeout 时已知 snapshot 会标注缺失 handler；snapshot 尚未知时状态为 unknown。close 最终关闭 bus/mailbox 并清账。
- 执行路径判断：admission 与所有 mailbox 预算写入均为同步函数，运行在拥有 Bus 的 asyncio loop 上；同一 loop 上的协程只能在 `await` 处交接，因此 reservation 更新之间没有真正的多线程竞争。PythonHost 确实有 worker 子进程、读取 IPC 的线程以及 `asyncio.to_thread` 阻塞操作，但这些线程/进程把帧交回 host 后，由 loop 上的 host 协程执行路由；它们不直接并发修改 mailbox。故本轮单测验证的是确定性容量语义，不是 concurrent load 或多线程竞争证明。

### 变更与确定性测试

本轮未发现需要修复的生产代码缺陷；新增回归覆盖到 `tests/test_events.py` 与 `tests/test_python_host.py`：

- 使用两个同时占用预算的执行 admission 填满可配置全局 8 个 handler slots；第三个执行被拒绝，sequence、terminal reservation 和 handler accounting 均不变。一个零 handler terminal 入队并被消费后，释放出的容量允许重试成功；close 后两类 reservation 均为零。
- 真实 worker timeout 后预算归零，旧执行的 late handler frame 被拒绝；排队 cancel 及运行中 cancel 导致的 queued request 清账得到验证。
- worker callback 超时保留 terminal 的 `incomplete`/missing ID 证据；消费 terminal 后未填 handler slot 归零，late callback result 被拒绝。worker epoch restart 后新执行完成且预算归零。
- shutdown/close 期间有预留的请求被清空，mailbox budget 归零，close 后 frame 被 runtime closing 拒绝。

验证：`python -m pytest tests/test_events.py tests/test_python_host.py -q`：**20 passed**；`python -m pytest -q`：**61 passed in 40.07s**；`git diff --check` 通过。未执行压力测试，也未测跨线程并发 admission。

### 结论与后续

当前证据支持：单 asyncio loop 上全局及每执行预算的 admission、消费释放、终态收缩和关闭清理具有确定性；测试覆盖了 timeout/cancel/restart/close 的终态观察与 late result 路径。它不支持“真实 concurrent load 已证明”，因为没有多线程写 mailbox，且本轮未进行负载实验。若未来允许其他线程直接调用 Bus，需先定义 thread-affinity 或加锁协议，再单独做并发压力验证。架构余项仍包括并发负载测试和 coalescer 对 handler ID 的独立去重。

## Paired live CLI test: presence of `on_finished` (2026-10-05)

**Question.** Does registering exactly one Python `on_finished` handler change observed completion latency for the same small numeric task? This is a four-run, interleaved live CLI comparison, not a general performance claim.

**Gateway and controls.** Before running, `127.0.0.1:3425` accepted a TCP connection and an authenticated `/v1/models` request returned HTTP 200. Each process received environment overrides `provider=openai`, `base_url=http://127.0.0.1:3425/v1`, `model=codex/gpt-6-luna`; the already configured API credential was passed only in process memory/environment. No credential value was printed or written. Each run used a unique `.scratch` workspace and config directory, maximum four activations, 40 second Python execution timeout.

**Task.** Both conditions used the same core request: use Python to define `normalize_score(value, maximum)` that clamps to `[0, maximum]`; assert `(-2,10)->0`, `(4,10)->4`, `(12,10)->10`; print exactly `NORMALIZE PASS` only after assertions; create no files. Treatment added one instruction: register exactly one `on_finished` handler before execution, returning execution status and whether stdout contains the marker. Baseline explicitly registered no handler.

**Order:** baseline / treatment / treatment / baseline. CLI prompt and environment settings were otherwise identical.

### Per-run observations

| Run | Condition | CLI exit | Wall time (s) | Activations | Provider requests | Prompt tokens | Completion tokens | Execution | `python.finished` stdout marker | Handler fired/result |
|---:|---|---:|---:|---:|---:|---:|---:|---|---|---|
| 1 | baseline | 0 | 12.099 | 2 | 2 | 8647 | 126 | succeeded | true | none |
| 2 | treatment | 0 | 16.031 | 2 | 2 | 8817 | 187 | succeeded | true | succeeded; stdout contains NORMALIZE PASS: True |
| 3 | treatment | 0 | 13.938 | 2 | 2 | 8817 | 181 | succeeded | true | status=succeeded; contains_NORMALIZE_PASS=True |
| 4 | baseline | 0 | 15.992 | 2 | 2 | 8612 | 91 | succeeded | true | none |

The CLI transcript itself contained the literal marker in baseline runs; in treatment, the marker was present in the journaled Python execution stdout and in each handler result, though it was not necessarily repeated in the final assistant transcript. The execution stdout is the marker correctness measure.

### Summary and paired differences

- Mean wall time: baseline **14.046s**, treatment **14.985s**; unpaired difference treatment − baseline **+0.939s**.
- Adjacent pairs (baseline then treatment): run 1→2 **+3.932s**; run 4 vs run 3 (baseline run 4, treatment run 3) **-2.054s**.
- Mean paired difference **+0.939s**. The pair signs disagree.
- Mean activation token totals: baseline **8738.0**, treatment **9001.0** (provider usage summed over both activations per CLI session).
- All four scheduled runs exited with CLI status 0; all four Python executions succeeded and journaled `NORMALIZE PASS`; baseline fired zero handlers, treatment fired exactly one and returned both execution status and marker membership.

### Failure retained and limitations

- An earlier treatment pilot in the same task attempt was also run and is retained as a failure: its Python execution succeeded, journaled stdout `NORMALIZE PASS`, and fired exactly one handler returning `status=succeeded; marker=True`; the following activation ended with journal error kind `shutdown`, and the CLI process did not produce a completed captured transcript/result before the attempt was abandoned. Its wall time is unavailable, so it is not included in the four scheduled timing rows or means. It is recorded here rather than silently discarded.
- Tiny sample (two per condition), one model/gateway, sequential calls, and variable provider latency limit inference. The wall-time differences are inconsistent in sign and do not establish a handler performance cost or benefit. Prompt wording necessarily differs by the treatment instruction. Token totals also include assistant follow-up activations and can vary by model response.
- Evidence source: per-run Nervipulsa SQLite journals (`activations`, `executions`, and `events`) plus measured process wall time; temporary journal/output files were removed after extraction. No unit tests were run for this live experiment.

## 2026-10-06 — Missing handler status in provider activation input

**Question.** When handler delivery is rejected or never completes, does the terminal `incomplete` state and its `missing_handler_ids` reach the actual model request, or are they only recorded by the journal/UI?

**Path reviewed.** `PythonHost` derives missing IDs from expected registered handler IDs versus delivered frames and includes `handler_result_status` / `missing_handler_ids` in the terminal `python.finished` payload. The bus validates and routes that terminal event to the LLM mailbox. `LLMActor._coalesce_handler_results()` waits up to its bounded window for the declared handler count, then returns the terminal and any received handler events sorted by sequence. In `_activate()`, each event is projected before the provider request; `Transcript.project()` retains the event payload in a compact JSON runtime-event envelope, `snapshot()` includes that message, and the backend receives the resulting `ModelRequest.messages`.

**Evidence.** Existing real-worker test `test_handler_timeout_discards_fired_frames_from_real_worker` asserts a timed-out handler produces terminal `handler_result_status == "incomplete"` and a non-empty `missing_handler_ids`. New deterministic regression `test_incomplete_handler_status_reaches_provider_activation` drives a valid terminal event through the live Runtime bus and actor/coalescer/projection path, with a `ScriptedBackend` mock recording the request. It parses the actual `backend.requests[0].messages` and asserts the `python.finished` envelope contains `handler_result_status: incomplete` and the recognizable ID `handler-missing-17`. Command: `python -m pytest tests/test_llm.py::test_incomplete_handler_status_reaches_provider_activation -q` — **1 passed**. No production fix was needed: status and IDs survive to provider input.

**Evidence boundary.** This proves model-context visibility for a deterministic terminal payload. It does not prove that a model notices, correctly interprets, or acts on the incomplete status. The provider regression injects the Host-shaped terminal event; Host derivation is separately covered by the real-worker timeout test. End-to-end injection of an actual dual-lane delivery rejection into the same provider-capturing runtime remains an open integration test. Rejection in one or both lanes must not be inferred as model-visible solely from a journal/UI delivery record.

**Open questions.** Add a single real Host-to-actor integration scenario where dual handler delivery is rejected and the Host-generated terminal is captured by the mock provider; verify both the computed missing ID and provider payload. Separately test model behavior with a scripted/controlled responder that must acknowledge the missing ID, while treating behavioral success as a distinct criterion from payload inclusion.



## 首个 worker 自定义库：`nervipulsa.hashline_edit`

持久 worker 暴露全局 `read_file(path)` 与 `edit_file(path, snapshot, put)`，也支持从 `nervipulsa.hashline_edit` 导入。读取产生 whole-file 4 位小写十六进制 xxHash32 snapshot tag，并以 `N:text` 格式渲染行。写入采用 OMP PUT 整文件替换语法：`PUT <4-hex-tag>\n<complete replacement text>`；调用者传入本次读取的 snapshot 和完整 PUT 字符串。标签与当前文件不符时拒绝写入，路径需位于 worker workspace 中，写入使用同目录临时文件和原子替换。

上游 OMP 项目在其 `Cargo.toml` 中声明 MIT 许可；算法来源和许可可以据此引用。本仓库的实现是独立 Python 实现，没有复制 Rust 源码。实现范围限于 UTF-8 文本；该文件编辑 API 不构成 Python 沙箱。



## 2026-10-05：静态环境发现安全加固

独立代码审查发现上一版 `python_environment` 会对模型提供的模块调用 `find_spec` 与 `import_module`。这意味着只读发现可触发目标模块及其父包顶层代码；此前关于只读安全性的表述不成立。保留本文件既有运行记录及历史 CLI 实验数据，但将其限定为旧实现上的行为观察，不能证明发现过程安全。

实现已改为静态发现：读取有限项目线索；从 workspace/解释器搜索目录定位候选源码路径；只用 AST 识别顶层声明和字面量 `__version__`；distribution 版本仅读取元数据。查询不调用 `find_spec`、不导入目标模块、不执行源码，也不修改 `sys.path`。严格限制模块/ API 名称、数量、源文件大小、单条线索和序列化结果预算。

## 2026-10-05：环境发现 workspace 外链边界与当前版本复验

### 发现与修复

静态 AST 检查不会执行目标模块。审查还发现 workspace 文档原文若直接投影进模型上下文，会扩大提示注入面；现环境发现事件只列出常见依赖清单文件名，不读 README、Markdown 或 setup/依赖文件正文。源码候选路径在静态读取前做 resolve containment 检查；搜索根有限制，事件中只用 workspace 相对路径或解释器搜索根标签。越界候选被忽略。

### 验证

- 修复前相关回归：`python -m pytest tests/test_python_environment.py tests/test_events.py tests/test_llm.py -q`：32 passed。
- 修复后 `python -m pytest tests/test_python_environment.py -q`：当前局部验证见本轮末尾；完整测试最新为 76 passed、1 skipped。skip 是 workspace 外链 containment 集成用例；当前 Git Bash 环境调用 `cmd.exe /c mklink /J` 失败，pytest 输出为 `'chcp' is not recognized`，所以真实 OS junction 路径未验证，不把 skip 计作通过。
- 当前版本 CLI live run 使用 `http://127.0.0.1:3425/v1` 和已配置 `codex/gpt-6-luna`。在隔离 workspace 中先调用环境发现检查本地 `widgetkit.py`（静态版本 0.3.1、API `scale`），之后显式导入并完成两项断言。CLI 退出码 0；journal 有 7 个事件，环境发现 payload 明确含 `read_only=true`、`install_supported=false`、候选路径及 `scale=true`；唯一 execution 在 epoch 1 succeeded，stdout `WIDGETKIT SMOKE PASS`，stderr 为空。最终回答复述发现和断言结果。三个 activation committed，两次工具调用（发现、执行），provider usage 共 14,277 tokens；该单样本不构成成本结论。

### 证据边界及后续

该真实模型运行确认当前版本的发现结果能进入后续 activation 并引导一次显式 smoke test；不保证其他模型遵循相同流程，也不证明静态 API 判断等于运行时 API 可用。符号链接 containment 的实际 OS 集成路径尚未在此受限环境中执行成功。旧版导入式实现的 CLI 记录仍是历史证据，不用于证明静态版本安全。临时 CLI workspace、journal、输出和 `__pycache__` 已清理。

## 2026-10-05 — Coalescer handler ID 去重

`LLMActor._coalesce_handler_results()` 原先按事件数量满足 `expected_handler_count`。若同一 `handler_id` 被重复投递，重复项可能提前结束等待，另一个 handler 的结果则未进入当前 activation。现在按每个 execution 的唯一 handler ID 计数；无 ID 的兼容事件以 event ID 作为各自独立结果。所有观察到的事件仍按全局序号投影并保留，避免为去重丢弃审计事件。

新增回归安排同一 handler ID 重复到达，再延迟投递第二个不同 ID，并在中间插入 user message。断言 coalescer 继续等待到第二个唯一结果，且 activation 批次仍保留全部事件并排序。验证：`python -m pytest tests/test_llm.py::test_counted_handler_coalescing_is_bounded_and_uses_initial_batch -q`：**1 passed**；`git diff --check` 通过。

## 2026-10-05 — 双 lane 拒收进入 provider 的端到端验证

端到端回归发现 Host 原先先发布 `python.finished`，再路由 `agent.handler_fired`。因此 handler 两条路由都被拒收时，终态已经按 worker 原始 fired frame 记录为 `complete`，遗漏了 Host 侧的投递失败。现在 Host 先尝试 handler 主 lane 与有界 ordinary fallback，按实际接受结果计算 `missing_handler_ids`，再发布 terminal。handler 事件与 terminal 在同一 Host 事件循环步骤内入队，LLM actor 后续按全局序号统一处理。相应的 Host 生命周期测试已调整为验证 handler observation 在 terminal 前入队。

`test_host_delivery_rejection_status_reaches_provider_and_is_acknowledged` 使用真实 Python worker 注册并触发 handler，通过可控 emitter 让两条投递路径均返回容量拒绝；断言 Host terminal 为 `incomplete` 且带 missing ID、实际 provider 请求包含该 ID，scripted responder 在最终 transcript 中明确确认该 ID。`test_incomplete_handler_status_reaches_provider_activation` 继续验证 Host 形状 terminal 的状态投影。目标命令 `python -m pytest tests/test_llm.py::test_host_delivery_rejection_status_reaches_provider_and_is_acknowledged tests/test_llm.py::test_incomplete_handler_status_reaches_provider_activation tests/test_python_host.py::test_queued_request_waits_for_handler_frames -q`：**3 passed**。

尚未在本机通过的项目：workspace 外链 containment 集成测试由于 junction 命令失败而 skipped；需在支持 junction/symlink 的 runner 上执行。

## 2026-10-05 — 环境发现收尾与元数据索引复核

修复 `test_llm.py` 中手动加入异常路径回归时造成的函数边界错误，恢复了原有的批量输入、环境发现 smoke test 和 provider usage 测试为独立用例。环境发现仍保持静态：不会导入目标库；只回传有限文件路径、distribution metadata 和源码 AST 声明。

全局 `importlib.metadata.packages_distributions()` 在这台 Windows 环境的单次测量耗时 8.483 秒，会让同步工具 dispatch 阻塞 event loop。实现改为仅扫描最多 32 个已解析解释器搜索根、每根最多 512 个发行版的 metadata 与 `top_level.txt`，构建 import-name 到 distribution 的映射；相同搜索根在进程内缓存。定向测量扫描 259 个 distributions、读取 153 个 `top_level.txt` 用时 0.158 秒。它仍是静态 metadata 读取，不执行包代码。

验证：核心环境发现筛选 `13 passed, 1 skipped`；完整 `python -m pytest` 为 **76 passed, 1 skipped in 49.44s**。skip 原因为 Windows junction 命令报 `'chcp' is not recognized`，不视作真实 OS 链接 containment 通过。`git diff --check` 已通过。真实并发容量压力测试仍未运行。

同日使用授权的本地 gateway 和 `codex/gpt-6-luna` 对最终静态发现实现做 CLI 复验。临时 `widgetkit.py` 含静态版本 `0.4.2` 与 `scale` API；journal 共 7 个事件，观察到 `python.environment_discovered` 先于 `python.requested`。发现 payload 为 succeeded、`scale=true`、`install_supported=false`；之后显式 Python execution succeeded，耗时 15 ms，断言 `scale(3) == 9`。共 3 次 committed activation、2 次工具调用、14,134 provider usage tokens。CLI 退出码为 0，最终答复正确复述发现和 smoke test；临时目录已清理。此证据是一轮本地体验，不推及其他模型或负载。

证据采集脚本在读取 journal 后因 SQLite 连接未显式 close，Windows 临时目录自动清理遇到 `WinError 32`；CLI 本身已正常退出且 journal 数据已读取，之后关闭进程句柄并手动清除该临时目录。该清理错误不影响本轮 CLI 结果。

## 2026-10-05 — 异步 Agent 文献与环境扫描非阻塞化

### 文献审查

核读并对照四项研究：

- Kim et al., [LLMCompiler（ICML 2024）](https://arxiv.org/abs/2312.04511)：将单任务内工具调用构造成依赖 DAG，允许独立函数并行；摘要报告最高 3.7× latency speedup、6.7× cost savings 和约 9% accuracy improvement。其关键条件是 planner 能识别依赖；它没有验证 Nervipulsa 的邮箱容量、用户持续输入或 worker 生命周期。
- [PASTE v3](https://arxiv.org/abs/2603.18897v3)：按历史模式预测后续工具并进行隔离推测执行。当前 v3 摘要报告 task completion time 降低 43.5%、observed tool latency 降低 1.8×；早期 v1 摘要报告的 48.5% 与吞吐指标不同，故本文固定引用版本，不混用数字。对 Nervipulsa 而言，猜测参数和有副作用的 Python 工具都需要额外一致性/回滚协议，当前没有采用 speculation。
- [AsyncTool v3](https://arxiv.org/abs/2605.27995v3)：在 12 个工具、358 条经验证单任务轨迹基础上构造 712 个双/三任务样本，评估 19 个模型，并用模拟工具延迟测量 task switching、dependency tracking 和 state maintenance；GPT-4.1 总分最高为 38.06。它适合启发未来延迟/交错 benchmark；轨迹合成与模拟时延的结果不能证明真实 event loop、Mailbox 或 Python worker 的正确性。
- [LLMs are General Asynchronous Agents v1](https://arxiv.org/abs/2609.35427v1)：使用 asyncio 风格 inference coroutine、共享 KV/cache blocks 与 attention views 进行并行模型推理，展示视频、游戏与监控场景；作者明确指出模型异步操作尚不可靠。该机制位于模型推理层，不等同于事件日志、handler 语义或跨进程恢复。

综合判断：这些工作分别研究计划内并行、工具 speculation、多任务延迟评测和模型 coroutine 并行；没有一项直接证明事件序号、背压、取消/迟到结果或 handler exactly-once 语义。公开摘要中的加速数字依赖不同 workload、模型和系统，不外推为 Nervipulsa 的性能收益。

### 实现与证据

源码审计发现 `python_environment` 在 `_deliver_one` 同步扫描路径、metadata 与 AST，会阻塞 LLMActor 所在线程。现在 `_complete_and_commit`、`_deliver_tools`、`_deliver_one` 沿调用链异步化，并通过 `asyncio.to_thread` 执行扫描；原有 `python.environment_discovered`、`activation_id`、`tool_call_id`、`reply_to` 和失败回执保持不变。

新增 `test_python_environment_scan_does_not_block_actor_loop`：使用 threading events 阻塞扫描；等待期间验证 loop callback 能运行、后续 `user.message` 投递被接受；释放后检查环境发现事件与该消息共同进入下一 provider 请求。定向测试 `python -m pytest tests/test_llm.py::test_python_environment_scan_does_not_block_actor_loop -q` 为 **1 passed in 1.45s**。之后完整 `python -m pytest` 为 **77 passed, 1 skipped in 47.32s**；skip 仍因当前 runner 的 `'chcp' is not recognized`，junction containment 未实机验证。

### 边界与后续

Actor 仍 await 当前发现扫描，所以后续消息在扫描期间可进入事件流，但不会在当前 activation 完成前启动下一轮推理。这项改动证明 event loop responsiveness，不证明并行 agent activation、用户可见延迟改善或负载下吞吐收益；本轮未做新的 live model run 或性能基准。

下一步优先：1）对 handler 全局槽位上限做真实并发 admission/消费压力测试；2）组合 cancel、timeout、restart、shutdown 与迟到 frame；3）建立带真实延迟工具的两条独立任务链基线，报告完成时间、事件顺序、依赖违规和清理情况；4）评估 junction containment runner。

## 2026-10-05 — Provider profiles 与 Magpie 兼容

### 外部实现对照

- [OpenCode providers](https://opencode.ai/docs/providers/) 将 provider ID、每 provider 的 options/baseURL、凭据与 model catalog 分开；自定义端点通过 OpenAI-compatible adapter 接入，不要求每个 vendor 都有独立的请求循环。
- [Goose providers](https://block.github.io/goose/docs/getting-started/providers/) 同时维护原生协议 provider 和大量 OpenAI-compatible endpoints；它把 provider-specific authentication/environment variables 留在各自配置边界，并特别提供多个自定义 OpenAI-compatible provider 的配置路径。
- [Magpie](https://github.com/yetone/magpie) 将不同上游汇聚到 gateway，并在 gateway 做 Chat Completions、Responses、Anthropic Messages、Gemini 之间的转换。对 Nervipulsa，正确的最小接入点是 Magpie 的 OpenAI-compatible Chat Completions：base URL `http://127.0.0.1:3425/v1`，route `/chat/completions`，模型名按 `provider/model` 原样传递；loopback 示例 token 为 `magpie`。共享给 LAN 时必须改用 Magpie gateway key。

### Nervipulsa 采用的边界

配置现在保存多个命名 profile（provider/adapter/base URL/model/API key），可用 `/config profiles` 查看、`/config use <name>` 切换；旧版单 provider 字段仍可加载。协议适配器与 profile 名称分离，内置 profile 覆盖 OpenAI、DeepSeek、OpenRouter、Ollama、LM Studio 和 Magpie，另可配置自定义 OpenAI-compatible URL。模型 ID 不做前缀改写。凭据只存在用户配置，公开展示仍遮蔽。

当前实现依旧只实现 OpenAI Chat Completions 请求/响应与工具调用格式。它不代表 Nervipulsa 原生支持 Anthropic Messages、Gemini 或 OpenAI Responses；Magpie 承担其上游转换。该决定沿用小型 agent 的自定义兼容端点模式，避免为 Magpie 已解决的协议转换重复维护多套 transcript 编码。

### 证据边界

provider profile 默认值、持久化与切换、Magpie 请求字段测试及 README 操作说明已完成。实现代理报告完整自动测试 **82 passed, 1 skipped**；之后我补入了 DeepSeek/OpenRouter/Ollama/LM Studio preset 默认值与 provider-specific API key preflight，没有重新跑 pytest，因此 82/1 不覆盖这最后的静态配置/auth 修改。最终 `git diff --check` 通过。

本轮实际复验 Magpie：`GET /v1/models` 使用 Bearer `magpie` 返回 HTTP 200，目录有 14 个模型且包含 `codex/gpt-6-luna`；之后以隔离的临时配置目录和 workspace 启动 Nervipulsa 的 `magpie` profile，传入该模型和短文本请求，CLI 展示预期 provider/model 并返回 `MAGPIE COMPATIBLE.`，退出码 0、stderr 为空。临时配置和 workspace 已清理。该结果证明本机此模型/gateway 的 Chat Completions profile 可用，不证明其他 profile 的真实服务可用或 Magpie 上所有模型均兼容；本轮也没有测试 Python tool-call 往返。


## 2026-10-05 — OMP Hashline Edit as first custom Python library

### Upstream facts and migration boundary

The first custom library is exposed inside the persistent worker as
`nervipulsa.hashline_edit`; the model imports it with
`from nervipulsa import hashline_edit`. This is a project-owned Python module and
uses only the standard library. Its design follows the public
[OMP edit-tool contract](https://github.com/can1357/oh-my-pi/blob/main/docs/tools/edit.md),
[Hashline format helpers](https://github.com/can1357/oh-my-pi/blob/main/crates/pi-edit/src/modes/hashline/format.rs),
[snapshot store](https://github.com/can1357/oh-my-pi/blob/main/crates/pi-edit/src/store.rs),
and [text normalization helpers](https://github.com/can1357/oh-my-pi/blob/main/crates/pi-edit/src/text.rs).
The upstream workspace declares MIT in its
[Cargo manifest](https://github.com/can1357/oh-my-pi/blob/main/Cargo.toml); this Python
implementation was written independently from the documented behavior, not copied
from its Rust source.

OMP's section header is `[path#TAG]`; TAG is the four-character uppercase hex form
of `xxHash32(seed=0) & 0xffff`. Before hashing, BOM is removed, CRLF and lone CR
are normalized to LF, and trailing ASCII space, tab, and CR are ignored per line.
The view uses `N:text` rows. Implemented PUT forms are `PUT N.=M:` for closed-range
replacement, `PUT <N:` before a line, `PUT >N:` after a line, and `PUT >$:` at EOF.
Every hunk in one patch addresses the same snapshot, and replacement body rows start
with `+`.

### Nervipulsa implementation and limits

`view_file(path)` records a bounded worker-epoch snapshot and returns the OMP-style
header and numbered rows. `edit(patch)` accepts one file section, validates the
four-digit tag and current normalized content against the newest cached matching
snapshot, rejects unknown/stale tags and overlapping or duplicate hunks, then writes via a
temporary file and atomic replacement. Paths are workspace-relative and resolved
before use; symlink/junction targets outside the worker workspace are rejected.
UTF-8 BOM and the detected LF/CRLF style are preserved. File and in-memory snapshot
limits are enforced. The 16-bit tag can collide; lookup follows the newest matching
retained snapshot and then compares its full text to the current file before writing.

This is intentionally a PUT-only subset: no Tree-sitter block locators, CUT, REM,
MV, clipboard registers, historical stale-tag recovery, or seen-line enforcement.
The four-hex tag is a locator, not a cryptographic identity; the implementation
also compares the full cached text before writing. The helper constrains only its
own API calls and is not a sandbox for arbitrary `python_exec` code. A concurrent
filesystem writer can still race the final compare-and-replace window.

No tests or live worker request were run in this slice. The algorithm and parser
have therefore not yet been checked against upstream compatibility vectors or a
real-model edit; those are the next verification steps.

## 2026-10-06 — Long-running task protocols and recovery boundaries

### Sources checked

- The official MCP [Tasks extension overview](https://modelcontextprotocol.io/extensions/tasks/overview) and [extension repository](https://github.com/modelcontextprotocol/ext-tasks) identify Tasks as an extension, not a core MCP request mode. Repository schema `2026-07-28` is marked stable; `draft` remains under development. A task response carries a handle, initial status, TTL, and suggested polling interval; the task must be durably created before the response is sent. Statuses are `working`, `input_required`, `completed`, `failed`, and `cancelled`; cancellation is cooperative. Notifications can replace polling when both sides support them.
- The current released [A2A specification](https://a2a-protocol.org/latest/specification/) reports version `1.0.0`. It allows a send operation to return either a direct message or a task, and defines Get/List/Cancel, status/artifact streaming, and optional push notification operations. Its primary scope is inter-agent interoperability; it is not a prescription for an in-process Python worker or bounded mailbox.
- [Google AIP-151](https://google.aip.dev/151) standardizes long-running operation resources and typed metadata/results. Its roughly 10-second threshold is explicitly a rule of thumb. It distinguishes failures preventing start (immediate request error) from failures during execution (terminal operation error), and describes concurrency conflicts and resource expiry.
- [Temporal Activity guidance](https://docs.temporal.io/activities) recommends idempotent activity code because retries can repeat side effects. An attempt starts from its initial state unless recorded heartbeat details supply a checkpoint. A worker crash after a side effect but before result confirmation cannot be interpreted as proof that the side effect did not happen.
- [AgentRewind](https://arxiv.org/abs/2608.14380) records aligned checkpoints of agent context and a controlled environment, then restores both to a selected point and adds memory from the failed trajectory. It evaluates long-horizon engineering work with MettleBench. This is recovery by coordinated rewind in a controlled environment, not recovery provided by an event log alone; its results do not establish rollback of arbitrary external side effects.

### Comparison with Nervipulsa

Nervipulsa's accepted `python.requested` currently means that the live process admitted the event and reserved terminal/handler mailbox capacity. It does not mean a durable job record was committed before returning the acceptance receipt. `Bus.call()` offers the event and invokes the observer; the `Journal` observer only enqueues to a bounded background queue (`queue_limit=4096`). When that queue is full, or a write fails, records can be dropped and `journal.incomplete` becomes true. Therefore this SQLite journal is an asynchronous observation/audit sidecar, not a durable admission queue, task store, or process-crash recovery mechanism.

The existing execution ID is the event ID for one attempt in one runtime session, while `worker_epoch` scopes worker-local state. A future durable task interface should distinguish a stable logical `task_id` from per-attempt `execution_id` and worker epoch. A restart must not blindly resubmit arbitrary Python code: filesystem/process effects may have happened even if the terminal event was not committed or consumed. Without an idempotency contract or restorable workspace snapshot, the honest terminal state for that window is unknown/reconciliation-required, not “safe to retry.”

### What to absorb, and what not to copy

1. Keep push events inside the current local runtime. MCP's pollable handle and A2A's Get/List/stream/push patterns are useful if Nervipulsa later exposes jobs to disconnected or external clients; they are unnecessary overhead for every local completion event.
2. Make the current guarantee explicit: acceptance is process-local and volatile. Do not describe the journal as recovery or durability.
3. If durable long-running work becomes a product requirement, define a task state resource before implementation: stable task ID; attempt IDs; queued/working/input-required and immutable terminal states; result/error; timestamps/deadline; cancellation-requested versus confirmed-stopped; retention/expiry; and authorization scope.
4. Specify crash windows before enabling automatic retry. Side-effecting tools need stable idempotency keys, explicit non-retryable behavior, or a restorable controlled environment. “Exactly once” cannot be inferred from event correlation or a unique ID.
5. Keep agent-context recovery and environment recovery aligned. AgentRewind suggests that restoring transcript/checkpoint state without the matching workspace state is inconsistent; conversely restoring files without recording prior-attempt lessons can repeat the same error.
6. Improve operational visibility now: show journal drop count and writer error in `/status`, while continuing to treat those diagnostics as evidence of incomplete observation rather than failed task delivery or recoverable execution.

### Implementation slice and evidence boundary

`Runtime.status()` now exposes `journal_dropped` and `journal_error` alongside the existing `journal_incomplete` flag. CLI `/status` prints the drop count and a bounded error excerpt when the journal is incomplete. This applies the operational-visibility lesson without changing admission, event delivery, or execution semantics. No tests were run in this research slice; `git diff --check` is the only planned static verification.

### Research conclusion

The most valuable direction is not to turn the current mailbox into a distributed job system prematurely. First preserve its strong in-process bounded-delivery contract and state its volatile acceptance boundary accurately. If restart recovery is later required, build a separate durable task/attempt layer with idempotency and aligned environment checkpoints; the existing observation journal is not that layer.

## 2026-10-06 — Mailbox ordinary admission micro-optimization

### Target and change

`Mailbox.ordinary_size` previously counted `Lane.ORDINARY` items by scanning the queued deque. `can_offer()` calls this property on each ordinary admission, so the capacity check cost grew linearly with queue occupancy. The mailbox now maintains `_ordinary_size`: increment only after a successful ordinary enqueue, decrement when ordinary items leave the queue through either `take_batch()` or `drain_available()`, and reset when `close()` clears queued state. Leased items remain outside ordinary queue capacity, as before. Rejection checks, event sequence sorting and all terminal/handler reservation paths are unchanged.

### Microbenchmark

A synthetic microbenchmark used preconstructed events and a mailbox capacity of 2,000, so event construction was excluded and the deque scan was large enough to measure. Values are a single local run of the benchmark harness; absolute timings vary by interpreter and host.

| Operation | Queue occupancy | Before | After |
|---|---:|---:|---:|
| `ordinary_size` | 2,000 | 165,391 ns | 97 ns |
| `can_offer` | 2,000 | 225,840 ns | 266 ns |
| `ordinary_size` | 1,000 | 98,508 ns | 100 ns |
| `can_offer` | 1,000 | 121,241 ns | 419 ns |

The measurements support the specific claim that ordinary capacity inspection is now O(1) rather than scanning queued items. They do not establish a comparable end-to-end latency reduction under the default mailbox limit of 64, nor a model-task throughput gain; provider latency will dominate ordinary interactive use. The change is most relevant to bursts, enlarged mailbox configurations and repeated saturated admission checks.

### Verification

`python -m pytest tests/test_events.py -q`: **9 passed**. Coverage asserts ordinary count after accepted/rejected admissions, after `take_batch()`, after `drain_available()`, and after close; the handler global-capacity test confirms ordinary-lane count remains independent of reserved slots. `git diff --check` passed. The full suite and application-level task latency were not run/measured in this slice.

## 2026-10-06 — Batched SQLite journal flush

### Change and invariants

`Journal._flush()` previously issued one `connection.execute()` per observation row, then committed once. It now prepares rows by table and calls `executemany()` once per table inside the same transaction. Each table's rows retain their original relative order, including repeated activation/execution IDs whose later UPSERT must win. The tables have no cross-table foreign-key dependency. Unknown kinds remain ignored; any serialization or database error rolls back the whole batch, records the same error and dropped count, and clears `pending` as before.

### Paired measurement

A local file-backed SQLite benchmark used WAL with `synchronous=NORMAL`, 50 AB/BA paired samples, 10 separate 64-row transactions per timed sample, and the same prebuilt workload in each arm: 32 events (one with a 32 KB text payload), 24 deliveries, 4 activation UPSERTs and 4 execution UPSERTs. Setup and row construction were outside the timer.

| Measurement | Per-item `execute` | Grouped `executemany` |
|---|---:|---:|
| Median time per 10-transaction sample | 22.148 ms | 21.359 ms |
| P10–P90 time | 15.731–58.029 ms | 14.810–52.358 ms |

Grouped writes won in 40 of 50 pairs. The ratio of the two overall medians is about 1.037 (roughly **3.6% less time**); the median of per-pair ratios was 1.102, with P10–P90 0.975–1.228. The different summaries and broad timing spread show that the win is modest and host-sensitive. This measures journal flush throughput only; it does not prove a reduction in production journal drops or user-visible request latency.

### Verification

`tests/test_journal.py` and `tests/test_cli.py`: **13 passed**. The new regression covers repeated activation/execution UPSERT ordering and unknown-kind handling. `git diff --check` passed. Full-suite and production-load/drop-rate measurements remain outstanding.

## 2026-10-07 — DeepSeek Harness architecture cross-check

### Scope and evidence boundary

Reviewed the official `deepseek-ai/deepseek-harness` README, architecture guide, tool pipeline, session log, generic jobs contract, model-facing job controls, subprocess package map, and safety notice. The repository's `master` head observed on 2026-10-07 was `5badb15009ae1756c3afe0ae0cef1faafc290ccc` (commit date 2026-10-03). This was a documentation and contract review; the project was not cloned, built, executed, or security-audited. Its own README labels it a developer preview with breaking changes, and its safety notice says it has not had a security audit and is not production-ready.

### Transferable ideas

1. **Keep capability contracts separate from implementations, and make lifecycle reversible.** DeepSeek Harness uses Cordis plugins for the model adapter, tool registry, session log, agent loop, and product layers. Profiles and ordered bundles compose a running application; disposing a plugin unwinds its registrations. Nervipulsa already has provider/tool protocols and injectable registries, but these are static composition seams rather than a dynamic plugin host. Preserve the service-definition/provider/consumer boundary. Add dynamic mount/unmount only when third-party or runtime-selected extensions become a concrete requirement; adopting all of Cordis now would add a large lifecycle and configuration surface.

2. **Make the reconstructable session record distinct from the observation journal.** DeepSeek's session package treats committed typed events as the source from which model history is projected; model-visible content must be reproducible from that log, and persistence backends expose an awaited flush checkpoint. It also distinguishes a tool never durably started from a started call with no durable result (`TOOL_NOT_STARTED` versus `TOOL_OUTCOME_UNKNOWN`) and warns against blind retries of side-effecting calls. Nervipulsa's SQLite `Journal` is intentionally a bounded, asynchronous observation sidecar that can drop rows; it cannot satisfy this contract. If restart recovery or resumable sessions become product requirements, define a separate durable session/task-attempt record and admission/flush boundary rather than strengthening claims about `Journal`.

3. **Use one explicit job contract for background work.** DeepSeek's generic `JobRegistry` gives each job an ID and owner fence, common status/progress/read/wait/cancel operations, bounded output retention, cursor-based non-consuming observation, and optional output byte caps. Cancellation settles only after work stops; a live waiter suppresses a duplicate completion notice. Its shipped registry is explicitly process-local: jobs die with the harness process, so the contract does not imply restart durability. This maps well to a future Nervipulsa API if Python work becomes user-visible background work that must be listed, inspected, or cancelled independently of a model activation. Today, the Python event path already delivers terminal observations automatically, so a generic job layer should be justified by a new use case rather than added as a second representation of every execution.

4. **Treat completion delivery and wakeups as a budgeted policy.** The job tools inject completion into a busy agent's next step and may wake an idle agent; several completions can share one step. A configurable consecutive-wakeup cap bounds self-triggering job chains, with the documented trade-off that notices beyond the cap wait for later input. Nervipulsa already coalesces worker terminal/handler observations into an activation. If it adds background-job wakeups, model the busy/idle/awaited cases explicitly and bound follow-up activations so completion delivery cannot create an unbounded model-cost loop.

5. **Keep tool policy outside tool implementations.** The DeepSeek tool pipeline separates schema validation, allow/deny/ask policy, a monotonic guard that later listeners cannot reverse, execution wrappers, result policy, and final observation. Cancellation is cooperative and its terminal reason depends on whether dispatch began; timeouts require an enforcing wrapper. Nervipulsa's current `Tool` protocol has schema, validation, and invocation, while Python timeout/cancellation is owned by the host/worker lifecycle. If the catalog grows, typed outcome states and a monotonic policy hook are clearer extensions than embedding provider-specific policy in each tool.

6. **Programmatic tool calling is a conditional performance idea.** DeepSeek's PTC mode derives an SDK from visible tool schemas and can schedule safe independent calls concurrently while keeping mutations ordered. It also documents that intermediate values are execution-local, not replayable, and can be unbounded. Nervipulsa's persistent Python worker is not equivalent to that fresh-per-run PTC environment. A generated Python tool SDK could be explored if several expensive tools create measurable round-trip cost; it should carry explicit concurrency classes, output limits, and replay semantics first.

### Direction for Nervipulsa

The strongest near-term lesson is to sharpen task outcome semantics around the existing event path: accepted means process-local admission; cancellation requested is distinct from confirmed stop; a crash after a side effect but before a terminal record means unknown outcome. A later background-job feature should start with a process-local contract, bounded output and owner checks, and must state that restart loses jobs. Durable session/task recovery remains a separate design. Keep the present small injectable registries until an actual plugin lifecycle requirement appears.

### Source references

- [Official repository README](https://github.com/deepseek-ai/deepseek-harness/blob/5badb15009ae1756c3afe0ae0cef1faafc290ccc/README.md)
- [Architecture and plugin composition](https://github.com/deepseek-ai/deepseek-harness/blob/5badb15009ae1756c3afe0ae0cef1faafc290ccc/docs/architecture.md)
- [Tool registry and execution pipeline](https://github.com/deepseek-ai/deepseek-harness/blob/5badb15009ae1756c3afe0ae0cef1faafc290ccc/packages/core/tools/README.md)
- [Session event log](https://github.com/deepseek-ai/deepseek-harness/blob/5badb15009ae1756c3afe0ae0cef1faafc290ccc/packages/core/session/README.md)
- [Background-job registry](https://github.com/deepseek-ai/deepseek-harness/blob/5badb15009ae1756c3afe0ae0cef1faafc290ccc/packages/jobs/jobs/README.md)
- [Model-facing job controls](https://github.com/deepseek-ai/deepseek-harness/blob/5badb15009ae1756c3afe0ae0cef1faafc290ccc/packages/jobs/tool-jobs/README.md)
- [Safety notice](https://github.com/deepseek-ai/deepseek-harness/blob/5badb15009ae1756c3afe0ae0cef1faafc290ccc/SAFETY.md)

## 2026-10-07 — Cross-harness architecture survey

### Scope and limits

Reviewed selected official documentation for OpenAI Agents SDK and hosted Agents API/Codex Harness, LangGraph, Google ADK, CrewAI Flows, Microsoft Agent Framework and AutoGen, OpenHands SDK/runtime, PydanticAI, and Claude Code. DeepSeek Harness is covered in the preceding section; Pi was compared in earlier research. This is a documentation-level comparison, not a source audit, execution benchmark, or exhaustive survey of every agent product. Claude Code and the hosted Codex Harness are closed implementations; only their public contracts can be compared.

### Findings by system

| System | Useful documented design | Relevance and boundary |
|---|---|---|
| OpenAI Agents SDK / hosted Codex Agents API | The SDK keeps a small primitive set and provides built-in trace/span recording for model calls, tools, handoffs, guardrails, and custom events. Its session API stores conversation items and can resume approval interruptions. The hosted Agents API is a different deployment model: OpenAI manages session orchestration, context compaction, recovery, and (optionally) the sandbox; the application supplies tools and chooses the environment. | Adopt end-to-end correlation and trace/span structure. Keep SDK conversation history distinct from durable execution. Treat the hosted API as a product boundary comparison, not a code pattern Nervipulsa can copy. |
| LangGraph | A checkpointer stores graph state per `thread_id` and super-step; a separate Store holds cross-thread application data. Pending writes let a resumed super-step retain other nodes' completed writes. Interrupts save state and resume with the same thread ID, but the interrupted node runs again from its beginning. | Strong recovery vocabulary, but checkpointing alone does not give exactly-once external effects. Code before a resumed interrupt may run again; any side-effect retry needs idempotency or reconciliation. Retention is also part of the design because checkpoints accumulate. |
| Google ADK | Events carry invocation/event identity, author, timestamp, streaming/turn-complete markers, branch, long-running tool IDs, and actions such as state/artifact deltas and control transfers. Sessions retain event history separately from state. State scopes distinguish session, user, app, and invocation-temporary values; restart durability depends on the selected SessionService. Google's long-running tutorial uses explicit state-machine steps plus webhook-driven wake/resume rather than polling. | Very close to Nervipulsa's event vocabulary. `invocation_id`, `longRunningToolIds`, explicit state deltas, and separate state scopes are useful concepts. The webhook/state-machine design is relevant only when tasks must sleep across long external waits. The blog is an implementation tutorial, not a guarantee that arbitrary side effects are exactly once. |
| CrewAI Flows | `@start`, `@listen`, and routers express event-triggered control flow; Pydantic models can make flow state typed. `@persist` supports resuming the same lineage and forking from an older state; the docs describe SQLite as the default persistence backend. | Useful for explicit task-state schemas and the difference between resume and fork. Its workflow DSL and multi-agent orchestration are larger than Nervipulsa needs today. Persistence claims here are based on the current official documentation, not independent recovery testing. |
| PydanticAI | Durable runs can be delegated to Temporal, DBOS, Prefect, Restate, and other engines through a backend seam. Its docs explicitly separate run durability from conversation storage. Its retry guide enumerates multiple layers—workflow engine, model fallback, agent/tool/output retry, provider SDK, and transport—and shows how attempt counts multiply. | Strongest reference for defining per-layer retry budgets and recovery semantics before adding retries. Durable execution should be a backend capability with explicit engine semantics, not an inference from a saved transcript or SQLite observation file. |
| Microsoft Agent Framework / AutoGen | Agent Framework combines session state, typed interfaces, middleware/telemetry, and explicit graph/workflow orchestration. AutoGen Core documents actor-style message passing and a host/worker distributed runtime; the distributed runtime is marked experimental. The AutoGen repository now says it is in maintenance mode and points toward the successor framework. | Reuse the separation between agent calls and deterministic workflows if Nervipulsa grows orchestration needs. Avoid building against AutoGen-only APIs as a strategic extension point; its distributed process model is not needed for the current single-runtime work. |
| OpenHands | The coding runtime uses a client/server boundary around a Docker execution environment. Its SDK persistence separates a base state snapshot from an append-only event directory and records messages, execution status, tool outputs, configuration, workspace context, usage, and agent state. | The separate execution environment and reproducible workspace are relevant to Python worker isolation. Nervipulsa's Windows AppContainer prototype remains a different, weaker isolation boundary; OpenHands' Docker model is not evidence that local execution is secure. The base-state plus event-log split is a useful persistence shape to study. |
| Claude Code | Public hooks are typed lifecycle events at session, turn, and tool-call boundaries; pre-tool hooks can block execution. Subagents use their own context and restricted tool sets, return summaries, and still consume the shared usage budget. | Good model for small lifecycle extension points and context/cost accounting. Internal implementation behavior cannot be inferred from public hook docs. |

### Cross-cutting conclusions for Nervipulsa

1. **Keep three records separate.** A reconstructable conversation/session record, a recoverable task/attempt state, and an observability trace answer different questions. Nervipulsa's bounded asynchronous `Journal` is only the third. It can report dropped/error status; it must not become the acceptance ledger or recovery source by implication.

2. **Represent external-effect uncertainty.** Use the existing distinction between admission and execution, and define a first-class unknown outcome for a crash after an external effect may have happened but before a result was committed. Recovery should reconcile or ask before retrying a non-idempotent action. `task_id`, `execution_id`, and `worker_epoch` should remain different identities if durable task support is designed.

3. **Budget retries by layer.** A model correction, provider SDK retry, transport retry, worker retry, and durable-engine retry can multiply. Any future retry policy needs explicit per-layer attempt and elapsed-time budgets, a total run cap, and an idempotency rule for tools. Current performance comparisons do not justify adding retries.

4. **Add trace correlation before a large telemetry system.** One trace ID per user task/turn, parent-child span IDs for activation/model/tool/worker phases, plus existing `session_id`, `execution_id`, and `worker_epoch`, would make asynchronous failures easier to follow. Trace payload policy should not silently persist secrets or full user code.

5. **Use a job registry only for independently managed work.** If Python or other work needs listing, output reads, wait, or cancellation outside the active model activation, a generic owner-scoped, bounded `JobRegistry` is a good next seam. A process-local backend must say that process restart loses work. Existing terminal and handler delivery already cover current model activation flow, so duplicating each execution as a second lifecycle model needs a concrete consumer.

6. **Model long idle workflows as explicit state machines.** If Nervipulsa later supports jobs that wait hours/days for webhooks or approval, persist the current step and pending signal, suspend without polling, and resume from a verified external event. Do not ask the model to infer progress from a huge transcript. This is beyond the current local coding-harness contract.

7. **Keep the execution environment pluggable and describe its actual boundary.** OpenHands demonstrates one containerized execution world; Nervipulsa already has process and AppContainer seams. Compare them by tested host reachability, filesystem/network permissions, process cleanup, and resource limits. Do not generalize a successful worker launch into a security claim.

### Prioritized learning plan

- **Near term:** write down the retry taxonomy and unknown-outcome contract alongside the existing admission/worker lifecycle research; add trace correlation only when it can be measured against real debugging cases.
- **Conditional feature:** introduce a common job registry if a producer needs independent status/output/cancel controls; first test ownership, bounded output, cancellation settlement, wakeup deduplication, and restart-loss behavior.
- **Before durability work:** specify stable task identity, attempt identity, acceptance checkpoint, idempotency/reconciliation, event retention, and resume semantics, then fault-inject the side-effect/terminal/consume boundaries already listed in pending work.
- **Defer:** a general dynamic plugin manager, a full graph workflow DSL, cloud scheduler integration, and multi-agent orchestration until product use cases justify them.

### Source references

- [OpenAI Agents SDK](https://openai.github.io/openai-agents-python/) · [Tracing](https://openai.github.io/openai-agents-python/tracing/) · [Sessions](https://openai.github.io/openai-agents-python/sessions/)
- [OpenAI hosted Codex Agents API](https://developers.openai.com/api/docs/guides/agents-api/overview)
- [LangGraph checkpointers](https://docs.langchain.com/oss/python/langgraph/checkpointers) · [interrupts](https://docs.langchain.com/oss/python/langgraph/interrupts) · [persistence](https://docs.langchain.com/oss/python/langgraph/persistence)
- [Google ADK events](https://adk.dev/events/) · [session state](https://adk.dev/sessions/state/) · [long-running ADK tutorial](https://developers.googleblog.com/build-long-running-ai-agents-that-pause-resume-and-never-lose-context-with-adk/)
- [CrewAI Flows](https://docs.crewai.com/v1.15.23/en/concepts/flows) · [Flow state and persistence](https://docs.crewai.com/v1.15.23/en/guides/flows/mastering-flow-state)
- [PydanticAI durable execution](https://ai.pydantic.dev/durable_execution/) · [retry layers](https://github.com/pydantic/pydantic-ai/blob/main/docs/retries.md)
- [Microsoft Agent Framework overview](https://learn.microsoft.com/en-us/agent-framework/overview/) · [AutoGen distributed runtime](https://microsoft.github.io/autogen/stable/user-guide/core-user-guide/framework/distributed-agent-runtime.html) · [AutoGen repository status](https://github.com/microsoft/autogen)
- [OpenHands runtime architecture](https://docs.openhands.dev/openhands/usage/architecture/runtime) · [conversation persistence](https://docs.openhands.dev/sdk/guides/convo-persistence)
- [Claude Code hooks](https://code.claude.com/docs/en/hooks) · [subagents](https://code.claude.com/docs/en/sub-agents)

## 2026-10-07 — Reuse request-size accounting

`LLMActor._process()` already measures transcript and tool-schema JSON characters for context accounting. `_complete_and_commit()` serialized both objects again solely to populate attempt metrics. It now reuses those activation metrics when the request snapshot exists; a missing snapshot or missing/invalid metric retains the prior serialization fallback. The `ModelRequest` payload and compaction decisions are unchanged.

Paired local microbenchmark: a fixed 16-message JSON transcript plus eight tool schemas, 100 calls per sample, warm-up followed by nine alternating samples. The transcript payload targets were about 10 KB, 50 KB, and 200 KB; tool schemas serialized to 1,632 characters. Direct serialization medians were **121.10 / 366.88 / 1,522.90 μs**; cached lookup with type guards was **0.451 / 0.486 / 1.249 μs**, saving approximately **0.121 / 0.366 / 1.522 ms per activation**. Counts matched exactly in every case.

This isolates character-accounting overhead on prebuilt objects. It does not include actor dispatch, provider serialization/network time, model inference, or prove an end-to-end coding-task speedup. AST parsing and `git diff --check` passed; no tests were added or run.

## 2026-10-09 — Session-scoped Journal identities

### Source and finding

Source message: user-provided Nervipulsa review sent-at `2026-10-09T01:19:02.161Z` (original timezone: Asia/Shanghai). Source inspection confirmed a deterministic collision path: each `Bus` starts its sequence at zero and generates IDs as `evt_{seq:08d}`, while `events.id` was a global primary key. `INSERT OR IGNORE` therefore silently discarded same-numbered events from later sessions. Python execution IDs reuse the request event ID; the global `executions.execution_id` key let a later session's upsert change fields on the earlier row while leaving its `session_id` unchanged. Activation IDs are UUID-derived and were not part of this deterministic collision path.

### Change

`events` now uses `(session_id, id)` as its primary key and retains `UNIQUE(session_id, seq)`. `executions` now uses `(session_id, execution_id)`, and its upsert is session-scoped. Startup detects the legacy primary keys with `PRAGMA table_info`, then transactionally rebuilds only those tables and copies existing columns explicitly. Same-session event ignore and execution upsert behavior are retained. The writer also flushes a partial batch after its 0.2-second queue wait times out idle, narrowing the in-memory observation window without changing event admission. Regression coverage exercises repeated IDs across two sessions, migration of existing rows, and idle flushing of one record. The stale non-interactive CLI test expectation was updated from a 30-second timeout to `timeout=None`, matching the existing EOF-drain behavior.

### Verification and limits

The independent targeted run `python -m pytest tests/test_journal.py tests/test_cli.py -v` completed with **16 passed in 3.10s**. The idle partial-batch test, legacy migration and cross-session identity cases, and the non-TTY EOF case passed. The main shell's repeated pytest invocations timed out without results; the independent runner completed the targeted command. No full suite was run. `git diff --check` passed after the implementation changes. The migration prevents future collisions but cannot reconstruct rows already silently dropped or recover overwritten historical execution fields. Idle flush narrows the observation loss window, but the Journal remains asynchronous and uses WAL with `synchronous=NORMAL`; it is not a durable admission or task-recovery store. Changes are currently uncommitted and unpushed.
