# Nervipulsa 的事件原生 Agent Runtime：有界 Handler 投递与生命周期语义

- **状态：** 研究稿（持续研究中）
- **版本日期：** 2026-10-05（Asia/Shanghai）
- **项目：** Nervipulsa
- **证据更新：** 2026-10-13（Asia/Shanghai）；见下方日期补记。

## 摘要

基于工具调用循环构建的 Agent harness，通常把工具执行结果视为唯一的运行时反馈。本文研究一种事件原生扩展：持久 Python worker 可注册完成回调，回调结果作为独立事件进入 agent 的观察流；事件总线、邮箱和执行生命周期负责关联、排序、容量控制与审计。研究重点不是证明回调 API 可运行，而是确定在并发、背压、超时和 worker 重启下，回调结果能否以有限资源进入模型上下文，同时保留可解释的失败语义。

Nervipulsa 当前实现采用每次 Python 请求的终态预留、最多 16 个 handler 结果槽位、全局有界 handler 容量、终态报告预期与缺失结果，以及按事件序号投影观察的 coalescer。源码审查与自动化测试覆盖这些主要账本路径；静态 `python_environment` 以受限路径、发行版 metadata 和 AST 线索发现自定义库，不导入代码、不自动安装。event-loop 阻塞回归证明环境扫描在线程执行期间，runtime loop 仍可接受后续用户消息，扫描结果随后进入模型上下文。2026-10-05 环境发现阶段的全量回归为 77 passed、1 skipped；2026-10-13 的 fault-semantics 研究版本全量套件为 124 passed。该研究版本新增两个经过门控的进程终止探针：Journal 预先 flush 后，在请求出队前和终态消费前重启都未从旧记录重放。它们只提供两个窗口的有限进程级证据，不构成通用跨进程恢复或断电保证。尚无并发负载证据。另于 2026-10-13 合入 Durable Effect Protocol，以 `intent → dispatching → observed | unknown` 记录 Python 执行副作用的可观察状态，并用真实 SQLite commit 故障注入验证关键提交边界；该 ledger 提供未知结果的持久审阅，不恢复任务或 transcript，也不自动重放。合并两条分支并修正旧 worker-exit 断言后，全量测试为 137 passed in 102.03s。

当前证据支持将其视为一个有真实端到端行为、具备初步有界投递契约的研究原型。它还不足以证明该桥接在成本、稳定性或通用性上优于普通工具调用循环。3425 handler 任务共两次 committed activation、一次 execution、9,589 provider usage tokens；静态环境发现任务则共三次 committed activation、一次 execution、14,134 tokens。两者是不同任务，不能作成本比较。高并发压力、cancel/shutdown 组合及严格配对成本比较仍需实验判定。

**关键词：** Agent runtime；事件驱动架构；有界队列；背压；持久 worker；可观察性；工具执行

## 1. 引言

常见 Agent harness 以“模型请求—工具调用—工具结果—模型请求”作为控制骨架。这种结构容易理解，但异步运行时信号通常被折叠进工具返回值：worker 重启、完成回调、用户中途发言、丢失结果或容量拒绝，未必能在同一套事件语义中被准确表达。

Nervipulsa 采用事件总线连接 CLI、LLM actor、Python host 与持久 Python worker。研究问题是：能否在这一事件主干上加入 worker 侧完成 handler，同时维持以下性质：

1. 每个已接纳执行都有可用的终态投递位置。
2. handler 扇出受每执行和全局容量约束。
3. 结果关联到正确的执行与 worker epoch，且顺序可解释。
4. 取消、超时、重启或投递失败可以显式表示，而非静默丢失。
5. 模型实际收到的观察与 journal 中的事件能够相互核对。

本文记录当前实现、实验和未决设计，不把原型结果外推为生产级可靠性结论。

## 2. 系统模型

### 2.1 组件与事件路径

系统由 CLI、`LLMActor`、`Bus`/`Mailbox`、`PythonHost` 和持久 worker 构成。模型发出 `python_exec` 后，actor 发布 `python.requested`。Host 串行消费请求并向 worker 发送执行帧。Worker 执行用户代码、冻结该执行对应的 handler ID 快照、运行回调并返回结果帧。Host 校验 request ID、worker epoch、触发 request ID 和 handler 唯一性，再发布 `python.finished` 与 `agent.handler_fired`。Actor 将相关观察合并到 activation 输入，按全局 `seq` 排序并投影到模型上下文。

Worker 的公开 API 当前窄化为 `on_finished(callback)` 与 `off_finished(handle)`。回调接收执行观察字典；注册会跨同一 worker epoch 的后续执行保留。每个 epoch 最多允许 16 个活动 handler。执行结束时使用冻结快照，因此执行期间的注册变更不会篡改当前执行已确定的回调集合。

### 2.2 有界投递协议

每个 Python 请求在 admission 阶段同时预留一个终态位置和最多 16 个 handler 结果位置。Mailbox 另设全局 handler 槽位上限，当前配置关系为 `handler_result_limit × result_limit`。handler 事件到达后占用一个槽位；即使已从邮箱取出、仍处于 actor 合并或 activation 批次中，该槽位仍计入占用，直至 `mark_consumed`。

当 `python.finished` 到达时，未使用的 handler 预留按 worker 报告的预期数量收缩；终态被消费后关闭尚未填充的位置。已接纳的 handler 结果继续占用各自槽位，直到消费。handler 专用 lane 拒绝或不可用时，Host 可尝试 ordinary fallback，但 fallback 仍需消耗同一执行的预留槽位。终态 lane 独立于 handler 扇出，避免普通消息或 handler 压力挤掉执行完成记录。

终态携带 `expected_handler_count`、handler 结果状态和 `missing_handler_ids`。已知预期数量为零时，actor 不等待；数量为正时等待匹配结果到齐或达到 25 ms 上限。历史终态缺少计数字段时保留有界 grace period。合并期间取出的其他事件仍按全局序号排序，因此用户消息不会被相关 handler 事件越过。

### 2.3 失败与生命周期

Worker epoch 用于拒绝重启前的旧 handler 帧。timeout/cancel 若发生在有效终态快照之前，会丢弃部分 handler 缓冲并使已知 handler 在终态中显示为缺失；worker 崩溃且快照未知时，状态标记为 unknown。Mailbox close 清理 queued、leased 状态及终态和 handler 预留。handler 专用与 ordinary fallback 均拒绝时，Host 按实际接受的 handler ID 构造 incomplete terminal；确定性测试已验证 missing 状态进入 provider 请求，但真实模型如何理解该状态尚未做体验验证。

## 3. 研究方法与证据边界

本文综合四类证据：

- **源码审查：** 检查 admission、reservation rollback、lane key 校验、handler 槽位账本、终态收缩、Host frame 校验、epoch 清理和 coalescer 投影路径。
- **外部文献审阅：** 对比 LLMCompiler、PASTE、AsyncTool 与 AsyncLLM 的并行位置、评测变量和适用边界；这些研究的性能数据未外推为本项目结果。
- **自动化测试：** 使用确定性单元与 subprocess 测试检查容量、重复和错误关联、回调快照、生命周期与排序等行为。2026-10-05 最近一次 `python -m pytest` 得到 77 passed、1 skipped，用时 47.32 秒；skip 是 Windows junction 集成用例，命令因 `'chcp' is not recognized` 失败，不能计为链接 containment 实机通过。
- **本地模型 CLI 观察：** 此前试验验证了 nonce、多 handler、跨执行注册和 epoch 重启行为，也观察到冗余调用及过早宣称成功。2026-10-05 当前版本在 3425 gateway 上完成一次安全 clamp 任务；handler 结果进入模型第二次 activation 并被最终答复正确使用。另保留 8317 配置导致的连接拒绝记录。静态环境发现另有独立本地 CLI 任务验证：事件日志显示环境发现先于 Python 请求，`widgetkit` 版本和 API 发现结果进入模型，随后 smoke test 成功。上述 live 样本只证明各自单次行为，不代表压力、普遍可靠性或成本优势。

实验并非随机化对照研究。曾有一次无 handler 控制与一次 nonce handler 任务都使用两次 committed activation；原始 token 数分别为 7,842 和 8,224，但提示词和生成代码不同，不能据此估计 handler 的因果成本。当前数据只支持提出后续配对实验，不支持性能优势结论。

## 4. 结果

### 4.1 已验证的运行行为

此前 live CLI 试验显示，handler 产生的随机 nonce 可以在不通过普通 stdout 暴露的情况下回到模型上下文。多 handler 试验中，两个不可预测 nonce 均被准确复述，journal 记录 `python.finished` 和对应 `agent.handler_fired`。同一 worker epoch 跨两次执行保留的注册可分别触发；timeout/restart 后，旧 epoch 帧被拒绝，新 epoch 注册正常路由。

**当前版本 3425 CLI 复验（2026-10-05）。** 进程级 overrides 使用 `http://127.0.0.1:3425/v1`、已配置模型 `deepseek-flash` 和已配置 API key；key 未输出或写入文件。隔离任务要求创建并运行 `clamp_module.py`，验证三项边界断言并输出 marker，同时尝试注册 `on_finished` callback。模型生成的 Python 代码正确调用一参数 `on_finished`，没有定义本地替代函数。Journal 事件序列为 `user.message`、`python.requested`、`python.started`、`python.finished`、`agent.handler_fired`、`assistant.message`。execution 成功，耗时 332 ms，marker 出现在 stdout；handler 预期数和到达数均为 1，结果表明状态为 succeeded 且 stdout 包含 marker。第二个 committed activation 同时消费 terminal 和 handler 两个事件，最终答复正确复述三项断言和 handler observation。这验证了当前代码下单任务的真实模型理解、执行、handler 路由和模型可见性。

本轮另有配置 URL 仍指向 8317 的尝试，journal 记录 `WinError 10061`、paused activation 和零次执行；它与此前已记录的首次连接拒绝均保留为失败证据，不能混同为 3425 成功运行。Windows 捕获子进程 stdout 时出现 GBK decode 异常，但 CLI 进程退出码为 0，最终 assistant.message 与完整事件序列从 SQLite journal 读取并核对。该输出捕获限制不影响对 journal 证据的确认。

另有受控试验在 Python 执行期间送入第二条用户消息：首个执行完成后，该消息得到处理，但模型随后产生一次不必要的 no-op Python 调用。较早一次较少约束的模型运行曾改错函数名、在结果返回前宣称成功，随后才根据失败观察纠正。这些现象提示 harness 的事件正确性不能替代模型行为评估。

### 4.2 当前容量与关联审计

源码和回归用例表明，admission 在下游容量检查与请求发布失败时回滚暂存预留和临时序号；terminal 的 `reply_to` 必须与预留 lane key 相符。审查发现过一个错 key 风险：终态曾可能按错误关联键调整另一执行的 handler 预算。现在该校验先于账本调整，并有回归测试覆盖。

全量回归最初为 58 passed、1 failed。失败夹具直接投递 handler event，却没有按生产 admission 契约预留 handler slot；Mailbox 正确拒绝了投递。修正夹具后，全量结果为 59 passed。该失败属于测试前置条件错误，不是放宽容量规则后消失的生产缺陷。

### 4.3 仍未证明的性质

当前测试证明配置范围内的账本转移和若干生命周期路径，不等于证明高并发下的容量行为。尚未进行接近全局槽位上限的并发压力实验，也未进行 cancel/restart/shutdown 组合负载测试。Actor coalescer 现在按唯一 handler ID 判断预期结果是否到齐；重复 ID 不会提前满足数量门槛，且所有事件仍保留在有序观察批次中。第三方 producer 的不同关联契约仍未测试。

Host UI 错误不一定单独投影为模型消息，但 incomplete terminal 含 `missing_handler_ids`，确定性测试已证明 provider 请求收到该状态。仍未通过真实模型体验确认模型是否会正确理解并据此改变行为。

### 4.4 环境发现期间的 event-loop 响应性

源码审计发现，`python_environment` 的路径与 metadata/AST 扫描原本在工具派发函数内同步运行，会占住 `LLMActor` 所在线程。实现现已将扫描放入 `asyncio.to_thread`，并沿提交链路逐层 await；事件发布、activation 关联与失败回执形状保持不变。确定性回归用阻塞扫描函数制造可控等待，断言期间 loop callback 能运行，第二条 `user.message` 被接受，扫描释放后发现事件与该消息共同进入后续 provider 请求。该测试证明 loop 可调度性和消息入队，不证明 actor 在首个 activation 结束前并行推理或模型能同时处理两个任务。最新全量测试为 77 passed、1 skipped；没有进行 live CLI 或负载下性能比较。

## 2026-10-13 补充：有限进程终止证据

本轮在固定生产源码基线 `41c83d0c3f2fbc81d49993072cbfa585d7047742` 上新增研究测试，没有修改 runtime 生产源码。父进程启动独立 Python 子进程运行 Runtime、PythonHost 和 SQLite Journal，在两个明确的 mailbox gate 上记录 Journal 与 effect marker，先显式 flush Journal，再强制终止 Runtime 子进程树；随后以相同 workspace 和 Journal 启动新的 OS 进程及 Runtime。

1. **请求已接纳、host 尚未 dequeue：**旧 session 的 `python.requested` 行已出现在 Journal，marker 尚不存在。新 Runtime 使用不同 session，不重放旧 request 或 terminal，marker 仍不存在。
2. **终态已入队、actor 尚未消费：**worker 已写入 marker，`python.finished` 已排入 actor mailbox 且写入 Journal。终止后新 Runtime 不重放旧 request 或 terminal，marker 保持存在。

这说明 Journal 行在这两个测试窗口中是观察记录，不是启动时的重放指令。测试验证的是**预先 flush 后的 Windows 进程终止**，不是断电、SQLite/WAL 写入中断或存储设备故障；marker 是本地文件 oracle，不代表远程副作用。结果也不证明任意 crash window 都没有 replay、自动恢复、exactly-once 或 retry safety。

最终 cleanup revision 的定向测试为 **2 passed in 7.49s**，全量套件为 **124 passed in 79.57s**。相关套件的一次早期运行有 1 项 `wait_until_idle(8)` 超时（66 passed、1 failed）；该项单独重跑通过，相关套件随后通过（67 passed in 132.35s）。这些数据来自研究 worktree；相关套件计时早于最后的 PID cleanup 收紧，全量与定向结果则是在 cleanup 收紧后取得。完整故障矩阵、命令与 JSON 记录见 [Failure Semantics Baseline](failure_semantics.md)、[`failure_baseline.json`](../benchmarks/failure_baseline.json) 和 [`test_subprocess_recovery.py`](../tests/test_subprocess_recovery.py)。


### 4.6 Durable Effect Protocol

提交 `6484416` 加入 per-execution `durable_effects` ledger。Host 在写 IPC execute frame 前先同步提交 `intent` 和 `dispatching`；dispatching commit 失败时不会发送 frame。收到 worker terminal 后，Host 先提交 `observed`，再发布 `python.finished`。若 observed commit 失败，Host 发出 `effect_state=unknown`；即使后续 unknown 写入失败，旧的 `dispatching` 行仍可由 `/effects` 发现。

真实 SQLite commit 故障注入验证了 intent commit 回滚后无记录、dispatch commit 回滚后保留 intent、observed commit 回滚后保留 dispatching；Host 集成测试检查 dispatch fail-closed 和 observed failure 的 unknown terminal。该协议把副作用结果不确定性持久化供人工审阅；它不把 Python 副作用与 SQLite 做原子事务，不恢复 transcript，不自动重放，也不提供 exactly-once 或 power-loss 保证。状态机和提交故障证据见 [Durable Effect Protocol](durable_effect_protocol.md)。

2026-10-14 的 T0–T4 门控进程终止实验进一步覆盖了 intent commit 后、dispatching commit 后、side effect 后、Host 收到 terminal 后但 observed commit 前，以及 observed commit 后 terminal 已入队但 actor 尚未消费五个窗口。重启后 `/effects` 对前四种未观察状态保留 unresolved 项；T4 的 effect row 为 observed，但新 session 不会恢复旧 terminal 或 transcript。五个定向进程测试通过；这些证据不覆盖 commit 进行中、WAL 中断或断电恢复。完整状态矩阵与范围见 [Durable Effect Protocol](durable_effect_protocol.md) 和 [`test_crash_boundaries.py`](../tests/test_crash_boundaries.py)。

### 4.7 Code-based first-use user experience review

README 与 CLI 源码走查显示，当前 CLI 能报告 execution 状态、耗时、输出预览和 context 用量，并提供 `/status`、`/logs`、`/output`、`/retry`、`/cancel` 与 `/effects` 等后续入口。第一次使用的主要摩擦在 provider/model/gateway 配置分散、执行前代码预览有长度上限，以及不同失败状态需要用户自行选恢复命令。Python 使用当前 OS 用户权限，`--dir` 不是 sandbox；因此这是一项用户信任与操作边界问题，而不只是输出布局问题。

此评估仅由文档与源码推导，没有运行当前版本 provider CLI、观察首次使用者或验证模型对 `unknown` 的处理。具体旅程、证据路径和改进建议见 [用户体验评估](user_experience_review.md)。该审查不新增 live-run 或用户成功率证据。

## 5. 讨论

### 5.1 架构判断

目前较有证据支持的选择是保留窄的完成 handler bridge，并使用每执行预期数、有限 handler 槽位、独立终态预留和显式 incomplete/unknown 状态。每个成功投递的 handler 仍保留独立 journal 事件；这样既能审计单条结果，也避免无界 fan-out 挤占普通事件与终态容量。

现阶段没有足够证据把 API 扩展成任意命名事件订阅。通用订阅会新增注册替换、取消、事件过滤、执行间状态、跨 epoch 重建和可观测性规则；这些复杂度应由具体用例与配对实验驱动。若 handler 不能减少模型激活、不能改善异步信息处理，或其失败状态无法可靠传达，那么扩展 API 的收益就需要重新论证。

### 5.2 成本与模型行为

单次 Python 周期的 handler 与无 handler 控制都观察到两次 activation，因此当前 hook 没有显示出降低模型调用次数的效果。它的潜在价值在于让 worker 自主产生结构化观察、关联多条异步结果，而非已证实的 token 或延迟节省。需要用同一任务、相同模型设置、随机化运行顺序和重复样本进行有/无 handler 配对；同时报告 activation 数、provider 请求数、端到端延迟、输入/输出 token、失败率和冗余工具调用。

### 5.3 延迟与完整性取舍

25 ms coalescer 上限是当前实现的有界等待策略，并非由实测用户体验确定的普适最优值。已知 handler 数使 actor 可在结果齐全时提前结束等待；若结果迟到，终态仍能表达缺失。后续应测量 handler 到达延迟分布、等待带来的端到端增量，以及缺失观察对最终任务质量的影响，再决定是否调整该上限或改为更明确的批次关闭协议。

### 5.4 相关工作与迁移边界

[LLMCompiler（ICML 2024）](https://arxiv.org/abs/2312.04511) 将单个请求中的可并行函数调用编译为依赖 DAG；论文报告最高 3.7× 延迟加速和 6.7× 成本节省。它要求规划器识别依赖，解决的是计划内工具并行，不是持续输入、worker 生命周期或 event mailbox 正确性。

[PASTE](https://arxiv.org/abs/2603.18897v3) 从重复工具模式预测后续调用并隔离推测结果。当前 v3 摘要报告任务完成时间降低 43.5%、观察到的工具延迟降低 1.8×；早期 v1 摘要曾报告不同的 48.5% 和吞吐口径。该版本差异说明引用性能数字必须固定论文版本。推测执行依赖工作负载模式，并引入错误预测和有副作用工具的风险；Nervipulsa 当前没有采用，也没有相应收益证据。

[AsyncTool](https://arxiv.org/abs/2605.27995v3) 通过模拟工具延迟和多任务交错，评估模型的任务切换、依赖跟踪与状态维护。它适合启发 Nervipulsa 后续设计真实延迟测试，但其模型分数不是 mailbox、取消、迟到帧或 handler 结果语义的系统级证明。

[LLMs are General Asynchronous Agents](https://arxiv.org/abs/2609.35427v1) 以 asyncio coroutine、共享 KV/cache blocks 和不同可见性 view 组织并行模型推理，并展示流式视频、游戏与系统监控场景。它讨论用户打断/steering，但不是持久 Python worker、事件日志恢复或工具结果去重的验证。该工作也明确指出当前模型异步操作尚不可靠。

这些论文提供三种不同的并发位置：工具 DAG 并行、推测工具执行、多个 LLM coroutine；而 Nervipulsa 当前已实现的是异步事件路由与有界 worker 结果回传。现有代码优化只确保同步静态扫描不会堵住 asyncio loop，不会让正在运行的 activation 被打断，也不会证明多任务吞吐提升。

2026-10-16 的执行语义对照进一步收窄了相关工作边界。[Pollard](https://arxiv.org/abs/2610.16145v1) 用父步骤、kind、attempt 和规范化输入绑定语义 step，并在严格重放时返回经校验的结果而不调用 wrapped operation；调用方仍选择步骤边界，且旧结果不会恢复变化后的外部世界。[KedgeFlow](https://arxiv.org/abs/2610.14810v1) 约束精确动作授权、单次 allowance、dispatch 与目标结果；lost-response 后保留已消费额度，但其评估在单机 etcd 与 mock target 上，论文明确未测试 native crash recovery 或真实目标 exactly-once。两者都不能直接替代本项目的 T0–T4 host-process 实测，也没有证明完整 agent session 恢复。

[Temporal 的 Event History](https://docs.temporal.io/encyclopedia/event-history.md) 展示了更完整的 workflow 控制状态恢复：worker 重启后 replay workflow code，已完成 Activity 取历史结果；未能报告完成的 Activity 仍可能重试，因此官方要求 effectful Activity 使用目标端幂等契约。[Transactional Outbox](https://microservices.io/patterns/data/transactional-outbox.html) 原子提交的是本地业务更新与 outbox message，relay 仍可能重复发布。它们分别解决任务历史与本地事务/消息一致性，均不等同于 Nervipulsa 当前只记录 Python execution 效果状态的 ledger。[Many Steps into One](https://arxiv.org/abs/2610.17544v1) 讨论可复用 reasoning memory 的 stale-answer 风险；除非 runtime 复用摘要或推理结果，否则与副作用恢复仅间接相关。SciXa 的 [durable execution 综合文章](https://scixa.com/article?slug=durable-execution-substrates)可作为术语检查表，本轮未能从页面提取验证作者、日期、文章类型或所称来源数量，不作为实验依据。

本地 `/effects` 的源码检查还发现，当前 CLI 只有列出 unresolved rows 的只读路径，没有记录目标端证据或操作者判定的受支持命令/API。因此现有能力应称为“人工可见的 unresolved 状态”，尚不是可闭环的人工 reconciliation workflow。详细引用、证据等级与保证矩阵见[2026-10-16 文献筛选记录](literature_screening_2026-10-16.md)。

## 6. 结论

Nervipulsa 已从单纯的工具调用循环扩展出可观察的事件执行主干，并实现 handler 结果的初步有界投递协议。历史 live CLI 试验提供了多个 handler 进入模型上下文的真实证据；2026-10-05 的 3425 CLI 复验验证了模型理解 `on_finished`、执行 Python 任务，并在后续 activation 中使用 terminal 与 handler 观察。静态环境发现任务也完成先发现后 smoke test；event-loop 回归验证扫描期间 loop 能调度并接收后续输入。2026-10-13 fault-semantics 基线分支全量套件为 124 passed；其两个进程终止探针只覆盖预先 flush Journal 后的门控窗口。随后合入的 Durable Effect Protocol 增加每次 Python 执行的 `intent → dispatching → observed | unknown` ledger，并通过真实 SQLite commit 故障注入验证失败边界。合并两条分支并修正 worker-exit 回归断言后，`python -m pytest -q` 为 **137 passed in 102.03s**。这些结果不证明并发压力下的容量行为、WAL 写入中断、断电恢复、通用任务恢复或 exactly-once 外部效果。

因此，当前结论仍是“已获得单轮当前版本 live 证据、可继续验证的事件原生 harness 原型”，不是“已被证明优于普通 harness 的新架构”。T0–T4 的本地进程终止实验覆盖了 Durable Effect Protocol 的五个已提交状态边界，并显示 `observed` 持久化不恢复 actor transcript；该测试目前仍是本地未提交工作树证据，尚不在 `origin/main`。文献对照也表明，durable workflow、outbox、目标端幂等、语义重放与授权控制面记录的是不同对象，不能由“存在 ledger”推导出任务恢复或 exactly-once。后续优先补齐 `/effects` 之后可审计的人工 resolution 路径，再用真实本地编码任务检验 unknown 是否帮助用户避免危险重试，以及 A2 handler 相对 A1 是否改善任务质量或错误可见性。当前进程测试不覆盖 WAL 写入中断、断电、设备故障，也不证明 exactly-once。

## 参考材料

1. Nervipulsa 项目研究日志：[runtime_harness_architecture_research.md](runtime_harness_architecture_research.md)。实现细节与实验记录来自项目源码审查、测试和历史 CLI 观察。
2. Kim et al., “An LLM Compiler for Parallel Function Calling,” ICML 2024, [arXiv:2312.04511](https://arxiv.org/abs/2312.04511)。
3. “Act While Thinking: Accelerating LLM Agents via Pattern-Aware Speculative Tool Execution,” [arXiv:2603.18897 v3](https://arxiv.org/abs/2603.18897v3)。该预印本不同版本的摘要报告口径有变化，正文讨论采用明确标注版本的 v3 数值。
4. “AsyncTool: Evaluating the Asynchronous Function Calling Capability under Multi-Task Scenarios,” [arXiv:2605.27995 v3](https://arxiv.org/abs/2605.27995v3)。
5. “LLMs are General Asynchronous Agents,” [arXiv:2609.35427 v1](https://arxiv.org/abs/2609.35427v1)。
6. 历史验证：`python -m pytest`，77 passed、1 skipped、47.32 s（2026-10-05；junction skip 原因见研究日志）。
7. 2026-10-13 fault-semantics 专项报告：[failure_semantics.md](failure_semantics.md)，原始记录见 [`failure_baseline.json`](../benchmarks/failure_baseline.json)。cleanup revision 全量命令 `python -m pytest -q` 为 124 passed in 79.57s。
8. 当前 effect ledger 状态与失败注入：[durable_effect_protocol.md](durable_effect_protocol.md)；实现提交 `6484416`。
9. 代码走查得出的首次使用旅程与体验摩擦：[user_experience_review.md](user_experience_review.md)；未进行当前版本 live provider 运行或首次使用者研究。
10. Muntaser Syed、Marius C. Silaghi, “Pollard: Governed Semantic Execution Trees for Framework-Independent AI Agent Control and Replay,” [arXiv:2610.16145v1](https://arxiv.org/abs/2610.16145v1), 2026-10-12。
11. Jun He、Deying Yu, “KedgeFlow: A Programmable Control Plane for Enterprise AI Agents,” [arXiv:2610.14810v1](https://arxiv.org/abs/2610.14810v1), 2026-10-10。
12. Temporal 官方文档：[Event History](https://docs.temporal.io/encyclopedia/event-history.md)、[Workflow Activity](https://docs.temporal.io/workflow-activity.md)、[Activity Definition](https://docs.temporal.io/activity-definition.md)；文档访问日期 2026-10-16。
13. Chris Richardson, [Pattern: Transactional Outbox](https://microservices.io/patterns/data/transactional-outbox.html)；页面访问日期 2026-10-16。
14. Jianghong Zhou、Hao Rong、Chen Lin、Daoan Zhang, “Many Steps into One: Reasoning Contraction and the Limits of Safe Memory Reuse for LLM Agents,” [arXiv:2610.17544v1](https://arxiv.org/abs/2610.17544v1), 2026-10-13。
15. SciXa, [Durable Execution Substrates for Interruption-Tolerant AI Agents](https://scixa.com/article?slug=durable-execution-substrates)；页面作者、日期和来源数量本轮未核实，作为二手背景材料而非实验依据。

## 后续实验清单

1. 对当前代码版本运行本地 gateway CLI live test，覆盖单 handler、多 handler、用户中途发言、超时及重启。
2. 在全局 handler slot ceiling 附近并发 admission，验证拒绝、释放和 close 后账本归零。
3. 组合测试 cancel、timeout、restart、shutdown 与迟到/部分 handler 结果。
4. 验证 handler 双路径拒绝时，模型是否得到可行动的 incomplete/error observation。
5. 运行严格配对 benchmark，测量 activation、provider 请求、延迟、token 和任务正确率。
6. 以用例和测量结果决定是否需要通用命名事件订阅 API。
7. 2026-10-16 已完成 durable workflow、transactional outbox、幂等执行、Pollard、KedgeFlow 与本地 effect ledger 的语义对照；结论见[文献筛选记录](literature_screening_2026-10-16.md)。后续设计并验证 `/effects` 之后的可审计人工 reconciliation 路径，避免把未知结果自动重放。
8. 进行当前版本首次使用者观察：从干净配置完成模型请求和文件修改任务，记录配置耗时、完成耗时、困惑点与失败后的恢复步骤；区分源代码推断、live CLI 观察与真实用户证据。
9. 用真实编码任务比较 A1 与 A2，测量任务质量、错误可见性、危险重试、人工接管和 token/延迟成本；脚本延迟差不能代替这些指标。

### 2026-10-05 补记：静态 Python 环境发现

较早的环境发现 CLI 样本使用临时 `widgetkit` 版本 `0.3.1`，先静态发现、后显式 import 与 smoke test；该记录保留为早期功能证据。当前最终静态实现的复验使用 `widgetkit` 版本 `0.4.2`，任务要求先调用 `python_environment`，再执行 `python_exec` 验证 `scale(3) == 9`，不安装任何包。

本轮 CLI 退出码 0。SQLite journal 的 7 个事件显示 `python.environment_discovered` 先于 `python.requested`；发现结果为 succeeded、版本 `0.4.2`、`api.scale=true`、`install_supported=false`。之后 Python execution succeeded，耗时 15 ms。共 3 次 committed activation、2 次工具调用，provider usage 合计 14,134 tokens。模型最终复述了发现结果与 smoke test 成功。该现场结果仅说明这一模型、本机环境和单个自定义库任务的行为。

自动化结果为 76 passed、1 skipped；skip 是 junction 集成命令在当前 Windows runner 中报 `'chcp' is not recognized`。静态路径隔离有确定性测试，但 OS 级 junction 行为尚无证据。全量测试和 live run 都不能替代真实并发容量压力、cancel/restart/shutdown 组合负载和严格配对成本实验。

## 7. 2026-10-06 — Long-running task protocols and recovery

### 7.1 Related runtime contracts

The stable `2026-07-28` [MCP Tasks extension](https://github.com/modelcontextprotocol/ext-tasks) returns a durable handle for long-running work. It requires task creation to be durable before the response, uses explicit task states, supports deferred result retrieval and input-required transitions, and defines cooperative cancellation. This is an MCP extension with capability negotiation, not a core MCP default. Its polling model addresses client disconnects; it does not replace Nervipulsa's in-process event delivery.

The latest released [A2A specification](https://a2a-protocol.org/latest/specification/) is `1.0.0` and exposes task resources across independent agents: a request may return a direct message or a task, with Get/List/Cancel and optional streaming or push updates. This is a useful external interoperability boundary, but it does not specify Nervipulsa's worker IPC, mailbox capacity, or callback reservation semantics.

[Google AIP-151](https://google.aip.dev/151) offers a uniform long-running operation resource with typed metadata and results. It distinguishes failure before an operation starts from failure after it starts. Its 10-second figure is a heuristic, not a universal cutoff.

[Temporal Activity guidance](https://docs.temporal.io/activities) makes the retry hazard explicit: a failed attempt starts over unless a heartbeat checkpoint is provided, and idempotent activity code is recommended because external side effects can be repeated. This is directly relevant to Python code that may write files or launch subprocesses. A timeout or lost terminal cannot show whether a side effect occurred.

[AgentRewind](https://arxiv.org/abs/2608.14380) explores agent-level rollback by aligning saved model context with controlled environment state, then injecting memory from the abandoned trajectory. Its benchmark concerns long-horizon engineering tasks. This differs from replaying an event journal: a log records observations, while rewind must restore the corresponding environment too.

### 7.2 Current guarantee and design implication

In Nervipulsa, `python.requested` is accepted after an in-process mailbox offer and capacity reservation. The observer then submits the event to a bounded asynchronous journal queue. That queue can drop records when full, and SQLite writer failures mark the journal incomplete. No durable task row is committed before the tool acceptance receipt. Thus current acceptance is process-local and volatile; journal persistence is best-effort observation, not durable admission or crash recovery.

The present `execution_id` is an event identifier for one execution attempt in one runtime session. If durable jobs are introduced, the design should separate a stable logical `task_id` from each attempt's `execution_id` and the worker's `worker_epoch`. A process restart during an arbitrary Python call must not trigger blind automatic replay: code may have performed external effects before its terminal event was persisted or consumed. The runtime must either provide an idempotency contract, restore a controlled workspace checkpoint, or report an unknown outcome requiring reconciliation. Event correlation alone cannot provide exactly-once effects.

For the current local CLI, retain event push and the bounded terminal/handler reservations. Add Get/List-style task operations only if clients need to disconnect and resume, or if execution outlives the local session. Before that implementation, define immutable terminal states, cancellation-requested versus confirmed-stopped, attempt history, deadlines, retention, and authorization. Keep context checkpoints and workspace snapshots aligned if rewind is supported.

Operational visibility can improve without changing execution semantics: `/status` now reports the journal drop count and a bounded writer-error excerpt when the journal is incomplete. Those fields diagnose missing audit observations; they do not change whether a Python task was admitted, delivered, or recoverable. This source change has not yet been tested in this slice.

### 7.3 Updated research question

The next recovery study should inject failure at each boundary: after mailbox admission but before journal flush; after worker side effects but before terminal emission; after terminal emission but before actor consumption; and during cancellation/worker termination. Measure whether status is recoverable, duplicate side effects occur, the worker epoch changes, and the model sees a truthful terminal state. Keep this separate from the existing mailbox-capacity study: durable task recovery is a new layer and a different guarantee.
