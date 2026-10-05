# Nervipulsa 的事件原生 Agent Runtime：有界 Handler 投递与生命周期语义

**研究稿（持续研究中）**  
**版本日期：** 2026-10-05（Asia/Shanghai）  
**项目：** Nervipulsa

## 摘要

基于工具调用循环构建的 Agent harness，通常把工具执行结果视为唯一的运行时反馈。本文研究一种事件原生扩展：持久 Python worker 可注册完成回调，回调结果作为独立事件进入 agent 的观察流；事件总线、邮箱和执行生命周期负责关联、排序、容量控制与审计。研究重点不是证明回调 API 可运行，而是确定在并发、背压、超时和 worker 重启下，回调结果能否以有限资源进入模型上下文，同时保留可解释的失败语义。

Nervipulsa 当前实现采用每次 Python 请求的终态预留、最多 16 个 handler 结果槽位、全局有界 handler 容量、终态报告预期与缺失结果，以及按事件序号投影观察的 coalescer。源码审查与自动化测试覆盖这些主要账本路径；静态 `python_environment` 以受限路径、发行版 metadata 和 AST 线索发现自定义库，不导入代码、不自动安装。本文新增一次 event-loop 阻塞回归：环境扫描在线程执行期间，runtime loop 仍可接受后续用户消息，扫描结果随后进入模型上下文。最新全量回归为 77 passed、1 skipped。该实现已有本地 CLI 先发现后 smoke test 的行为证据，但尚无并发负载或跨进程恢复证据。

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

## 6. 结论

Nervipulsa 已从单纯的工具调用循环扩展出可观察的事件执行主干，并实现了 handler 结果的初步有界投递协议。历史 live CLI 试验提供了多个 handler 进入模型上下文的真实证据；2026-10-05 的当前版本 3425 CLI 复验进一步验证了模型理解 `on_finished`、成功执行安全 Python 任务、terminal 与 handler 观察进入同一后续 activation，并被最终回答正确使用。当前静态环境发现版本也在本地 CLI 中完成 `widgetkit` 的先发现后 smoke test 流程；最新异步扫描改动由阻塞回归验证 event loop 可响应并接收后续输入。当前源码审计与 77 passed、1 skipped 的全量测试支持主要容量、关联、回滚、排序、发现结果回执和 event-loop 非阻塞路径；OS junction containment 集成仍因 runner 命令问题跳过。

因此，当前结论是“已获得单轮当前版本 live 证据、可继续验证的事件原生 harness 原型”，而不是“已被证明更令人满意的新架构”。下一阶段应优先完成并发容量与生命周期压力测试、缺失/拒绝结果对模型可见性的验证，以及有/无 handler 的配对基准。完成这些实验后，再决定保留窄 bridge、扩展为通用订阅，或移除该机制。

## 参考材料

1. Nervipulsa 项目研究日志：[runtime_harness_architecture_research.md](runtime_harness_architecture_research.md)。实现细节与实验记录来自项目源码审查、测试和历史 CLI 观察。
2. Kim et al., “An LLM Compiler for Parallel Function Calling,” ICML 2024, [arXiv:2312.04511](https://arxiv.org/abs/2312.04511)。
3. “Act While Thinking: Accelerating LLM Agents via Pattern-Aware Speculative Tool Execution,” [arXiv:2603.18897 v3](https://arxiv.org/abs/2603.18897v3)。该预印本不同版本的摘要报告口径有变化，正文讨论采用明确标注版本的 v3 数值。
4. “AsyncTool: Evaluating the Asynchronous Function Calling Capability under Multi-Task Scenarios,” [arXiv:2605.27995 v3](https://arxiv.org/abs/2605.27995v3)。
5. “LLMs are General Asynchronous Agents,” [arXiv:2609.35427 v1](https://arxiv.org/abs/2609.35427v1)。
6. 最新全量验证：`python -m pytest`，77 passed、1 skipped、47.32 s（2026-10-05；junction skip 原因见研究日志）。

## 后续实验清单

1. 对当前代码版本运行本地 gateway CLI live test，覆盖单 handler、多 handler、用户中途发言、超时及重启。
2. 在全局 handler slot ceiling 附近并发 admission，验证拒绝、释放和 close 后账本归零。
3. 组合测试 cancel、timeout、restart、shutdown 与迟到/部分 handler 结果。
4. 验证 handler 双路径拒绝时，模型是否得到可行动的 incomplete/error observation。
5. 运行严格配对 benchmark，测量 activation、provider 请求、延迟、token 和任务正确率。
6. 以用例和测量结果决定是否需要通用命名事件订阅 API。

### 2026-10-05 补记：静态 Python 环境发现

较早的环境发现 CLI 样本使用临时 `widgetkit` 版本 `0.3.1`，先静态发现、后显式 import 与 smoke test；该记录保留为早期功能证据。当前最终静态实现的复验使用 `widgetkit` 版本 `0.4.2`，任务要求先调用 `python_environment`，再执行 `python_exec` 验证 `scale(3) == 9`，不安装任何包。

本轮 CLI 退出码 0。SQLite journal 的 7 个事件显示 `python.environment_discovered` 先于 `python.requested`；发现结果为 succeeded、版本 `0.4.2`、`api.scale=true`、`install_supported=false`。之后 Python execution succeeded，耗时 15 ms。共 3 次 committed activation、2 次工具调用，provider usage 合计 14,134 tokens。模型最终复述了发现结果与 smoke test 成功。该现场结果仅说明这一模型、本机环境和单个自定义库任务的行为。

自动化结果为 76 passed、1 skipped；skip 是 junction 集成命令在当前 Windows runner 中报 `'chcp' is not recognized`。静态路径隔离有确定性测试，但 OS 级 junction 行为尚无证据。全量测试和 live run 都不能替代真实并发容量压力、cancel/restart/shutdown 组合负载和严格配对成本实验。
