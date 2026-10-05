# Nervipulsa 的事件原生 Agent Runtime：有界 Handler 投递与生命周期语义

**研究稿（持续研究中）**  
**版本日期：** 2026-10-05（Asia/Shanghai）  
**项目：** Nervipulsa

## 摘要

基于工具调用循环构建的 Agent harness，通常把工具执行结果视为唯一的运行时反馈。本文研究一种事件原生扩展：持久 Python worker 可注册完成回调，回调结果作为独立事件进入 agent 的观察流；事件总线、邮箱和执行生命周期负责关联、排序、容量控制与审计。研究重点不是证明回调 API 可运行，而是确定在并发、背压、超时和 worker 重启下，回调结果能否以有限资源进入模型上下文，同时保留可解释的失败语义。

Nervipulsa 当前实现采用每次 Python 请求的终态预留、最多 16 个 handler 结果槽位、全局有界 handler 容量、终态报告预期与缺失结果，以及按事件序号投影观察的 coalescer。源码审查与全量自动化测试验证了这些主要账本路径；最近一次全量回归为 59 项通过。此前的本地模型 CLI 试验证明了单个和多个 handler 结果可以到达模型上下文，也暴露了冗余工具调用、过早报告成功及 token 成本对照不充分等问题。

当前证据支持将其视为一个有真实端到端行为、具备初步有界投递契约的研究原型。它还不足以证明该桥接在成本、稳定性或通用性上优于普通工具调用循环。高并发压力、当前版本的本地 gateway 体验、失败信号到模型的保证，以及通用事件订阅 API 的必要性，仍需实验判定。

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

Worker epoch 用于拒绝重启前的旧 handler 帧。timeout/cancel 若发生在有效终态快照之前，会丢弃部分 handler 缓冲并使已知 handler 在终态中显示为缺失；worker 崩溃且快照未知时，状态标记为 unknown。Mailbox close 清理 queued、leased 状态及终态和 handler 预留。handler 专用与 ordinary fallback 均失败时，Host 记下未投递结果并发出 UI 错误；但当前实现并不保证该 UI 错误一定进入 LLM 上下文。

## 3. 研究方法与证据边界

本文综合三类证据：

- **源码审查：** 检查 admission、reservation rollback、lane key 校验、handler 槽位账本、终态收缩、Host frame 校验、epoch 清理和 coalescer 投影路径。
- **自动化测试：** 使用确定性单元与 subprocess 测试检查容量、重复和错误关联、回调快照、生命周期与排序等行为。当前审计轮全量命令 `python -m pytest` 得到 59 passed，用时 71.59 秒。
- **本地模型 CLI 观察：** 此前试验在本地 OpenAI 兼容 gateway 上执行 Python 任务，验证 handler nonce 能从 worker 回传并被模型复述；也观察了多 handler 顺序、跨执行持久注册、worker 重启后的旧 epoch 拒收、用户中途发消息和模型行为错误。当前审计轮未重新运行 CLI，因此这些属于历史实验证据，不代表 2026-10-05 的当前代码版本已完成 live 复验。

实验并非随机化对照研究。曾有一次无 handler 控制与一次 nonce handler 任务都使用两次 committed activation；原始 token 数分别为 7,842 和 8,224，但提示词和生成代码不同，不能据此估计 handler 的因果成本。当前数据只支持提出后续配对实验，不支持性能优势结论。

## 4. 结果

### 4.1 已验证的运行行为

此前 live CLI 试验显示，handler 产生的随机 nonce 可以在不通过普通 stdout 暴露的情况下回到模型上下文。多 handler 试验中，两个不可预测 nonce 均被准确复述，journal 记录 `python.finished` 和对应 `agent.handler_fired`。同一 worker epoch 跨两次执行保留的注册可分别触发；timeout/restart 后，旧 epoch 帧被拒绝，新 epoch 注册正常路由。

另有受控试验在 Python 执行期间送入第二条用户消息：首个执行完成后，该消息得到处理，但模型随后产生一次不必要的 no-op Python 调用。较早一次较少约束的模型运行曾改错函数名、在结果返回前宣称成功，随后才根据失败观察纠正。这些现象提示 harness 的事件正确性不能替代模型行为评估。

### 4.2 当前容量与关联审计

源码和回归用例表明，admission 在下游容量检查与请求发布失败时回滚暂存预留和临时序号；terminal 的 `reply_to` 必须与预留 lane key 相符。审查发现过一个错 key 风险：终态曾可能按错误关联键调整另一执行的 handler 预算。现在该校验先于账本调整，并有回归测试覆盖。

全量回归最初为 58 passed、1 failed。失败夹具直接投递 handler event，却没有按生产 admission 契约预留 handler slot；Mailbox 正确拒绝了投递。修正夹具后，全量结果为 59 passed。该失败属于测试前置条件错误，不是放宽容量规则后消失的生产缺陷。

### 4.3 仍未证明的性质

当前测试证明配置范围内的账本转移和若干生命周期路径，不等于证明高并发下的容量行为。尚未进行接近全局槽位上限的并发压力实验，也未进行 cancel/restart/shutdown 组合负载测试。Actor coalescer 依据 request ID 统计到达结果数；handler ID 去重由 Host 保证，另一种 producer 若不遵守该约束，coalescer 本身不会独立发现重复 ID。

即使终态标记 missing，模型也未必会收到 Host 发出的 UI 错误事件。因而“系统可审计地知道结果不完整”已具备实现路径，但“模型总会看见并据此调整行为”尚未成立。

## 5. 讨论

### 5.1 架构判断

目前较有证据支持的选择是保留窄的完成 handler bridge，并使用每执行预期数、有限 handler 槽位、独立终态预留和显式 incomplete/unknown 状态。每个成功投递的 handler 仍保留独立 journal 事件；这样既能审计单条结果，也避免无界 fan-out 挤占普通事件与终态容量。

现阶段没有足够证据把 API 扩展成任意命名事件订阅。通用订阅会新增注册替换、取消、事件过滤、执行间状态、跨 epoch 重建和可观测性规则；这些复杂度应由具体用例与配对实验驱动。若 handler 不能减少模型激活、不能改善异步信息处理，或其失败状态无法可靠传达，那么扩展 API 的收益就需要重新论证。

### 5.2 成本与模型行为

单次 Python 周期的 handler 与无 handler 控制都观察到两次 activation，因此当前 hook 没有显示出降低模型调用次数的效果。它的潜在价值在于让 worker 自主产生结构化观察、关联多条异步结果，而非已证实的 token 或延迟节省。需要用同一任务、相同模型设置、随机化运行顺序和重复样本进行有/无 handler 配对；同时报告 activation 数、provider 请求数、端到端延迟、输入/输出 token、失败率和冗余工具调用。

### 5.3 延迟与完整性取舍

25 ms coalescer 上限是当前实现的有界等待策略，并非由实测用户体验确定的普适最优值。已知 handler 数使 actor 可在结果齐全时提前结束等待；若结果迟到，终态仍能表达缺失。后续应测量 handler 到达延迟分布、等待带来的端到端增量，以及缺失观察对最终任务质量的影响，再决定是否调整该上限或改为更明确的批次关闭协议。

## 6. 结论

Nervipulsa 已从单纯的工具调用循环扩展出可观察的事件执行主干，并实现了 handler 结果的初步有界投递协议。历史 live CLI 试验提供了 handler 结果进入模型上下文的真实证据；当前源码审计与 59 项全量测试支持主要容量、关联、回滚和排序路径。近期发现并修复的错关联键问题说明，账本边界审查仍能发现测试之外的协议缺陷。

因此，当前结论是“可继续验证的事件原生 harness 原型”，而不是“已被证明更令人满意的新架构”。下一阶段应优先完成当前代码版本的本地 CLI 端到端复验、并发容量与生命周期压力测试、缺失/拒绝结果对模型可见性的验证，以及有/无 handler 的配对基准。完成这些实验后，再决定保留窄 bridge、扩展为通用订阅，或移除该机制。

## 参考材料

1. Nervipulsa 项目研究日志：[runtime_harness_architecture_research.md](runtime_harness_architecture_research.md)。本文中的实现细节与实验记录均来自该项目内的源码审查、测试结果和历史 CLI 观察；未引用外部文献，也未将历史运行结果描述为本轮复验。
2. 本轮全量测试记录：`python -m pytest`，59 passed，71.59 s（2026-10-04 审计记录，Asia/Shanghai）。

## 后续实验清单

1. 对当前代码版本运行本地 gateway CLI live test，覆盖单 handler、多 handler、用户中途发言、超时及重启。
2. 在全局 handler slot ceiling 附近并发 admission，验证拒绝、释放和 close 后账本归零。
3. 组合测试 cancel、timeout、restart、shutdown 与迟到/部分 handler 结果。
4. 验证 handler 双路径拒绝时，模型是否得到可行动的 incomplete/error observation。
5. 运行严格配对 benchmark，测量 activation、provider 请求、延迟、token 和任务正确率。
6. 以用例和测量结果决定是否需要通用命名事件订阅 API。
