# Nervipulsa 文献筛选与协议对照｜2026-10-16

## 范围与快照

本记录核对了用户提供的四条材料，并用 Temporal 官方文档和 Transactional Outbox 模式补齐执行语义对照。证据范围包括 arXiv 元数据/摘要、Pollard 与 KedgeFlow 中与身份、重放、未知结果、评估和限制相关的正文段落，以及项目当前本地代码和 T0–T4 测试记录；没有逐页审读四篇材料的全部正文。

截至本记录时，工作树 `main` 与 `origin/main` 都指向 `993683921ce54ffefaa3fa4444574f272373a0e2`。T0–T4 测试文件尚未提交，处于本地未跟踪状态；因此用户查看 GitHub `main` 时看不到这轮实验，仓库页面说早期进程探针先于 effect ledger 是准确的。本文把本地实验与远端 `main` 的证据明确分开。

## 文献核对

### Pollard：语义步骤身份与严格重放

Muntaser Syed、Marius C. Silaghi，arXiv v1 于 2026-10-12 提交；页面标注已被 IEEE ICTAI 2026 short paper 接收。[arXiv v1](https://arxiv.org/abs/2610.16145v1)

Pollard 让调用方保留 agent 控制循环，由调用方选定并包装 model/tool/note 等语义步骤。步骤 ID 绑定父节点、步骤类型、attempt 和规范化输入；同一执行树记录拒绝、完成、资源预留/结算与分支。严格重放校验节点和结果后返回已保存结果，不调用被包装操作。它因此比仅按工具名和参数命中的缓存更能保留因果来源。

论文的证据比摘要更细：E2 在单机 PostgreSQL 进程组中注入连接、确认、重启、重试和 lease 故障；保留 lease 的 122 组未违反精确预算约束，lease 过期后旧 owner 又完成的两例则被保留为定理前提外的 overshoot。E3 对 155 个严格重放观察没有调用 wrapped operation，并用 ancestry 与结果变异检查错误复用。E7 测了指定 framework release 和公开边界，不代表任意后台调用都被捕获。论文明确依赖 transactional store、完成期间保留 reservation lease、调用方完整包装边界及外部持有 seal 等前提；旧工具结果不会重建已变化的外部世界。

**与 host-crash recovery 的关系：**Pollard 确实测了与 reservation/store 有关的进程重启和确认丢失；这不是完整 Agent Runtime 的任务恢复实验。论文没有声称从运行时宿主崩溃后恢复所有变量、模型上下文、等待状态或远端副作用。它与 T0–T4 的重合在提交/确认窗口，验证对象和恢复目标不同。

### KedgeFlow：受保护动作的授权与一次性额度

Jun He、Deying Yu，arXiv v1 于 2026-10-10 提交，元数据标注 12 页、1 图、3 表。[arXiv v1](https://arxiv.org/abs/2610.14810v1)；作者页面称其为开放的候选执行控制协议，[白皮书](https://www.openkedge.io/paper/kedgeflow)。

KedgeFlow 把 proposal、admission、精确动作 grant、共享 single-use allowance 的 claim、dispatch authorization、目标端条件与 outcome record 分开。Lost response 时 allowance 仍被消费；`Unknown` 不会自动变成“没发生”，也不会因为超时重新派发。目标系统是否原子执行 guard、是否有可查询证据，仍决定能否确认实际 commit。

短论文报告：三成员 etcd 测试集群都运行在同一台主机；6,560 次并发 claim 对 82 个 allowance 各只产生一个成功 claim；32-call gateway race 产生一个 mock effect；模拟目标已 commit、HTTP 响应丢失后，重试没有产生第二次 dispatch。实验使用合成身份/授权输入和 mock target。论文明确将 distributed reservation、所有生产旁路路径覆盖、native crash recovery、真实 target exactly-once 和不同实现互操作列为未测试。它列出五个可能崩溃窗口，但把这些窗口标为由代码推导，**不是执行过的 crash-injection 结果**。因此 KedgeFlow 是授权与执行边界的对照，不是已证实的通用 host-recovery 系统。

### Durable execution 综述：可用作术语与检查清单

用户给出的 SciXa 页面正文涵盖 checkpoint、write-ahead intent、effect ledger、restore/replay、idempotency、fencing、lease、compensation 和 unknown outcome；可用于检查概念是否混淆。[文章正文](https://scixa.com/article?slug=durable-execution-substrates)

当前页面提取结果没有显示可核验的作者、发布日期、文章类型或“25 sources”计数；这些元数据在本轮保持未核实。该页面是二手工程性综合材料，不是某个 runtime 的实现或故障实验。其“at-least-once + receiver idempotency key”建议与分布式系统惯例一致，但不应被当成 Nervipulsa 已具备恢复保证的证据。

### Many Steps into One：与运行时效果语义有条件相关

Jianghong Zhou、Hao Rong、Chen Lin、Daoan Zhang，arXiv v1 于 2026-10-13 提交。[arXiv v1](https://arxiv.org/abs/2610.17544v1)

论文研究 reasoning contraction：把多步推理压缩成可复用 memory/template 后，依赖事实变化会造成 stale answers。摘要报告了编辑事实上的多跳问答和带 exact verifier 的代码任务实验；研究对象是可复用推理/记忆的认证和失效，不是 Python 工具副作用、host 崩溃、重试或 transcript 恢复。只有当 Nervipulsa 把旧模型结论、摘要或工具输出作为可复用 memory 时，才可迁移“来源版本、依赖变化与失效”这个问题；当前 effect ledger 记录某次 Python execution 的终态，不等于论文中的 reasoning memory。

## 与成熟执行模式的语义对照

| 模式 | 持久化对象 | 重放/重试语义 | 对副作用的关键前提 | 相对 Nervipulsa 的差异 |
| --- | --- | --- | --- | --- |
| Temporal durable workflow | 持久 Workflow Event History、活动参数/结果和控制事件 | Worker 崩溃后用 history replay workflow 代码重建状态；已完成 Activity 从 history 取结果，但未能上报完成的 Activity 可能重试、重复执行 | 写入 Activity 需要幂等；可用稳定 idempotency key，真正去重由目标服务执行 | 提供任务级控制状态恢复与协调服务；Activity 的外部执行仍不是天然 exactly-once。参考[Event History](https://docs.temporal.io/encyclopedia/event-history.md)、[Workflow Activity](https://docs.temporal.io/workflow-activity.md)、[Activity 幂等与重试](https://docs.temporal.io/activity-definition.md)。 |
| Transactional Outbox | 同一数据库事务中的业务状态和待发布消息 | Relay 在事务提交后发布；若发布后、记录已发布前崩溃，可能重复投递 | 消费方按稳定消息 ID 幂等；模式原子覆盖的是本地 DB 更新与 outbox 行，不是任意远端工具效果 | 可改善本地状态变更与事件发布一致性，但本身不保存 agent 控制状态、不恢复 transcript，也不把外部 API 与本地 SQLite 变成一个事务。[模式说明](https://microservices.io/patterns/data/transactional-outbox.html)。 |
| Idempotent effect API | 目标端按稳定 key 保存操作身份与结果 | 调用方可在不确定响应后用同一 key 重试并取回相同操作结果 | 必须由真正执行副作用的接收端支持 key、保留窗口和参数绑定 | 可把 unknown 变成安全探测/重试的条件；Nervipulsa 当前任意 Python 代码并不自动提供目标端幂等 key。[Temporal Activity 文档](https://docs.temporal.io/activity-definition.md)也明确说去重由 Activity 调用的服务实施。 |
| Pollard | ancestry-bound semantic step tree、预算预留/结算和验证过的步骤结果 | Strict replay 对匹配节点返回已存结果，不调用 wrapped operation | 调用方必须选对语义边界并包装所有相关操作；store、lease、seal 等前提成立 | 比 Nervipulsa 多了可验证的语义步骤身份和重放；Nervipulsa 当前按 execution 记副作用不确定状态，不是 step tree。 |
| KedgeFlow | exact-action grant、单次 allowance、dispatch/outcome lineage | 不确定时保留已消费 allowance，不自动重新 dispatch；查询可凭目标证据修正未知结论 | 完整中介、可信 issuer、target guard/证据和不能旁路的凭证边界 | 提供授权与执行控制面；Nervipulsa ledger 不授权 Python 代码做什么，也不隔离任意 Python 的 OS 权限。 |
| Nervipulsa 当前 Effect Ledger | SQLite `(session_id, execution_id)` 的 `intent → dispatching → observed | unknown` | `observed` 持久化在 `python.finished` 发布前；未确认结果不会自动重跑；`/effects` 跨 session 列出未决项 | SQLite 状态只覆盖本地 ledger；Python effect 可在结果不确定时已经发生，进程内 actor/session/transcript 不随 ledger 恢复 | 提高进程重启后的不确定性可见性，不是 durable workflow、回滚、自动 reconciliation 或 exactly-once。详见[协议说明](durable_effect_protocol.md)。 |

这些模式不能用“都有日志/ledger”归为一类。Temporal 的核心是恢复 workflow 控制状态；Outbox 是本地数据库事务与消息发布的桥；idempotency 是目标端对重复操作的处理契约；Pollard 连接步骤身份、治理与严格结果重放；KedgeFlow 约束谁能派发哪个精确动作；Nervipulsa 当前只持久化 Python execution 的效果观察状态。

## 本地 T0–T4 结果与 GitHub `main` 的边界

2026-10-14 的本地 Windows OS 进程树终止实验，对五个提交后窗口进行了门控测试：T0 `intent` 已提交、dispatching 前；T1 `dispatching` 已提交、execute frame 写出前；T2 worker 已追加文件 marker、terminal 前；T3 host 收到 terminal、`observed` 提交前；T4 `observed` 已提交且 `python.finished` 已入旧 mailbox、actor 尚未消费。重启后 T0–T3 都保留 unresolved 且不重放；T2/T3 的 marker 只出现一次。T4 ledger 显示 observed，但新 session 和 model input 没有恢复旧 terminal。定向测试 `python -m pytest tests/test_crash_boundaries.py -q`：5 passed。

这是已提交 SQLite commit 之后的 OS 进程终止证据，不覆盖 WAL commit 中断、断电、内核/设备故障、远端副作用、任务/transcript 恢复或 exactly-once。当前这批测试和文档仍未提交；远端 `main` 的文档快照不包含它们。因此应同时保留两句话：**本周筛到的论文没有提供与 T0–T4 等价的 Agent Runtime host-crash/task-recovery 实验；Nervipulsa 本地工作树已经有一个范围有限的 T0–T4 进程终止实验。**

## 人工核对路径：目前是可见性，不是可闭环对账

源码检查发现 CLI `/effects` 只列出 `state != observed` 的行，并显示 session、execution、状态、时间、错误和“不要自动重放”警告（[`cli.py`](../nervipulsa/cli.py#L259)、[`journal.py`](../nervipulsa/journal.py#L412)）。当前没有 CLI/API 将操作者查到的外部事实写回 ledger，也没有受审计的 reconcile/resolve 状态。因此操作者可在外部检查效果，但无法通过产品支持的路径把结论记入该账本；该行会继续显示 unresolved。KedgeFlow 的 reconciliation 设计更完整，但它的参考实现也只用 scripted observations 测 reconciliation library，并未部署 authenticated target-query service。

这是一项具体 UX/协议缺口。若补齐，应追加记录操作者、时间、execution/target、证据来源、判定（已发生/确认未发生/仍未知）及后续动作；保留原 `unknown` 事实，不把人工判断伪装成 worker 的 `observed` terminal，也不触发自动重放。需要再对照实际 effect 类型设计，避免一个“resolve”按钮把不同证据强度和重试权限混在一起。

## 研究判断与后续

当前材料不支持声称 Nervipulsa 发明了 durable execution、unknown outcome、write-ahead intent、幂等重试或人工对账。可检验的候选贡献较窄：一个本地 coding-agent runtime 把 Python effect 的不确定状态持久化，并用 T0–T4 测量 effect 已知状态与 actor 消费/任务恢复之间的分界。尚需把本地实验合入、在 CI 重现，再做真实副作用类型的人工对账走查与 A1/A2 配对 coding task 测量。

下一轮优先级：

1. 先补全 view-only `/effects` 后的人工作业路径设计和证据记录字段；实现前明确哪些结论只能由目标端回读证实。
2. 提交或以新 commit 固定 T0–T4 测试与文档，再将同样边界纳入 Windows/Linux CI；另行设计 SQLite/WAL 写入中终止与持久性假设实验。
3. 用同一组本地 coding tasks 配对比较 A1/A2：任务质量、false-success、unknown 的可理解性、危险重试、人工接管、token 和延迟；ScriptedBackend 消融只能解释机制，不能替代这组测量。

## 来源

- [Pollard, arXiv:2610.16145v1](https://arxiv.org/abs/2610.16145v1)
- [KedgeFlow, arXiv:2610.14810v1](https://arxiv.org/abs/2610.14810v1)；[KedgeFlow 白皮书页面](https://www.openkedge.io/paper/kedgeflow)
- [SciXa durable execution article](https://scixa.com/article?slug=durable-execution-substrates)（页面作者/日期/来源计数本轮未核实）
- [Many Steps into One, arXiv:2610.17544v1](https://arxiv.org/abs/2610.17544v1)
- [Temporal Event History](https://docs.temporal.io/encyclopedia/event-history.md)；[Workflow Activity](https://docs.temporal.io/workflow-activity.md)；[Activity Definition](https://docs.temporal.io/activity-definition.md)；[Side Effects](https://docs.temporal.io/develop/go/workflows/side-effects.md)
- [Transactional Outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html)
