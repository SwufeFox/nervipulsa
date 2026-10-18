# LayerFS 与 Nervipulsa：文件状态分叉层研究

- **研究日期：** 2026-10-18（Asia/Shanghai）
- **状态：** 桌面研究完成；尚未集成或运行 LayerFS
- **对象：** Ephemeral-AI-Lab/layerfs 的 v0.1.7 发布版、当前 `main` 文档与源码说明
- **证据类型：** 仓库 README、v0.1.7 release record、版本化 SDK guide、当前 SDK guide、开发路线图与机制手册。未克隆或执行项目；未独立复现其测试、持久性和性能结果。

## 结论摘要

LayerFS 与 Nervipulsa 的职责确实互补：LayerFS 管文件系统历史和隔离工作区，Nervipulsa 管 Agent 执行事件、模型可见性及副作用状态。最值得验证的接入点是“每个候选执行拥有独立 Workspace，成功候选对应一个可记录的 Commit ID”，而不是把 LayerFS 当成 event log 或可靠任务恢复引擎。

LayerFS v0.1.7 是 2026-10-15 发布的 Developer Preview。它采用重写后的 `core/`、JavaScript SDK 和容器内 daemon；v0.1.6 的 CLI 与公开 Rust SDK 已删除，旧 Store 不支持原地迁移，必须匹配 SDK/addon/daemon 版本。仓库首页截至本次查阅仍突出 v0.1.6，和 Releases 中的 v0.1.7 不一致；应以版本标签及发布记录描述版本事实。

v0.1.7 的逻辑 Commit/Branch 历史不能被解释为 crash-durable storage。发布记录称只执行 Disposable profile（SQLite WAL、`synchronous=OFF`），Durable profile 未运行且被禁用；daemon/container 重启恢复与不确定操作重放明确排除。即使 Store 在活进程中提供不可变历史，这也不构成断电保证。

## 版本与证据分层

| 范围 | 本次核对结果 | 不应推导的结论 |
| --- | --- | --- |
| v0.1.7 release/tag | 2026-10-15 发布；新 `core/` 架构；29 个公开 JS SDK 方法；仅源码发布，无 npm 包、预编译二进制或公开镜像。 | 不能把旧版 `crates/` API 当作 v0.1.7 API；不能假设旧 Store 可迁移。 |
| 发布验证 | SDK 标准用例 26 项加 S328、XPROCESS 的记录大体通过；B7 未运行、A9 保留历史失败。四个 live UI stories 和 fresh-browser 检查记录通过。 | release record 明确没有在 release commit 重跑完整 workspace suite、独立 FUSE/container gates。不是“所有 gate 全绿”。 |
| 耐久性 | 只运行 Disposable/WAL/OFF；Durable 未运行且禁用。新 Node 进程可用保存的 Sandbox ID 继续访问，但 daemon/container 重启恢复不支持。 | 进程间复用 ID 不等于 daemon 重启恢复，也不等于断电耐久性。 |
| 性能 | v0.1.7 声明不作性能主张。计数器 v5 是单个组件诊断：一次样本 +1.3351% 操作时间、分配相同；v2–v4 有失败结果。 | 不能据此断言 SDK、FUSE、分支创建或 Agent 工作负载快，也不能把 CAS/CDC/COW 的机制目标当成实测收益。 |
| 当前 `main` | 当前指南描述 `core/` 重写实现及内部机制；1.0 roadmap 仍列出 Durable 是否启用、restart recovery、发布 gate、分发方式等待决策。 | 规划文档不等于兼容契约或已交付能力；main 与发布 tag 需分别引用。 |

### 运行和集成门槛

v0.1.7 需要 Docker、Node 20+、Rust `+1.85.1`，自行构建 Linux daemon 与 host native addon，并配置私有 home、Docker socket、镜像 digest、共享 volume 和 daemon 路径。实际产品路径依赖 Linux daemon/container；在 Nervipulsa 当前 Windows PythonHost 上不能直接当作普通原生目录 API 使用。合理的初始试验环境是 Linux 主机或 Linux VM，再通过单独 Node adapter 调用 JS SDK。

SDK 的 `DSH_LAYERFS_*` 环境变量 fallback 是针对 deepseek-harness 环境传递方式的配置兼容点；它不等于已交付的 Nervipulsa 插件、调用适配器或运行时语义集成。SDK 文档还要求先顺序创建 Sandbox；创建/发现共享 project lock，竞争创建可返回 `Busy`。Sandbox 建立后，独立 Sandbox 的后续工作才可并行。

## 机制：实现含义与边界

- **CAS**：已定型对象以内容身份查找、校验并复用；存在对象仍可能需要读取和认证，不等于零 I/O。对象身份也不等于垃圾回收协议。
- **CDC**：按内容定义边界切块，局部改动可能保留后续块身份；文档不承诺固定重同步距离或复用率。它减少可复用单元的粒度，但不保证每种负载都更快。
- **COW**：文件/目录结构可沿变化路径重建，旧结构共享；逻辑状态完整而物理表示增量。工作区挂载不扫描/物化完整树，但写入、Commit、合并、认证和 SQLite 事务仍有成本。
- **Commit/Branch/Workspace**：Branch 从既定状态分叉；Workspace 是某 Branch head 的隔离可写视图；Commit 保存改动。若挂载后 Branch head 前进，Commit 会尝试与新 head 合并；冲突时在 Workspace 中写标记且不提交。`main` 只有显式 publish 才会改变。
- **执行副作用**：`sandbox.exec` 只执行命令，不自动 Commit。旧 Workspace 文件状态回退无法撤销网络、数据库、消息等外部副作用。未知操作不会自动重试。
- **Durability 与恢复**：异步/逻辑历史、fresh Node process 对 Sandbox ID 的复用、daemon/container 重启恢复、SQLite durability 是不同性质；v0.1.7 只对前两者中的受限行为提供实现描述，不提供后两者所需的完整保证。

## 与 Nervipulsa 的对应关系

| 问题 | Nervipulsa | LayerFS | 组合责任 |
| --- | --- | --- | --- |
| 状态对象 | 事件、activation、执行终态及 effect ledger | 文件树、Branch、Commit、Workspace | 各自保存权威状态，通过显式 ID 互相关联。 |
| 并行候选 | 当前 host 的 Python worker 串行；runtime 尚无候选分支调度器 | 独立 Sandbox/Workspace 可对相同基线并行工作（先顺序创建 Sandbox） | 由 runtime 创建候选记录和调度；LayerFS 提供隔离文件视图。 |
| 成功选择 | 可投影/评估执行结果，但无文件树分叉 | 通过 Commit/Branch diff 和 publish 保留或提升文件状态 | Runtime 选择候选；LayerFS 不替模型判定正确性。 |
| 失败恢复 | `unknown` 需检查，runtime 不重放副作用 | 可丢弃未提交 Workspace，或从旧 Commit 新开 Branch | 只能回退 LayerFS 管辖的文件状态；外部副作用需独立核验。 |
| 故障原子性 | SQLite event/effect Journal | 独立 LayerFS Store | 两个 Store 无共同事务；必须有 Intent/Confirm/Unknown 查询协议。 |
| 安全隔离 | 当前 Python worker 使用 OS 用户权限，`--dir` 不是沙箱 | 容器化 Linux daemon 与 Workspace 投影 | 要确认实际命令执行边界、容器权限与挂载；不能只因“workspace 独立”就称为安全沙箱。 |

原提案的 `Intent → Execute → Commit → Confirm → Record` 顺序有遗漏：若 Commit 已发布、Confirm 之前 host 崩溃，仍会有未关联 Commit。不能把 Commit 后的 confirm 误写成原子成功。建议使用可核验的两阶段观察语义：

1. Nervipulsa 先持久化候选 execution ID、基线 Commit ID、预期 LayerFS project/branch/workspace 和唯一操作 ID。
2. 执行前记录 `dispatching`；LayerFS 操作使用明确 branch/workspace 与 message 关联值。若 SDK 不提供幂等键或按操作 ID 查询，不自动重试不确定的写操作。
3. Commit 返回后，runtime 把 Commit ID 和结果写入本地 Journal；若在两者之间崩溃，重启后以只读 Branch/Commit 历史和操作标记核对。
4. 只有完成核验后才记 `observed`；证据不足则记 `unknown`。文件状态事实和命令是否发生副作用要分别记录。
5. 候选胜出后的 publish 也作为另一个外部 effect 处理；不要把 merge/Commit 成功等同于 main 已更新。

这是协议设计建议，尚未在 LayerFS 或 Nervipulsa 中实现/测试。其可恢复程度取决于 LayerFS Store 本身能否在目标故障模型下保留已确认 Commit；v0.1.7 当前没有提供该耐久性保证。

## 建议的 Nervipulsa 原型实验

先做一个 Linux VM 上的外部 adapter 原型，不改动现有 event Journal schema。选择小型、可复现代码仓库；从同一基线分配 3 个候选 Branch/Workspace，执行相同 bug 修复任务，分别运行固定测试。记录：初始化/分支创建时间，Sandbox 创建时间，读写/Commit 时间，CPU/RSS/磁盘增长，SDK 调用数，最终 root/Commit ID，候选间 diff、冲突和 false success，丢弃 cleanup，以及 Node/daemon/host 在每个操作边界被终止后的可观察状态。

第一轮先验证语义，不比较“快多少”：

- 同一基线隔离：候选 A 写入后，B 的视图不变。
- 丢弃：未 Commit Workspace 被 discard/unmount 后不改变 Branch head。
- Commit：保存后可从对应 ref 读回确切文件内容与 Commit ID。
- 并发 head 移动：一个 Workspace 挂载后另一条路径推进 Branch，再尝试 Commit，核实 clean merge 与 conflict 两种结果。
- 跨存储窗口：在 fork、exec、Commit 返回、Commit Journal 记录、publish 前后分别杀死进程；重启后只读检查，不做猜测性重放。
- 外部副作用对照：候选执行一个本地 marker 和一个可控 HTTP mock 写入；回滚 workspace 后证明 marker/HTTP 不是由文件历史自动撤销。
- 资源比较：对同一文件树和固定编辑序列，与临时完整目录复制作配对基线；重复多轮、交错执行，报告中位数/分布与磁盘实际占用。不要拿分支创建微基准代表全任务性能。

只有这些语义跑通且故障边界可解释后，再评估是否加入 Nervipulsa `WorkspaceAdapter` 接口或模型工具。生产级 effect state、异步持久化和统一事务仍不在原型结论内。

## 来源

- [LayerFS v0.1.7 release](https://github.com/Ephemeral-AI-Lab/layerfs/releases/tag/v0.1.7)
- [v0.1.7 release record](https://github.com/Ephemeral-AI-Lab/layerfs/blob/v0.1.7/release-notes/0.1.7/README.md)
- [v0.1.7 SDK/UI guide](https://github.com/Ephemeral-AI-Lab/layerfs/blob/v0.1.7/docs/versioned/0.1.7/sdk-and-ui.md)
- [v0.1.7 SDK reference](https://github.com/Ephemeral-AI-Lab/layerfs/blob/v0.1.7/docs/versioned/0.1.7/sdk-reference.md)
- [Current JS SDK guide](https://github.com/Ephemeral-AI-Lab/layerfs/blob/main/sdk/js/README.md)
- [Current CAS/CDC/delta handbook](https://github.com/Ephemeral-AI-Lab/layerfs/blob/main/cas_cdc_deltaencoding_handbook.md)
- [1.0 roadmap](https://github.com/Ephemeral-AI-Lab/layerfs/blob/main/docs/roadmap/1.0/README.md)
- [v0.1.7 phase 5 verification record](https://github.com/Ephemeral-AI-Lab/layerfs/blob/main/core/docs/issues/327/PHASE5-20261015.md)
- [v0.1.7 counter measurement record](https://github.com/Ephemeral-AI-Lab/layerfs/blob/main/core/docs/issues/327/COUNTER-SCREEN-20261015.md)



## Nervipulsa follow-up (2026-10-18)

用户要求吸收有价值的设计后，先实现了一个不改变会话共享目录语义的落点：可选的 `--fingerprint-workspace` 在每次执行前后记录文件树 SHA-256 identity，并通过 `/logs` 查看。默认关闭，避免每次执行都扫描大工作区；它只关联状态身份，不提供分支、隔离、恢复或外部副作用回滚。实现说明见 [Durable Effect Protocol 文档](durable_effect_protocol.md) 和 [README](../README.md)。该改动仅做语法解析与 diff 检查，尚未运行测试或用真实工作区测量扫描成本，因此这是可审阅的原型切片，不是已验证的生产能力。

LayerFS 仓库 Stars/Forks 数仍未独立核实：本次 GitHub REST repository metadata 请求遇到 rate limit。仓库当前页面显示的 v0.1.6 banner 与 Release 页面及 v0.1.7 发布记录确认的 v0.1.7 发布状态不一致。