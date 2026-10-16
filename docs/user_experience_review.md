# 用户体验评估：本地开发者首次使用 Nervipulsa

- **评估日期：** 2026-10-13（Asia/Shanghai）
- **代码基线：** `main`，`0280147 docs(research): integrate durable effect evidence`
- **方法：** README 与源码走查；没有运行当前版本 CLI、连接模型 provider 或进行用户访谈。
- **目标用户：** 希望在本地仓库中让 coding agent 读写文件、运行 Python 并检查结果的开发者。

## 总体判断

Nervipulsa 已有不错的工程反馈：执行会显示状态、耗时和输出，超长输出可取回；暂停、取消、日志和未确认副作用也有对应命令。它会把“请求已接受”“代码已结束”“外部副作用已确认”区分开。这让熟悉终端和 agent 工具的开发者更容易查错。

第一次使用仍偏手动。用户要先拼齐 provider、模型、网关等前置条件；开始执行前只能看到有限代码摘录；失败后还要自己选 `/logs`、`/output`、`/retry` 或 `/effects`。因此目前更像给本地开发者试用的工程原型，尚未形成新用户不用研究命令就能走通的完整闭环。

## 按用户旅程观察

### 1. 安装与首次配置

README 给出 Python 3.11+、安装命令、交互式启动命令及 `/config` 设置方式。配置保存在用户配置目录，API key 输入时隐藏；`/config show` 也会遮蔽密钥。默认 Magpie profile 需要先启动本地 gateway、登录至少一个上游 provider，并选择 gateway 模型目录中的模型。只启动 CLI 并不代表第一次请求已经具备可用配置。

**用户感受：** 文档把依赖讲清楚了，但首次成功路径分散在安装、Magpie、provider 与 profile 几处。新用户可能到第一次模型请求失败时，才发现忘了启动 gateway 或选模型。

**源码/文档依据：** [README 配置与启动说明](../README.md)、[CLI 启动提示与配置入口](../nervipulsa/cli.py#L510)、[配置展示与密钥遮蔽](../nervipulsa/cli.py#L360)。

### 2. 提交任务与观察执行

`python_exec` 的立即回执表示 accepted/rejected，最终输出通过关联的运行时事件到达。worker 使用持久 Python interpreter，变量和 imports 在正常执行之间保留；多个 Python 请求按接纳顺序串行执行。CLI 展示 Python 请求的代码摘录（最多 240 字符）、运行状态、完成时间、输出或异常以及 context 用量。较大的输出写入 workspace 的 `.nervipulsa/outputs/`，可用 `/output` 读取完整 stdout/stderr。

**用户感受：** “接受”和“完成”有清楚区分，等待期间也能知道是否在运行；但代码预览不完整，没有要求用户在执行前检查或批准。由于 Python 代码使用当前 OS 用户权限，`--dir` 只是默认工作目录，不是安全边界。对可信 workspace 中的小任务这很方便；对影响范围不明的任务，用户不容易在副作用发生前建立信心。

**源码/文档依据：** [Python 工具契约](../nervipulsa/tools.py#L13)、[工具接纳回执](../nervipulsa/tools.py#L88)、[runtime 代码摘录与结果渲染](../nervipulsa/runtime.py#L271)、[README 输出与 `/output` 说明](../README.md)。

### 3. 执行中的输入与纠偏

交互 CLI 支持多行输入、命令补全，并在用户打字时异步输出。Python 执行期间，用户消息可以进入运行时；README 的 scripted demo 观察到输入在 Python 运行期间被接收，Python terminal 则在后续 activation 中交给模型。一次 provider 请求/模型 activation 不能被新消息打断；该消息须等待当前请求结束后由后续 activation 处理。PythonHost 同时只执行一个请求，所以这不代表 Python 并行执行。

**用户感受：** 可以先发纠偏意见，不必等 Python worker 空闲；但新消息不能中断正在进行的模型请求。当前请求结束后，后续 activation 可以处理这条消息，即使 Python 任务仍在运行。用户能提前输入，但不能期待模型即时打断或并行响应。

**源码/文档依据：** [README scripted trace 与限制](../README.md)、[PythonHost 串行调度](../nervipulsa/python_host.py#L293)、[actor 从 inbox 取批次并顺序 activation](../nervipulsa/llm.py#L814)。

### 4. 失败、取消与恢复

CLI 提供 `/status`、`/logs`、`/output`、`/retry`、`/cancel` 和 `/effects`。模型请求暂停后可显式 `/retry`；Python 取消时 CLI 先反馈 `Cancellation requested.`，之后再报告取消结果。超时或执行中取消会重启 worker 并清空 interpreter namespace，但不会撤销已经发生的文件或外部操作。若执行结果不确定，`/effects` 列出未解决记录，并明确提示不要自动重放。

**用户感受：** 这是项目做得比较诚实的部分：它没有把失败终态说成“什么副作用都没发生”，也没有默默重放代码。摩擦在于命令的对象不同：`/retry` 重试暂停的模型 activation，并不等于重跑 Python；取消回执表示请求已发出，并不等于 worker 已停止；`unknown` 最终需要用户检查文件或外部系统来对账。

**源码/文档依据：** [取消和恢复命令](../nervipulsa/cli.py#L259)、[取消回执](../nervipulsa/cli.py#L290)、[状态诊断](../nervipulsa/cli.py#L328)、[Durable Effect Protocol](durable_effect_protocol.md)。

## 体验优势

- **状态可追踪：** `/status` 显示 model、workspace、actor、Python 队列、当前执行、context 预算、worker epoch 和 Journal 状态；执行输出有预览，完整输出有路径可取。
- **结果语义较诚实：** accepted 不冒充 succeeded；取消、超时、worker exit 和持久化失败可以标为 unknown；`/effects` 阻止用户把未确认副作用当成可安全重试。
- **适合本地开发工作流：** persistent interpreter、`python_environment` 静态发现、Hashline Edit 和显式 smoke test 能组成读、改、运行、核验的基础工具链；这些能力仍需要具体任务验证模型是否能正确使用。

## 优先改进

1. **做一次启动就绪检查。** 集中报告当前 profile、模型是否已选、key 是否已配置，以及本地 endpoint 是否可达；缺项旁直接给出下一条命令。保留 API key 遮蔽。
2. **补一条完整的五分钟 quickstart。** 用临时或示例 workspace 演示读文件、生成并检查改动、运行验证、查看完整输出；明确 Python 使用的权限和执行目录。可探索 trusted workspace 的预览/确认选项，避免把所有用户都锁进逐次确认。
3. **把恢复建议放到故障回执旁，并支持可审计的人工判断。** 明确区分“取消已请求”与“已确认停止”，提醒 `/retry` 只重试模型轮次；对 worker 状态为 `unknown` 的执行，先查目标系统再判断，不能盲目重跑。现在 `/effects decide <session_id> <execution_id> <occurred|not_occurred|undetermined> --by "<operator>" --evidence "<what was checked>"` 会追加保存人工判断，`/effects history` 可查看完整记录，`/effects` 仍显示原 worker 状态和最新人工判断。人工记录是操作者自述，不是目标端认证查询；它不把 `unknown` 改成 `observed`，也不授权或执行重试。

## 证据边界与下一步

以上是代码与文档推导出的体验评估，不是实际用户研究，也不是当前版本的 live provider 测试。当前 README 记录的真实模型观察使用旧 adapter，不能证明当前配置流程和模型行为已验证。交互输出是否清晰、首次配置耗时、模型是否理解 unknown 并避免盲目重试，都应通过当前版本的 live CLI 任务验证。

建议下一轮让一名第一次使用者从干净配置开始完成一个小型文件修改任务，记录配置时间、首次成功率、卡住的位置、从异常中恢复所需命令数；另设长 Python 任务测试中途输入、取消反馈和 unknown 对账。报告结果时区分代码检查、自动化测试、live run 和真实用户观察。
