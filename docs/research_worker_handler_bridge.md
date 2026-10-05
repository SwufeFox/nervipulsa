# Nervipulsa 研究备忘录：worker 侧 `call/handle` 桥接接口

状态：研究草案（v0.4 基线上的 v0.5 方向探索）· 本文只做设计与风险评估，不改内核。
关联：`Nervipulsa_Design_v0.4.md` §14「未来最值得单独验证的扩展」。

## 0. 一句话主张

> 让模型写出的程序成为事件系统的一等参与者：worker 内注册的 handler 可以在
> 不唤醒 LLM 的前提下响应未来事件。这不是"更多工具"，而是把 agent 从
> *每轮循环都要模型推理* 改成 *模型编程事件循环*。

### 0.1 动机（为什么这条路线值得单独验证）

当前运行时有一个结构性成本：**每个 `python.finished` 都必然触发一次新的模型
推理**。一次真实修 bug 会话（读文件 → 改文件 → 跑测试 → 看结果 → 再改）中，
N 次执行 = N 次模型往返，每次都重放整段不断增长的 transcript。

设计书 §13.4 已经明确：判断架构优劣要看「任务完成率、成本、延迟、重复执行数、
无效激活数、实现复杂度」，而"重复处理不再每次都调用 LLM"正是设计书自己给出的
高价值方向。本备忘录把它落成可实现的契约。

### 0.2 期望收益与可证伪的判据

| 收益 | 可证伪判据 |
| --- | --- |
| 模型调用数下降 | 同一任务下 `activations / python_exec 次数` 比值下降 |
| 用户感知延迟下降 | 事件产生 → 被处理 的延迟不再包含模型往返 |
| 结构性验收（§13.4 第一条） | 换掉 LLM 或 Python 实现，内核与另一方控制语义不变 |

判据都可被 `benchmarks/bench_runtime.py` 的扩展探针证伪——若收益不成立，
就如实记录并停止该方向。

---

## 1. 现状事实（引用行号基于当前实现）

以下事实由一次只读代码调研确认，关键结论可直接从源码核对。

### 1.1 传输层已经是对称的

`nervipulsa/framing.py:1-62`：4 字节大端长度 + UTF-8 JSON，1 MiB 上限，
`encode_frame`/`read_frame` 无方向语义。**`ready` 帧就是 worker→host 主动通知的
既有先例**（无 `request_id`，`nervipulsa/python_worker.py:183`）。

### 1.2 但 host 侧三个消费点都是请求耦合的

| 消费点 | 行为 | 依据 |
| --- | --- | --- |
| 启动期 | 非 `ready` 帧丢弃 | `python_host.py:376-390` |
| 执行中 | `request_id != record.event_id` → `late_frames += 1` 丢弃 | `python_host.py:579-582` |
| 空闲期 | 一切帧 → `late_frames += 1` 丢弃 | `python_host.py:456-476` |

结论：**障碍不在协议，而在 host 的消费逻辑**。worker 今天发出的任何主动通知
只会留下一个 `late_frames` 计数。

### 1.3 worker 是单线程串行的

`python_worker.py:184-276`：主循环 `read_frame → exec → write_frame`，
非 `execute` 帧静默 `continue`（:188-189），exec 期间不读任何新帧（:221-243）。
`timeout` 字段 worker 侧不读（:190-193）——超时强制完全在 host。

### 1.4 其余代码事实

- `process_tree.py:144-194`：socketpair + fd 继承（Windows `lpAttributeList` handle_list / POSIX `pass_fds`），环境过滤剥掉全部 `NERVIPULSA_*` 与含 KEY/TOKEN/SECRET/PASSWORD/CREDENTIAL/PASSWD 的变量。
- `process_tree.py:104-141`：任何超时/取消/空闲死亡/shutdown 都是**无差别杀整棵进程树**（Windows `TerminateJobObject` / POSIX `killpg`），无优雅关闭。
- `journal.py:91-146`：五张表（events/deliveries/activations/executions/tool_calls），**没有任何 handler 注册概念**；自由扩展点只有 `executions.metadata_json` 与 `tool_calls.reason`。
- `python_host.py:678-745`：`_emit_finished` 与 `Execution` record、`_done` 去重强绑定——worker 主动事件没有现成出口。

---

## 2. 设计：最小受控桥

### 2.1 原则（对齐设计书 §1.1 与 §2.1 状态归属表）

1. **内核零改动**。Bus、路由、Mailbox、投递语义全部不动；host 新增能力必须是
   *内部* 扩展，不改变任何既有事件类型的有效载荷契约。
2. **worker 仍是不可信执行边界**。handler 代码与 `python_exec` 同源同权——不新增
   权限，不沙箱化，也不因此把 handler 当作"更安全"的东西。
3. **handler 生命周期 = worker epoch**。worker 死 = 所有 handler 蒸发。不提供
   跨 epoch 持久化，这与「worker 拥有 namespace」的既有归属一致。
4. **可观测优先于能力**。每次注册/注销/触发都进 journal，事件类型是新增的、
   有界的、可枚举的。

### 2.2 新增帧 kind（协议扩展，全为新增，不改变既有 kind）

| kind | 方向 | 载荷 | 语义 |
| --- | --- | --- | --- |
| `handler.register` | host→worker | `handler_id`（host 分配的 `hd_` 前缀 id）、`event`（订阅的事件 kind：`finished`/`environment_changed`/`interval`）、`match`（可选 `request_id`/`status` 过滤）、`code` | 在 worker namespace 内定义并登记 handler |
| `handler.deregister` | host→worker | `handler_id` | 注销 |
| `handler.list` | host→worker | — | 要求 worker 回 `handler.snapshot` |
| `handler.snapshot` | worker→host | `handlers: [{handler_id, event, match}]` | 宿主侧审计/回执 |
| `handler.fired` | worker→host | `handler_id`、`trigger`、`result`（小载荷） | handler 运行后的主动通知 |

### 2.3 主动 emit（不引入）

本设计**不**给 handler 提供 `event.emit` 通道。理由：handler 已经能通过
`handler.fired` 上报；一个自由 emit 通道会让 worker 绕过 host 的保留结果名额
（`Mailbox.reserve_terminal`，`nervipulsa/events.py:179-189`）从而打破背压
不变量。若未来需要，必须先给出新的 lane 与名额语义。

### 2.4 worker 侧运行形态

worker 主循环从「串行 exec + 静默丢弃」改为一个 **dispatch 表**：

```python
DISPATCH = {
    "execute": _on_execute,
    "handler.register": _on_register,
    "handler.deregister": _on_deregister,
    "handler.list": _on_list,
}
```

关键改造点（`python_worker.py`）：

1. **读写分离**。主循环仍单线程，但 socket 写改为经过一个 `threading.Lock`；
   handler 在独立线程运行时写帧必须持锁（现状 :173/:183/:217/:259 完全无锁）。
   这是 handler 并发化的前置条件。
2. **handler 线程池**（有界，默认 2）。`finished` 类 handler 在 executor 里跑，
   不阻塞主循环；`interval` 类 handler 由一个单调时钟线程触发。
3. **handler 与用户代码的隔离**。handler 代码在 *独立 namespace* 中 exec（共享
   `builtins`，不共享用户 namespace）。理由：用户代码可以覆盖任意名字，若共享
   namespace，用户代码一次 `handler = None` 就能破坏注册表——注册表必须由
   worker 自身持有，不由用户代码持有。
4. **输出隔离**。handler 的 stdout/stderr 也走 PipeCapture，但用 **独立 capture
   实例**，避免复用现状中 leftover 被带入下一次用户执行的路径
   （`python_worker.py:56-57`）独立 capture 才能让 handler 输出可归因。

### 2.5 host 侧路由（唯一的结构性改动）

把 `_frame_q` 的消费逻辑从「请求耦合」改成「kind 优先，再按 request_id」：

```python
def _route_frame(self, frame, epoch):
    kind = frame.get("kind")
    if kind == "handler.fired":
        self._on_handler_fired(frame)      # 新出口，不要求 Execution record
        return
    if kind in {"handler.snapshot"}:
        self._on_handler_snapshot(frame)   # 审计回执
        return
    # 其余保持现状
```

需要同步改的三个消费点：
- `_execute_blocking`：`request_id` 不匹配的帧不再静默丢，先过 `_route_frame`；
- `_drain_idle`：同理；
- spawn 等待窗口：`ready` 之外的帧同样过 `_route_frame`。

新增事件类型（全为新增，不动既有类型）：

| 事件 | lane | 说明 |
| --- | --- | --- |
| `agent.handler_registered` | ordinary→UI | 注册回执（成功/失败 + 原因） |
| `agent.handler_fired` | ordinary→LLM inbox | handler 触发（含 handler_id、trigger、result） |
| `python.environment_changed`（不变） | — | epoch 切换时 host 主动 **deregister-all** 并广播 |

注意：`handler.fired` 触发的是 `agent.handler_fired` 事件，**不是**
`python.finished`。handler 输出是观察（observation），不是工具结果（tool result），
不得伪造第二个 tool result。

### 2.6 生命周期与版本语义（设计书点名的四个要求）

设计书 §14 点名「稳定 handler 名称、替换／注销、资源清理和版本语义」。逐条：

1. **稳定 handler 名称**：`handler_id` 由 host 分配（`hd_<12hex>`），worker 侧
   禁止自造 id；host 侧保持 `handler_id → 订阅条件` 映射，journal 落库。
   稳定性定义：同一 `handler_id` 重复注册 = 替换（见 2）。
2. **替换／注销**：重复 `handler.register` 同 id = **替换**（幂等替换，非报错），
   worker 回执 `replaced: true`；`handler.deregister` 幂等（不存在也算成功）。
3. **资源清理**：三个时机——(a) epoch 切换（worker 死亡）→ 全部蒸发，host 发
   `python.environment_changed` 通知模型；(b) 显式 deregister；(c) 会话关闭。
   **不做** handler 的跨 epoch 恢复，理由：handler 闭包引用的 namespace 状态本就
   无法跨进程恢复，虚假恢复比诚实失败更危险。
4. **版本语义**：每次注册时 host 记录 `registered_epoch`；若触发时
   `current_epoch != registered_epoch`，host 丢弃并回执
   `agent.error(kind="stale_handler")`。这样 handler 永远不会用旧 worker 的身份
   发言。

### 2.7 handler 事件投递语义

handler 触发投递到 **LLM inbox**，不直接产生 `python.finished`——handler 输出是
**观察（observation）**，不是工具结果（tool result），不得伪造第二个 tool result
（设计书 §13.2 "JSON observation" 一行已明确这条边界）。

---

## 3. 三个最大风险（与调研结论一致）与缓解

### 风险 1：双向不匹配帧静默丢弃

- 依据：`python_host.py:580-582`（执行中）、`:456-475`（空闲）、`:376-390`（启动）；
  `python_worker.py:188-189`。
- 缓解：§2.5 的 `_route_frame` 改造 + 新增测试断言「worker 主动帧必不落入
  `late_frames`」。
- 残余风险：`late_frames` 的语义从"异常信号"弱化为"仅未匹配的 execute 响应"。
  需要更新 `tests/test_python_host.py:254-263` 的既有断言。

### 风险 2：worker 单线程 + socket 写无锁

- 并发 handler 与主循环在同一阻塞 socket 上无锁交错
  （`python_worker.py:173` 与 `:183/:217/:259` 的写点均无锁）。
- 缓解：socket 写全部过一把 `threading.Lock`（小改动，可独立落地并测试）。
- 残余风险：有界线程池仍可能与用户代码争 GIL；interval handler 与 exec 串行。

### 风险 3：handler 状态易失 + journal 零审计

- 依据：`process_tree.py:104-`（无差别杀树）、`journal.py:91-146`（无 handler 表）。
- 缓解：不恢复、只审计。journal 新增 `handler_registry` 表
  （handler_id、event、match、registered_epoch、deregistered_at、fired_count）。
  epoch 切换时逐条记 `deregistered_at`（worker 死亡）。
- 沙箱保证：**没有**。handler 权限 = `python_exec` 权限，这点必须写进工具描述。

### 风险 4：1 MiB 帧上限

- `framing.py:14`：任何经帧内联的 handler 注册（含 code 文本）≤ 1 MiB；worker→host
  的 `handler.fired` 载荷建议上限 64 KiB，超出拒绝（或落 artifact）。
- 缔约：这与现有 stdout 预览 4 KiB + artifact 的分层一致。

---

## 4. 分阶段落地路径（每步独立可测、可回滚）

| 阶段 | 内容 | 验收 |
| --- | --- | --- |
| **S1** | socket 写加锁 + `_route_frame` 骨架（未知 kind 计数不丢弃） | 既有 47 测试全绿；新测试证明主动帧不再计入 `late_frames` |
| **S1.5** | journal 加 `handler_registry` 表（只写不读） | journal 排除 API-key 检查仍通过 |
| **S2** | handler 注册/注销/列表 + 回执事件 | 注册→回执事件；重复注册=替换；deregister 幂等 |
| **S3** | `finished` 触发路径 + `agent.handler_fired` 投递 | 触发→事件按 seq 递增，不伪造 tool result |
| **S4** | `interval` handler | 单调时钟线程；与 exec 串行不冲突 |
| **S5** | 工具描述更新 + 真实模型实验 | 模型能在无凭据脚本中注册一个 handler 并被触发 |

每阶段都在独立分支上，按设计书 §15 的要求保护旧实现与工作区。

## 5. 铺垫性事实：为什么这值得做（成本侧证据）

- 当前成本结构：每 `python.finished` → 1 次模型激活。24 次执行 = 48 次模型调用
  （benchmarks/bench_runtime.py `tool_session` 探针：24 activations → 48 model_calls）。
- 已测量：上下文测量成本已优化约 1100 倍（37.44ms → 0.033ms，859K chars），
  host 空闲唤醒 ~24 倍（44.8ms → 1.9ms）。这两项是"让现有循环更快"；
  handler 桥则是"减少循环圈数"——两者正交，前者已落地，后者是下一个数量级。

---

## 6. 明确不做（本备忘录的边界）

- 不做 handler 的跨 epoch 恢复（理由见 §2.6.3）。
- 不做 handler 之间的通信总线、优先级、学习型调度（§14 表格明确排除）。
- 不做 handler 沙箱或权限提升；handler = `python_exec` 同权。
- 不做 MCP / 插件市场 / 远程消息代理。
- 不提前实现：本文只交付设计 + 风险 + 分阶段路径，S1 的 socket 写加锁是最小的
  安全前置改动，可以单独落地。

## 7. 对既有文档的影响（若 S2 以后落地）

- `Nervipulsa_Design_v0.4.md` §14 的"本版不实现：运行时注册或热插拔事件 handler"
  行需改为"v0.5 候选，见研究备忘录"。
- README 的工具描述与 Execution boundary 需说明 handler 与 `python_exec` 同权。
- `TEST_RESULTS.md` 需新增 handler 注册/触发/epoch 切换的记录。
