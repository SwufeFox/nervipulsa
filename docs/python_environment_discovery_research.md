# Python 环境发现：实现与验证记录

> **当前状态（2026-10-05）：** `python_environment` 仅用受限文件路径、发行版 metadata 和 AST 做静态发现；不导入或执行目标模块，不把 README、Markdown、依赖清单或 setup 文件正文放进模型上下文，不提供安装功能。当前实现的端到端证据见“当前静态实现 live test”。

## 目标

让模型在使用陌生或自定义 Python 库前，先确认 workspace/解释器中是否存在候选模块，并核对源码里静态可见的 API 名称。真正执行 import 或调用库，必须发生在模型明确提交 `python_exec` 请求之后；环境发现本身不执行模块代码。

## 当前实现

- 工具返回 Python 版本、是否处于 virtual environment、依赖清单文件名，以及用户请求模块的候选位置、静态版本、metadata 映射到的发行版和 AST 可见 API 名称。
- 不读取 README、Markdown、requirements、pyproject、setup 文件正文。`project_files` 只返回清单路径与类别，避免任意项目文档内容直接进入模型上下文。
- 不调用 `find_spec`、`import_module`，不改 `sys.path`。发行版索引只扫描最多 32 个解释器搜索根中的 distribution metadata 和 `top_level.txt`，支持导入名与发行版名不同；workspace 源码中只有字面量 `__version__` 才能静态识别。若导入名映射到多个不同发行版版本，结果会标记歧义。
- 查询上限：20 个模块名、40 个 API 名、32 个唯一搜索根（每根最多索引 512 个发行版）、每模块最多 8 个候选、单源文件 256 KiB、累计源码 1 MB、最终事件 48 KB。返回路径为 workspace 相对路径或 `<python-path-N>` 标签，不暴露完整用户目录。
- workspace 路径经过 resolve containment 检查；越界链接目标不会进入结果。查询错误以 `status=failed` 事件回执，不应中断 activation。
- 自动安装不支持。静态 AST 只能显示源码中直接可见的声明；动态导出、扩展模块或运行时注册 API 可能无法识别。`found=true` 也不等于导入或 API 调用一定成功，后续仍需 smoke test。

## 安全审查历史

最初版本直接对模型提供的模块名执行 `find_spec` 和 `import_module`。模块导入可能执行包和父包顶层代码，因此那版不能称为只读。该版的 `quirkmath` live run 结果只作为历史功能观察保留，不作为当前静态工具的只读性、安全性或行为证据。

静态化后又发现 workspace 文档正文进入上下文、链接 containment 的 Windows 集成测试无法执行等问题。文档与依赖清单现在仅列文件名；链接越界有路径 containment 和确定性模拟测试覆盖。当前 Windows 测试环境中，集成用例调用 junction 命令失败，实际 pytest 输出为 `'chcp' is not recognized`，因此 OS 级链接集成测试跳过。这个 skip 不视为链接行为的实机证明；后续应在可创建 symlink/junction 的 runner 上复验。

## 当前静态实现 live test

**环境：** 2026-10-05，本地 `http://127.0.0.1:3425/v1` 与 `codex/gpt-6-luna`，使用此前用户授权配置；密钥未输出或写入记录。

临时 workspace 含 `widgetkit.py`（字面量版本 `0.4.2`、顶层 `scale` 函数）和空 `pyproject.toml`。任务明确要求先用 `python_environment` 检查，再通过 `python_exec` 导入并验证 `scale(3) == 9`，不安装依赖。

本轮 CLI 退出码为 0。SQLite journal 有 7 个事件，顺序包含 `python.environment_discovered` 后接 `python.requested`，证明当前版本先执行静态发现再执行 smoke test。发现结果为 `widgetkit.found=true`、版本 `0.4.2`、`api.scale=true`、`install_supported=false`；Python execution succeeded，耗时 15 ms，stdout 验证结果为 `True`。共 3 次 committed activation、2 次工具调用、1 次 execution；journal 中 provider usage 合计 14,134 tokens。临时 workspace 在读取 journal 后清理。

这是当前静态实现的一次本地模型行为证据：模型遵循了先发现再显式执行的流程；不证明所有模型都会如此，也不证明动态 API、所有包布局或高负载表现。

## 自动化验证

当前环境发现相关回归覆盖：

- 模块顶层副作用未在 discovery 阶段运行；
- 不调用导入解析 API、不改变 `sys.path`；
- 模块名、数量、总结果尺寸限制；
- 项目依赖清单、README 和文档正文不进入返回结果；
- workspace 自定义模块静态 API 识别；
- metadata import-name 到发行版名称映射；
- 搜索根数量上限、路径隐私与 containment；
- 解析异常通过失败事件送达模型上下文。

最新验证：核心环境发现筛选 `13 passed, 1 skipped`；全量 `python -m pytest` **77 passed, 1 skipped in 47.32s**。junction skip 的 pytest 原因是 `'chcp' is not recognized`；不计作真实 OS 链接 containment 通过。

环境扫描现经 `asyncio.to_thread` 执行，不再同步占住 runtime event loop。阻塞扫描回归确认扫描未释放时 loop callback 仍能运行，第二条用户消息已被接受；释放后发现事件与消息进入后续 provider 请求。LLMActor 仍 await 当前发现调用，因此这不是并行 activation 或多任务工具调度。

## 后续问题

1. 在允许创建 junction/symlink 的 Windows runner 上验证真实链接 containment。
2. 用一组动态 re-export、namespace package、二进制 extension package 测量 AST 发现的假阴性。
3. 用多份陌生自定义库任务评估模型是否能把静态线索转化为正确调用，测量无效 import、修复轮数、执行次数及用户可见延迟。
4. 只有用户用例证明有必要时，才设计受控安装；需先定义项目隔离环境、锁文件、来源限制与回滚。
