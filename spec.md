# Pipeline DAG 执行引擎需求规格书

## 1. 项目概述
构建一个基于 Python 的高性能、可扩展的任务编排工具。该工具通过 YAML 定义复杂的 DAG（有向无环图）工作流，支持任务插件化、细粒度执行控制（Pipeline/Step/Task）、以及一个非阻塞的交互式 REPL 终端，用于实时监控、干预和错误修复。

---

## 2. 核心功能需求

### 2.1 任务插件化系统 (Task Plugin System)
*   **统一接口**: 所有任务必须继承 `BaseTask` 抽象基类。
*   **动态加载**: 支持在 YAML 中通过类路径（如 `module.TaskClass`）指定执行器，引擎需在运行时动态实例化。
*   **数据契约**: 任务间通过 `input.json` 和 `output.json` 传递数据，需保证相同类型任务的 Schema 一致性。

### 2.2 多层级执行控制 (Granular Execution)
*   **加载与持久化**: 支持从指定路径加载 YAML，并在本地保存已加载的 Pipeline 配置与状态。
*   **多层级结构**:
    *   **Pipeline**: 顶层容器，包含多个步骤。
    *   **Step (步骤)**: 线性执行序列。支持 `skip` 模式，跳过时需从 `./manual_data/` 加载预设输出。
    *   **Task (任务)**: Step 内的最小执行单元，支持复杂的 DAG 依赖关系。
*   **四级运行模式**:
    1.  **端到端运行**: 执行整个 Pipeline 的所有步骤。
    2.  **指定步骤运行**: 仅执行特定的 Step（需校验前置依赖）。
    3.  **指定任务运行**: 仅执行某个 Step 下的特定 Task。
    4.  **断点续传**: 当 Pipeline 运行出错停止后，支持在人工干预后从失败点继续运行。
*   **并发调度**: 
    *   同步骤内，无依赖关系的多个任务必须支持并行执行。
    *   基于拓扑排序自动管理执行顺序。
*   **数据流向**: 下游任务可引用上游任务生成的 JSON 数据作为输入。


### 2.3 状态管理与干预 (Lifecycle & Intervention)
*   **中止与恢复**: 
    *   支持对正在运行的 **Pipeline 实例** 进行实时中止（Abort）。`stop` 命令以 instance_id 为目标，中止整个 run。
    *   支持对已中止或因错停止的流程进行恢复运行（Resume）。
*   **错误修复 (Manual Fix)**: 
    *   当任务出错停止时，支持用户通过指令手动补充 `input` 数据或直接提供 `output` 结果，以驱动后续流程。

### 2.4 可观测性与进度监控
*   **动态进度注入**: 任务可动态推送 0-100% 的进度值。
*   **多维度查询**:
    *   **Pipeline 视图**: 查看总进度、整体结果、开始/结束时间。
    *   **Step 视图**: 查看该步骤内所有任务的执行情况、依赖关系、错误信息。
    *   **Task 视图**: 查看具体任务的输入输出快照、实时进度及详细堆栈日志。

---

## 3. 命令行交互设计 (CLI & REPL)

系统采用“子命令 + 交互式 REPL”的混合模式。

### 3.1 核心指令集
| 指令 | 目标 | 说明 |
| :--- | :--- | :--- |
| `load <path>` | 系统 | 加载并保存一个 Pipeline 配置文件。 |
| `list [--pipeline]` | 系统 | 列出已注册的 pipeline（pipeline_id / type / name）。默认行为。 |
| `list --instance` | 系统 | 列出运行实例（pipeline_id / instance_id / status）。 |
| `start <id> [--step S] [--task T]` | Pipeline/Step/Task | 启动执行。`--step`/`--task` 支持细粒度启动。 |
| `stop <instance_id>` | Pipeline 实例 | 中止指定的 pipeline 运行实例（整个 run）。 |
| `resume <instance_id>` | Pipeline 实例 | 恢复被中止或失败的 pipeline 实例。 |
| `status <instance_id>` | Pipeline 实例 | 查看指定 pipeline 实例的整体进度和结果。 |
| `inspect <instance_id>` | Pipeline 实例 | 查看 Step 或 Task 的详细错误、输入输出及进度。 |
| `fix <instance_id>` | 错误处理 | 手动注入数据以修复出错的任务状态（→ Fixed 或 → New）。 |

### 3.2 交互特性
*   **非阻塞 REPL**: 任务在后台异步执行，前台 REPL 始终保持响应，允许用户在任务执行时输入查询或干预指令。
*   **终端风格**: 采用文本行交互，支持命令补全与历史记录。
*   **Tab 补全与联想**: REPL 支持 Tab 键补全及边输入边联想（complete_while_typing）。补全范围覆盖：命令名、pipeline_id（已 `load` 的）、instance_id（已启动的实例，含 `pipeline=<pid> | status=<status>` 旁注）、`--step`/`--task` 参数值（从 PipelineSpec 动态读取）、以及 `load` / `fix --output` / `fix --input` 的文件路径。
*   **stop 范围**: `stop` 始终中止整个实例；task 级 Paused 状态仅由调度器内部产生（abort_event 处理路径），不作为用户接口暴露。

---

## 3. 技术实现建议

| 组件 | 推荐技术 | 理由 |
| :--- | :--- | :--- |
| **异步调度** | `asyncio` | 核心引擎，处理非阻塞任务分发与 REPL 交互。 |
| **图运算** | `NetworkX` | 验证 DAG 合法性，计算拓扑排序及任务依赖。 |
| **终端 UI** | `Rich` | 渲染多层级进度条、状态表格和彩色日志。 |
| **REPL 构建** | `prompt_toolkit` | 实现专业级的交互式 Shell 体验。 |
| **状态存储** | `JSON` | 持久化已加载的 Pipeline 配置及运行快照。 |
| **数据验证** | `Pydantic` | 强类型校验任务间的输入输出数据。 |

---

## 4. 关键设计约束 (Design Constraints)

1.  **解耦**: 执行引擎不应感知具体的业务逻辑，仅负责任务的调度和数据传递。
2.  **状态机模型**: 必须为 Pipeline、Step、Task 定义严谨的状态机（`New`, `Running`, `Paused`, `Success`, `Failed`, `Skipped`, `Fixed`）。状态迁移规则：仅允许合法的前置状态（如 `finish_task` 仅从 RUNNING 迁移，其余状态静默忽略），防止并发竞争导致的非法覆盖。进程重启后，遗留 RUNNING 任务自动复位为 FAILED；`resume` 重新调度 FAILED / PAUSED 任务。
3.  **线程/协程安全**: 确保 REPL 读取状态时，不会与后台写入状态产生竞态冲突。
4.  **原子化存储**: 每个 Task 完成后，其结果必须立即落盘，确保在系统崩溃后可从该点恢复。进程重启后，遗留在 RUNNING 状态的任务必须自动复位为 FAILED，以便 `resume` 重调度。
5.  **环境隔离**: 任务执行过程中的异常不应导致整个 REPL 进程崩溃。
6.  **容错性**: 当 Step 被跳过时，引擎必须强制检查前置依赖数据是否已通过手动方式补全。

---

## 5. 给 Claude 的开发建议 (CLAUDE.md)

*   **Design Phase (Opus)**: 重点设计 **异步调度器 (Scheduler)** 与 **状态管理器 (StateManager)**。Opus 应负责处理如何从“手动补全数据”状态恢复到“运行”状态的复杂逻辑。
*   **Coding Phase (Sonnet)**: 负责实现 YAML 解析器、子命令参数定义以及基于 `Rich` 的终端界面渲染。