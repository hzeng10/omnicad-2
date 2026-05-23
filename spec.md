# Pipeline DAG 执行引擎需求规格书

## 1. 项目概述
构建一个基于 Python 的高性能、可扩展的任务编排工具。该工具通过 YAML 定义复杂的 DAG（有向无环图）工作流，支持任务插件化、细粒度执行控制（Pipeline/Step/Task）、以及一个非阻塞的交互式 REPL 终端，用于实时监控、干预和错误修复。

---

## 2. 核心功能需求

### 2.1 任务插件化系统 (Task Plugin System)
*   **统一接口**: 所有任务必须继承 `BaseTask` 抽象基类。
*   **动态加载**: 支持在 YAML 中通过类路径（如 `module.TaskClass`）指定执行器，引擎需在运行时动态实例化。
*   **数据契约**: 任务间通过 `input.json` 和 `output.json` 传递数据，需保证相同类型任务的 Schema 一致性。Task 可通过 `output: PATH` 在 YAML 中声明结果 JSON 的对外落盘位置（可选）；通过 `output_mode` 控制写入语义（见 §2.2 并行共享输出）。
*   **自动加载（默认）**: CLI 启动时（REPL 与子命令均适用）自动扫描 `./pipelines/*/pipeline.yaml`（一级深度）并注册，等价于逐一调用 `load` 命令。可通过 `--pipelines-dir DIR`（或 env `PIPELINE_AUTOLOAD_DIR`）改变目录，`--no-autoload`（或 env `PIPELINE_NO_AUTOLOAD`）禁用。单个 YAML 解析失败时跳过并写 WARNING 到 stderr，不阻断其余文件或子命令本身。

### 2.2 多层级执行控制 (Granular Execution)
*   **加载与持久化**: 支持从指定路径加载 YAML，并在本地保存已加载的 Pipeline 配置与状态。
*   **instance_id 格式**: 每个运行实例的唯一标识格式为 `<pipeline_id>_yyyyMMdd-hhmmss_<4digit>`（UTC 时间，4 位随机数字），例：`cad_drawing_pipeline_20260513-093024_7392`。由 `start` 命令打印，`stop/resume/status/inspect/fix` 命令接受此格式作为参数。
*   **多层级结构**:
    *   **Pipeline**: 顶层容器，包含多个步骤。
    *   **Step (步骤)**: 线性执行序列。支持 `skip` 模式，跳过时需从 `./manual_data/` 加载预设输出（若 step 配置了 `output: PATH`，则改为从该路径读取）。Step 完成后若配置 `output: PATH`，引擎将 `{task_id: output}` 聚合结果写入该路径。
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
*   **并行共享输出（Shared Output）**：同一 Step 内的多个并行 Task 可安全地将结果合并写入同一个 JSON 文件。通过 `output_mode` 字段控制：

    | `output_mode` | 语义 | 适用场景 |
    |---|---|---|
    | `overwrite`（默认） | 整体覆写目标文件（MIRROR）；单 task 独占路径的标准用法 | 每个 task 有独立输出文件 |
    | `accumulate` | 以 `task_id` 为 key 累积合并到共享文件：`{task_id: output, ...}` | 多个并行 task 共享同一输出路径 |

    **安全保障**：
    - 同一 Step 内若多个 Task 声明相同 `output:` 路径，但未设置 `output_mode: accumulate`，YAML 加载时立即报 `ValidationError`（防止静默数据丢失）。
    - 所有到同一路径的 accumulate 写入由 per-path `asyncio.Lock` 序列化，防止并发读改写竞争（TOCTOU）。

    **任务代码中的直接写入**：async task（`execute()`）可调用 `async with self.shared_json(path) as data:` 对共享 JSON 文件执行安全的读改写。该 API 使用与 MIRROR 写入相同的 per-path 锁注册表，确保引擎写入和任务直接写入不互相竞争。

    **YAML 示例**（`cad_identify_cost_estimation` 的 `recognize` step，3 个并行识别任务共享一个输出文件）：
    ```yaml
    steps:
      - id: recognize
        name: "多类设备并行识别"
        max_parallelism: 3
        tasks:
          - id: rec_building
            plugin: pipelines.cad_identify_pipeline.tasks.RecBuildingTask
            depends_on_steps: [split_subgraph]
            output: results/detections.json
            output_mode: accumulate        # 必须三个都声明

          - id: rec_cable
            plugin: pipelines.cad_identify_pipeline.tasks.RecCableTask
            depends_on_steps: [split_subgraph]
            output: results/detections.json
            output_mode: accumulate

          - id: rec_schematic
            plugin: pipelines.cad_identify_pipeline.tasks.RecSchematicTask
            depends_on_steps: [split_subgraph]
            output: results/detections.json
            output_mode: accumulate
    ```
    运行完成后 `results/detections.json` 的内容：
    ```json
    {
      "rec_building":  { "entity_type": "building",  "entities": [...] },
      "rec_cable":     { "entity_type": "cable",     "entities": [...] },
      "rec_schematic": { "entity_type": "schematic", "entities": [...] }
    }
    ```


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
*   **运行日志**: 每个 instance 一份 `<run_dir>/run.log`，捕获引擎生命周期事件、`pipeline_engine` 包内 Python logging 输出、Task 自定义 `self.logger` 日志，以及 task 内 stdout/stderr 写入。格式：`<UTC时间>  <级别>  [<step/task>]  <消息>`。ERROR 行在 REPL `log` 命令中以红色高亮。resume 时日志追加到同一文件。

---

## 3. 命令行交互设计 (CLI & REPL)

系统采用“子命令 + 交互式 REPL”的混合模式。

### 3.1 核心指令集

**输出模式说明**：`omnicad <subcommand>` 一次性子命令默认输出单个 JSON 对象（见 §3.3）到 stdout，便于 AI Agent 解析。REPL 交互模式（无子命令进入的提示符模式）保持 Rich 文本渲染，行为不变。

| 指令 | 目标 | 说明 |
| :--- | :--- | :--- |
| `load <path>` | 系统 | 加载并保存一个 Pipeline 配置文件。 |
| `list [--pipeline]` | 系统 | 列出已注册的 pipeline（pipeline_id / type / name）。默认行为。 |
| `list --instance` | 系统 | 列出运行实例（pipeline_id / instance_id / status）。 |
| `start <id> [--step S] [--task T]` | Pipeline/Step/Task | 启动执行。`--step`/`--task` 支持细粒度启动；始终阻塞到完成。 |
| `stop <instance_id>` | Pipeline 实例 | 中止指定的 pipeline 运行实例（整个 run）。 |
| `resume <instance_id>` | Pipeline 实例 | 恢复被中止或失败的 pipeline 实例。 |
| `status <instance_id>` | Pipeline 实例 | 查看指定 pipeline 实例的整体进度和结果。 |
| `inspect <instance_id> [--step S] [--task T]` | Pipeline 实例 | 查看 Step 或 Task 的详细错误、输入输出及进度。 |
| `fix <instance_id>` | 错误处理 | 手动注入数据以修复出错的任务状态（→ Fixed 或 → New）。 |
| `log <instance_id> [--tail N] [--offset N] [--all] [--errors-only]` | Pipeline 实例 | REPL：分页显示 run.log，ERROR 行红色高亮；CLI 子命令：JSON 结构化行记录。 |
| `clear` | REPL | 清除当前控制台显示内容。 |

### 3.2 CLI JSON 输出契约

所有 `omnicad <subcommand>` 一次性子命令输出以下信封格式（扁平 + `ok` 字段），以 **`indent=2`** 格式化输出，便于人工阅读，同时保持 `json.loads()` 兼容。

**成功**：
```json
{
  "ok": true,
  "command": "list",
  "scope": "pipeline",
  "pipelines": [...]
}
```

**失败（exit code = 1）**：
```json
{
  "ok": false,
  "command": "start",
  "error": {
    "message": "pipeline 'x' 未加载",
    "type": "PipelineError",
    "pipeline_id": "x",
    "step_id": null,
    "task_id": null
  }
}
```

每个命令的 payload 字段：

| 命令 | 成功 payload 字段（除 `ok`/`command`） |
| :--- | :--- |
| `load` | `loaded: [{path, pipeline_id, ok, error?}]` |
| `lint` | `path, pipeline_id, valid` |
| `list --pipeline` | `scope: "pipeline", pipelines: [{pipeline_id, type, name}]` |
| `list --instance` | `scope: "instance", instances: [{pipeline_id, instance_id, status}]` |
| `start` | `runs: [{pipeline_id, run_id, ok, final_status}]` |
| `stop` | `stopped: instance_id` |
| `resume` | `resumed: run_id, final_status` |
| `status` | `state: PipelineRunState`（含完整 steps/tasks 树，datetime 为 ISO 字符串） |
| `inspect` | 无 step：`state: ...`；有 step：`step_id, step_status, tasks: [...]`；有 step+task：`task: {id, status, progress, error, input, output, log_tail, ...}` |
| `fix` | `instance_id, task, mode: "output"\|"input", new_status: "fixed"\|"new"` |
| `log` | `run_id, log_path, total, start, end, lines: [{timestamp, level, ctx, message, raw}]` |

### 3.3 交互特性
*   **非阻塞 REPL**: 任务在后台异步执行，前台 REPL 始终保持响应，允许用户在任务执行时输入查询或干预指令。
*   **终端风格**: 采用文本行交互，支持命令补全与历史记录。
*   **Tab 补全与联想**: REPL 支持 Tab 键补全及边输入边联想（complete_while_typing）。补全范围覆盖：命令名、pipeline_id（已 `load` 的）、instance_id（已启动的实例，含 `pipeline=<pid> | status=<status>` 旁注）、`--step`/`--task` 参数值（从 PipelineSpec 动态读取）、以及 `load` / `fix --output` / `fix --input` 的文件路径。补全行为细节：
    * instance_id 候选按启动时间戳**倒序**排列（最新实例在前），适用于 `status` / `inspect` / `stop` / `resume` / `log` / `fix` 等所有接受 `ref` 参数的命令。
    * `start` / `inspect` 命令中，若已输入 `--step <step_id>`，则 `--task` 补全仅列出该 step 内的任务（以裸 task_id 形式显示，不带 `step_id/` 前缀）；未输入 `--step` 时，以 `step_id/task_id` 格式列出全部任务（原有行为）。
*   **stop 范围**: `stop` 始终中止整个实例；task 级 Paused 状态仅由调度器内部产生（abort_event 处理路径），不作为用户接口暴露。

### 3.4 HTTP REST API（serve 模式）

`omnicad serve [--host HOST] [--port PORT] [--workspace PATH]` 启动 FastAPI HTTP 服务器（默认 127.0.0.1:8765，无鉴权），提供与 CLI 子命令语义一致的 REST 接口。

**工作区互斥锁**：`serve` 启动时在 `.pipeline_runs/.serve.lock` 上获取 `fcntl` 排他锁；同一 workspace 的第二个 serve 进程立即以 exit code 1 退出。

**核心端点**：

| 方法 | 路径 | 对应 CLI |
| :--- | :--- | :--- |
| `POST` | `/runs` | `start`（异步，立即返回 202） |
| `GET`  | `/runs` | `list --instance` |
| `GET`  | `/runs/{run_id}` | `status` |
| `POST` | `/runs/{run_id}:stop` | `stop` |
| `POST` | `/runs/{run_id}:resume` | `resume`（异步，立即返回 202；响应仅含 `resumed`，无 `final_status`） |
| `GET`  | `/runs/{run_id}/events` | SSE 实时事件流 |
| `POST` | `/runs/{run_id}/tasks/{step_id}/{task_id}:fix` | `fix` |
| `GET`  | `/runs/{run_id}/log` | `log` |
| `POST` | `/lint` | `lint` |
| `POST` | `/pipelines` | `load` |
| `GET`  | `/pipelines` | `list --pipeline` |

**SSE 事件流**（`GET /runs/{run_id}/events`）：订阅状态变更事件，每 25 秒发送一次心跳保活（`: heartbeat`），检测到客户端断开后停止推送。pipeline 到达终态时推送 `event: terminal` 后关闭流。

**resume 守卫（C2）**：REST 的 `:resume` 拒绝对已处于 `SUCCESS` / `FIXED` / `SKIPPED` 终态的 run 发起 resume，防止重复执行有副作用的任务。CLI `resume` 子命令不受此限制（允许重新运行已完成的 run）。

**REST resume 非阻塞（H2）**：`:resume` 在触发后台调度后立即返回 202 Accepted，响应体仅含 `{"ok": true, "command": "resume", "resumed": "<run_id>"}`，不含 `final_status`（因为 run 尚未完成）。客户端通过 `GET /runs/{run_id}` 轮询或订阅 SSE 事件流跟踪执行进度。CLI `resume` 子命令仍阻塞到完成并返回 `final_status`，行为不变。

**响应信封**：所有端点返回与 CLI JSON 子命令一致的 `{"ok": bool, "command": "...", ...payload}` 信封格式，由 `envelope_ok()` / `envelope_err()` 统一构建（定义在 `pipeline_engine/api/schemas.py`）。

### 3.5 CLI 品牌定制 (Branding)

CLI 名称、REPL 提示符、启动 Logo、版本号、副标题统一由 `config/branding.json` 配置；Schema 定义在 `config/branding.schema.json`（JSON Schema Draft 2020-12）。修改该文件无需重新构建或安装。

**启动横幅显示规则**：

| 调用方式 | 横幅显示 |
|---|---|
| `omnicad`（无子命令，进入 REPL） | 显示 — Logo + 版本 + 描述居中，圆角 Panel 包裹 |
| `omnicad <subcommand>`（一次性子命令） | 不显示 — stdout 保持纯 JSON，stderr 保持静默 |

**配置字段（`config/branding.json`）**：

| 字段 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `name` | string | ✓ | — | CLI 内部标识符（信息性；实际二进制名由 `pyproject.toml` 决定） |
| `display_name` | string | ✓ | — | 横幅中展示的人类可读名称，如 `OmniCAD` |
| `prompt` | string | ✓ | — | REPL 提示符根字符串，自动追加 `> ` |
| `version` | string | ✓ | — | 版本字符串；`"@auto"` 从安装包元数据自动推导 |
| `description` | string | ✓ | — | 显示在版本号下方的短副标题 |
| `logo` | string | — | `""` | 多行 ASCII logo，空字符串禁用 logo 块 |
| `logo_style` | string | — | `light_steel_blue1` | Rich 样式，作用于 logo 字形 |
| `border_style` | string | — | `grey50` | Rich 样式，作用于 Panel 边框 |
| `tagline_style` | string | — | `grey70` | Rich 样式，作用于版本 / 描述行 |
| `box_style` | string | — | `ROUNDED` | Panel 边框形状：`ROUNDED` / `HEAVY` / `DOUBLE` / `SQUARE` / `MINIMAL` |

详见 design.md §6.16。

### 3.6 多语言支持 (i18n)

CLI 所有用户可见字符串（命令帮助、REPL 提示、状态标签、错误消息）均通过 `pipeline_engine/i18n.py` 的 `t("key")` 函数提供，支持运行时语言切换，无需重新构建。

**语言解析优先级（高→低）**：

1. `OMNICAD_LANG` 环境变量
2. `config/i18n.json` 配置文件 `{"language": "..."}` 
3. 系统 locale 自动探测（`zh_*` → `zh_CN`，`en_*` → `en`）
4. 硬编码后备：`zh_CN`

**横幅 / API 作用域说明**：

| 内容 | 是否受 i18n 影响 |
|---|---|
| CLI `--help` 文本、REPL 提示符、状态标签 | ✓ 受语言设置影响 |
| `config/branding.json` 品牌字段（用户自定义） | ✗ 由用户自行管理 |
| JSON 响应信封（`ok`、`command` 等字段名） | ✗ 固定为 English（程序化接口，不面向自然语言） |

**扩展点**：在 `config/i18n/` 目录下新建 `<locale>.json` 平铺键值文件，无需修改任何代码。缺失键自动回退 `zh_CN.json`。

当前支持：`zh_CN`（默认）、`en`。详见 design.md §6.17。

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
3.  **状态机守卫强化**：`start_pipeline()` / `start_step()` 仅允许从 `NEW` 状态发起（非 NEW 状态时抛 `PipelineError`），防止 SUCCESS → RUNNING 非法迁移。`recover_task`（fix 流程）仅允许从 `FAILED` / `PAUSED` / `NEW` 发起，若任务处于 `RUNNING` 则立即抛 `PipelineError`，防止并发改写。
4.  **线程/协程安全**: 确保 REPL 读取状态时，不会与后台写入状态产生竞态冲突。
5.  **原子化存储**: 每个 Task 完成后，其结果必须立即落盘，确保在系统崩溃后可从该点恢复。进程重启后，遗留在 RUNNING 状态的任务必须自动复位为 FAILED，以便 `resume` 重调度。
6.  **环境隔离**: 任务执行过程中的异常不应导致整个 REPL 进程崩溃。
7.  **容错性**: 当 Step 被跳过时，引擎必须强制检查前置依赖数据是否已通过手动方式补全。若 step 配置了 `output: PATH`，则改为校验该路径存在；否则回退到 `manual_data/<step_id>/output.json`。
8.  **只读恢复**: `status` / `inspect` / `log` / `list --instance` 等查询命令从磁盘恢复 run 状态时，**必须跳过** `demote_orphans_sync` 降级操作（即 `restore_writeback=False`）。该约束防止 CLI 子命令与 REPL 进程并发运行时相互污染 `state.json`。只有 `resume` / `fix` 命令需要降级并写回（`restore_writeback=True`）。
9.  **视图与状态解耦**: CLI JSON 输出与 REPL 终端渲染必须通过统一的 view-model 层（`pipeline_engine.view_model`）从 runtime state 派生，不允许直接调用 `state.model_dump()` 或手工拼装展示字典作为对外接口。view-model 与 runtime state 在字段集合/顺序/语义上保持透明等价，以防止两套渲染长尾发散。

---

## 5. 给 Claude 的开发建议 (CLAUDE.md)

*   **Design Phase (Opus)**: 重点设计 **异步调度器 (Scheduler)** 与 **状态管理器 (StateManager)**。Opus 应负责处理如何从“手动补全数据”状态恢复到“运行”状态的复杂逻辑。
*   **Coding Phase (Sonnet)**: 负责实现 YAML 解析器、子命令参数定义以及基于 `Rich` 的终端界面渲染。