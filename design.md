# Pipeline DAG Engine — 技术方案设计（V3）

> 评审记录：
> V1 → V2：强化 `fix --output` 的"跳过失败步骤继续执行"语义；双入口 BaseTask；工作目录默认 CWD + `--workspace`；`resume` 双模式；覆盖率 90%；支持多 Pipeline 并发。
> V2 → V3：CAD 示例具体化为可演示样板（§10），包含 7 个 mock 任务、混合双入口、进度推送、失败恢复演示。

**维护约定**：需求或设计变更时必须**同时更新 `spec.md` 和本文件**，保证两份文档同步。

---

## Context

构建一个与业务解耦的、基于 Python 3.10+ 的 DAG 任务编排命令行工具。用户用 YAML 描述工作流，引擎负责：YAML 解析与 schema 校验、DAG 合法性检查、任务并发调度、状态持久化、中止/恢复/断点续传、**通过补齐 output.json 跳过失败步骤继续执行**、多 Pipeline 并行运行、以及一个非阻塞的 REPL 监控界面。本设计仅作引擎本体，DXF 等业务任务作为外部插件存在。

---

## 已确认的关键决策

| 维度 | 选型 |
|---|---|
| 任务执行模型 | **async + 线程池兜底**：`BaseTask` 暴露 `async def execute()`；为 CPU 密集任务提供 `run_sync()` 钩子，引擎自动通过 `asyncio.to_thread` 卸载到线程池 |
| CLI 形态 | **REPL 优先 + 一次性子命令**：默认 `pipeline_cli` 进入 REPL；同时支持 `pipeline_cli run/lint/...` 一次性调用 |
| CAD 示例 | **作为可演示样板交付**：`examples/cad_pipeline/` 提供 7 个 mock 任务（含 8s、10s 两个长时任务） |
| 工作目录 | **默认在 CWD 下** `./.pipeline_runs/...`；支持全局 `--workspace <path>` 自定义 |
| 多 Pipeline | **多实例并行**：单进程内多个 RunContext，各自独立 scheduler / state |
| 失败修复 | **fix --output 是核心机制**：补齐 output.json → 任务置 RECOVERED → 自动继续下游 |
| Resume 模式 | 默认仅重调度 Failed；`--include-paused` 同时调度 Paused |
| 测试覆盖率 | ≥ 90% |

---

## 1. 技术选型（含理由）

| 组件 | 库 | 理由 |
|---|---|---|
| 异步调度 | `asyncio` (stdlib) | 与 REPL 共享事件循环，零额外依赖 |
| 线程池兜底 | `asyncio.to_thread` (stdlib) | CPU 密集任务不阻塞事件循环 |
| DAG 算法 | `networkx >= 3.0` | 成熟稳定，提供 `is_directed_acyclic_graph`、`topological_sort`、`ancestors` |
| Schema 校验 | `pydantic >= 2.5` | 强类型、错误信息友好、v2 性能好 |
| YAML | `PyYAML >= 6.0` | 事实标准 |
| 终端 UI | `rich >= 13.0` | 进度条 / 表格 / 彩色日志，asyncio 友好 |
| 交互式 REPL | `prompt_toolkit >= 3.0` | 命令补全、历史、与 asyncio 原生集成（`PromptSession.prompt_async`） |
| 子命令解析 | `typer >= 0.12`（基于 click） | 类型安全的 CLI、自动生成 help |
| 测试 | `pytest >= 8.0` + `pytest-asyncio >= 0.23` + `pytest-cov` | 标准组合 |
| 文件原子写 | stdlib `os.replace` | 跨平台原子重命名 |

---

## 2. 项目目录结构

```
omnicad-2/
├── pyproject.toml
├── CLAUDE.md
├── spec.md                         # 需求规格（需求变更时同步更新）
├── design.md                       # 本文件（设计变更时同步更新）
├── pipeline_engine/                # 引擎本体（与业务解耦）
│   ├── __init__.py
│   ├── cli.py                      # typer 入口
│   ├── repl.py                     # prompt_toolkit + asyncio REPL
│   ├── models/
│   │   ├── pipeline_spec.py        # YAML schema 模型
│   │   └── runtime_state.py        # 运行时状态模型
│   ├── core/
│   │   ├── base_task.py            # BaseTask 抽象基类
│   │   ├── plugin_loader.py        # 动态加载 module.ClassName
│   │   ├── yaml_parser.py          # YAML → Pydantic
│   │   ├── dag_validator.py        # NetworkX 校验 & 拓扑排序
│   │   ├── scheduler.py            # AsyncScheduler（单 run 调度）
│   │   ├── run_context.py          # RunContext：单次 run 的容器
│   │   ├── run_manager.py          # RunManager：多 run 协调器
│   │   ├── state_manager.py        # StateManager（asyncio.Lock 保护，按 run_id 隔离）
│   │   ├── storage.py              # 工作目录读写、原子落盘
│   │   └── errors.py               # PipelineError 体系
│   └── builtin/
│       └── manual_data_loader.py   # skip 模式从 manual_data/ 加载
├── examples/
│   └── cad_pipeline/               # CAD 设备成本汇总示例（mock 版）
│       ├── __init__.py
│       ├── pipeline.yaml
│       ├── tasks.py                # 7 个 mock 任务，含长时任务与进度推送
│       ├── schemas.py              # Pydantic InputModel / OutputModel
│       ├── mock_data/
│       │   ├── dxf_entities.json
│       │   ├── subgraphs.json
│       │   ├── recognized_items.json
│       │   └── recover_cable.json  # 失败恢复演示用兜底数据
│       └── README.md
└── tests/
    ├── conftest.py
    ├── unit/
    │   ├── test_yaml_parser.py
    │   ├── test_dag_validator.py
    │   ├── test_plugin_loader.py
    │   ├── test_state_manager.py
    │   ├── test_storage.py
    │   └── test_base_task.py
    ├── integration/
    │   ├── test_scheduler.py
    │   ├── test_skip_mode.py
    │   ├── test_abort_resume.py
    │   ├── test_fix_command.py
    │   ├── test_multi_pipeline.py
    │   └── test_repl_commands.py
    └── e2e/
        ├── test_cad_example.py
        └── test_cad_failure_recovery.py
```

---

## 3. Pipeline YAML Schema

### 3.1 字段定义

```yaml
version: "1.0"
pipeline:
  id: cad_cost_estimation          # 唯一标识，正则 [a-z][a-z0-9_]*
  name: "CAD 设备成本汇总"
  description: "..."               # 可选
  max_parallelism: 4               # 全局并发上限；step 级可覆盖

steps:                             # 线性序列，按数组顺序执行
  - id: parse_dxf
    name: "解析 DXF"
    skip: false                    # true 时强制从 <workspace>/manual_data/<step_id>/output.json 加载
    max_parallelism: 2             # 可选，覆盖全局
    tasks:
      - id: read_dxf
        plugin: examples.cad_pipeline.tasks.ReadDxfTask
        config:                    # 透传给任务的静态参数
          dxf_path: ./input.dxf
        inputs: {}                 # 静态输入（与上游产出合并）

      - id: parse_entities
        plugin: examples.cad_pipeline.tasks.ParseEntitiesTask
        depends_on: [read_dxf]    # step 内任务级依赖

  - id: split_subgraph
    depends_on_steps: [parse_dxf] # 跨 step 依赖
    tasks:
      - id: split
        plugin: examples.cad_pipeline.tasks.SplitSubgraphTask
        depends_on_steps: [parse_dxf]

  - id: recognize
    max_parallelism: 3
    tasks:                         # 三个任务无依赖，全部并行
      - { id: rec_building, plugin: ..., depends_on_steps: [split_subgraph] }
      - { id: rec_cable,    plugin: ..., depends_on_steps: [split_subgraph] }
      - { id: rec_schematic,plugin: ..., depends_on_steps: [split_subgraph] }

  - id: aggregate
    tasks:
      - id: merge
        plugin: examples.cad_pipeline.tasks.MergeAndDedupTask
        depends_on_steps: [recognize]
```

### 3.2 依赖语义（含失败补齐机制）

- `depends_on: [task_id, ...]`：step 内任务级依赖。引擎读取上游 `output.json`，注入为 `inputs[task_id]`。
- `depends_on_steps: [step_id, ...]`：跨 step 依赖。引擎聚合该 step 所有叶子任务的产出，注入为 `inputs[step_id] = {task_id: output, ...}`。  
  **特例（skip=true 步骤）**：被跳过步骤没有任务产出文件，调度器改为直接读取 `<workspace>/manual_data/<step_id>/output.json` 的内容作为 `inputs[step_id]`，下游透明消费。
- 同 step 内无 `depends_on` 的任务全部并行（受 `max_parallelism` 限流）。
- **关键不变量：依赖就绪判定 = 上游 `output.json` 文件存在，与上游 status 字段无关**。这是 `fix --output` 跳过失败步骤的根基——output.json 被手动补齐后，下游即可消费。

---

## 4. Pydantic 数据模型

### 4.1 YAML schema 模型（`models/pipeline_spec.py`）

```python
from typing import Any
from pydantic import BaseModel, field_validator
import re

ID_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")

class TaskSpec(BaseModel):
    id: str
    plugin: str                              # "module.path.ClassName"
    depends_on: list[str] = []
    depends_on_steps: list[str] = []
    config: dict[str, Any] = {}
    inputs: dict[str, Any] = {}

    @field_validator("id")
    @classmethod
    def _check_id(cls, v): assert ID_PATTERN.match(v); return v

class StepSpec(BaseModel):
    id: str
    name: str | None = None
    skip: bool = False
    max_parallelism: int | None = None
    depends_on_steps: list[str] = []
    tasks: list[TaskSpec]

class PipelineMeta(BaseModel):
    id: str
    name: str
    description: str | None = None
    max_parallelism: int = 8

class PipelineSpec(BaseModel):
    version: str
    pipeline: PipelineMeta
    steps: list[StepSpec]
```

### 4.2 运行时状态模型（`models/runtime_state.py`）

```python
from enum import Enum
from datetime import datetime
from pydantic import BaseModel

class Status(str, Enum):
    PENDING   = "pending"
    RUNNING   = "running"
    PAUSED    = "paused"
    SUCCESS   = "success"
    FAILED    = "failed"
    SKIPPED   = "skipped"     # step-level skip=true
    RECOVERED = "recovered"   # 手动 fix --output 补齐后的任务状态

class TaskState(BaseModel):
    id: str
    status: Status = Status.PENDING
    progress: int = 0
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: str | None = None
    stack_trace: str | None = None
    input_path: str | None = None
    output_path: str | None = None
    log_path: str | None = None
    recovered_by: str | None = None   # 审计：记录 fix 操作信息

class StepState(BaseModel):
    id: str
    status: Status = Status.PENDING
    tasks: dict[str, TaskState] = {}
    started_at: datetime | None = None
    finished_at: datetime | None = None

class PipelineRunState(BaseModel):
    pipeline_id: str
    run_id: str
    status: Status = Status.PENDING
    steps: dict[str, StepState] = {}
    started_at: datetime | None = None
    finished_at: datetime | None = None
    workspace: str
```

---

## 5. BaseTask 接口（双入口）

```python
# pipeline_engine/core/base_task.py
from abc import ABC
from typing import Any, Awaitable, Callable
import asyncio

ProgressCallback = Callable[[int], Awaitable[None]]

class BaseTask(ABC):
    """所有用户任务必须继承此类。"""

    InputModel:  type | None = None   # 可选，声明后引擎自动校验
    OutputModel: type | None = None

    def __init__(self, task_id: str, config: dict[str, Any]) -> None:
        self.task_id = task_id
        self.config = config

    async def execute(self, inputs: dict[str, Any], progress: ProgressCallback) -> dict[str, Any]:
        """子类重载 run_sync 时，引擎自动通过 asyncio.to_thread 调度。"""
        if type(self).run_sync is not BaseTask.run_sync:
            return await asyncio.to_thread(self.run_sync, inputs, _SyncProgressAdapter(progress))
        raise NotImplementedError("子类必须实现 execute 或 run_sync 之一")

    def run_sync(self, inputs: dict[str, Any], progress) -> dict[str, Any]:
        """CPU 密集任务的同步入口，由引擎自动卸载到线程池。"""
        raise NotImplementedError
```

- 子类任选其一实现；
- `_SyncProgressAdapter` 用 `asyncio.run_coroutine_threadsafe` 把同步 progress 调用投回事件循环；
- `InputModel` / `OutputModel` 由引擎在 dispatch 前后用 Pydantic 校验，失败抛 `PipelineError`。

---

## 6. 引擎核心组件

### 6.1 RunContext（`core/run_context.py`）

封装单次 run 的全部上下文，是多 Pipeline 并行的核心抽象：

```python
class RunContext:
    pipeline_spec: PipelineSpec
    run_id: str
    workspace: Path
    scheduler: AsyncScheduler
    state_manager: StateManager
    abort_event: asyncio.Event
    main_task: asyncio.Task | None
```

每个 RunContext 拥有独立的 scheduler 和 state_manager，彼此无共享可变状态。

### 6.2 RunManager（`core/run_manager.py`）

进程级单例，协调所有 RunContext：

```python
class RunManager:
    _runs: dict[str, RunContext]           # key: run_id
    _registry: dict[str, PipelineSpec]     # key: pipeline_id
    _lock: asyncio.Lock

    async def load(self, yaml_path: Path) -> str
    async def start_run(self, pipeline_id: str, *, step=None, task=None) -> str
    async def stop(self, run_id: str, *, step=None, task=None) -> None
    async def resume(self, run_id: str, *, include_paused=False) -> None
    async def fix(self, run_id: str, task_locator: str, *, input_path=None, output_path=None) -> None
    def list_runs(self) -> list[RunSummary]
    def get_run(self, ref: str) -> RunContext   # ref = run_id 或 pipeline_id（歧义时报错）
```

### 6.3 AsyncScheduler（`core/scheduler.py`）

- NetworkX 构建 step 内与 step 间 DAG；
- `asyncio.Semaphore` 限流（步骤级 + 进程级两层）；
- 依赖就绪判定：上游 `output.json` 文件存在（status 无关）；
- dispatch：`asyncio.create_task(_run_task(...))`，完成后原子写盘 → 更新状态 → 触发下游；
- 监听 `abort_event`，触发后不再派发新任务，等 in-flight 任务自然结束。

### 6.4 StateManager（`core/state_manager.py`）

- 持有 `PipelineRunState`，所有读写走 `async with self._lock:`；
- 每次变更后立即调用 `storage.persist_state()`（原子写 `state.json`）；
- 每个 run 一个实例，彼此隔离。

**状态机非法迁移守卫**（V3.1 新增）：

| 方法 | 允许的 from 状态 | 其他状态时的行为 |
|---|---|---|
| `finish_task` | RUNNING | 静默丢弃（task 已 PAUSED/RECOVERED/SUCCESS） |
| `fail_task` | RUNNING、PENDING | 静默丢弃（终态或 PAUSED 任务不可被改写） |
| `update_progress` | RUNNING | 静默丢弃（任务已停止/暂停时进度无意义） |
| `recover_task` | 任意 | 始终允许（fix 可覆盖 FAILED/PAUSED/PENDING） |
| `reset_for_resume` | FAILED（或 +PAUSED） | 仅允许声明的状态，其余无变化 |

**孤儿 RUNNING 复位**（V3.1 新增）：

`demote_orphans_sync()` 同步方法（无锁，调用时 RunContext 尚未注册故无并发）：  
在 `restore_runs_from_disk` 加载旧 `state.json` 后立即调用，将残留的 RUNNING task / step / pipeline 全部置为 FAILED（`error="interrupted: process exited before completion"`），并原子写盘。这确保进程崩溃后的 `resume` 能正确重调度被中断的任务。

### 6.5 Storage（`core/storage.py`）

工作目录结构（根目录由 `--workspace` 控制，默认 CWD）：

```
<workspace>/.pipeline_runs/
├── registry.json
└── <pipeline_id>/<run_id>/
    ├── pipeline_spec.yaml
    ├── state.json                  # 原子写，每次状态变更后刷新
    └── <step_id>/<task_id>/
        ├── input.json
        ├── output.json
        └── log.txt
```

关键方法：
- `atomic_write_json(path, obj)`：先写 `.tmp` 再 `os.replace`；
- `load_manual_data(step_id)`：从 `<workspace>/manual_data/<step_id>/output.json` 读取，缺失 → `PipelineError`；
- `fix_output(run_id, step_id, task_id, src_path)`：原子复制 src_path → 对应 output.json。

### 6.6 DAG Validator（`core/dag_validator.py`）

- `build_task_graph(step) -> nx.DiGraph`；
- `build_step_graph(pipeline) -> nx.DiGraph`（数组顺序 + `depends_on_steps`）；
- `is_directed_acyclic_graph` 校验，失败时用 `nx.find_cycle` 打印环路详情；
- 返回 `topological_generations`（同代可并行）。

### 6.7 PipelineError（`core/errors.py`）

```python
class PipelineError(Exception):
    def __init__(self, message: str, *, pipeline_id=None, step_id=None, task_id=None, cause=None): ...
```

引擎内部所有抛出均用此类；用户任务异常被引擎捕获并包装。

---

## 7. CLI / REPL 指令

### 7.1 全局参数

```
pipeline_cli [--workspace PATH] <subcommand>
```

### 7.2 一次性子命令

```
pipeline_cli                            # 进入 REPL
pipeline_cli load <path> [<path>...]
pipeline_cli run <pipeline_id> [--step S] [--task T] [--wait]
pipeline_cli lint <path>
pipeline_cli list [--runs]
pipeline_cli status <ref>
pipeline_cli inspect <ref> [--step S] [--task T]
```

`run` 默认 detach（打印 run_id 即返回）；`--wait` 阻塞等待。

### 7.3 REPL 指令

| 指令 | 行为 |
|---|---|
| `load <path>` | 解析 YAML → 校验 → 注册 |
| `list [--runs]` | 列已加载 Pipeline 或所有 run |
| `run <id> [--step S] [--task T]` | 后台启动，立即返回 run_id |
| `stop <ref> [--step S] [--task T]` | 触发 abort_event |
| `resume <ref> [--include-paused]` | 默认仅调度 Failed；加 `--include-paused` 同时调度 Paused |
| `status <ref>` | Rich 表格输出全貌 |
| `status --all` | 所有活跃 run 总览 |
| `status <ref> --watch` | Rich Live 实时刷新（显式触发，避免抢屏） |
| `inspect <ref> --step S --task T` | 输出 input.json / output.json / log.txt / stack_trace |
| `fix <ref> --task T --output PATH` | 补齐 output.json → RECOVERED → 自动触发下游 |
| `fix <ref> --task T --input PATH` | 写入 input.json → Pending，等 resume 调度 |
| `exit` | 退出；有活跃 run 时提示 |

非阻塞实现：REPL 主协程跑 `PromptSession.prompt_async()`；`run` 通过 `asyncio.create_task` 派发。

---

## 8. 多 Pipeline 并发模型

- 单事件循环、单进程，所有 RunContext 共用；
- 进程级 `asyncio.Semaphore(cpu_count())` 跨 run 限流线程池；
- `pipeline_id` 寻址歧义时强制使用 `run_id`；
- 进程重启后可从磁盘 `state.json` 恢复并 `resume`。

---

## 9. fix --output 完整流程

假设 `recognize` step 下的 `rec_cable` 任务失败：

1. `inspect <run_id> --step recognize --task rec_cable` → 查看堆栈；
2. 外部生成符合 `RecognizeOutput` schema 的 `recover.json`；
3. `fix <run_id> --task rec_cable --output ./recover.json`：
   - 用 `OutputModel` 校验（若已声明）；
   - 原子复制为工作目录下的 `recognize/rec_cable/output.json`；
   - task status → `RECOVERED`，`recovered_by` 记录审计信息；
4. `resume <run_id>`：
   - 调度器检测到 `rec_cable.output.json` 存在 → 依赖就绪；
   - 下游 `aggregate/merge` 被调度，消费补齐产出；
   - Pipeline 继续至完成。

---

## 10. CAD Pipeline 示例

### 10.1 目标

1. **功能验证**：覆盖串行 step、step 内并行、长时任务、进度推送、状态机转换、fix 修复。
2. **观感验证**：Rich 进度条流畅、REPL 非阻塞、`status --watch` 实时刷新。
3. **参考样板**：业务方构建新 pipeline 的完整参考实现。

### 10.2 任务清单与耗时

| Step | Task | plugin 类 | 入口 | 耗时 | 进度推送 |
|---|---|---|---|---:|---|
| `parse_dxf` | `read_dxf` | `ReadDxfTask` | `run_sync` | 2s | 每 200ms |
| `parse_dxf` | `parse_entities` | `ParseEntitiesTask` | `execute` | 3s | 每 300ms |
| `split_subgraph` | `split` | `SplitSubgraphTask` | `run_sync` | 2s | 每 200ms |
| `recognize` | `rec_building` | `RecBuildingTask` | `run_sync` | **8s** | 每 800ms |
| `recognize` | `rec_cable` | `RecCableTask` | `execute` | **10s** | 每 1s |
| `recognize` | `rec_schematic` | `RecSchematicTask` | `run_sync` | 6s | 每 600ms |
| `aggregate` | `merge` | `MergeAndDedupTask` | `execute` | 2s | 每 200ms |

`recognize` step 三任务并行，总耗时应 ≈ 10s（非 24s）——这是并行调度的关键验收点。

### 10.3 schema（`examples/cad_pipeline/schemas.py`）

```python
from pydantic import BaseModel

class Entity(BaseModel):
    id: str; layer: str; type: str
    bbox: tuple[float, float, float, float]

class ReadDxfOutput(BaseModel):
    file_path: str; entity_count: int; raw_path: str

class ParseEntitiesOutput(BaseModel):
    entities: list[Entity]

class Subgraph(BaseModel):
    id: str; bbox: tuple[float, float, float, float]; entity_ids: list[str]

class SplitSubgraphOutput(BaseModel):
    subgraphs: list[Subgraph]

class RecognizedItem(BaseModel):
    category: str; name: str; count: int; subgraph_id: str

class RecognizeOutput(BaseModel):
    items: list[RecognizedItem]

class CostSummaryItem(BaseModel):
    category: str; name: str; total_count: int; unit_price: float; subtotal: float

class MergeOutput(BaseModel):
    summary: list[CostSummaryItem]; grand_total: float
```

### 10.4 mock 任务样板（`examples/cad_pipeline/tasks.py` 节选）

```python
import time, asyncio
from pipeline_engine.core.base_task import BaseTask
from .schemas import ReadDxfOutput, RecognizeOutput

class ReadDxfTask(BaseTask):
    OutputModel = ReadDxfOutput

    def run_sync(self, inputs, progress):
        for i in range(10):
            time.sleep(0.2)
            progress(int((i + 1) / 10 * 100))
        return {"file_path": self.config["dxf_path"], "entity_count": 1234, "raw_path": "..."}

class RecCableTask(BaseTask):
    """长时 async 任务，模拟 10s 识别调用。"""
    OutputModel = RecognizeOutput

    async def execute(self, inputs, progress):
        for i in range(10):
            await asyncio.sleep(1.0)
            await progress((i + 1) * 10)
        return {"items": [
            {"category": "cable", "name": "YJV-4x16", "count": 42, "subgraph_id": "sg_1"},
            {"category": "cable", "name": "YJV-3x10", "count": 18, "subgraph_id": "sg_2"},
        ]}
```

`PIPELINE_DEMO_FAIL=<task_id>` 环境变量让指定任务抛错（供演示和 e2e 测试用）。
`PIPELINE_DEMO_FAST=1` 将所有 sleep × 0.1（CI 加速）。

### 10.5 演示脚本

```bash
# REPL 交互演示
python -m pipeline_cli
> load examples/cad_pipeline/pipeline.yaml
> run cad_cost_estimation
> status cad_cost_estimation --watch          # 实时观察进度
> inspect <run_id> --step recognize --task rec_cable

# 失败恢复演示
PIPELINE_DEMO_FAIL=rec_cable python -m pipeline_cli
> run cad_cost_estimation
> inspect <run_id> --step recognize --task rec_cable
> fix <run_id> --task rec_cable --output examples/cad_pipeline/mock_data/recover_cable.json
> resume <run_id>
> status <run_id>                             # 最终 Success
```

### 10.6 验收点

- ✅ `recognize` step 总耗时 ≈ 10s（并行生效）
- ✅ Rich 进度条 0→100 流畅，长时任务动画可见
- ✅ REPL 运行期间能响应 `status`/`inspect`/`fix`，无卡顿
- ✅ `status --watch` 不抢屏
- ✅ `fix --output` + `resume` → RECOVERED → aggregate 顺利完成
- ✅ 最终 `aggregate/merge/output.json` 包含 `grand_total`

---

## 11. 测试用例规划（覆盖率 ≥ 90%）

### 11.1 单元测试（`tests/unit/`）

| 文件 | 覆盖点 |
|---|---|
| `test_yaml_parser.py` | 合法解析；缺失字段、类型错误、id 正则违规 |
| `test_dag_validator.py` | 单 step 拓扑、跨 step 拓扑、环路检测（自环、长环） |
| `test_plugin_loader.py` | 加载 BaseTask 子类；非 BaseTask 子类报错；模块不存在 |
| `test_state_manager.py` | 状态机迁移（含 RECOVERED）；并发读写不丢更新 |
| `test_storage.py` | 原子写不留半文件；`manual_data` 缺失/格式错；`--workspace` |
| `test_base_task.py` | `run_sync` → `to_thread`；progress 跨线程投递；I/O Pydantic 校验 |

### 11.2 集成测试（`tests/integration/`）

| 文件 | 覆盖点 |
|---|---|
| `test_scheduler.py` | 3 个无依赖任务真正并行；线性链按序；跨 step 数据注入 |
| `test_skip_mode.py` | skip=true 从 manual_data 加载；缺失 → PipelineError |
| `test_abort_resume.py` | stop → Paused；resume 默认仅调度 Failed；`--include-paused`；run_id 不变 |
| `test_fix_command.py` | `fix --output` → RECOVERED + 下游触发；`fix --input` → Pending |
| `test_multi_pipeline.py` | 并发 2 个 pipeline；pipeline_id 歧义报错；run_id 寻址 |
| `test_repl_commands.py` | run 期间并发 status/inspect；无 RuntimeError |

### 11.3 端到端测试（`tests/e2e/`）

- `test_cad_example.py`：跑完整 `cad_pipeline`（FAST 模式），断言状态 + 产出文件 + 并行耗时。
- `test_cad_failure_recovery.py`：`DEMO_FAIL=rec_cable` → fix → resume → Success 全链路。

### 11.4 测试基础设施

- `pytest-asyncio` 的 `asyncio_mode = "auto"`
- `conftest.py` 提供 `tmp_workspace`、`stub_pipeline_spec`、`make_recover_json` fixture
- 门限：`pytest --cov=pipeline_engine --cov-fail-under=90`

---

## 12. 实现阶段

1. **骨架**：pyproject、目录、PipelineError、Status、Pydantic 模型 → `test_yaml_parser` + `test_dag_validator` 绿
2. **存储与状态**：Storage（`--workspace`）、StateManager → 单测绿
3. **任务接口与加载**：BaseTask、PluginLoader → 单测绿
4. **单 run 调度器**：AsyncScheduler、依赖注入（基于 output.json 存在性）、限流 → `test_scheduler` 绿
5. **skip / fix / resume**：状态转换 + RECOVERED 路径 → `test_fix_command`、`test_abort_resume` 绿
6. **RunContext / RunManager**：多 run 协调层 → `test_multi_pipeline` 绿
7. **CLI + REPL**：typer + prompt_toolkit + rich → `test_repl_commands` 绿
8. **CAD 示例落地**：schemas、tasks、yaml、mock_data、README、recover_cable.json → 手工演示通畅
9. **e2e 收口**：`test_cad_example` + `test_cad_failure_recovery` → 全绿，覆盖率 ≥ 90%
