# Pipeline DAG 执行引擎

基于 Python 3.10+ 的 CLI 工具，通过 YAML 文件定义并执行 DAG（有向无环图）工作流。引擎与业务逻辑完全解耦——只负责任务调度、数据路由和状态管理；业务任务以插件形式在运行时动态加载。

## 核心特性

- **YAML 定义工作流** — 在单个文件中声明步骤、任务、依赖关系和并发上限；Pydantic v2 在加载时自动校验 Schema
- **DAG 调度** — NetworkX 检测环路并解析拓扑顺序；同一 step 内无依赖关系的 task 自动并行执行
- **异步 + 线程池** — IO 密集型任务使用 `async def execute()`；CPU 密集型任务使用 `def run_sync()` 并通过 `asyncio.to_thread` 在线程池中运行
- **原子状态持久化** — 每次 task 状态变更先写盘（`os.replace`）再更新内存，进程崩溃后可从快照完整恢复
- **失败恢复** — `fix --output` 为失败 task 注入替换的 `output.json`；下游 task 正常消费（依赖就绪以文件存在为准，而非状态字段）
- **中止 / 恢复** — `stop` 通过 `asyncio.Event` 触发有序中止；`resume` 仅重调度 FAILED（可选 PAUSED）任务，保留已完成的工作
- **多 pipeline 并发** — 多个 pipeline 在同一进程内运行，各自持有独立的 `RunContext`；共享 `asyncio.Semaphore` 限制总线程池用量
- **交互式 REPL** — 基于 `prompt_toolkit` 的 REPL 与调度器共享事件循环，非阻塞运行；`status --watch` 在任务执行中实时刷新 Rich 表格
- **完善的状态机守卫** — `finish_task`/`fail_task`/`update_progress` 均内置非法迁移检查，防止并发竞争导致状态混乱
- **90%+ 测试覆盖率** — 涵盖单元、集成、端到端三层

## 快速开始

```bash
pip install -e .

# 校验 pipeline YAML 语法
pipeline_cli lint examples/cad_pipeline/pipeline.yaml

# 运行并等待完成
pipeline_cli run cad_cost_estimation \
  --workspace /tmp/demo \
  --wait

# 进入交互式 REPL
pipeline_cli --workspace /tmp/demo
```

## REPL 命令

```
load <path>                                    注册 pipeline YAML 文件
list [--runs]                                  列出 pipeline 或活跃 run
run <pipeline_id> [<id>...] [--step S] [--task T]  启动 run（非阻塞）
status <ref> [--watch] [--all]                 查看运行状态；--watch 持续刷新
inspect <ref> [--step S] [--task T]            查看 task 详情（输入/输出/日志/堆栈）
stop <ref>                                     有序中止 run
resume <ref> [--include-paused]                重调度 FAILED（可选 PAUSED）任务
fix <ref> --task S/T --output PATH             注入恢复的 output.json → RECOVERED
fix <ref> --task S/T --input PATH              替换 input.json → 复位为 PENDING
help / exit
```

## Pipeline YAML 格式

完整 Schema 参见 [`config/pipeline.schema.json`](config/pipeline.schema.json)。

```yaml
version: "1.0"
pipeline:
  id: my_pipeline
  name: "我的 Pipeline"
  max_parallelism: 4        # 进程级最大并发 task 数

steps:
  - id: step_one
    tasks:
      - id: task_a
        plugin: mypackage.tasks.TaskA   # BaseTask 子类的点分路径
        config: { key: value }
        inputs: { static_param: 42 }

      - id: task_b
        plugin: mypackage.tasks.TaskB
        depends_on: [task_a]            # step 内依赖（控制执行顺序）

  - id: step_two
    depends_on_steps: [step_one]        # 跨 step 依赖
    tasks:
      - id: task_c
        plugin: mypackage.tasks.TaskC
        depends_on_steps: [step_one]    # 将 step_one 的叶子 task 输出注入 inputs
```

### skip=true（跳过步骤）

```yaml
  - id: manual_step
    skip: true        # 跳过执行，从 manual_data/manual_step/output.json 加载预置输出
    tasks:
      - id: placeholder
        plugin: pipeline_engine.builtin.ManualDataLoader
```

## 编写任务插件

```python
from pipeline_engine.core.base_task import BaseTask
from pydantic import BaseModel

class MyInputModel(BaseModel):
    count: int

class MyOutputModel(BaseModel):
    result: str

class MyTask(BaseTask):
    InputModel  = MyInputModel   # 可选：Pydantic 输入校验
    OutputModel = MyOutputModel  # 可选：Pydantic 输出校验

    async def execute(self, inputs: dict, progress) -> dict:
        """IO 密集型任务（异步执行）"""
        await progress(50)
        result = process(inputs["count"])
        await progress(100)
        return {"result": result}

    # CPU 密集型任务替代方案，引擎自动通过 asyncio.to_thread 在线程池调用
    def run_sync(self, inputs: dict, progress) -> dict:
        progress(100)
        return {"result": heavy_compute(inputs["count"])}
```

## 失败恢复工作流

```
# 1. 任务执行中失败
pipeline> status my_pipeline
#  recognize / rec_cable   failed   RuntimeError: ...

# 2. 查看错误详情
pipeline> inspect <run_id> --step recognize --task rec_cable

# 3. 注入修复后的输出数据
pipeline> fix <run_id> --task recognize/rec_cable --output ./recovered.json
#  rec_cable → RECOVERED

# 4. 恢复执行（已完成的任务跳过，RECOVERED 任务不重跑）
pipeline> resume <run_id>
pipeline> status <run_id>
#  pipeline   success
```

## I/O 数据约定

完整约定参见 [`config/task_io.schema.json`](config/task_io.schema.json)。

| 文件 | 位置 | 说明 |
|---|---|---|
| `input.json` | `<workspace>/.pipeline_runs/<pipeline_id>/<run_id>/<step_id>/<task_id>/` | 引擎在执行前写入 |
| `output.json` | 同上 | 任务成功后引擎原子写入 |
| `log.txt` | 同上 | 任务代码可自行写入日志 |
| `manual_data/<step_id>/output.json` | `<workspace>/` | skip=true step 的预置输出 |
| `state.json` | `<workspace>/.pipeline_runs/<pipeline_id>/<run_id>/` | run 状态快照 |

## CAD Pipeline 示例

`examples/cad_pipeline/` 包含一个完整的模拟 pipeline（7 个任务，4 个步骤），演示所有引擎特性：串行步骤、step 内并行、长时任务实时进度、`run_sync` vs `async execute`，以及 fix/resume 恢复路径。

```bash
# 快速模式（sleep 时间缩短至 10%）
PIPELINE_DEMO_FAST=1 pipeline_cli --workspace /tmp/cad_demo
pipeline> load examples/cad_pipeline/pipeline.yaml
pipeline> run cad_cost_estimation
pipeline> status cad_cost_estimation --watch
```

## 技术栈

| 组件 | 库 | 说明 |
|---|---|---|
| 异步调度 | `asyncio`（标准库） | 核心引擎，REPL 和任务共享事件循环 |
| DAG 验证 | `NetworkX >= 3.0` | 环路检测 + 拓扑排序 |
| Schema 校验 | `Pydantic >= 2.5` | YAML Schema + 任务 I/O 契约 |
| YAML 解析 | `PyYAML >= 6.0` | pipeline 配置文件 |
| 终端 UI | `Rich >= 13.0` | 进度表格、彩色状态、JSON 格式化 |
| 交互式 REPL | `prompt_toolkit >= 3.0` | 命令补全 + 历史记录 |
| CLI | `Typer >= 0.12` | 子命令定义 |

## 运行测试

```bash
pytest                                          # 运行所有测试
pytest tests/unit/                             # 仅单元测试
pytest tests/integration/                      # 仅集成测试
pytest tests/e2e/ -v                           # 端到端测试（CAD 示例）
pytest --cov=pipeline_engine --cov-fail-under=90   # 带覆盖率检查
```

## 架构概览

```
Pipeline
  └── Step[]          （线性序列；skip=true 时加载 manual_data）
        └── Task[]    （Step 内 DAG；无依赖的 task 并行执行）
```

状态机（Pipeline / Step / Task 共用）：

```
PENDING → RUNNING → SUCCESS
                  → FAILED
                  → PAUSED   （abort_event 触发）
         SKIPPED              （step 级别 skip=true）
任意    → RECOVERED           （fix --output 后）
FAILED  → PENDING             （reset_for_resume）
PAUSED  → PENDING             （reset_for_resume --include-paused）
```

关键模块（`pipeline_engine/core/`）：

| 模块 | 职责 |
|---|---|
| `scheduler.py` | 驱动单次 run 的完整执行流程（拓扑排序 + 并发分发） |
| `state_manager.py` | 运行时状态的唯一可信来源（asyncio.Lock 保护 + 状态机守卫） |
| `run_manager.py` | 进程级协调器（加载/启动/停止/恢复/修复多个 pipeline） |
| `run_context.py` | 单次 run 的运行时容器（scheduler + state_manager + abort_event） |
| `plugin_loader.py` | 动态加载 BaseTask 子类（点分路径 → 类实例） |
| `storage.py` | 所有磁盘 I/O 的统一入口（原子写入 + 注册表） |
| `dag_validator.py` | DAG 合法性校验 + 拓扑分代计算 |
| `base_task.py` | 所有业务任务必须继承的抽象基类 |
