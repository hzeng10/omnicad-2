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

---

## 安装与环境准备

> Ubuntu 22.04/24.04 的系统 Python 受保护，直接 `pip install` 会报 `externally-managed-environment`。请使用虚拟环境。

```bash
cd ~/dev/omnicad-2

# 1. 创建虚拟环境（只需执行一次）
python3 -m venv .venv

# 2. 激活虚拟环境（每次打开新终端都需要执行）
source .venv/bin/activate

# 3. 安装项目及依赖
pip install -e .

# 4. 验证安装
pipeline_cli --help
```

激活后命令行前缀会变为 `(.venv)`，表示已进入虚拟环境。退出虚拟环境执行 `deactivate`。

> 示例 pipeline 位于 `examples/cad_pipeline/`，包含 4 个步骤 / 7 个任务，覆盖串行、并行、长时任务、fix/resume 等所有特性。设置 `PIPELINE_DEMO_FAST=1` 可将所有 sleep 缩短至 10% 以便快速体验。

---

## 使用示例

### 场景一：校验与注册

```bash
# 校验 YAML 语法与 DAG 合法性（不执行）
$ pipeline_cli lint examples/cad_pipeline/pipeline.yaml
OK — pipeline 'cad_cost_estimation' 校验通过。

# 注册 pipeline 到本地 registry
$ pipeline_cli load examples/cad_pipeline/pipeline.yaml --workspace /tmp/cad_demo
Loaded: cad_cost_estimation

# 列出已注册的 pipeline
$ pipeline_cli list --workspace /tmp/cad_demo
  cad_cost_estimation             CAD 设备成本汇总（示例）
```

### 场景二：运行完整 pipeline 并查看结果

```bash
# 运行并阻塞等待完成
$ PIPELINE_DEMO_FAST=1 pipeline_cli run cad_cost_estimation --workspace /tmp/cad_demo --wait
Started: 20260512T083056_948823_1b1fe2  (pipeline: cad_cost_estimation)
  20260512T083056_948823_1b1fe2: success

# 查看运行状态（支持跨进程：从磁盘恢复上次运行的状态）
$ pipeline_cli status 20260512T083056_948823_1b1fe2 --workspace /tmp/cad_demo
╭────────────────┬────────────────┬─────────┬──────────┬───────╮
│ Step           │ Task           │ Status  │ Progress │ Error │
├────────────────┼────────────────┼─────────┼──────────┼───────┤
│ parse_dxf      │ read_dxf       │ success │     100% │       │
│                │ parse_entities │ success │     100% │       │
│ split_subgraph │ split          │ success │     100% │       │
│ recognize      │ rec_building   │ success │     100% │       │
│                │ rec_cable      │ success │     100% │       │
│                │ rec_schematic  │ success │     100% │       │
│ aggregate      │ merge          │ success │     100% │       │
╰────────────────┴────────────────┴─────────┴──────────┴───────╯
Pipeline 状态: success
```

### 场景三：inspect 查看 task 详情（输入 / 输出）

`inspect` 显示指定 task 的完整输入输出 JSON、进度、错误堆栈。下例展示 `merge` 任务——它通过 `depends_on_steps: [recognize]` 自动聚合了3个并行识别任务的输出作为输入：

```bash
$ pipeline_cli inspect <run_id> --step aggregate --task merge --workspace /tmp/cad_demo

─────────────────────────────── merge ────────────────────────────────
状态    : success  进度 : 100%

输入 (aggregate/merge/input.json):
{
  "recognize": {
    "rec_building": {
      "items": [
        {"category": "building", "name": "Office Block A", "count": 3, "subgraph_id": "sg_0"},
        {"category": "building", "name": "Warehouse B",    "count": 1, "subgraph_id": "sg_1"}
      ]
    },
    "rec_cable": {
      "items": [
        {"category": "cable", "name": "YJV-4x16", "count": 42, "subgraph_id": "sg_2"},
        {"category": "cable", "name": "YJV-3x10", "count": 18, "subgraph_id": "sg_3"}
      ]
    },
    "rec_schematic": {
      "items": [
        {"category": "schematic", "name": "Panel-MCC1", "count": 2, "subgraph_id": "sg_0"},
        {"category": "schematic", "name": "Panel-MCC2", "count": 1, "subgraph_id": "sg_1"}
      ]
    }
  }
}

输出 (aggregate/merge/output.json):
{
  "summary": [
    {"category": "building",   "name": "Office Block A", "total_count": 3,  "unit_price": 500000.0, "subtotal": 1500000.0},
    {"category": "building",   "name": "Warehouse B",    "total_count": 1,  "unit_price": 200000.0, "subtotal":  200000.0},
    {"category": "cable",      "name": "YJV-3x10",       "total_count": 18, "unit_price":     32.0, "subtotal":     576.0},
    {"category": "cable",      "name": "YJV-4x16",       "total_count": 42, "unit_price":     45.0, "subtotal":    1890.0},
    {"category": "schematic",  "name": "Panel-MCC1",     "total_count": 2,  "unit_price":  15000.0, "subtotal":   30000.0},
    {"category": "schematic",  "name": "Panel-MCC2",     "total_count": 1,  "unit_price":  12000.0, "subtotal":   12000.0}
  ],
  "grand_total": 1744466.0
}
```

### 场景四：任务失败 → fix 注入修复数据 → resume 恢复

这是引擎的核心能力：单个任务失败后不必重跑整个 pipeline，只需注入修复数据再恢复即可。

```bash
# 1. 模拟 rec_cable 任务报错
$ PIPELINE_DEMO_FAST=1 PIPELINE_DEMO_FAIL=rec_cable \
  pipeline_cli run cad_cost_estimation --workspace /tmp/cad_demo2 --wait
Started: 20260512T083200_497976_239806  (pipeline: cad_cost_estimation)
Task recognize/rec_cable failed: RuntimeError: Intentional failure injected via PIPELINE_DEMO_FAIL=rec_cable
  20260512T083200_497976_239806: failed

# 2. 查看失败状态：rec_cable 失败，其余任务正常完成
$ pipeline_cli status <run_id> --workspace /tmp/cad_demo2
│ recognize │ rec_building  │  success  │ 100% │                              │
│           │ rec_cable     │  failed   │   0% │ RuntimeError: Intentional... │
│           │ rec_schematic │  success  │ 100% │                              │
│ aggregate │ merge         │  success  │ 100% │                              │
Pipeline 状态: failed

# 3. inspect 查看失败任务的完整错误堆栈
$ pipeline_cli inspect <run_id> --step recognize --task rec_cable --workspace /tmp/cad_demo2
状态    : failed  进度 : 0%
错误    : RuntimeError: Intentional failure injected via PIPELINE_DEMO_FAIL=rec_cable
Traceback (most recent call last):
  File ".../scheduler.py", line 220, in _dispatch_task
    output = await task_instance.execute(validated_inputs, progress_cb)
  File ".../tasks.py", line 113, in execute
    _check_fail(self.task_id)
RuntimeError: Intentional failure injected via PIPELINE_DEMO_FAIL=rec_cable

# 4. fix --output：注入人工准备好的恢复数据（引擎自动校验 OutputModel 契约）
$ pipeline_cli fix <run_id> \
  --task recognize/rec_cable \
  --output examples/cad_pipeline/mock_data/recover_cable.json \
  --workspace /tmp/cad_demo2
Fixed (output): task 'recognize/rec_cable' → RECOVERED

# 5. resume：仅重调度 FAILED 任务；RECOVERED 任务跳过，不重复执行
$ PIPELINE_DEMO_FAST=1 pipeline_cli resume <run_id> --workspace /tmp/cad_demo2
Resumed: 20260512T083200_497976_239806
  20260512T083200_497976_239806: success

# 6. 最终状态：rec_cable 为 recovered（已跳过），整体 success
$ pipeline_cli status <run_id> --workspace /tmp/cad_demo2
│ recognize │ rec_building  │  success   │ 100% │
│           │ rec_cable     │ recovered  │   0% │   ← 保留注入数据，未重跑
│           │ rec_schematic │  success   │ 100% │
│ aggregate │ merge         │  success   │ 100% │
Pipeline 状态: success
```

### 场景五：细粒度执行 — 仅运行单个 step

```bash
# 只执行 parse_dxf 步骤，其余步骤不触发
$ PIPELINE_DEMO_FAST=1 pipeline_cli run cad_cost_estimation \
  --step parse_dxf --workspace /tmp/cad_step --wait
Started: 20260512T083250_212844_f15b79  (pipeline: cad_cost_estimation)
  20260512T083250_212844_f15b79: pending    ← pipeline 整体仍是 pending（只完成了一步）

$ pipeline_cli status <run_id> --workspace /tmp/cad_step
│ parse_dxf │ read_dxf       │ success │ 100% │
│           │ parse_entities │ success │ 100% │
Pipeline 状态: pending
```

### 场景六：交互式 REPL

不带子命令启动时进入 REPL。REPL 与调度器共享事件循环——任务在后台执行，前台随时可输入命令查询或干预。

```
$ pipeline_cli --workspace /tmp/cad_demo
Pipeline REPL  (输入 help 查看命令)
工作目录: /tmp/cad_demo

pipeline> load examples/cad_pipeline/pipeline.yaml
已加载: cad_cost_estimation

pipeline> run cad_cost_estimation
Started: 20260512T...  (pipeline: cad_cost_estimation)

# status --watch 持续刷新进度表格，任务完成后自动退出
pipeline> status cad_cost_estimation --watch

# 查看 task 详情
pipeline> inspect <run_id> --step recognize --task rec_cable

# 任务失败后注入修复数据
pipeline> fix <run_id> --task recognize/rec_cable --output /path/to/fix.json
修复成功 (output): task 'recognize/rec_cable' → RECOVERED

# 恢复执行
pipeline> resume <run_id>
已恢复: 20260512T...

pipeline> exit
```

---

## REPL 命令参考

```
load <path> [<path>...]                            注册 pipeline YAML 文件
list [--runs]                                      列出 pipeline 或所有 run
run <pipeline_id> [<id>...] [--step S] [--task T]  启动 run（非阻塞）
status <ref> [--watch] [--all]                     查看状态；--watch 持续刷新
inspect <ref> [--step S] [--task T]                查看 task 详情
stop <ref> [--step S --task T]                     有序中止 run（或单个 task）
resume <ref> [--include-paused]                    重调度 FAILED（可选 PAUSED）任务
fix <ref> --task S/T --output PATH                 注入恢复的 output.json → RECOVERED
fix <ref> --task S/T --input PATH                  替换 input.json → 复位为 PENDING
help / exit
```

---

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

### skip=true（跳过步骤，使用预置数据）

```yaml
  - id: manual_step
    skip: true        # 跳过执行，从 manual_data/manual_step/output.json 加载预置输出
    tasks:
      - id: placeholder
        plugin: pipeline_engine.builtin.ManualDataLoader
```

---

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

---

## I/O 数据约定

完整约定参见 [`config/task_io.schema.json`](config/task_io.schema.json)。

| 文件 | 位置 | 说明 |
|---|---|---|
| `input.json` | `<workspace>/.pipeline_runs/<pipeline_id>/<run_id>/<step_id>/<task_id>/` | 引擎在执行前写入 |
| `output.json` | 同上 | 任务成功后引擎原子写入 |
| `log.txt` | 同上 | 任务代码可自行写入日志 |
| `manual_data/<step_id>/output.json` | `<workspace>/` | skip=true step 的预置输出 |
| `state.json` | `<workspace>/.pipeline_runs/<pipeline_id>/<run_id>/` | run 状态快照，进程崩溃后恢复用 |

---

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

---

## 运行测试

```bash
pytest                                              # 运行所有测试
pytest tests/unit/                                 # 仅单元测试
pytest tests/integration/                          # 仅集成测试
pytest tests/e2e/ -v                               # 端到端测试（CAD 示例）
pytest --cov=pipeline_engine --cov-fail-under=90   # 带覆盖率检查
```

---

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
                  → PAUSED     （abort_event 触发）
         SKIPPED               （step 级别 skip=true）
任意    → RECOVERED            （fix --output 后）
FAILED  → PENDING              （reset_for_resume）
PAUSED  → PENDING              （reset_for_resume --include-paused）
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
