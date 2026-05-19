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
- **运行日志** — 每个 instance 一份 `run.log`，汇聚引擎生命周期事件、`pipeline_engine.*` Python logging、Task `self.logger` 自定义日志及 stdout/stderr；`log` 命令分页查看，ERROR 行红色高亮，resume 续写同一文件
- **完善的状态机守卫** — `finish_task`/`fail_task`/`update_progress` 均内置非法迁移检查，防止并发竞争导致状态混乱
- **90%+ 测试覆盖率** — 涵盖单元、集成、端到端三层

---

## 安装与环境准备

> Ubuntu/macOS 的系统 Python 受保护，直接 `pip install` 会报 `externally-managed-environment`；Windows 同样推荐使用虚拟环境隔离依赖。

### Linux / macOS

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

### Windows (PowerShell)

```powershell
cd $HOME\dev\omnicad-2

# 1. 创建虚拟环境（只需执行一次）
py -3 -m venv .venv

# 2. 激活虚拟环境（每次打开新终端都需要执行）
.\.venv\Scripts\Activate.ps1

# 3. 安装项目及依赖
pip install -e .

# 4. 验证安装
pipeline_cli --help
```

> 若 PowerShell 提示脚本执行被禁止，以管理员权限执行 `Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned` 后重试。

### Windows (cmd.exe)

```cmd
cd %USERPROFILE%\dev\omnicad-2
py -3 -m venv .venv
.venv\Scripts\activate.bat
pip install -e .
pipeline_cli --help
```

激活后命令行前缀会变为 `(.venv)`，表示已进入虚拟环境。退出虚拟环境执行 `deactivate`。

> 示例 pipeline 位于 `pipelines/cad_identify_pipeline/`，包含 4 个步骤 / 7 个任务，覆盖串行、并行、长时任务、fix/resume 等所有特性。设置 `PIPELINE_DEMO_FAST=1` 可将所有 sleep 缩短至 10% 以便快速体验。

### Autoload（自动加载）

CLI 启动时（REPL 与一次性子命令均适用）自动扫描 `./pipelines/*/pipeline.yaml`（一级深度），将每个找到的 YAML 视同 `load` 命令注册到引擎，无需手动 `load`。

| 选项 | 说明 |
|---|---|
| `--pipelines-dir DIR` | 指定发现目录（默认 `./pipelines`），也可用 `PIPELINE_AUTOLOAD_DIR` env var |
| `--no-autoload` | 禁用自动加载，也可用 `PIPELINE_NO_AUTOLOAD=1` env var |

### CLI 输出格式

`pipeline_cli <subcommand>` 一次性子命令默认输出**单个 JSON 对象**到 stdout，便于 AI Agent 直接 `json.loads()` 解析。REPL 交互模式（无子命令）保持 Rich 文本渲染，行为不变。

```bash
# JSON 输出示例
$ pipeline_cli list | jq .ok
true
$ pipeline_cli list | jq '.pipelines[].pipeline_id'
"cad_identify_cost_estimation"

# 失败时 ok=false，exit code=1
$ pipeline_cli start no_such | jq .error.message
"pipeline 'no_such' 未加载"

# 禁用 autoload（单次调用）
$ pipeline_cli --no-autoload list
{"ok": true, "command": "list", "scope": "pipeline", "pipelines": []}
```

---

## 使用示例

### 场景一：校验与注册

```bash
# 校验 YAML 语法与 DAG 合法性（不执行）— JSON 输出
$ pipeline_cli lint pipelines/cad_identify_pipeline/pipeline.yaml
{"ok": true, "command": "lint", "pipeline_id": "cad_identify_cost_estimation", "valid": true, "path": "..."}

# 注册 pipeline 到本地 registry
$ pipeline_cli load pipelines/cad_identify_pipeline/pipeline.yaml --workspace /tmp/cad_demo
{"ok": true, "command": "load", "loaded": [{"pipeline_id": "cad_identify_cost_estimation", "ok": true, ...}]}

# autoload 已注册时无需手动 load（默认扫描 ./pipelines/*/pipeline.yaml）
$ pipeline_cli list --workspace /tmp/cad_demo | jq .pipelines
[{"pipeline_id": "cad_identify_cost_estimation", "type": "CAD图识别及算量", "name": "CAD 设备成本汇总（示例）"}]

# 列出运行实例
$ pipeline_cli list --instance --workspace /tmp/cad_demo | jq .instances
[]
```

### 场景二：运行完整 pipeline 并查看结果

```bash
# 运行并阻塞等待完成 — JSON 输出
$ PIPELINE_DEMO_FAST=1 pipeline_cli start cad_identify_cost_estimation --workspace /tmp/cad_demo --wait \
  | jq '{run_id: .runs[0].run_id, status: .runs[0].final_status}'
{"run_id": "cad_identify_cost_estimation_20260513-093024_7392", "status": "success"}

# 查看运行状态（支持跨进程：从磁盘恢复上次运行的状态）
$ RUN=cad_identify_cost_estimation_20260513-093024_7392
$ pipeline_cli status "$RUN" --workspace /tmp/cad_demo | jq .state.status
"success"
$ pipeline_cli status "$RUN" --workspace /tmp/cad_demo
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
$ pipeline_cli inspect <instance_id> --step aggregate --task merge --workspace /tmp/cad_demo

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
  pipeline_cli start cad_identify_cost_estimation --workspace /tmp/cad_demo2 --wait
Started: 20260512T083200_497976_239806  (pipeline: cad_identify_cost_estimation)
Task recognize/rec_cable failed: RuntimeError: Intentional failure injected via PIPELINE_DEMO_FAIL=rec_cable
  20260512T083200_497976_239806: failed

# 2. 查看失败状态：rec_cable 失败，其余任务正常完成
$ pipeline_cli status <instance_id> --workspace /tmp/cad_demo2
│ recognize │ rec_building  │  success  │ 100% │                              │
│           │ rec_cable     │  failed   │   0% │ RuntimeError: Intentional... │
│           │ rec_schematic │  success  │ 100% │                              │
│ aggregate │ merge         │  success  │ 100% │                              │
Pipeline 状态: failed

# 3. inspect 查看失败任务的完整错误堆栈
$ pipeline_cli inspect <instance_id> --step recognize --task rec_cable --workspace /tmp/cad_demo2
状态    : failed  进度 : 0%
错误    : RuntimeError: Intentional failure injected via PIPELINE_DEMO_FAIL=rec_cable
Traceback (most recent call last):
  File ".../scheduler.py", line 220, in _dispatch_task
    output = await task_instance.execute(validated_inputs, progress_cb)
  File ".../tasks.py", line 113, in execute
    _check_fail(self.task_id)
RuntimeError: Intentional failure injected via PIPELINE_DEMO_FAIL=rec_cable

# 4. fix --output：注入人工准备好的恢复数据（引擎自动校验 OutputModel 契约）
$ pipeline_cli fix <instance_id> \
  --task recognize/rec_cable \
  --output pipelines/cad_identify_pipeline/mock_data/recover_cable.json \
  --workspace /tmp/cad_demo2
Fixed (output): task 'recognize/rec_cable' → FIXED

# 5. resume：仅重调度 FAILED 任务；FIXED 任务跳过，不重复执行
$ PIPELINE_DEMO_FAST=1 pipeline_cli resume <instance_id> --workspace /tmp/cad_demo2
Resumed: 20260512T083200_497976_239806
  20260512T083200_497976_239806: success

# 6. 最终状态：rec_cable 为 fixed（已跳过），整体 success
$ pipeline_cli status <instance_id> --workspace /tmp/cad_demo2
│ recognize │ rec_building  │  success  │ 100% │
│           │ rec_cable     │  fixed    │   0% │   ← 保留注入数据，未重跑
│           │ rec_schematic │  success  │ 100% │
│ aggregate │ merge         │  success  │ 100% │
Pipeline 状态: success
```

### 场景五：细粒度执行 — 仅运行单个 step

```bash
# 只执行 parse_dxf 步骤，其余步骤不触发
$ PIPELINE_DEMO_FAST=1 pipeline_cli start cad_identify_cost_estimation \
  --step parse_dxf --workspace /tmp/cad_step --wait
Started: 20260512T083250_212844_f15b79  (pipeline: cad_identify_cost_estimation)
  20260512T083250_212844_f15b79: new    ← pipeline 整体仍是 new（只完成了一步）

$ pipeline_cli status <instance_id> --workspace /tmp/cad_step
│ parse_dxf │ read_dxf       │ success │ 100% │
│           │ parse_entities │ success │ 100% │
Pipeline 状态: new
```

### 场景六：交互式 REPL

不带子命令启动时进入 REPL。REPL 与调度器共享事件循环——任务在后台执行，前台随时可输入命令查询或干预。

```
$ pipeline_cli --workspace /tmp/cad_demo
Pipeline REPL  (输入 help 查看命令)
工作目录: /tmp/cad_demo

pipeline> load pipelines/cad_identify_pipeline/pipeline.yaml
已加载: cad_identify_cost_estimation

pipeline> start cad_identify_cost_estimation
Started: 20260512T...  (pipeline: cad_identify_cost_estimation)

# status --watch 持续刷新进度表格，任务完成后自动退出
pipeline> status cad_identify_cost_estimation --watch

# 查看 task 详情
pipeline> inspect <instance_id> --step recognize --task rec_cable

# 任务失败后注入修复数据
pipeline> fix <instance_id> --task recognize/rec_cable --output /path/to/fix.json
修复成功 (output): task 'recognize/rec_cable' → FIXED

# 恢复执行
pipeline> resume <instance_id>
已恢复: 20260512T...

# 查看运行日志（默认最后 100 行，ERROR 行红色高亮）
pipeline> log <instance_id>
pipeline> log <instance_id> --errors-only          # 只看错误行
pipeline> log <instance_id> --tail 50              # 最后 50 行
pipeline> log <instance_id> --offset 100 --tail 50 # 向上翻页

# 清屏
pipeline> clear

pipeline> exit
```

---

## REPL 命令参考

```
load <path> [<path>...]                                            注册 pipeline YAML 文件
list [--pipeline]                                                  列出 pipeline（pipeline_id / type / name）
list --instance                                                    列出运行实例（pipeline_id / instance_id / status）
start <pipeline_id> [<id>...] [--step S] [--task T]                启动 pipeline 实例（非阻塞）
status <instance_id> [--watch] [--all]                             查看 pipeline 实例状态；--watch 持续刷新
inspect <instance_id> [--step S] [--task T]                        查看 task 详情
stop <instance_id>                                                 中止指定 pipeline 实例（整个 run）
resume <instance_id> [--include-paused]                            重调度 FAILED（可选 PAUSED）任务
fix <instance_id> --task S/T --output PATH                         注入恢复的 output.json → FIXED
fix <instance_id> --task S/T --input PATH                          替换 input.json → 复位为 NEW
log <instance_id> [--tail N] [--offset N] [--all] [--errors-only]  查看 run 日志；ERROR 行红色高亮
clear                                                              清屏
help / exit
```

**`log` 命令说明**

| 参数 | 说明 | 默认 |
|---|---|---|
| `--tail N` | 显示最后 N 行 | 100 |
| `--offset N` | 从末尾第 N 行往前取（实现"上翻页"：`--offset 100 --tail 100`） | 0 |
| `--all` | 显示全部内容，忽略 `--tail` | — |
| `--errors-only` | 仅显示 `ERROR` 级别行 | — |

日志文件路径：`<workspace>/.pipeline_runs/<pipeline_id>/<instance_id>/run.log`

> **Tab 补全**：按 Tab 可补全命令名、pipeline ID（已 `load` 的）、instance ID（已启动的 pipeline 实例，旁注 `pipeline=<pid> | status=<status>`）、`--step`/`--task` 参数值，以及 `load`/`fix --output`/`fix --input`/`log` 的文件路径与 flag。

---

## Pipeline YAML 格式

完整 Schema 参见 [`config/pipeline.schema.json`](config/pipeline.schema.json)。

```yaml
version: "1.0"
pipeline:
  id: my_pipeline
  name: "我的 Pipeline"
  type: "AI数据工程"          # 必填，业务分类标签（用于 list --pipeline 展示）
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
| `input.json` | `<workspace>/.pipeline_runs/<pipeline_id>/<instance_id>/<step_id>/<task_id>/` | 引擎在执行前写入 |
| `output.json` | 同上 | 任务成功后引擎原子写入 |
| `manual_data/<step_id>/output.json` | `<workspace>/` | skip=true step 的预置输出 |
| `state.json` | `<workspace>/.pipeline_runs/<pipeline_id>/<instance_id>/` | run 状态快照，进程崩溃后恢复用 |
| `run.log` | `<workspace>/.pipeline_runs/<pipeline_id>/<instance_id>/` | 全量运行日志（引擎生命周期 + Python logging + task self.logger + stdout）；resume 续写，`log` 命令查看 |

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

## 第三方依赖

### 运行时依赖

| 软件包 | 版本 | 许可证 | 商业使用 |
|---|---|---|---|
| pydantic | 2.13.4 | MIT | ✓ |
| networkx | 3.6.1 | BSD-3-Clause | ✓ |
| PyYAML | 6.0.1 | MIT | ✓ |
| rich | 15.0.0 | MIT | ✓ |
| prompt_toolkit | 3.0.52 | BSD | ✓ |
| typer | 0.25.1 | MIT | ✓ |

### 开发 / 测试依赖（不随产品分发）

| 软件包 | 版本 | 许可证 | 商业使用 |
|---|---|---|---|
| pytest | 9.0.3 | MIT | ✓ |
| pytest-asyncio | 1.3.0 | Apache-2.0 | ✓ |
| pytest-cov | 7.1.0 | MIT | ✓ |

所有依赖均为宽松许可证（MIT / BSD / Apache-2.0），可在商业产品中使用，无 Copyleft 强制开源要求。主要义务：保留版权声明；Apache-2.0 额外要求标注修改内容，并附带专利授权。

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
NEW     → RUNNING → SUCCESS
                  → FAILED
                  → PAUSED     （abort_event 触发，调度器内部）
         SKIPPED               （step 级别 skip=true）
任意    → FIXED                （fix --output 后）
FAILED  → NEW                  （reset_for_resume）
PAUSED  → NEW                  （reset_for_resume --include-paused）
```

关键模块（`pipeline_engine/core/`）：

| 模块 | 职责 |
|---|---|
| `scheduler.py` | 驱动单次 run 的完整执行流程（拓扑排序 + 并发分发） |
| `state_manager.py` | 运行时状态的唯一可信来源（asyncio.Lock 保护 + 状态机守卫） |
| `run_manager.py` | 进程级协调器（加载/启动/停止/恢复/修复多个 pipeline） |
| `run_context.py` | 单次 run 的运行时容器（scheduler + state_manager + abort_event） |
| `run_logger.py` | per-run 日志管理（FileHandler 隔离 + ContextVar 多 run 并发安全 + stdout 路由） |
| `plugin_loader.py` | 动态加载 BaseTask 子类（点分路径 → 类实例） |
| `storage.py` | 所有磁盘 I/O 的统一入口（原子写入 + 注册表） |
| `dag_validator.py` | DAG 合法性校验 + 拓扑分代计算 |
| `base_task.py` | 所有业务任务必须继承的抽象基类；暴露 `self.logger` 供任务写入 run.log |
