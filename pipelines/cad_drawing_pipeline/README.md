# cad_drawing_pipeline — CAD 自动出图示例

## 业务故事板

模拟"根据需求规约自动生成 CAD 图纸"流程，共 4 个步骤 / 6 个任务：

```
Step 1  parse_requirement     (1 task)   解析 requirement.json
Step 2  generate_layout       (2 tasks)  并发生成平面与电气布局
Step 3  refine_drawing        (skip)     合图精修（预置 manual_data）
Step 4  export_dxf            (2 tasks)  导出 DXF + 校验（可注入失败演示）
```

## 接口覆盖对照表

| 任务 | 入口 | InputModel | OutputModel | progress | 特点 |
|:---|:---|:---:|:---:|:---:|:---|
| `ParseRequirement` | `async execute` | ✅ | ✅ | — | YAML `inputs:` → InputModel 校验（最干净的用法） |
| `GenerateFloorLayout` | `async execute` | — | ✅ | ✅ | 4 段 progress 推送（25/50/75/100）；跨 step 依赖 |
| `GenerateElectricalLayout` | `run_sync` | — | ✅ | — | 线程池执行；阻塞 I/O 模拟；跨 step 依赖 |
| `RefineDrawing` | （skip stub） | — | ✅ | — | `skip: true` 步骤；从 `manual_data/` 加载输出 |
| `ExportDXF` | `async execute` | ✅ | ✅ | ✅ | InputModel 包装 skip 步骤的 manual_data 输出 |
| `ValidateDXF` | `async execute` | ✅ | ✅ | — | InputModel 包装同 step 内 `depends_on` 输出；`PIPELINE_DEMO_FAIL` 演示 |

### InputModel 用法说明

| 场景 | 数据来源 | InputModel 写法 |
|:---|:---|:---|
| 静态配置（推荐） | YAML `inputs: {key: value}` | 字段名与 YAML key 1:1 对应 |
| 同 step 内依赖 | `inputs["task_id"]` = 上游 task 输出 | 字段名 = 上游 task_id |
| 跨 step 依赖（skip） | `inputs["step_id"]` = manual_data 全量 JSON | 字段名 = step_id |
| 跨 step 依赖（普通） | `inputs["step_id"]` = `{task_id: output_dict}` | 字段名 = step_id，类型为 dict（嵌套） |

## 前置条件：手动数据

步骤 `refine_drawing` 标记 `skip: true`。启动前，必须在工作区准备：

```
<workspace>/manual_data/refine_drawing/output.json
```

内容必须符合 `RefineDrawingOutput` schema（字段：`drawing_id`, `layers`, `elements`, `scale`）。
参考文件：`pipelines/cad_drawing_pipeline/mock_data/refine_drawing/output.json`

若文件缺失，引擎在 `start` 时立即报错（非运行时报错）。

## 快速演示

### 1. 正常流程

```bash
# 准备 manual_data（复制到 workspace）
mkdir -p /tmp/draw_demo/manual_data/refine_drawing
cp pipelines/cad_drawing_pipeline/mock_data/refine_drawing/output.json \
   /tmp/draw_demo/manual_data/refine_drawing/

# 启动 REPL
PIPELINE_DEMO_FAST=1 pipeline_cli --workspace /tmp/draw_demo

pipeline> load pipelines/cad_drawing_pipeline/pipeline.yaml
pipeline> start cad_drawing_pipeline
# 打印: instance_id = cad_drawing_pipeline_20260513-093024_7392
pipeline> status <Tab>           # Tab 补全 instance_id
pipeline> inspect <instance_id> --step export_dxf --task validate
```

### 2. 失败注入 + fix 恢复

```bash
# 启动时注入失败
PIPELINE_DEMO_FAST=1 PIPELINE_DEMO_FAIL=validate_dxf \
  pipeline_cli --workspace /tmp/draw_demo

pipeline> start cad_drawing_pipeline
pipeline> inspect <instance_id> --step export_dxf --task validate
# → status: FAILED

# 准备恢复数据
cat > /tmp/recovered_validation.json << 'EOF'
{"is_valid": true, "checked_rules": ["manually_verified"], "issues": []}
EOF

pipeline> fix <instance_id> --task export_dxf/validate \
               --output /tmp/recovered_validation.json
pipeline> resume <instance_id>
pipeline> status <instance_id>   # → SUCCESS
```

### 3. 进度观察

`GenerateFloorLayout` 和 `ExportDXF` 会推送 4 段 progress（25/50/75/100）。
在 `status --watch` 或 `inspect` 中可以看到进度条滚动。

## 文件说明

```
pipelines/cad_drawing_pipeline/
├── pipeline.yaml           # DAG 定义（含注释说明每个接口演示点）
├── schemas.py              # 所有 InputModel / OutputModel 及注释
├── tasks.py                # 6 个 BaseTask 子类，每个含接口说明注释
└── mock_data/
    ├── requirement.json    # 顶层输入（供 ParseRequirement 读取）
    ├── floor_layout.json   # 静态参考（LayoutOutput schema 示例）
    ├── electrical_layout.json
    └── refine_drawing/
        └── output.json     # skip 步骤的预置输出（必须存在于 workspace）
```
