# Changelog

## [Unreleased] — 安全与可靠性审计修复（2026-05-19）

本轮审计聚焦四个维度：**并发竞态**、**内存泄漏**、**异步语义**、**状态机迁移合法性**。
共修复 2 个 CRITICAL、10 个 HIGH、6 个 MEDIUM、5 个 LOW 级问题，测试数量从 373 增至 399，覆盖率维持 ≥ 90%。

---

### CRITICAL

#### C1 — SSE 订阅者列表并发迭代 RuntimeError（`state_manager.py`）
`_notify()` 在迭代 `_subscribers` 时，若 SSE 客户端并发断开并调用 `unsubscribe()`，会触发
`RuntimeError: list changed size during iteration`，导致状态广播链路崩溃。
**修复**：`_notify()` 在迭代前先对订阅者列表制作快照 `subs = list(self._subscribers)`，对快照迭代；`unsubscribe()` 新增幂等保护（重复调用不抛异常）。

#### C2 — 跨进程 resume 重复执行已完成的 run（`runs.py`）
REST `POST /runs/{id}:resume` 未检查 pipeline 终态，可对已 SUCCESS/FIXED/SKIPPED 的 run 发起
resume，导致有副作用的任务被重复执行。
**修复**：resume 端点在调用 `cmd_resume` 前检查状态；若处于 `_RESUME_BLOCKED_STATUSES`（SUCCESS / FIXED / SKIPPED）则返回 422，拒绝请求。CLI `resume` 子命令不受此限制。

---

### HIGH

#### H1 — `_runs` 字典无界增长导致 serve 模式 OOM（`run_manager.py`）
每次 `start_run` 都向 `_runs` 写入 RunContext，从不清除，长期运行会无限占用内存。
**修复**：新增 `_prune_terminal_runs()`，在 `_MAX_RUNS = 200` 上限触发时按插入顺序驱逐最旧的终态 run（RUNNING/PAUSED 不受影响）。

#### H2 — SSE handler 订阅者队列在异常路径下泄漏（`events.py`）
`subscribe()` 在 `try` 块外执行，客户端在第一次 yield 前断开时 `finally` 不会调用 `unsubscribe()`，导致队列永久挂在订阅者列表中。
**修复**：将 `subscribe()` 移至 `_stream()` 生成器内 `try` 块首行，确保 `finally: sm.unsubscribe(q)` 无论如何都执行。

#### H3 — `list_instances` 无锁迭代期间并发插入导致 RuntimeError（`run_manager.py`）
`for ctx in list(self._runs.values()):` 虽制作了快照，但循环体内有 `await`，让出期间 `start_run()` 可向 `_runs` 插入新 entry，导致不一致。
**修复**：`list_instances()` 在 `async with self._lock:` 内执行 `snap = list(self._runs.items())`，出锁后对快照进行 await 操作。

#### H4 — `resume()` / `stop()` 竞态导致 stop 不生效（`run_manager.py`）
`resume()` 在锁外覆盖 `ctx.abort_event` 和 `ctx.main_task`；并发 `stop()` 可能 set 了旧的 abort_event（无人监听），使新启动的 run 无法被 stop。
**修复**：与 H9 合并修复——`abort_event` 和 `main_task` 的替换统一在 `async with self._lock:` 内完成；`stop()` 同样在同一 lock 内读取并 set abort_event。

#### H5 — `_persist()` 在 `asyncio.Lock` 内执行同步磁盘 I/O（`state_manager.py`）
高并发时 task 状态变更被磁盘延迟串行化，调度器整体吞吐受限；慢盘场景下更加显著。
**修复**：所有 transition 方法改为"锁内 mutate 内存 + 取快照，出锁后 `_async_persist(snapshot)`"模式；`_async_persist` 持有独立 `_persist_lock`（FIFO 顺序保证），通过 `asyncio.to_thread` 将 JSON 写入卸载到线程池。

#### H6 — abort 时插件将 CancelledError 转为 PipelineError 导致任务被错误记为 FAILED（`scheduler.py`）
插件内部捕获 `CancelledError` 并 re-raise 为 `PipelineError` 时，外层 `except Exception` 分支不检查 `abort_event`，将任务标记为 FAILED 而非 PAUSED。用户 stop + resume 后发现需要 fix 才能继续，与文档语义不一致。
**修复**：`_dispatch_task` 的 `except asyncio.CancelledError` 和 `except Exception` 分支均在处理前优先检查 `abort_event.is_set()`；若已 set，无论异常类型一律将任务置为 PAUSED。

#### H7 — `recover_task` / `replace_task_input` 允许对 RUNNING 任务操作（`state_manager.py`）
fix 流程可在任务仍在运行时将其状态硬切回 NEW/FIXED，与活跃的 asyncio.Task 形成状态错乱。
**修复**：`recover_task()` 和 `replace_task_input()` 首行检查源状态，若为 RUNNING 则抛 `PipelineError`。

#### H8 — `--no-wait` 语义不可实现，run 随进程退出被取消（`cli.py`）
`asyncio.run()` 返回时关闭事件循环，所有后台 Task 立即被取消，导致 `--no-wait` 下 run 在启动后即被销毁。
**修复**：从 `pipeline_cli start` 彻底移除 `--no-wait` 选项，CLI 始终阻塞等待完成。后台执行改为在 `serve` 模式下通过 `POST /runs`（返回 202）支持。

#### H9 — `resume()` 状态迁移非原子，中间态可被外部观察（`run_manager.py`）
`transition_pipeline_status(RUNNING)` 之后、`create_task` 之前存在窗口期，外部 `is_active()` 会看到"pipeline=RUNNING 但 main_task=done"的矛盾状态。
**修复**：`start_run()` 和 `resume()` 均在 `async with self._lock:` 内同时赋值 `ctx.abort_event` 和 `ctx.main_task = asyncio.create_task(...)`，消除中间态。

#### H10 — `resume()` 未清零 `finished_at` / `started_at`，重启后时间戳错配（`state_manager.py`）
resume 后 UI 可能显示"status=RUNNING、finished_at=上一次失败时间"的矛盾状态。
**修复**：`reset_pipeline_status(NEW)` 同时将 `started_at` 和 `finished_at` 清为 `None`；`reset_for_resume` 同样清除 task 的 `error`、`stack_trace`、`started_at`、`finished_at`、`progress`。

---

### MEDIUM

#### M1 — SSE 满队列事件丢弃无日志（`state_manager.py`）
慢客户端导致事件被静默丢弃，运维侧无任何可见信号。
**修复**：丢弃时调用 `_logger.warning("dropped event for slow subscriber, queue=%d", id(queue))`。

#### M2 — 终态状态集合各自硬编码，`_TERMINAL_STATUSES` 漏 FIXED / SKIPPED（`runtime_state.py` / `events.py` / `runs.py`）
SSE 流在 pipeline 到达 FIXED / SKIPPED 时不关闭，客户端永久挂起。
**修复**：在 `models/runtime_state.py` 定义 `TERMINAL_PIPELINE_STATUSES` 规范常量（单一事实来源）；`events.py` 的 `_TERMINAL_STATUSES` 和 `runs.py` 的 `_RESUME_BLOCKED_STATUSES` 均从此常量派生。

#### M3 — SUCCESS → RUNNING 非法状态迁移未被拦截（`state_manager.py`）
`start_pipeline()` / `start_step()` 不检查源状态，允许从 SUCCESS 再次迁移到 RUNNING，为跨进程重复 resume 埋下隐患。
**修复**：`start_pipeline()` 和 `start_step()` 在源状态非 NEW 时抛 `PipelineError`，拒绝非法迁移。

#### M4 — SSE 客户端断开检测依赖 yield 失败，延迟最长数分钟（`events.py`）
已实现：主循环以 `asyncio.wait_for(q.get(), timeout=25.0)` 驱动，超时发心跳；每次迭代开头检查 `await request.is_disconnected()` 快速退出。断开检测延迟 ≤ 25 秒。（审计时已存在，无需修改。）

#### M5 — `_notify` / `_persist` 顺序性（`state_manager.py`）
当前实现顺序为"mutate → persist → notify"，已是较安全顺序；若 persist 抛异常则 notify 不会发出，内存与磁盘保持一致。风险极低，列为观察项，暂不修改。

#### M6 — `_resolve_run` 在锁外读取 `_runs`（`run_manager.py`）
与并发 `start_run()` / `_prune_terminal_runs()` 形成读写竞态。
**修复**：`_resolve_run()` 注释标明须在持锁状态下调用；新增 `async _get_ctx(ref)` 辅助方法，内部加锁后调用 `_resolve_run`，供所有无锁调用方使用；`runs.py` resume 端点改用 `svc.rm.get_run_state(run_id)` 公共接口，不再直接访问 `_resolve_run`。

---

### LOW

#### L1 — `fix()` 的 `is_active()` 检查与 `_resolve_run` 之间存在 TOCTOU（`run_manager.py`）
`_get_ctx()` 释放锁后、`is_active()` 执行前，并发 `resume()` 可在此窗口启动 run，使活跃性检查失效。
**修复**：`_resolve_run()` 查找与 `is_active()` 检查合并到同一 `async with self._lock:` 块，消除竞态窗口。

#### L2 — `_RunAwareStream._buffers` 字典永不清理（`run_logger.py`）
每个 run 在 `_buffers[run_id]` 留下一条 entry，run 结束后不删除，serve 模式下无界增长。
**修复**：`RunLogger.detach()` 在移除 FileHandler 后，从 `sys.stdout` 和 `sys.stderr` 的 `_buffers` 中 `pop` 当前 `run_id` 的条目。

#### L3 — REST 响应 `command` 字段各处硬编码，容易写错（`routers/*.py`）
每个端点各自拼装 `{"ok": True, "command": "xxx", ...}`，无编译期保护。
**修复**：在 `schemas.py` 新增 `envelope_ok(command, **payload)` 和 `envelope_err(command, message, type, **payload)` 辅助函数，所有端点统一使用。

#### L4 — `serve` 未检测同一 workspace 的进程冲突（`cli.py`）
两个 `serve` 进程指向同一 workspace 时并发写 `.pipeline_runs/`，导致状态文件相互覆盖。
**修复**：`serve` 启动时在 `.pipeline_runs/.serve.lock` 上调用 `fcntl.flock(LOCK_EX | LOCK_NB)` 获取排他锁；第二个进程立即以 exit code 1 退出并打印错误；进程退出后锁自动释放。

#### L5 — `restore_runs_from_disk` 缺乏混合状态场景的测试覆盖（`tests/unit/test_run_manager.py`）
仅有 happy path，C2 / H3 / H4 等修复缺少回归测试。
**修复**：新增 7 个回归测试，覆盖：加载所有有效 run、跳过未注册 pipeline、跳过缺失 state.json、不覆盖已在内存中的 run、`write_back=True` 降级孤儿 RUNNING、`write_back=False` 保持 RUNNING 不变、混合 SUCCESS/FAILED/PAUSED/NEW 全部加载正确。

---

### 测试 & 文档

- 全量测试从 **373** 条增至 **399** 条，覆盖率从 90.01% 提升至 **90.63%**。
- `spec.md`：移除 `--no-wait`，`final_status` 无条件返回，新增 §4 状态机守卫约束，新增 §3.4 HTTP REST API 需求节。
- `design.md`：更新 §4.3 状态机守卫表、§6.2 RunManager 并发安全说明、§6.3 Scheduler abort 优先级、§6.4 StateManager I/O 解耦 / 新增守卫 / resume 字段清零 / 终态常量、§6.8 RunLogger buffer 清理、§7.2 移除 `--no-wait`，新增 §6.14 HTTP REST API 设计节。
- `README.md`：更新第三方依赖版本表（fastapi 0.136.1、uvicorn 0.47.0、sse-starlette 3.4.4、httpx 0.28.1）。

---

### 提交历史

| Commit | 内容 |
|---|---|
| `fb697bc` | fix(audit): P0/P1 批量修复（C1, C2, H1, H2, H4, H7, H10, M1, M2） |
| `9901169` | perf(H5): _persist() 移至锁外，消除同步 I/O 阻塞 |
| `e6d4d71` | fix(H6): abort 时插件异常一律置 PAUSED |
| `83b246d` | fix(H9): main_task 在 _lock 内赋值 |
| `4f6b081` | fix(H10): reset_pipeline_status 清除 started_at |
| `e43921d` | fix(H3): list_instances 在锁内制作快照 |
| `4384f36` | refactor(M2): TERMINAL_PIPELINE_STATUSES 单一来源 |
| `da763c9` | fix(M3): start_pipeline/start_step 拒绝非 NEW 源状态 |
| `6c9c954` | fix(H8): 移除 --no-wait |
| `cd4bf8f` | fix(M6): _resolve_run 加锁，_get_ctx() 辅助方法 |
| `5267ccf` | fix(L1): fix() 的 is_active() 与 resolve 合并到同一锁块 |
| `cb401a4` | fix(L2): detach() 清理 _buffers |
| `0f0f808` | refactor(L3): envelope_ok/envelope_err 统一响应信封 |
| `fbdbbe8` | fix(L4): fcntl 工作区排他锁 |
| `d44e037` | test(L5): restore_runs_from_disk 7 个回归测试 |
| `57363be` | docs: spec.md / design.md 同步审计修复结果 |
| `0a89aed` | docs(README): 更新第三方依赖版本 |
