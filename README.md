# 纯 C++ Dataflow 引擎调研项目

本仓库聚焦纯 C++17 数据流引擎演进：先稳定 `DataFrame / SQL / Streaming` 最小闭环，再逐步扩展到可分布式运行时。当前路线不是外部大数据框架的同名移植，而是围绕本地执行、可诊断边界、可扩展调度接口做内核化实现。

## 本轮里程碑（2026-03-28）

当前已对齐的执行路线是：`operator chain in-proc + cross-process RPC + simple jobmaster`。

- 单进程路径：`sql_demo`、`df_demo`、`stream_demo` 继续保留，用于验证算子链、SQL 解析/规划、本地执行语义。
- 多进程路径：新增 `actor + rpc + jobmaster` 最小闭环，由 scheduler 统一接入 worker / client 并完成任务受理、分发、回传。
- 可观测性：多进程链路已具备基础日志与失败文案，便于按步骤排查 `listen / connect / accept / result`。
- 兼容性底线：现有单节点示例命令保持不变，仍优先保证 `bazel build //:sql_demo` 与 `bazel run //:sql_demo` 可用。

## 当前本地多进程架构

```text
client
  -> scheduler(jobmaster)
      -> worker
          -> in-proc operator chain
      -> result back to client
```

角色说明：

- `in-proc operator chain`：单个 worker 进程内部仍使用本地算子链执行，不把算子拆成跨节点执行图。
- `cross-process RPC`：client、scheduler、worker 之间通过 RPC/codec 传输任务与结果。
- `simple jobmaster`：当前由 `actor_rpc_scheduler` 承担最小 jobmaster 职责，只负责接收、分配、转发与状态观测，不做复杂容错、重试、资源隔离。
- `single-node fallback`：保留 `--single-node` 本地回退路径，避免影响已有单机闭环。

这意味着当前版本已经具备“本地多进程协作执行”的最小形态，但还不是完整分布式运行时。

## 术语统一

为避免外部生态专有命名侵入仓库，统一使用以下术语：

- `SparkSession` -> `DataflowSession`
- `spark.sql(...)` -> `session.sql(...)`
- `spark.read...` -> `session.read...`
- `Spark-like compatibility` -> `语义对齐` 或 `外部接口语义参考`

新增文档、注释、接口说明不要再引入 `spark`/`Spark` 命名。

## 当前能力边界

已具备：

- `DataflowSession` / `DataFrame` / `StreamingDataFrame`
- `read_csv`
- 基础算子：`select / filter / withColumn / drop / repartition`
- 聚合：`groupBy + sum`，以及 `SUM / COUNT / AVG / MIN / MAX`
- 本地 SQL 主链路：`SqlParser -> SqlPlanner -> DataFrame`
- 临时视图：`createTempView / resolve`
- actor/rpc 最小闭环：`scheduler / worker / client / smoke`

当前限制：

- 当前版本不扩展窗口函数（window）。
- 当前版本不扩展 CTE / 子查询。
- 当前版本不扩展 `UNION` 相关能力。
- `JOIN` 仅保留现有最小能力，不在本轮继续做更复杂 join 语义扩展。
- 当前多进程链路仍是本地多进程验证，不包含真正分布式调度、容错恢复、状态迁移、资源治理。
- 错误恢复能力仍需继续加强，当前主要依赖明确日志与失败文案排查。

## SQL 规划执行模型（v1）

`session.sql()` 已按 `Parser -> Planner -> DataFrame` 的三层推进：

- `SqlParser` 将 SQL 解析成 `SqlQuery` AST。当前已支持 `ON (...)`、`HAVING`、`LIMIT`，并可解析简单的括号谓词写法（如 `WHERE a.score > (5)`）。
- `SqlPlanner::buildLogicalPlan(...)` 生成逻辑步骤：`Scan -> (Join?) -> (Filter?) -> (Aggregate?) -> (Having?) -> Project -> (Limit?)`。
- `SqlPlanner::buildPhysicalPlan(...)` 将逻辑步骤映射到物理步骤，当前策略：
  - `Join` 与 `Aggregate` 作为 barrier 阶段；
  - 连续 `Filter/Project/Limit/Having` 可尝试融合为 `FusedUnary`；
  - 连续 `Project` 去重、连续 `Limit` 取小值（优化边界保留可验证）。
- `materializeFromPhysical(...)` 按物理步骤回放为现有 `DataFrame` 算子链，复用本地执行器。

## SQL DDL（v1）语义

参考 Spark/Flink 的 `Catalog` 语义，当前支持以下建表类型：

- `CREATE TABLE`：普通表，既可被 `SELECT`，也可被 `INSERT`。
- `CREATE SOURCE TABLE`：源表，偏向上游输入，当前版本限制为只读，禁止 `INSERT`。
- `CREATE SINK TABLE`：结果表，偏向下游输出，当前版本限制为只写，禁止 `SELECT`（含 `JOIN` 输入）。

当前版本仍是本地内存 catalog，`SOURCE`/`SINK` 只记录表语义并做执行约束校验，不直接引入外部连接器。

## SQL DDL / DML 支持（v1）

### DDL

- `CREATE TABLE`: 创建普通内存表，支持读写。
- `CREATE SOURCE TABLE`: 创建源表，仅可读。
- `CREATE SINK TABLE`: 创建汇聚/结果表，仅可用于写入。

示例：

```sql
CREATE TABLE users (id INT, name STRING, score INT);
CREATE SOURCE TABLE source_users (name STRING, score INT);
CREATE SINK TABLE summary_by_region (region STRING, score_sum INT);
```

### DML

- `INSERT INTO ... VALUES`：支持按列顺序插入或显式列名。
- `INSERT INTO ... SELECT`：支持把子查询结果写入目标表（当前解析器仅允许 `SELECT` 子查询语法）。

示例：

```sql
INSERT INTO users (id, name, score) VALUES (1, 'alice', 10), (2, 'bob', 20);

INSERT INTO summary_by_region
SELECT name AS region, SUM(score) AS score_sum
FROM users
GROUP BY name;
```

### 语义限制

- 任何 `INSERT` 到 `SOURCE` 表都会返回语义错误。
- 对 `SINK` 表执行 `SELECT`（或 `JOIN` 中作为右表）会返回语义错误。
- `SELECT` 中必须满足本版本约束（例如 `HAVING` 依赖聚合语义、`GROUP BY` 与投影一致性等）。

### 执行模型

- scheduler 只负责编排与分发，不直接执行 SQL。
- 包含 `dashboard` 在内的提交入口都走 worker 路径（`JobMaster + worker`）。
- 当未禁用自启动 worker（`--no-auto-worker`）且 worker 不可用时，调度返回 `no worker available`，不会在 scheduler 内本地“伪执行”。

### 调度策略（当前版本）

- 提交入口：`dashboard/client` 先构建计划并通过 `submitRemote` 创建 `job -> chain -> task`。
- 排队语义：远端可执行任务进入 `JobMaster` 的 `pending_remote_tasks_` 队列；只有 `chain` 变为可运行且调度器发现空闲 worker 时才会派发。
- 派发规则：`dispatcher` 按 `idle_workers` 空闲 worker 数从队列头取任务，按到达顺序依次下发（先到先服务）。
- worker 绑定：每个任务与 worker 连接绑定；发送成功后任务状态变更为 `TASK_SUBMITTED/CHAIN_SUBMITTED`，失败则回灌 `TASK_FAILED/JOB_FAILED`。
- 并发与并行：`chain` 并发受 `chain_parallelism` 和可用 worker 约束；每条 task 支持重试与 heartbeat 观测。
- 结果确认：worker 完成后返回 `Result`，scheduler 由 worker 响应更新 `job/chain/task` 快照并回推给提交端。
- 管理建议：用日志事件与 `/api/jobs` 快照观察 `JOB_* / CHAIN_* / TASK_*` 的状态变化判断是否在排队与是否正在运行。
- 当前默认 worker 策略：scheduler 默认自启动本地 worker（`--local-workers`，默认 2）。如需人工模式可使用 `--no-auto-worker`，此时才会出现 `no-worker-available` 回退路径。

## 构建与启动命令

### 单机示例

```bash
bazel run //:sql_demo
bazel run //:df_demo
bazel run //:stream_demo
```

### 构建 actor/rpc/jobmaster 路线

```bash
bazel build //:actor_rpc_scheduler //:actor_rpc_worker //:actor_rpc_client //:actor_rpc_smoke
```

### smoke 自检

```bash
bazel run //:actor_rpc_smoke
```

预期输出：

```text
[smoke] actor rpc codec roundtrip ok
```

### 三进程联调

终端 A：

```bash
bazel run //:actor_rpc_scheduler -- --listen 127.0.0.1:61000 --node-id scheduler --dashboard-enabled --dashboard-listen 127.0.0.1:8080
```

默认 scheduler 会自启动一个本地 worker（`--no-auto-worker` 可关闭），如果你希望强制手工 worker，可再按需启动：

终端 B：

```bash
bazel run //:actor_rpc_worker -- --connect 127.0.0.1:61000 --node-id worker-1
```

终端 C：

```bash
bazel run //:actor_rpc_client -- --connect 127.0.0.1:61000 --payload "demo payload"
```

预期日志：

```text
[scheduler] listen 127.0.0.1:61000
[worker] connected 127.0.0.1:61000
[client] job accepted: job_1
[client] job result: rows=..., cols=..., schema=..., first_row=...
```

### Dashboard（TypeScript + React）

- 访问地址：`http://127.0.0.1:8080`
- 前端代码只维护 `src/dataflow/runner/dashboard/app.ts`，通过 Bazel 目标
  `//:dashboard_app_js` 自动编译并输出 `src/dataflow/runner/dashboard/app.js`，避免重复维护两份代码。
- 页面能力：  
  - 通过 `payload` / `SQL` 触发任务提交
  - 查看 `/api/jobs` 与 `/api/jobs/{id}` 的任务历史/详情
  - 每秒自动刷新并可手动 `刷新`
- 启动示例（保留 RPC + 打开 Dashboard）：

```bash
./scripts/run_actor_rpc_scheduler.sh -- --listen 127.0.0.1:61000 --node-id scheduler --dashboard-enabled --dashboard-listen 127.0.0.1:8080
```

如果你已经编译过且希望跳过 TS 编译：

```bash
BUILD_DASHBOARD=0 ./scripts/run_actor_rpc_scheduler.sh -- --listen 127.0.0.1:61000 --node-id scheduler --dashboard-enabled --dashboard-listen 127.0.0.1:8080
```

### 一键联调（推荐）

```bash
./scripts/run_actor_rpc_e2e.sh
```

参数示例：

```bash
./scripts/run_actor_rpc_e2e.sh --payload "hello world"
./scripts/run_actor_rpc_e2e.sh --sql "SELECT 1 AS x"
DO_BUILD=1 ./scripts/run_actor_rpc_e2e.sh --sql "SELECT 1 AS x"
```

脚本配置环境变量：

- `ADDRESS`：RPC 地址（默认 `127.0.0.1:61000`）
- `WORKER_ID`：worker 名（默认 `worker-1`）
- `PAYLOAD`：默认 payload（默认 `demo payload`）
- `TIMEOUT_SECONDS`：超时（默认 `20`）
- `DO_BUILD`：是否先构建（`1`/`0`，默认 `0`）
- `BUILD_DASHBOARD`：是否先构建 dashboard TS（`1`/`0`，默认 `1`，底层调用 `bazel build //:dashboard_app_js`）

提交时若未发现可用 worker，Dashboard 接口会返回 `503 Service Unavailable`
并带有 `message: "scheduler reject: no worker available"`，说明任务尚未进入可执行调度队列。

### 单节点回退路径

```bash
bazel run //:actor_rpc_client -- --single-node --payload "local-only payload"
```

## 调试与验收步骤

### 1. build

```bash
bazel build //:sql_demo //:df_demo //:stream_demo \
  //:actor_rpc_scheduler //:actor_rpc_worker //:actor_rpc_client //:actor_rpc_smoke
```

### 2. smoke

```bash
bazel run //:actor_rpc_smoke
```

验收标准：

- 进程返回码为 `0`
- 输出包含 `[smoke] actor rpc codec roundtrip ok`
- 不得出现 `codec roundtrip failed` 或 `codec mismatch`

### 3. 多进程联动

按顺序启动 `scheduler -> worker -> client`。

验收标准：

- scheduler 先打印 `[scheduler] listen 127.0.0.1:61000`
- worker 打印 `[worker] connected 127.0.0.1:61000`
- client 先收到 `job accepted`，再收到 `job result`
- 不得出现 `no-worker-available`
- 不得出现 `cannot connect`
- 不得出现 `scheduler closed connection`

### 4. 常见错误规避

- `scheduler reject: no-worker-available`：通常是 worker 未连接成功，或 client 启动顺序过早。
- `[worker] cannot connect ...` / `[client] cannot connect ...`：通常是 scheduler 未启动、地址不一致或端口错误。
- `scheduler failed to listen on ...`：通常是端口占用或地址绑定失败。
- `Invalid --listen endpoint` / `Invalid --connect endpoint`：参数格式必须为 `host:port`。

## 示例源码扩展名核对

当前示例源码仍统一使用 `.cc`，`BUILD.bazel` 中可直接看到：

- `src/dataflow/examples/wordcount.cc`
- `src/dataflow/examples/dataframe_demo.cc`
- `src/dataflow/examples/stream_demo.cc`
- `src/dataflow/examples/stream_stateful_demo.cc`
- `src/dataflow/examples/stream_state_container_demo.cc`
- `src/dataflow/examples/sql_demo.cc`
- `src/dataflow/examples/actor_rpc_scheduler.cc`
- `src/dataflow/examples/actor_rpc_worker.cc`
- `src/dataflow/examples/actor_rpc_client.cc`
- `src/dataflow/examples/actor_rpc_smoke.cc`

额外核对命令：

```bash
find src/dataflow/examples -maxdepth 1 -type f ! -name '*.cc'
```

若该命令无输出，则说明示例目录下没有混入非 `.cc` 源文件。

## 一次 build/smoke 成功摘要命令

```bash
bazel build //:sql_demo //:df_demo //:stream_demo \
  //:actor_rpc_scheduler //:actor_rpc_worker //:actor_rpc_client //:actor_rpc_smoke \
  && bazel run //:actor_rpc_smoke \
  && echo '[summary] build+smoke ok'
```
