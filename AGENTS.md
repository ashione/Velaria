# AGENTS.md

本仓库用于纯 C++17 数据流引擎调研与演进，当前工作重心是先稳住本地最小闭环，再逐步把执行链路扩展到本地多进程与后续分布式运行时。

## 适用范围

- 主要语言：`C++17`
- 构建系统：`Bazel`
- 当前重点目录：`sql`、`runtime`、`planner`、`api`、`stream`、`catalog`
- 当前执行路线：`operator chain in-proc + cross-process RPC + simple jobmaster`

## 开发协作规则

- 优先保证语义一致性、边界可诊断、最小可用先行。
- 修改前先明确影响范围：`SQL / Planner / Executor / Session / Stream / Runtime`。
- 小步推进，每次改动围绕一个可验证目标，不做无关重构。
- 保持现有 C++/Bazel 风格，新增示例源码统一使用 `.cc`，头文件使用 `.h`。
- `session` 对外入口统一使用 `DataflowSession`。
- 现阶段不做 JVM/Python 宿主栈移植，核心计算逻辑保持纯 C++ 实现。
- 单节点示例命令保持可用，不要为了多进程实验破坏 `sql_demo / df_demo / stream_demo`。

## 命名与术语约束

不要再引入 `spark` / `Spark` 命名。统一替代为：

- `SparkSession` -> `DataflowSession`
- `spark.sql(...)` -> `session.sql(...)`
- `spark.read...` -> `session.read...`
- `Spark-like compatibility` -> `语义对齐` 或 `外部接口语义参考`

如果需要描述兼容关系，使用“语义对齐”“接口映射”“外部行为参考”，不要把外部框架名直接放进仓库主接口或注释里。

## 常用命令

### 构建

```bash
bazel build //:sql_demo
bazel build //:df_demo
bazel build //:stream_demo
bazel build //:actor_rpc_scheduler //:actor_rpc_worker //:actor_rpc_client //:actor_rpc_smoke
```

### 运行示例

```bash
bazel run //:sql_demo
bazel run //:df_demo
bazel run //:stream_demo
bazel run //:actor_rpc_smoke
./scripts/run_actor_rpc_scheduler.sh -- --listen 127.0.0.1:61000 --node-id scheduler --dashboard-enabled --dashboard-listen 127.0.0.1:8080
bazel run //:actor_rpc_worker -- --connect 127.0.0.1:61000 --node-id worker-1
bazel run //:actor_rpc_client -- --connect 127.0.0.1:61000 --payload "demo payload"
bazel build //:dashboard_app_js
```

### 一次 build/smoke 摘要

```bash
bazel build //:sql_demo //:df_demo //:stream_demo \
  //:actor_rpc_scheduler //:actor_rpc_worker //:actor_rpc_client //:actor_rpc_smoke \
  && bazel run //:actor_rpc_smoke \
  && echo '[summary] build+smoke ok'
```

## 最小验收清单

### actor/rpc smoke

```bash
bazel run //:actor_rpc_smoke
```

通过标准：

- 输出 `[smoke] actor rpc codec roundtrip ok`
- 返回码为 `0`

### 本地多进程联动

按顺序执行：

```bash
bazel run //:actor_rpc_scheduler -- --listen 127.0.0.1:61000 --node-id scheduler
bazel run //:actor_rpc_worker -- --connect 127.0.0.1:61000 --node-id worker-1
bazel run //:actor_rpc_client -- --connect 127.0.0.1:61000 --payload "demo payload"
```

或一键执行：

```bash
./scripts/run_actor_rpc_e2e.sh --payload "demo payload"
```

可选：通过 dashboard 启动同一运行链路：

```bash
./scripts/run_actor_rpc_scheduler.sh -- --listen 127.0.0.1:61000 --node-id scheduler --dashboard-enabled --dashboard-listen 127.0.0.1:8080
```

通过标准：
- scheduler 输出 `[scheduler] listen 127.0.0.1:61000`
- worker 输出 `[worker] connected 127.0.0.1:61000`
- client 先输出 `job accepted`，再输出 `job result`
- 日志中不出现 `no-worker-available`
- 日志中不出现 `cannot connect`
- 日志中不出现 `scheduler closed connection`

### 单节点底线

```bash
bazel run //:sql_demo
bazel run //:df_demo
bazel run //:stream_demo
```

修改与调试期间，以上单节点链路应保持不变。

## 错误规避要点

- 编译标准固定为 `C++17`，不要引入更高版本语法或库假设。
- 示例文件统一保持 `.cc`；不要新建 `.cpp` / `.cxx` 示例。
- `--listen` / `--connect` 参数必须是 `host:port`。
- 多进程验收顺序必须是 `scheduler -> worker -> client`。
- 若出现 `no-worker-available`，先查 worker 是否已连上 scheduler。
- 若出现 `cannot connect`，先查 scheduler 是否已监听、端口是否一致。
- `Value` 当前允许 `Int64/Double` 跨类型比较；若改成严格类型模式，需同步更新 Planner 与示例。
- `Schema` / `Join` 场景要注意对象生命周期，避免临时对象引用外泄。
- 当前多进程路径只是本地多进程验证，不要误写成完整分布式调度已完成。

## 当前边界

- 当前 SQL 仍是 v1 子集。
- 当前版本不扩展 window / CTE / 子查询 / `UNION`。
- `JOIN` 仅保留现有最小能力，不在本轮继续做更复杂 join 扩展。
- 错误恢复能力还需继续加强，当前优先依赖清晰日志与失败文案。
- 分布式运行时尚未真正接入，当前以本地执行器与本地多进程协作为准。
