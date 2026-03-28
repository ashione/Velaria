# 纯 C++ Dataflow 引擎调研项目

当前目标是将“DataFrame/SQL/Streaming/作业可观测性”先落到一个可运行的 C++ 本地内核，再逐步扩展到分布式运行时。项目不依赖 JVM/Python 主栈，不是“同名套壳”移植，而是为可插拔大模型 Agent 预留算子/调度与观察点的内核实现。

## 最新状态（2026-03-28）

- 已完成并可编译运行的核心闭环：`sql_demo` 已恢复可用。
- 已完成内容：
  - `session/sql` 从字符串入口走 `SqlParser -> SqlPlanner -> DataFrame` 流程。
  - SQL v1 子集支持：
    - `SELECT / FROM / WHERE / GROUP BY / HAVING / JOIN / LIMIT`
    - 聚合：`SUM / COUNT / AVG / MIN / MAX`
    - 列别名、`table.*` 展开、列歧义报错、未注册视图报错
  - 执行器与目录解析已修复常见运行时错误（类型比较兼容与 join 时 schema 生命周期安全）
- 编译标准已固定：
  - 根目录 `.bazelrc` 设置 `-std=c++17`

## 目标与原则
- 以最小可用实现为先：先保证可运行、可诊断、语义明确。
- 接口优先聚焦一致性，不追求一开始的全量语法兼容。
- Planner 与执行器边界清晰：SQL 只负责语义下推，不直接执行具体执行逻辑。
- 预留可扩展点（序列化、Runtime、调度、执行插件、Agent 接入）。

## 已实现能力清单（v0.1 / v0.2）
- 代码位置：`src/dataflow/`
- 已落地能力：
  - `DataflowSession` / `DataFrame` / `StreamingDataFrame`
  - 数据读取：`read_csv`
  - 核心算子：`select / filter / withColumn / drop / repartition`、`groupBy + sum`
  - 连接：`join`（inner/left）
  - 计数/显示/提取：`count / show / collect`
  - SQL：`Parser -> Planner -> DataFrame`
  - 视图：`ViewCatalog` + `createTempView / resolve`
  - 序列化：`Serializer`、`ProtoLikeSerializer`、`ArrowLikeSerializer` 占位实现
  - 状态抽象：`StateStore`（KV、Key-Map、Value-List、Key-List）
- 构建：`Bazel` + `BUILD.bazel`

## 运行说明

### 快速启动

```bash
bazel build //:sql_demo
./bazel-bin/sql_demo
```

或直接：

```bash
bazel run //:sql_demo
```

### 其他示例

- `bazel run //:df_demo`
- `bazel run //:wordcount_demo`
- `bazel run //:stream_demo`
- `bazel run //:stream_stateful_demo`
- `bazel run //:stream_state_container_demo`

### 已知要求

- 推荐始终使用 `bazel build //:target` 后直接运行 `./bazel-bin/<target>`，
  以便你在自定义 `cxxopt` 下稳定复现（当前固定 `C++17`）。

## 最近修复记录（便于回溯）

- 已修复 `sql_demo` 的编译与运行问题：临时对象生命周期、类型比较兼容、join 中 schema 生命周期管理。
- SQL 子查询演示输出样例已稳定：
  - 聚合：`name,total`
  - Join：`name	dept	bonus`

## 下一阶段建议

- 短期（S1）：补齐 SQL 解析容错与更清晰错误码（语法/语义分层）。
- 中期（S2）：引入可运行时注册的执行后端（本地算子/分布式 stub）。
- 长期（S3）：加入作业追踪（DAG、算子耗时、输入输出规模）与 Agent 插件钩子。

## 你可立即确认的事项

- 是否先补 `LEFT`/`RIGHT` join 与 `ORDER BY / DISTINCT`？
- 是否将 `Value` 的比较策略改为严格类型或保留当前数字自动对齐？
- 是否先对接统一的可观测面板（状态机事件流 + 指标）？
