# 纯 C++ Dataflow 分布式引擎调研

## 目标
- 优先对齐常见 DataFrame / SQL 使用模型
- 采用 C++ 生态可落地方案，避免依赖 JVM/Python 主栈
- 先交付可运行的本地核心，再逐步扩展到分布式
- 目标能力优先级：DataFrame + SQL + Streaming 首发

## 现状定位与原则
- 强调“接口兼容层”而非同名替换。
- 以可维护的最小可用版本为先，先保证行为和可观测性，再做完整兼容。
- 统一规划路径：Parser -> Planner -> 逻辑计划 -> 执行器

## 第一轮结论
1. **先做核心语义，不追求一比一 API 复制**
   - 阶段 1：DataFrame + SQL（可查询最小子集）
   - 阶段 2：Streaming 与状态化能力补齐
   - 阶段 3：按需补齐扩展 API

2. **先复用可控生态，后内核增强**
   - 计算与执行算子可继续轻量自研
   - 计划/表达式可借鉴 Substrait 等思想做可扩展结构
   - 通信与调度通过可插拔方式接入（如 gRPC/本地运行时）

3. **兼容接口不追求“同名同义”**
   - 显式提供命名映射与行为差异说明
   - 重点是迁移路径稳定、语义明确、易于替换实现

## 技术路线
- 路线 A（长期可控）：自研执行核心，完全掌控状态、容错与调度语义。
- 路线 B（最快验证）：基于现成 C++ 表达式/执行库补齐分布式接口。
- 路线 C（弹性优先）：RPC-first worker/grid 先打通，再做深度优化。

## 已实现版本（v0.1 本地内核 + v0.2 流式起步）
- 代码位置：`src/dataflow/`
- 已落地能力：
  - `DataflowSession` / `DataFrame`
  - `read_csv`
  - `select` / `filter` / `withColumn` / `drop` / `repartition`(No-op) / `cache`(eager)
  - `groupBy + sum`（含 `GroupedDataFrame`）
  - `join`（inner/left）
  - `count` / `show` / `collect`
  - SQL 管线：`SELECT ...` -> `Parser -> Planner -> DataFrame`
  - SQL v1 支持子集：
    - `SELECT / FROM / WHERE / GROUP BY / HAVING / JOIN / LIMIT`
    - 聚合函数：`SUM / COUNT / AVG / MIN / MAX`
    - 列别名、`table.*`、字段歧义报错
  - 简易 Catalog：`createTempView` / `resolve`
  - 序列化抽象：`ProtoLikeSerializer` / `ArrowLikeSerializer` 占位实现
  - Streaming API：`StreamingDataFrame`（select/filter/withColumn/drop/groupBy+sum）
  - StateStore 抽象：KV、Key-Map、Value-List、Key-List
- 构建：`Bazel` + `BUILD.bazel`

## 运行示例
- `bazel run //:df_demo`
- `bazel run //:wordcount_demo`
- `bazel run //:stream_demo`
- `bazel run //:stream_stateful_demo`
- `bazel run //:stream_state_container_demo`
- `bazel run //:sql_demo`

## 里程碑（建议）
- M0（1~2 周）：Streaming 契约冻结
  - 定义 `readStream/writeStream` 与 micro-batch 语义
  - 明确 checkpoint 与一致性边界
- M1（2~4 周）：批式与 SQL v1 稳定
  - `SqlParser` / `SqlPlanner` 错误可恢复
  - SQL 与 DataFrame 计划解释能力
- M2（4~8 周）：分布式批+流统一
  - 作业生命周期、checkpoint/恢复、失败重试
  - 支持 WordCount/Join 的流式场景
- M3（6~10 周）：批式增强与兼容收口
  - 窗口、更多聚合、join 完整性补齐
  - SQL 兼容增强与性能基线
- M4（8~12 周）：工程化
  - 监控指标、作业追踪、超时取消、失败诊断
  - 与既有平台互操作评估（metastore/同源 SQL）

## 你下一步可确认的输入
- 目标运行环境：裸机 / K8s / 私有云
- 延迟与吞吐目标（P95 / TPS）
- 是否先按 batch、先行 stream，或双栈并行
- 是否需要更强的安全治理（列级权限/PII 掩码）
