# 纯 C++ Spark-like 分布式 Dataflow 引擎调研

## 目标
- 实现"接口上尽量像 Spark",优先对齐核心编程模型:
  - `RDD-like` 变换与行动(Transformation + Action)
  - DataFrame / Dataset 风格 SQL 与 DataFrame API(至少 SQL 支持)
  - 分布式执行(多节点、数据分区、shuffle、容错)
  - 作业提交/调度与资源治理
- 采用 C++ 生态可落地方案,避免依赖 JVM/Python 主栈

## 第一轮结论(先行调研)
1. **"完全复制 Spark API"在纯 C++ 下几乎不现实,建议走兼容层路线**:
   - 不是抄 API 数量,而是先达成"核心语义等价 + 可迁移查询特性 + 可替代生态接口"。
   - 分层目标:
     - 阶段 1:C++ DataFrame + SQL(最少但主力)
     - 阶段 2:DataFrame + RDD 兼容(核心变换)
     - 阶段 3:MLlib/结构化流可插拔(按需)

2. **C++分布式计算内核可优先复用生态构件**:
   - 计算与执行算子:Apache Arrow/Acero(Columnar)或自研轻量算子树;
   - 计划/表达式:可考虑基于 Substrait/自定义 LogicalPlan + PhysicalPlan;
   - 通信与 RPC:gRPC(优先)
   - 存储与交换格式:Parquet/Arrow IPC

3. **Spark 兼容接口建议不追求"一比一同名"**,先对齐"开发者迁移路径":
   - 命名映射 + 行为差异说明(例如:Spark 的 lazy execution、shuffle、cache 语义)
   - 提供兼容模板:`map/flatMap/filter/groupByKey/reduceByKey`、`select/filter/groupBy/join`、`read/write`、`cache/persist`、`count/collect/take`

## 参考 Flink / Ray / Mobius 的启发

### Flink 的可借鉴点
- **作业模型与流式核心**:Flink 的 DataStream/DataSet + Table/SQL 体系给了"端到端可验证的分布式执行模型",尤其是检查点(Checkpoint)和容错语义(如 exactly-once)可作为你们第二阶段目标。
- **状态管理能力**:有状态算子 + 状态后端(RocksDB/内存)是复杂窗口、join 的关键参考。
- **算子图优化**:Planner / optimizer 在 SQL 场景成熟,适合借鉴其逻辑计划->物理计划阶段设计。
- **代价**:Flink 的完整实现偏重 JVM/生态耦合,直接复刻难度高,但"算子接口 + 算法阶段划分"非常适合抄作。

### Ray / Mobius 的可借鉴点
- **弹性资源编排**:Ray 的任务/actor 调度、资源隔离和自动扩缩容对"动态并行度 + 弹性作业"很有参考价值。
- **对象传输模型**:对象存储/引用式共享思路可简化 Shuffle 中间数据分发,尤其比纯复制更省网络。
- **Mobius 思路**:可作为"轻量 Dataflow DAG + task graph + runtime plugin"参考:关注 API 兼容层与 runtime 解耦,而不是一次性把 SQL/运行时打成一块。
- **代价**:Ray 生态本身多偏通用并行计算,SQL/高级优化器要自己补齐。

### 结合你的目标(Spark-like + C++)的关键结论
- **第一阶段不必照抄 Flink/Spark**:直接把 Flink 的"容错-状态-作业生命周期"思想 + Ray 的"任务并行调度"抽象融入你自己的 C++ 核心。
- **建议采用"2 层引擎"**:
  1. `Core Engine`:轻量 Spark-like DSL + 算子执行(你们主要代码);
  2. `Runtime Layer`:参考 Flink 的 checkpoint/恢复流程 + Ray 风格资源调度策略。

## 候选技术路线(对比)

### 路线 A:自研 Execution Core(推荐用于长期可控)
- 组件:
  - Master/Worker 调度器(自研)
  - 基于 DAG 的 stage/task 调度
  - 分区化数据集(Partition)
  - Shuffle 服务(落盘 + 网络传输)
  - 容错(失败重试 + lineage 重新计算)
- 优点:技术栈可控,真正"纯 C++";缺点:初期开发量大

### 路线 B:基于现成 C++ 执行层(最快验证)
- 组件:选定成熟 C++ 表达式/算子引擎,补齐分布式调度层
- 优点:可快速拿到 SQL/表达式优化能力;缺点:分布式接口整合成本较高

### 路线 C:RPC-first 分布式网格(先 API、后优化)
- 组件:每个节点运行轻量 Worker;中心调度器只负责 DAG 与资源分配
- 优点:容易上手;缺点:早期性能瓶颈在调度/传输,需后续优化

## 构建与依赖建议
- 采用 **Bazel (Bzlmod)** 作为主构建/依赖管理(兼顾可复现性、增量构建、proto/gRPC/多模块扩展)
- 阶段性兼容:PoC 可通过最小二进制依赖快速推进,核心路径逐步替换为 Bazel 统一管理
- 详细策略与迁移建议见:`plans/build-system.md`

## 里程碑(建议)
> 你新要求:**优先支持流式**。里程碑已调整为 Streaming-first。

1. **M0(1~2 周)**:Streaming 契约冻结(v0.2A)
   - 定义 `readStream/writeStream` 契约、micro-batch 语义、checkpoint 与语义一致性边界
   - 定义 Streaming API 兼容映射(Spark-like `readStream`, `writeStream`, `trigger`, `awaitTermination`)
   - 输出:`plans/streaming-first-roadmap.md`

2. **M1(2~4 周)**:流式内核 v0.2
   - 本地流式 source(目录增量/内存队列) + stream 查询生命周期
   - 流式执行:`map/filter/withColumn/drop/groupBy/sum` 在 micro-batch 上跑通
   - 最小状态管理(内存状态) + 再次输出对齐

3. **M2(4~8 周)**:分布式批+流一体化
   - Master/Worker/Executor 适配流式作业控制
   - checkpoint + offset 恢复、失败重试语义
   - 可跑通 WordCount/Join 的 streaming 版本

4. **M3(6~10 周)**:批式增强与兼容收口(与流并行推进)
   - DataFrame v1(批):窗口、更多聚合函数、Join 完整性(right/full)
   - SQL 子系统扩展(where/having/basic函数)
   - 输出 v1 文档 + 性能 baseline

5. **M4(8~12 周)**:工程化
   - 监控指标、作业追踪、作业取消/超时、失败诊断
   - 与现有 Spark 集群互操作评估(metastore/catalag/兼容 SQL)

## 关键决策清单
- 先支持"接口兼容程度":
  - **目标 A:`spark-like`(面向 80% 常见用例)**
    - **当前阶段优先级:Streaming + DataFrame**(你已确认)
  - 目标 B:完整 `RDD`(长期)
- 已确认路线:**DataFrame/Streaming 共用同一计划层,RDD 作为兼容扩展层**,避免一次性复刻
- 详细 v1 兼容清单与执行优先级:见 `plans/dataframe-first-v1.md`
- 运行时:
  - 首版:gRPC + 本地文件系统(或对象存储)
  - 后续:支持 RDMA/K8s/容器编排
- 资源隔离:线程池 + CPU 份额 + 内存上限(可演进到 cgroups)

## 已实现版本(v0.1 本地内核 + v0.2 流式起步)
- 代码位置:`src/dataflow/`
- 已落地能力:
  - Spark-like `SparkSession` / `DataFrame`
  - `read_csv`
  - `select` / `filter` / `withColumn` / `drop` / `repartition`(No-op)/`cache`(v0.1 eager)
  - `groupBy + sum`(`GroupedDataFrame`)
  - `join(inner/left)`(v0.1)
  - `count` / `show` / `collect`
  - `sql("SELECT * FROM view [LIMIT N]")`
  - 序列化抽象:`ProtoLikeSerializer` / `ArrowLikeSerializer` 占位实现
  - 流式 API：`StreamingDataFrame`（支持 select/filter/withColumn/drop/groupBy+sum）
  - 状态后端抽象：`StateStore`
    - KV: `get/put/remove`
    - Key-Map: `getMapField/putMapField/removeMapField/getMapFields`
    - Value-List: `getValueList/appendValueToList/popValueFromList`
    - Key-List: `listKeys`
    - 默认 `MemoryStateStore`，并预留 `RocksDbStateStore`
  - `groupBy().sum(..., stateful=true)`：累计状态聚合（可用于滑动累计场景）
  - Bazel 构建:`MODULE.bazel` + `WORKSPACE` + `BUILD.bazel`
- 运行示例:
  - `bazel run //:df_demo`
  - `bazel run //:wordcount_demo`
  - `bazel run //:stream_demo`(v0.2 流式 PoC)
  - `bazel run //:stream_stateful_demo`(stateful 流式累计演示)
  - `bazel run //:stream_state_container_demo`(state store 抽象能力示例)

## RocksDB 状态后端说明
- 当前源码在 `stream.h/.cc` 已内置 `RocksDbStateStore` 实现。
- 这是按 `DATAFLOW_USE_ROCKSDB` 条件编译：
  - 未定义该宏时默认使用内存状态（零外部依赖）
  - 定义后可通过 `#include <rocksdb/db.h>` 链接 RocksDB 实现
- 启用示例（伪代码）：
  - `-c opt -c dbg` 编译时加 `--copt=-DDATAFLOW_USE_ROCKSDB=1`（具体请按你的 Bazel 设置绑定 dependency）
  - 然后在 `Bazel` 里加 rocksdb 依赖后，替换为 `makeRocksDbStateStore("/path/to/db")`

```cpp
// e.g.
auto state = makeRocksDbStateStore("/tmp/dataflow-state");
auto stream = StreamingDataFrame(source).withStateStore(state)
                  .groupBy({"key"}).sum("value", true);
```

## 你下一步可确认的输入(建议先给)
- 流式优先下，确认第一版是否只做 micro-batch（建议）
- 目标运行环境（裸机 / K8s / 私有云）
- 预期吞吐/延迟目标（每批延迟/每秒 TPS）
- 是否需要和现有 Spark 集群互操作（如读 Hive Metastore、同源 SQL 兼容）

