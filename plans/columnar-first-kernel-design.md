# Columnar-first Kernel 设计研究

## 当前角色

这份文档记录 Velaria core runtime 下一阶段 columnar-first kernel 的研究设计与后续实现记录。

它最初不是实现计划；在研究问题被确认或显式推迟，并且用户明确批准从研究进入实现后，后续实现切片会记录在本文末尾的实现记录中。

本文补充以下文档：

- [core-runtime-columnar-plan.md](./core-runtime-columnar-plan.md)
- [../docs/core-boundary.md](../docs/core-boundary.md)
- [../docs/runtime-contract.md](../docs/runtime-contract.md)
- [../docs/benchmarks.md](../docs/benchmarks.md)

## 范围分类

这是 core kernel 内部的 data/runtime refactor。

最高风险范围：

- data/runtime migration，因为它会影响内部表表示、operator 数据流、aggregate state 布局、source pushdown 执行，以及 Arrow-backed column 的生命周期。

较低风险的子范围：

- `src/dataflow/core/execution/` 下的模块边界提取
- `src/dataflow/core/execution/runtime/` 下的 optimizer 扩展
- `src/dataflow/examples/` 与 `src/dataflow/tests/` 下的 benchmark / regression 补充

本工作不得改变稳定 public contract：

- `DataflowSession`
- `DataFrame`
- `StreamingDataFrame`
- 文件输入语义
- stream progress / explain / checkpoint 字段
- Python 生态层对同一 runtime behavior 的投影

## 当前状态

当前 runtime 形态：

- `Table` 仍以 `rows` 作为主要 concrete field。
- `ColumnarTable` 作为可复用的 `columnar_cache` sidecar 保留。
- `ValueColumnBuffer` 可以承载 materialized `Value`，也可以承载 Arrow-backed storage。
- 多个 operator 已经会主动保留或重建 columnar cache。
- 部分 aggregate 路径已经有 `dense`、`fixed-hash`、`packed-hash`、`sort-streaming` 等优化布局。
- CSV / line / JSON 已经有 file-source pushdown，但许多路径仍通过通用 `Value` decode 和 generic reducer 执行。

已有基础主要位于：

- `src/dataflow/core/execution/table.h`
- `src/dataflow/core/execution/columnar_batch.h`
- `src/dataflow/core/execution/runtime/execution_optimizer.h`
- `src/dataflow/core/execution/runtime/aggregate_layout.h`
- `src/dataflow/core/execution/file_source.h`
- `src/dataflow/core/execution/nanoarrow_ipc_codec.h`

### 代码阅读补充结论

2026-06-13 的进一步代码阅读给出以下约束：

- `columnar_batch.cc` 已经接近 3K 行，且同时承载 cache helper、Arrow-backed column copy/share、filter/project/limit/window/string kernels 等职责。新的 execution substrate 更适合放在相邻的 `columnar_exec.h/.cc`，避免继续放大 `columnar_batch.*` 的职责。
- `file_source.cc` 已经超过 4K 行，当前 pushdown、聚合、finalize、format-specific scan 交织在同一实现文件里。typed source pushdown 应优先抽出 shared reducer / typed slot 接口，再让 CSV、line、JSON 分别适配，而不是继续把新逻辑堆进单个函数族。
- `analyzeSourceExecution(...)` 当前只对 CSV 做 tokenizer/decode mode 策略；非 CSV source 直接保留 generic scalar scan path。这说明下一阶段若要获得通用优势，不能只优化 CSV tokenizer，还要把 line / JSON 的 typed reducer 接口纳入设计。
- `AggregatePartialLayoutKind` 当前只有 `GenericTable` 与 `KeyColumnar`。如果要让 `generic-table` 场景迁到更通用的 columnar state，需要引入更明确的 state-columnar / key-state-columnar layout，而不是复用含义过宽的 `KeyColumnar`。
- `aggregate_layout.cc` 已经有 `AggregatePartialBatch`、`BinaryKeyColumn`、typed fixed/string key merge 等基础，说明 columnar aggregate state 不需要从零开始；风险主要在 null semantics、string key 表示和 optimizer 选择边界。

## 同类产品对标

本节只记录对 columnar-first kernel 有直接借鉴价值的点。它不是产品定位对比，也不意味着 Velaria 要复制这些项目的完整架构。

### DuckDB: vectorized execution format

参考：

- <https://duckdb.org/docs/current/internals/vector>

可借鉴点：

- DuckDB 明确把 `Vector` 作为 execution-time in-memory data container，把 `DataChunk` 作为一组 vectors。这个分层直接支持 Velaria 的 `ColumnarExecColumn` / `ColumnarExecBatch` 方向。
- DuckDB 使用固定 size 的 vectorized execution，默认 `STANDARD_VECTOR_SIZE` 为 2048 tuples。Velaria 不需要马上固定同一个数字，但应该让 `ColumnarExecBatch` 有明确 batch-size policy，而不是让每个 source/operator 自行决定。
- DuckDB 支持 flat、constant、dictionary、sequence 等 vector formats，并通过 unified vector format 避免每个函数为每种 encoding 组合写专门实现。Velaria 应借鉴“少量基础 encoding + generic decoded/access view”的方向，而不是为 CSV/line/JSON 或每个 operator 堆独立特化。

不应照搬：

- DuckDB 是完整嵌入式数据库，Velaria 当前不应引入完整数据库 storage/catalog/transaction 语义。
- 不应一开始就实现所有 vector formats。Velaria 第一阶段只需要 flat、constant、dictionary/selection-like view 和 Arrow-backed fallback。

对 Velaria 的设计动作：

- `ColumnarExecBatch` 应显式携带 batch row count，并保留后续固定 batch-size 策略入口。
- `ColumnarExecColumn` 至少预留 `Flat`、`Constant`、`DictionaryView` 或 `SelectionView`、`ArrowBacked`、`ValueFallback` 这几类表示。
- function / aggregate kernel 应优先面向 decoded/access view，而不是直接依赖某一种 physical encoding。

### Velox: vectors, dictionary encoding, decoded vectors

参考：

- <https://facebookincubator.github.io/velox/develop/vectors.html>
- <https://facebookincubator.github.io/velox/develop/dictionary-encoding.html>

可借鉴点：

- Velox 把 vectors 作为 execution data 的基础表示，并且每个 vector 明确有 type、encoding、size。
- Velox 的 buffers 有 ownership / view 区分，适合指导 Velaria 处理 Arrow-backed lifetime owner 与 borrowed views。
- Velox dictionary encoding 既用于重复值压缩，也用于表达 filter 后的 row subset，避免复制数据。
- Velox 用 `DecodedVector` 把多层 dictionary / encoding 访问收敛成一个性能敏感路径可用的 view，并且按 selected rows decode。

不应照搬：

- Velox 是服务多个上层 SQL 引擎的统一执行库，Velaria 当前不需要完整复杂类型、connector 体系或全套 memory pool。
- 不应把 `DecodedVector` 概念做成过度抽象的虚调用层；Velaria 应先服务 `Int64`、`Double`、`String`、`Bool`、`FixedVector` 这几类现有类型。

对 Velaria 的设计动作：

- `ColumnarExecColumn` 必须显式记录 type、encoding、row count、nullability 和 owner/view 生命周期。
- `ColumnarExecView` 应把 selection vector 作为一等状态，用于 filter/project/aggregate 链路延迟 compact。
- 对 string group keys，可以先研究 dictionary ids，但必须保证 null 与原始 string identity 的语义稳定。

### Apache DataFusion: RecordBatch streaming and provider layers

参考：

- <https://docs.rs/datafusion/latest/datafusion/>
- <https://datafusion.apache.org/blog/2026/03/31/writing-table-providers/>

可借鉴点：

- DataFusion 以 Arrow `RecordBatch` 作为 execution unit，operator 尽量一批入、一批出；full sort / hash aggregate 这类 pipeline breaker 另行处理。
- DataFusion 把 table capability、physical execution plan、record-batch stream 分成不同层，`scan()` / `execute()` 应尽量轻量，把真正的数据生产放在 stream 中。
- 对 custom table provider，filter pushdown capability 与 actual pruning/scan 分开表达，这对 Velaria 的 source pushdown diagnostics 很有价值。

不应照搬：

- DataFusion 基于 Rust、Arrow crate 和 async stream；Velaria 当前仍是 C++20 本地 kernel，不应为了对齐而引入 async runtime 依赖。
- DataFusion 的通用 provider model 比 Velaria 当前 source/sink ABI 更宽，不应在本阶段扩展 public connector surface。

对 Velaria 的设计动作：

- `ColumnarExecBatch` 应对齐“operator batch as execution unit”的概念，但保留 Velaria 自己的 `Table` public boundary。
- source pushdown 应拆成 capability/shape 判断与实际 scan/reducer 执行两层，方便 explain/fallback。
- 对 aggregate、sort 这类 pipeline breaker，要在 explain / optimizer feedback 中清楚标记，而不是假装所有 operator 都可 one-batch-in one-batch-out。

### Polars: lazy optimizer, pushdown, explain

参考：

- <https://docs.pola.rs/user-guide/lazy/optimizations/>
- <https://docs.pola.rs/user-guide/lazy/query-plan/>

可借鉴点：

- Polars lazy optimizer 把 predicate pushdown、projection pushdown、slice pushdown、common subplan elimination、cardinality estimation 等作为可解释的 plan-level 优化。
- Polars 的 query plan 能展示 optimized / non-optimized 差异，尤其能说明 filter 被推到 scan 阶段。

不应照搬：

- Velaria 当前 SQL v1 surface 保持窄边界，不应因为参考 Polars 而扩展 lazy dataframe API 或更宽 SQL 功能。
- 不应把优化只做在 Python 生态层。Velaria 的 optimizer 和 execution decision 必须属于 core kernel。

对 Velaria 的设计动作：

- typed path selection、pushdown success/fallback、projection pruning、slice/limit pushdown 应有可解释输出。
- `Bound Plan Access` 需要 schema fingerprint / catalog invalidation，否则 plan reuse 会带来 stale binding 风险。
- benchmark 不应只测单 operator，还应保留 plan-and-execute 与 reused-plan 两类路径。

### ClickHouse: granules, sparse indexes, data skipping

参考：

- <https://clickhouse.com/docs/primary-indexes>
- <https://clickhouse.com/docs/optimize/skipping-indexes>

可借鉴点：

- ClickHouse 用 granule 作为读取和 skipping 的基本单位，通过稀疏索引和 data skipping 减少不必要 I/O 与内存处理。
- 这些机制强调 block/granule 级 stats、explainable skipping 和 compact in-memory index，而不是逐行索引。

不应照搬：

- ClickHouse 是面向持久列存的数据库系统；Velaria 当前 columnar-first kernel 先解决 execution-time 表示，不应提前引入完整 MergeTree-like storage 语义。
- 不应把 primary-index 设计塞进本轮 execution substrate。它更适合后续与 `analytical-storage-format-v1.md` 对齐。

对 Velaria 的设计动作：

- `ColumnarExecBatch` 可以预留 block/granule provenance，用于后续 stats pruning 和 benchmark diagnostics。
- 当前阶段只做 execution-time selection/pruning；持久 block stats 与 sparse index 放到 storage-format 后续阶段。

## 对标后的收敛判断

这些产品共同说明：真正有性能和通用优势的 columnar-first 不是“某个 operator 的快路径”，而是一组可组合的内部机制：

- batch/vector 是 execution unit
- column 有 type、encoding、nullability、ownership
- selection/dictionary view 用来延迟 copy
- decoded/access view 用来避免 encoding 组合爆炸
- source pushdown 是 capability + shape + execution 三层协同
- explain 必须能说明 optimizer 选择和 fallback
- storage-level skipping 与 execution substrate 相关，但不应抢跑

因此 Velaria 的当前研究方向保持不变，但应更明确：

- 第一阶段优先建 `columnar_exec.*`，不要继续扩大 `columnar_batch.*`。
- `ColumnarExecColumn` 不只是 typed array，还要有 encoding 和 ownership model。
- `ColumnarExecView` 应成为 selection vector / dictionary-like view 的承载点。
- source pushdown pilot 要覆盖 shared typed reducer interface，不能只做 CSV tokenizer 优化。
- aggregate state-columnar layout 要作为独立概念进入 optimizer，而不是继续把语义塞进 `GenericTable` / `KeyColumnar` 二选一。

## 最近本地基线

以下基线来自 2026-06-13 当前 linked worktree。它们是本地研究数字，不是发布级 benchmark 结论。

验证基线：

```bash
bazel run //:actor_rpc_smoke
bazel test //:core_regression
bazel build //:sql_demo //:df_demo //:stream_demo
bazel test //:experimental_regression
```

观察结果：

- actor RPC smoke 通过
- core regression 通过，11/11
- 单节点 demo build 通过
- experimental regression 通过，1/1

批聚合基线：

```bash
bazel run //:batch_aggregate_benchmark -- 1048576 3
```

代表性观察：

| Scenario | Selected impl | Partial layout | Runtime shape | Rows/s |
|---|---|---|---|---:|
| `single-int64-low-domain` | `dense` | `generic-table` | `generic-single-int64-key` | `13.6M` |
| `single-int64-high-domain` | `hash-fixed` | `generic-table` | `sum-single-int64-key` | `2.2M` |
| `double-int64` | `hash-packed` | `key-columnar` | `generic-packed-keys-2` | `6.2M` |
| `mixed-string-int64` | `hash-packed` | `key-columnar` | `generic-packed-keys-2` | `3.4M` |
| `int64-two-string` | `hash-packed` | `generic-table` | `generic-packed-keys-3` | `2.3M` |
| `ordered-string` | `sort-streaming` | `generic-table` | `generic-single-string-key` | `3.7M` |

解读：

- 现有执行形态选择已经有价值。
- `generic-table` partial layout 仍然足够常见，会限制更广泛的 columnar-first 收益。
- 下一步通用优势应来自 typed key/state columns，而不是 benchmark-specific shortcut。

字符串 builtin 基线：

```bash
bazel run //:string_builtin_benchmark -- 100000 5
```

代表性观察：

| Case | Avg time | Rows/s |
|---|---:|---:|
| `copy-column` | `88,858 us` | `1.13M` |
| `single-arg-functions` | `208,334 us` | `480K` |
| `multi-arg-functions` | `324,071 us` | `309K` |
| `dependent-chain` | `431,491 us` | `232K` |
| `sql-plan-and-execute` | `453,824 us` | `220K` |
| `sql-reused-plan` | `167,980 us` | `595K` |

解读：

- plan reuse 和 bound column access 是实质性能因素。
- columnar-first kernel 不应只改数据布局，还应避免重复执行同一 plan 时反复绑定列访问形态。

文件源基线：

```bash
bazel run //:file_source_benchmark -- 200000 3
```

代表性 pushdown ratio：

| Case | No pushdown | Pushdown | Ratio |
|---|---:|---:|---:|
| `sql_csv_predicate_and_group_count` | `706,433 us` | `111,309 us` | `0.158` |
| `sql_csv_predicate_or_group_count` | `666,416 us` | `176,085 us` | `0.264` |
| `sql_csv_predicate_mixed_group_count` | `727,813 us` | `268,078 us` | `0.368` |
| `sql_line_predicate_or_group_count` | `1,023,015 us` | `250,730 us` | `0.245` |
| `sql_json_predicate_or_group_count` | `1,435,851 us` | `460,597 us` | `0.321` |

解读：

- source pushdown 方向正确。
- 下一步收益应减少 pushdown 路径中的 decode 和 reducer 开销。
- CSV、line、JSON 应尽量共享同一 typed pushdown shape。

## 目标状态

目标是 columnar-first internal execution substrate，不是新的 public table API，也不是第二套执行引擎。

目标数据流：

```text
source / Arrow / retained cache
  -> ColumnarExecBatch / ColumnarExecView
  -> bound column access + typed filter/project/aggregate kernels
  -> ColumnarExecBatch
  -> Table / rows / Arrow / sink boundary only when required
```

关键性质：

- public `Table` 和 `DataFrame` 行为保持稳定
- row materialization 变成显式边界动作
- source scan 可以直接产生 filter、key、aggregate state 所需的 typed slots
- operator 之间可以传递 typed column views，而不是先构造 `Row<Value>`
- aggregate state 对常见形态使用 typed key/state columns
- 不支持的形态回退到现有 generic path，并带有可诊断原因

## 不变量

以下行为不得改变：

- `DataflowSession` 仍是唯一 public session 入口。
- `session.sql(...)`、`session.read(...)` 和 streaming SQL 语义保持稳定。
- `DataFrame::toTable()` / `toRows()` 继续显式 materialize rows。
- `to_arrow()` 继续优先使用 retained columnar / Arrow backing。
- stream progress 使用 `rowCount()` 语义，而不是 `rows.size()`。
- `explainStreamSql(...)` 仍只返回 `logical`、`physical`、`strategy` 三段。
- source/sink ABI 生命周期与 checkpoint 行为不变。
- Python 生态层只投影同一 core behavior，不进入执行热路径。
- actor/rpc 仍属于 experimental runtime，不定义 core kernel contract。

## 方案对比

| Option | Why it fits | Risk | Decision |
|---|---|---|---|
| 在现有 `Table` 后引入 typed columnar substrate | 能带来可测性能收益，同时保留 public contract。先让 operator 共享更强内部形态，再决定是否改 `Table` 本身。 | 需要谨慎处理 `ColumnarTable`、Arrow backing、row materialization 之间的 adapter。 | 优先采用。 |
| 直接把 `Table` 改成 columnar-primary | 与目标内部模型最一致。 | 在性能收益被证明前，会牵动 tests、sinks、RPC、serializers 和 row boundaries，改动面过大。 | 推迟。 |
| 继续增加 operator-specific fast path | 单个 benchmark 可能快速改善。 | 会分裂语义，让 optimizer 决策越来越难解释，也容易变成 benchmark-shape shortcut。 | 不作为主策略。 |
| 先推进 distributed / actor runtime | 对执行拓扑研究有价值。 | 不能解决本地 decode/materialization 成本，还会让 experimental runtime 反向驱动 kernel 设计。 | 保持 experimental。 |

## 拟议内部组件

### `ColumnarExecBatch`

职责：

- 表示 operator 之间交换的内部 batch
- own 或 borrow column buffers
- 把 row count 与 schema 放在同一个执行对象里
- 只通过显式边界函数提供 row materialization

待研究字段：

- schema reference 或 schema value
- row count
- `ColumnarExecColumn` 列数组
- optional source provenance，用于诊断
- optional Arrow-backed lifetime owner

### `ColumnarExecColumn`

职责：

- 表示一个 typed column slot
- 避免所有热路径都被迫经过 `Value`

候选变体：

- `Int64`
- `Double`
- `String`
- `Bool`
- `Float32`
- `FixedVector`
- `ValueFallback`
- `ArrowBacked`

每种变体都必须显式携带 nullability。

### `ColumnarExecView`

职责：

- 提供轻量 projected views
- 支持 filter/project/aggregate 不复制完整列
- 当 filter 尚未 compact rows 时显式携带 selection vector

这对 `filter -> project -> aggregate` 链路很重要，因为过早 copy 会抵消 columnar execution 的收益。

### Bound Plan Access

职责：

- 对 plan column indices 和 typed expectations 做一次绑定
- 让同一 plan 重复执行时复用绑定结果

动机来自 string builtin benchmark 中 `sql-plan-and-execute` 与 `sql-reused-plan` 的明显差距。

待研究问题：

- 绑定结果应放在现有 physical plan node、`ExecutionOptimizer`，还是新的 execution-preparation object 中？

## 数据流设计

### Source Pushdown

目标：

- scanner 只 emit filter、key、aggregate expression 需要的列
- `COUNT` 和 numeric `SUM` 使用 typed reducer state
- string key 在可行时使用稳定 view 或 dictionary-like ids
- CSV、line、JSON parse 后尽量共享同一 `SourcePushdownExecShape`

候选执行形态：

- `ConjunctiveFilterOnly`
- `SingleKeyCount`
- `SingleKeyNumericAggregate`
- `MultiKeyCount`
- `MultiKeyNumericAggregate`
- `Generic`

回退规则：

- source 或 expression 不能安全产生 typed slots 时，回退到当前 generic source pushdown path
- fallback 必须能通过 optimizer / explain diagnostics 观察到，不能静默发生

### Filter / Project

目标：

- filter 产生 selection vector 或 compacted typed batch
- project 只做重排/重命名时产生 view
- computed column 在结果类型已知时追加 typed column
- row materialization 推迟到 row boundary 明确要求时

初始优先级：

- `Int64`、`Double`、`String` comparison filters
- projection 与 alias
- 语义已稳定的简单 string builtin chain

### Aggregate

目标：

- 把更多 `generic-table` partial layouts 替换为 key-columnar/state-columnar layouts
- 常见 reducer shape 保持 typed aggregate state
- mixed 或 unsupported `Value` shapes 继续走 generic fallback

初始优先级：

- `COUNT(*)`
- numeric `SUM`
- 1 到 3 个 group keys
- `Int64` 与 `String` keys
- nullable keys 通过显式 null bitmap 处理

本工作不扩展 SQL aggregate 语义。

### Arrow / Nanoarrow

目标：

- 把 Arrow backing 作为 `ColumnarExecBatch` 的一等输入
- 对 prefix limit、projection 和简单 filter，在安全时保留 backing
- 只有需要 compaction 时才 materialize selected values

待研究问题：

- `ArrowColumnBacking` 应直接复用多少，哪些部分应被更干净的 execution-layer column type 包装？

## 分阶段研究与实现边界

### Phase 0: Research and Contract Lock

负责文件/模块：

- `plans/columnar-first-kernel-design.md`
- `plans/core-runtime-columnar-plan.md`
- 仅当稳定 contract 文案变化时，才更新 `docs/runtime-contract.md`

进入条件：

- 当前 core regression 为 green
- 当前 benchmark baseline 已记录

退出条件：

- 用户批准从研究进入实现
- phase 1 的开放问题已解决或显式推迟

回滚：

- revert documentation-only changes

### Phase 1: Execution Substrate Skeleton

负责文件/模块：

- `src/dataflow/core/execution/` 下新增或现有文件
- 可能是 `columnar_batch.h/.cc`，也可能是相邻的新 `columnar_exec.*`
- `src/dataflow/tests/` 下的聚焦测试

范围：

- 引入 `ColumnarExecBatch`、`ColumnarExecColumn`、`ColumnarExecView`
- 添加 `Table` / `ColumnarTable` adapters
- 添加显式 row materialization boundary
- 暂不做广泛 operator 迁移

退出条件：

- adapters 能正确 roundtrip rows 与 Arrow-backed columns
- `bazel test //:core_regression` 通过
- 无 public API 变化

回滚：

- 删除新 substrate 和 adapters；现有 `Table + columnar_cache` 路径保持不动

### Phase 2: Typed Source Pushdown Pilot

负责文件/模块：

- `src/dataflow/core/execution/file_source.*`
- `src/dataflow/core/execution/csv.*`
- `src/dataflow/core/execution/runtime/execution_optimizer.*`
- `src/dataflow/tests/file_source_test.cc`
- `src/dataflow/tests/source_materialization_test.cc`
- `src/dataflow/examples/file_source_benchmark.cc`

范围：

- 为 source filters 和 `COUNT` / numeric `SUM` 试点 typed slots
- 让 CSV、line、JSON 通过共享 execution shape 保持一致
- 保留 generic fallback

退出条件：

- file-source tests 通过
- `file_source_benchmark -- 200000 3` 中 covered SQL pushdown cases 的 ratio 保持 `< 0.40`
- covered pushdown case 不回退超过 `5%`，除非有记录清楚的语义修复

回滚：

- optimizer 回退到现有 generic pushdown path

### Phase 3: Columnar Aggregate State

负责文件/模块：

- `src/dataflow/core/execution/runtime/executor.*`
- `src/dataflow/core/execution/runtime/aggregate_layout.*`
- `src/dataflow/core/execution/runtime/execution_optimizer.*`
- `src/dataflow/tests/planner_v03_test.cc`
- `src/dataflow/examples/batch_aggregate_benchmark.cc`

范围：

- 把更多 aggregate partials 从 `generic-table` 迁到 typed key-columnar/state-columnar layouts
- 从 1 到 3 个 keys 和 `COUNT` / numeric `SUM` 开始
- 保持 dense、fixed-hash、packed-hash、sort-streaming optimizer choices 可观察

退出条件：

- 至少一个当前 `generic-table` aggregate scenario 迁到 columnar partial/state layout
- `batch_aggregate_benchmark -- 1048576 3` 无 scenario 回退超过 `5%`
- 至少一个目标 scenario 有实质改善，且改善能归因到减少 row/value materialization

回滚：

- optimizer 禁用新 columnar partial layout，回到当前 generic table partials

### Phase 4: Bound Column Access for Reused Plans

负责文件/模块：

- `src/dataflow/core/logical/planner/`
- `src/dataflow/core/execution/runtime/`
- `src/dataflow/core/contract/api/session.cc`
- string builtin benchmark 与 SQL regression tests

范围：

- 对可复用 plan shape 绑定 column access 与 result type expectations
- 在 plan 和 schema 稳定时，避免重复 SQL 执行反复做相同绑定

退出条件：

- `string_builtin_benchmark -- 100000 5` 保持 `sql-reused-plan` 优势，并在可测处降低 repeated binding overhead
- SQL regression 保持 green

回滚：

- 禁用 bound-access reuse，回到当前 execution preparation 路径

## 验证

任何实现阶段完成前，至少需要运行：

```bash
bazel test //:core_regression
bazel build //:sql_demo //:df_demo //:stream_demo
bazel run //:batch_aggregate_benchmark -- 1048576 3
bazel run //:string_builtin_benchmark -- 100000 5
bazel run //:file_source_benchmark -- 200000 3
```

触碰 source/sink 或 stream internals 时：

```bash
bazel test //:experimental_regression
./scripts/run_stream_observability_regression.sh
```

触碰 Python projections 或 Arrow interop 时：

```bash
bazel test //:python_ecosystem_regression
```

Benchmark guardrails：

- covered file-source SQL pushdown ratio 保持 `< 0.40`
- aggregate benchmark 无 scenario 回退超过 `5%`，除非有记录清楚的语义原因
- string builtin benchmark 无 case 回退超过 `5%`，除非有记录清楚的语义原因
- explain / fallback diagnostics 能说明 typed path selection 和 fallback reasons

## 风险登记

| Risk | Impact | Probability | Detection signal | Mitigation |
|---|---|---:|---|---|
| 延迟 row materialization 意外改变 public behavior | High | Medium | `core_regression`、row count mismatch、Python row output diff | 保持 `Table` public behavior 稳定；只在显式边界 materialize rows |
| Arrow-backed buffer 生命周期错误 | High | Medium | Arrow export 损坏、nanoarrow tests 失败、崩溃 | 在 `ColumnarExecBatch` 中保留 owner；增加生命周期测试 |
| typed path 变成碎片化特判 | Medium | Medium | optimizer 复杂度上升、fallback 覆盖不足 | shape selection 集中在 optimizer；必须保留 generic fallback |
| 某个 source format 变快但另一个回退 | Medium | Medium | file-source benchmark 按 CSV/line/JSON 拆分 | 共享 pushdown shape，并保留 per-format benchmark gates |
| aggregate null semantics 漂移 | High | Medium | nullable aggregate tests 失败、group count mismatch | null bitmap 是 typed key identity 的一部分 |
| plan binding cache 在 schema/catalog 变化后 stale | High | Low | SQL regression 失败、列映射错误 | 绑定 schema fingerprint，catalog 更新时失效 |
| experimental runtime 依赖未稳定 substrate 细节 | Medium | Low | actor/stream tests 失败、layering review | actor/rpc 继续通过 adapter 和 row-compatible boundaries 接入 |

## 回滚策略

每个阶段都应可逆：

- 在新 substrate 被证明有效前，保留现有 `Table + columnar_cache` 行为。
- optimizer selection 必须显式，使 typed paths 可以被禁用。
- 保留 generic source pushdown 和 generic aggregate paths。
- 在内部收益被测出前，不改 public data structures。
- 本路线不移除 stream sinks、memory snapshots、actor RPC formats 的 row-compatible boundaries。

当前没有任何不可逆步骤被批准。

## 停止条件

出现以下情况时暂停研究或实现：

- `core_regression` 失败且原因不能直接解释
- benchmark 回退超过 `5%` 且没有明确语义原因
- typed paths 需要扩展 SQL surface 才能成立
- fallback diagnostics 比当前更不精确
- explicit read 和 probed read 的 source format 行为分叉
- Python 或 stream contracts 需要 public field rename
- 设计要求 actor/rpc 成为 core kernel 的一部分

## 开放研究问题

1. `ColumnarExecBatch` 已落在相邻的 `columnar_exec.*`，当前 include 方向是 `columnar_exec.*` 复用 `columnar_batch.*` 的 cache/value view 能力，避免反向依赖。
2. typed execution columns 应包装 `ValueColumnBuffer`，还是让 `ValueColumnBuffer` 退为 compatibility/fallback 表示？
3. 什么 schema fingerprint 足以保证 bound-plan column access 安全？
4. selection vector 是否应跨多个 operator 保持一等身份，还是 filter 在高选择率时立即 compact？
5. multi-key aggregate state 中 string keys 应用 direct string views、dictionary ids，还是 packed tagged keys？
6. CSV、line、JSON source pushdown 能否共享一个 typed reducer interface，而不让各自 parser 控制流变得别扭？
7. typed path selection 应如何出现在 explain output 中，同时不改变稳定 top-level explain contract？
8. phase 1 implementation branch 应选择哪个 benchmark case 作为首个验收目标？本轮已选择
   `single-int64-low-domain` 作为首个 proof point。

## 2026-06-13 实现记录

用户已批准从研究进入实现切片。本轮实现没有引入新的 public API，也没有直接创建完整
`ColumnarExecBatch`，而是先在现有 aggregate execution substrate 内证明 typed key/state
方向能带来可测收益。

实现内容：

- dense single-`INT64` group key + single numeric `SUM` 从
  `generic-single-int64-key` runtime shape 切到 `sum-single-int64-key`。
- executor 为该 shape 增加 dense typed sum slots，直接使用 `std::vector<double>` 保存
  aggregate state，避免为每个 dense slot 构造 generic `AggregateAccumulator`。
- dense / hash single-`INT64` group key + `COUNT(*)` 增加 `count-single-int64-key`
  runtime shape；dense 低基数场景直接使用 typed count slots，非 dense 或 fallback 场景使用
  typed hash count。
- dense / hash single-`INT64` group key + `AVG` 增加 `avg-single-int64-key`
  runtime shape；dense 低基数场景直接使用 typed sum/count state slots。
- SUM / COUNT / AVG 的 single-int64 typed paths 在 optimizer 层显式暴露
  `state-columnar` partial layout。
- two-`INT64` group key + single numeric `SUM` / `COUNT` / `AVG` 在保留
  `hash-packed` grouping decision 的同时，从 `generic-packed-keys-2` 切到 typed
  double-int64 reducers，并显式暴露 `state-columnar` partial layout。
- unsupported dense shape 仍回退到现有 hash/generic 路径。
- `planner_v03_test` 增加失败优先测试，覆盖 optimizer selection、结果语义和 columnar cache
  validation。

性能证据：

```bash
bazel run //:batch_aggregate_benchmark -- 1048576 5
```

同一台机器、同一参数、baseline worktree 为 `d02b11c`，当前分支为
`auto/columnar-first-kernel-performance`：

| Version | Runtime shape | Best elapsed runs | Avg best elapsed | Avg rows/s |
|---|---|---:|---:|---:|
| baseline `d02b11c` | `generic-single-int64-key` | `51 ms`, `53 ms` | `52.0 ms` | `20.17M` |
| current branch | `sum-single-int64-key` | `39 ms`, `38 ms` | `38.5 ms` | `27.24M` |

COUNT 场景使用同一 benchmark 中新增的 `single-int64-low-domain-count`。baseline worktree
只添加 benchmark 场景，不包含 COUNT typed 实现：

| Version | Runtime shape | Best elapsed runs | Avg best elapsed | Avg rows/s |
|---|---|---:|---:|---:|
| baseline `d02b11c` + benchmark scenario | `generic-single-int64-key` | `41 ms`, `41 ms` | `41.0 ms` | `25.58M` |
| current branch | `count-single-int64-key` | `28 ms`, `28 ms` | `28.0 ms` | `37.45M` |

AVG 场景使用同一 benchmark 中新增的 `single-int64-low-domain-avg`。baseline worktree
只添加 benchmark 场景，不包含 AVG typed 实现：

| Version | Runtime shape | Partial layout | Best elapsed runs | Avg best elapsed | Avg rows/s |
|---|---|---|---:|---:|---:|
| baseline `d02b11c` + benchmark scenario | `generic-single-int64-key` | `generic-table` | `55 ms`, `52 ms` | `53.5 ms` | `19.61M` |
| current branch | `avg-single-int64-key` | `state-columnar` | `38 ms`, `38 ms` | `38.0 ms` | `27.59M` |

Two-key 场景使用同一 benchmark 里的 `double-int64` / `double-int64-count` /
`double-int64-avg`。这些切片保留两列 `INT64` key 的 `hash-packed` grouping decision，
但把 reducer state 从 generic accumulator 切到 typed double-int64 reducers：

| Version | Runtime shape | Partial layout | Best elapsed runs | Avg best elapsed | Avg rows/s |
|---|---|---|---:|---:|---:|
| pre-change current branch | `generic-packed-keys-2` | `key-columnar` | `163 ms` | `163.0 ms` | `6.43M` |
| current branch | `sum-double-int64-key` | `state-columnar` | `122 ms`, `121 ms` | `121.5 ms` | `8.63M` |

| Scenario | Baseline shape/layout | Baseline best elapsed | Current shape/layout | Current best elapsed runs | Avg best elapsed | Avg rows/s | Speedup |
|---|---|---:|---|---:|---:|---:|---:|
| `double-int64-count` | `generic-packed-keys-2` / `key-columnar` | `159 ms` | `count-double-int64-key` / `state-columnar` | `107 ms`, `109 ms` | `108.0 ms` | `9.71M` | `1.47x` |
| `double-int64-avg` | `generic-packed-keys-2` / `key-columnar` | `166 ms` | `avg-double-int64-key` / `state-columnar` | `122 ms`, `123 ms` | `122.5 ms` | `8.56M` | `1.36x` |

结论：

- 目标 scenario 的平均 best elapsed 从 `52.0 ms` 降到 `38.5 ms`，约 `1.35x` speedup。
- 这是局部、可归因的收益：compact int64 key + numeric SUM 的 dense state 从 generic accumulator
  迁到 typed sum slots。
- COUNT 场景的平均 best elapsed 从 `41.0 ms` 降到 `28.0 ms`，约 `1.46x` speedup。
- AVG 场景的平均 best elapsed 从 `53.5 ms` 降到 `38.0 ms`，约 `1.41x` speedup。
- two-INT64 SUM 场景从 `163.0 ms` 降到 `121.5 ms`，约 `1.34x` speedup；
  这说明 state-columnar reducer 不只适用于 single-key dense path，也能改善 fixed-width
  multi-key packed grouping 下的 reducer state 开销。
- two-INT64 COUNT 场景从 `159.0 ms` 降到 `108.0 ms`，约 `1.47x` speedup。
- two-INT64 AVG 场景从 `166.0 ms` 降到 `122.5 ms`，约 `1.36x` speedup。
- mixed string/INT64 SUM 的 state-only 试验被拒绝：把 reducer state 从
  `AggregateAccumulator` 换成 typed sum vector、并尝试给 hash-packed reserve 加上限后，
  `mixed-string-int64` 从 `304 ms` 变为 `315 ms` / `298 ms`，nullable variant 从
  `296 ms` 变为 `329 ms` / `284 ms`；加入 capped reserve 的后续尝试进一步退化到
  `355 ms` / `348 ms`。这说明 mixed-key 的当前瓶颈不主要在 reducer state，而在
  string key hashing / packed key lookup / bucket policy 一侧。
- source predicate aggregate 的 typed shape 已开始接入：single-key `COUNT` 和 numeric
  `SUM` / `AVG` 在存在 `predicate_expr` 时也由 optimizer 显式分类为 typed source
  pushdown shape；CSV 侧 single-key count/numeric aggregate scan path 已能捕获并评估
  predicate expression，不再因为 `OR` / mixed predicate 回到更重的 generic aggregate state。
  同机 `file_source_benchmark -- 200000 3` 中，`sql_csv_predicate_or_group_count` 从
  `156,629 us` 降到 `144,639 us`，`sql_csv_predicate_mixed_group_count` 从
  `267,744 us` 降到 `245,180 us`；covered pushdown ratio 继续低于 `0.40`。
- 这还不是完整 columnar-first kernel。下一阶段仍应把 state-columnar layout、typed reducer
  interface、selection/dictionary view 和 explain diagnostics 做成可组合机制。

## 2026-06-14 实现记录

本轮把 Phase 1 的 internal execution substrate skeleton 落到代码中，但仍保持 public
`DataflowSession` / `DataFrame` / `Table` contract 不变，也没有迁移广泛 operator。

实现内容：

- 新增 `src/dataflow/core/execution/columnar_exec.h` 与
  `src/dataflow/core/execution/columnar_exec.cc`。
- `ColumnarExecBatch` 显式携带 schema、row count、provenance、source-cache owner 和
  execution columns。
- `ColumnarExecColumn` 显式记录 type、encoding、row count、nullability 和 value access view。
- 预留并实现基础 encoding 枚举：`flat`、`constant`、`dictionary-view`、
  `arrow-backed`、`value-fallback`。
- `ColumnarExecView` 支持 projection 与 selection vector；空 selection 作为合法过滤结果被
  `has_selection` 明确区分，不再和“未过滤”混淆。
- 新增 `Table` / retained `ColumnarTable` 到 `ColumnarExecBatch` 的 adapter，以及
  `ColumnarExecBatch` / `ColumnarExecView` 到 `Table` 的显式 materialization boundary。
- Arrow-backed lazy table 进入 `ColumnarExecBatch` 时保留 `arrow-backed` encoding；只有
  `valueAt(...)` 或 row materialization 边界才按需读取值。
- 新增 `scripts/run_columnar_kernel_benchmark_gate.sh`，把 batch aggregate typed-shape
  selection、file-source pushdown ratio 和 string builtin plan-reuse guardrail 收敛成一个
  可重复本地 gate。

验证状态记录在 `.delivery/runs/columnar-first-kernel-performance/verification.md`。当前这一步的
关键语义验收是：

- `ColumnarExecBatch` 能从 value-backed `Table` 建立 flat execution columns。
- `ColumnarExecView` 能表达 projection、非空 selection 和空 selection。
- `ColumnarExecBatch::validate(...)` 能拒绝 row-count mismatch。
- Arrow-backed lazy input 在 execution batch 内保持 `arrow-backed` encoding。

被拒绝路径：

- 第二次 mixed string/`INT64` dictionary-id reducer 尝试被 benchmark 否决。该尝试把
  mixed key path 切到 dictionary-id style reducer，但 `batch_aggregate_benchmark -- 1048576 5`
  中 `mixed-string-int64` 退化到 `634 ms`，nullable variant 为 `301 ms`，未能证明通用收益。
- 该路径已经从生产 diff 和测试期望中移除；当前结论保持为：mixed string/int key 的收益点不应
  只替换 reducer state，而要先设计稳定的 dictionary/key-id view、null identity 与 hash bucket
  policy。

## 当前建议

继续在现有 public `Table` contract 后推进 typed columnar execution substrate。当前 proof points
已经验证 dense typed aggregate state 对 SUM / COUNT / AVG 有收益，Phase 1 substrate skeleton
也已经具备 adapter、view、Arrow-backed owner 和显式 materialization boundary；但仍不应把方向退化为
operator-specific shortcut。

下一条实现切片建议在两个方向中选一个：

- 继续 multi-key reducer generalization：先做 string dictionary/key encoding 或 key-id
  view，再评估 mixed string/int key 的 typed state；不要再只替换 reducer state。
- 继续推进 source pushdown typed reducer interface：当前 single-key predicate aggregate
  已接入 typed shape，下一步应把 CSV、line、JSON 的多聚合/多 key reducer 收敛到共享
  state-columnar reducer，而不是让三个 scanner 各自维护一份状态机。

无论选择哪条，都应保持所有 public APIs 与 row boundaries 稳定；`ColumnarExecBatch` /
`ColumnarExecView` 应继续作为内部 substrate 逐步接入 operator，而不是一次性重写 executor。
