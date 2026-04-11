# Benchmark

本文档集中记录当前 `simd` 分支可复现的 benchmark 入口与最近一轮性能快照。

## 复现命令

向量 benchmark：

```bash
./scripts/run_vector_search_benchmark.sh
```

这个 wrapper 现在会同时校验：

- `vector-query` exact-scan 基线
- `hybrid-search` 过滤候选集后的联合检索基线，覆盖 `none` / `medium` / `high` 三档 selectivity

Batch benchmark：

```bash
bazel build //:tpch_q1_style_benchmark
./bazel-bin/tpch_q1_style_benchmark 500 4096 4 4 0 0 single string-keys q18
./bazel-bin/tpch_q1_style_benchmark 500 4096 4 4 0 0 single numeric-keys q1
./bazel-bin/tpch_q1_style_benchmark 500 4096 4 4 0 0 all string-keys q6
```

File-input benchmark：

```bash
bazel run //:file_source_benchmark -- 200000 3
perf record --call-graph=dwarf bazel-bin/file_source_benchmark -- 200000 3
perf report
```

这个 file-input benchmark 会输出以下 JSON 行：

- CSV hardcode / explicit / auto-probed 路径
- CSV scan-only / full-columnar / full-row-materialize / projected / filter-pushdown / aggregate-pushdown 子 case
- line split / regex explicit 路径，以及 direct filter-pushdown / aggregate-pushdown 子 case
- JSON lines explicit / auto-probed 路径，以及 direct filter-pushdown / aggregate-pushdown 子 case
- SQL 文件注册，以及 CSV / line / JSON predicate-pushdown 路径

## 口径说明

- 快照时间：2026 年 4 月 6 日
- 当前分支：`simd`
- batch 行数：`500 * 4096 = 2,048,000`
- `main` 基线是在独立 `main` worktree 上用同一套 benchmark harness 测得
- benchmark 统一串行执行，避免多进程同时抢占 CPU
- 本轮 follow-up 数据覆盖了 actor execution optimizer 拆分、C++20 热路径更新（`std::span` / `<bit>`）、枚举化 filter compare，以及 numeric window-key-sum fast path
- 对重复测量的 batch case，表中使用 3 次串行复测的中位数

## TPCH-Like Batch Suite

当前 `simd` 快照：

| Benchmark | 行数 | 模式 | 耗时 | Rows/s | 说明 |
|---|---:|---|---:|---:|---|
| `q1`（`string-keys`） | 2,048,000 | `single-process` | `2907 ms` | `704,506` | 3 次复测中位数，`result_rows=6` |
| `q1`（`string-keys`） | 2,048,000 | `actor-credit` | `2786 ms` | `735,104` | 3 次复测中位数，`coord_serialize_ms≈2.0s` |
| `q1`（`string-keys`） | 2,048,000 | `auto-selected` | `1334 ms` | `1,535,230` | 3 次复测中位数，实际选择 `single-process` |
| `q1`（`numeric-keys`） | 2,048,000 | `single-process` | `3228 ms` | `634,449` | 3 次复测中位数，`result_rows=32,896` |
| `q1`（`numeric-keys`） | 2,048,000 | `actor-credit` | `5405 ms` | `378,908` | 3 次复测中位数，actor transport 仍是主瓶颈 |
| `q1`（`numeric-keys`） | 2,048,000 | `auto-selected` | `1496 ms` | `1,368,980` | 3 次复测中位数，实际选择 `single-process` |
| `q6-like-scan-filter-sum` | 2,048,000 | `batch` | `747 ms` | `2,741,630` | 3 次复测中位数，`result_rows=1` |
| `q3-like-join-group-order` | 2,304,000 | `batch` | `5346 ms` | `430,976` | 沿用上一轮快照，本轮未重跑 |
| `q18-like-high-card-group-order` | 2,048,000 | `batch` | `1050 ms` | `1,950,480` | 3 次复测中位数，`result_rows=100` |

已测 `main` 对比快照：

| Benchmark | 行数 | `simd` | `main` 基线 | 提升 |
|---|---:|---:|---:|---:|
| `q18-like-high-card-group-order` | 2,048,000 | `1050 ms` | `2421 ms` | `2.31x` 更快 |
| `q1` 单进程（`numeric-keys`） | 2,048,000 | `3228 ms` | `7936 ms` | `2.46x` 更快 |
| `q6-like-scan-filter-sum` | 2,048,000 | `747 ms` | `5168 ms` | `6.92x` 更快 |

## String Builtins

当前 `simd` 快照，`100,000` 行，`5` 轮：

| Case | 平均耗时 | Rows/s |
|---|---:|---:|
| `copy-column` | `237,197 us` | `421,591` |
| `single-arg-functions` | `524,354 us` | `190,711` |
| `multi-arg-functions` | `816,910 us` | `122,412` |
| `dependent-chain` | `1,234,700 us` | `80,992` |
| `sql-plan-and-execute` | `1,485,770 us` | `67,305` |
| `sql-reused-plan` | `374,353 us` | `267,127` |

## Vector Exact Scan

当前 `simd` 快照：

| 行数 | 维度 | Metric | warm query 平均耗时 | cold query |
|---|---:|---|---:|---:|
| `10,000` | `128` | `cosine` | `2,938 us` | `57,800 us` |
| `10,000` | `128` | `dot` | `2,728 us` | `55,215 us` |
| `10,000` | `128` | `l2` | `2,722 us` | `54,046 us` |
| `10,000` | `768` | `cosine` | `19,527 us` | `336,414 us` |
| `10,000` | `768` | `dot` | `14,533 us` | `253,381 us` |
| `10,000` | `768` | `l2` | `14,673 us` | `235,871 us` |
| `100,000` | `128` | `cosine` | `28,136 us` | `564,258 us` |
| `100,000` | `128` | `dot` | `26,256 us` | `544,686 us` |
| `100,000` | `128` | `l2` | `27,889 us` | `545,915 us` |
| `100,000` | `768` | `cosine` | `143,918 us` | `2,504,985 us` |
| `100,000` | `768` | `dot` | `141,099 us` | `2,410,975 us` |
| `100,000` | `768` | `l2` | `142,327 us` | `2,400,458 us` |

## Vector + Column 联合检索

当前 benchmark harness 也会输出 `hybrid-search` JSON 行，覆盖：

- 无过滤 / 接近全量候选集
- 中等 selectivity 过滤
- 高 selectivity 过滤

复现入口仍然使用上面的同一个脚本，`--quick` 仍是仓库内轻量校验 preset。

当前 `--quick` 快照（`10,000` 行，`128` 维，`top_k=10`）：

| Metric | 过滤档位 | Selectivity | 候选行数 | warm query 平均耗时 | cold query | warm explain 平均耗时 |
|---|---|---:|---:|---:|---:|---:|
| `cosine` | `none` | `1.00` | `10,000` | `3,346 us` | `38,855 us` | `3,100 us` |
| `cosine` | `medium` | `0.20` | `2,000` | `676 us` | `9,113 us` | `586 us` |
| `cosine` | `high` | `0.01` | `100` | `98 us` | `956 us` | `43 us` |
| `dot` | `none` | `1.00` | `10,000` | `2,979 us` | `37,938 us` | `2,734 us` |
| `dot` | `medium` | `0.20` | `2,000` | `637 us` | `9,224 us` | `568 us` |
| `dot` | `high` | `0.01` | `100` | `116 us` | `1,065 us` | `47 us` |
| `l2` | `none` | `1.00` | `10,000` | `2,932 us` | `38,922 us` | `2,844 us` |
| `l2` | `medium` | `0.20` | `2,000` | `670 us` | `12,136 us` | `610 us` |
| `l2` | `high` | `0.01` | `100` | `189 us` | `1,184 us` | `45 us` |

这组 quick 快照的形态比较明确：

- `none` 基本贴近纯 `vector-query` 基线
- `medium` 会把 warm query 延迟压到全量扫描的大约四分之一
- `high` 已经进入亚 `0.2 ms` 的 warm query 区间
- `explainHybridSearch(...)` 目前为了给出 `returned_rows` 会实际跑 search，所以 explain 开销也会跟候选集规模一起变化

## Vector Transport

当前 `simd` 快照：

| 行数 | 维度 | Codec | 序列化 | 反序列化 | Payload bytes |
|---|---:|---|---:|---:|---:|
| `10,000` | `128` | `proto` | `208,568 us` | `719,269 us` | `14,295,898` |
| `10,000` | `128` | `binary` | `50,719 us` | `30,375 us` | `5,171,773` |
| `10,000` | `128` | `arrow-ipc` | `110,836 us` | `2,978 us` | `5,200,512` |
| `10,000` | `768` | `proto` | `1,145,527 us` | `4,232,640 us` | `84,681,100` |
| `10,000` | `768` | `binary` | `276,843 us` | `131,815 us` | `30,771,773` |
| `10,000` | `768` | `arrow-ipc` | `575,440 us` | `38,952 us` | `30,800,512` |
| `100,000` | `128` | `proto` | `2,103,876 us` | `7,087,232 us` | `143,058,371` |
| `100,000` | `128` | `binary` | `492,848 us` | `304,959 us` | `51,791,773` |
| `100,000` | `128` | `arrow-ipc` | `1,114,975 us` | `38,956 us` | `52,000,512` |
| `100,000` | `768` | `proto` | `11,880,117 us` | `42,873,547 us` | `846,907,112` |
| `100,000` | `768` | `binary` | `2,886,000 us` | `1,320,479 us` | `307,791,773` |
| `100,000` | `768` | `arrow-ipc` | `5,841,326 us` | `244,186 us` | `308,000,512` |

## 这些结果反映的内容

这组结果覆盖了当前 column-first runtime 的几项核心改动：

- execution optimizer lowering
- selection-vector filter
- Arrow 前缀 zero-copy slice
- 通过 optimizer 选择 aggregate fast path
