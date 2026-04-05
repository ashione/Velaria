# Benchmark

本文档集中记录当前 `simd` 分支可复现的 benchmark 入口与最近一轮性能快照。

## 复现命令

向量 benchmark：

```bash
./scripts/run_vector_search_benchmark.sh
```

Batch benchmark：

```bash
bazel build //:tpch_q1_style_benchmark
./bazel-bin/tpch_q1_style_benchmark 500 4096 4 4 0 0 single string-keys q18
./bazel-bin/tpch_q1_style_benchmark 500 4096 4 4 0 0 single numeric-keys q1
./bazel-bin/tpch_q1_style_benchmark 500 4096 4 4 0 0 all string-keys q6
```

## 口径说明

- 快照时间：2026 年 4 月 6 日
- 当前分支：`simd`
- batch 行数：`500 * 4096 = 2,048,000`
- `main` 基线是在独立 `main` worktree 上用同一套 benchmark harness 测得
- benchmark 统一串行执行，避免多进程同时抢占 CPU

## TPCH-Like Batch Suite

当前 `simd` 快照：

| Benchmark | 行数 | 模式 | 耗时 | Rows/s | 说明 |
|---|---:|---|---:|---:|---|
| `q1`（`string-keys`） | 2,048,000 | `single-process` | `2858 ms` | `716,585` | `result_rows=6` |
| `q1`（`string-keys`） | 2,048,000 | `actor-credit` | `2748 ms` | `745,269` | `coord_serialize_ms=2011`，`input_payload_bytes=8,298,000` |
| `q1`（`string-keys`） | 2,048,000 | `auto-selected` | `1204 ms` | `1,701,000` | 实际选择 `single-process` |
| `q1`（`numeric-keys`） | 2,048,000 | `single-process` | `4727 ms` | `433,256` | `result_rows=32,896` |
| `q1`（`numeric-keys`） | 2,048,000 | `actor-credit` | `4696 ms` | `436,116` | `coord_serialize_ms=59`，`coord_merge_ms=14` |
| `q1`（`numeric-keys`） | 2,048,000 | `auto-selected` | `3369 ms` | `607,896` | 实际选择 `single-process` |
| `q6-like-scan-filter-sum` | 2,048,000 | `batch` | `1282 ms` | `1,597,500` | `result_rows=1` |
| `q3-like-join-group-order` | 2,304,000 | `batch` | `5346 ms` | `430,976` | `result_rows=8` |
| `q18-like-high-card-group-order` | 2,048,000 | `batch` | `1045 ms` | `1,959,810` | `result_rows=100` |

已测 `main` 对比快照：

| Benchmark | 行数 | `simd` | `main` 基线 | 提升 |
|---|---:|---:|---:|---:|
| `q18-like-high-card-group-order` | 2,048,000 | `1045 ms` | `2421 ms` | `2.32x` 更快 |
| `q1` 单进程（`numeric-keys`） | 2,048,000 | `5046 ms` | `7936 ms` | `1.57x` 更快 |
| `q6-like-scan-filter-sum` | 2,048,000 | `1282 ms` | `5168 ms` | `4.03x` 更快 |

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
