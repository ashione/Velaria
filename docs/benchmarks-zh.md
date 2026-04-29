# Benchmark

本文档集中记录当前开发分支可复现的 benchmark 入口与最近一轮性能快照。

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
bazel run //:batch_aggregate_benchmark
bazel build //:tpch_q1_style_benchmark
./bazel-bin/tpch_q1_style_benchmark 500 4096 4 4 0 0 single string-keys q18
./bazel-bin/tpch_q1_style_benchmark 500 4096 4 4 0 0 single numeric-keys q1
./bazel-bin/tpch_q1_style_benchmark 500 4096 4 4 0 0 all string-keys q6
```

String builtin benchmark：

```bash
bazel run //:string_builtin_benchmark
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

当前 file-source 的优化器/执行器分层：

- executor lowering 会把 source pushdown 分类成 `ConjunctiveFilterOnly`、`SingleKeyCount`、`SingleKeyNumericAggregate`、`Generic`
- source 端会根据这些 `shape` 选择更轻的 fast path
- 当前收益最明显的是 line split、line regex、JSON 按命中字段解析，以及简单 CSV 单 key aggregate

## 最新本地回归快照

- 快照时间：2026 年 4 月 29 日
- 当前分支：`auto/sql-optimization-a`
- 运行方式：开发机本地串行 Bazel benchmark
- 统计口径：默认外层串行复测 `3` 次后取平均值；表格如注明其他复测次数则以表格为准；各 benchmark 保持自身内部 `rounds`
- 覆盖范围：仅 native execution benchmark；不包含 Python API 开销、打包 CLI 启动时间或 agent runtime 启动时间
- 这组快照包含 columnar 热路径调优：重复执行循环不再做全量 cache 校验，packed aggregate key 也避免了逐行字符串拷贝。
- Batch aggregate 表格是在移除 benchmark 专用 sorted-key 探测后重新串行复测 `5` 次得到，只使用优化器选择的 dense、packed-hash、fixed-hash 和 sort-streaming 执行路径。
- 重复 legacy SQL 文本会在 `kBeforeSqlParse` 后复用有界 parse cache；after-parse 和 plan-build plugin hook 仍会逐次执行。

String builtin 快照，`100,000` 行，内部 `5` 轮：

| Case | 平均耗时 | Rows/s |
|---|---:|---:|
| `copy-column` | `99,416 us` | `1,005,872` |
| `single-arg-functions` | `226,192 us` | `442,103` |
| `multi-arg-functions` | `329,301 us` | `303,674` |
| `dependent-chain` | `489,359 us` | `204,349` |
| `sql-plan-and-execute` | `549,990 us` | `181,821` |
| `sql-reused-plan` | `171,188 us` | `584,153` |

Batch aggregate 快照，`1,048,576` 行，外层 `5` 次复测：

| Scenario | Selected impl | 耗时 | Rows/s | 输出 group |
|---|---|---:|---:|---:|
| `single-int64-low-domain` | `dense` | `57.4 ms` | `18,267,875` | `1,024` |
| `single-int64-high-domain` | `hash-fixed` | `561.6 ms` | `1,867,123` | `1,048,576` |
| `double-int64` | `hash-packed` | `201.4 ms` | `5,206,435` | `4,096` |
| `mixed-string-int64` | `hash-packed` | `335.0 ms` | `3,130,078` | `16,384` |
| `mixed-string-int64-nullable` | `hash-packed` | `324.0 ms` | `3,236,346` | `17,921` |
| `int64-two-string` | `hash-packed` | `500.4 ms` | `2,095,476` | `32,768` |
| `int64-string-bool` | `hash-packed` | `427.4 ms` | `2,453,383` | `32,768` |
| `ordered-string` | `sort-streaming` | `311.4 ms` | `3,367,296` | `32,768` |

Stream runtime 快照：

| Case | 耗时 | Rows/s | last batch latency |
|---|---:|---:|---:|
| `stateless-single` | `1.5 s` | `170,694` | `45.0 ms` |
| `stateless-local-workers` | `1.9 s` | `138,088` | `55.3 ms` |
| `stateful-single` | `2.9 s` | `91,697` | `91.7 ms` |
| `stateful-local-workers` | `5.4 s` | `48,460` | `167.7 ms` |
| `stateful-single-sliding` | `5.5 s` | `47,496` | `170.3 ms` |
| `stateful-local-workers-sliding` | `9.9 s` | `26,394` | `317.0 ms` |

Stream actor runtime 快照：

| Case | 耗时 | Rows/s | 输出行数 |
|---|---:|---:|---:|
| `single-process` | `157.7 ms` | `835,750` | `1,280` |
| `actor-credit` | `247.3 ms` | `530,352` | `1,280` |
| `auto-selected` | `110.7 ms` | `1,186,292` | `1,280` |

File-source benchmark 快照，`200,000` 行，内部 `3` 轮：

| Case | 最佳耗时 | 输出行数 |
|---|---:|---:|
| `probe_csv` | `95 us` | `3` |
| `probe_line` | `96 us` | `3` |
| `probe_json_lines` | `97 us` | `3` |
| `read_csv_hardcode_group_sum` | `57,103 us` | `16` |
| `read_csv_explicit_group_sum` | `149,302 us` | `16` |
| `read_csv_auto_group_sum` | `151,156 us` | `16` |
| `read_csv_scan_only` | `63,231 us` | `200,000` |
| `read_csv_full_columnar_only` | `184,786 us` | `200,000` |
| `read_csv_full_materialize_rows` | `336,807 us` | `200,000` |
| `read_csv_projected_group_sum` | `148,139 us` | `16` |
| `read_csv_filter_only` | `260,682 us` | `99,800` |
| `read_csv_aggregate_pushdown` | `179,868 us` | `16` |
| `read_line_hardcode_group_sum` | `54,728 us` | `16` |
| `read_line_explicit_group_sum` | `286,047 us` | `16` |
| `read_line_auto_group_sum` | `285,538 us` | `16` |
| `read_line_filter_only` | `292,154 us` | `99,800` |
| `read_line_aggregate_pushdown` | `293,716 us` | `16` |
| `read_line_regex_parse` | `111 us` | `1` |
| `read_line_regex_hardcode_group_sum` | `244,894 us` | `16` |
| `read_line_regex_explicit_group_sum` | `783,038 us` | `16` |
| `read_json_hardcode_group_sum` | `165,309 us` | `16` |
| `read_json_explicit_group_sum` | `641,127 us` | `16` |
| `read_json_auto_group_sum` | `635,095 us` | `16` |
| `read_json_filter_only` | `635,698 us` | `99,800` |
| `read_json_aggregate_pushdown` | `638,904 us` | `16` |
| `sql_create_table_probe_only_json` | `1,808,254 us` | `1` |
| `sql_create_table_explicit_json` | `1,766,208 us` | `1` |
| `sql_csv_predicate_and_group_count` | `131,533 us` | `1` |
| `sql_csv_predicate_or_group_count` | `180,255 us` | `2` |
| `sql_csv_predicate_mixed_group_count` | `294,823 us` | `2` |
| `sql_line_predicate_or_group_count` | `208,696 us` | `2` |
| `sql_json_predicate_or_group_count` | `505,876 us` | `2` |

File-source SQL predicate pushdown 快照，`200,000` 行，内部 `3` 轮：

| Case | 无 pushdown | pushdown | Ratio |
|---|---:|---:|---:|
| `sql_csv_predicate_and_group_count` | `760,868 us` | `131,533 us` | `0.173` |
| `sql_csv_predicate_or_group_count` | `736,269 us` | `180,255 us` | `0.245` |
| `sql_csv_predicate_mixed_group_count` | `791,505 us` | `294,823 us` | `0.372` |
| `sql_line_predicate_or_group_count` | `1,110,699 us` | `208,696 us` | `0.188` |
| `sql_json_predicate_or_group_count` | `1,641,476 us` | `505,876 us` | `0.308` |

相对 2026 年 4 月 26 日本地 baseline 的对比：

| 领域 | Case | 4 月 26 baseline | 当前 | 变化 |
|---|---|---:|---:|---:|
| batch aggregate | `single-int64-low-domain` | `64.0 ms` | `57.4 ms` | `-10.3%` |
| batch aggregate | `single-int64-high-domain` | `551.0 ms` | `561.6 ms` | `+1.9%` |
| batch aggregate | `double-int64` | `304.0 ms` | `201.4 ms` | `-33.8%` |
| batch aggregate | `mixed-string-int64` | `499.0 ms` | `335.0 ms` | `-32.9%` |
| batch aggregate | `mixed-string-int64-nullable` | `452.0 ms` | `324.0 ms` | `-28.3%` |
| batch aggregate | `int64-two-string` | `690.0 ms` | `500.4 ms` | `-27.5%` |
| batch aggregate | `int64-string-bool` | `584.0 ms` | `427.4 ms` | `-26.8%` |
| batch aggregate | `ordered-string` | `337.0 ms` | `311.4 ms` | `-7.6%` |
| string builtin | `dependent-chain` | `488,326 us` | `489,359 us` | `+0.2%` |
| string builtin | `sql-plan-and-execute` | `520,191 us` | `549,990 us` | `+5.7%` |
| string builtin | `sql-reused-plan` | `170,364 us` | `171,188 us` | `+0.5%` |
| file-source SQL pushdown | `sql_csv_predicate_and_group_count` | `103,135 us` | `131,533 us` | `+27.5%` |
| file-source SQL pushdown | `sql_csv_predicate_or_group_count` | `149,105 us` | `180,255 us` | `+20.9%` |
| file-source SQL pushdown | `sql_csv_predicate_mixed_group_count` | `255,453 us` | `294,823 us` | `+15.4%` |
| file-source SQL pushdown | `sql_line_predicate_or_group_count` | `170,632 us` | `208,696 us` | `+22.3%` |
| file-source SQL pushdown | `sql_json_predicate_or_group_count` | `420,300 us` | `505,876 us` | `+20.4%` |

负数表示比 baseline 更快。剩余退化集中在重复 SQL planning 和 file-source SQL pushdown 的绝对耗时；pushdown 相对 no-pushdown 的 ratio 仍保持预期收益。

这组快照用于本地回归守护，适合发现数量级回退并确认 pushdown 收益形态；它不是跨机器、发布级别的严格 benchmark。

当前已知退化：

- 最新快照中，file-source SQL pushdown 的绝对耗时仍慢于 2026-04-26 本地 baseline
- 重复 SQL planning 已通过有界 legacy parse cache 改善，但 Python-facing benchmark 里仍应继续区分 planning 与 execution/Arrow export
- 下一阶段性能恢复应通过 typed source pushdown 与 reducer specialization 完成，不增加 benchmark-shape shortcut

后续章节保留 2026 年 4 月 6 日 `simd` 分支的历史测量结果，用于对比参考。

## 历史 File-Source 对比

当前代表性 file-source case 相比干净 `main` 的对比快照：

| Case | clean `main` | current | 变化 |
|---|---:|---:|---:|
| `read_line_regex_explicit_group_sum` | `5679936 us` | `641735 us` | `-88.7%` |
| `sql_csv_predicate_and_group_count` | `133011 us` | `109146 us` | `-17.9%` |
| `sql_csv_predicate_or_group_count` | `307313 us` | `171556 us` | `-44.2%` |
| `sql_csv_predicate_mixed_group_count` | `462000 us` | `275583 us` | `-40.3%` |
| `sql_line_predicate_or_group_count` | `314852 us` | `174627 us` | `-44.5%` |
| `sql_json_predicate_or_group_count` | `604404 us` | `420423 us` | `-30.4%` |

## 历史口径说明

- 快照时间：2026 年 4 月 6 日
- 当前分支：`simd`
- batch 行数：`500 * 4096 = 2,048,000`
- `main` 基线是在独立 `main` worktree 上用同一套 benchmark harness 测得
- benchmark 统一串行执行，避免多进程同时抢占 CPU
- 本轮 follow-up 数据覆盖了 actor execution optimizer 拆分、C++20 热路径更新（`std::span` / `<bit>`）、枚举化 filter compare，以及 numeric window-key-sum fast path
- 对重复测量的 batch case，表中使用 3 次串行复测的中位数

解释边界：

- `file_source_benchmark` 是原生 C++ benchmark，不包含 Python API 层开销，也不包含打包 CLI 的启动时间。
- `python/benchmarks/bench_stage_paths.py` 是另一套 Python API benchmark。在那套 harness 里，`Session.sql(...)` 只统计建计划，第一次 `to_arrow()` 才会把执行和 Arrow 导出一起触发。
- Python stage benchmark 里的 `hardcode` 是针对特定场景的 Python 标准库基线，使用 `csv.DictReader` 加手写聚合/过滤逻辑；它适合做该 harness 内部对比，但不是原生内核的性能上限。
- 打包后的 `./dist/velaria-cli` 启动时间需要单独测量。当前单文件 CLI 由 PyInstaller `--onefile` 打包，冷启动会额外包含 Python/bootstrap 开销，而不只是内核执行时间。

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
