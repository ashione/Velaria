# Velaria：纯 C++20 本地数据流内核

`README-zh.md` 是中文镜像文档，对应英文主文档位于 [README.md](./README.md)。两份文档必须保持同步。

Velaria 是一个以极致性能为目标、batch/stream 一体化、列式原生、内建向量能力的 C++20 数据流内核。

## 1. 这个项目是什么

Velaria 是一个本地优先的 C++20 数据流引擎研究项目。

它当前的目标保持收敛：

- 保持一个 native kernel 作为执行真相来源
- 把极致性能作为 native 路径的一等目标
- 让 batch 和 stream 共用同一套 kernel
- 让列式表示尽可能深地贯穿运行时
- 把向量能力放在 core kernel 内，而不是外挂子系统
- 稳住单机链路
- 通过正式支持的 Python 生态层向外暴露能力
- 把同机 actor/rpc 路径放在实验通道，而不是第二套内核

这个仓库当前不宣称自己已经是完整 distributed runtime。
当前工作的重点仍然是先稳住本地内核，再谨慎地从 in-proc operator chaining 扩展到同机多进程协作。

## 2. 特点与架构设计

Velaria 围绕一个 kernel 和两个非 kernel 层组织。

### Core Kernel

负责：

- 本地 batch 与 streaming 执行
- column-first runtime 方向
- logical planning 与最小 SQL 映射
- source/sink ABI
- explain / progress / checkpoint contract
- 本地 vector search

### Python Ecosystem

负责：

- `python_api` 里的 native binding
- Arrow 输入与输出
- CLI、打包与 `uv` 工作流
- Excel / Bitable / custom stream adapter
- 本地 workspace 与 run tracking

不负责：

- 执行热路径语义
- 独立的 explain / progress / checkpoint contract
- 第二套执行引擎

### Experimental Runtime

负责：

- 同机 `actor/rpc/jobmaster` 实验
- transport / codec / scheduler 观测

不代表：

- 已完成 distributed scheduling
- 已支持 production distributed execution

### 核心设计特点

- 只有一个 native kernel 作为执行真相来源
- 极致性能是一等设计目标，而不是后补优化
- batch 和 stream 共用同一套 kernel，而不是分裂成两套执行引擎
- runtime 方向明确是 column-first
- 列式 ownership 应尽可能深地保留到执行内部
- vector search 是内建的 kernel 能力
- `DataflowSession` 是统一的公开 session 入口
- rows 是兼容边界，不是热路径主形态
- SQL 是 ingress surface，不反向主导 runtime 架构

黄金路径是：

```text
Arrow / CSV / Python ingress
  -> DataflowSession / DataFrame / StreamingDataFrame
  -> local runtime kernel
  -> sink
  -> explain / progress / checkpoint
```

## 3. 现在的现状和详细介绍

当前已具备：

- 一个 native kernel 同时支持 batch + streaming
- native 路径明确以性能优先为导向
- 明确朝 column-first 执行推进，并在兼容边界上采用 lazy row materialization
- batch SQL v1：
  - `CREATE TABLE`、`CREATE SOURCE TABLE`、`CREATE SINK TABLE`
  - `INSERT INTO ... VALUES`
  - `INSERT INTO ... SELECT`
  - 支持列投影/别名、`WHERE`、`GROUP BY`、`ORDER BY`、`LIMIT`、当前最小 `JOIN` 的 `SELECT`
- 当前 batch builtins：
  - string：`LOWER`、`UPPER`、`TRIM`、`LTRIM`、`RTRIM`、`LENGTH`、`LEN`、`CHAR_LENGTH`、`CHARACTER_LENGTH`、`REVERSE`、`CONCAT`、`CONCAT_WS`、`LEFT`、`RIGHT`、`SUBSTR` / `SUBSTRING`、`POSITION`、`REPLACE`
  - numeric/date：`ABS`、`CEIL`、`FLOOR`、`ROUND`、`YEAR`、`MONTH`、`DAY`
- stream 路径：
  - `readStream(...)`、`readStreamCsvDir(...)`
  - `single-process` 和 `local-workers`
  - query-local backpressure、progress snapshot、checkpoint path
  - 基础 stream operators 与 stateful grouped aggregates
- stream SQL 子集：
  - `streamSql(...)` 接收 `SELECT`
  - `explainStreamSql(...)` 接收 `SELECT` 或 `INSERT INTO <sink> SELECT ...`
  - `startStreamSql(...)` 接收 `INSERT INTO <sink> SELECT ...`
  - stream source 必须是 source table，stream target 必须是 sink table
  - unbounded-source `ORDER BY` 会被显式拒绝
- fixed-dimension `float32` 的本地 exact vector search
- Python Arrow 输入/输出与 workspace-backed run tracking
- 可通过正式支持的 Python 生态层接入 AI / agent / skill，并利用 workspace 与 artifact 管理能力复用结果、管理本地数据
- 同机 actor/rpc/jobmaster smoke 路径

当前约束：

- `CREATE SOURCE TABLE` 是只读表，拒绝 `INSERT`
- `CREATE SINK TABLE` 可写但不能作为查询输入
- SQL v1 不扩展到 `CTE`、子查询、`UNION` 或更复杂 join 语义
- Python callback / Python UDF 不进入热路径
- 仓库不宣称已完成 distributed runtime

稳定公开 surface：

- `DataflowSession`
- `DataFrame`
- `StreamingDataFrame`
- `StreamingQuery`
- `StreamingQueryProgress`
- `snapshotJson()`
- 返回 `logical / physical / strategy` 的 `explainStreamSql(...)`

更详细的说明见：

- 边界与职责：[docs/core-boundary.md](./docs/core-boundary.md)
- runtime contract：[docs/runtime-contract.md](./docs/runtime-contract.md)
- streaming runtime 形态：[docs/streaming_runtime_design.md](./docs/streaming_runtime_design.md)
- Python 生态细节：[python_api/README.md](./python_api/README.md)
- 当前维护中的主 plan：[plans/core-runtime-columnar-plan.md](./plans/core-runtime-columnar-plan.md)
- `plans/` 目录索引：[plans/README-zh.md](./plans/README-zh.md)

## 4. 如果要加入开发

仓库基线：

- 语言基线：`C++20`
- 构建系统：`Bazel`
- 对外 session 入口保持为 `DataflowSession`
- 不要破坏 `sql_demo / df_demo / stream_demo`
- 示例源码统一使用 `.cc`
- 本仓库中的 Python 命令统一使用 `uv`

真实入口：

```bash
bazel run //:sql_demo
bazel run //:df_demo
bazel run //:stream_demo
bazel run //:actor_rpc_smoke
uv run --project python_api python python_api/velaria_cli.py --help
./dist/velaria-cli --help
```

最小验证：

```bash
bazel build //:sql_demo //:df_demo //:stream_demo \
  //:actor_rpc_scheduler //:actor_rpc_worker //:actor_rpc_client //:actor_rpc_smoke

bazel run //:sql_demo
bazel run //:df_demo
bazel run //:stream_demo
bazel run //:actor_rpc_smoke

./scripts/run_python_ecosystem_regression.sh
```

开发文档：

- 英文：[docs/development.md](./docs/development.md)
- 中文：[docs/development-zh.md](./docs/development-zh.md)
