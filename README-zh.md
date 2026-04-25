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

- `python` 里的 native binding
- Arrow 输入与输出
- CLI、打包与 `uv` 工作流
- 本地 app-side service 与 Electron 原型支持
- 离线 embedding 生成 helper 与版本化 embedding 资产管理
- 离线 keyword index 生成 helper 与可复用 BM25 关键词检索资产管理
- Excel / Bitable / custom stream adapter
- 本地 workspace 与 run tracking
- 通过 Claude Agent SDK 或 Codex App Server runtime 实现 AI 辅助数据分析

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
- batch 文件输入已经支持显式与自动探测两条入口：
  - `session.read_csv(...)`、`session.read_line_file(...)`、`session.read_json(...)`
  - `session.probe(...)` 与 `session.read(...)` 用于通用 file input
- batch SQL 文件注册已支持：
  - `CREATE TABLE ... USING csv|line|json OPTIONS(...)`
  - `CREATE TABLE ... OPTIONS(path: '...')` 通过 source probing 自动建表
- batch SQL v1：
  - `CREATE TABLE`、`CREATE SOURCE TABLE`、`CREATE SINK TABLE`
  - `INSERT INTO ... VALUES`
  - `INSERT INTO ... SELECT`
  - 支持列投影/别名、`WHERE`（含 `AND` / `OR`）、`GROUP BY`、`ORDER BY`、`LIMIT`、当前最小 `JOIN`、`UNION` / `UNION ALL` 的 `SELECT`
  - 当前已支持 batch `KEYWORD SEARCH(...) QUERY '...' TOP_K ...`，以及关键词预筛选辅助的 `HYBRID SEARCH ...`
- 当前 batch builtins：
  - string：`LOWER`、`UPPER`、`TRIM`、`LTRIM`、`RTRIM`、`LENGTH`、`LEN`、`CHAR_LENGTH`、`CHARACTER_LENGTH`、`REVERSE`、`CONCAT`、`CONCAT_WS`、`LEFT`、`RIGHT`、`SUBSTR` / `SUBSTRING`、`POSITION`、`REPLACE`
  - numeric/date：`ABS`、`CEIL`、`FLOOR`、`ROUND`、`YEAR`、`MONTH`、`DAY`
- stream 路径：
  - `readStream(...)`、`readStreamCsvDir(...)`
  - `single-process` 和 `local-workers`
  - query-local backpressure、progress snapshot、checkpoint path
  - 基础 stream operators 与 stateful grouped aggregates
  - fixed tumbling 与 fixed sliding window assignment
- stream SQL 子集：
  - `streamSql(...)` 接收 `SELECT`
  - `explainStreamSql(...)` 接收 `SELECT` 或 `INSERT INTO <sink> SELECT ...`
  - `startStreamSql(...)` 接收 `INSERT INTO <sink> SELECT ...`
  - stream source 必须是 source table，stream target 必须是 sink table
  - 支持 `WINDOW BY ... EVERY ... [SLIDE ...] AS ...` 的固定 tumbling/sliding 窗口
  - unbounded-source `ORDER BY` 会被显式拒绝
- fixed-dimension `float32` 的本地 exact vector search
- Python Arrow 输入/输出与 workspace-backed run tracking
- Python 侧支持 realtime queue-backed stream source/sink，可把 Arrow batch 直接写入长运行本地流查询
- 本地 agentic event service，支持 `external_event` Source 接入、Monitor 生命周期、search / grounding 与 `FocusEvent` 轮询
- 面向 Arrow / Parquet 数据集的可复用 keyword index 资产，以及桌面导入流里的异步索引构建
- `app/` 下的本地桌面原型，由 `velaria-service` 提供本地服务
- 桌面端导入流可以在保存同一份数据集后，异步构建可复用的 embedding 数据集与 keyword index
- macOS 桌面原型打包，当前可产出 `.dmg`
- 可通过正式支持的 Python 生态层接入 AI / agent / skill，并利用 workspace 与 artifact 管理能力复用结果、管理本地数据
- 支持通过可配置 LLM runtime 将自然语言转换为 SQL
- CLI `ai` 子命令支持 SQL 生成、session 管理与 agent 分析
- 桌面 app Analyze 页面内置 AI SQL 助手与 session 管理
- 同机 actor/rpc/jobmaster smoke 路径

当前约束：

- `CREATE SOURCE TABLE` 是只读表，拒绝 `INSERT`
- `CREATE SINK TABLE` 可写但不能作为查询输入
- SQL v1 不扩展到 `CTE`、子查询、更复杂 join 语义，也不支持 aggregate `KEYWORD SEARCH` / `HYBRID SEARCH`
- Python callback / Python UDF 不进入热路径
- Electron 桌面 app 仍然只是本地原型，还不是稳定公开产品面
- AI runtime 需要安装 `claude-agent-sdk` 或 `codex-app-server-sdk`（可选依赖）
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
- 本地 agentic service / 协议：[docs/agentic-service-api.md](./docs/agentic-service-api.md)
- streaming runtime 形态：[docs/streaming_runtime_design.md](./docs/streaming_runtime_design.md)
- Python 生态细节：[python/README.md](./python/README.md)
- 当前维护中的主 plan：[plans/core-runtime-columnar-plan.md](./plans/core-runtime-columnar-plan.md)
- `plans/` 目录索引：[plans/README-zh.md](./plans/README-zh.md)

## 4. Usage 示例

自动探测文件源的 batch SQL：

```sql
CREATE TABLE rpc_input OPTIONS(path: '/tmp/input.jsonl');
SELECT * FROM rpc_input LIMIT 5;
```

显式 CSV 源的 batch SQL：

```sql
CREATE TABLE csv_input USING csv OPTIONS(path: '/tmp/input.csv', delimiter: ',');
SELECT * FROM csv_input LIMIT 5;
```

Python batch 文件输入：

```python
import velaria

session = velaria.Session()
probe = session.probe("/tmp/input.jsonl")
df = session.read("/tmp/input.jsonl")
```

显式 CSV reader：

```python
csv_df = session.read_csv("/tmp/input.csv")
```

Regex line 输入：

```python
regex_df = session.read_line_file(
    "/tmp/events.log",
    mappings=[("uid", 1), ("action", 2), ("latency", 3), ("ok", 4), ("note", 5)],
    mode="regex",
    regex_pattern=r'^uid=(\d+) action="([^"]+)" latency=(\d+) ok=(true|false) note=(.+)$',
)
```

CLI 示例：

```bash
uv run --project python python python/velaria_cli.py file-sql \
  --csv /tmp/input.csv \
  --input-type csv \
  --query "SELECT * FROM input_table LIMIT 5"

uv run --project python python python/velaria_cli.py file-sql \
  --input-path /tmp/input.jsonl \
  --input-type auto \
  --query "SELECT * FROM input_table LIMIT 5"

uv run --project python python python/velaria_cli.py file-sql \
  --input-path /tmp/events.log \
  --input-type line \
  --line-mode regex \
  --regex-pattern '^uid=(\\d+) action=\"([^\"]+)\" latency=(\\d+) ok=(true|false) note=(.+)$' \
  --mappings 'uid:1,action:2,latency:3,ok:4,note:5' \
  --query "SELECT * FROM input_table LIMIT 5"
```

真实入口：

```bash
bazel run //:sql_demo
bazel run //:df_demo
bazel run //:stream_demo
bazel run //:file_source_benchmark -- 200000 3
# 会输出 CSV / line / JSON file-source 子 case 的 JSON 行
uv run --project python python python/velaria_cli.py --help
./dist/velaria-cli --help
```

桌面 app 原型入口：

```bash
cd app
npm install
npm start

bash app/scripts/build-sidecar-macos.sh
bash app/scripts/package-macos.sh
```

macOS 未签名内测包说明：

- 当前 `.dmg` 可以在没有 Apple Developer 账号的情况下分发，但它不是 notarized 正式安装包
- Gatekeeper 可能会提示“已损坏”或阻止首次打开
- 如果你信任这个构建版本，先在 Finder 里对安装后的 app 执行“右键 -> 打开”
- 如果系统仍然拦截，可以手动移除隔离属性：

```bash
xattr -dr com.apple.quarantine /Applications/Velaria.app
```

当前 file-source pushdown 已经把执行形态从 executor lowering 传递到 source 执行侧。
目前的 `shape` 包括：

- `ConjunctiveFilterOnly`
- `SingleKeyCount`
- `SingleKeyNumericAggregate`
- `Generic`

这样 source 端可以针对简单 conjunctive filter 和单 key aggregate 选择更轻的 fast path，而不是所有情况都走同一套通用执行路径。

## 5. 开发文档

- 英文：[docs/development.md](./docs/development.md)
- 中文：[docs/development-zh.md](./docs/development-zh.md)

## 6. 打包说明

当前仓库可见的 release 打包包括：

- Linux native wheel：
  - `manylinux x86_64`
  - `manylinux aarch64`
- macOS native wheel：
  - `universal2`
- macOS 桌面原型：
  - `.dmg`

Linux release 会保持“每个 OS/arch 一个 wheel”，而不是按 SIMD 指令集继续拆更多 wheel；同一 wheel 内部通过 runtime SIMD dispatch 选择后端。
