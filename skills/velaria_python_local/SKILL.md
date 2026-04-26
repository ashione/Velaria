---
name: velaria-python-local
description: How to use a locally installed Velaria Python package for local analysis and stream SQL workflows.
---

# Velaria Local Python Skill

这个 Skill 用于说明如何把 `velaria` 的 Python 包作为本地分析工具使用，不涉及仓库实现代码或编译/构建逻辑。  
核心思路：**把数据先加载为临时视图，再用 `session.sql(...)` 写 SQL 处理。**

在 Velaria Agent 中处理数据任务时，优先调用已注册的 Velaria local functions/MCP tools：

- HTTP(S) 数据集 URL：先用 `velaria_dataset_download` 本地化，或直接把 URL 传给 `velaria_dataset_import` / `velaria_read` / `velaria_sql` / `velaria_dataset_process`。
- 本地文件：用 `velaria_dataset_import` 注册、`velaria_read` / `velaria_schema` 检查、`velaria_sql` 查询、`velaria_dataset_process` 保存 run/artifact。
- 编码、中文列名、`invalid token byte` 或 SQL 标识符问题：先用 `velaria_dataset_normalize` 转成 UTF-8 CSV 和 SQL-safe 字段名，再按返回的 `schema` / `column_mapping` 写 SQL。
- SQL 函数、能力边界和常见模板：按需用 `velaria_sql_capabilities`、`velaria_sql_function_search`、`velaria_sql_query_patterns` 或资源 `velaria://sql/catalog` 检索，不要凭记忆猜函数。
- 不要在 Velaria 工具失败前先写 `curl`、`wget` 或自定义 Python 下载脚本；只有 Velaria 工具无法覆盖时再回退到通用方式。

本 Skill 默认只使用 `uv` 执行。仓库内可直接使用的入口只有两类：

- 源码入口：`uv run --project python python python/velaria_cli.py ...`
- 打包产物：`./dist/velaria-cli ...`

如果你已经把 wheel 安装到了独立环境，也可以使用安装后的 `velaria-cli` / `velaria_cli`，但下面的示例统一使用仓库内可见入口。

## 1. 环境准备

推荐先创建一个本地环境并安装 `velaria`：

```bash
uv venv .venv
source .venv/bin/activate

# 二选一：
uv pip install velaria
# 或
uv pip install /path/to/velaria-<version>-<python_tag>-<abi_tag>-<platform_tag>.whl

uv run --project python python python/velaria_cli.py --help
uv run python -c "import velaria; print(velaria.__version__)"
```

> 若外部 whl 已包含原生扩展，则可直接使用 `Session`；若是纯 Python 子集或兼容层缺失，会在首次创建 `Session` 时报错。  
> 本 Skill 不包含仓库构建步骤，只描述“已安装包后如何使用”。
> 若你只想做一次性验证，也可以用 `uv run --with velaria ...` 临时拉起环境，但不作为本 Skill 的默认路径。

## 2. Skill 脚本

目录：`skills/velaria_python_local/scripts/`

- `smoke.py`：最小本地 smoke。
- `query_csv_to_sql.py`：CSV 直接转 SQL。
- `query_excel_to_sql.py`：Excel 直接转 SQL。
- `query_bitable_to_sql.py`：Bitable 记录转 SQL（需要环境变量 `FEISHU_BITABLE_*`）。
- `read_xlsx.py`：最小 Excel 读取示例（兼容旧示例用途）。

## 3. 核心数据链路（数据 → SQL）

```python
from velaria import Session

session = Session()
df = session.read_csv("data/sales.csv")
session.create_temp_view("sales", df)
result = session.sql("SELECT region, SUM(amount) AS amount_sum FROM sales GROUP BY region").to_arrow()
print(result.to_pylist())
```

如果你更适合用 CLI 而不是临时 Python 片段，在已安装 `velaria` 的环境里也可以直接使用 workspace/run store。

所有 CLI 顶层命令和子命令都支持 `--help`，例如
`uv run --project python python python/velaria_cli.py run diff --help`。
`uv run --project python python python/velaria_cli.py -i` 可以进入交互式模式。

```bash
uv run --project python python python/velaria_cli.py -i

uv run --project python python python/velaria_cli.py run start -- file-sql \
  --run-name "regional_row_count" \
  --description "regional row count for the current CSV snapshot" \
  --tag regional \
  --tag daily-check \
  --csv path/to/file.csv \
  --query "SELECT region, COUNT(*) AS cnt FROM input_table GROUP BY region"

uv run --project python python python/velaria_cli.py run list --tag regional --query "row count" --limit 20
uv run --project python python python/velaria_cli.py run result --run-id <run_id>
uv run --project python python python/velaria_cli.py run diff --run-id <run_id> --other-run-id <other_run_id>
uv run --project python python python/velaria_cli.py run show --run-id <run_id>
uv run --project python python python/velaria_cli.py artifacts list --run-id <run_id>
```

## 3.1 当前 SQL v1 边界

批量 SQL 当前适合直接走 `session.sql(...)` 的形态：

- `CREATE TABLE`、`CREATE SOURCE TABLE`、`CREATE SINK TABLE`
- `INSERT INTO ... VALUES`
- `INSERT INTO ... SELECT`
- `SELECT` 的列投影 / 别名、`WHERE`（含列对列谓词）、`GROUP BY`、`ORDER BY`、`LIMIT`、当前最小 `JOIN`
- 当前内建函数：`LOWER`、`UPPER`、`TRIM`、`LTRIM`、`RTRIM`、`LENGTH`、`LEN`、`CHAR_LENGTH`、`CHARACTER_LENGTH`、`REVERSE`、`CONCAT`、`CONCAT_WS`、`LEFT`、`RIGHT`、`SUBSTR` / `SUBSTRING`、`POSITION`、`REPLACE`、`CAST`、`ABS`、`CEIL`、`FLOOR`、`ROUND`、`YEAR`、`MONTH`、`DAY`、`ISO_YEAR`、`ISO_WEEK`、`WEEK`、`YEARWEEK`、`NOW`、`TODAY`、`CURRENT_TIMESTAMP`、`currentTimestamp`、`UNIX_TIMESTAMP`
- 投影中的已支持标量函数可以嵌套，例如 `SUBSTR(CAST(open AS STRING), 1, 6)`

约束：

- `CREATE SOURCE TABLE` 是只读表，不允许 `INSERT`
- `CREATE SINK TABLE` 允许写入，但不能作为查询输入
- `ORDER BY` 当前只支持对 `SELECT` 输出中可见的列排序
- 超出当前范围的 SQL 形态会直接返回明确错误，例如 `not supported in SQL v1`

stream SQL 当前边界：

- `session.stream_sql(...)` 只适合 `SELECT`
- `session.explain_stream_sql(...)` 适合 `SELECT` 或 `INSERT INTO <sink> SELECT ...`
- `session.start_stream_sql(...)` 只适合 `INSERT INTO <sink> SELECT ...`
- stream source 必须是 source table，stream target 必须是 sink table
- 适合的能力是 filter / projection / window / stateful aggregate，以及 bounded source 上的 `ORDER BY`
- unbounded stream 上的 `ORDER BY` 会被显式拒绝；当前 runtime 不对无界输入承诺全局有序结果

## 4. 新增功能与参数说明

### 4.1 `velaria_cli.py run start`

用途：

- 启动一次被 workspace 跟踪的执行
- 自动生成 `run_id`
- 自动创建 run 目录
- 自动记录 `run.json`、`inputs.json`、`stdout.log`、`stderr.log`、`progress.jsonl`、`artifacts/`

基础语法：

```bash
uv run --project python python python/velaria_cli.py run start -- <action> ...
```

公共参数：

- `--run-name`：给本次执行起一个更易读的名字，便于人工检索
- `--description`：给本次 run 追加一段备注/描述，便于后续 `run show`、索引检索和人工回看
- `--tag`：给 run 打标签，支持重复传入或逗号分隔，便于后续 `run list --tag ...` 过滤
- `--timeout-ms`：超时毫秒数；超时后 run 会标记为 `timed_out`

当前支持的 action：

- `file-sql`
- `vector-search`
- `stream-sql-once`

### 4.2 `file-sql`（tracked run 模式）

用途：

- 读取本地文件输入
- 按 `--input-type` 或自动探测注册成临时视图
- 执行 `session.sql(...)`
- 把结果落到 artifact 文件，并生成 preview

关键参数：

- `--csv` / `--input-path`：输入文件路径
- `--table`：临时视图名，默认 `input_table`
- `--input-type`：输入类型，可选 `auto`、`csv`、`line`、`json`
- `--delimiter`：CSV 或 line split 的分隔符
- `--line-mode`：line 模式，可选 `split`、`regex`
- `--regex-pattern`：line regex 模式下的正则
- `--mappings`：line 的列映射，例如 `uid:1,action:2`
- `--columns`：json 或顺序 line 的列名列表，例如 `id,name,score`
- `--json-format`：json 输入格式，可选 `json_lines`、`json_array`
- `--query`：要执行的 SQL
- `--output-path`：结果文件路径；不传时默认写到 `run_dir/artifacts/result.parquet`

附加行为：

- 会生成 `explain.json`
- `logical` 来自 `DataFrame.explain()`
- `physical` / `strategy` 是 Python 生态层补齐的本地 batch 执行说明

### 4.3 `vector-search`（tracked run 模式）

用途：

- 从 CSV 读入向量列
- 调用 `Session.vector_search(...)`
- 落结果 artifact，并把 native vector explain 单独保存

关键参数：

- `--csv`：输入 CSV 路径
- `--vector-column`：向量列名
- `--query-vector`：查询向量，例如 `1.0,0.0,0.0`
- `--metric`：距离指标，可选 `cosine`、`cosin`、`dot`、`l2`
- `--top-k`：返回前 K 个结果
- `--output-path`：结果文件路径；不传时默认写到 `run_dir/artifacts/result.parquet`

附加行为：

- explain 不会包装成 `logical/physical/strategy`
- native explain 会单独写到 `run_dir/artifacts/vector_explain.txt`

### 4.4 `stream-sql-once`

用途：

- 用目录流 CSV 作为 source
- 建立 CSV sink
- 执行一次 `INSERT INTO ... SELECT ...` 的流式 SQL
- 把流式 progress 逐行写到 `progress.jsonl`

适合场景：

- agent 需要拿到一次性的流式执行快照
- 需要保留 native `snapshotJson()` 结果做状态观察
- 需要把 sink 文件当成 artifact 管理

关键参数：

- `--source-csv-dir`：输入 source 目录
- `--source-table`：source 临时视图名，默认 `input_stream`
- `--source-delimiter`：source CSV 分隔符，默认 `,`
- `--sink-table`：sink 表名，默认 `output_sink`
- `--sink-schema`：sink schema，必填，用于 `CREATE SINK TABLE`
- `--sink-path`：sink 文件路径；默认 `run_dir/artifacts/stream_result.csv`
- `--sink-delimiter`：sink CSV 分隔符，默认 `,`
- `--query`：流式 SQL，必须以 `INSERT INTO` 开头
- `--trigger-interval-ms`：trigger 间隔
- `--checkpoint-delivery-mode`：checkpoint 投递模式，例如 `at-least-once`、`best-effort`
- `--execution-mode`：执行模式，例如 `single-process`、`local-workers`
- `--local-workers`：本地 worker 数
- `--max-inflight-partitions`：最大并发 partition 数
- `--max-batches`：最多处理批次数；默认 `1`，保证“once”语义

附加行为：

- `explain.json` 会保留 native `logical/physical/strategy`
- `progress.jsonl` 每一行都直接写 native `snapshotJson()`，不改字段名

### 4.5 `run list`

用途：

- 浏览最近的 tracked runs
- 按 `status`、`action`、`tag` 过滤
- 快速定位某一类分析任务的历史结果

基础语法：

```bash
uv run --project python python python/velaria_cli.py run list \
  [--status succeeded] [--action file-sql] [--tag slow-query] [--query triage] [--limit 20]
```

关键参数：

- `--status`：只看指定状态，例如 `running`、`succeeded`、`failed`
- `--action`：只看指定 action，例如 `file-sql`
- `--tag`：只看带指定标签的 run
- `--query`：按 `run_name`、`description`、`tags`、`action` 做关键词过滤
- `--limit`：最多返回多少条

返回结果会额外带上适合人工浏览的摘要字段，例如 `artifact_count` 和 `duration_ms`。

### 4.6 `run show`

用途：

- 查看单个 run 的完整元数据
- 同时列出该 run 关联的 artifacts

关键参数：

- `--run-id`：目标 run id
- `--limit`：返回 artifact 条数上限

### 4.7 `run result`

用途：

- 直接返回某个 run 的主结果 artifact
- 不需要手动先找 `artifact_id`
- 同时返回 artifact metadata 和 preview

关键参数：

- `--run-id`：目标 run id
- `--limit`：最多预览多少行，默认 `50`

### 4.8 `run diff`

用途：

- 比较两次 run 的核心 metadata
- 比较两边主结果 artifact 的 schema、row_count 和 preview
- 适合快速确认结果是否发生变化

关键参数：

- `--run-id`：左侧 run id
- `--other-run-id`：右侧 run id
- `--limit`：两边 preview 最多各返回多少行

### 4.9 `run status`

用途：

- 查看 run 当前状态
- 对 stream run 返回最后一条 progress snapshot
- 对 batch / vector run 返回状态和 artifact 摘要

关键参数：

- `--run-id`：目标 run id
- `--limit`：返回 artifact 条数上限

### 4.10 `artifacts list`

用途：

- 从 artifact 索引列出结果文件、explain 文件、preview 缓存对应的记录

关键参数：

- `--run-id`：只看某个 run 的 artifacts
- `--limit`：最多返回多少条

### 4.11 `artifacts preview`

用途：

- 读取某个 artifact 的前 N 行 preview
- 若索引中已有 preview，直接复用
- 若没有，则现算并回写索引

关键参数：

- `--artifact-id`：目标 artifact id
- `--limit`：最多预览多少行，默认 `50`

支持的预览格式：

- `csv`
- `parquet`
- `arrow`

约束：

- preview 会限制大小，避免 SQLite / JSONL 索引膨胀

### 4.12 `run cleanup`

用途：

- 清理旧 run 的索引记录
- 可选删除 run 目录文件

关键参数：

- `--keep-last N`：保留最新 N 个 run
- `--ttl-days D`：删除超过 D 天的 run
- `--delete-files`：显式删除 run 目录；不带该参数时默认只清索引，不删文件

## 5. 脚本示例

### 5.1 CSV 到 SQL

```bash
uv run --project python python \\
  skills/velaria_python_local/scripts/query_csv_to_sql.py \\
  "path/to/file.csv" \\
  --query "SELECT region, COUNT(*) AS cnt FROM csv_data GROUP BY region"
```

### 5.2 Excel 到 SQL

```bash
uv run --project python python \\
  skills/velaria_python_local/scripts/query_excel_to_sql.py \\
  "python/tests/fixtures/employee_import_mock.xlsx" \\
  --sheet "员工" \\
  --query "SELECT name, dept, COUNT(*) AS cnt FROM excel_data GROUP BY name, dept"
```

### 5.3 Bitable 到 SQL

```bash
FEISHU_BITABLE_APP_ID=... \\
FEISHU_BITABLE_APP_SECRET=... \\
FEISHU_BITABLE_BASE_URL="https://my.feishu.cn/base/...?...&view=..." \\
uv run --project python python \\
  skills/velaria_python_local/scripts/query_bitable_to_sql.py \\
  --query "SELECT owner, COUNT(*) AS cnt FROM bitable_data GROUP BY owner"
```

### 5.4 读取本地 Excel 示例

```bash
uv run --project python python \\
  skills/velaria_python_local/scripts/read_xlsx.py \\
  "python/tests/fixtures/employee_import_mock.xlsx" --sheet "员工" \\
  --query "SELECT name, COUNT(*) AS cnt FROM sheet_data GROUP BY name"
```

## 6. Agent 辅助分析



```bash
```

配置 Agent runtime。两个 runtime（Codex / Claude）共用相同的配置文件和配置键前缀 `agent*`。

**Codex runtime** 默认使用本地 `codex app-server`：

```json
{
  "agentRuntime": "codex",
  "agentAuthMode": "local",
  "agentProvider": "openai",
  "agentModel": "gpt-5.4-mini",
  "agentReasoningEffort": "none",
  "agentRuntimeWorkspace": "~/.velaria/ai-runtime",
  "agentCodexNetworkAccess": true
}
```


```json
{
  "agentRuntime": "claude",
  "agentAuthMode": "local",
  "agentProvider": "anthropic",
  "agentModel": "claude-sonnet-4-20250514",
  "agentReasoningEffort": "none",
  "agentRuntimeWorkspace": "~/.velaria/ai-runtime",
  "agentNetworkAccess": true
}
```

未显式设置 `agentModel` 时，Codex 默认 `gpt-5.4-mini`，Claude 默认 `claude-sonnet-4-20250514`。
`agentReasoningEffort` 默认是 `none`，两个 runtime 均支持。
`agentRuntimeWorkspace` 是 runtime 工作目录，用于保存 agent thread、session 与工具日志。
`agentAuthMode: "local"` 复用本地 Codex 或 Claude 登录；需要显式凭证时改为
`agentAuthMode: "api_key"`，并设置 `agentApiKey` / `agentBaseUrl`。
Codex 的 `agentCodexNetworkAccess` 和 Claude 的 `agentNetworkAccess` 分别控制各自 runtime 的网络访问，默认开启。
只有需要覆盖本地可执行文件时才设置 `agentRuntimePath` / `agentCodexRuntimePath` / `agentClaudeRuntimePath`。
代理直接使用标准环境变量，如 `http_proxy`、`https_proxy`、`all_proxy`。
Velaria usage skill 与 SQL catalog 都是按需资源：需要 SQL 函数、能力边界或常见模板时，优先调用 `velaria_sql_capabilities`、`velaria_sql_function_search`、`velaria_sql_query_patterns`，或读取 `velaria://sql/catalog`。

### CLI 模式

```bash
# 交互式 Agent：普通输入会直接发给 active agent thread
uv run --project python python python/velaria_cli.py -i

# 历史兼容的非交互 SQL 生成
uv run --project python python python/velaria_cli.py ai generate-sql \
  --prompt "按地区统计平均分数" \
  --schema "name,score,region,department"
```

### 交互模式

```bash
uv run --project python python python/velaria_cli.py -i
› 找出每个部门分数最高的人
› /status
› :run list --limit 5
```

### App 模式

在 Settings 页面配置 Agent Provider（支持 OpenAI 兼容接口或 Claude）。
在 Analyze 页面使用 Agent SQL Assistant 输入框生成 SQL。
支持 Session 管理（开启/关闭会话）以保持上下文。

## 7. Skill 自检

```bash
uv run --project python python skills/velaria_python_local/scripts/smoke.py
```

输出 `ok` 代表最小场景（CSV batch + streaming sink）通过。

## 8. 最小执行清单

1. 先创建或激活一个本地环境，并安装 `velaria`
2. 选一个脚本加载数据，确保 `session.create_temp_view(...)` 成功
3. 用 `session.sql(...)`/脚本 `--query` 将你的业务分析逻辑落到 SQL
4. 若需可追踪执行、状态查看和 artifact 预览，优先使用
   `uv run --project python python python/velaria_cli.py run ...`
   与
   `uv run --project python python python/velaria_cli.py artifacts ...`
