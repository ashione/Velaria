---
name: velaria-python-local
description: How to use a locally installed Velaria Python package for local analysis and stream SQL workflows.
---

# Velaria Local Python Skill

这个 Skill 用于说明如何把 `velaria` 的 Python 包作为本地分析工具使用，不涉及仓库实现代码或编译/构建逻辑。  
核心思路：**把数据先加载为临时视图，再用 `session.sql(...)` 写 SQL 处理。**

本 Skill 默认只使用 `uv` 执行：假设你已通过其他渠道安装好 `velaria`，并在命令行里可复用 `uv` 运行环境。

安装 `velaria` wheel 或 package 后，默认 CLI 命令是：

- `velaria-cli`
- `velaria_cli`

下文统一用 `velaria-cli` 举例；如果你的环境只暴露了 `velaria_cli`，可直接等价替换。

## 1. 环境准备

```bash
uv run --with velaria --with "pyarrow==23.0.1" --with pandas --with openpyxl \\
  python -c "import velaria; print(velaria.__version__)"

uv run --with velaria --with "pyarrow==23.0.1" --with pandas --with openpyxl \\
  velaria-cli --help
```

> 若外部 whl 已包含原生扩展，则可直接使用 `Session`；若是纯 Python 子集或兼容层缺失，会在首次创建 `Session` 时报错。  
> `uv run --with` 需要可解析的包（index 名称或本地 wheel），本 Skill 不包含仓库构建步骤。  

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

如果你更适合用 CLI 而不是临时 Python 片段，现在也可以直接使用 workspace/run store。

```bash
velaria-cli run start -- csv-sql \
  --csv path/to/file.csv \
  --query "SELECT region, COUNT(*) AS cnt FROM input_table GROUP BY region"

velaria-cli run show --run-id <run_id>
velaria-cli artifacts list --run-id <run_id>
```

## 4. 新增功能与参数说明

### 4.1 `velaria-cli run start`

用途：

- 启动一次被 workspace 跟踪的执行
- 自动生成 `run_id`
- 自动创建 run 目录
- 自动记录 `run.json`、`inputs.json`、`stdout.log`、`stderr.log`、`progress.jsonl`、`artifacts/`

基础语法：

```bash
velaria-cli run start -- <action> ...
```

公共参数：

- `--run-name`：给本次执行起一个更易读的名字，便于人工检索
- `--timeout-ms`：超时毫秒数；超时后 run 会标记为 `timed_out`

当前支持的 action：

- `csv-sql`
- `vector-search`
- `stream-sql-once`

### 4.2 `csv-sql`（tracked run 模式）

用途：

- 读取本地 CSV
- 注册成临时视图
- 执行 `session.sql(...)`
- 把结果落到 artifact 文件，并生成 preview

关键参数：

- `--csv`：输入 CSV 路径
- `--table`：临时视图名，默认 `input_table`
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

### 4.5 `run show`

用途：

- 查看单个 run 的完整元数据
- 同时列出该 run 关联的 artifacts

关键参数：

- `--run-id`：目标 run id
- `--limit`：返回 artifact 条数上限

### 4.6 `run status`

用途：

- 查看 run 当前状态
- 对 stream run 返回最后一条 progress snapshot
- 对 batch / vector run 返回状态和 artifact 摘要

关键参数：

- `--run-id`：目标 run id
- `--limit`：返回 artifact 条数上限

### 4.7 `artifacts list`

用途：

- 从 artifact 索引列出结果文件、explain 文件、preview 缓存对应的记录

关键参数：

- `--run-id`：只看某个 run 的 artifacts
- `--limit`：最多返回多少条

### 4.8 `artifacts preview`

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

### 4.9 `run cleanup`

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
uv run --with velaria --with "pyarrow==23.0.1" \\
  python skills/velaria_python_local/scripts/query_csv_to_sql.py \\
  "path/to/file.csv" \\
  --query "SELECT region, COUNT(*) AS cnt FROM csv_data GROUP BY region"
```

### 5.2 Excel 到 SQL

```bash
uv run --with velaria --with pandas --with openpyxl \\
  python skills/velaria_python_local/scripts/query_excel_to_sql.py \\
  "python_api/tests/fixtures/employee_import_mock.xlsx" \\
  --sheet "员工" \\
  --query "SELECT name, dept, COUNT(*) AS cnt FROM excel_data GROUP BY name, dept"
```

### 5.3 Bitable 到 SQL

```bash
FEISHU_BITABLE_APP_ID=... \\
FEISHU_BITABLE_APP_SECRET=... \\
FEISHU_BITABLE_BASE_URL="https://my.feishu.cn/base/...?...&view=..." \\
uv run --with velaria --with pandas --with openpyxl \\
  python skills/velaria_python_local/scripts/query_bitable_to_sql.py \\
  --query "SELECT owner, COUNT(*) AS cnt FROM bitable_data GROUP BY owner"
```

### 5.4 读取本地 Excel 示例

```bash
uv run --with velaria --with pandas --with openpyxl \\
  python skills/velaria_python_local/scripts/read_xlsx.py \\
  "python_api/tests/fixtures/employee_import_mock.xlsx" --sheet "员工" \\
  --query "SELECT name, COUNT(*) AS cnt FROM sheet_data GROUP BY name"
```

## 6. Skill 自检

```bash
uv run --with velaria --with "pyarrow==23.0.1" \\
  python skills/velaria_python_local/scripts/smoke.py
```

输出 `ok` 代表最小场景（CSV batch + streaming sink）通过。

## 7. 最小执行清单

1. 确认 `uv` 环境可运行并已解析 `velaria`
2. 选一个脚本加载数据，确保 `session.create_temp_view(...)` 成功
3. 用 `session.sql(...)`/脚本 `--query` 将你的业务分析逻辑落到 SQL
4. 若需可追踪执行、状态查看和 artifact 预览，优先使用 `velaria-cli run ...` 与 `velaria-cli artifacts ...`
