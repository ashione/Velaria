---
name: velaria-python-local
description: How to use a locally installed Velaria Python package for local analysis and stream SQL workflows.
---

# Velaria Local Python Skill

这个 Skill 用于说明如何把 `velaria` 的 Python 包作为本地分析工具使用，不涉及仓库实现代码或编译/构建逻辑。  
核心思路：**把数据先加载为临时视图，再用 `session.sql(...)` 写 SQL 处理。**

本 Skill 默认只使用 `uv` 执行：假设你已通过其他渠道安装好 `velaria`，并在命令行里可复用 `uv` 运行环境。

## 1. 环境准备

```bash
uv run --with velaria --with "pyarrow==23.0.1" --with pandas --with openpyxl \\
  python -c "import velaria; print(velaria.__version__)"
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

## 4. 脚本示例

### 4.1 CSV 到 SQL

```bash
uv run --with velaria --with "pyarrow==23.0.1" \\
  python skills/velaria_python_local/scripts/query_csv_to_sql.py \\
  "path/to/file.csv" \\
  --query "SELECT region, COUNT(*) AS cnt FROM csv_data GROUP BY region"
```

### 4.2 Excel 到 SQL

```bash
uv run --with velaria --with pandas --with openpyxl \\
  python skills/velaria_python_local/scripts/query_excel_to_sql.py \\
  "python_api/tests/fixtures/employee_import_mock.xlsx" \\
  --sheet "员工" \\
  --query "SELECT name, dept, COUNT(*) AS cnt FROM excel_data GROUP BY name, dept"
```

### 4.3 Bitable 到 SQL

```bash
FEISHU_BITABLE_APP_ID=... \\
FEISHU_BITABLE_APP_SECRET=... \\
FEISHU_BITABLE_BASE_URL="https://my.feishu.cn/base/...?...&view=..." \\
uv run --with velaria --with pandas --with openpyxl \\
  python skills/velaria_python_local/scripts/query_bitable_to_sql.py \\
  --query "SELECT owner, COUNT(*) AS cnt FROM bitable_data GROUP BY owner"
```

### 4.4 读取本地 Excel 示例

```bash
uv run --with velaria --with pandas --with openpyxl \\
  python skills/velaria_python_local/scripts/read_xlsx.py \\
  "python_api/tests/fixtures/employee_import_mock.xlsx" --sheet "员工" \\
  --query "SELECT name, COUNT(*) AS cnt FROM sheet_data GROUP BY name"
```

## 5. Skill 自检

```bash
uv run --with velaria --with "pyarrow==23.0.1" \\
  python skills/velaria_python_local/scripts/smoke.py
```

输出 `ok` 代表最小场景（CSV batch + streaming sink）通过。

## 6. 最小执行清单

1. 确认 `uv` 环境可运行并已解析 `velaria`
2. 选一个脚本加载数据，确保 `session.create_temp_view(...)` 成功
3. 用 `session.sql(...)`/脚本 `--query` 将你的业务分析逻辑落到 SQL
