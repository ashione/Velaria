# AGENTS.md

本仓库（Velaria，当前版本 `v0.2.10`）用于纯 C++20 数据流引擎调研与演进，当前工作重心是先稳住本地最小闭环，再逐步把执行链路扩展到本地多进程与后续分布式运行时。

## 适用范围

- 主要语言：`C++20`（核心内核）、`Python 3.12+`（生态层）、`TypeScript`（桌面 app 原型）
- 构建系统：`Bazel`（C++ 侧，依赖 `rules_cc 0.2.17`、`rules_python 1.7.0`）、`uv`（Python 侧）、`npm + electron-builder`（桌面 app）
- 当前重点目录：`src/dataflow/core/`（内核）、`src/dataflow/experimental/`（实验运行时）、`python/`（Python 生态）、`app/`（桌面原型）
- 当前执行路线：`operator chain in-proc + cross-process RPC + simple jobmaster`

## 仓库结构概览

```
Velaria/
├── src/dataflow/
│   ├── core/                     # 内核
│   │   ├── contract/api/         # DataflowSession, DataFrame 公开接口
│   │   ├── contract/catalog/     # Catalog 元数据
│   │   ├── contract/source_sink_abi.h  # Source/Sink ABI
│   │   ├── execution/            # 执行层：Value, Table, ColumnarBatch, CSV, FileSource, NanoArrow IPC
│   │   ├── execution/runtime/    # Executor, ExecutionOptimizer, AggregateLayout, SIMD Dispatch, VectorIndex
│   │   ├── execution/serial/     # Serializer
│   │   ├── execution/stream/     # Stream, BinaryRowBatch
│   │   └── logical/              # SQL Parser, SQL Planner, Plan
│   ├── ai/                       # AI Plugin Runtime
│   ├── experimental/
│   │   ├── rpc/                  # ActorRPC Codec, RPC Codec, Serialization
│   │   ├── runner/               # Actor Runner
│   │   ├── runtime/              # ActorRuntime, ByteTransport, JobMaster, RPC Runner
│   │   ├── stream/               # Actor Stream Runtime, Actor Execution Optimizer
│   │   └── transport/            # IPC Transport
│   ├── interop/python/           # Python native binding (python_module.cc)
│   ├── examples/                 # 所有 demo/benchmark/CLI 入口 (.cc)
│   └── tests/                    # C++ 回归测试 (.cc)
├── python/
│   ├── velaria/                  # Python 包（cli/, ai_runtime/, embedding, keyword_index, agentic_*, bitable, excel, custom_stream, workspace/）
│   ├── tests/                    # Python 测试
│   ├── examples/                 # Python 示例
│   ├── benchmarks/               # Python 基准
│   ├── experimental/             # Python 实验功能
│   ├── pyproject.toml            # Python 项目配置（deps: pyarrow, pandas, openpyxl, jieba）
│   └── velaria_service/          # 本地 service 包（_router, handlers, helpers）
├── app/                          # Electron 桌面 app 原型
│   ├── src/main/                 # Electron main process
│   ├── src/preload/              # Preload scripts
│   ├── src/renderer/             # 前端渲染层
│   └── scripts/                  # build-sidecar, package-macos
├── docs/                         # 设计文档与 benchmark 报告
├── plans/                        # 演进计划与路线图
├── scripts/                      # 构建、回归、发布、benchmark 脚本
├── skills/                       # 用户面技能说明
├── tools/                        # Bazel 辅助规则 (python_configure.bzl)
├── BUILD.bazel                   # 根 Bazel 构建文件
└── MODULE.bazel                  # Bazel 模块定义（nanoarrow 0.8.0, cppjieba 5.6.3, limonp）
```

## C++ 外部依赖（Bazel 管理）

| 依赖 | 版本 | 用途 |
|------|------|------|
| nanoarrow | 0.8.0 | Arrow 列式格式与 IPC 编解码 |
| cppjieba | 5.6.3 | 中文分词（keyword search） |
| limonp | master | cppjieba 工具库依赖 |

## Python 依赖（uv 管理）

| 依赖 | 版本范围 | 用途 |
|------|---------|------|
| pyarrow | >=23,<24 | Arrow 交换层 |
| pandas | >=2.1,<3 | 数据帧操作 |
| openpyxl | >=3.1,<4 | Excel 文件读取 |
| jieba | >=0.42,<1 | 中文分词 |
| sentence-transformers | >=5.3.0 (optional: embedding) | 离线 embedding 生成 |
| socksio | >=1.0.0 (optional: embedding) | SOCKS 代理支持 |

## AI Runtime（可选依赖）

| 依赖 | 版本范围 | 用途 |
|------|---------|------|
| claude-agent-sdk | >=0.1.60 (optional: ai-claude) | Claude Agent SDK runtime |
| codex-app-server-sdk | >=0.1.0 (optional: ai-codex) | Codex App Server runtime |

## Bazel 构建目标一览

### 核心库

- `dataflow_core` — 主内核库（batch/stream/SQL/vector/SIMD）
- `dataflow_actor_rpc_codec` — Actor RPC 编解码
- `dataflow_transport` — 传输层
- `dataflow_actor_runtime` — Actor 运行时
- `dataflow_actor_runner` — Actor Runner（含 scheduler/worker/client 依赖）
- `dataflow_stream_actor_runtime` — Stream Actor 运行时

### 可执行入口

- `sql_demo`, `df_demo`, `stream_demo`, `stream_sql_demo` — 单节点 demo
- `actor_rpc_scheduler`, `actor_rpc_worker`, `actor_rpc_client` — 多进程组件
- `actor_rpc_smoke` — RPC 冒烟测试
- `velaria_cli` — C++ CLI
- `_velaria_native` / `velaria_pyext` — Python 原生扩展

### Benchmark

- `stream_benchmark`, `stream_actor_benchmark` — 流式基准
- `tpch_q1_style_benchmark` — TPC-H Q1 风格基准
- `batch_aggregate_benchmark` — 批聚合基准
- `string_builtin_benchmark` — 字符串内置函数基准
- `vector_search_benchmark` — 向量搜索基准
- `file_source_benchmark` — 文件源基准

### 回归测试套件

- `core_regression` — 核心回归（columnar_batch, planner, source_sink_abi, sql, simd, stream, vector, source_materialization, file_source）
- `python_ecosystem_regression` — Python 生态回归
- `experimental_regression` — 实验运行时回归

## 开发协作规则

- 优先保证语义一致性、边界可诊断、最小可用先行。
- 修改前先明确影响范围：`SQL / Planner / Executor / Session / Stream / Runtime`。
- 涉及 SQL、Planner、Executor、Session、Stream、Runtime、Agent runtime 或 CLI 交互链路的改动，修改前必须先跑当前基线端到端验证或最小可复现 smoke，并记录基线结果；修改后再跑同一链路和必要回归，确保问题是被修复而不是被测试环境变化掩盖。
- 小步推进，每次改动围绕一个可验证目标，不做无关重构。
- 保持现有 C++/Bazel 风格，新增示例源码统一使用 `.cc`，头文件使用 `.h`。
- `session` 对外入口统一使用 `DataflowSession`。
- 现阶段不做 JVM/Python 宿主栈移植，核心计算逻辑保持纯 C++ 实现。
- 单节点示例命令保持可用，不要为了多进程实验破坏 `sql_demo / df_demo / stream_demo`。
- 所有 Python 相关命令统一显式使用 `uv` 执行，包括测试、脚本、依赖安装；不要直接调用 `python` / `pip`。
- 涉及 `python`、多维表格导入、embedding pipeline、desktop sidecar 的开发与验证时，默认可继续使用 `python/.venv`；如需隔离环境，使用 `UV_PROJECT_ENVIRONMENT=<env-name> uv ... --project python` 显式指定，不要在文档或规则里写死某个特定环境名。
- `README.md` 保持英文，`README-zh.md` 保持中文；后续修改 README 内容时必须同步更新这两份文档。
- `skills/*.md` 面向最终用户使用说明，不写仓库内部编译、Bazel 构建、源码同步或其他实现侧操作；只保留用户可直接执行的使用方式、参数说明与输入输出约束。
- 仓库文档若展示 Python CLI 命令，必须使用仓库内真实可见入口：源码脚本 `uv run --project python python python/velaria_cli.py ...`，或已打包产物 `./dist/velaria-cli ...`；不要默认写成全局可执行的 `velaria-cli ...`，除非文档已明确提供安装该命令的步骤。
- Agent/runtime core 必须保持领域无关和抽象纯粹；不要在 core、agent 指令或本地 function 中硬编码某个行业、数据源、指数、字段名或业务场景的特化映射/兜底逻辑。遇到编码、列名、表名等问题时，优先做通用规范化、显式 metadata / mapping 返回和可诊断错误，让 agent 或用户基于上下文决定业务语义。
- 新功能开发禁止一开始就堆多个版本、兼容路径或“莫须有”的兜底逻辑；先实现一条语义清晰、可验证的主路径。只有在真实用例、测试或运行错误证明边界存在后，才补充最小化、可诊断、可删除的处理分支。

## 本轮协作沉淀

- 根 `README.md` / `README-zh.md` 只保留项目目标、分层模型、稳定 contract、当前范围、真实入口与最小运行/验证路径；避免写过长的大段说明，细节下沉到 `python/README.md`。
- 新增 Python 生态能力时，根 README 只做高层概念归位：说明它属于 `Python Ecosystem`，负责什么、不负责什么；具体参数、CLI 示例与端到端用法写进 `python/README.md`。
- `skills/*.md` 的示例命令必须直接可执行，优先写源码入口或打包产物入口；不要假设存在额外安装步骤、shell alias 或全局命令。
- Python 生态层的新能力如果面向 agent/skill 调用，默认要求 stdout 维持机器可读 JSON，日志落文件，失败路径也不能退化成 traceback 噪音。
- 类似 workspace/run tracking 这类 Python 侧能力，要明确保持 kernel contract 不变：`explain` 继续对齐 `logical/physical/strategy`，`progress` 继续直接使用原生 `snapshotJson()`，不要在生态层发明第二套语义。
- 本轮多维表格批量导入优化默认落在 Python 生态层时，要求优先采用 stream/batch 方式驱动 embedding provider，避免先整表 `to_pylist()` 后再做一次大批量物化；如确需回退到整表物化，必须先说明原因与边界。
- 本轮 Python 侧验证默认仍可直接使用项目默认环境；若某次任务需要隔离依赖或避免污染现有环境，再显式设置 `UV_PROJECT_ENVIRONMENT=<env-name>` 后运行 `uv sync` / `uv run`。

## 命名与术语约束

不要再引入外部框架专名。统一使用仓库内部术语：

- `Session` 风格命名统一使用 `DataflowSession`
- SQL 入口统一使用 `session.sql(...)`
- 读取入口统一使用 `session.read(...)`
- `compatibility` 表达统一改为 `语义对齐` 或 `接口语义参考`

如果需要描述兼容关系，使用“语义对齐”“接口映射”“外部行为参考”，不要把外部框架名直接放进仓库主接口或注释里。

### SQL DDL 建表语义

- `CREATE TABLE`：普通表，支持 `SELECT` 与 `INSERT`。
- `CREATE SOURCE TABLE`：源表，当前约束为只读，不允许 `INSERT` 写入。
- `CREATE SINK TABLE`：汇聚/结果表，当前约束为禁止 `SELECT` 查询使用该表（包括 join 输入）。

### SQL DML 运行约束

- `INSERT INTO ... VALUES`：支持全列/显式列插入。
- `INSERT INTO ... SELECT`：支持查询结果写入目标表（目标表不应为 `SOURCE TABLE`）。
- `SINK TABLE` 可用于写入，不应用于查询输入。

### 调度执行模型

- scheduler 负责接入、快照记录、分发与状态收集，不在本地执行 SQL。
- 客户端提交均应走 worker 执行链路。

### 当前调度策略（v1）

- 任务建模：提交后生成 `job`，再拆成 `chain` 与 `task`；远端执行走 `RemoteTaskSpec` 队列。
- 队列与触发：`chain` 达到可运行状态后进入待调度队列；`scheduler` 周期性扫描 `idle_workers` 并调用 `dispatch` 下发任务。
- 典型流：
  - `SUBMITTED -> CHAIN_READY -> CHAIN_SCHEDULED -> CHAIN_RUNNING -> CHAIN_SUCCEEDED/FAILED`（task 相应变化）。
- 排队：当无空闲 worker 时，任务保留在 `JobMaster` 的 remote pending queue，不会丢弃。
- 分发方式：`first-come-first-served` 的空闲 worker 分配；由 `task_to_worker` 跟踪绑定。
- 失败与恢复：task 发送失败触发本地失败落盘并通知客户端；链内失败进入 jobmaster 重试逻辑（当前重试策略按配置）。
- worker 供给：scheduler 默认自动启动本地 worker（`--local-workers`，默认 1）；如指定 `--no-auto-worker` 则走手工挂载模式。

## 环境准备

### 必要工具

| 工具 | 用途 | 安装方式 |
|------|------|---------|
| Bazel | C++ 构建 | `brew install bazel` |
| uv | Python 包管理 | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| Python 3.12+ | Python 运行时 | `brew install python@3.13` |
| Node.js + npm | 桌面 app | `brew install node` |

### 首次 bootstrap

```bash
# 1. C++ 构建验证
bazel build //:sql_demo //:df_demo //:stream_demo

# 2. Python 生态 bootstrap
bazel build //:velaria_pyext
bazel run //python:sync_native_extension
uv sync --project python --python python3.13

# 3. 桌面 app（可选）
cd app && npm install && cd ..

# 4. AI Runtime（可选）
uv sync --project python --extra ai-claude    # 或 --extra ai-codex
```

## 常用命令

### 构建

```bash
bazel build //:sql_demo
bazel build //:df_demo
bazel build //:stream_demo
bazel build //:actor_rpc_scheduler //:actor_rpc_worker //:actor_rpc_client //:actor_rpc_smoke
```

### 运行示例

```bash
bazel run //:sql_demo
bazel run //:df_demo
bazel run //:stream_demo
bazel run //:actor_rpc_smoke
./scripts/run_actor_rpc_scheduler.sh -- --listen 127.0.0.1:61000 --node-id scheduler
bazel run //:actor_rpc_worker -- --connect 127.0.0.1:61000 --node-id worker-1
bazel run //:actor_rpc_client -- --connect 127.0.0.1:61000 --payload "demo payload"
```

### Python 示例

```bash
uv run --project python python python/examples/demo_batch_sql_arrow.py
uv run --project python python python/examples/demo_stream_sql.py
uv run --project python python python/examples/demo_vector_search.py
uv run --project python python python/velaria_cli.py --help
```

### AI 辅助分析

```bash
uv run --project python python python/velaria_cli.py ai generate-sql \
  --prompt "找出每个部门的平均分数" \
  --schema "name,score,region,department"

uv run --project python python python/velaria_cli.py ai session start --schema "name,score"
uv run --project python python python/velaria_cli.py ai session list
uv run --project python python python/velaria_cli.py ai session close --session-id <id>
```

### 一次 build/smoke 摘要

```bash
bazel build //:sql_demo //:df_demo //:stream_demo \
  //:actor_rpc_scheduler //:actor_rpc_worker //:actor_rpc_client //:actor_rpc_smoke \
  && bazel run //:actor_rpc_smoke \
  && echo '[summary] build+smoke ok'
```

### 回归测试

```bash
# C++ 核心回归
bazel test //:core_regression

# Python 生态回归
bazel test //:python_ecosystem_regression

# 实验运行时回归
bazel test //:experimental_regression

# 脚本入口
./scripts/run_core_regression.sh
./scripts/run_python_ecosystem_regression.sh
./scripts/run_experimental_regression.sh
./scripts/run_stream_observability_regression.sh
```

### Python 测试

```bash
uv sync --project python
uv run --project python python -m unittest
bazel build //:velaria_pyext
bazel test //python:custom_stream_source_test
bazel test //python:streaming_v05_test
bazel test //python:arrow_stream_ingestion_test
```

### 桌面 app

```bash
cd app && npm install && npm start
bash app/scripts/build-sidecar-macos.sh
bash app/scripts/package-macos.sh
```

## 最小验收清单

### actor/rpc smoke

```bash
bazel run //:actor_rpc_smoke
```

通过标准：

- 输出 `[smoke] actor rpc codec roundtrip ok`
- 返回码为 `0`

### 本地多进程联动

按顺序执行：

```bash
bazel run //:actor_rpc_scheduler -- --listen 127.0.0.1:61000 --node-id scheduler
bazel run //:actor_rpc_worker -- --connect 127.0.0.1:61000 --node-id worker-1
bazel run //:actor_rpc_client -- --connect 127.0.0.1:61000 --payload "demo payload"
```

默认 scheduler 会自启动一个本地 worker；如需验证手工挂载链路，可按上面保留 worker 步骤单独运行，或使用 `--no-auto-worker`。

或一键执行：

```bash
./scripts/run_actor_rpc_e2e.sh --payload "demo payload"
```

通过标准：
- scheduler 输出 `[scheduler] listen 127.0.0.1:61000`
- worker 输出 `[worker] connected 127.0.0.1:61000`
- client 先输出 `job accepted`，再输出 `job result`
- 日志中通常不出现 `no-worker-available`（除非配置 `--no-auto-worker`）
- 日志中不出现 `cannot connect`
- 日志中不出现 `scheduler closed connection`

### 单节点底线

```bash
bazel run //:sql_demo
bazel run //:df_demo
bazel run //:stream_demo
```

修改与调试期间，以上单节点链路应保持不变。

## 错误规避要点

- 编译标准固定为 `C++20`，不要引入更高版本语法或库假设。
- 示例文件统一保持 `.cc`；不要新建 `.cpp` / `.cxx` 示例。
- `--listen` / `--connect` 参数必须是 `host:port`。
- 多进程验收顺序必须是 `scheduler -> worker -> client`。
- Python 测试、脚本、依赖安装统一使用 `uv run ...` / `uv pip ...`，不要直接使用 `python3` / `pip3`。
- Python 生态层若需要隔离依赖或避免污染默认环境，使用 `UV_PROJECT_ENVIRONMENT=<env-name>` 显式创建和复用隔离环境，并在当前任务说明中写清环境用途；不要把具体环境名硬编码进仓库规则。
- 若出现 `no-worker-available`，优先确认是否启动了 `--no-auto-worker`，然后再检查 worker 是否已连上 scheduler。
- 若出现 `cannot connect`，先查 scheduler 是否已监听、端口是否一致。
- `Value` 当前允许 `Int64/Double` 跨类型比较；若改成严格类型模式，需同步更新 Planner 与示例。
- `Schema` / `Join` 场景要注意对象生命周期，避免临时对象引用外泄。
- 当前多进程路径只是本地多进程验证，不要误写成完整分布式调度已完成。

## 当前边界

- 当前 SQL 仍是 v1 子集。
- 当前版本已支持最小 window SQL：`WINDOW BY <time_col> EVERY <window_ms> AS <output_col>`；但不继续横向扩展 ANSI window 语法。
- 当前版本不扩展 CTE / 子查询 / `UNION`。
- `JOIN` 仅保留现有最小能力，不在本轮继续做更复杂 join 扩展。
- Python 继续只做 Arrow 交换层和前端入口，不引入 Python callback/UDF 进入热路径。
- 错误恢复能力还需继续加强，当前优先依赖清晰日志与失败文案。
- 分布式运行时尚未真正接入，当前以本地执行器与本地多进程协作为准。
