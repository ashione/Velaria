# Velaria 可视化数据 App v1 调研

## 当前角色

这份文档用于回答一个更接近产品的问题：

- 用户真正需要的不是单独一个文件格式
- 而是一个可视化 app
- 这个 app 要能完成数据导入、数据预览、分析执行、结果导出，以及中间结果和结果数据的本地存储

本文的重点不是 UI 样式，而是产品边界、技术架构和现有仓库能力的承接方式。

它与 [analytical-storage-format-v1.md](./analytical-storage-format-v1.md) 的关系是：

- `visual-data-app-v1.md`
  - 回答“app 应该长成什么样、后端依托什么、数据生命周期如何组织”
- `analytical-storage-format-v1.md`
  - 回答“app 里真正需要长期保存的分析表，底层怎么落盘”

## 1. 结论先行

如果目标是做一个可视化 app，当前仓库最合理的方向不是“先把 C++ 内核直接做成 UI 应用”，而是：

- 把 Velaria 继续保持为执行核心
- 把 `python_api` 视为 app 的本地后端承载层
- 在其上补一个可视化前端
- 再把分析型存储格式作为 app 的持久数据层逐步接入

推荐架构固定为三层：

- `Frontend`
  - 可视化交互层
  - 负责导入流程、表格预览、分析编排、运行状态展示、结果导出
- `Local App Service`
  - 本地 Python 服务层
  - 负责调用 `velaria.Session`、管理 workspace/run/artifact、组织导入导出与持久化
- `Velaria Kernel`
  - C++20 内核
  - 负责 batch / stream / vector 的真实执行语义

这三层里，真正需要新建设的是：

- 可视化前端
- 面向前端的本地 app service

当前仓库已经具备大量后端基础，不需要重新造第二个执行引擎。

## 2. 当前仓库已经具备的 app 基础

### 2.1 数据导入能力

当前 Python 生态层已经能承担 app 的导入后端：

- 通用文件入口：
  - `Session.probe(...)`
  - `Session.read(...)`
- 显式文件入口：
  - `Session.read_csv(...)`
  - `Session.read_line_file(...)`
  - `Session.read_json(...)`
- Arrow 输入：
  - `Session.create_dataframe_from_arrow(...)`
- Excel 输入：
  - `read_excel(...)`
- Bitable 输入：
  - `bitable.py` 适配器

参考：

- [python_api/README.md](../python_api/README.md)
- [excel.py](../python_api/velaria/excel.py)
- [runtime-contract.md](../docs/runtime-contract.md)

结论：

- app v1 没必要先发明自己的导入层
- 直接复用 Python 生态层，补一个 GUI 导入向导即可

### 2.2 数据分析能力

当前仓库已经具备 app 所需的核心分析能力：

- batch SQL
- stream SQL
- Arrow/表对象转换
- vector search / explain
- explain / progress / checkpoint contract

Python CLI 已经证明这些能力可以被组织成“命令 -> 结构化 JSON -> artifact”的工作流。

参考：

- [python_api/README.md](../python_api/README.md)
- [runtime-contract.md](../docs/runtime-contract.md)
- [cli.py](../python_api/velaria/cli.py)

### 2.3 导出与结果预览能力

当前 CLI 已经支持结果写出和 preview：

- 输出格式：
  - `csv`
  - `parquet`
  - `arrow/ipc`
- artifact preview：
  - CSV preview
  - Parquet preview
  - Arrow preview

参考：

- [cli.py](../python_api/velaria/cli.py)

### 2.4 run / artifact / workspace 管理能力

当前 Python workspace 层已经很接近 app 的“本地任务中心”：

- 每次 run 都有独立 `run_dir`
- 会写入：
  - `run.json`
  - `inputs.json`
  - `stdout.log`
  - `stderr.log`
  - `progress.jsonl`
  - `explain.json`
  - `artifacts/`
- 同时使用 `artifacts.sqlite` 作为本地索引

参考：

- [workspace/run_store.py](../python_api/velaria/workspace/run_store.py)
- [workspace/artifact_index.py](../python_api/velaria/workspace/artifact_index.py)

结论：

- app 不应该再造第三套“任务历史”和“结果索引”
- 直接复用 workspace / runs / artifacts 这套模型即可

## 3. 当前缺口

真正缺的不是执行能力，而是 app 侧能力：

- 没有前端工程
- 没有桌面壳或浏览器 UI
- 没有面向 GUI 的本地 service API
- 没有“数据源 / 数据集 / 保存结果 / 导出任务”的产品对象层
- 没有把 run/artifact/workspace 组织成最终用户可理解的导航结构
- 没有把“临时结果”和“持久数据”区分清楚

这里有一个关键判断：

- `workspace runs + artifacts`
  - 适合承接任务历史、结果文件、preview、run metadata
- `分析型存储格式 bundle`
  - 适合承接用户明确保存下来的数据集或物化结果

两者不是一回事，不应该混成一层。

## 4. 推荐产品模型

app v1 里，数据对象建议固定为五类：

- `Source`
  - 用户导入的原始来源
  - 例如 CSV / JSON / Excel / Bitable
- `Dataset`
  - 经过导入确认后的可复用数据集
  - 可只引用原始文件，也可物化保存
- `Analysis Run`
  - 一次分析执行
  - 由 query / transform / stream-once / vector-search 等动作产生
- `Artifact`
  - run 产物
  - 例如结果表、explain 文本、导出文件、preview
- `Saved Table`
  - 用户明确保存的数据
  - 后续落到内部分析型存储格式 bundle

这样分层后，产品语义会清楚很多：

- 导入不等于保存
- 执行不等于持久化
- artifact 不等于正式数据集

## 5. 推荐架构

### 5.1 总体架构

推荐 v1 架构：

```text
Frontend UI
  -> Local App Service (Python)
    -> velaria.Session / workspace / artifact index
      -> C++ kernel
      -> local files / saved bundles
```

### 5.2 Frontend

前端只负责：

- 文件选择与导入向导
- 表格 preview
- schema 展示
- SQL 编辑器 / transform 配置
- run 列表 / run 详情
- artifact preview
- 导出按钮
- 保存为数据集 / 保存为分析表

前端不负责：

- 直接调用 C++ API
- 自己解析文件
- 自己维护结果索引

### 5.3 Local App Service

本地 app service 负责：

- 调用 `velaria.Session`
- 封装 import / analyze / export / save
- 把 CLI 已有的 JSON 输出模型提升为 GUI 可用 service API
- 管理 workspace / run / artifact
- 管理持久化数据集和 saved table

这个 service 应该是 app 的真正后端。

### 5.4 Velaria Kernel

内核继续只负责：

- DataFrame / StreamingDataFrame / SQL / vector 的执行
- progress / explain / checkpoint contract
- 文件 reader 与后续分析型 bundle reader/writer

内核不负责：

- GUI
- 用户项目管理
- run 历史导航
- 应用级数据目录组织

## 6. v1 推荐实现路径

### 6.1 不直接把 CLI 当最终 app 后端

当前 CLI 已经是很好的原型和 contract 基础，但不适合直接成为最终 GUI 后端。

原因：

- CLI 适合命令行批执行
- GUI 会有更多交互式动作：
  - preview schema
  - 列选择
  - 保存命名
  - 取消执行
  - 周期刷新 run 状态
- 如果所有动作都靠 spawn CLI 子进程，后续会很快变成一层又厚又脆的命令包装

推荐做法：

- 保留 CLI 作为用户入口和自动化入口
- 新增一个 Python app service 层
- 让 app service 复用 CLI 内部已验证过的 workspace / artifact / preview 逻辑

### 6.2 app service 的职责

建议 app service 至少提供以下操作：

- `import_source`
  - 输入文件路径或外部源配置
  - 返回 schema / preview / probe 结果
- `create_dataset`
  - 把导入结果注册为 app 内数据集
- `run_analysis`
  - 执行 SQL 或标准 transform
- `list_runs`
  - 列出历史执行
- `get_run`
  - 读取 run 明细、状态、explain、progress
- `preview_artifact`
  - 读取结果 preview
- `export_artifact`
  - 导出为 csv / parquet / arrow
- `save_table`
  - 明确把结果保存为内部分析型 bundle
- `list_saved_tables`
  - 列出持久化数据

注意：

- `save_table` 对应的是正式持久化
- 普通 run artifact 仍然只属于 workspace 产物

## 7. app 里的存储分层

这是当前最需要厘清的部分。

app 不应该只有“一种存储”。

### 7.1 临时运行层

用途：

- 保存运行历史
- 保存 explain / logs / preview / result artifact
- 支持 run diff、artifact preview、错误回溯

承载：

- 继续使用 `~/.velaria/runs`
- 继续使用 `~/.velaria/index/artifacts.sqlite`

这层已经基本存在。

### 7.2 正式数据层

用途：

- 保存用户明确选择保留的数据集
- 保存物化分析结果
- 支撑后续本地 reopen、复查、重复分析

承载：

- 使用 [analytical-storage-format-v1.md](./analytical-storage-format-v1.md) 中的内部分析型 bundle

这层是接下来要建设的新能力。

### 7.3 导出交换层

用途：

- 与外部系统交换数据

承载：

- CSV
- Parquet
- Arrow / IPC

这层不应该被 Velaria 自有 bundle 替代。

## 8. v1 产品范围

如果目标是尽快做出可用 app，建议 v1 范围固定为：

- 导入：
  - CSV
  - JSON
  - Excel
- 预览：
  - schema
  - 前 N 行
  - 基础行数信息
- 分析：
  - batch SQL
  - explain
  - run status
- 导出：
  - CSV
  - Parquet
  - Arrow
- 保存：
  - “保存结果为数据集”
  - “保存数据集到内部 bundle”
- 历史：
  - run 列表
  - run 详情
  - artifact preview

v1 不做：

- 多用户协作
- 云端同步
- 完整权限系统
- 大规模 notebook 替代
- 拖拽式 DAG builder
- 在线分布式集群管理
- 复杂 dashboard builder

## 9. 推荐交互流

### 9.1 导入流

```text
选择文件
  -> probe / read
  -> 展示 schema + preview
  -> 用户确认
  -> 创建 dataset
```

### 9.2 分析流

```text
选择 dataset
  -> 输入 SQL / 选择分析模板
  -> 创建 run
  -> 观察 progress / explain / result preview
  -> 决定导出或保存
```

### 9.3 保存流

```text
分析结果预览
  -> 用户点击“保存为数据集”
  -> app service 调用内部 bundle writer
  -> 生成 saved table
  -> 后续可再次打开分析
```

### 9.4 导出流

```text
artifact / saved table
  -> 选择导出格式
  -> 导出为 csv / parquet / arrow
```

## 10. 为什么“文件格式”不是第一优先级

你前面提的“统计的存储格式”是必要的，但它不是 app 的第一层问题。

真正的优先级应该是：

1. 把 app 的对象模型定清楚
2. 把本地 app service 定清楚
3. 让导入、分析、预览、导出先闭环
4. 再把分析型存储格式接到“正式保存数据”的路径上

否则会出现一个常见误区：

- 先投入大量时间设计底层 bundle
- 但用户在 app 里仍然没有清晰的数据项目、运行历史、结果预览和导出路径

那样产品不会真正可用。

## 11. 推荐技术路线

基于当前仓库现实，推荐顺序是：

### Phase 1

- 先做本地 app service
- 复用 `python_api` 与 workspace / artifact 逻辑
- 通过 service 暴露 import / analyze / export / run / preview

### Phase 2

- 做可视化前端
- 前端先以“本地 web app”方式跑通也可以
- 后续再决定是否包装成桌面壳

### Phase 3

- 接入内部分析型 bundle
- 让“保存为数据集 / 保存为分析表”落地

### Phase 4

- 加强数据管理
- 增加 saved tables 浏览、重命名、删除、二次分析

## 12. 对仓库的直接含义

如果按这个方向推进，后续仓库工作应该分成三条线：

- `python_api`
  - app service
  - workspace / artifact / preview 继续作为后端基础
- `core`
  - 继续提供执行能力
  - 后续补分析型 bundle 的 reader / writer
- `frontend`
  - 新建前端工程
  - 不进入 core kernel 语义层

这里最重要的一点是：

- app 应该建立在现有 `python_api` 之上
- 而不是绕开它，直接让前端去拼接 C++ 层细节

## 13. 最终结论

你的真实目标是一个“本地可视化数据 app”，而不是单独一个存储格式。

因此正确的技术判断是：

- 先把 app 的产品边界和本地服务层定下来
- 把现有 `python_api + workspace + artifact` 视为 app 后端基础
- 把分析型存储格式放在“用户明确保存的数据层”
- 不把自有存储格式误当成整个 app 的解决方案

换句话说：

- `Velaria kernel` 是执行内核
- `python_api` 是 app backend 的起点
- `visual app` 是产品层
- `analysis bundle` 是持久化子系统

这四者职责要分开，系统才会稳定。
