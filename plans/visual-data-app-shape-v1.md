# Velaria 可视化数据 App 形态分析 v1

## 当前角色

这份文档只回答一个问题：

- 这个 app 从用户视角看，应该长什么样

它先于交互协议、先于本地服务实现、也先于 Velaria 内部文件格式。

顺序固定为：

1. `app shape`
2. `interaction model`
3. `storage model`

原因很简单：

- 如果 app 形态不清楚，交互会变成零散按钮集合
- 如果交互不清楚，文件格式会被过早设计成错误对象

## 1. 一句话定义

Velaria app v1 不应该长成“通用 BI dashboard 平台”，也不应该长成“IDE 式开发工具”。

它更适合被定义为：

- 一个本地优先的数据工作台
- 用户可以导入数据、检查数据、运行分析、比较结果、导出结果、保存数据集

所以它的产品气质应该是：

- `data workbench`
- 不是 `dashboard builder`
- 不是 `notebook replacement`
- 不是 `ETL orchestration studio`

## 2. 用户真正要完成的任务

从你现在的目标看，用户的核心任务很集中：

- 把数据弄进来
- 看看数据是什么
- 试几种分析方式
- 看结果是否可信
- 导出结果
- 把有价值的中间结果或最终结果保存下来，后续继续分析

这决定了 app 首页不应该先放图表，也不应该先放 SQL 编辑器。

首页应该先回答三个问题：

- 我有哪些数据
- 最近跑过什么
- 我接下来最可能要做什么

## 3. app v1 的主模式

app v1 建议固定为三个主模式：

- `Data`
  - 管数据导入、数据集、saved tables
- `Analyze`
  - 管 SQL / transform / vector / stream-once 执行
- `Runs`
  - 管历史执行、结果预览、对比、导出

这三个模式是用户心智上最稳的分层。

不建议 v1 一开始就分成很多模块：

- Sources
- Projects
- Pipelines
- Catalog
- Explorer
- Jobs
- Dashboards
- Storage

那样信息架构会过重，而当前仓库还没有足够多产品对象支撑它。

## 4. app 的整体布局

推荐整体布局固定为：

```text
左侧主导航
  -> Data / Analyze / Runs / Settings

中间主工作区
  -> 当前页面的主内容

右侧上下文侧栏
  -> schema / preview / explain / run status / export / save actions
```

为什么不是“多标签 IDE 布局”：

- 当前用户任务以数据对象和运行对象为主
- 不是以源码文件和终端为主
- schema、preview、run status 更适合做上下文侧栏，而不是新开 tab

## 5. 首页应该长什么样

首页建议是一个工作台总览，不是 marketing landing page。

首页固定包含四块：

- `Recent Datasets`
  - 最近导入或保存的数据集
- `Recent Runs`
  - 最近分析执行
- `Quick Actions`
  - Import File
  - New SQL Analysis
  - Open Saved Table
  - View Last Result
- `Health / Local Status`
  - 当前 workspace 路径
  - 最近一次执行状态
  - 现有 saved tables 数量

首页不建议放：

- 大幅 hero 文案
- 炫技图表
- 复杂 KPI 仪表盘

因为这个 app 的第一价值不是“看板展示”，而是“数据工作闭环”。

## 6. Data 模块应该长什么样

`Data` 模块应该是 app 的核心入口，不是附属页。

### 6.1 左侧列表

左侧列表显示三类对象：

- Imported Sources
- Datasets
- Saved Tables

### 6.2 中间主区

中间主区分成两个层次：

- 上半部分：对象概览
  - 名称
  - 类型
  - 来源
  - schema 概况
  - 行数
  - 最后更新时间
- 下半部分：数据预览表格
  - 前 N 行
  - 列名
  - 类型

### 6.3 右侧上下文

右侧上下文固定显示：

- schema
- source metadata
- import settings
- actions
  - Analyze
  - Export
  - Save as Table
  - Replace / Re-import

### 6.4 为什么 `Data` 要放这么重

因为这个 app 的真实主对象不是“图表”，而是“数据集”。

如果 `Data` 模块做轻了，用户很快会迷失：

- 不知道哪个是原始文件
- 哪个是运行结果
- 哪个是正式保存的数据

## 7. Analyze 模块应该长什么样

`Analyze` 模块建议是一个双栏工作台，而不是单纯 SQL 编辑器。

### 7.1 左栏

左栏固定显示：

- 当前数据集 / saved table 选择器
- 可用字段
- 常见分析模板
  - Preview
  - Filter
  - Group By
  - Sort / Limit
  - Vector Search
  - Stream Once

### 7.2 中间主区

主区提供两种模式切换：

- `SQL`
  - 原生 SQL 输入
- `Guided`
  - 结构化配置

不要只做 SQL，也不要只做拖拽。

原因：

- 只做 SQL，门槛太高
- 只做 Guided，很快受限
- 双模式可以覆盖更广用户，同时都映射到同一执行核心

### 7.3 下半区结果区

结果区固定显示：

- result preview
- row count
- schema
- explain 摘要
- run status

### 7.4 右侧动作栏

动作栏固定包含：

- Run
- Cancel
- Export Result
- Save as Dataset
- Save as Table
- Compare with Previous Run

## 8. Runs 模块应该长什么样

`Runs` 模块不是日志页，而是“分析历史中心”。

### 8.1 列表页

列表至少展示：

- run name
- action
- status
- created at
- duration
- tags
- artifact count

这些字段当前已有 run / artifact 基础可以支撑。

### 8.2 详情页

详情页建议固定分成五个页签：

- Summary
- Result Preview
- Explain
- Progress
- Logs

为什么这样分：

- Summary 用来帮助用户快速判断这次 run 是什么
- Result Preview 是最常打开的页
- Explain 适合排查计划与执行策略
- Progress 适合 stream-once 和后续更长任务
- Logs 只在出错时重要

### 8.3 Compare 视图

`Runs` 模块应该内建 compare，而不是放到后面。

原因：

- 当前仓库已有 run diff 能力雏形
- 比较分析结果是数据 app 的高频动作

compare 视图建议显示：

- metadata diff
- schema diff
- row count diff
- preview diff

## 9. app v1 不该长成什么样

以下方向都不适合 v1：

### 9.1 不要先做 dashboard 首页

因为当前仓库的结果结构、图表推荐、可视化语义都还没稳定。

如果先做 dashboard，会把注意力拉到图表层，掩盖真正还没完成的数据工作流。

### 9.2 不要先做 notebook 式多 cell 编辑器

原因：

- notebook 会立刻引入 cell 生命周期、变量状态、执行顺序、输出绑定等复杂度
- 当前仓库更适合“单次分析 run”模型

### 9.3 不要先做复杂项目树

当前对象数还不够多。

更合适的是：

- 清晰的数据对象列表
- 清晰的 run 列表
- 清晰的保存动作

## 10. v1 的核心页面清单

v1 建议固定为以下页面：

1. `Home`
2. `Data List`
3. `Data Detail`
4. `Import Wizard`
5. `Analyze Workspace`
6. `Run List`
7. `Run Detail`
8. `Settings`

其中真正的核心是：

- `Data Detail`
- `Analyze Workspace`
- `Run Detail`

这三个页面构成最小闭环。

## 11. v1 的最小视觉语言

虽然这里不是做 UI 设计稿，但需要先定视觉倾向。

这个 app 不适合做得太“消费级”，也不适合太“IDE 味”。

推荐视觉方向：

- 明亮底色
- 强信息密度
- 表格和 schema 优先
- 图表克制
- 动作按钮明显，但不花哨

重点应该让用户快速扫到：

- 当前数据是什么
- 行列结构是什么
- 最近运行结果是什么
- 下一步能点什么

## 12. 信息架构的真正主轴

这个 app 的主轴应该是：

```text
Data
  -> Analyze
    -> Result
      -> Export or Save
```

不是：

```text
Chart
  -> Dashboard
    -> Share
```

也不是：

```text
SQL file
  -> notebook cells
    -> dev workflow
```

这点非常关键，因为它会决定：

- 首页长什么样
- 主导航怎么分
- 交互是否以 run 为中心
- 文件格式最终承载什么对象

## 13. 对后续交互设计的直接约束

一旦 app 形态按本文固定，后续交互设计就必须遵守这些约束：

- “导入”必须先产生可预览对象，不能直接掉进 SQL 编辑器
- “运行”必须产生 run 记录，而不是只有一次性结果弹窗
- “保存”必须明确区分：
  - 保存 artifact
  - 保存 dataset
  - 保存正式 table
- “导出”必须是对象动作，不应该散落在各个临时按钮里

## 14. 对文件格式设计的直接约束

一旦 app 形态按本文固定，后续文件格式也会自然收敛：

- 文件格式首先要服务 `Saved Table`
- 而不是服务临时 run artifact
- 也不是服务 UI session state

也就是说：

- `workspace runs + artifacts`
  - 继续承载任务历史
- `Velaria analytical bundle`
  - 承载正式保存的数据对象

这样职责才不会打架。

## 15. 最终结论

这个 app v1 应该长成一个 `本地数据工作台`。

它的骨架应该是：

- 首页总览
- Data 模块
- Analyze 模块
- Runs 模块

它的用户心智应该是：

- 先看数据
- 再跑分析
- 然后查看结果
- 最后导出或保存

只有把这个形态先定下来，后续去讨论：

- 交互如何编排
- 本地服务如何设计
- Velaria 的内部分析型文件格式如何落位

才不会本末倒置。
