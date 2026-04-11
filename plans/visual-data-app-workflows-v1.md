# Velaria 可视化数据 App 工作流设计 v1

## 当前角色

这份文档承接 [visual-data-app-shape-v1.md](./visual-data-app-shape-v1.md)，把产品形态进一步细化成三个关键工作流：

- 导入数据
- 展示数据
- 分析数据

它回答的问题不是底层服务怎么调，也不是存储格式怎么编码，而是：

- 用户进入 app 后，具体会看到什么
- 每一步会做出什么决定
- 每一步结束后会产出什么对象

## 1. 工作流总原则

三个工作流必须遵守同一条主线：

```text
Import
  -> Dataset
    -> Analyze
      -> Run
        -> Result Preview
          -> Export or Save
```

这条主线有几个强约束：

- 导入结束后必须先形成 `Dataset`
- 分析结束后必须形成 `Run`
- 结果必须先能 preview，再允许导出或保存
- “保存”必须晚于“分析结果可见”

这几个约束能防止 app 变成：

- 一导入就丢给 SQL 编辑器
- 一执行就只给下载文件
- 一保存就搞不清楚保存的是数据还是结果

## 2. 导入数据：Import Wizard

### 2.1 导入页的目标

导入页不是一个“选文件”弹窗，而是一个完整向导。

它的目标是让用户完成三件事：

- 选对来源
- 确认 schema 和数据长相
- 生成一个可继续分析的 `Dataset`

### 2.2 导入页的结构

导入页建议固定为四步：

1. `Choose Source`
2. `Inspect`
3. `Configure`
4. `Confirm`

#### Step 1: Choose Source

当前 v1 只放最真实的来源：

- CSV
- JSON
- Excel
- Existing Saved Table

如果 Bitable 要进 v1，也建议先放到“More Sources”折叠区，不要占首页心智。

这一步用户只做两件事：

- 选择来源类型
- 选择本地路径或输入外部源配置

#### Step 2: Inspect

这一步必须自动运行 probe / read-preview。

页面固定显示：

- 检测到的格式
- schema
- warnings
- 前 N 行 preview
- 行数估计或可用时的真实行数

如果读取失败，用户应该停留在这一步，不允许进入后续分析。

#### Step 3: Configure

这一步只暴露最少但必要的配置：

- CSV
  - delimiter
  - header on/off
- JSON
  - lines / array
  - columns
- Excel
  - sheet
  - datetime handling

不建议 v1 在导入向导里塞进太多高级选项。

如果配置改动，需要重新刷新 preview。

#### Step 4: Confirm

确认页固定要求用户填写或确认：

- dataset name
- source label
- tags
- import summary

完成后创建 `Dataset`。

### 2.3 导入工作流的状态机

导入页的状态建议固定为：

```text
idle
  -> source_selected
  -> probing
  -> preview_ready
  -> config_dirty
  -> reprobe
  -> confirm_ready
  -> dataset_created
  -> failed
```

其中最重要的规则是：

- `preview_ready` 才能进入 `confirm_ready`
- `dataset_created` 前不能直接进入 `Analyze`

### 2.4 导入完成后产出什么

导入完成后不应该只把文件路径塞进内存。

最小产出对象应包含：

- dataset id
- dataset name
- source type
- source path
- schema summary
- preview snapshot
- import options
- created at

注意：

- 这里是 `Dataset`
- 还不是 `Saved Table`
- 也不是 `Run`

## 3. 展示数据：Data Detail

### 3.1 数据详情页的目标

`Data Detail` 不是一个“静态表格页”，它承担三个责任：

- 帮用户建立对数据的信心
- 帮用户决定下一步分析方向
- 帮用户决定是否保存、替换、导出

### 3.2 页面结构

建议固定成 `Summary + Preview + Schema + Actions` 四段结构。

#### 顶部 Summary 带

固定显示：

- dataset name
- source
- rows
- columns
- created at / last updated
- dataset type
  - imported source
  - run result
  - saved table

#### 中部 Preview 表格

这是页面中心。

表格至少支持：

- 横向滚动
- 固定表头
- 前 N 行分页或虚拟滚动
- null 显示
- 简单排序

但 v1 不建议在这里加入复杂表格编辑能力。

#### 右侧 Schema 侧栏

固定显示：

- column name
- logical type
- nullable
- quick stats
  - null count
  - min/max if available

如果当前还没有 stats，这个区域至少应能显示类型和列名。

#### 页面动作

固定动作只有四类：

- Analyze
- Export
- Save as Table
- Re-import / Replace

不要在数据详情页放太多“高级动作”，否则用户会不知道主路径是什么。

### 3.3 数据详情页的两个关键信号

一个合格的数据详情页，必须让用户在 5-10 秒内回答这两个问题：

- 这份数据大概是什么
- 我下一步最适合点哪个动作

如果页面只是一张大表，那这两个问题都回答不好。

## 4. 分析数据：Analyze Workspace

### 4.1 分析页的目标

分析页不是写 SQL 的地方，而是“把一次分析从想法变成 run”的地方。

它必须支持两类用户：

- 会写 SQL 的用户
- 不想从零写 SQL 的用户

所以 v1 必须同时提供：

- `SQL Mode`
- `Guided Mode`

### 4.2 Analyze Workspace 的布局

建议固定为三栏：

- 左栏：数据与字段
- 中栏：分析配置
- 下栏：结果与解释

#### 左栏

固定包含：

- 当前 dataset selector
- 字段列表
- 类型分组
- quick templates

模板建议只放最常见的六类：

- Preview
- Filter
- Group By
- Aggregate
- Sort / Limit
- Vector Search

如果以后要加 stream-once，可以作为高级模板，而不是默认入口。

#### 中栏：SQL Mode

SQL Mode 至少应包含：

- SQL editor
- current dataset context
- run button
- explain button

执行前最好提供一段很轻的上下文提示：

- 当前表名
- 可用列
- 最近一次运行状态

#### 中栏：Guided Mode

Guided Mode 不应该做成复杂拖拽 DAG。

v1 更适合固定表单式：

- Select columns
- Filters
- Group by columns
- Aggregates
- Order by
- Limit

页面底部可以实时生成对应 SQL preview。

这样既避免“黑盒分析”，也避免用户一上来就手写 SQL。

### 4.3 下栏：结果区

结果区必须在一次 run 完成后稳定显示：

- status
- result preview
- row count
- schema
- explain summary

如果是长时任务，还应展示 progress。

结果区不能只显示“成功/失败”。

### 4.4 右侧动作

分析页右侧动作建议固定为：

- Run
- Cancel
- Export Result
- Save Result as Dataset
- Save Result as Table
- Compare with Previous Run

这几个动作里最关键的是最后三个，因为它们决定结果会进入哪个生命周期。

## 5. 结果出来之后怎么流转

一次分析执行结束后，用户通常有四种动作：

- 看一眼就走
- 导出
- 保存为 dataset
- 保存为 table

这四种动作必须清晰分开。

### 5.1 看一眼就走

这时只生成：

- `Run`
- `Artifact`

结果只存在于 run history 和 artifacts 中。

### 5.2 导出

导出是一次对象动作，不改变 app 内部对象层级。

导出后：

- run 仍然只是 run
- artifact 仍然只是 artifact

### 5.3 保存为 Dataset

适合：

- 这份结果还想继续做下一轮分析
- 但还不想把它提升为正式持久表

此时生成的是新的 `Dataset`。

### 5.4 保存为 Table

适合：

- 这是正式结果
- 需要长期持久化
- 后续可复开、重复使用

此时才进入 Velaria 内部分析型文件格式。

## 6. 三个页面之间如何串起来

最小闭环建议固定为：

```text
Import Wizard
  -> Data Detail
  -> Analyze Workspace
  -> Run Detail / Result Preview
  -> Save as Dataset or Save as Table
```

其中：

- `Import Wizard`
  - 把原始来源转成 `Dataset`
- `Data Detail`
  - 帮用户确认这份 dataset 可不可以分析
- `Analyze Workspace`
  - 把 dataset 转成 `Run`
- `Run Detail`
  - 帮用户判断结果是不是有价值
- `Save`
  - 决定结果进入临时生命周期还是正式生命周期

## 7. v1 页面上不应该做的事

### 7.1 导入页不做图表推荐

因为导入页的目标是确认数据，不是解释数据。

### 7.2 数据详情页不做在线编辑

因为当前内核和后续分析型存储格式都不是事务性行编辑系统。

### 7.3 分析页不做 DAG 拖拽

因为当前仓库的执行模型和用户目标都还没要求可视化 pipeline builder。

### 7.4 结果页不做全功能 dashboard

结果页的任务是验证与决策，不是发布报表。

## 8. 对后续交互设计的约束

只要上面三页结构成立，后续交互设计就要满足：

- 每一步都要有清晰的主动作
- 每一步都要有清晰的返回对象
- 页面间跳转不能绕过对象层

也就是说：

- 不能从导入直接跳到下载
- 不能从分析直接跳过 run 记录
- 不能从结果直接默认保存为 table

## 9. 最终结论

“如何导入、展示和分析数据”这三个问题，本质上对应 app v1 的三个核心页面：

- `Import Wizard`
- `Data Detail`
- `Analyze Workspace`

它们之间必须形成一个稳定闭环：

- 导入产生 `Dataset`
- 分析产生 `Run`
- 结果决定 `Export / Save as Dataset / Save as Table`

只要这个闭环先定稳，后面再讨论：

- Node / Python 服务怎么协作
- 具体 API 如何建模
- Velaria 自有文件格式如何承载 `Saved Table`

才会顺。
