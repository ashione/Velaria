# Velaria Agentic Monitor Execution v1

## 当前角色

这份文档是 monitor 执行语义的设计真源。

它回答的问题是：

- monitor 的统一模型是什么
- batch 和 stream 的关系是什么
- `external_event` 如何成为一等公民 source
- stream monitor 的硬边界是什么
- 规则 DSL 如何编译并进入 runtime

## 统一 monitor 模型

v1 只定义一套 `MonitorRecord`，通过 `execution_mode` 区分执行方式：

- `batch`
- `stream`

不拆分成两套产品对象的原因是：

- source、template、signal、focus event、run、artifact 都共享同一语义
- 真正变化的是执行器，不是用户心智上的主对象

## 规则 DSL 与 runtime 的关系

v1 中，monitor 的规则表达层采用结构化 DSL。

但 runtime 不直接执行 DSL 原文，而是执行编译产物：

```text
Rule DSL
  -> parse
  -> validate
  -> normalize
  -> compile
  -> CompiledRule + ExecutionSpec + PromotionRule + EventExtractionSpec + SuppressionRule
  -> runtime execute
```

这条链路的含义是：

- DSL 是表达层
- SQL / stream SQL 是计算子语言
- runtime 不承担第二套规则执行语义
- validate 和 compile 默认发生在 monitor create / validate 阶段

## 规则模型分层

v1 的 event 规则模型拆成以下几层：

- `source`
  - 看什么 observation
- `execution`
  - 以 batch 还是 stream 方式执行
- `signal`
  - 如何算出候选命中
- `promote`
  - 哪些 signal 升级成 FocusEvent
- `event`
  - FocusEvent 如何映射 title / summary / severity / key fields
- `suppress`
  - 如何做 cooldown / dedupe

这意味着：

- 不是“一条 SQL 就等于一个 event rule”
- SQL 主要负责 `signal`
- 事件升级与映射由结构化规则承担

## batch 执行模式

batch monitor 的标准语义是：

1. 在调度周期触发一次执行
2. 生成 `current_snapshot`
3. 读取 `previous_snapshot`
4. 执行模板编译后的 batch rule
5. 得到结果表
6. 结果表形成 `Signal`
7. 满足升级条件时，生成 `FocusEvent`

batch 适合：

- 本地文件
- saved dataset
- bitable 快照
- 周期性巡检或对账

在 batch 模式下，推荐规则形态是：

- `signal.query_sql`：对 `current_snapshot / previous_snapshot` 做比较
- `PromotionRule`：定义结果行如何升级为 FocusEvent
- `EventExtractionSpec`：定义结果如何映射为事件卡片
- `SuppressionRule`：定义冷却和去重

## stream 执行模式

stream monitor 的标准语义是：

1. 启动一个长期运行的 `StreamingQuery`
2. 从 stream source 持续读取 observation
3. 在 runtime 内做窗口、聚合、状态计算
4. 产出有限结果行
5. 用 `EventExtractionSpec` 把结果行映射成 `Signal`
6. 满足升级条件时，生成 `FocusEvent`

这里最关键的约束是：

- `stream` 是持续计算层
- 它不是事件对象本身
- `FocusEvent` 来自结果行映射，不直接来自无限状态

在 stream 模式下，推荐规则形态是：

- `signal.query_sql`：有限窗口 stream SQL
- `PromotionRule`：定义窗口结果行何时升级
- `EventExtractionSpec`：定义窗口结果如何生成 FocusEvent
- `SuppressionRule`：定义窗口内外的重复抑制

## `external_event` 一等公民 source

v1 中，`external_event` 是与 `saved_dataset / local_file / bitable` 并列的一等公民 source。

这意味着：

- monitor 可以直接绑定外部事件源
- 外部 agent 和外部系统都可以把 observation 写入 Velaria
- stream monitor 有真实的持续输入面

## `Webhook + custom JSON` 首批 adapter

v1 首批内置 adapter 固定为：

- `Webhook + custom JSON`

原因：

- 接入门槛最低
- 能覆盖最多种外部系统
- 最适合作为外部 agent 写入 observation 的统一入口

v1 不承诺：

- 大量第三方原生 connector
- 消息队列大全
- 第三方业务 API 全量接入

## 标准事件表

外部事件进入 Velaria 后，必须先落成标准事件表。

统一形态是：

- `StandardEventRow`

这意味着 monitor 绑定的不是某个第三方协议原语，而是：

- 标准化后的 observation 表

这样 batch 和 stream 都能复用同一查询和映射逻辑。

## `Observation -> Feature -> Signal -> FocusEvent`

v1 执行语义固定为：

```text
Observation
  -> Feature
    -> Signal
      -> FocusEvent
```

其中：

- `Observation`
  - 来自 source 的标准化输入
- `Feature`
  - 执行层内部的计算结果，不单独暴露
- `Signal`
  - 规则命中的候选判断
- `FocusEvent`
  - 满足升级条件后的重点事件

## Signal 升级条件

Signal 升级成 FocusEvent 的条件由模板定义，不依赖外部 agent 二次判定。

这条规则的意义是：

- 让运行语义稳定
- 让 FocusEvent 的产生可复现
- 让后续 agent 消费建立在确定输出上，而不是再次主观判断上

v1 推荐的升级表达方式是：

- 结构化条件树
- 必要时允许受控小表达式
- 不推荐把完整 promotion 逻辑写成自由 SQL

## cooldown

每条 rule 都有自己的 cooldown 行为。

默认语义：

- cooldown 内重复命中可以继续产生 run 和 artifact
- 但不重复升级为新的 FocusEvent

这可以同时保留：

- 运行证据
- 事件降噪

cooldown 与 dedupe 共同构成 `SuppressionRule`。

## validate / enable / error

monitor 的标准生命周期是：

```text
draft -> validated -> enabled -> running -> error
```

### validate

v1 至少校验：

- 模板参数
- source schema
- dry-run

### enable

通过 validate 后，monitor 必须显式 enable 才进入运行态。

### error

运行失败时默认行为：

- monitor 保持 enabled
- 状态进入 `error`
- 不自动静默 disable

## 证据链

无论 batch 还是 stream，FocusEvent 都必须绑定证据链。

最低证据链要求：

- `run_id`
- `artifact_ids`
- 结果预览

stream 场景下还允许带上：

- progress 线索
- checkpoint 线索
- 窗口边界

## stream monitor 的硬边界

v1 对 stream monitor 的硬边界如下。

### 只支持 `closed-window event extraction`

也就是说：

- 只有窗口闭合后产出的有限结果行，才能映射成 `Signal / FocusEvent`

### 默认 `processing_time`

v1 默认窗口时间语义是：

- `processing_time`

不要求 v1 先引入完整 watermark 体系。

### 默认 `drop_and_count`

迟到数据默认：

- 丢弃
- 计数
- 将计数暴露到运行态或事件上下文

### 明确禁止

以下执行形状不能作为 v1 的标准 monitor 输出面：

- 无界全局累计
- 不闭合窗口的持续状态
- 无界 `ORDER BY`
- 把无限状态直接映射成 FocusEvent

## 失败与恢复

### source 拉取失败

- batch：本次 run 失败并留下错误证据
- stream：query 进入错误态并留下进度 / checkpoint 线索

### checkpoint 恢复

stream monitor 允许基于现有 streaming contract 恢复，但恢复后输出仍必须遵守有限窗口边界。

### 连续失败

- monitor 进入 `error`
- 等待 agent 或人处理
- 不自动停用

### 重复事件抑制

- 通过 rule cooldown 控制
- 抑制 FocusEvent 噪声，不抑制运行证据

## 与 DSL 文档的关系

- [agentic-rule-dsl-v1.md](./agentic-rule-dsl-v1.md)
  - 定义 DSL 语法、表达层和编译目标
- 本文
  - 定义这些编译目标在 batch / stream monitor 中如何执行
