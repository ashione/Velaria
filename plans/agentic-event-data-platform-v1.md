# Velaria Agentic Event Data Platform v1

## 当前角色

这份文档是当前 `agentic / event / monitor / search / stream` 方向的总设计入口。

它回答的问题是：

- Velaria 在这个方向上的本体到底是什么
- 为什么它不是普通规则监控系统
- 为什么它不是普通 agent memory
- 核心对象和主链路应该怎样理解

它不负责：

- 逐字段模型定义
- API 字段表
- UI 细节
- 实施顺序细节

这些细节由配套子文档承接。

## 一句话定义

Velaria v1 是一个面向外部 agent / skill 的本地事件型数据底座，用来把零散的 observation 持续转成可检索、可追溯、可继续消费的 `FocusEvent + Evidence Chain`。

## 核心问题

### 为什么 agent 的 memory / state 不够

当前很多 agent 已经有：

- 文本型 memory
- 任务型 state

但它们在数据处理上仍然常见这些问题：

- 数据入口零散：文件、webhook、表格、流式输入各走各路
- 处理过程零散：临时脚本、临时 SQL、临时逻辑无法复用
- 结果零散：本次判断与上次判断之间缺少统一对象模型
- 证据零散：很难回答“为什么这次触发”和“这次与上次有何不同”
- 查询零散：没有统一的模糊检索层去找模板、历史事件、可用 source

换句话说，agent 通常能“记住说过什么”和“知道做到哪一步”，但很难稳定地“知道观察到了什么、算出了什么、为什么该升级成一个重点事件”。

### 为什么需要事件型数据底座

Velaria 要解决的不是“再做一个规则引擎”，而是把以下链路变成统一系统：

```text
Observation
  -> Feature
    -> Signal
      -> FocusEvent
        -> Run / Artifact / Preview / Search
```

这条链路解决的核心问题是：

- 让 agent 有稳定的数据对象，而不是一堆临时文件
- 让重点判断有证据链，而不是只有一句提示
- 让历史案例可复用，而不是每次重新猜测
- 让 stream 与 batch 都能成为 monitor 的执行模式，而不是割裂成两套系统

## 产品定位

Velaria v1 的产品定位固定为：

- 一个面向外部 agent / skill 的本地事件型数据底座
- 一个把外部事件、快照数据、模板规则、检索 grounding、运行证据串起来的本地 sidecar
- 一个以 `FocusEvent` 为主消费对象、以 `Run + Artifact` 为证据链的系统

它首先服务的是：

- 外部 agent / skill
- 需要把 observation 提炼成重点事件的人或系统

它不是首先服务于：

- BI 仪表盘用户
- 通用数据仓库用户
- 传统告警平台的多租户运维团队

## Skill 使用方式

v1 中的 `skill` 不是 Velaria 内部的执行内核，而是 Velaria 的主要上层消费者之一。

skill 在系统中的标准角色是：

- 接收用户的自然语言目标或任务上下文
- 调用 Velaria 的 grounding / search 能力
- 选择模板和 source
- 创建、校验、启用 monitor
- 持续轮询 `FocusEvent`
- 再把事件、signal、run、artifact 作为后续动作输入

换句话说，skill 与 Velaria 的关系固定为：

```text
User intent
  -> Skill
    -> Velaria search / grounding / monitor / focus-event APIs
      -> Skill follow-up action
```

这意味着：

- Velaria 不替 skill 做自然语言理解
- skill 不绕过 Velaria 直接拼接内部数据对象
- Velaria 提供稳定对象和证据链
- skill 提供意图理解、模板选择、后续动作编排

v1 下，skill 的标准使用方式是：

- 先查，再建
- 先 validate，再 enable
- 先 poll `FocusEvent`，再按需追 `Signal / Run / Artifact`

不建议的 skill 使用方式包括：

- 不经过 grounding 直接创建 monitor
- 直接用自由 SQL 代替模板
- 把原始 observation 当成主要消费对象
- 只看通知文案，不读取证据链

## 非目标

v1 明确不做：

- 自由 SQL-first 的通用规则平台
- 多租户、多团队协作监控平台
- push-first 的自动编排系统
- “任何持续查询都能直接报警”的无边界 stream 系统
- 把无界全局累计状态直接暴露成标准事件输出
- 把所有原始 observation 都抬成长期主对象

## 核心对象总览

v1 的核心对象分层如下：

- `Observation`
  - 原始输入事实
  - 例如 webhook 事件、本地文件快照、表格导入结果、流式 source
- `Feature`
  - 从 observation 计算出的中间特征
  - v1 不作为一等公民对象对外暴露
- `Signal`
  - 规则或模板命中的候选判断
  - 可查询、半显式
- `FocusEvent`
  - 真正值得 agent 或人消费的重点事件
  - 是一等公民对象
- `Run / Artifact`
  - 事件的执行证据链
- `Template`
  - 受控规则骨架
- `SearchHit / GroundingBundle`
  - monitor 创建前的统一检索与 grounding 结果

对外暴露层级固定为：

- `FocusEvent` 显式
- `Signal` 半显式
- `Feature` 内部

## 主链路

Velaria v1 的主链路固定为：

```text
Natural-language intent
  -> grounding
    -> template selection
      -> compiled rule
        -> batch/stream execution
          -> signal result
            -> focus event promotion
              -> run/artifact evidence
                -> agent consumption
```

这条链路的关键约束是：

- monitor 创建前必须先 grounding
- template 是规则生成的唯一合法骨架
- execution 统一支持 `batch | stream`
- `FocusEvent` 只能来自结果行映射，不能直接来自无限状态
- 每个重点事件都必须能回链到 `run + artifact`

## 执行模式

### batch

batch 模式用于：

- 周期性快照检查
- `current_snapshot / previous_snapshot` 对比
- 单次查询输出结果表

它适合：

- 周期性导出的表格
- 本地文件
- saved dataset
- 补充性、对账型、巡检型 observation

### stream

stream 模式用于：

- 持续流入的 external event
- 持续计算与窗口化聚合
- 在有限窗口闭合后产出可消费结果行

它的角色是：

- 持续计算层
- 不是最终事件对象本身

stream monitor 的硬边界是：

- 只支持 `closed-window event extraction`
- 默认 `processing_time`
- 默认 `drop_and_count`
- 禁止把无界全局累计作为标准 monitor 输出面

## 外部事件 source

`external_event` 是 v1 的一等公民 source。

这意味着：

- Velaria 不是只观察本地文件和表格
- 外部系统、外部 agent、本地 agent 都可以把 observation 写入 Velaria
- 进入 Velaria 的外部事件必须先标准化成事件表，再进入 batch/stream 执行链

v1 首批内置 adapter 固定为：

- `Webhook + custom JSON`

这条路径的意义在于：

- 降低外部系统接入门槛
- 让 stream monitor 有真实输入面
- 让 agent 可以把外部 observation 写入同一底座

## 与普通规则监控系统的区别

Velaria v1 与普通规则监控系统的根本差别不在于“规则更复杂”，而在于对象模型和消费方式不同。

普通规则监控系统通常是：

```text
rule -> alert
```

Velaria v1 是：

```text
intent -> grounding -> template -> signal -> focus event -> run/artifact evidence
```

具体差别体现在：

- 主对象不是 `alert`，而是 `FocusEvent + Evidence Chain`
- 规则生成前有 grounding，不允许跳过检索自由生成
- 输出不是终点，而是外部 agent 后续动作的起点
- 历史不是简单告警日志，而是可搜索、可复用的事件与运行证据
- stream 是持续计算层，而不是“连续发告警”的黑箱

## 与普通 agent memory 的区别

普通 agent memory 更像：

- 文本事实记忆
- 任务上下文记忆
- 会话状态缓存

Velaria v1 更像：

- 结构化 observation 层
- 可计算的 signal 层
- 可行动的 focus event 层
- 可复盘的 run/artifact 证据层

前者解决“记住发生过什么”，后者解决“为什么这件事值得处理，以及如何继续处理”。

## 文档导航

这份总设计由以下文档展开：

- [agentic-user-journeys-v1.md](./agentic-user-journeys-v1.md)
  - 三类角色的主使用路径，以及 skill 的标准调用顺序
- [agentic-event-model-v1.md](./agentic-event-model-v1.md)
  - 领域模型、关系和生命周期
- [agentic-search-grounding-v1.md](./agentic-search-grounding-v1.md)
  - 模糊查询、混合检索、skill grounding 和 monitor 创建主路径
- [agentic-monitor-execution-v1.md](./agentic-monitor-execution-v1.md)
  - batch / stream / external event 的执行语义与边界
- [agentic-rule-dsl-v1.md](./agentic-rule-dsl-v1.md)
  - 事件规则 DSL、语法、校验、编译与运行时关系
