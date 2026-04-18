# Velaria Agentic User Journeys v1

## 当前角色

这份文档是当前 agentic 方向的用户使用路径真源。

它回答的问题是：

- 谁从哪里进入 Velaria
- 他们要完成什么任务
- 过程里会创建和消费哪些对象
- 失败时如何恢复

这份文档优先于对象模型细节，因为：

- 只有先把真实路径写清楚，才能知道哪些对象真的是一等公民
- 无法在路径中找到位置的对象，不应该在 v1 被过度建模

## 为什么先写用户路径

Velaria 当前讨论的对象很多：

- source
- dataset
- template
- monitor
- signal
- focus event
- run
- artifact
- search hit

如果不先回答“谁怎么用”，这些对象很容易各自成立，却无法形成闭环。

所以 v1 先固定三条路径：

- 外部 agent / skill 主路径
- 配置者 / 开发者路径
- 接收者 / 分析者路径

## 共同主线

三条路径必须共享同一条主线：

```text
Observe
  -> Ground
    -> Create Monitor
      -> Validate
        -> Enable
          -> Execute
            -> Signal
              -> FocusEvent
                -> Run / Artifact
                  -> Consume / Analyze
```

这条主线的强约束是：

- monitor 创建前必须 grounding
- validate 通过后才能 enable
- 事件消费从 `FocusEvent` 开始，不从原始 observation 开始
- 所有重点事件都要能回到 `Run / Artifact`

## 路径一：外部 agent / skill

### 角色

外部 agent / skill 是 v1 的首要消费者和主要操作者。

在这条路径中：

- `agent` 表示有较强自主决策能力的调用方
- `skill` 表示更窄、更可复用的调用单元

两者在 v1 中共享同一套 Velaria 调用顺序，只是决策宽度不同。

### 起点

用户给 agent 一个自然语言意图，例如：

- “观察这个事件流里是否出现连续异常”
- “监控这份 source，在窗口内发现需要升级处理的重点事件”

### 目标

让 agent：

- 找到合适的模板和历史案例
- 创建一个可运行的 monitor
- 拉取后续 FocusEvent
- 打开证据链继续分析或执行后续动作

让 skill：

- 以稳定对象而不是临时脚本的方式使用 Velaria
- 把“创建 monitor”和“消费重点事件”变成标准可复用流程

### 前置条件

- Velaria sidecar 已启动
- 至少有一个可用 source
- search / grounding 接口可用

### 步骤 1：grounding

agent 先调用 search / grounding 接口，检索：

- 相似 `Template`
- 相似 `FocusEvent`
- 可用 `Dataset / Field / Source`

本步产出对象：

- `SearchHit`
- `GroundingBundle`

skill 在这一步的最小调用要求是：

- 不能跳过 grounding 直接创建 monitor
- 必须至少检索 `templates` 和 `events`

### 步骤 2：模板选择

agent 基于 grounding 结果选择一个模板族，并填入参数。

本步产出对象：

- `TemplateRecord`
- `TemplateParamDef` 的具体填充值

### 步骤 3：monitor create

agent 提交：

- `intent_text`
- `grounding_bundle`
- `template_id`
- `template_params`
- `source_binding`
- `execution_mode`

Velaria 负责生成 monitor 定义和编译后的规则。

本步产出对象：

- `MonitorRecord`
- `CompiledRule`

skill 在这一步的职责是：

- 把用户意图映射到模板参数
- 显式提交 source、execution mode 和 grounding 结果
- 不直接操作内部存储文件

### 步骤 4：validate

monitor 创建后必须先 validate，至少检查：

- 模板参数是否合法
- source schema 是否满足模板要求
- dry-run 是否能产出合法结果

本步产出对象：

- `MonitorState`
- 校验结果

skill 在这一步不能偷跳：

- 不允许“创建即启用”作为默认主路径
- 必须根据 validate 结果决定是否继续 enable

### 步骤 5：enable

validate 通过后，agent 显式启用 monitor。

本步产出对象：

- 进入 `enabled` 状态的 `MonitorRecord`

### 步骤 6：event poll

agent 使用 cursor 拉取新产生的 `FocusEvent`。

本步产出对象：

- `FocusEventCard`
- `EventCursor`

skill 的标准消费顺序固定为：

1. poll `FocusEvent`
2. 读取 `context_json`
3. 必要时读取 `SignalRecord`
4. 再按 `run_id / artifact_ids` 追证据

### 步骤 7：run / artifact consume

agent 拿到事件后，继续读取：

- `run_id`
- `artifact_ids`
- preview
- 相关 signal

本步产出对象：

- `Run`
- `Artifact`
- `SignalRecord`

skill 在这一步的目标不是重新做事件判定，而是：

- 解释为什么触发
- 组织后续动作
- 复用历史上下文

### 步骤 8：事件状态推进

agent 在消费完成后推进事件状态：

- `consumed`
- `archived`

### 每一步产出对象

- grounding：`SearchHit`, `GroundingBundle`
- 模板选择：`TemplateRecord`
- create：`MonitorRecord`, `CompiledRule`
- validate：`MonitorState`
- enable：可运行 monitor
- poll：`FocusEventCard`, `EventCursor`
- consume：`Run`, `Artifact`, `SignalRecord`

### skill 的最小调用协议

外部 skill 在 v1 里最少应遵守下面的调用顺序：

```text
search / grounding
  -> monitor create
    -> monitor validate
      -> monitor enable
        -> focus-event poll
          -> signal / run / artifact read
            -> event consume / archive
```

这条顺序的意义是：

- 避免 skill 绕过系统主路径
- 确保 monitor 创建可解释
- 确保事件消费有证据链

### 成功完成态

一条从自然语言意图到可运行 monitor，再到可消费 `FocusEvent` 的链路闭环完成。

### 常见失败与恢复

- grounding 结果太弱
  - 调整 query_text 或补充 source 限定再检索
- 模板参数不匹配
  - 回到模板选择阶段重新填参
- validate 失败
  - 保留草案 monitor，修正 source 或参数后重试
- monitor 连续失败
  - 进入 `error` 状态，agent 或人检查 `Run / Artifact / progress`
- poll 为空
  - 保持 cursor 不动，等待下一次轮询

## 路径二：配置者 / 开发者

### 角色

配置者 / 开发者负责把新的 source、schema binding、模板可执行性接入系统。

### 起点

- 有新 source 要接入
- 有新的 external event webhook 要接入
- monitor 在 validate 或运行中失败

### 目标

让 source 能被标准化接入，让 monitor 能稳定执行。

### 前置条件

- 有 sidecar 和本地存储权限
- 能查看运行状态和错误输出

### 步骤 1：接入 source

创建或更新：

- `SourceBinding`
- `ExternalEventSource`

### 步骤 2：定义 schema binding

如果是外部事件，需要把输入映射成标准事件表。

本步产出对象：

- `EventSchemaBinding`
- `StandardEventRow`

### 步骤 3：预览标准化结果

检查标准事件表是否具备模板需要的字段和时间语义。

本步产出对象：

- `SourceSnapshot`

### 步骤 4：校验模板适配性

确认模板参数和 source schema 匹配。

### 步骤 5：dry-run monitor

在不正式启用前做 dry-run，观察：

- batch 结果
- stream 结果
- window 输出
- cooldown 行为

本步产出对象：

- `Run`
- `Artifact`
- `MonitorState`

### 步骤 6：查看失败与恢复信息

如果失败，需要查看：

- `MonitorState`
- `Run`
- `Artifact`
- stream progress / checkpoint 线索

### 每一步产出对象

- 接入：`SourceBinding`, `ExternalEventSource`
- schema：`EventSchemaBinding`, `StandardEventRow`
- 预览：`SourceSnapshot`
- dry-run：`Run`, `Artifact`, `MonitorState`

### 成功完成态

新的 source 或 schema binding 可被 monitor 稳定消费。

### 常见失败与恢复

- schema 不匹配
  - 修正 binding，再次生成标准事件表
- dry-run 无结果
  - 回看模板升级条件和 source 数据粒度
- stream 不闭合
  - 回到执行设计，确认窗口和 emit 边界

## 路径三：接收者 / 分析者

### 角色

接收者 / 分析者不负责创建 monitor，而负责理解事件、追证据、复盘历史。

### 起点

- 收到一个 `FocusEvent`
- 或收到桌面提醒后打开相关事件

### 目标

快速回答：

- 为什么会触发
- 证据是什么
- 和过去哪些事件相似
- 后续要不要进一步动作

### 前置条件

- 事件已存在
- 对应 run / artifact 可访问

### 步骤 1：查看 FocusEvent

先从 `FocusEventCard` 看：

- title
- summary
- key fields
- severity

### 步骤 2：查看对应 Signal

如果需要解释“为什么升级”，再查看 `SignalRecord`。

### 步骤 3：追到 Run / Artifact

查看：

- result preview
- source snapshot
- 相关 artifact

### 步骤 4：搜索历史相似事件

通过 search 找到：

- 相似 FocusEvent
- 相似模板
- 相似 signal 输出

### 步骤 5：决定后续动作

后续动作不在 Velaria v1 内闭环，可以是：

- 继续分析
- 交给外部 agent
- 人工处理

### 每一步产出对象

- 起点：`FocusEventCard`
- 解释：`SignalRecord`
- 证据：`Run`, `Artifact`
- 复用：`SearchHit`

### 成功完成态

完成一次从事件到证据到历史相似案例的复盘闭环。

### 常见失败与恢复

- 事件信息不足
  - 回查 signal 和 artifact
- artifact 预览不够
  - 追到完整 run 结果
- 找不到相似案例
  - 调整 query_text 或扩大检索范围

## 路径间共享对象

三条路径共享但角色不同的对象如下：

- `TemplateRecord`
  - agent 用于创建
  - 开发者用于维护
- `MonitorRecord`
  - agent 用于创建和启用
  - 开发者用于验证和调试
- `SignalRecord`
  - 开发者和分析者用于解释
- `FocusEventCard`
  - agent 和分析者的主消费对象
- `Run / Artifact`
  - 所有路径的证据层

其中对 skill 最重要的共享对象是：

- `GroundingBundle`
- `MonitorRecord`
- `FocusEventCard`
- `SignalRecord`
- `Run / Artifact`

## 失败点与恢复模式

系统级常见失败点包括：

- grounding 候选不足
- 模板与 source schema 不匹配
- validate 失败
- enabled monitor 运行失败
- stream query 无法稳定闭窗
- cursor 消费中断

v1 的默认恢复模式是：

- monitor 保留但进入 `error`
- 不自动静默 disable
- 错误定位从 `MonitorState -> Run -> Artifact` 逐层展开
- 事件消费从 cursor 继续，而不是重新扫全量 observation

## v1 不覆盖的路径

v1 暂不覆盖：

- 多人协作审批式 monitor 管理
- 外部 webhook / Slack / Feishu 的多通道通知闭环
- 自由 SQL 直接生成 monitor
- 多 source 联合分析主路径
- 外部 agent 自动修改模板定义
