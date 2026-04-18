# Velaria Agentic Event Model v1

## 当前角色

这份文档是当前 agentic/event 方向的领域模型真源。

它负责：

- 定义一等公民对象
- 规定对象之间的关系
- 说明对象生命周期和状态迁移
- 写死 v1 的边界

它不负责：

- UI 交互
- API 路由清单
- 实施步骤

## 建模原则

v1 的建模原则固定为：

- 用通用抽象，不混用交易场景专属名词
- 优先建模消费对象，再建模内部执行对象
- 能在用户路径中找到位置的对象才是一等公民
- 同一语义只建一套主对象，不并列造第二套

主抽象链路固定为：

```text
Observation -> Feature -> Signal -> FocusEvent
```

## 对外暴露层级

v1 的对外暴露层级固定为：

- `FocusEvent` 显式
- `Signal` 半显式
- `Feature` 内部

这意味着：

- agent 主查询对象是 `FocusEvent`
- 需要解释时可查询 `Signal`
- `Feature` 只存在于执行与结果解释层，不做独立公共 API

## Observation 层模型

### DatasetRecord

角色：

- 表示可复用的数据集资产

核心字段：

- `dataset_id`
- `name`
- `source_kind`
- `source_spec`
- `materialized_path`
- `schema`
- `tags`
- `keyword_index_path`
- `embedding_dataset_path`
- `created_at`
- `updated_at`

关联对象：

- 可被 `SourceBinding` 引用
- 可参与 grounding 和 search

生命周期：

- created
- indexed
- updated
- archived

v1 边界：

- 允许作为 batch source
- 不要求成为所有 observation 的唯一承载方式

### SourceBinding

角色：

- 描述 monitor 绑定的观察来源

核心字段：

- `source_id`
- `kind`
- `spec`
- `refresh_mode`
- `fingerprint_fields`

关联对象：

- 被 `MonitorRecord` 使用
- 绑定到 `DatasetRecord` 或 `ExternalEventSource`

生命周期：

- created
- attached
- updated
- detached

v1 边界：

- `kind` 支持：
  - `saved_dataset`
  - `local_file`
  - `bitable`
  - `external_event`

### SourceSnapshot

角色：

- 描述一次标准化 observation 快照

核心字段：

- `snapshot_id`
- `source_id`
- `monitor_id`
- `dataset_id`
- `captured_at`
- `row_count`
- `schema`
- `artifact_id`
- `run_id`

关联对象：

- 被 batch monitor 用作 `current_snapshot / previous_snapshot`
- 可被 `FocusEventCard.context_json` 引用

生命周期：

- captured
- referenced
- superseded

v1 边界：

- batch 场景必须可用
- stream 场景只要求能引用标准化结果或检查点相关产物

### ExternalEventSource

角色：

- 描述外部事件入口

核心字段：

- `source_id`
- `kind`
- `name`
- `schema_binding_id`
- `auth_spec`
- `created_at`

关联对象：

- 被 `SourceBinding` 引用
- 由 `EventSchemaBinding` 解释其输入结构

生命周期：

- created
- active
- updated
- disabled

v1 边界：

- 首批只要求 `kind = webhook`
- 不是第三方系统连接器大全

### EventSchemaBinding

角色：

- 把外部 JSON 或原始 observation 映射成标准事件表

核心字段：

- `binding_id`
- `field_mappings`
- `time_field`
- `type_field`
- `key_field`
- `payload_projection`

关联对象：

- 服务于 `ExternalEventSource`
- 产出 `StandardEventRow`

生命周期：

- draft
- valid
- active
- superseded

v1 边界：

- 必须能生成统一查询字段
- 不要求覆盖任意复杂 JSON 语义

### StandardEventRow

角色：

- 标准事件表中的单行 observation

核心字段：

- `event_time`
- `event_type`
- `source_key`
- `payload_json`
- 扩展字段集合

关联对象：

- 被 batch / stream query 直接查询

生命周期：

- ingested
- queried
- referenced

v1 边界：

- 是外部事件进入执行层的统一形态
- 不是直接的 `FocusEvent`

## Template / Search 层模型

### TemplateRecord

角色：

- 规则生成的受控骨架

核心字段：

- `template_id`
- `name`
- `category`
- `description`
- `applies_to`
- `required_fields`
- `parameter_defs`
- `sql_template`
- `title_template`
- `summary_template`
- `severity_default`
- `tags`
- `version`

关联对象：

- 被 `GroundingBundle` 命中
- 被 `MonitorRecord` 选择
- 编译为 `CompiledRule`

生命周期：

- drafted
- published
- versioned
- deprecated

v1 边界：

- 是 monitor 生成的唯一合法骨架
- 不允许被绕过

### TemplateParamDef

角色：

- 描述模板参数的类型和校验规则

核心字段：

- `name`
- `kind`
- `required`
- `multi`
- `allowed_values`
- `default_value`
- `validation`

关联对象：

- 归属于 `TemplateRecord`
- 被 `MonitorRecord.template_params` 实例化

生命周期：

- created
- bound

v1 边界：

- 必须足够约束模板输入

### RuleSpec

角色：

- 规则 DSL 解析后的统一规则抽象

核心字段：

- `version`
- `name`
- `source`
- `execution`
- `signal`
- `promote`
- `event`
- `suppress`

关联对象：

- 由 `TemplateRecord` 和参数实例化得到
- 被编译为 `CompiledRule`
- 归一化为 `PromotionRule`、`EventExtractionSpec`、`SuppressionRule`

生命周期：

- parsed
- validated
- normalized
- compiled

v1 边界：

- 是 DSL 的内部统一抽象
- 不直接作为 runtime 原样执行对象

### GroundingBundle

角色：

- monitor 创建前的统一 grounding 结果

核心字段：

- `bundle_id`
- `query_text`
- `template_hits`
- `event_hits`
- `dataset_hits`
- `field_hits`
- `created_at`

关联对象：

- 由 search 生成
- 被 monitor create 使用

生命周期：

- created
- consumed
- expired

v1 边界：

- monitor 创建必须先获得 grounding 结果

### SearchHit

角色：

- search 返回的统一候选对象

核心字段：

- `target_kind`
- `target_id`
- `title`
- `score`
- `score_breakdown`
- `match_reason`
- `matched_fields`
- `source_ref`
- `snippet`

关联对象：

- 归属于 `GroundingBundle`

生命周期：

- emitted
- selected
- ignored

v1 边界：

- 必须返回 `match_reason`
- 必须说明命中的字段或来源

## Monitor / Execution 层模型

### MonitorRecord

角色：

- 监控定义本体

核心字段：

- `monitor_id`
- `name`
- `enabled`
- `intent_text`
- `source`
- `template_id`
- `template_params`
- `compiled_rules`
- `execution_mode`
- `interval_sec`
- `cooldown_sec`
- `tags`
- `created_at`
- `updated_at`

关联对象：

- 绑定 `SourceBinding`
- 选择 `TemplateRecord`
- 产出 `CompiledRule`
- 运行后关联 `MonitorState`

生命周期：

- draft
- validated
- enabled
- running
- error
- disabled
- archived

v1 边界：

- 统一支持 `batch | stream`
- 不拆成两套 monitor 产品对象

### CompiledRule

角色：

- 模板编译后的可执行规则

核心字段：

- `rule_id`
- `monitor_id`
- `template_id`
- `sql`
- `title_template`
- `summary_template`
- `severity`
- `output_mapping`
- `validation_status`
- `validation_error`

关联对象：

- 归属于 `MonitorRecord`
- 产出 `SignalRecord`

生命周期：

- compiled
- validated
- active
- invalid

v1 边界：

- 只能由模板编译得到
- 不支持自由规则作为主路径

### SignalSpec

角色：

- 描述候选命中如何计算

核心字段：

- `query_sql`
- `query_mode`
- `expected_output_fields`

关联对象：

- 归属于 `RuleSpec`
- 编译为 `CompiledRule`

生命周期：

- defined
- compiled

v1 边界：

- 以 SQL 作为计算子语言
- stream 结果必须是有限窗口输出

### PromotionRule

角色：

- 描述 signal 在什么条件下升级为 FocusEvent

核心字段：

- `condition_tree`
- `min_rows`
- `grouping_key`
- `promotion_mode`

关联对象：

- 归属于 `RuleSpec`
- 消费 `SignalRecord`
- 产出 `FocusEventCard`

生命周期：

- configured
- evaluated

v1 边界：

- 以结构化条件树为主
- 不以自由 SQL 作为默认主路径
- 升级条件由模板声明，不依赖外部 agent 二次决定

### BatchExecutionSpec

角色：

- 描述 batch monitor 的执行方式

核心字段：

- `snapshot_mode`
- `input_binding`
- `comparison_mode`
- `schedule`

关联对象：

- 被 `MonitorRecord` 在 batch 模式使用

生命周期：

- configured
- executed

v1 边界：

- 默认围绕 `current_snapshot / previous_snapshot`

### StreamExecutionSpec

角色：

- 描述 stream monitor 的执行方式

核心字段：

- `source_table`
- `query_sql`
- `trigger_interval_ms`
- `checkpoint_delivery_mode`
- `output_mode`
- `window`
- `late_data_policy`
- `event_extraction_spec`

关联对象：

- 被 `MonitorRecord` 在 stream 模式使用

生命周期：

- configured
- started
- running
- stopped

v1 边界：

- 只允许 `closed-window`
- 默认 `processing_time`
- 默认 `drop_and_count`

### EventExtractionSpec

角色：

- 描述结果行如何映射成 signal / focus event

核心字段：

- `signal_fields`
- `focus_promotion_condition`
- `event_title_mapping`
- `event_summary_mapping`
- `key_field_mapping`
- `sample_row_limit`

关联对象：

- 归属于 `StreamExecutionSpec` 或 `CompiledRule.output_mapping`

生命周期：

- configured
- applied

v1 边界：

- FocusEvent 必须来自结果行映射，不直接来自无限状态

### SuppressionRule

角色：

- 描述 FocusEvent 的去重与降噪策略

核心字段：

- `cooldown`
- `dedupe_by`
- `reopen_policy`

关联对象：

- 归属于 `RuleSpec`
- 与 `RuleState` 协同工作

生命周期：

- configured
- applied

v1 边界：

- v1 先只支持 cooldown 和 dedupe
- 不做复杂多级抑制策略

### MonitorState

角色：

- 描述 monitor 的运行时状态

核心字段：

- `monitor_id`
- `status`
- `last_run_at`
- `last_success_at`
- `last_snapshot_id`
- `last_error`
- `stream_query_id`
- `active_window_count`
- `last_emitted_window_end`
- `dropped_late_rows`

关联对象：

- 归属于 `MonitorRecord`

生命周期：

- idle
- running
- error
- disabled

v1 边界：

- 运行失败时默认进入 `error`
- 不自动静默 disable

### RuleState

角色：

- 描述 rule 级别的触发与 cooldown 状态

核心字段：

- `rule_id`
- `monitor_id`
- `last_triggered_at`
- `cooldown_until`
- `consecutive_hits`

关联对象：

- 归属于 `CompiledRule`

生命周期：

- idle
- hit
- cooldown

v1 边界：

- cooldown 期间可继续记 run，但不重复升级 FocusEvent

## Event / Consumption 层模型

### SignalRecord

角色：

- 表示规则命中的候选判断结果

核心字段：

- `signal_id`
- `monitor_id`
- `rule_id`
- `created_at`
- `result_rows`
- `reason_summary`
- `run_id`
- `artifact_ids`

关联对象：

- 由 `CompiledRule` 生成
- 可升级为 `FocusEventCard`

生命周期：

- emitted
- queried
- promoted
- expired

v1 边界：

- 可查询
- 不作为主操作对象

### FocusEventCard

角色：

- 真正值得 agent 或人消费的重点事件

核心字段：

- `event_id`
- `monitor_id`
- `rule_id`
- `triggered_at`
- `severity`
- `title`
- `summary`
- `key_fields`
- `sample_rows`
- `status`
- `run_id`
- `artifact_ids`
- `context_json`

关联对象：

- 由 `SignalRecord` 升级得到
- 通过 `run_id / artifact_ids` 关联证据层

生命周期：

- new
- seen
- consumed
- archived

v1 边界：

- 是主消费对象
- 升级条件由模板声明，不依赖外部 agent 二次决定

### NotificationRecord

角色：

- 记录事件通知投递结果

核心字段：

- `notification_id`
- `event_id`
- `channel`
- `sent_at`
- `status`
- `error`

关联对象：

- 归属于 `FocusEventCard`

生命周期：

- pending
- sent
- failed

v1 边界：

- v1 只支持 `desktop`

### EventCursor

角色：

- 支持外部 agent 增量拉取事件

核心字段：

- `consumer_id`
- `last_seen_event_id`
- `last_seen_at`

关联对象：

- 被 FocusEvent poll 使用

生命周期：

- created
- advanced
- reset

v1 边界：

- pull-first，不做 push-first 编排

## 模型关系图

```text
ExternalEventSource / DatasetRecord
  -> SourceBinding
    -> SourceSnapshot / StandardEventRow
      -> MonitorRecord
        -> CompiledRule
          -> SignalRecord
            -> FocusEventCard
              -> NotificationRecord

TemplateRecord
  -> TemplateParamDef
  -> RuleSpec
  -> GroundingBundle
    -> SearchHit
  -> MonitorRecord

FocusEventCard
  -> Run / Artifact
  -> EventCursor
```

## 生命周期与状态迁移

### Monitor

```text
draft -> validated -> enabled -> running -> error
  \-> disabled
```

### Signal

```text
emitted -> queried -> promoted
```

### FocusEvent

```text
new -> seen -> consumed -> archived
```

### Notification

```text
pending -> sent
pending -> failed
```

## v1 约束

- `Observation -> Feature -> Signal -> FocusEvent` 是统一主抽象
- `Feature` 不做独立公共对象
- `FocusEventCard` 是一等公民
- `SignalRecord` 半显式，只用于解释和调试
- `MonitorRecord` 必须先 grounding、再创建、再 validate、再 enable
- `StreamExecutionSpec` 只支持有限窗口输出
