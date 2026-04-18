# Velaria Agentic Rule DSL v1

## 当前角色

这份文档定义 Velaria v1 的规则 DSL。

它回答的问题是：

- event 规则模型应该如何表达
- DSL 与 SQL、stream SQL、runtime 的关系是什么
- DSL 的语法是什么
- DSL 如何被校验、编译和执行

它不负责：

- 重新定义执行内核
- 定义第二套 SQL 语义
- 直接替代已有的 batch / stream runtime contract

## 为什么需要 DSL

如果只用自由 SQL 表达 event 规则，会出现这些问题：

- 太自由，不利于模板约束
- 不利于 agent 稳定生成
- 不利于 schema 校验和 explain
- 事件升级、映射、去重、cooldown 会散落到多处隐式逻辑

如果完全不用 SQL，则会浪费 Velaria 已有的：

- batch SQL
- stream SQL
- window / aggregate
- progress / checkpoint

所以 v1 的规则表达层固定为：

- **结构化 DSL** 负责描述事件语义
- **SQL / stream SQL** 作为计算子语言
- **runtime** 只执行编译后的产物，而不直接把 DSL 当成第二套执行引擎

## 设计原则

v1 DSL 必须满足：

- agent 易生成
- 人易读
- schema 易校验
- 可编译到现有 batch / stream 执行面
- 能显式表达 signal、promotion、event extraction、suppression

v1 不追求：

- 完整可编程语言
- 自定义文本解析器
- 任意嵌套脚本逻辑

## DSL 与 runtime 的关系

DSL 不是直接被内核原样执行，而是走下面的链路：

```text
Rule DSL
  -> parse
  -> validate
  -> normalize
  -> compile
  -> CompiledRule + ExecutionSpec + PromotionRule + EventExtractionSpec + SuppressionRule
  -> runtime execute
```

这意味着：

- DSL 是表达层
- runtime 执行的是编译结果
- 编译器可以在 monitor create / validate 阶段运行
- runtime 不需要理解整套 DSL 文本语法

v1 推荐模式固定为：

- 创建 monitor 时编译
- enable 前 validate
- 运行时执行编译产物
- source schema 变化时触发 revalidate / recompile

## 顶层语法

DSL 推荐使用：

- YAML
- 或等价 JSON

顶层结构固定为：

```yaml
version: v1
name: string
source: SourceSpec
execution: ExecutionSpec
signal: SignalSpec
promote: PromotionRule
event: EventExtractionSpec
suppress: SuppressionRule
```

六段语义分别是：

- `source`
  - 看什么 observation
- `execution`
  - 以 batch 还是 stream 方式执行
- `signal`
  - 如何产出候选命中
- `promote`
  - 什么条件下升级成 FocusEvent
- `event`
  - 如何把结果行映射成事件卡片
- `suppress`
  - 如何做 cooldown / dedupe

## DSL 完整示例

### stream 示例

```yaml
version: v1
name: burst-events
source:
  kind: external_event
  binding: market_ticks

execution:
  mode: stream
  window:
    kind: tumbling
    time_semantics: processing_time
    size: 60s

signal:
  sql: |
    SELECT
      source_key,
      event_type,
      COUNT(*) AS cnt
    FROM input_stream
    GROUP BY TUMBLE(event_time, INTERVAL '60' SECOND), source_key, event_type
    HAVING cnt >= 5

promote:
  when:
    all:
      - field: cnt
        op: ">="
        value: 5

event:
  severity:
    rules:
      - when: "cnt >= 20"
        level: critical
      - when: "cnt >= 5"
        level: warning
  title: "{source_key} 高频事件"
  summary: "event_type={event_type}, cnt={cnt}"
  key_fields: [source_key, event_type, cnt]
  sample_rows: 5

suppress:
  cooldown: 300s
  dedupe_by: [source_key, event_type]
```

### batch 示例

```yaml
version: v1
name: abnormal-delta
source:
  kind: saved_dataset
  binding: daily_snapshot

execution:
  mode: batch
  compare:
    current: current_snapshot
    previous: previous_snapshot

signal:
  sql: |
    SELECT
      c.account_id,
      c.amount AS current_amount,
      p.amount AS previous_amount,
      c.amount - p.amount AS delta
    FROM current_snapshot c
    JOIN previous_snapshot p
      ON c.account_id = p.account_id
    WHERE ABS(c.amount - p.amount) >= 1000

promote:
  when:
    any:
      - field: delta
        op: ">="
        value: 5000
      - field: delta
        op: "<="
        value: -5000

event:
  severity:
    rules:
      - when: "ABS(delta) >= 10000"
        level: critical
      - when: "ABS(delta) >= 5000"
        level: warning
  title: "账户 {account_id} 金额异常变化"
  summary: "current={current_amount}, previous={previous_amount}, delta={delta}"
  key_fields: [account_id, current_amount, previous_amount, delta]
  sample_rows: 3

suppress:
  cooldown: 1h
  dedupe_by: [account_id]
```

## 分段语法细节

### `source`

```yaml
source:
  kind: saved_dataset | local_file | bitable | external_event
  binding: string
```

规则：

- `kind` 必填
- `binding` 必填
- v1 不在 DSL 中承载复杂 connector 配置；复杂配置由 source 注册层负责

### `execution`

#### batch

```yaml
execution:
  mode: batch
  compare:
    current: current_snapshot
    previous: previous_snapshot
```

#### stream

```yaml
execution:
  mode: stream
  window:
    kind: tumbling | sliding
    time_semantics: processing_time
    size: 60s
    slide: 10s
```

v1 约束：

- `mode` 必填
- stream 只允许有限窗口
- `time_semantics` 固定为 `processing_time`
- 不允许无界全局窗口

### `signal`

```yaml
signal:
  sql: |
    SELECT ...
```

规则：

- `signal.sql` 是计算子语言
- batch 走 batch SQL
- stream 走 stream SQL
- stream SQL 必须产出有限结果行

### `promote`

promotion 不推荐用完整自由 SQL，而采用结构化条件树。

#### 基本语法

```yaml
promote:
  when:
    all:
      - field: cnt
        op: ">="
        value: 5
```

支持：

- `all`
- `any`
- `not`

#### 复合示例

```yaml
promote:
  when:
    any:
      - field: score
        op: ">="
        value: 0.9
      - all:
          - field: count
            op: ">="
            value: 10
          - field: ratio
            op: ">"
            value: 0.2
```

#### 运算符

v1 只支持：

- `==`
- `!=`
- `>`
- `>=`
- `<`
- `<=`
- `in`
- `not_in`

### `event`

#### title / summary

```yaml
event:
  title: "{source_key} 出现重点事件"
  summary: "cnt={cnt}, score={score}"
```

#### severity

```yaml
event:
  severity:
    rules:
      - when: "cnt >= 20"
        level: critical
      - when: "cnt >= 5"
        level: warning
```

#### 其他字段

```yaml
event:
  key_fields: [source_key, event_type, cnt]
  sample_rows: 5
```

### `suppress`

```yaml
suppress:
  cooldown: 300s
  dedupe_by: [source_key, event_type]
```

v1 先只做：

- `cooldown`
- `dedupe_by`

## 小型表达式子语法

v1 允许一个很小的表达式语法，用于：

- `event.severity.rules[].when`
- 未来少量映射条件

### 示例

```text
cnt >= 20
ABS(delta) >= 5000
score >= 0.8 AND rank <= 3
```

### 支持范围

- 字段名
- 数字 / 字符串字面量
- `AND` / `OR` / `NOT`
- 比较运算符
- 极少数内建函数：
  - `ABS`
  - `LOWER`
  - `UPPER`

### 不支持

- 用户自定义函数
- 任意嵌套脚本
- 完整编程表达式系统

## 抽象语法树

DSL 解析后应落成统一 AST，而不是直接把 YAML 透传给 runtime。

推荐 AST 节点：

- `RuleSpec`
- `SourceSpec`
- `ExecutionSpec`
- `SignalSpec`
- `PromotionSpec`
- `EventSpec`
- `SuppressSpec`

这样可以做到：

- 结构化校验
- 编译缓存
- schema 变更时局部重编译

## 校验阶段

v1 至少做三层校验。

### 1. 结构校验

- 顶层字段是否齐全
- DSL schema 是否合法
- 必填参数是否存在

### 2. source / template 校验

- source 是否存在
- source schema 是否满足模板要求
- batch / stream 模式是否合法

### 3. 执行合法性校验

- SQL 是否可解析
- stream 是否为有限窗口
- event 映射字段是否在 signal 结果中存在
- suppression 字段是否可用

## 编译产物

DSL 编译后的最小执行产物应固定为：

- `CompiledRule`
- `BatchExecutionSpec` 或 `StreamExecutionSpec`
- `PromotionRule`
- `EventExtractionSpec`
- `SuppressionRule`

也就是说：

- SQL 负责编译成可执行查询
- 事件语义负责编译成结构化 runtime config

## runtime 行为

runtime 不直接执行 DSL 原文，而执行编译产物：

- batch：执行 batch SQL
- stream：执行 stream SQL
- 结果行先形成 `Signal`
- 再按 `PromotionRule` 升级成 `FocusEvent`
- 再按 `SuppressionRule` 做冷却和去重

## v1 非目标

v1 不做：

- 自定义文本 DSL 解析器
- 让 runtime 直接原样执行 DSL 文本
- 全部语义都用自由 SQL 表达
- 复杂脚本化 rule engine

## 与其他文档的关系

- [agentic-event-model-v1.md](./agentic-event-model-v1.md)
  - 定义 `RuleSpec / PromotionRule / SuppressionRule / EventExtractionSpec` 等对象
- [agentic-monitor-execution-v1.md](./agentic-monitor-execution-v1.md)
  - 定义 DSL 编译后的 batch / stream 执行边界
- [agentic-search-grounding-v1.md](./agentic-search-grounding-v1.md)
  - 定义 agent 如何通过模板和 grounding 选择合适的 DSL 规则骨架
