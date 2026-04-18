# Velaria Agentic Search And Grounding v1

## 当前角色

这份文档是当前 agentic 方向中 search 与 grounding 的设计真源。

它回答的问题是：

- 为什么必须有模糊查询 / 匹配
- search 在 v1 里服务哪些对象
- grounding 为什么是 monitor 创建的强制前置步骤

## 为什么必须有模糊查询 / 匹配

如果没有模糊查询 / 匹配层，agent 在 Velaria 里会卡在三个地方：

- 找不到该用哪个模板
- 找不到与当前意图相似的历史事件
- 找不到应该绑定哪个 source、dataset 或 field

这会导致：

- monitor 创建高度依赖硬编码
- 重复发明相似规则
- 历史经验无法复用
- event 消费缺少“为什么这样建”的上下文

所以 search 在 v1 中不是附属功能，而是主路径能力。

## agent 查询优先级

v1 的 agent 查询优先级固定为：

```text
FocusEvent > Signal > Observation
```

含义是：

- 优先看值得处理的重点事件
- 解释时回看 signal
- 需要追证据时再回到 observation

## 检索目标

v1 固定支持四类检索目标：

- `templates`
- `events`
- `datasets`
- `fields`

其中优先级固定为：

- 第一优先级：`templates + events`
- 第二优先级：`datasets + fields`

原因：

- monitor 创建的主问题是找到相似模板和历史案例
- source / field 匹配是必要能力，但不是第一消费面

## 检索机制

v1 默认检索机制固定为：

- `keyword / BM25`
- `embedding`
- `hybrid ranking`

这意味着：

- 既保留精确词命中
- 也支持语义相似
- 最终返回统一排序结果

v1 不支持：

- 只用 embedding 作为唯一主路径
- 只用 keyword 作为唯一主路径
- 把检索逻辑藏在 monitor create 内部而不暴露成一等公民 API

## 检索结果对象

search 的统一结果对象是 `SearchHit`。

每个结果必须至少包含：

- `target_kind`
- `target_id`
- `title`
- `score`
- `score_breakdown`
- `match_reason`
- `matched_fields`
- `source_ref`

### match_reason

`match_reason` 必须显式区分：

- `keyword_match`
- `embedding_match`
- `hybrid_match`

这样 agent 才能知道：

- 为什么这个候选被排到前面
- 它是词面相似还是语义相似

## GroundingBundle

`GroundingBundle` 是 monitor 创建前的统一 grounding 结果。

它至少聚合：

- 模板候选
- 历史事件候选
- 数据集候选
- 字段候选

它的作用不是“自动替 agent 做决定”，而是：

- 给 agent 一个带理由的候选集
- 让 monitor 创建有可解释来源
- 让后续复盘知道“创建时参考了哪些相似案例”

对于 skill 而言，`GroundingBundle` 还有一个额外作用：

- 让 skill 的 monitor 创建成为可审计的标准调用，而不是隐式拼装

## 从自然语言意图到 monitor 的主路径

v1 的 monitor 创建主路径固定为：

1. 输入自然语言意图
2. 调 grounding/search
3. 返回模板和历史事件候选
4. 选择模板
5. 绑定 source / dataset / fields
6. 填充模板参数
7. 创建 monitor
8. validate
9. enable

这个流程有两条硬规则：

- 先 grounding，再生成 monitor
- 不能跳过检索直接自由生成

对 skill 的具体要求是：

- skill 必须显式调用 search / grounding API
- skill 必须把 grounding 结果作为 monitor 创建输入的一部分
- skill 不能只根据自身短期 memory 直接构造 monitor

## 模板与历史事件复用

### 模板复用

模板复用的目标是：

- 避免每次从零发明规则
- 把常见 monitor 形状收敛成受控模板族

v1 的模板族固定聚焦：

- 阈值
- 变化
- 排名
- 窗口聚合

### 历史事件复用

历史事件复用的目标是：

- 给 agent 参考“过去什么样的结果会升级成 FocusEvent”
- 给调试者参考“过去哪些场景有效”
- 给分析者参考“当前事件与历史相似案例的关系”

## v1 约束

- search 是一等公民 API，不是 monitor create 的隐藏副作用
- monitor 创建前必须 grounding
- `templates + events` 是第一优先级匹配对象
- `SearchHit` 必须带 `match_reason`
- grounding 结果只提供候选，不替 agent 做最终选择
- skill 使用 Velaria 创建 monitor 时，必须遵守 `search -> grounding -> create -> validate -> enable` 的标准顺序

## 非目标

v1 不做：

- 完全自由的自然语言到 monitor 端到端自动生成
- 不经过 grounding 的直接 monitor 创建主路径
- 多 source 复杂联邦检索
- 为所有内部对象都建立重量级长期索引
