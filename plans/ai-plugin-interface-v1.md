# AI 插件化接入方案（v0.1 骨架）

## 当前角色

这份文档记录 AI plugin hook 接入的初版设计说明。
当前它主要是历史设计背景，不是仓库的主状态板；当前仓库状态以源码和主 plan 为准。

## 当前实现对照（2026-04）

当前仓库已经具备最小 plugin runtime 骨架，并已接入主链路：

- 已存在实现文件：
  - `src/dataflow/ai/plugin_runtime.h`
  - `src/dataflow/ai/plugin_runtime.cc`
- 已接入的当前源码路径：
  - `src/dataflow/core/contract/api/session.cc`
  - `src/dataflow/core/contract/api/dataframe.cc`
  - `src/dataflow/core/execution/stream/stream.cc`
  - `BUILD.bazel`
- 当前可见 hook 点包括：
  - `kBeforeSqlParse`
  - `kAfterSqlParse`
  - `kBeforePlanBuild`
  - `kAfterPlanBuild`
  - `kPlanBeforeExecute`
  - `kPlanAfterExecute`
  - `kStreamingBatchStart`
  - `kStreamingBatchEnd`

当前仍应按“骨架能力”理解这条线：

- 目标是提供统一 hook/runtime 入口，而不是引入第二套执行语义
- 该文档中的后续扩展设想不自动等于当前仓库承诺

## 历史部分说明

下文保留最初设计说明。
如果示例路径或“待新增/待接入”的措辞与当前仓库现状不一致，应以本节和当前源码路径为准。

本次改造只引入插件接口与挂钩，不改变现有 SQL/执行语义。

## 一、目标

1. 提供统一钩子：`before/after` 关键阶段可注入 AI 决策；
2. 默认无插件时执行路径与当前行为一致；
3. 支持失败隔离（降级到默认执行）；
4. 支持按插件和按 HookPoint 的禁用/回退策略。

## 二、落地文件（已新增）

- `src/dataflow/ai/plugin_runtime.h`
- `src/dataflow/ai/plugin_runtime.cc`
- `src/dataflow/core/contract/api/dataframe.h` / `.cc`（新增 `explain()` 与 materialize 钩子）
- `src/dataflow/core/contract/api/session.cc`（session.sql 钩子）
- `src/dataflow/core/execution/stream/stream.cc`（streaming batch 钩子）
- `BUILD.bazel`（引入新源码）

## 三、Hook 点定义

### HookPoint（C++ 枚举）

- `kBeforeSqlParse`
  - 调用时机：`DataflowSession::sql` 入参 SQL；
  - 输入/输出：`PluginPayload.sql`、`summary`；
  - 典型用途：语句清洗、模板纠偏、权限前置扫描。

- `kAfterSqlParse`
  - 调用时机：`SqlParser::parse` 成功；
  - 典型用途：语法/语义提示、审计标记。

- `kBeforePlanBuild`
  - 调用时机：SQL 进入 `SqlPlanner` 前；
  - 典型用途：查询改写策略挂钩（例如：候选列名纠错）。

- `kAfterPlanBuild`
  - 调用时机：`planner.plan(...)` 生成 `DataFrame` 后；
  - 典型用途：规则检查（如慢查询白名单）、计划摘要记录。

- `kPlanBeforeExecute`
  - 调用时机：`DataFrame::materialize()` 执行前；
  - 输入：`PluginPayload.plan`；
  - 典型用途：执行限流、行数预算策略。

- `kPlanAfterExecute`
  - 调用时机：`Executor::execute` 返回后；
  - 典型用途：返回规模统计、执行后告警。

- `kStreamingBatchStart`
  - 调用时机：`StreamingQuery::awaitTermination` 每批开始；
  - 典型用途：批次配额、offset 保护。

- `kStreamingBatchEnd`
  - 调用时机：每批写 sink 后；
  - 典型用途：失败自动重试建议、批次监控。

## 四、数据契约（Payload）

`PluginPayload` 当前包含：
- `sql`：当前语句；
- `plan`：计划摘要（由 `DataFrame::explain()` 提供）；
- `summary`：阶段摘要；
- `row_count`：执行/批次行数；
- `message`、`attributes`：扩展字段。

## 五、插件行为/返回

`PluginResult.action`：
- `Continue`：继续执行；
- `Block`：阻断当前阶段；
- `Abort`：直接中断并抛错；
- `Fallback`：触发备用策略（当前为语义保留，默认仍走原执行路径）。

`PluginResult.note` 可用于告警/日志。

## 六、配置示例（JSON）

```json
{
  "plugin_manager": {
    "global": {
      "enabled": true,
      "fail_open": true,
      "auto_disable_on_error": false
    },
    "plugins": [
      {
        "name": "governance_guard",
        "enabled": true,
        "fail_open": false,
        "auto_disable_on_error": true,
        "config": {
          "max_output_rows": "5000",
          "blocked_hook_points": ["kAfterPlanBuild"]
        }
      },
      {
        "name": "nlp_rewrite",
        "enabled": false,
        "fail_open": true,
        "auto_disable_on_error": false,
        "config": {
          "policy": "suggest_only"
        }
      }
    ]
  }
}
```

## 七、禁用策略

### 1）按插件禁用
- `setPluginPolicy(name, enabled=false)`
- `unregisterPlugin(name)` 移除执行位点

### 2）按全局禁用
- `setGlobalPolicy` 设置 `fail_open` 为 `true` 时，任一插件异常不应影响主路径；
- `fail_open=false` 时，插件异常会导致执行中断（仅用于沙箱/严格治理环境）。

### 3）按 HookPoint 降级
- 在插件内部读取 `payload.summary/payload.attributes` 决定是否对特定阶段返回 `Block`；
- 当前骨架保留 `blocked_hook_points` 读取口，便于外层配置适配器实现。

## 八、落地建议（本周）

1. 在 `DataflowSession` 同步实现 `registerPlugin` / `setPluginPolicy` 公共 API；
2. 增加 2-3 个示例插件（静态）：`policy_guard`、`sql_lint`、`lineage_guard`；
3. 给所有钩子追加 `reason` 到日志，形成 `trace_id` 级执行审计链。
