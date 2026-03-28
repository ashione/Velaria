# AGENTS.md

本仓库用于纯 C++ 数据流引擎调研与演进，目标是稳定 DataFrame/SQL/Streaming 最小闭环后逐步扩展到可分布式运行时。

## 代码边界
- 主要语言：C++17
- 构建系统：Bazel
- 当前重点：`sql`、`runtime`、`planner`、`api`、`stream`、`catalog`

## 关键约定
- 优先保证语义一致性、边界可诊断、最小可用先行。
- 不做 JVM/Python 宿主栈移植；核心计算逻辑以 C++ 实现。
- 不再使用 Spark 命名词条作为 API 同名依赖。
- `session` 对外入口使用 `DataflowSession`。
- 修改需尽量保持现有风格，不做无关重构。

## 常用命令
- 全量编译：
  - `bazel build //:sql_demo`
  - `bazel build //:df_demo`
- 快速运行示例：
  - `bazel run //:sql_demo`
  - `bazel run //:df_demo`
  - `bazel run //:stream_demo`
- 如遇解析/类型错误，优先复现并补齐：
  - `bazel build //:sql_demo`
  - `./bazel-bin/sql_demo`

## 推荐提交流程
- 修改前明确影响范围：SQL/Executor/Planner/Session/Stream。
- 小步提交，每次提交围绕一组可验证目标。
- 避免在未确认的情况下做大范围接口改动。

## 当前已知坑位
- SQL 解析器为 v1 子集，暂不支持子查询、CTE、窗口函数。
- 需继续加严错误恢复能力（语法错误提示和恢复建议）。
- 分布式运行时尚未接入，当前以本地执行器为准。

## 风险提示
- `Value` 当前允许数字类型（Int64/Double）做跨类型比较；如果要改为严格类型模式，需同步更新 Planner 和示例。
- `Schema`/Join 场景需要留意对象生命周期，避免临时对象引用外泄。
