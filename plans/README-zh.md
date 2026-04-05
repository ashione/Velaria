# Plans 目录说明

中文索引文档，对应英文版位于 [README.md](./README.md)。

`plans/` 目录同时包含当前计划追踪、已实现设计背景和较早阶段的路线图说明。
阅读前先用这份索引判断哪份文档是当前权威入口。

## 建议阅读顺序

- 面向仓库外的当前实现范围：
  - [../README-zh.md](../README-zh.md)
  - [../README.md](../README.md)
- 稳定边界与 contract：
  - [../docs/core-boundary.md](../docs/core-boundary.md)
  - [../docs/runtime-contract.md](../docs/runtime-contract.md)
  - [../docs/streaming_runtime_design.md](../docs/streaming_runtime_design.md)
- 当前维护中的工作计划：
  - [core-runtime-columnar-plan.md](./core-runtime-columnar-plan.md)

## 当前有效计划

- [core-runtime-columnar-plan.md](./core-runtime-columnar-plan.md)
  - 当前 core runtime columnar 路线的状态板
  - 维护已实现项、明确不做项和下一阶段
  - 当前活跃的 core-runtime 路线应优先更新这份文档

## 仍有参考价值的历史设计说明

- [stream-sql-v1.md](./stream-sql-v1.md)
  - 当前 stream SQL 子集与拒绝策略的设计背景
- [python-api-v1.md](./python-api-v1.md)
  - Python 绑定、Arrow 输入和 CLI 形态的设计背景
- [ai-plugin-interface-v1.md](./ai-plugin-interface-v1.md)
  - AI plugin hook 接入路径的设计说明
- [build-system.md](./build-system.md)
  - 较早的构建系统决策说明；仓库当前已经实际采用 Bazel

这些文档可以帮助理解为什么现有实现长成现在这样，但它们不是主要状态板。每份历史文档的开头都应明确说明当前角色、已吸收实现和与当前主 plan 的关系。

## 较早路线图与已过时的计划说明

- [dataframe-first-v1.md](./dataframe-first-v1.md)
  - DataFrame-first 阶段的早期路线图
- [streaming-first-roadmap.md](./streaming-first-roadmap.md)
  - 早期的 streaming-first 路线图
- [review-dataframe-first-v1.md](./review-dataframe-first-v1.md)
  - 对旧版 DataFrame-first 计划的审核记录

除非这些内容被显式吸收到当前维护中的计划里，否则应把它们视为历史背景。

## 更新规则

- 仓库面对外的实现范围变化时，优先更新根 README
- 稳定 contract 或分层边界变化时，更新 `docs/`
- 当前 core-runtime columnar 路线状态变化时，更新 [core-runtime-columnar-plan.md](./core-runtime-columnar-plan.md)
- 不要把旧 plan 文档静默升级成当前状态板
