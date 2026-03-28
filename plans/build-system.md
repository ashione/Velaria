# 依赖与构建系统建议（Bazel 优先）

## 结论（先给结果）
你这个场景下**推荐用 Bazel**。

理由：
- 这是分布式/多模块系统，后续会有 C++、可能还有插件、proto、测试、代码生成等多语言组件，Bazel 对 `增量构建 + 可重复构建 + 远端缓存` 友好。
- 适合后续扩展到 `CI/CD 多节点并行构建` 和可复用的算子库。
- 对 `proto/gRPC/flatbuffers`、`codegen`、`测试`、`交叉编译` 的链路整合比传统 CMake 更统一。

---

## 什么时候不选 Bazel（替代方案）
- 如果团队规模很小、短期只做 PoC：
  - `CMake + Conan`/`vcpkg` 的门槛更低，上手快。
- 但你要走“长期平台化、多人协同、版本稳定、可复现”方向，Bazel 更适配。

---

## 推荐方案（阶段化）

### 阶段 1（v1 PoC）
- `Bazel + bzlmod` 管理外部依赖（最小依赖栈）
- 先集中托管依赖：
  - gRPC / Protobuf
  - Apache Arrow（如可行）
  - fmt / abseil / spdlog（后续）
- 避免引入过多二进制巨型依赖（例如一次性全量编译 parquet/cpp 大集成库）
- 对于仍不确定可行性的库，使用 `repository_rule` + `cc_import` 或预编译方式先跑通最小链路

### 阶段 2（正式化）
- 建立统一仓库结构：
  - `WORKSPACE/WORKSPACE.bazel`（或 WORKSPACE.bzlmod 迁移）
  - `MODULE.bazel`（启用 Bzlmod）
  - 多模块 `BUILD.bazel`（core / planner / runtime / sql / scheduler / client）
- 引入统一风格：
  - `cc_library`（按层）
  - `cc_binary`（worker/driver）
  - `cc_test`（规则测试）
- 依赖分层原则：
  - 底层基础库 -> planner -> sql -> runtime -> cli

### 阶段 3（规模化）
- CI 中启用：
  - `bazel build //...`
  - `bazel test //...`
  - `bazel run //...` 的 smoke
- 考虑 `remote cache`（可选）
- 逐步把生成式文件（proto）纳入统一 `proto_library` 规则

---

## 你这个项目里的实操建议
我建议你直接采用：
1. **顶层优先 Bazel（Bzlmod）**
2. 先做一个最小可编译骨架（3~5 个 `cc_library`）
3. 先放弃一开始全量依赖，先保留 `CSV/JSON parser` 的本地实现或轻量实现，验证执行链路
4. 再逐步接入 Arrow/Parquet + gRPC

---

## 迁移策略（如果你担心学习成本）
- 你完全可以按下面顺序：
  - Week 1：CMake 风格思路写目录+接口；同时搭 Bazel 骨架
  - Week 2：把核心模块迁到 Bazel 构建 target
  - Week 3：弃用旧构建入口，统一 Bazel

---

如果你认可，我下一步直接给你一版：
- `MODULE.bazel`（最小依赖清单）
- `WORKSPACE.bzl` / `BUILD.bazel` 示例
- `src/` 到 `BUILD` 的模块分解建议（按你 DataFrame 优先路线）
