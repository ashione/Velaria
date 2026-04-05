# 计划审核报告（DataFrame-first v1）

## 当前角色

这份文档是对早期 `dataframe-first-v1` 路线图的审核记录。
它现在属于历史评审材料，不是当前仓库状态或当前待办的直接来源。

## 当前意义（2026-04）

这份审核记录仍有价值的部分主要是：

- 提醒不要把 API 面和 SQL 方言范围放大过快
- 强调要给阶段目标补验收标准
- 强调依赖、错误模型、catalog 边界需要先收敛

当前不应再按这份文档理解的内容包括：

- 它对“接下来数周如何推进”的节奏建议
- 它对当时未落地路线图的逐条整改要求
- 把它当作当前主 plan 或当前 review 结论

## 与当前主 plan 的关系

当前状态和下一阶段以 `plans/core-runtime-columnar-plan.md` 为准。
这份文档只保留为“早期路线图曾经被如何审视”的背景资料。

## 历史部分说明

下文保留原始评审内容。
如果其中的建议后来已经被 README、`docs/` 或当前主 plan 吸收，应以后者为准。

## 结论
整体方向是“**正确且可执行**”，但当前计划仍有 5 处关键缺口，需要修订后再下第一行代码。

## 一、通过项
1. **路线正确**：DataFrame 优先 + SQL 为主，RDD 后置，适配你当前目标。
2. **架构分层清晰**：Logical/Physical plan + scheduler/runtime 的分层是成熟方向。
3. **兼容原则合理**：强调“语义兼容 + 映射文档”，比一比一 API 更务实。
4. **Bazel 选择得当**：长周期分布式项目利于复现和增量构建。

## 二、待修订（高优先级）
1. **术语边界冲突**
   - README 目标仍写 `RDD-like`，而且 v1 只做 DataFrame。
   - 建议明确：
     - `RDD-like` 写为“未来兼容层”，当前版本默认关闭。 

2. **里程碑缺少验收标准**
   - M2/M3/M4 只写产出，不给“可验收指标”。
   - 建议每阶段补一个 “Definition of Done”：
     - API 数量
     - 运行示例数
     - 成功率/吞吐基线

3. **依赖策略风险**
   - Arrow/Parquet 一口气引入可能拖慢 PoC。
   - 建议先固定“最小运行时接口”并通过 adaptor（适配层）隔离第三方库。

4. **分布式基础未分层到故障场景**
   - 已有重试与 lineage，但无“节点失效恢复、僵尸 task、重复执行幂等性”定义。
   - 建议补充：
     - task 幂等约束
     - shuffle 文件清理策略
     - 重启后任务状态重建策略

5. **SQL 方言边界太模糊**
   - v1 若盲目支持 ANSI 很容易失控。
   - 建议把 SQL v1 限定为 `SELECT/WHERE/GROUP BY/JOIN/CTE` + 10~20 个常用函数。

## 三、建议修订版（立即可执行）
- **本周（计划落地）**：冻结 `api-v1` 清单 + `error model` + `catalog model`。
- **两周（单机）**：完成 DataFrame v1 15-20 API + 本地 SQL 子集 + 10 个单测。
- **三周（分布式）**：完成 3 节点 mock + wordcount/groupBy/join + 失败重试验证。
- **四周（可发布试验版）**：补齐文档、兼容差异表、性能基线。

## 四、建议你的决定（请回复）
1. SQL v1 是否限定为 `Core SQL`（建议）：
   `SELECT/WHERE/GROUP BY/HAVING/JOIN/UNION/CTE` + 常见日期与字符串函数？
2. 是否允许第一版对 catalog 只支持 session-local temp view（不连外部 metastore）？
3. 你是否接受“先不支持可逆 cancel（graceful cancel）”并在 v1 末补上？
