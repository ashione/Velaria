# Stream SQL v1 设计说明

## 目标

在不推翻现有 `DataflowSession -> StreamingDataFrame -> StreamingQuery` 主链路的前提下，补上一个最小可用的流式 SQL 入口。

当前设计目标不是实现完整流式 SQL，而是把已经稳定存在的流式 API 能力收敛成统一的 SQL 入口，优先打通：

- `csv source`
- 单表流式查询
- 流式聚合
- `csv sink`

## 当前接口

### 查询入口

```cpp
StreamingDataFrame DataflowSession::streamSql(const std::string& sql);
```

语义：

- 只接受 `SELECT`
- 返回 `StreamingDataFrame`
- 允许调用方继续挂接 `writeStream(...)`

### 启动入口

```cpp
StreamingQuery DataflowSession::startStreamSql(
    const std::string& sql,
    const StreamingQueryOptions& options = {});
```

语义：

- 只接受 `INSERT INTO sink_table SELECT ...`
- 内部会把 `SELECT` 子句先翻译成 `StreamingDataFrame`
- 再把结果绑定到已注册 sink 并启动 query

## 当前 SQL 语义

### 已支持

- `CREATE SOURCE TABLE ... USING csv OPTIONS(...)`
- `CREATE SINK TABLE ... USING csv OPTIONS(...)`
- `SELECT ... FROM source`
- `WHERE`
- `GROUP BY`
- `HAVING`
- `LIMIT`
- `SUM(col)`
- `COUNT(*)`
- `INSERT INTO sink_table SELECT ...`

### 当前不支持

- `JOIN`
- `AVG`
- `MIN`
- `MAX`
- `window SQL`
- `INSERT INTO ... VALUES`
- `CTE`
- 子查询
- `UNION`
- 多表查询

## SQL 到流式 API 的映射

### source / sink

- `CREATE SOURCE TABLE ... USING csv`
  - 映射到 `DirectoryCsvStreamSource`
- `CREATE SINK TABLE ... USING csv`
  - 映射到 `FileAppendStreamSink`

### 查询

- `FROM source_table`
  - 映射到已注册的 `StreamingDataFrame`
- `WHERE col op value`
  - 映射到 `.filter(col, op, value)`
- `GROUP BY k`
  - 映射到 `.groupBy({k})`
- `SUM(v)`
  - 映射到 `.sum(v, true, alias)`
- `COUNT(*)`
  - 映射到 `.count(true, alias)`
- `HAVING`
  - 映射到聚合结果上的 `.filter(...)`
- `LIMIT`
  - 映射到 `.limit(n)`

## 当前 DDL 设计

### `CREATE SOURCE TABLE`

示例：

```sql
CREATE SOURCE TABLE stream_events (key STRING, value INT)
USING csv OPTIONS(path '/tmp/stream-input', delimiter ',');
```

约束：

- 当前只支持 `USING csv`
- `path` 必填，且应指向目录
- `delimiter` 可选，默认 `,`

### `CREATE SINK TABLE`

示例：

```sql
CREATE SINK TABLE stream_summary (key STRING, value_sum INT)
USING csv OPTIONS(path '/tmp/stream-output.csv', delimiter ',');
```

约束：

- 当前只支持 `USING csv`
- `path` 必填，且应指向输出文件
- `delimiter` 可选，默认 `,`

### 当前不支持的 DDL

- `CREATE TABLE ... USING csv` 普通表
- 非 `csv` provider
- 在 `USING csv` 上声明更复杂 connector 参数

## 当前 DML 设计

### `INSERT INTO ... SELECT ...`

示例：

```sql
INSERT INTO stream_summary
SELECT key, SUM(value) AS value_sum
FROM stream_events
WHERE value > 6
GROUP BY key
HAVING value_sum > 15
LIMIT 10;
```

语义：

- 这不是一次性 batch insert
- 它对应的是一个被启动并运行的 `StreamingQuery`
- 因此需要通过 `startStreamSql(...)` 进入，而不是走 `submit(...)`

### 当前不支持的 DML

- `INSERT INTO ... VALUES`
- `INSERT INTO ... SELECT` + 列表重排
- `INSERT OVERWRITE`

## 设计取舍

### 为什么先不做完整 planner

当前仓库已经有两套稳定对象：

- batch：`DataFrame`
- streaming：`StreamingDataFrame`

stream SQL v1 先做“轻量翻译层”而不是“复用 batch planner 到 streaming runtime”，原因是：

- 现有流式 API 已有稳定算子
- 当前流式能力本身就不是完整 SQL 超集
- 先落一个受限翻译层，风险更低，调试边界更清晰

### 为什么 `INSERT INTO` 单独用启动接口

在 batch 里，`INSERT INTO` 是一次性动作。

在 streaming 里，`INSERT INTO sink SELECT ...` 实际上表示：

- 创建一个持续运行的 query
- 从 source 拉流
- 聚合/过滤
- 持续写入 sink

因此它的自然返回值是 `StreamingQuery`，而不是 `DataFrame` 或 `Table`。

## 当前边界与风险

### `LIMIT`

当前 `LIMIT` 复用 `StreamingDataFrame::limit`，语义是现有 streaming API 的 batch 级限制，不应解读为无限流上的严格全局 SQL limit。

### 聚合列支持

当前只稳定支持：

- `SUM`
- `COUNT(*)`

原因是它们能直接映射到现有状态聚合实现。

### schema 约束

当前 `CREATE SOURCE/SINK TABLE` 中的列定义主要承担接口语义和可读性作用，真正执行仍依赖现有 source/sink 行为与运行时表结构。

更严格的 schema 对齐、列重排和 provider 级校验，属于下一阶段工作。

## 下一步建议

### 短期

- 支持 `INSERT INTO sink(col1, ...) SELECT ...`
- 增加 source/sink schema 对齐校验
- 支持 `COUNT(col)` 或拒绝文案更精确
- 收紧 `LIMIT` 文档和运行期说明

### 中期

- 支持窗口 SQL 到 `.window(...)` 的映射
- 支持更多 provider
- 评估是否抽出独立 `StreamingSqlPlanner`

### 长期

- 统一 batch / streaming 的逻辑计划表示
- 在 planner 层而不是 session 层做 SQL 到 runtime 的双路径分发
