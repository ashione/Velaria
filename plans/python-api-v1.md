# Python API v1 设计说明

## 目标

在不改写现有 C++ 执行核心的前提下，为 Velaria 提供可用的 Python 调用入口，并满足以下约束：

- 继续复用现有 `DataflowSession` / `DataFrame` / `StreamingDataFrame` / `StreamingQuery`
- 支持 batch SQL、stream API、stream SQL
- 支持 Python `3.12` / `3.13`
- 支持 `CSV` 文件输入，也支持 `Arrow` 进程内直接传数
- 对长时间 native 调用显式释放 GIL

## 绑定方式

当前版本不引入 `pybind11` / `Cython`，而是采用手写 `CPython extension`。

原因：

- 绑定层足够薄，围绕现有对象即可
- 可以精确控制 GIL 释放点
- 不把仓库主链路绑定到额外 C++ 模板依赖
- 对象所有权和错误边界更可诊断

当前对外暴露四类 Python 对象：

- `Session`
- `DataFrame`
- `StreamingDataFrame`
- `StreamingQuery`

## 输入与输出模型

### CSV

保留文件型输入：

- `Session.read_csv(...)`
- `Session.read_stream_csv_dir(...)`
- stream SQL DDL:
  - `CREATE SOURCE TABLE ... USING csv`
  - `CREATE SINK TABLE ... USING csv`

主要覆盖：

- 本地 demo
- 文件回放
- sink 落盘

### Arrow

新增进程内直接传数：

- `Session.create_dataframe_from_arrow(pyarrow_obj)`
- `Session.create_stream_from_arrow(pyarrow_obj_or_batches)`
- `DataFrame.to_arrow()`

其中：

- `create_dataframe_from_arrow(...)`
  - 输入可为 `pyarrow.Table` 或可被 `pyarrow.table(...)` 归一化的对象
- `create_stream_from_arrow(...)`
  - 输入可为单个 `pyarrow.Table`
  - 输入可为单个 `pyarrow.RecordBatch`
  - 输入也可为多批 `Table / RecordBatch` 的 Python 序列

这意味着 Python 侧既可继续走 CSV，也可完全跳过文件落盘，直接把内存中的 Arrow 数据交给 batch / stream 执行链。

## 当前内部表示

内部执行核心仍使用：

- `Table`
- `Row`
- `Value`

Python 边界做的是：

- `Table -> Arrow` 导出
- `Arrow -> Table` 导入

这样能在不扰动现有 planner / runtime 的情况下先打通 Python 使用面。

## Arrow 交换设计

### 输出

`DataFrame.to_arrow()` 当前通过 Arrow C Data Interface 导出：

- `__arrow_c_array__`
- `ArrowSchema`
- `ArrowArray`

再由 `pyarrow` 导入为 `pyarrow.Table`。

当前覆盖类型：

- `Nil`
- `Int64`
- `Double`
- `String`

### 输入

Arrow 输入当前采取“先归一化，再转 `Table`”策略：

- 对 `Table` / `RecordBatch` 做 Python 层对象归一化
- 再按列读取成 Python 列表
- 最后映射回 C++ `Value`

优点：

- 简单直接
- 与现有 `Value` 类型体系一致

边界：

- 当前不是零拷贝导入
- 大批量输入下成本仍高于原生 Arrow 内部执行

因此这属于 `API v1` 的可用方案，不是最终性能形态。

## GIL 策略

以下调用在 native 执行期间显式释放 GIL：

- `Session.read_csv(...)`
- `Session.sql(...)`
- `Session.stream_sql(...)`
- `Session.start_stream_sql(...)`
- `DataFrame.count()`
- `DataFrame.to_rows()`
- `DataFrame.show()`
- `StreamingQuery.start()`
- `StreamingQuery.await_termination(...)`
- `StreamingQuery.stop()`

当前明确不支持：

- Python callback
- Python UDF
- Python 自定义 source
- Python 自定义 sink

## Packaging 设计

### Bazel 产物

当前有三类 Python 相关产物：

- `//:velaria_pyext`
  - native `_velaria.so`
- `//python_api:velaria_whl`
  - pure-Python wheel
- `//python_api:velaria_native_whl`
  - 将 `_velaria.so` 注入 wheel，产出本地平台 wheel

### uv

`python_api/` 下提供：

- `pyproject.toml`
- `uv.lock`

用于：

- Python 依赖同步
- 本地 demo / 开发环境管理

当前 `pyarrow` 版本已对齐到支持 `Python 3.13` 的版本线。

## Native 扩展加载策略

`velaria/__init__.py` 当前按以下顺序加载 native 扩展：

1. 若设置了 `VELARIA_PYTHON_EXT`
   - 显式按给定路径加载
2. 若已安装 native wheel
   - 直接从包内导入 `._velaria`
3. 若处于源码树开发态
   - 自动定位仓库根
   - 自动查找 `bazel-bin/_velaria.so`
   - 必要时通过 `bazel info bazel-bin` 解析真实输出目录

因此源码树开发时通常不再需要手工设置环境变量。

## 现状与边界

### 已完成

- Python `3.12` / `3.13`
- batch SQL
- stream API
- stream SQL
- Arrow batch 输入
- Arrow stream 输入
- Arrow 输出
- pure wheel
- native wheel

### 当前边界

- Arrow 输入仍是导入复制，不是零拷贝
- stream Arrow 输入当前映射到 `MemoryStreamSource`
- 还未支持：
  - `__arrow_c_stream__`
  - `RecordBatchReader`
  - Python callback sink
  - Python callback source
  - Python UDF

## 下一步建议

### 短期

- 增加 Python API 回归测试
- 支持 `RecordBatchReader` / `__arrow_c_stream__`
- 更明确的类型/空值错误文案

### 中期

- 评估 `Arrow -> Table` 零拷贝或半零拷贝导入
- 让 stream Arrow 输入支持更明确的多批 offset / progress 语义
- 评估是否在 Python API 暴露 `StreamingQueryOptions`

### 长期

- 评估内部执行表示是否逐步引入 Arrow-native 列式路径
- 统一 C++ 与 Python 边界上的 batch / stream 数据交换协议
