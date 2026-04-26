#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "src/dataflow/core/logical/planner/plan.h"
#include "src/dataflow/core/execution/table.h"

namespace dataflow {

struct StringColumnBuffer {
  std::vector<std::string> values;
  std::vector<uint8_t> is_null;
};

struct Int64ColumnBuffer {
  std::vector<int64_t> values;
  std::vector<uint8_t> is_null;
};

struct DoubleColumnBuffer {
  std::vector<double> values;
  std::vector<uint8_t> is_null;
};

struct ArrowColumnBacking {
  std::string format;
  std::string child_format;
  std::shared_ptr<void> null_bitmap;
  std::shared_ptr<void> value_buffer;
  std::shared_ptr<void> extra_buffer;
  std::shared_ptr<void> child_value_buffer;
  int32_t fixed_list_size = 0;
  std::size_t length = 0;
  std::size_t child_length = 0;
  int64_t null_count = 0;
};

struct ValueColumnBuffer {
  std::vector<Value> values;
  std::shared_ptr<const ArrowColumnBacking> arrow_backing;
};

struct ColumnarTable {
  Schema schema;
  std::vector<ValueColumnBuffer> columns;
  std::vector<std::string> arrow_formats;
  std::vector<std::size_t> batch_row_counts;
  std::size_t row_count = 0;
};

struct ValueColumnView {
  std::shared_ptr<const ColumnarTable> owner;
  const ValueColumnBuffer* buffer = nullptr;
};

struct RowSelection {
  std::size_t input_row_count = 0;
  std::vector<uint8_t> selected;
  std::vector<std::size_t> indices;
  std::size_t selected_count = 0;
};

std::shared_ptr<ColumnarTable> makeColumnarCache(const Table& table);
std::shared_ptr<const ColumnarTable> ensureColumnarCache(const Table* table);
std::size_t valueColumnRowCount(const ValueColumnBuffer& buffer);
bool valueColumnIsNullAt(const ValueColumnBuffer& buffer, std::size_t row_index);
std::string_view valueColumnStringViewAt(const ValueColumnBuffer& buffer, std::size_t row_index);
std::string valueColumnStringAt(const ValueColumnBuffer& buffer, std::size_t row_index);
int64_t valueColumnInt64At(const ValueColumnBuffer& buffer, std::size_t row_index);
double valueColumnDoubleAt(const ValueColumnBuffer& buffer, std::size_t row_index);
Value valueColumnValueAt(const ValueColumnBuffer& buffer, std::size_t row_index);
ValueColumnView viewValueColumn(const Table& table, std::size_t column_index);
std::vector<ValueColumnView> viewValueColumns(const Table& table,
                                              const std::vector<std::size_t>& indices);

StringColumnBuffer makeNullStringColumn(std::size_t row_count);
StringColumnBuffer makeConstantStringColumn(std::size_t row_count, std::string value);
StringColumnBuffer materializeStringColumn(const Table& table, std::size_t column_index);

Int64ColumnBuffer makeNullInt64Column(std::size_t row_count);
Int64ColumnBuffer makeConstantInt64Column(std::size_t row_count, int64_t value);
Int64ColumnBuffer materializeInt64Column(const Table& table, std::size_t column_index);
DoubleColumnBuffer makeNullDoubleColumn(std::size_t row_count);
DoubleColumnBuffer materializeDoubleColumn(const Table& table, std::size_t column_index);
ValueColumnBuffer materializeValueColumn(const Table& table, std::size_t column_index);
std::vector<ValueColumnBuffer> materializeValueColumns(const Table& table,
                                                       const std::vector<std::size_t>& indices);
std::vector<std::string> materializeSerializedKeys(const Table& table,
                                                   const std::vector<std::size_t>& indices);
std::unordered_map<std::string, std::vector<std::size_t>> buildHashBuckets(
    const std::vector<std::string>& keys);

RowSelection vectorizedFilterSelection(const ValueColumnBuffer& input, const Value& rhs,
                                       bool (*pred)(const Value& lhs, const Value& rhs));
RowSelection vectorizedFilterSelection(const ValueColumnView& input, const Value& rhs,
                                       bool (*pred)(const Value& lhs, const Value& rhs));
RowSelection vectorizedFilterSelection(const ValueColumnBuffer& input, const Value& rhs,
                                       const std::string& op);
RowSelection vectorizedFilterSelection(const ValueColumnView& input, const Value& rhs,
                                       const std::string& op);
RowSelection vectorizedFilterSelection(const ValueColumnBuffer& input, const Value& rhs,
                                       const std::string& op, std::size_t max_selected);
RowSelection vectorizedFilterSelection(const ValueColumnView& input, const Value& rhs,
                                       const std::string& op, std::size_t max_selected);
RowSelection vectorizedColumnCompareSelection(const ValueColumnView& lhs,
                                             const ValueColumnView& rhs,
                                             const std::string& op);
Table projectTable(const Table& table, const std::vector<std::size_t>& indices,
                   const std::vector<std::string>& aliases = {},
                   bool materialize_rows = true);
Table filterTable(const Table& table, const RowSelection& selection,
                  bool materialize_rows = true);
Table limitTable(const Table& table, std::size_t limit, bool materialize_rows = true);
Table sortTable(const Table& table, const std::vector<std::size_t>& indices,
                const std::vector<bool>& ascending);
Table topNTable(const Table& table, const std::vector<std::size_t>& indices,
                const std::vector<bool>& ascending, std::size_t limit,
                bool materialize_rows = false);
Table concatenateTables(const std::vector<Table>& tables, bool materialize_rows = false);

std::vector<Value> vectorizedWindowStart(const ValueColumnBuffer& input, uint64_t window_ms);
std::vector<Value> vectorizedWindowStart(const ValueColumnView& input, uint64_t window_ms);
Table assignSlidingWindow(const Table& table, std::size_t time_column_index, uint64_t window_ms,
                          uint64_t slide_ms, const std::string& output_column,
                          bool materialize_rows = false,
                          bool format_window_as_timestamp = false);
std::vector<Value> vectorizedStringLength(const StringColumnBuffer& input);
std::vector<Value> vectorizedStringLower(const StringColumnBuffer& input);
std::vector<Value> vectorizedStringUpper(const StringColumnBuffer& input);
std::vector<Value> vectorizedStringTrim(const StringColumnBuffer& input);
std::vector<Value> vectorizedStringLtrim(const StringColumnBuffer& input);
std::vector<Value> vectorizedStringRtrim(const StringColumnBuffer& input);
std::vector<Value> vectorizedStringReverse(const StringColumnBuffer& input);
std::vector<Value> vectorizedStringConcat(const std::vector<StringColumnBuffer>& inputs);
std::vector<Value> vectorizedStringConcatWs(const std::vector<StringColumnBuffer>& inputs);
std::vector<Value> vectorizedStringLeft(const StringColumnBuffer& input,
                                        const Int64ColumnBuffer& length);
std::vector<Value> vectorizedStringRight(const StringColumnBuffer& input,
                                         const Int64ColumnBuffer& length);
std::vector<Value> vectorizedStringSubstr(const StringColumnBuffer& input,
                                          const Int64ColumnBuffer& start_one_based,
                                          const Int64ColumnBuffer* length);
std::vector<Value> vectorizedStringReplace(const StringColumnBuffer& input,
                                           const StringColumnBuffer& from,
                                           const StringColumnBuffer& to);
std::vector<Value> vectorizedStringPosition(const StringColumnBuffer& needle,
                                           const StringColumnBuffer& input);
std::vector<Value> vectorizedAbs(const DoubleColumnBuffer& input);
std::vector<Value> vectorizedCeil(const DoubleColumnBuffer& input);
std::vector<Value> vectorizedFloor(const DoubleColumnBuffer& input);
std::vector<Value> vectorizedRound(const DoubleColumnBuffer& input);
std::vector<Value> vectorizedDateYear(const ValueColumnView& input);
std::vector<Value> vectorizedDateMonth(const ValueColumnView& input);
std::vector<Value> vectorizedDateDay(const ValueColumnView& input);

std::vector<Value> computeComputedColumnValues(
    Table* table,
    ComputedColumnKind function,
    const std::vector<ComputedColumnArg>& args);

void appendNamedColumn(Table* table, const std::string& column_name, std::vector<Value>&& values,
                       bool materialize_rows = true);
void appendNamedColumn(Table* table, const std::string& column_name, ValueColumnBuffer&& column,
                       bool materialize_rows = true);
void appendColumn(Table* table, std::vector<Value>&& values, bool materialize_rows = true);
void appendColumn(Table* table, ValueColumnBuffer&& column, bool materialize_rows = true);

}  // namespace dataflow
