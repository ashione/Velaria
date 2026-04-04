#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
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

struct ValueColumnBuffer {
  std::vector<Value> values;
};

struct ColumnarTable {
  Schema schema;
  std::vector<ValueColumnBuffer> columns;
};

struct ValueColumnView {
  std::shared_ptr<const ColumnarTable> owner;
  const ValueColumnBuffer* buffer = nullptr;

  const std::vector<Value>& values() const { return buffer->values; }
};

struct RowSelection {
  std::vector<uint8_t> selected;
  std::size_t selected_count = 0;
};

std::shared_ptr<ColumnarTable> makeColumnarCache(const Table& table);
std::shared_ptr<const ColumnarTable> ensureColumnarCache(const Table* table);
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
Table projectTable(const Table& table, const std::vector<std::size_t>& indices,
                   const std::vector<std::string>& aliases = {});
Table filterTable(const Table& table, const RowSelection& selection);
Table limitTable(const Table& table, std::size_t limit);
Table sortTable(const Table& table, const std::vector<std::size_t>& indices,
                const std::vector<bool>& ascending);

std::vector<Value> vectorizedWindowStart(const ValueColumnBuffer& input, uint64_t window_ms);
std::vector<Value> vectorizedWindowStart(const ValueColumnView& input, uint64_t window_ms);
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

void appendNamedColumn(Table* table, const std::string& column_name, std::vector<Value>&& values);
void appendColumn(Table* table, std::vector<Value>&& values);

}  // namespace dataflow
