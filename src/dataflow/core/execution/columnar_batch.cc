#include "src/dataflow/core/execution/columnar_batch.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <stdexcept>

#include "src/dataflow/core/execution/runtime/simd_dispatch.h"

namespace dataflow {

namespace {

constexpr char kGroupDelim = '\x1f';

int64_t parseEpochMillis(const std::string& raw);

std::tm toUtcTm(std::time_t seconds) {
  std::tm tm = {};
#if defined(_WIN32)
  if (gmtime_s(&tm, &seconds) != 0) {
    throw std::runtime_error("failed to convert epoch to UTC");
  }
#else
  if (gmtime_r(&seconds, &tm) == nullptr) {
    throw std::runtime_error("failed to convert epoch to UTC");
  }
#endif
  return tm;
}

[[maybe_unused]] std::string valueAsString(const Value& value) {
  if (value.type() == DataType::Nil) {
    return std::string();
  }
  return value.toString();
}

bool arrowBitmapHasValue(const uint8_t* bitmap, std::size_t index) {
  if (bitmap == nullptr) {
    return true;
  }
  return ((bitmap[index >> 3] >> (index & 7)) & 0x01u) != 0;
}

[[maybe_unused]] int64_t valueAsInt64(const Value& value) {
  if (value.type() == DataType::Nil) {
    throw std::runtime_error("computed function argument cannot be null");
  }
  if (!value.isNumber()) {
    throw std::runtime_error("computed function argument must be numeric");
  }
  return value.asInt64();
}

double valueAsDouble(const Value& value) {
  if (value.type() == DataType::Nil) {
    throw std::runtime_error("computed function argument cannot be null");
  }
  if (!value.isNumber()) {
    throw std::runtime_error("computed function argument must be numeric");
  }
  return value.asDouble();
}

int64_t valueAsEpochMillis(const Value& value) {
  if (value.type() == DataType::Nil) {
    throw std::runtime_error("window assign source column cannot be null");
  }
  if (value.isNumber()) {
    return value.asInt64();
  }
  return parseEpochMillis(value.toString());
}

bool matchesCompareOp(int compare_result, const std::string& op) {
  if (op == "=" || op == "==") return compare_result == 0;
  if (op == "!=") return compare_result != 0;
  if (op == "<") return compare_result < 0;
  if (op == ">") return compare_result > 0;
  if (op == "<=") return compare_result <= 0;
  if (op == ">=") return compare_result >= 0;
  throw std::runtime_error("unsupported filter op: " + op);
}

NumericCompareOp toNumericCompareOp(const std::string& op) {
  if (op == "=" || op == "==") return NumericCompareOp::Eq;
  if (op == "!=") return NumericCompareOp::Ne;
  if (op == "<") return NumericCompareOp::Lt;
  if (op == ">") return NumericCompareOp::Gt;
  if (op == "<=") return NumericCompareOp::Le;
  if (op == ">=") return NumericCompareOp::Ge;
  throw std::runtime_error("unsupported filter op: " + op);
}

bool tryMatchesValueCompare(const Value& lhs, const Value& rhs, const std::string& op) {
  try {
    return matchesCompareOp(lhs == rhs ? 0 : (lhs < rhs ? -1 : 1), op);
  } catch (...) {
    return false;
  }
}

template <typename T>
int compareScalar(const T& lhs, const T& rhs) {
  return lhs < rhs ? -1 : (lhs > rhs ? 1 : 0);
}

int64_t parseEpochMillis(const std::string& raw) {
  if (raw.empty()) {
    return 0;
  }
  const bool numeric =
      std::all_of(raw.begin(), raw.end(), [](char ch) { return ch >= '0' && ch <= '9'; });
  if (numeric) {
    return static_cast<int64_t>(std::stoll(raw));
  }

  std::tm tm = {};
  std::istringstream in(raw);
  in >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
  if (in.fail()) {
    in.clear();
    in.str(raw);
    in >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
  }
  if (in.fail()) {
    throw std::runtime_error("unsupported timestamp format: " + raw);
  }
  return static_cast<int64_t>(timegm(&tm)) * 1000;
}

int64_t valueColumnEpochMillisAt(const ValueColumnBuffer& buffer, std::size_t row_index) {
  if (valueColumnIsNullAt(buffer, row_index)) {
    throw std::runtime_error("window assign source column cannot be null");
  }
  if (!buffer.values.empty()) {
    const auto& value = buffer.values[row_index];
    if (value.isNumber()) {
      return value.asInt64();
    }
    return parseEpochMillis(value.toString());
  }
  if (buffer.arrow_backing == nullptr) {
    throw std::runtime_error("window assign source column cannot be null");
  }
  switch (buffer.arrow_backing->format.empty() ? '?' : buffer.arrow_backing->format[0]) {
    case 'b':
    case 'i':
    case 'I':
    case 'l':
    case 'L':
    case 'f':
    case 'g':
      return valueColumnInt64At(buffer, row_index);
    case 'u':
    case 'U':
      return parseEpochMillis(valueColumnStringAt(buffer, row_index));
    default:
      return parseEpochMillis(valueColumnValueAt(buffer, row_index).toString());
  }
}

std::string trimString(std::string input) {
  auto is_space = [](char ch) { return std::isspace(static_cast<unsigned char>(ch)) != 0; };
  auto start = input.begin();
  while (start != input.end() && is_space(*start)) ++start;
  auto end = input.end();
  while (end != start && is_space(*(end - 1))) --end;
  return std::string(start, end);
}

std::string ltrimString(std::string input) {
  auto is_space = [](char ch) { return std::isspace(static_cast<unsigned char>(ch)) != 0; };
  auto start = input.begin();
  while (start != input.end() && is_space(*start)) ++start;
  return std::string(start, input.end());
}

std::string rtrimString(std::string input) {
  auto is_space = [](char ch) { return std::isspace(static_cast<unsigned char>(ch)) != 0; };
  auto end = input.end();
  while (end != input.begin() && is_space(*(end - 1))) --end;
  return std::string(input.begin(), end);
}

std::string lowerString(std::string input) {
  for (char& ch : input) {
    ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
  }
  return input;
}

std::string upperString(std::string input) {
  for (char& ch : input) {
    ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
  }
  return input;
}

std::string reverseString(std::string input) {
  std::reverse(input.begin(), input.end());
  return input;
}

std::string replaceString(std::string source, const std::string& from, const std::string& to) {
  if (from.empty()) {
    return source;
  }
  std::size_t pos = 0;
  while ((pos = source.find(from, pos)) != std::string::npos) {
    source.replace(pos, from.size(), to);
    pos += to.size();
  }
  return source;
}

template <typename Fn>
std::vector<Value> unaryStringKernel(const StringColumnBuffer& input, Fn&& fn) {
  std::vector<Value> out(input.values.size());
  for (std::size_t i = 0; i < input.values.size(); ++i) {
    if (input.is_null[i] != 0) {
      continue;
    }
    out[i] = fn(input.values[i]);
  }
  return out;
}

}  // namespace

const std::vector<Value>& materializeValueBuffer(const ValueColumnBuffer* buffer) {
  if (buffer == nullptr) {
    throw std::invalid_argument("materializeValueBuffer buffer is null");
  }
  if (!buffer->values.empty() || buffer->arrow_backing == nullptr) {
    return buffer->values;
  }

  const auto& backing = *buffer->arrow_backing;
  std::vector<Value>& out = const_cast<std::vector<Value>&>(buffer->values);
  out.resize(backing.length);
  const auto* validity = static_cast<const uint8_t*>(backing.null_bitmap.get());

  if (backing.format == "b") {
    const auto* bits = static_cast<const uint8_t*>(backing.value_buffer.get());
    for (std::size_t row = 0; row < backing.length; ++row) {
      if (!arrowBitmapHasValue(validity, row)) continue;
      out[row] = Value(static_cast<int64_t>(((bits[row >> 3] >> (row & 7)) & 0x01u) != 0));
    }
    return out;
  }
  if (backing.format == "n") {
    return out;
  }
  if (backing.format == "i") {
    const auto* values = static_cast<const int32_t*>(backing.value_buffer.get());
    for (std::size_t row = 0; row < backing.length; ++row) {
      if (!arrowBitmapHasValue(validity, row)) continue;
      out[row] = Value(static_cast<int64_t>(values[row]));
    }
    return out;
  }
  if (backing.format == "I") {
    const auto* values = static_cast<const uint32_t*>(backing.value_buffer.get());
    for (std::size_t row = 0; row < backing.length; ++row) {
      if (!arrowBitmapHasValue(validity, row)) continue;
      out[row] = Value(static_cast<int64_t>(values[row]));
    }
    return out;
  }
  if (backing.format == "l") {
    const auto* values = static_cast<const int64_t*>(backing.value_buffer.get());
    for (std::size_t row = 0; row < backing.length; ++row) {
      if (!arrowBitmapHasValue(validity, row)) continue;
      out[row] = Value(values[row]);
    }
    return out;
  }
  if (backing.format == "L") {
    const auto* values = static_cast<const uint64_t*>(backing.value_buffer.get());
    for (std::size_t row = 0; row < backing.length; ++row) {
      if (!arrowBitmapHasValue(validity, row)) continue;
      out[row] = Value(static_cast<int64_t>(values[row]));
    }
    return out;
  }
  if (backing.format == "f") {
    const auto* values = static_cast<const float*>(backing.value_buffer.get());
    for (std::size_t row = 0; row < backing.length; ++row) {
      if (!arrowBitmapHasValue(validity, row)) continue;
      out[row] = Value(static_cast<double>(values[row]));
    }
    return out;
  }
  if (backing.format == "g") {
    const auto* values = static_cast<const double*>(backing.value_buffer.get());
    for (std::size_t row = 0; row < backing.length; ++row) {
      if (!arrowBitmapHasValue(validity, row)) continue;
      out[row] = Value(values[row]);
    }
    return out;
  }
  if (backing.format == "u") {
    const auto* offsets = static_cast<const int32_t*>(backing.value_buffer.get());
    const auto* data = static_cast<const char*>(backing.extra_buffer.get());
    for (std::size_t row = 0; row < backing.length; ++row) {
      if (!arrowBitmapHasValue(validity, row)) continue;
      const auto begin = offsets[row];
      const auto end = offsets[row + 1];
      out[row] = Value(std::string(data + begin, data + end));
    }
    return out;
  }
  if (backing.format == "U") {
    const auto* offsets = static_cast<const int64_t*>(backing.value_buffer.get());
    const auto* data = static_cast<const char*>(backing.extra_buffer.get());
    for (std::size_t row = 0; row < backing.length; ++row) {
      if (!arrowBitmapHasValue(validity, row)) continue;
      const auto begin = offsets[row];
      const auto end = offsets[row + 1];
      out[row] = Value(std::string(data + begin, data + end));
    }
    return out;
  }
  if (backing.format.rfind("+w:", 0) == 0) {
    const auto* values = static_cast<const float*>(backing.child_value_buffer.get());
    for (std::size_t row = 0; row < backing.length; ++row) {
      if (!arrowBitmapHasValue(validity, row)) continue;
      std::vector<float> vec(static_cast<std::size_t>(backing.fixed_list_size));
      std::memcpy(vec.data(), values + row * static_cast<std::size_t>(backing.fixed_list_size),
                  sizeof(float) * static_cast<std::size_t>(backing.fixed_list_size));
      out[row] = Value(std::move(vec));
    }
    return out;
  }

  throw std::runtime_error("unsupported Arrow backing format: " + backing.format);
}

std::size_t valueColumnRowCount(const ValueColumnBuffer& buffer) {
  if (!buffer.values.empty()) {
    return buffer.values.size();
  }
  if (buffer.arrow_backing != nullptr) {
    return buffer.arrow_backing->length;
  }
  return 0;
}

bool valueColumnIsNullAt(const ValueColumnBuffer& buffer, std::size_t row_index) {
  if (!buffer.values.empty()) {
    return buffer.values[row_index].isNull();
  }
  if (buffer.arrow_backing == nullptr) {
    return true;
  }
  if (buffer.arrow_backing->format == "n") {
    return true;
  }
  return !arrowBitmapHasValue(static_cast<const uint8_t*>(buffer.arrow_backing->null_bitmap.get()),
                              row_index);
}

std::string_view valueColumnStringViewAt(const ValueColumnBuffer& buffer, std::size_t row_index) {
  if (!buffer.values.empty()) {
    if (buffer.values[row_index].isNull()) {
      return std::string_view();
    }
    return buffer.values[row_index].asString();
  }
  if (buffer.arrow_backing == nullptr || valueColumnIsNullAt(buffer, row_index)) {
    return std::string_view();
  }
  const auto& backing = *buffer.arrow_backing;
  if (backing.format == "u") {
    const auto* offsets = static_cast<const int32_t*>(backing.value_buffer.get());
    const auto* data = static_cast<const char*>(backing.extra_buffer.get());
    const auto begin = offsets[row_index];
    const auto end = offsets[row_index + 1];
    return std::string_view(data + begin, static_cast<std::size_t>(end - begin));
  }
  if (backing.format == "U") {
    const auto* offsets = static_cast<const int64_t*>(backing.value_buffer.get());
    const auto* data = static_cast<const char*>(backing.extra_buffer.get());
    const auto begin = offsets[row_index];
    const auto end = offsets[row_index + 1];
    return std::string_view(data + begin, static_cast<std::size_t>(end - begin));
  }
  return std::string_view();
}

std::string valueColumnStringAt(const ValueColumnBuffer& buffer, std::size_t row_index) {
  if (!buffer.values.empty()) {
    return buffer.values[row_index].toString();
  }
  if (buffer.arrow_backing == nullptr) {
    return std::string();
  }
  const auto& backing = *buffer.arrow_backing;
  if (valueColumnIsNullAt(buffer, row_index)) {
    return std::string("null");
  }
  if (backing.format == "u") {
    const auto* offsets = static_cast<const int32_t*>(backing.value_buffer.get());
    const auto* data = static_cast<const char*>(backing.extra_buffer.get());
    return std::string(data + offsets[row_index], data + offsets[row_index + 1]);
  }
  if (backing.format == "U") {
    const auto* offsets = static_cast<const int64_t*>(backing.value_buffer.get());
    const auto* data = static_cast<const char*>(backing.extra_buffer.get());
    return std::string(data + offsets[row_index], data + offsets[row_index + 1]);
  }
  if (backing.format == "b") {
    const auto* bits = static_cast<const uint8_t*>(backing.value_buffer.get());
    return ((bits[row_index >> 3] >> (row_index & 7)) & 0x01u) != 0 ? "1" : "0";
  }
  if (backing.format == "i") {
    return std::to_string(static_cast<const int32_t*>(backing.value_buffer.get())[row_index]);
  }
  if (backing.format == "I") {
    return std::to_string(static_cast<const uint32_t*>(backing.value_buffer.get())[row_index]);
  }
  if (backing.format == "l") {
    return std::to_string(static_cast<const int64_t*>(backing.value_buffer.get())[row_index]);
  }
  if (backing.format == "L") {
    return std::to_string(static_cast<const uint64_t*>(backing.value_buffer.get())[row_index]);
  }
  if (backing.format == "f") {
    return Value(static_cast<double>(static_cast<const float*>(backing.value_buffer.get())[row_index]))
        .toString();
  }
  if (backing.format == "g") {
    return Value(static_cast<const double*>(backing.value_buffer.get())[row_index]).toString();
  }
  return valueColumnValueAt(buffer, row_index).toString();
}

int64_t valueColumnInt64At(const ValueColumnBuffer& buffer, std::size_t row_index) {
  if (!buffer.values.empty()) {
    return buffer.values[row_index].asInt64();
  }
  if (buffer.arrow_backing == nullptr) {
    throw std::runtime_error("value column is not numeric");
  }
  const auto& backing = *buffer.arrow_backing;
  switch (backing.format.empty() ? '?' : backing.format[0]) {
    case 'b': {
      const auto* bits = static_cast<const uint8_t*>(backing.value_buffer.get());
      return static_cast<int64_t>(((bits[row_index >> 3] >> (row_index & 7)) & 0x01u) != 0);
    }
    case 'i':
      return static_cast<const int32_t*>(backing.value_buffer.get())[row_index];
    case 'I':
      return static_cast<int64_t>(static_cast<const uint32_t*>(backing.value_buffer.get())[row_index]);
    case 'l':
      return static_cast<const int64_t*>(backing.value_buffer.get())[row_index];
    case 'L':
      return static_cast<int64_t>(static_cast<const uint64_t*>(backing.value_buffer.get())[row_index]);
    case 'f':
      return static_cast<int64_t>(static_cast<const float*>(backing.value_buffer.get())[row_index]);
    case 'g':
      return static_cast<int64_t>(static_cast<const double*>(backing.value_buffer.get())[row_index]);
    default:
      return valueColumnValueAt(buffer, row_index).asInt64();
  }
}

double valueColumnDoubleAt(const ValueColumnBuffer& buffer, std::size_t row_index) {
  if (!buffer.values.empty()) {
    return buffer.values[row_index].asDouble();
  }
  if (buffer.arrow_backing == nullptr) {
    throw std::runtime_error("value column is not numeric");
  }
  const auto& backing = *buffer.arrow_backing;
  switch (backing.format.empty() ? '?' : backing.format[0]) {
    case 'b': {
      const auto* bits = static_cast<const uint8_t*>(backing.value_buffer.get());
      return static_cast<double>(((bits[row_index >> 3] >> (row_index & 7)) & 0x01u) != 0);
    }
    case 'i':
      return static_cast<const int32_t*>(backing.value_buffer.get())[row_index];
    case 'I':
      return static_cast<const uint32_t*>(backing.value_buffer.get())[row_index];
    case 'l':
      return static_cast<double>(static_cast<const int64_t*>(backing.value_buffer.get())[row_index]);
    case 'L':
      return static_cast<double>(static_cast<const uint64_t*>(backing.value_buffer.get())[row_index]);
    case 'f':
      return static_cast<const float*>(backing.value_buffer.get())[row_index];
    case 'g':
      return static_cast<const double*>(backing.value_buffer.get())[row_index];
    default:
      return valueColumnValueAt(buffer, row_index).asDouble();
  }
}

Value valueColumnValueAt(const ValueColumnBuffer& buffer, std::size_t row_index) {
  if (!buffer.values.empty()) {
    return buffer.values[row_index];
  }
  const auto& values = materializeValueBuffer(&buffer);
  return values[row_index];
}

const std::vector<Value>& ValueColumnView::values() const {
  return materializeValueBuffer(buffer);
}

std::shared_ptr<const ColumnarTable> snapshotColumnarCache(const Table* table) {
  if (table == nullptr) {
    throw std::invalid_argument("snapshotColumnarCache table is null");
  }
  std::lock_guard<std::mutex> lock(table->columnar_cache_mu);
  return table->columnar_cache;
}

std::shared_ptr<ColumnarTable> makeColumnarCache(const Table& table) {
  auto cache = std::make_shared<ColumnarTable>();
  cache->schema = table.schema;
  cache->columns.reserve(table.schema.fields.size());
  cache->arrow_formats.resize(table.schema.fields.size());
  cache->row_count = table.rows.size();
  cache->batch_row_counts.push_back(table.rows.size());
  for (std::size_t column_index = 0; column_index < table.schema.fields.size(); ++column_index) {
    ValueColumnBuffer column;
    column.values.reserve(table.rows.size());
    for (const auto& row : table.rows) {
      if (column_index >= row.size()) {
        throw std::runtime_error("column index out of range");
      }
      column.values.push_back(row[column_index]);
    }
    cache->columns.push_back(std::move(column));
  }
  return cache;
}

std::shared_ptr<const ColumnarTable> ensureColumnarCache(const Table* table) {
  if (table == nullptr) {
    throw std::invalid_argument("ensureColumnarCache table is null");
  }
  std::lock_guard<std::mutex> lock(table->columnar_cache_mu);
  if (!table->columnar_cache) {
    table->columnar_cache = makeColumnarCache(*table);
  }
  return table->columnar_cache;
}

ValueColumnView viewValueColumn(const Table& table, std::size_t column_index) {
  const auto cache = ensureColumnarCache(&table);
  if (column_index >= cache->columns.size()) {
    throw std::runtime_error("column index out of range");
  }
  return ValueColumnView{cache, &cache->columns[column_index]};
}

std::vector<ValueColumnView> viewValueColumns(const Table& table,
                                              const std::vector<std::size_t>& indices) {
  const auto cache = ensureColumnarCache(&table);
  std::vector<ValueColumnView> out;
  out.reserve(indices.size());
  for (const auto index : indices) {
    if (index >= cache->columns.size()) {
      throw std::runtime_error("column index out of range");
    }
    out.push_back(ValueColumnView{cache, &cache->columns[index]});
  }
  return out;
}

StringColumnBuffer makeNullStringColumn(std::size_t row_count) {
  StringColumnBuffer out;
  out.values.resize(row_count);
  out.is_null.assign(row_count, static_cast<uint8_t>(1));
  return out;
}

StringColumnBuffer makeConstantStringColumn(std::size_t row_count, std::string value) {
  StringColumnBuffer out;
  out.values.assign(row_count, std::move(value));
  out.is_null.assign(row_count, 0);
  return out;
}

StringColumnBuffer materializeStringColumn(const Table& table, std::size_t column_index) {
  const auto value_column = viewValueColumn(table, column_index);
  StringColumnBuffer out;
  const auto row_count = valueColumnRowCount(*value_column.buffer);
  out.values.resize(row_count);
  out.is_null.assign(row_count, 0);
  for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
    if (valueColumnIsNullAt(*value_column.buffer, row_index)) {
      out.is_null[row_index] = 1;
      continue;
    }
    if (value_column.buffer->arrow_backing != nullptr &&
        (value_column.buffer->arrow_backing->format == "u" ||
         value_column.buffer->arrow_backing->format == "U")) {
      const auto view = valueColumnStringViewAt(*value_column.buffer, row_index);
      out.values[row_index].assign(view.data(), view.size());
    } else {
      out.values[row_index] = valueColumnStringAt(*value_column.buffer, row_index);
    }
  }
  return out;
}

Int64ColumnBuffer makeNullInt64Column(std::size_t row_count) {
  Int64ColumnBuffer out;
  out.values.resize(row_count);
  out.is_null.assign(row_count, static_cast<uint8_t>(1));
  return out;
}

Int64ColumnBuffer makeConstantInt64Column(std::size_t row_count, int64_t value) {
  Int64ColumnBuffer out;
  out.values.assign(row_count, value);
  out.is_null.assign(row_count, 0);
  return out;
}

Int64ColumnBuffer materializeInt64Column(const Table& table, std::size_t column_index) {
  const auto value_column = viewValueColumn(table, column_index);
  Int64ColumnBuffer out;
  const auto row_count = valueColumnRowCount(*value_column.buffer);
  out.values.resize(row_count);
  out.is_null.assign(row_count, 0);
  for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
    if (valueColumnIsNullAt(*value_column.buffer, row_index)) {
      out.is_null[row_index] = 1;
      continue;
    }
    out.values[row_index] = valueColumnInt64At(*value_column.buffer, row_index);
  }
  return out;
}

ValueColumnBuffer materializeValueColumn(const Table& table, std::size_t column_index) {
  return *viewValueColumn(table, column_index).buffer;
}

DoubleColumnBuffer makeNullDoubleColumn(std::size_t row_count) {
  DoubleColumnBuffer out;
  out.values.resize(row_count);
  out.is_null.assign(row_count, static_cast<uint8_t>(1));
  return out;
}

DoubleColumnBuffer materializeDoubleColumn(const Table& table, std::size_t column_index) {
  const auto value_column = viewValueColumn(table, column_index);
  DoubleColumnBuffer out;
  out.values.resize(value_column.values().size());
  out.is_null.assign(value_column.values().size(), 0);
  for (std::size_t row_index = 0; row_index < value_column.values().size(); ++row_index) {
    const auto& value = value_column.values()[row_index];
    if (value.isNull()) {
      out.is_null[row_index] = 1;
      continue;
    }
    try {
      out.values[row_index] = valueAsDouble(value);
    } catch (...) {
      out.is_null[row_index] = 1;
      throw;
    }
  }
  return out;
}

std::vector<ValueColumnBuffer> materializeValueColumns(const Table& table,
                                                       const std::vector<std::size_t>& indices) {
  const auto columns = viewValueColumns(table, indices);
  std::vector<ValueColumnBuffer> out;
  out.reserve(columns.size());
  for (const auto& column : columns) {
    out.push_back(*column.buffer);
  }
  return out;
}

std::vector<std::string> materializeSerializedKeys(const Table& table,
                                                   const std::vector<std::size_t>& indices) {
  const auto columns = viewValueColumns(table, indices);
  std::vector<std::string> out;
  const auto row_count = columns.empty() ? table.rowCount() : valueColumnRowCount(*columns.front().buffer);
  out.reserve(row_count);
  for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
    std::string key;
    for (std::size_t i = 0; i < columns.size(); ++i) {
      if (i > 0) {
        key.push_back(kGroupDelim);
      }
      key += valueColumnStringAt(*columns[i].buffer, row_index);
    }
    out.push_back(std::move(key));
  }
  return out;
}

std::unordered_map<std::string, std::vector<std::size_t>> buildHashBuckets(
    const std::vector<std::string>& keys) {
  std::unordered_map<std::string, std::vector<std::size_t>> buckets;
  for (std::size_t row_index = 0; row_index < keys.size(); ++row_index) {
    buckets[keys[row_index]].push_back(row_index);
  }
  return buckets;
}

RowSelection vectorizedFilterSelection(const ValueColumnBuffer& input, const Value& rhs,
                                       bool (*pred)(const Value& lhs, const Value& rhs)) {
  RowSelection out;
  const auto row_count = valueColumnRowCount(input);
  out.selected.assign(row_count, 0);
  for (std::size_t i = 0; i < row_count; ++i) {
    if (!pred(valueColumnValueAt(input, i), rhs)) {
      continue;
    }
    out.selected[i] = 1;
    ++out.selected_count;
  }
  return out;
}

RowSelection vectorizedFilterSelection(const ValueColumnView& input, const Value& rhs,
                                       bool (*pred)(const Value& lhs, const Value& rhs)) {
  return vectorizedFilterSelection(*input.buffer, rhs, pred);
}

RowSelection vectorizedFilterSelection(const ValueColumnBuffer& input, const Value& rhs,
                                       const std::string& op) {
  return vectorizedFilterSelection(input, rhs, op, 0);
}

RowSelection vectorizedFilterSelection(const ValueColumnView& input, const Value& rhs,
                                       const std::string& op) {
  return vectorizedFilterSelection(*input.buffer, rhs, op, 0);
}

RowSelection vectorizedFilterSelection(const ValueColumnBuffer& input, const Value& rhs,
                                       const std::string& op, std::size_t max_selected) {
  RowSelection out;
  const auto row_count = valueColumnRowCount(input);
  out.selected.assign(row_count, 0);
  const bool bounded = max_selected != 0;

  if (rhs.isNull() || input.arrow_backing == nullptr) {
    for (std::size_t i = 0; i < row_count; ++i) {
      const auto lhs = valueColumnValueAt(input, i);
      if (!tryMatchesValueCompare(lhs, rhs, op)) {
        continue;
      }
      out.selected[i] = 1;
      ++out.selected_count;
      if (bounded && out.selected_count >= max_selected) {
        break;
      }
    }
    return out;
  }

  if (rhs.type() == DataType::String &&
      (input.arrow_backing->format == "u" || input.arrow_backing->format == "U")) {
    const auto rhs_view = std::string_view(rhs.asString());
    for (std::size_t i = 0; i < row_count; ++i) {
      if (valueColumnIsNullAt(input, i)) {
        continue;
      }
      if (!matchesCompareOp(compareScalar(valueColumnStringViewAt(input, i), rhs_view), op)) {
        continue;
      }
      out.selected[i] = 1;
      ++out.selected_count;
      if (bounded && out.selected_count >= max_selected) {
        break;
      }
    }
    return out;
  }

  if (rhs.isNumber()) {
    switch (input.arrow_backing->format.empty() ? '?' : input.arrow_backing->format[0]) {
      case 'b':
      case 'i':
      case 'I':
      case 'l':
      case 'L':
      case 'f':
      case 'g': {
        DoubleColumnBuffer numeric;
        numeric.values.resize(row_count);
        numeric.is_null.assign(row_count, 0);
        for (std::size_t i = 0; i < row_count; ++i) {
          if (valueColumnIsNullAt(input, i)) {
            numeric.is_null[i] = 1;
            continue;
          }
          numeric.values[i] = valueColumnDoubleAt(input, i);
        }
        auto selected = simdDispatch().select_double(
            numeric.values.data(), numeric.is_null.data(), row_count, rhs.asDouble(),
            toNumericCompareOp(op), max_selected);
        out.selected = std::move(selected.selected);
        out.selected_count = selected.selected_count;
        return out;
      }
      default:
        break;
    }
  }

  for (std::size_t i = 0; i < row_count; ++i) {
    const auto lhs = valueColumnValueAt(input, i);
    if (!tryMatchesValueCompare(lhs, rhs, op)) {
      continue;
    }
    out.selected[i] = 1;
    ++out.selected_count;
    if (bounded && out.selected_count >= max_selected) {
      break;
    }
  }
  return out;
}

RowSelection vectorizedFilterSelection(const ValueColumnView& input, const Value& rhs,
                                       const std::string& op, std::size_t max_selected) {
  return vectorizedFilterSelection(*input.buffer, rhs, op, max_selected);
}

Table projectTable(const Table& table, const std::vector<std::size_t>& indices,
                   const std::vector<std::string>& aliases, bool materialize_rows) {
  Table out;
  out.schema.fields.reserve(indices.size());
  for (std::size_t i = 0; i < indices.size(); ++i) {
    const auto index = indices[i];
    if (index >= table.schema.fields.size()) {
      throw std::runtime_error("select index out of range");
    }
    out.schema.fields.push_back(i < aliases.size() && !aliases[i].empty()
                                    ? aliases[i]
                                    : table.schema.fields[index]);
    out.schema.index[out.schema.fields.back()] = i;
  }
  if (const auto cache_in = snapshotColumnarCache(&table)) {
    auto cache = std::make_shared<ColumnarTable>();
    cache->schema = out.schema;
    cache->columns.reserve(indices.size());
    cache->arrow_formats.reserve(indices.size());
    cache->batch_row_counts = cache_in->batch_row_counts;
    cache->row_count = table.rowCount();
    for (const auto index : indices) {
      cache->columns.push_back(cache_in->columns[index]);
      if (index < cache_in->arrow_formats.size()) {
        cache->arrow_formats.push_back(cache_in->arrow_formats[index]);
      } else {
        cache->arrow_formats.emplace_back();
      }
    }
    out.columnar_cache = std::move(cache);
    if (materialize_rows) {
      const auto row_count =
          cache_in->row_count > 0 ? cache_in->row_count : table.rowCount();
      out.rows.reserve(row_count);
      for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
        Row projected;
        projected.reserve(indices.size());
        for (const auto index : indices) {
          const auto& values = materializeValueBuffer(&cache_in->columns[index]);
          projected.push_back(values[row_index]);
        }
        out.rows.push_back(std::move(projected));
      }
    }
  } else {
    out.rows.reserve(table.rows.size());
    for (const auto& row : table.rows) {
      Row projected;
      projected.reserve(indices.size());
      for (const auto index : indices) {
        if (index >= row.size()) {
          throw std::runtime_error("select index out of range");
        }
        projected.push_back(row[index]);
      }
      out.rows.push_back(std::move(projected));
    }
  }
  return out;
}

Table filterTable(const Table& table, const RowSelection& selection, bool materialize_rows) {
  if (selection.selected.size() != table.rowCount()) {
    throw std::runtime_error("selection size mismatch");
  }
  Table out(table.schema, {});
  if (const auto cache_in = snapshotColumnarCache(&table)) {
    auto cache = std::make_shared<ColumnarTable>();
    cache->schema = out.schema;
    cache->columns.reserve(cache_in->columns.size());
    cache->arrow_formats = cache_in->arrow_formats;
    cache->row_count = selection.selected_count;
    if (selection.selected_count > 0) {
      cache->batch_row_counts.push_back(selection.selected_count);
    }
    for (const auto& input_column : cache_in->columns) {
      ValueColumnBuffer output_column;
      output_column.values.reserve(selection.selected_count);
      const auto& values = materializeValueBuffer(&input_column);
      for (std::size_t row_index = 0; row_index < values.size(); ++row_index) {
        if (selection.selected[row_index] != 0) {
          output_column.values.push_back(values[row_index]);
        }
      }
      cache->columns.push_back(std::move(output_column));
    }
    out.columnar_cache = std::move(cache);
    if (materialize_rows) {
      out.rows.reserve(selection.selected_count);
      for (std::size_t row_index = 0; row_index < selection.selected.size(); ++row_index) {
        if (selection.selected[row_index] == 0) {
          continue;
        }
        Row row;
        row.reserve(cache_in->columns.size());
        for (const auto& input_column : cache_in->columns) {
          const auto& values = materializeValueBuffer(&input_column);
          row.push_back(values[row_index]);
        }
        out.rows.push_back(std::move(row));
      }
    }
  } else {
    out.rows.reserve(selection.selected_count);
    for (std::size_t row_index = 0; row_index < table.rows.size(); ++row_index) {
      if (selection.selected[row_index] == 0) {
        continue;
      }
      out.rows.push_back(table.rows[row_index]);
    }
  }
  return out;
}

Table limitTable(const Table& table, std::size_t limit, bool materialize_rows) {
  Table out(table.schema, {});
  const auto row_count = std::min(table.rowCount(), limit);
  if (const auto cache_in = snapshotColumnarCache(&table)) {
    auto cache = std::make_shared<ColumnarTable>();
    cache->schema = out.schema;
    cache->columns.reserve(cache_in->columns.size());
    cache->arrow_formats = cache_in->arrow_formats;
    cache->row_count = row_count;
    if (row_count > 0) {
      cache->batch_row_counts.push_back(row_count);
    }
    for (const auto& input_column : cache_in->columns) {
      ValueColumnBuffer output_column;
      output_column.values.reserve(row_count);
      const auto& values = materializeValueBuffer(&input_column);
      for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
        output_column.values.push_back(values[row_index]);
      }
      cache->columns.push_back(std::move(output_column));
    }
    out.columnar_cache = std::move(cache);
    if (materialize_rows) {
      out.rows.reserve(row_count);
      for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
        Row row;
        row.reserve(cache_in->columns.size());
        for (const auto& input_column : cache_in->columns) {
          const auto& values = materializeValueBuffer(&input_column);
          row.push_back(values[row_index]);
        }
        out.rows.push_back(std::move(row));
      }
    }
  } else {
    out.rows.reserve(row_count);
    for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
      out.rows.push_back(table.rows[row_index]);
    }
  }
  return out;
}

Table sortTable(const Table& table, const std::vector<std::size_t>& indices,
                const std::vector<bool>& ascending) {
  if (!ascending.empty() && ascending.size() != indices.size()) {
    throw std::runtime_error("ORDER BY direction count mismatch");
  }
  const auto row_count = table.rowCount();
  if (indices.empty() || row_count < 2) {
    return table;
  }

  const auto columns = viewValueColumns(table, indices);
  std::vector<bool> directions = ascending;
  if (directions.empty()) {
    directions.assign(indices.size(), true);
  }

  std::vector<std::size_t> row_order(row_count);
  for (std::size_t i = 0; i < row_order.size(); ++i) {
    row_order[i] = i;
  }

  std::stable_sort(
      row_order.begin(), row_order.end(),
      [&](std::size_t lhs_index, std::size_t rhs_index) {
        for (std::size_t column_index = 0; column_index < columns.size(); ++column_index) {
          const auto& lhs = columns[column_index].values()[lhs_index];
          const auto& rhs = columns[column_index].values()[rhs_index];
          if (lhs.isNull() && rhs.isNull()) {
            continue;
          }
          if (lhs.isNull()) {
            return false;
          }
          if (rhs.isNull()) {
            return true;
          }
          if (lhs == rhs) {
            continue;
          }
          return directions[column_index] ? (lhs < rhs) : (lhs > rhs);
        }
        return false;
      });

  Table out(table.schema, {});
  if (!table.rows.empty()) {
    out.rows.reserve(row_count);
    for (auto row_index : row_order) {
      out.rows.push_back(table.rows[row_index]);
    }
  }
  if (const auto cache_in = snapshotColumnarCache(&table)) {
    auto cache = std::make_shared<ColumnarTable>();
    cache->schema = out.schema;
    cache->arrow_formats = cache_in->arrow_formats;
    cache->batch_row_counts = {row_count};
    cache->row_count = row_count;
    cache->columns.reserve(cache_in->columns.size());
    for (std::size_t column_index = 0; column_index < cache_in->columns.size(); ++column_index) {
      const auto& input_column = cache_in->columns[column_index];
      ValueColumnBuffer output_column;
      output_column.values.reserve(row_order.size());
      for (auto row_index : row_order) {
        output_column.values.push_back(valueColumnValueAt(input_column, row_index));
      }
      cache->columns.push_back(std::move(output_column));
    }
    out.columnar_cache = std::move(cache);
  }
  return out;
}

Table concatenateTables(const std::vector<Table>& tables, bool materialize_rows) {
  Table out;
  std::size_t total_rows = 0;
  for (const auto& table : tables) {
    if (table.schema.fields.empty()) {
      continue;
    }
    if (out.schema.fields.empty()) {
      out.schema = table.schema;
    } else if (out.schema.fields != table.schema.fields) {
      throw std::runtime_error("concatenateTables schema mismatch");
    }
    total_rows += table.rowCount();
  }
  if (out.schema.fields.empty()) {
    return out;
  }

  auto cache = std::make_shared<ColumnarTable>();
  cache->schema = out.schema;
  cache->columns.resize(out.schema.fields.size());
  cache->arrow_formats.resize(out.schema.fields.size());
  cache->row_count = total_rows;

  for (const auto& table : tables) {
    if (table.schema.fields.empty() || table.rowCount() == 0) {
      continue;
    }
    const auto source_cache = ensureColumnarCache(&table);
    cache->batch_row_counts.push_back(table.rowCount());
    for (std::size_t column = 0; column < source_cache->columns.size(); ++column) {
      auto& out_values = cache->columns[column].values;
      const auto& source_values = materializeValueBuffer(&source_cache->columns[column]);
      out_values.insert(out_values.end(), source_values.begin(), source_values.end());
    }
  }
  out.columnar_cache = std::move(cache);
  if (materialize_rows) {
    materializeRows(&out);
  }
  return out;
}

std::vector<Value> vectorizedWindowStart(const ValueColumnBuffer& input, uint64_t window_ms) {
  if (window_ms == 0) {
    throw std::runtime_error("window size cannot be zero");
  }
  const auto window = static_cast<int64_t>(window_ms);
  const auto row_count = valueColumnRowCount(input);
  std::vector<Value> out(row_count);
  for (std::size_t i = 0; i < row_count; ++i) {
    if (valueColumnIsNullAt(input, i)) {
      continue;
    }
    const auto ts_ms = valueColumnEpochMillisAt(input, i);
    const auto window_start = (ts_ms / window) * window;
    out[i] = Value(window_start);
  }
  return out;
}

std::vector<Value> vectorizedWindowStart(const ValueColumnView& input, uint64_t window_ms) {
  return vectorizedWindowStart(*input.buffer, window_ms);
}

std::vector<Value> vectorizedStringLength(const StringColumnBuffer& input) {
  return unaryStringKernel(input, [](const std::string& value) {
    return Value(static_cast<int64_t>(value.size()));
  });
}

std::vector<Value> vectorizedStringLower(const StringColumnBuffer& input) {
  return unaryStringKernel(input, [](const std::string& value) { return Value(lowerString(value)); });
}

std::vector<Value> vectorizedStringUpper(const StringColumnBuffer& input) {
  return unaryStringKernel(input, [](const std::string& value) { return Value(upperString(value)); });
}

std::vector<Value> vectorizedStringTrim(const StringColumnBuffer& input) {
  return unaryStringKernel(input, [](const std::string& value) { return Value(trimString(value)); });
}

std::vector<Value> vectorizedStringLtrim(const StringColumnBuffer& input) {
  return unaryStringKernel(input, [](const std::string& value) { return Value(ltrimString(value)); });
}

std::vector<Value> vectorizedStringRtrim(const StringColumnBuffer& input) {
  return unaryStringKernel(input, [](const std::string& value) { return Value(rtrimString(value)); });
}

std::vector<Value> vectorizedStringReverse(const StringColumnBuffer& input) {
  return unaryStringKernel(input, [](const std::string& value) { return Value(reverseString(value)); });
}

std::vector<Value> vectorizedStringConcat(const std::vector<StringColumnBuffer>& inputs) {
  if (inputs.empty()) {
    throw std::runtime_error("CONCAT expects at least 1 argument");
  }
  const auto row_count = inputs.front().values.size();
  std::vector<Value> out(row_count);
  for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
    bool has_null = false;
    std::string result;
    for (const auto& input : inputs) {
      if (input.values.size() != row_count || input.is_null.size() != row_count) {
        throw std::runtime_error("columnar kernel input row count mismatch");
      }
      if (input.is_null[row_index] != 0) {
        has_null = true;
        break;
      }
      result += input.values[row_index];
    }
    if (!has_null) {
      out[row_index] = Value(std::move(result));
    }
  }
  return out;
}

std::vector<Value> vectorizedStringConcatWs(const std::vector<StringColumnBuffer>& inputs) {
  if (inputs.size() < 2) {
    throw std::runtime_error("CONCAT_WS expects at least 2 arguments");
  }
  const auto row_count = inputs.front().values.size();
  std::vector<Value> out(row_count);
  for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
    bool has_null = false;
    for (const auto& input : inputs) {
      if (input.values.size() != row_count || input.is_null.size() != row_count) {
        throw std::runtime_error("columnar kernel input row count mismatch");
      }
      if (input.is_null[row_index] != 0) {
        has_null = true;
        break;
      }
    }
    if (has_null) {
      continue;
    }
    const auto& delim = inputs[0].values[row_index];
    std::string result;
    for (std::size_t i = 1; i < inputs.size(); ++i) {
      if (i > 1) {
        result += delim;
      }
      result += inputs[i].values[row_index];
    }
    out[row_index] = Value(std::move(result));
  }
  return out;
}

std::vector<Value> vectorizedStringLeft(const StringColumnBuffer& input,
                                        const Int64ColumnBuffer& length) {
  if (input.values.size() != length.values.size() || input.is_null.size() != length.is_null.size() ||
      input.values.size() != input.is_null.size()) {
    throw std::runtime_error("columnar kernel input row count mismatch");
  }
  std::vector<Value> out(input.values.size());
  for (std::size_t i = 0; i < input.values.size(); ++i) {
    if (input.is_null[i] != 0 || length.is_null[i] != 0) {
      continue;
    }
    if (length.values[i] <= 0) {
      out[i] = Value(std::string());
      continue;
    }
    const auto byte_length = static_cast<std::size_t>(length.values[i]);
    out[i] = byte_length >= input.values[i].size()
                 ? Value(input.values[i])
                 : Value(input.values[i].substr(0, byte_length));
  }
  return out;
}

std::vector<Value> vectorizedStringRight(const StringColumnBuffer& input,
                                         const Int64ColumnBuffer& length) {
  if (input.values.size() != length.values.size() || input.is_null.size() != length.is_null.size() ||
      input.values.size() != input.is_null.size()) {
    throw std::runtime_error("columnar kernel input row count mismatch");
  }
  std::vector<Value> out(input.values.size());
  for (std::size_t i = 0; i < input.values.size(); ++i) {
    if (input.is_null[i] != 0 || length.is_null[i] != 0) {
      continue;
    }
    if (length.values[i] <= 0) {
      out[i] = Value(std::string());
      continue;
    }
    const auto byte_length = static_cast<std::size_t>(length.values[i]);
    out[i] = byte_length >= input.values[i].size()
                 ? Value(input.values[i])
                 : Value(input.values[i].substr(input.values[i].size() - byte_length));
  }
  return out;
}

std::vector<Value> vectorizedStringSubstr(const StringColumnBuffer& input,
                                          const Int64ColumnBuffer& start_one_based,
                                          const Int64ColumnBuffer* length) {
  if (input.values.size() != start_one_based.values.size() ||
      input.is_null.size() != start_one_based.is_null.size() ||
      input.values.size() != input.is_null.size()) {
    throw std::runtime_error("columnar kernel input row count mismatch");
  }
  if (length != nullptr &&
      (input.values.size() != length->values.size() || input.is_null.size() != length->is_null.size())) {
    throw std::runtime_error("columnar kernel input row count mismatch");
  }
  std::vector<Value> out(input.values.size());
  for (std::size_t i = 0; i < input.values.size(); ++i) {
    if (input.is_null[i] != 0 || start_one_based.is_null[i] != 0 ||
        (length != nullptr && length->is_null[i] != 0)) {
      continue;
    }
    const auto start = std::max<int64_t>(1, start_one_based.values[i]);
    if (start > static_cast<int64_t>(input.values[i].size())) {
      out[i] = Value(std::string());
      continue;
    }
    const auto byte_start = static_cast<std::size_t>(start - 1);
    if (length == nullptr) {
      out[i] = Value(input.values[i].substr(byte_start));
      continue;
    }
    if (length->values[i] <= 0) {
      out[i] = Value(std::string());
      continue;
    }
    const auto byte_length = static_cast<std::size_t>(length->values[i]);
    const auto end = std::min<std::size_t>(input.values[i].size(), byte_start + byte_length);
    out[i] = Value(input.values[i].substr(byte_start, end - byte_start));
  }
  return out;
}

std::vector<Value> vectorizedStringReplace(const StringColumnBuffer& input,
                                           const StringColumnBuffer& from,
                                           const StringColumnBuffer& to) {
  if (input.values.size() != from.values.size() || input.values.size() != to.values.size() ||
      input.is_null.size() != from.is_null.size() || input.is_null.size() != to.is_null.size() ||
      input.values.size() != input.is_null.size()) {
    throw std::runtime_error("columnar kernel input row count mismatch");
  }
  std::vector<Value> out(input.values.size());
  for (std::size_t i = 0; i < input.values.size(); ++i) {
    if (input.is_null[i] != 0 || from.is_null[i] != 0 || to.is_null[i] != 0) {
      continue;
    }
    out[i] = Value(replaceString(input.values[i], from.values[i], to.values[i]));
  }
  return out;
}

std::vector<Value> vectorizedStringPosition(const StringColumnBuffer& needle,
                                            const StringColumnBuffer& input) {
  if (needle.values.size() != input.values.size() || needle.is_null.size() != input.is_null.size() ||
      input.values.size() != input.is_null.size()) {
    throw std::runtime_error("columnar kernel input row count mismatch");
  }
  std::vector<Value> out(input.values.size());
  for (std::size_t i = 0; i < input.values.size(); ++i) {
    if (needle.is_null[i] != 0 || input.is_null[i] != 0) {
      continue;
    }
    const auto position = input.values[i].find(needle.values[i]);
    out[i] = position == std::string::npos ? Value(static_cast<int64_t>(0))
                                           : Value(static_cast<int64_t>(position + 1));
  }
  return out;
}

std::vector<Value> vectorizedAbs(const DoubleColumnBuffer& input) {
  std::vector<Value> out(input.values.size());
  for (std::size_t i = 0; i < input.values.size(); ++i) {
    if (input.is_null[i] != 0) {
      continue;
    }
    const auto abs_value = std::fabs(input.values[i]);
    out[i] = Value(abs_value);
  }
  return out;
}

std::vector<Value> vectorizedCeil(const DoubleColumnBuffer& input) {
  std::vector<Value> out(input.values.size());
  for (std::size_t i = 0; i < input.values.size(); ++i) {
    if (input.is_null[i] != 0) {
      continue;
    }
    out[i] = Value(std::ceil(input.values[i]));
  }
  return out;
}

std::vector<Value> vectorizedFloor(const DoubleColumnBuffer& input) {
  std::vector<Value> out(input.values.size());
  for (std::size_t i = 0; i < input.values.size(); ++i) {
    if (input.is_null[i] != 0) {
      continue;
    }
    out[i] = Value(std::floor(input.values[i]));
  }
  return out;
}

std::vector<Value> vectorizedRound(const DoubleColumnBuffer& input) {
  std::vector<Value> out(input.values.size());
  for (std::size_t i = 0; i < input.values.size(); ++i) {
    if (input.is_null[i] != 0) {
      continue;
    }
    out[i] = Value(std::round(input.values[i]));
  }
  return out;
}

std::vector<Value> vectorizedDateYear(const ValueColumnView& input) {
  std::vector<Value> out(input.values().size());
  for (std::size_t i = 0; i < input.values().size(); ++i) {
    if (input.values()[i].isNull()) {
      continue;
    }
    const int64_t millis = valueAsEpochMillis(input.values()[i]);
    std::time_t sec = static_cast<std::time_t>(millis / 1000);
    const auto tm_utc = toUtcTm(sec);
    out[i] = Value(static_cast<int64_t>(tm_utc.tm_year + 1900));
  }
  return out;
}

std::vector<Value> vectorizedDateMonth(const ValueColumnView& input) {
  std::vector<Value> out(input.values().size());
  for (std::size_t i = 0; i < input.values().size(); ++i) {
    if (input.values()[i].isNull()) {
      continue;
    }
    const int64_t millis = valueAsEpochMillis(input.values()[i]);
    std::time_t sec = static_cast<std::time_t>(millis / 1000);
    const auto tm_utc = toUtcTm(sec);
    out[i] = Value(static_cast<int64_t>(tm_utc.tm_mon + 1));
  }
  return out;
}

std::vector<Value> vectorizedDateDay(const ValueColumnView& input) {
  std::vector<Value> out(input.values().size());
  for (std::size_t i = 0; i < input.values().size(); ++i) {
    if (input.values()[i].isNull()) {
      continue;
    }
    const int64_t millis = valueAsEpochMillis(input.values()[i]);
    std::time_t sec = static_cast<std::time_t>(millis / 1000);
    const auto tm_utc = toUtcTm(sec);
    out[i] = Value(static_cast<int64_t>(tm_utc.tm_mday));
  }
  return out;
}

std::vector<Value> computeComputedColumnValues(Table* table, ComputedColumnKind function,
                                              const std::vector<ComputedColumnArg>& args) {
  if (table == nullptr) {
    throw std::invalid_argument("computed function table is null");
  }
  struct BoundComputedArg {
    bool is_literal = false;
    size_t source_column_index = 0;
    bool literal_is_null = false;
    bool literal_is_numeric = false;
    int64_t literal_int64 = 0;
    double literal_double = 0.0;
    std::string literal_string;
    std::string source_column_name;
  };

  std::vector<BoundComputedArg> bound;
  bound.reserve(args.size());
  for (const auto& arg : args) {
    BoundComputedArg item;
    item.is_literal = arg.is_literal;
    item.source_column_index = arg.source_column_index;
    item.source_column_name = arg.source_column_name;
    if (arg.is_literal) {
      item.literal_is_null = arg.literal.isNull();
      if (!item.literal_is_null) {
        item.literal_string = arg.literal.type() == DataType::String ? arg.literal.asString()
                                                                    : arg.literal.toString();
        if (arg.literal.isNumber()) {
          item.literal_is_numeric = true;
          item.literal_int64 = arg.literal.asInt64();
          item.literal_double = arg.literal.asDouble();
        }
      }
    }
    bound.push_back(std::move(item));
  }

  const auto row_count = table->rows.size();

  auto resolve_source_index = [&](const BoundComputedArg& arg) {
    if (arg.source_column_index != static_cast<size_t>(-1)) {
      return arg.source_column_index;
    }
    if (arg.source_column_name.empty()) {
      throw std::runtime_error("computed function argument has no source column");
    }
    return table->schema.indexOf(arg.source_column_name);
  };

  auto materialize_string_args = [&]() {
    std::vector<StringColumnBuffer> columns;
    columns.reserve(bound.size());
    for (const auto& arg : bound) {
      if (arg.is_literal) {
        if (arg.literal_is_null) {
          columns.push_back(makeNullStringColumn(row_count));
          continue;
        }
        columns.push_back(makeConstantStringColumn(row_count, arg.literal_string));
        continue;
      }
      columns.push_back(materializeStringColumn(*table, resolve_source_index(arg)));
    }
    return columns;
  };

  auto materialize_int64_arg = [&](const BoundComputedArg& arg) -> Int64ColumnBuffer {
    if (arg.is_literal) {
      if (arg.literal_is_null) {
        return makeNullInt64Column(row_count);
      }
      if (!arg.literal_is_numeric) {
        throw std::runtime_error("computed function argument must be numeric");
      }
      return makeConstantInt64Column(row_count, arg.literal_int64);
    }
    return materializeInt64Column(*table, resolve_source_index(arg));
  };

  auto materialize_double_arg = [&](const BoundComputedArg& arg) -> DoubleColumnBuffer {
    if (arg.is_literal) {
      if (arg.literal_is_null) {
        return makeNullDoubleColumn(row_count);
      }
      if (!arg.literal_is_numeric) {
        throw std::runtime_error("computed function argument must be numeric");
      }
      DoubleColumnBuffer out;
      out.values.assign(row_count, arg.literal_double);
      out.is_null.assign(row_count, 0);
      return out;
    }
    return materializeDoubleColumn(*table, resolve_source_index(arg));
  };

  auto materialize_date_field = [&](const BoundComputedArg& arg, int which) {
    std::vector<Value> output(row_count);
    for (std::size_t i = 0; i < row_count; ++i) {
      Value value;
      if (arg.is_literal) {
        if (arg.literal_is_null) {
          continue;
        }
        value = arg.literal_is_numeric ? Value(arg.literal_double) : Value(arg.literal_string);
      } else {
        const auto input = viewValueColumn(*table, resolve_source_index(arg));
        if (i >= input.values().size()) {
          throw std::runtime_error("computed function argument index out of range");
        }
        value = input.values()[i];
      }
      if (value.isNull()) {
        continue;
      }
      const int64_t millis = valueAsEpochMillis(value);
      std::time_t sec = static_cast<std::time_t>(millis / 1000);
      const auto tm_utc = toUtcTm(sec);
      int64_t field = 0;
      if (which == 0) {
        field = static_cast<int64_t>(tm_utc.tm_year + 1900);
      } else if (which == 1) {
        field = static_cast<int64_t>(tm_utc.tm_mon + 1);
      } else {
        field = static_cast<int64_t>(tm_utc.tm_mday);
      }
      output[i] = Value(field);
    }
    return output;
  };

  switch (function) {
    case ComputedColumnKind::Copy:
      throw std::runtime_error("computed function must not be COPY");
    case ComputedColumnKind::StringLength:
      if (bound.size() != 1) throw std::runtime_error("LENGTH expects 1 argument");
      return vectorizedStringLength(materialize_string_args()[0]);
    case ComputedColumnKind::StringLower:
      if (bound.size() != 1) throw std::runtime_error("LOWER expects 1 argument");
      return vectorizedStringLower(materialize_string_args()[0]);
    case ComputedColumnKind::StringUpper:
      if (bound.size() != 1) throw std::runtime_error("UPPER expects 1 argument");
      return vectorizedStringUpper(materialize_string_args()[0]);
    case ComputedColumnKind::StringTrim:
      if (bound.size() != 1) throw std::runtime_error("TRIM expects 1 argument");
      return vectorizedStringTrim(materialize_string_args()[0]);
    case ComputedColumnKind::StringConcat:
      return vectorizedStringConcat(materialize_string_args());
    case ComputedColumnKind::StringReverse:
      if (bound.size() != 1) throw std::runtime_error("REVERSE expects 1 argument");
      return vectorizedStringReverse(materialize_string_args()[0]);
    case ComputedColumnKind::StringConcatWs:
      return vectorizedStringConcatWs(materialize_string_args());
    case ComputedColumnKind::StringLeft:
      if (bound.size() != 2) throw std::runtime_error("LEFT requires 2 arguments");
      return vectorizedStringLeft(materialize_string_args()[0], materialize_int64_arg(bound[1]));
    case ComputedColumnKind::StringRight:
      if (bound.size() != 2) throw std::runtime_error("RIGHT requires 2 arguments");
      return vectorizedStringRight(materialize_string_args()[0], materialize_int64_arg(bound[1]));
    case ComputedColumnKind::StringPosition:
      if (bound.size() != 2) throw std::runtime_error("POSITION requires 2 arguments");
      {
        auto string_args = materialize_string_args();
        return vectorizedStringPosition(string_args[0], string_args[1]);
      }
    case ComputedColumnKind::StringSubstr:
      if (bound.size() < 2 || bound.size() > 3) throw std::runtime_error("SUBSTR expects 2 or 3 arguments");
      if (bound.size() == 2) {
        auto input = materialize_string_args();
        return vectorizedStringSubstr(input[0], materialize_int64_arg(bound[1]), nullptr);
      }
      {
        auto input = materialize_string_args();
        auto length = materialize_int64_arg(bound[2]);
        return vectorizedStringSubstr(input[0], materialize_int64_arg(bound[1]), &length);
      }
    case ComputedColumnKind::StringLtrim:
      if (bound.size() != 1) throw std::runtime_error("LTRIM/RTRIM requires 1 argument");
      return vectorizedStringLtrim(materialize_string_args()[0]);
    case ComputedColumnKind::StringRtrim:
      if (bound.size() != 1) throw std::runtime_error("LTRIM/RTRIM requires 1 argument");
      return vectorizedStringRtrim(materialize_string_args()[0]);
    case ComputedColumnKind::StringReplace:
      if (bound.size() != 3) throw std::runtime_error("REPLACE requires 3 arguments");
      {
        auto input = materialize_string_args();
        return vectorizedStringReplace(input[0], input[1], input[2]);
      }
    case ComputedColumnKind::NumericAbs:
      if (bound.size() != 1) throw std::runtime_error("ABS expects 1 argument");
      return vectorizedAbs(materialize_double_arg(bound[0]));
    case ComputedColumnKind::NumericCeil:
      if (bound.size() != 1) throw std::runtime_error("CEIL expects 1 argument");
      return vectorizedCeil(materialize_double_arg(bound[0]));
    case ComputedColumnKind::NumericFloor:
      if (bound.size() != 1) throw std::runtime_error("FLOOR expects 1 argument");
      return vectorizedFloor(materialize_double_arg(bound[0]));
    case ComputedColumnKind::NumericRound:
      if (bound.size() != 1) throw std::runtime_error("ROUND expects 1 argument");
      return vectorizedRound(materialize_double_arg(bound[0]));
    case ComputedColumnKind::DateYear:
      if (bound.size() != 1) throw std::runtime_error("YEAR expects 1 argument");
      if (!bound[0].is_literal) {
        return vectorizedDateYear(viewValueColumn(*table, resolve_source_index(bound[0])));
      }
      return materialize_date_field(bound[0], 0);
    case ComputedColumnKind::DateMonth:
      if (bound.size() != 1) throw std::runtime_error("MONTH expects 1 argument");
      if (!bound[0].is_literal) {
        return vectorizedDateMonth(viewValueColumn(*table, resolve_source_index(bound[0])));
      }
      return materialize_date_field(bound[0], 1);
    case ComputedColumnKind::DateDay:
      if (bound.size() != 1) throw std::runtime_error("DAY expects 1 argument");
      if (!bound[0].is_literal) {
        return vectorizedDateDay(viewValueColumn(*table, resolve_source_index(bound[0])));
      }
      return materialize_date_field(bound[0], 2);
  }
  throw std::runtime_error("unsupported computed function");
}

void appendColumn(Table* table, std::vector<Value>&& values, bool materialize_rows) {
  if (table == nullptr) {
    throw std::invalid_argument("appendColumn table is null");
  }
  const auto cache_snapshot = snapshotColumnarCache(table);
  if (materialize_rows && table->rows.empty() && cache_snapshot != nullptr) {
    materializeRows(table);
  }
  std::size_t expected_row_count = table->rowCount();
  if (expected_row_count == 0 && cache_snapshot != nullptr && cache_snapshot->columns.empty()) {
    expected_row_count = values.size();
  }
  if (values.size() != expected_row_count) {
    throw std::runtime_error("computed column row count mismatch");
  }
  const bool maintain_rows = !table->rows.empty();
  if (maintain_rows) {
    for (auto& row : table->rows) {
      row.reserve(row.size() + 1);
    }
  }
  const bool maintain_cache = cache_snapshot != nullptr;
  ValueColumnBuffer appended;
  if (maintain_rows) {
    if (maintain_cache) {
      appended.values = values;
    }
    for (std::size_t row_index = 0; row_index < table->rows.size(); ++row_index) {
      table->rows[row_index].push_back(std::move(values[row_index]));
    }
  } else if (maintain_cache) {
    appended.values = std::move(values);
  }
  std::lock_guard<std::mutex> lock(table->columnar_cache_mu);
  if (table->columnar_cache) {
    auto cache = table->columnar_cache;
    if (!cache.unique()) {
      cache = std::make_shared<ColumnarTable>(*cache);
    }
    cache->schema = table->schema;
    cache->row_count = expected_row_count;
    cache->columns.push_back(std::move(appended));
    if (cache->arrow_formats.size() < cache->columns.size()) {
      cache->arrow_formats.push_back("");
    }
    table->columnar_cache = std::move(cache);
  }
}

void appendColumn(Table* table, ValueColumnBuffer&& column, bool materialize_rows) {
  if (table == nullptr) {
    throw std::invalid_argument("appendColumn table is null");
  }
  const auto cache_snapshot = snapshotColumnarCache(table);
  if (materialize_rows && table->rows.empty() && cache_snapshot != nullptr) {
    materializeRows(table);
  }
  std::size_t expected_row_count = table->rowCount();
  if (expected_row_count == 0 && cache_snapshot != nullptr && cache_snapshot->columns.empty()) {
    expected_row_count = valueColumnRowCount(column);
  }
  if (valueColumnRowCount(column) != expected_row_count) {
    throw std::runtime_error("computed column row count mismatch");
  }
  const bool maintain_rows = !table->rows.empty();
  const bool maintain_cache = cache_snapshot != nullptr;
  if (maintain_rows) {
    const auto& values = materializeValueBuffer(&column);
    for (std::size_t row_index = 0; row_index < table->rows.size(); ++row_index) {
      table->rows[row_index].push_back(values[row_index]);
    }
  }
  ValueColumnBuffer appended = maintain_cache ? std::move(column) : ValueColumnBuffer{};
  if (maintain_rows && maintain_cache && appended.values.empty()) {
    appended.values = materializeValueBuffer(&appended);
  }
  std::lock_guard<std::mutex> lock(table->columnar_cache_mu);
  if (table->columnar_cache) {
    auto cache = table->columnar_cache;
    if (!cache.unique()) {
      cache = std::make_shared<ColumnarTable>(*cache);
    }
    cache->schema = table->schema;
    cache->row_count = expected_row_count;
    cache->columns.push_back(std::move(appended));
    if (cache->arrow_formats.size() < cache->columns.size()) {
      if (cache->columns.back().arrow_backing != nullptr) {
        cache->arrow_formats.push_back(cache->columns.back().arrow_backing->format);
      } else {
        cache->arrow_formats.push_back("");
      }
    }
    table->columnar_cache = std::move(cache);
  }
}

void appendNamedColumn(Table* table, const std::string& column_name, std::vector<Value>&& values,
                       bool materialize_rows) {
  if (table == nullptr) {
    throw std::invalid_argument("appendNamedColumn table is null");
  }
  if (!table->columnar_cache) {
    table->columnar_cache = makeColumnarCache(*table);
  }
  table->schema.fields.push_back(column_name);
  table->schema.index[column_name] = table->schema.fields.size() - 1;
  appendColumn(table, std::move(values), materialize_rows);
}

void appendNamedColumn(Table* table, const std::string& column_name, ValueColumnBuffer&& column,
                       bool materialize_rows) {
  if (table == nullptr) {
    throw std::invalid_argument("appendNamedColumn table is null");
  }
  if (!table->columnar_cache) {
    table->columnar_cache = makeColumnarCache(*table);
  }
  table->schema.fields.push_back(column_name);
  table->schema.index[column_name] = table->schema.fields.size() - 1;
  appendColumn(table, std::move(column), materialize_rows);
}

}  // namespace dataflow
