#include "src/dataflow/core/execution/columnar_batch.h"

#include <algorithm>
#include <cctype>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <stdexcept>

namespace dataflow {

namespace {

constexpr char kGroupDelim = '\x1f';

std::string valueAsString(const Value& value) {
  if (value.type() == DataType::Nil) {
    return std::string();
  }
  if (value.type() == DataType::String) {
    return value.asString();
  }
  return value.toString();
}

int64_t valueAsInt64(const Value& value) {
  if (value.type() == DataType::Nil) {
    throw std::runtime_error("string function argument cannot be null");
  }
  if (!value.isNumber()) {
    throw std::runtime_error("string function argument must be numeric");
  }
  return value.asInt64();
}

int64_t valueAsEpochMillis(const Value& value) {
  if (value.type() == DataType::Nil) {
    throw std::runtime_error("window assign source column cannot be null");
  }
  if (value.isNumber()) {
    return value.asInt64();
  }
  const auto raw = value.toString();
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
  out.values.resize(value_column.values().size());
  out.is_null.assign(value_column.values().size(), 0);
  for (std::size_t row_index = 0; row_index < value_column.values().size(); ++row_index) {
    const auto& value = value_column.values()[row_index];
    if (value.isNull()) {
      out.is_null[row_index] = 1;
      continue;
    }
    out.values[row_index] = valueAsString(value);
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
  out.values.resize(value_column.values().size());
  out.is_null.assign(value_column.values().size(), 0);
  for (std::size_t row_index = 0; row_index < value_column.values().size(); ++row_index) {
    const auto& value = value_column.values()[row_index];
    if (value.isNull()) {
      out.is_null[row_index] = 1;
      continue;
    }
    out.values[row_index] = valueAsInt64(value);
  }
  return out;
}

ValueColumnBuffer materializeValueColumn(const Table& table, std::size_t column_index) {
  return *viewValueColumn(table, column_index).buffer;
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
  const auto row_count = columns.empty() ? table.rows.size() : columns.front().values().size();
  out.reserve(row_count);
  for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
    std::string key;
    for (std::size_t i = 0; i < columns.size(); ++i) {
      if (i > 0) {
        key.push_back(kGroupDelim);
      }
      key += columns[i].values()[row_index].toString();
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
  out.selected.assign(input.values.size(), 0);
  for (std::size_t i = 0; i < input.values.size(); ++i) {
    if (!pred(input.values[i], rhs)) {
      continue;
    }
    out.selected[i] = 1;
    ++out.selected_count;
  }
  return out;
}

RowSelection vectorizedFilterSelection(const ValueColumnView& input, const Value& rhs,
                                       bool (*pred)(const Value& lhs, const Value& rhs)) {
  RowSelection out;
  out.selected.assign(input.values().size(), 0);
  for (std::size_t i = 0; i < input.values().size(); ++i) {
    if (!pred(input.values()[i], rhs)) {
      continue;
    }
    out.selected[i] = 1;
    ++out.selected_count;
  }
  return out;
}

Table projectTable(const Table& table, const std::vector<std::size_t>& indices,
                   const std::vector<std::string>& aliases) {
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
  if (const auto cache_in = snapshotColumnarCache(&table)) {
    auto cache = std::make_shared<ColumnarTable>();
    cache->schema = out.schema;
    cache->columns.reserve(indices.size());
    for (const auto index : indices) {
      cache->columns.push_back(cache_in->columns[index]);
    }
    out.columnar_cache = std::move(cache);
  }
  return out;
}

Table filterTable(const Table& table, const RowSelection& selection) {
  if (selection.selected.size() != table.rows.size()) {
    throw std::runtime_error("selection size mismatch");
  }
  Table out(table.schema, {});
  out.rows.reserve(selection.selected_count);
  for (std::size_t row_index = 0; row_index < table.rows.size(); ++row_index) {
    if (selection.selected[row_index] == 0) {
      continue;
    }
    out.rows.push_back(table.rows[row_index]);
  }
  if (const auto cache_in = snapshotColumnarCache(&table)) {
    auto cache = std::make_shared<ColumnarTable>();
    cache->schema = out.schema;
    cache->columns.reserve(cache_in->columns.size());
    for (const auto& input_column : cache_in->columns) {
      ValueColumnBuffer output_column;
      output_column.values.reserve(selection.selected_count);
      for (std::size_t row_index = 0; row_index < input_column.values.size(); ++row_index) {
        if (selection.selected[row_index] != 0) {
          output_column.values.push_back(input_column.values[row_index]);
        }
      }
      cache->columns.push_back(std::move(output_column));
    }
    out.columnar_cache = std::move(cache);
  }
  return out;
}

Table limitTable(const Table& table, std::size_t limit) {
  Table out(table.schema, {});
  const auto row_count = std::min(table.rows.size(), limit);
  out.rows.reserve(row_count);
  for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
    out.rows.push_back(table.rows[row_index]);
  }
  if (const auto cache_in = snapshotColumnarCache(&table)) {
    auto cache = std::make_shared<ColumnarTable>();
    cache->schema = out.schema;
    cache->columns.reserve(cache_in->columns.size());
    for (const auto& input_column : cache_in->columns) {
      ValueColumnBuffer output_column;
      output_column.values.reserve(row_count);
      for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
        output_column.values.push_back(input_column.values[row_index]);
      }
      cache->columns.push_back(std::move(output_column));
    }
    out.columnar_cache = std::move(cache);
  }
  return out;
}

std::vector<Value> vectorizedWindowStart(const ValueColumnBuffer& input, uint64_t window_ms) {
  if (window_ms == 0) {
    throw std::runtime_error("window size cannot be zero");
  }
  const auto window = static_cast<int64_t>(window_ms);
  std::vector<Value> out(input.values.size());
  for (std::size_t i = 0; i < input.values.size(); ++i) {
    const auto ts_ms = valueAsEpochMillis(input.values[i]);
    const auto window_start = (ts_ms / window) * window;
    out[i] = Value(window_start);
  }
  return out;
}

std::vector<Value> vectorizedWindowStart(const ValueColumnView& input, uint64_t window_ms) {
  if (window_ms == 0) {
    throw std::runtime_error("window size cannot be zero");
  }
  const auto window = static_cast<int64_t>(window_ms);
  std::vector<Value> out(input.values().size());
  for (std::size_t i = 0; i < input.values().size(); ++i) {
    const auto ts_ms = valueAsEpochMillis(input.values()[i]);
    const auto window_start = (ts_ms / window) * window;
    out[i] = Value(window_start);
  }
  return out;
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

void appendColumn(Table* table, std::vector<Value>&& values) {
  if (table == nullptr) {
    throw std::invalid_argument("appendColumn table is null");
  }
  if (values.size() != table->rows.size()) {
    throw std::runtime_error("computed column row count mismatch");
  }
  for (auto& row : table->rows) {
    row.reserve(row.size() + 1);
  }
  ValueColumnBuffer appended;
  const bool maintain_cache = snapshotColumnarCache(table) != nullptr;
  if (maintain_cache) {
    appended.values.reserve(values.size());
  }
  for (std::size_t row_index = 0; row_index < table->rows.size(); ++row_index) {
    if (maintain_cache) {
      appended.values.push_back(values[row_index]);
    }
    table->rows[row_index].push_back(std::move(values[row_index]));
  }
  std::lock_guard<std::mutex> lock(table->columnar_cache_mu);
  if (table->columnar_cache) {
    auto cache = table->columnar_cache;
    if (!cache.unique()) {
      cache = std::make_shared<ColumnarTable>(*cache);
    }
    cache->schema = table->schema;
    cache->columns.push_back(std::move(appended));
    table->columnar_cache = std::move(cache);
  }
}

void appendNamedColumn(Table* table, const std::string& column_name, std::vector<Value>&& values) {
  if (table == nullptr) {
    throw std::invalid_argument("appendNamedColumn table is null");
  }
  if (!table->columnar_cache) {
    table->columnar_cache = makeColumnarCache(*table);
  }
  table->schema.fields.push_back(column_name);
  table->schema.index[column_name] = table->schema.fields.size() - 1;
  appendColumn(table, std::move(values));
}

}  // namespace dataflow
