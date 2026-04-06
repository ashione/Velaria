#include "src/dataflow/core/execution/csv.h"

#include <array>
#include <charconv>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <unordered_map>

#include "src/dataflow/core/execution/columnar_batch.h"

namespace dataflow {

namespace {

constexpr std::size_t kReadAllColumns = static_cast<std::size_t>(-1);
constexpr std::size_t kCsvReadChunkSize = 1 << 16;

bool isIntLexically(std::string_view s) {
  if (s.empty()) return false;
  std::size_t pos = 0;
  if (s[0] == '-' || s[0] == '+') pos = 1;
  if (pos >= s.size()) return false;
  for (; pos < s.size(); ++pos) {
    if (s[pos] < '0' || s[pos] > '9') return false;
  }
  return true;
}

enum class Int64ParseStatus {
  NotInteger,
  Parsed,
  Overflow,
};

Int64ParseStatus parseInt64(const std::string& s, int64_t* out) {
  if (!isIntLexically(s)) {
    return Int64ParseStatus::NotInteger;
  }
  const auto result = std::from_chars(s.data(), s.data() + s.size(), *out);
  if (result.ec == std::errc() && result.ptr == s.data() + s.size()) {
    return Int64ParseStatus::Parsed;
  }
  if (result.ec == std::errc::result_out_of_range) {
    return Int64ParseStatus::Overflow;
  }
  return Int64ParseStatus::NotInteger;
}

bool parseDouble(const std::string& s, double* out) {
  if (s.empty()) return false;
  char* end = nullptr;
  *out = std::strtod(s.c_str(), &end);
  return end != s.c_str() && *end == '\0';
}

Value parseCell(std::string cell) {
  if (cell.empty()) return Value();
  if (cell == "true" || cell == "TRUE") return Value(true);
  if (cell == "false" || cell == "FALSE") return Value(false);
  if (cell.size() >= 2 && cell.front() == '[' && cell.back() == ']') {
    const auto vec = Value::parseFixedVector(cell);
    if (!vec.empty()) {
      return Value(vec);
    }
  }
  int64_t int_value = 0;
  switch (parseInt64(cell, &int_value)) {
    case Int64ParseStatus::Parsed:
      return Value(int_value);
    case Int64ParseStatus::Overflow:
      return Value(std::move(cell));
    case Int64ParseStatus::NotInteger:
      break;
  }
  double double_value = 0.0;
  if (parseDouble(cell, &double_value)) {
    return Value(double_value);
  }
  return Value(std::move(cell));
}

std::string normalizeGroupKeyCell(const std::string& cell) {
  int64_t int_value = 0;
  switch (parseInt64(cell, &int_value)) {
    case Int64ParseStatus::Parsed:
      return std::to_string(int_value);
    case Int64ParseStatus::Overflow:
      return cell;
    case Int64ParseStatus::NotInteger:
      break;
  }
  double double_value = 0.0;
  if (parseDouble(cell, &double_value)) {
    std::ostringstream out;
    out.precision(17);
    out << double_value;
    return out.str();
  }
  return cell;
}

std::string encodeGroupKeyValue(const Value& value) {
  switch (value.type()) {
    case DataType::Nil:
      return "n:";
    case DataType::Bool:
      return value.asBool() ? "b:1" : "b:0";
    case DataType::Int64:
      return "i:" + std::to_string(value.asInt64());
    case DataType::Double: {
      std::ostringstream out;
      out.precision(17);
      out << value.asDouble();
      return "d:" + out.str();
    }
    case DataType::String:
      return "s:" + value.asString();
    case DataType::FixedVector:
      return "v:" + value.toString();
  }
  return "n:";
}

std::string encodeGroupKeyRow(const std::vector<Value>& key_values) {
  std::string out;
  for (const auto& value : key_values) {
    const auto encoded = encodeGroupKeyValue(value);
    out.append(std::to_string(encoded.size()));
    out.push_back(':');
    out.append(encoded);
  }
  return out;
}

bool matchesCompareOp(int compare_result, const std::string& op) {
  if (op == "=" || op == "==") return compare_result == 0;
  if (op == "!=") return compare_result != 0;
  if (op == "<") return compare_result < 0;
  if (op == ">") return compare_result > 0;
  if (op == "<=") return compare_result <= 0;
  if (op == ">=") return compare_result >= 0;
  throw std::invalid_argument("unsupported compare op: " + op);
}

bool tryMatchesValueFilter(const Value& lhs, const Value& rhs, const std::string& op) {
  try {
    return matchesCompareOp(lhs == rhs ? 0 : (lhs < rhs ? -1 : 1), op);
  } catch (...) {
    return false;
  }
}

bool tryMatchesRawCellFilter(std::string_view cell, const Value& rhs, const std::string& op) {
  if (rhs.type() == DataType::Bool) {
    const bool rhs_bool = rhs.asBool();
    if (cell == "true" || cell == "TRUE" || cell == "false" || cell == "FALSE") {
      const bool lhs_bool = (cell == "true" || cell == "TRUE");
      const int compare_result = lhs_bool == rhs_bool ? 0 : (lhs_bool ? 1 : -1);
      return matchesCompareOp(compare_result, op);
    }
  }

  if (rhs.type() == DataType::String) {
    const int compare_result = std::string_view(rhs.asString()) == cell
                                   ? 0
                                   : (cell < std::string_view(rhs.asString()) ? -1 : 1);
    return matchesCompareOp(compare_result, op);
  }

  int64_t int_value = 0;
  switch (parseInt64(std::string(cell), &int_value)) {
    case Int64ParseStatus::Parsed:
      return tryMatchesValueFilter(Value(int_value), rhs, op);
    case Int64ParseStatus::Overflow:
    case Int64ParseStatus::NotInteger:
      break;
  }

  double double_value = 0.0;
  if (parseDouble(std::string(cell), &double_value)) {
    return tryMatchesValueFilter(Value(double_value), rhs, op);
  }

  if (cell.empty()) {
    return tryMatchesValueFilter(Value(), rhs, op);
  }
  return tryMatchesValueFilter(Value(std::string(cell)), rhs, op);
}

template <typename RowConsumer>
void scanCsvRows(std::istream* input, char delimiter, std::size_t max_columns,
                 RowConsumer&& on_row) {
  if (input == nullptr) {
    throw std::invalid_argument("csv input is null");
  }
  std::array<char, kCsvReadChunkSize> chunk{};
  std::vector<std::string> row;
  if (max_columns == kReadAllColumns) {
    row.reserve(16);
  } else {
    row.reserve(max_columns);
  }
  std::string cell;
  cell.reserve(64);
  bool in_quotes = false;
  bool pending_quote = false;
  bool skip_next_lf = false;

  auto emit_cell = [&]() {
    if (max_columns == kReadAllColumns || row.size() < max_columns) {
      row.push_back(std::move(cell));
    }
    cell.clear();
  };
  auto emit_row = [&]() -> bool {
    emit_cell();
    auto out = std::move(row);
    row.clear();
    if (max_columns == kReadAllColumns) {
      row.reserve(16);
    } else {
      row.reserve(max_columns);
    }
    return on_row(std::move(out));
  };
  auto push_char = [&](char ch) {
    if (max_columns == kReadAllColumns || row.size() < max_columns) {
      cell.push_back(ch);
    }
  };

  while (input->good()) {
    input->read(chunk.data(), static_cast<std::streamsize>(chunk.size()));
    const std::streamsize read = input->gcount();
    if (read <= 0) {
      break;
    }
    for (std::streamsize i = 0; i < read; ++i) {
      char ch = chunk[static_cast<std::size_t>(i)];
      if (skip_next_lf) {
        skip_next_lf = false;
        if (ch == '\n') {
          continue;
        }
      }
      if (pending_quote) {
        if (ch == '"') {
          push_char('"');
          pending_quote = false;
          continue;
        }
        in_quotes = false;
        pending_quote = false;
      }
      if (ch == '"') {
        if (in_quotes) {
          pending_quote = true;
        } else {
          in_quotes = true;
        }
        continue;
      }
      if (!in_quotes && ch == delimiter) {
        emit_cell();
        continue;
      }
      if (!in_quotes && (ch == '\n' || ch == '\r')) {
        if (!emit_row()) {
          return;
        }
        if (ch == '\r') {
          skip_next_lf = true;
        }
        continue;
      }
      push_char(ch);
    }
  }
  if (pending_quote) {
    pending_quote = false;
    in_quotes = false;
  }
  if (!row.empty() || !cell.empty()) {
    (void)emit_row();
  }
}

template <typename RowStartFn, typename CellFn, typename RowEndFn>
void scanCsvCells(std::istream* input, char delimiter, std::size_t capture_until_column,
                  const std::vector<uint8_t>* capture_mask, RowStartFn&& on_row_start,
                  CellFn&& on_cell, RowEndFn&& on_row_end) {
  if (input == nullptr) {
    throw std::invalid_argument("csv input is null");
  }
  std::array<char, kCsvReadChunkSize> chunk{};
  std::string cell;
  cell.reserve(64);
  bool in_quotes = false;
  bool pending_quote = false;
  bool skip_next_lf = false;
  std::size_t column_index = 0;
  bool keep_cell = true;
  auto recompute_keep_cell = [&]() {
    keep_cell = column_index <= capture_until_column &&
                (capture_mask == nullptr || (*capture_mask)[column_index] != 0);
  };
  recompute_keep_cell();
  on_row_start();

  auto emit_cell = [&]() -> bool {
    if (column_index <= capture_until_column && keep_cell) {
      if (!on_cell(column_index, std::move(cell))) {
        return false;
      }
    }
    cell.clear();
    ++column_index;
    if (capture_mask != nullptr && column_index < capture_mask->size()) {
      recompute_keep_cell();
    } else {
      keep_cell = (capture_mask == nullptr && column_index <= capture_until_column);
    }
    return true;
  };
  auto emit_row = [&]() -> bool {
    if (!emit_cell()) {
      return false;
    }
    const std::size_t total_columns = column_index;
    column_index = 0;
    recompute_keep_cell();
    cell.clear();
    if (!on_row_end(total_columns)) {
      return false;
    }
    on_row_start();
    return true;
  };
  auto push_char = [&](char ch) {
    if (keep_cell) {
      cell.push_back(ch);
    }
  };

  while (input->good()) {
    input->read(chunk.data(), static_cast<std::streamsize>(chunk.size()));
    const std::streamsize read = input->gcount();
    if (read <= 0) {
      break;
    }
    for (std::streamsize i = 0; i < read; ++i) {
      char ch = chunk[static_cast<std::size_t>(i)];
      if (skip_next_lf) {
        skip_next_lf = false;
        if (ch == '\n') {
          continue;
        }
      }
      if (pending_quote) {
        if (ch == '"') {
          push_char('"');
          pending_quote = false;
          continue;
        }
        in_quotes = false;
        pending_quote = false;
      }
      if (ch == '"') {
        if (in_quotes) {
          pending_quote = true;
        } else {
          in_quotes = true;
        }
        continue;
      }
      if (!in_quotes && ch == delimiter) {
        if (!emit_cell()) {
          return;
        }
        continue;
      }
      if (!in_quotes && (ch == '\n' || ch == '\r')) {
        if (!emit_row()) {
          return;
        }
        if (ch == '\r') {
          skip_next_lf = true;
        }
        continue;
      }
      push_char(ch);
    }
  }
  if (pending_quote) {
    pending_quote = false;
    in_quotes = false;
  }
  if (column_index > 0 || !cell.empty()) {
    (void)emit_row();
  }
}


Schema readCsvHeader(std::ifstream* input, const std::string& path, char delimiter) {
  if (input == nullptr || !input->is_open()) {
    throw std::runtime_error("cannot open csv file: " + path);
  }
  std::vector<std::string> header;
  scanCsvRows(input, delimiter, kReadAllColumns, [&](std::vector<std::string>&& cells) {
    header = std::move(cells);
    return false;
  });
  return Schema(std::move(header));
}

Table loadCsvInternal(const std::string& path, char delimiter, bool materialize_rows,
                      const Schema* schema_hint,
                      const std::vector<std::size_t>* projected_columns) {
  Schema schema;
  if (schema_hint == nullptr) {
    std::ifstream header_input(path, std::ios::binary);
    schema = readCsvHeader(&header_input, path, delimiter);
  } else {
    schema = *schema_hint;
  }

  std::ifstream input(path, std::ios::binary);
  if (!input.is_open()) {
    throw std::runtime_error("cannot open csv file: " + path);
  }

  std::vector<uint8_t> projected_mask(schema.fields.size(), static_cast<uint8_t>(1));
  std::size_t last_required_column =
      schema.fields.empty() ? 0 : (schema.fields.size() - 1);
  if (projected_columns != nullptr && !projected_columns->empty()) {
    std::fill(projected_mask.begin(), projected_mask.end(), static_cast<uint8_t>(0));
    last_required_column = 0;
    for (const auto column_index : *projected_columns) {
      if (column_index >= projected_mask.size()) {
        throw std::runtime_error("projected csv column index out of range");
      }
      projected_mask[column_index] = 1;
      last_required_column = std::max(last_required_column, column_index);
    }
  }

  Table table;
  table.schema = schema;
  table.columnar_cache = std::make_shared<ColumnarTable>();
  table.columnar_cache->schema = table.schema;
  table.columnar_cache->columns.resize(table.schema.fields.size());
  table.columnar_cache->arrow_formats.resize(table.schema.fields.size());
  const std::size_t scanned_column_count = last_required_column + 1;
  bool skip_header = true;
  std::size_t row_start = 0;
  Row row;
  scanCsvCells(
      &input, delimiter, scanned_column_count - 1, &projected_mask,
      [&]() {
        row_start = table.columnar_cache->row_count;
        if (materialize_rows) {
          row.clear();
          row.reserve(table.schema.fields.size());
        }
      },
      [&](std::size_t column_index, std::string&& cell_value) {
        Value parsed = parseCell(std::move(cell_value));
        table.columnar_cache->columns[column_index].values.push_back(parsed);
        if (materialize_rows) {
          row.push_back(std::move(parsed));
        }
        return true;
      },
      [&](std::size_t total_columns) {
        if (skip_header) {
          skip_header = false;
          for (std::size_t i = 0; i < table.columnar_cache->columns.size(); ++i) {
            if (materialize_rows || projected_mask[i] != 0) {
              table.columnar_cache->columns[i].values.resize(row_start);
            }
          }
          return true;
        }
        const bool row_valid =
            materialize_rows ? (total_columns == table.schema.fields.size())
                             : (total_columns >= scanned_column_count);
        if (row_valid) {
          if (materialize_rows) {
            table.rows.push_back(row);
          }
          table.columnar_cache->row_count += 1;
        } else {
          for (std::size_t i = 0; i < table.columnar_cache->columns.size(); ++i) {
            if (materialize_rows || projected_mask[i] != 0) {
              table.columnar_cache->columns[i].values.resize(row_start);
            }
          }
        }
        return true;
      });

  if (table.columnar_cache->row_count > 0) {
    if (projected_columns != nullptr) {
      for (std::size_t column_index = 0; column_index < projected_mask.size(); ++column_index) {
        if (projected_mask[column_index] != 0) {
          continue;
        }
        auto backing = std::make_shared<ArrowColumnBacking>();
        backing->format = "n";
        backing->length = table.columnar_cache->row_count;
        backing->null_count = static_cast<int64_t>(table.columnar_cache->row_count);
        table.columnar_cache->columns[column_index].arrow_backing = std::move(backing);
      }
    }
    table.columnar_cache->batch_row_counts.push_back(table.columnar_cache->row_count);
  }
  return table;
}

}  // namespace

Schema read_csv_schema(const std::string& path, char delimiter) {
  std::ifstream input(path);
  return readCsvHeader(&input, path, delimiter);
}

Table load_csv(const std::string& path, char delimiter, bool materialize_rows) {
  return loadCsvInternal(path, delimiter, materialize_rows, nullptr, nullptr);
}

Table load_csv_projected(const std::string& path, const Schema& schema,
                         const std::vector<std::size_t>& projected_columns, char delimiter,
                         bool materialize_rows) {
  return loadCsvInternal(path, delimiter, materialize_rows, &schema, &projected_columns);
}

bool execute_csv_source_pushdown(const std::string& path, const Schema& schema,
                                 const SourcePushdownSpec& pushdown,
                                 char delimiter, bool materialize_rows, Table* out) {
  if (out == nullptr) {
    throw std::invalid_argument("csv source pushdown output is null");
  }
  if (pushdown.has_aggregate) {
    const SourceFilterPushdownSpec* filter = pushdown.filter.enabled ? &pushdown.filter : nullptr;
    if (!try_execute_csv_aggregate(path, schema, pushdown.aggregate.keys,
                                   pushdown.aggregate.aggregates, filter,
                                   delimiter, out)) {
      return false;
    }
    if (pushdown.limit != 0 && out->rowCount() > pushdown.limit) {
      *out = limitTable(*out, pushdown.limit, false);
    }
    return true;
  }
  if (!pushdown.filter.enabled && pushdown.limit == 0) {
    const bool require_all_columns =
        pushdown.projected_columns.empty() || pushdown.projected_columns.size() >= schema.fields.size();
    *out = require_all_columns
               ? load_csv(path, delimiter, materialize_rows)
               : load_csv_projected(path, schema, pushdown.projected_columns, delimiter,
                                    materialize_rows);
    return true;
  }

  if (pushdown.filter.enabled && pushdown.filter.column_index >= schema.fields.size()) {
    return false;
  }

  const bool require_all_columns =
      pushdown.projected_columns.empty() || pushdown.projected_columns.size() >= schema.fields.size();
  std::vector<uint8_t> projected_mask(schema.fields.size(), static_cast<uint8_t>(require_all_columns ? 1 : 0));
  std::size_t last_required_column =
      schema.fields.empty() ? 0 : (schema.fields.size() - 1);
  if (!require_all_columns) {
    last_required_column = 0;
    for (const auto column_index : pushdown.projected_columns) {
      if (column_index >= schema.fields.size()) {
        return false;
      }
      projected_mask[column_index] = 1;
      last_required_column = std::max(last_required_column, column_index);
    }
  }
  if (pushdown.filter.enabled) {
    projected_mask[pushdown.filter.column_index] = 1;
    last_required_column = std::max(last_required_column, pushdown.filter.column_index);
  }

  std::ifstream input(path, std::ios::binary);
  if (!input.is_open()) {
    throw std::runtime_error("cannot open csv file: " + path);
  }

  Table table;
  table.schema = schema;
  table.columnar_cache = std::make_shared<ColumnarTable>();
  table.columnar_cache->schema = table.schema;
  table.columnar_cache->columns.resize(table.schema.fields.size());
  table.columnar_cache->arrow_formats.resize(table.schema.fields.size());

  bool skip_header = true;
  std::size_t row_start = 0;
  Row row;
  bool filter_match = !pushdown.filter.enabled;
  scanCsvCells(
      &input, delimiter, last_required_column, &projected_mask,
      [&]() {
        row_start = table.columnar_cache->row_count;
        filter_match = !pushdown.filter.enabled;
        if (materialize_rows) {
          row.clear();
          row.reserve(table.schema.fields.size());
        }
      },
      [&](std::size_t column_index, std::string&& cell_value) {
        if (skip_header) {
          return true;
        }
        if (pushdown.filter.enabled && column_index == pushdown.filter.column_index) {
          filter_match =
              tryMatchesRawCellFilter(cell_value, pushdown.filter.value, pushdown.filter.op);
        }
        Value parsed = parseCell(std::move(cell_value));
        table.columnar_cache->columns[column_index].values.push_back(parsed);
        if (materialize_rows && (require_all_columns || projected_mask[column_index] != 0)) {
          row.push_back(std::move(parsed));
        }
        return true;
      },
      [&](std::size_t total_columns) {
        if (skip_header) {
          skip_header = false;
          for (std::size_t i = 0; i < table.columnar_cache->columns.size(); ++i) {
            if (projected_mask[i] != 0) {
              table.columnar_cache->columns[i].values.resize(row_start);
            }
          }
          return true;
        }
        if (total_columns < last_required_column + 1 || !filter_match) {
          for (std::size_t i = 0; i < table.columnar_cache->columns.size(); ++i) {
            if (projected_mask[i] != 0) {
              table.columnar_cache->columns[i].values.resize(row_start);
            }
          }
          return true;
        }
        if (materialize_rows) {
          table.rows.push_back(row);
        }
        table.columnar_cache->row_count += 1;
        if (pushdown.limit != 0 && table.columnar_cache->row_count >= pushdown.limit) {
          return false;
        }
        return true;
      });

  if (table.columnar_cache->row_count > 0) {
    for (std::size_t column_index = 0; column_index < projected_mask.size(); ++column_index) {
      if (projected_mask[column_index] != 0) {
        continue;
      }
      auto backing = std::make_shared<ArrowColumnBacking>();
      backing->format = "n";
      backing->length = table.columnar_cache->row_count;
      backing->null_count = static_cast<int64_t>(table.columnar_cache->row_count);
      table.columnar_cache->columns[column_index].arrow_backing = std::move(backing);
    }
    table.columnar_cache->batch_row_counts.push_back(table.columnar_cache->row_count);
  }
  *out = std::move(table);
  return true;
}

bool try_execute_csv_aggregate(const std::string& path, const Schema& schema,
                               const std::vector<std::size_t>& key_indices,
                               const std::vector<AggregateSpec>& aggs,
                               const SourceFilterPushdownSpec* filter,
                               char delimiter,
                               Table* out) {
  if (out == nullptr) {
    throw std::invalid_argument("csv aggregate output is null");
  }
  if (key_indices.empty() || aggs.empty()) {
    return false;
  }
  std::size_t last_required_column = 0;
  for (const auto key_index : key_indices) {
    if (key_index >= schema.fields.size()) {
      return false;
    }
    last_required_column = std::max(last_required_column, key_index);
  }
  if (filter != nullptr) {
    if (filter->column_index >= schema.fields.size()) {
      return false;
    }
    last_required_column = std::max(last_required_column, filter->column_index);
  }
  for (const auto& agg : aggs) {
    if (agg.function != AggregateFunction::Count &&
        (agg.value_index >= schema.fields.size())) {
      return false;
    }
    if (agg.function != AggregateFunction::Count) {
      last_required_column = std::max(last_required_column, agg.value_index);
    }
  }

  std::ifstream input(path);
  if (!input.is_open()) {
    throw std::runtime_error("cannot open csv file: " + path);
  }

  struct AggregateState {
    std::vector<double> sums;
    std::vector<std::size_t> counts;
    std::vector<double> mins;
    std::vector<bool> has_min;
    std::vector<bool> min_is_int;
    std::vector<int64_t> min_ints;
    std::vector<double> maxs;
    std::vector<bool> has_max;
    std::vector<bool> max_is_int;
    std::vector<int64_t> max_ints;
  };

  auto init_state = [&]() {
    AggregateState state;
    state.sums.assign(aggs.size(), 0.0);
    state.counts.assign(aggs.size(), 0);
    state.mins.assign(aggs.size(), 0.0);
    state.has_min.assign(aggs.size(), false);
    state.min_is_int.assign(aggs.size(), false);
    state.min_ints.assign(aggs.size(), 0);
    state.maxs.assign(aggs.size(), 0.0);
    state.has_max.assign(aggs.size(), false);
    state.max_is_int.assign(aggs.size(), false);
    state.max_ints.assign(aggs.size(), 0);
    return state;
  };

  struct AggregateEntry {
    std::vector<Value> key_values;
    AggregateState state;
  };

  std::unordered_map<std::string, std::size_t> entry_index;
  std::vector<AggregateEntry> ordered_entries;
  std::vector<uint8_t> capture_mask(last_required_column + 1, static_cast<uint8_t>(0));
  std::vector<std::size_t> key_position_by_column(last_required_column + 1,
                                                  static_cast<std::size_t>(-1));
  for (std::size_t i = 0; i < key_indices.size(); ++i) {
    capture_mask[key_indices[i]] = 1;
    key_position_by_column[key_indices[i]] = i;
  }
  if (filter != nullptr) {
    capture_mask[filter->column_index] = 1;
  }
  std::vector<std::vector<std::size_t>> agg_positions_by_column(last_required_column + 1);
  for (std::size_t i = 0; i < aggs.size(); ++i) {
    if (aggs[i].function == AggregateFunction::Count) {
      continue;
    }
    capture_mask[aggs[i].value_index] = 1;
    agg_positions_by_column[aggs[i].value_index].push_back(i);
  }

  bool skip_header = true;
  bool filter_match = (filter == nullptr);
  std::vector<std::string> key_cells(key_indices.size());
  std::vector<Value> key_values(key_indices.size());
  std::vector<double> numeric_values(aggs.size(), 0.0);
  std::vector<bool> numeric_present(aggs.size(), false);
  std::vector<bool> numeric_is_int(aggs.size(), false);
  std::vector<int64_t> int_values(aggs.size(), 0);

  scanCsvCells(
      &input, delimiter, last_required_column, &capture_mask,
      [&]() {
        filter_match = (filter == nullptr);
        for (auto& key_cell : key_cells) {
          key_cell.clear();
        }
        for (auto& key_value : key_values) {
          key_value = Value();
        }
        std::fill(numeric_values.begin(), numeric_values.end(), 0.0);
        std::fill(numeric_present.begin(), numeric_present.end(), false);
        std::fill(numeric_is_int.begin(), numeric_is_int.end(), false);
        std::fill(int_values.begin(), int_values.end(), int64_t(0));
      },
      [&](std::size_t column_index, std::string&& cell) {
        if (filter != nullptr && column_index == filter->column_index) {
          filter_match = tryMatchesRawCellFilter(cell, filter->value, filter->op);
        }
        if (column_index < key_position_by_column.size() &&
            key_position_by_column[column_index] != static_cast<std::size_t>(-1)) {
          key_cells[key_position_by_column[column_index]] = cell;
        }
        if (column_index < agg_positions_by_column.size()) {
          for (const auto agg_index : agg_positions_by_column[column_index]) {
            int64_t parsed_int = 0;
            switch (parseInt64(cell, &parsed_int)) {
              case Int64ParseStatus::Parsed:
                numeric_values[agg_index] = static_cast<double>(parsed_int);
                numeric_present[agg_index] = true;
                numeric_is_int[agg_index] = true;
                int_values[agg_index] = parsed_int;
                break;
              case Int64ParseStatus::Overflow:
              case Int64ParseStatus::NotInteger: {
                double parsed_double = 0.0;
                if (parseDouble(cell, &parsed_double)) {
                  numeric_values[agg_index] = parsed_double;
                  numeric_present[agg_index] = true;
                }
                break;
              }
            }
          }
        }
        return true;
      },
      [&](std::size_t total_columns) {
        if (skip_header) {
          skip_header = false;
          return true;
        }
        if (total_columns < last_required_column + 1 || !filter_match) {
          return true;
        }

    for (std::size_t key_pos = 0; key_pos < key_cells.size(); ++key_pos) {
      key_values[key_pos] = parseCell(key_cells[key_pos]);
    }
    const auto encoded_key = encodeGroupKeyRow(key_values);
    auto it = entry_index.find(encoded_key);
    if (it == entry_index.end()) {
      AggregateEntry entry;
      entry.key_values = key_values;
      entry.state = init_state();
      ordered_entries.push_back(std::move(entry));
      it = entry_index.emplace(encoded_key, ordered_entries.size() - 1).first;
    }
    auto& state = ordered_entries[it->second].state;
    for (std::size_t i = 0; i < aggs.size(); ++i) {
      const auto& agg = aggs[i];
      switch (agg.function) {
        case AggregateFunction::Count:
          state.counts[i] += 1;
          break;
        case AggregateFunction::Sum:
        case AggregateFunction::Avg:
          if (numeric_present[i]) {
            state.sums[i] += numeric_values[i];
            state.counts[i] += 1;
          }
          break;
        case AggregateFunction::Min:
          if (numeric_present[i] &&
              (!state.has_min[i] || numeric_values[i] < state.mins[i])) {
            state.mins[i] = numeric_values[i];
            state.has_min[i] = true;
            state.min_is_int[i] = numeric_is_int[i];
            state.min_ints[i] = int_values[i];
          }
          break;
        case AggregateFunction::Max:
          if (numeric_present[i] &&
              (!state.has_max[i] || numeric_values[i] > state.maxs[i])) {
            state.maxs[i] = numeric_values[i];
            state.has_max[i] = true;
            state.max_is_int[i] = numeric_is_int[i];
            state.max_ints[i] = int_values[i];
          }
          break;
      }
    }
        return true;
      });

  Table result;
  for (const auto key_index : key_indices) {
    result.schema.fields.push_back(schema.fields[key_index]);
  }
  for (const auto& agg : aggs) {
    result.schema.fields.push_back(agg.output_name);
  }
  for (std::size_t i = 0; i < result.schema.fields.size(); ++i) {
    result.schema.index[result.schema.fields[i]] = i;
  }
  auto cache = std::make_shared<ColumnarTable>();
  cache->schema = result.schema;
  cache->columns.resize(result.schema.fields.size());
  cache->arrow_formats.resize(result.schema.fields.size());
  cache->row_count = ordered_entries.size();
  for (auto& column : cache->columns) {
    column.values.reserve(ordered_entries.size());
  }
  for (const auto& entry : ordered_entries) {
    const auto& state = entry.state;
    for (std::size_t key_pos = 0; key_pos < entry.key_values.size(); ++key_pos) {
      cache->columns[key_pos].values.push_back(entry.key_values[key_pos]);
    }
    for (std::size_t i = 0; i < aggs.size(); ++i) {
      Value value;
      switch (aggs[i].function) {
        case AggregateFunction::Count:
          value = Value(static_cast<int64_t>(state.counts[i]));
          break;
        case AggregateFunction::Sum:
          value = Value(state.sums[i]);
          break;
        case AggregateFunction::Avg:
          value = Value(state.counts[i] == 0 ? 0.0
                                             : state.sums[i] /
                                                   static_cast<double>(state.counts[i]));
          break;
        case AggregateFunction::Min:
          value = !state.has_min[i]
                      ? Value()
                      : (state.min_is_int[i] ? Value(state.min_ints[i]) : Value(state.mins[i]));
          break;
        case AggregateFunction::Max:
          value = !state.has_max[i]
                      ? Value()
                      : (state.max_is_int[i] ? Value(state.max_ints[i]) : Value(state.maxs[i]));
          break;
      }
      cache->columns[key_indices.size() + i].values.push_back(std::move(value));
    }
  }
  result.columnar_cache = std::move(cache);
  *out = std::move(result);
  return true;
}

void save_csv(const Table& table, const std::string& path) {
  std::ofstream output(path);
  if (!output.is_open()) {
    throw std::runtime_error("cannot write csv file: " + path);
  }

  for (size_t i = 0; i < table.schema.fields.size(); ++i) {
    if (i > 0) output << ',';
    output << table.schema.fields[i];
  }
  output << '\n';

  for (const auto& row : table.rows) {
    for (size_t i = 0; i < row.size(); ++i) {
      if (i > 0) output << ',';
      output << row[i].toString();
    }
    output << '\n';
  }
}

}  // namespace dataflow
