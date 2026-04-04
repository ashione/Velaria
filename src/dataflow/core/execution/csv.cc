#include "src/dataflow/core/execution/csv.h"

#include <charconv>
#include <cstdlib>
#include <fstream>
#include <stdexcept>
#include <string_view>
#include <system_error>

#include "src/dataflow/core/execution/columnar_batch.h"

namespace dataflow {

namespace {

template <typename Consumer>
void parseCsvRow(const std::string& line, char delimiter, Consumer&& consume) {
  std::string cell;
  cell.reserve(line.size());
  bool in_quotes = false;
  for (std::size_t i = 0; i < line.size(); ++i) {
    const char ch = line[i];
    if (ch == '"') {
      if (in_quotes && i + 1 < line.size() && line[i + 1] == '"') {
        cell.push_back('"');
        ++i;
      } else {
        in_quotes = !in_quotes;
      }
      continue;
    }
    if (ch == delimiter && !in_quotes) {
      consume(std::move(cell));
      cell.clear();
      continue;
    }
    cell.push_back(ch);
  }
  consume(std::move(cell));
}

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

Schema readCsvHeader(std::ifstream* input, const std::string& path, char delimiter) {
  if (input == nullptr || !input->is_open()) {
    throw std::runtime_error("cannot open csv file: " + path);
  }
  std::string line;
  if (!std::getline(*input, line)) {
    return Schema();
  }
  std::vector<std::string> header;
  parseCsvRow(line, delimiter, [&](std::string cell) { header.push_back(std::move(cell)); });
  return Schema(std::move(header));
}

Table loadCsvInternal(const std::string& path, char delimiter, bool materialize_rows,
                      const Schema* schema_hint,
                      const std::vector<std::size_t>* projected_columns) {
  std::ifstream input(path);
  Schema schema = schema_hint == nullptr ? readCsvHeader(&input, path, delimiter) : *schema_hint;
  if (schema_hint != nullptr && !input.is_open()) {
    throw std::runtime_error("cannot open csv file: " + path);
  }
  if (schema_hint != nullptr) {
    std::string line;
    if (!std::getline(input, line)) {
      return Table();
    }
  }

  std::vector<uint8_t> projected_mask(schema.fields.size(), static_cast<uint8_t>(1));
  if (projected_columns != nullptr && !projected_columns->empty()) {
    std::fill(projected_mask.begin(), projected_mask.end(), static_cast<uint8_t>(0));
    for (const auto column_index : *projected_columns) {
      if (column_index >= projected_mask.size()) {
        throw std::runtime_error("projected csv column index out of range");
      }
      projected_mask[column_index] = 1;
    }
  }

  Table table;
  table.schema = schema;
  table.columnar_cache = std::make_shared<ColumnarTable>();
  table.columnar_cache->schema = table.schema;
  table.columnar_cache->columns.resize(table.schema.fields.size());
  table.columnar_cache->arrow_formats.resize(table.schema.fields.size());

  std::string line;
  while (std::getline(input, line)) {
    const auto row_start = table.columnar_cache->row_count;
    Row row;
    if (materialize_rows) {
      row.reserve(table.schema.fields.size());
    }
    std::size_t column_index = 0;
    bool row_valid = true;
    bool in_quotes = false;
    bool keep_cell =
        column_index < table.schema.fields.size() &&
        (materialize_rows || projected_mask[column_index] != 0);
    std::string cell;
    if (keep_cell) {
      cell.reserve(line.size());
    }
    auto finish_cell = [&] {
      if (!row_valid) {
        return;
      }
      if (column_index >= table.schema.fields.size()) {
        row_valid = false;
        return;
      }
      if (keep_cell) {
        Value parsed = parseCell(std::move(cell));
        table.columnar_cache->columns[column_index].values.push_back(parsed);
        if (materialize_rows) {
          row.push_back(std::move(parsed));
        }
        cell.clear();
      }
      ++column_index;
      keep_cell =
          column_index < table.schema.fields.size() &&
          (materialize_rows || projected_mask[column_index] != 0);
    };
    for (std::size_t i = 0; i < line.size(); ++i) {
      const char ch = line[i];
      if (ch == '"') {
        if (in_quotes && i + 1 < line.size() && line[i + 1] == '"') {
          if (keep_cell) {
            cell.push_back('"');
          }
          ++i;
        } else {
          in_quotes = !in_quotes;
        }
        continue;
      }
      if (ch == delimiter && !in_quotes) {
        finish_cell();
        continue;
      }
      if (keep_cell) {
        cell.push_back(ch);
      }
    }
    finish_cell();
    if (row_valid && column_index == table.schema.fields.size()) {
      if (materialize_rows) {
        table.rows.push_back(std::move(row));
      }
      table.columnar_cache->row_count += 1;
    } else {
      for (std::size_t i = 0; i < table.columnar_cache->columns.size(); ++i) {
        if (materialize_rows || projected_mask[i] != 0) {
          table.columnar_cache->columns[i].values.resize(row_start);
        }
      }
    }
  }

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
