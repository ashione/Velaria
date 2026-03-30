#include "src/dataflow/core/execution/csv.h"

#include <cstdlib>
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace dataflow {

namespace {

bool isInt(const std::string& s) {
  if (s.empty()) return false;
  std::size_t pos = 0;
  if (s[0] == '-' || s[0] == '+') pos = 1;
  if (pos >= s.size()) return false;
  for (; pos < s.size(); ++pos) {
    if (s[pos] < '0' || s[pos] > '9') return false;
  }
  return true;
}

bool isDouble(const std::string& s) {
  if (s.empty()) return false;
  char* end = nullptr;
  std::strtod(s.c_str(), &end);
  return *end == '\0';
}

Value parseCell(const std::string& cell) {
  if (cell.empty()) return Value();
  if (cell.size() >= 2 && cell.front() == '[' && cell.back() == ']') {
    const auto vec = Value::parseFixedVector(cell);
    if (!vec.empty()) {
      return Value(vec);
    }
  }
  if (isInt(cell)) {
    return Value(static_cast<int64_t>(std::stoll(cell)));
  }
  if (isDouble(cell)) {
    return Value(std::stod(cell));
  }
  return Value(cell);
}

}  // namespace

Table load_csv(const std::string& path, char delimiter) {
  std::ifstream input(path);
  if (!input.is_open()) {
    throw std::runtime_error("cannot open csv file: " + path);
  }

  std::string line;
  if (!std::getline(input, line)) {
    return Table();
  }

  std::vector<std::string> header;
  {
    std::stringstream ss(line);
    std::string token;
    while (std::getline(ss, token, delimiter)) {
      header.push_back(token);
    }
  }

  Table table;
  table.schema = Schema(header);

  while (std::getline(input, line)) {
    std::stringstream ss(line);
    std::string cell;
    Row row;
    while (std::getline(ss, cell, delimiter)) {
      row.push_back(parseCell(cell));
    }
    if (!row.empty() && row.size() == table.schema.fields.size()) {
      table.rows.push_back(std::move(row));
    }
  }

  return table;
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
