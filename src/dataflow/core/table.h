#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "src/dataflow/core/value.h"

namespace dataflow {

using Row = std::vector<Value>;

struct Schema {
  std::vector<std::string> fields;
  std::unordered_map<std::string, size_t> index;

  Schema() = default;
  explicit Schema(std::vector<std::string> cols);

  size_t indexOf(const std::string& col) const;
  bool has(const std::string& col) const;
};

struct Table {
  Schema schema;
  std::vector<Row> rows;

  Table() = default;
  Table(Schema s, std::vector<Row> r) : schema(std::move(s)), rows(std::move(r)) {}

  size_t rowCount() const { return rows.size(); }
};

}  // namespace dataflow
