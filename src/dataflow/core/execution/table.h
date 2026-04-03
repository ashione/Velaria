#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "src/dataflow/core/execution/value.h"

namespace dataflow {

struct ColumnarTable;
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
  mutable std::shared_ptr<ColumnarTable> columnar_cache;
  mutable std::mutex columnar_cache_mu;

  Table() = default;
  Table(Schema s, std::vector<Row> r) : schema(std::move(s)), rows(std::move(r)) {}
  Table(const Table& other);
  Table& operator=(const Table& other);
  Table(Table&& other) noexcept;
  Table& operator=(Table&& other) noexcept;

  size_t rowCount() const { return rows.size(); }
};

}  // namespace dataflow
