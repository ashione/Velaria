#include "src/dataflow/core/execution/table.h"

#include <stdexcept>

#include "src/dataflow/core/execution/columnar_batch.h"

namespace dataflow {

Schema::Schema(std::vector<std::string> cols) : fields(std::move(cols)) {
  for (size_t i = 0; i < fields.size(); ++i) {
    index.emplace(fields[i], i);
  }
}

size_t Schema::indexOf(const std::string& col) const {
  auto it = index.find(col);
  if (it == index.end()) {
    throw std::out_of_range("column not found: " + col);
  }
  return it->second;
}

bool Schema::has(const std::string& col) const {
  return index.find(col) != index.end();
}

Table::Table(const Table& other) : schema(other.schema), rows(other.rows) {
  std::lock_guard<std::mutex> lock(other.columnar_cache_mu);
  if (other.columnar_cache) {
    columnar_cache = std::make_shared<ColumnarTable>(*other.columnar_cache);
  }
}

Table& Table::operator=(const Table& other) {
  if (this == &other) {
    return *this;
  }
  std::lock_guard<std::mutex> lock(other.columnar_cache_mu);
  schema = other.schema;
  rows = other.rows;
  if (other.columnar_cache) {
    columnar_cache = std::make_shared<ColumnarTable>(*other.columnar_cache);
  } else {
    columnar_cache.reset();
  }
  return *this;
}

Table::Table(Table&& other) noexcept : schema(std::move(other.schema)), rows(std::move(other.rows)) {
  std::lock_guard<std::mutex> lock(other.columnar_cache_mu);
  columnar_cache = std::move(other.columnar_cache);
}

Table& Table::operator=(Table&& other) noexcept {
  if (this == &other) {
    return *this;
  }
  std::lock_guard<std::mutex> lock(other.columnar_cache_mu);
  schema = std::move(other.schema);
  rows = std::move(other.rows);
  columnar_cache = std::move(other.columnar_cache);
  return *this;
}

}  // namespace dataflow
