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

size_t Table::rowCount() const {
  if (!rows.empty()) {
    return rows.size();
  }
  std::lock_guard<std::mutex> lock(columnar_cache_mu);
  if (!columnar_cache || columnar_cache->columns.empty()) {
    return columnar_cache ? columnar_cache->row_count : 0;
  }
  if (columnar_cache->row_count > 0) {
    return columnar_cache->row_count;
  }
  return valueColumnRowCount(columnar_cache->columns.front());
}

void materializeRows(Table* table) {
  if (table == nullptr) {
    throw std::invalid_argument("materializeRows table is null");
  }
  if (!table->rows.empty()) {
    return;
  }
  std::shared_ptr<ColumnarTable> cache;
  {
    std::lock_guard<std::mutex> lock(table->columnar_cache_mu);
    cache = table->columnar_cache;
  }
  if (!cache || cache->columns.empty()) {
    return;
  }
  const std::size_t row_count =
      cache->row_count > 0 ? cache->row_count : valueColumnRowCount(cache->columns.front());
  table->rows.resize(row_count);
  for (auto& row : table->rows) {
    row.reserve(cache->columns.size());
  }
  for (const auto& column : cache->columns) {
    if (valueColumnRowCount(column) != row_count) {
      throw std::runtime_error("columnar cache row count mismatch");
    }
    for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
      table->rows[row_index].push_back(valueColumnValueAt(column, row_index));
    }
  }
}

}  // namespace dataflow
