#include "src/dataflow/core/table.h"

#include <stdexcept>

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

}  // namespace dataflow
