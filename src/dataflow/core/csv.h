#pragma once

#include <string>

#include "src/dataflow/core/table.h"

namespace dataflow {

Table load_csv(const std::string& path, char delimiter = ',');
void save_csv(const Table& table, const std::string& path);

}  // namespace dataflow
