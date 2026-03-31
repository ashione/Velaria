#pragma once

#include <string>

#include "src/dataflow/core/execution/table.h"

namespace dataflow {

Table load_nanoarrow_ipc_table(const std::string& path);
void save_nanoarrow_ipc_table(const Table& table, const std::string& path);

}  // namespace dataflow
