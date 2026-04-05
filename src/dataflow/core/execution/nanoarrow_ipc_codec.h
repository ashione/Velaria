#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "src/dataflow/core/execution/table.h"

namespace dataflow {

std::vector<uint8_t> serialize_nanoarrow_ipc_table(const Table& table);
std::vector<uint8_t> serialize_nanoarrow_ipc_table(const Table& table, std::size_t* payload_size);
Table deserialize_nanoarrow_ipc_table(const std::vector<uint8_t>& payload,
                                      bool materialize_rows = false);
Table deserialize_nanoarrow_ipc_table(const uint8_t* payload, std::size_t size,
                                      bool materialize_rows = false);
Table load_nanoarrow_ipc_table(const std::string& path, bool materialize_rows = true);
void save_nanoarrow_ipc_table(const Table& table, const std::string& path);

}  // namespace dataflow
