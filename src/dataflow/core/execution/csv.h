#pragma once

#include <string>
#include <vector>

#include "src/dataflow/core/logical/planner/plan.h"
#include "src/dataflow/core/execution/table.h"

namespace dataflow {

Schema read_csv_schema(const std::string& path, char delimiter = ',');
Table load_csv(const std::string& path, char delimiter = ',', bool materialize_rows = true);
Table load_csv_projected(const std::string& path, const Schema& schema,
                         const std::vector<std::size_t>& projected_columns,
                         char delimiter = ',', bool materialize_rows = false);
bool execute_csv_source_pushdown(const std::string& path, const Schema& schema,
                                 const SourcePushdownSpec& pushdown,
                                 char delimiter, bool materialize_rows, Table* out);
bool try_execute_csv_aggregate(const std::string& path, const Schema& schema,
                               const std::vector<std::size_t>& key_indices,
                               const std::vector<AggregateSpec>& aggs,
                               const SourceFilterPushdownSpec* filter,
                               char delimiter, Table* out);
void save_csv(const Table& table, const std::string& path);

}  // namespace dataflow
