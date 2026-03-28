#pragma once

#include <string>

#include "src/dataflow/api/dataframe.h"
#include "src/dataflow/catalog/catalog.h"
#include "src/dataflow/sql/sql_ast.h"

namespace dataflow {
namespace sql {

class SqlPlanner {
 public:
  DataFrame plan(const SqlQuery& query, const ViewCatalog& catalog) const;
};

}  // namespace sql
}  // namespace dataflow
