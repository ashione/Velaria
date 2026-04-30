#pragma once

#include <string>
#include <vector>

#include "src/dataflow/core/logical/sql/frontend/sql_diagnostic.h"
#include "src/dataflow/core/logical/sql/sql_ast.h"

namespace dataflow {
namespace sql {

struct SqlFrontendResult {
  SqlStatement statement;
  std::vector<SqlDiagnostic> diagnostics;
  std::string frontend_name;
  std::string frontend_version;
};

}  // namespace sql
}  // namespace dataflow
