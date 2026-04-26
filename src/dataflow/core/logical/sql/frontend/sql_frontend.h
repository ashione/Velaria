#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "src/dataflow/core/logical/sql/frontend/sql_diagnostic.h"
#include "src/dataflow/core/logical/sql/sql_ast.h"

namespace dataflow {
namespace sql {

enum class SqlFrontendKind { Legacy, PgQuery, Dual };

struct SqlFrontendConfig { SqlFrontendKind kind = SqlFrontendKind::Legacy; };

struct SqlFrontendResult {
  SqlStatement statement;
  std::vector<SqlDiagnostic> diagnostics;
  std::string frontend_name;
  std::string frontend_version;
};

SqlFrontendConfig sqlFrontendConfigFromEnv();

}  // namespace sql
}  // namespace dataflow
