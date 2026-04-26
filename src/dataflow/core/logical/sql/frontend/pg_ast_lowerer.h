#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "src/dataflow/core/logical/sql/frontend/sql_diagnostic.h"
#include "src/dataflow/core/logical/sql/frontend/sql_feature_validator.h"
#include "src/dataflow/core/logical/sql/frontend/unbound_plan.h"
#include "src/dataflow/core/logical/sql/sql_ast.h"

// Forward declaration for RAII holder
namespace dataflow { namespace sql {
class PgQueryParseResultHolder;
} }

namespace dataflow {
namespace sql {

class PgAstLowerer {
 public:
  PgAstLowerer() = default;

  // Lower a libpg_query protobuf parse tree into a Velaria SqlQuery AST.
  // Validation is integrated: disallowed features produce diagnostics.
  // Returns true on success (query was fully lowered).
  bool lower(const PgQueryParseResultHolder& parse_result,
             const SqlFeaturePolicy& policy,
             SqlQuery& out_query,
             UnboundPlan& out_unbound,
             std::vector<SqlDiagnostic>& out_diagnostics);
};

}  // namespace sql
}  // namespace dataflow
