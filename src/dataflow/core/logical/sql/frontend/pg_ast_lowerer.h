#pragma once

#include <string>
#include <string_view>
#include <optional>
#include <unordered_map>
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

struct VelariaSqlExtensions {
  bool has_create_kind = false;
  TableKind create_kind = TableKind::Regular;
  std::string create_provider;
  std::unordered_map<std::string, std::string> create_options;
  std::optional<KeywordSearchSpec> keyword_search;
  std::optional<HybridSearchSpec> hybrid_search;
  std::optional<WindowSpec> window;
};

class PgAstLowerer {
 public:
  PgAstLowerer() = default;

  // Lower a libpg_query protobuf parse tree into a Velaria SqlStatement AST.
  // Validation is integrated: disallowed features produce diagnostics.
  // Returns true on success (statement was fully lowered).
  bool lower(const PgQueryParseResultHolder& parse_result,
             const SqlFeaturePolicy& policy,
             const VelariaSqlExtensions& extensions,
             SqlStatement& out_statement,
             UnboundPlan& out_unbound,
             std::vector<SqlDiagnostic>& out_diagnostics);
};

}  // namespace sql
}  // namespace dataflow
