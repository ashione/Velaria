#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "src/dataflow/core/logical/sql/frontend/sql_diagnostic.h"

namespace dataflow { namespace sql {
class PgQueryParseResultHolder;
} }

namespace dataflow {
namespace sql {

struct SqlFeaturePolicy {
  bool allow_multi_statement = false;
  bool allow_dml = false;
  bool allow_ddl = false;
  bool allow_cte = false;
  bool allow_subquery = false;
  bool allow_window_function = false;
  bool allow_set_operation = false;
  bool allow_join = true;

  static SqlFeaturePolicy agentDefault() { SqlFeaturePolicy p; p.allow_join = true; return p; }
  static SqlFeaturePolicy cliDefault() { SqlFeaturePolicy p; p.allow_join = true; return p; }
};

// Walks the libpg_query protobuf parse tree and checks for features
// disallowed by the policy. Returns diagnostics for each violation.
std::vector<SqlDiagnostic> validateFeatures(
    const PgQueryParseResultHolder& parse_result,
    const SqlFeaturePolicy& policy);

}  // namespace sql
}  // namespace dataflow
