#include "src/dataflow/core/logical/sql/frontend/pg_query_frontend.h"
#include "src/dataflow/core/logical/sql/frontend/pg_ast_lowerer.h"
#include "src/dataflow/core/logical/sql/frontend/pg_query_raii.h"
#include "src/dataflow/core/logical/sql/frontend/sql_feature_validator.h"

extern "C" {
#include "pg_query.h"
}

namespace dataflow {
namespace sql {

struct PgQueryFrontend::Impl { SqlFeaturePolicy policy; };

PgQueryFrontend::PgQueryFrontend() : impl_(std::make_unique<Impl>()) {}
PgQueryFrontend::~PgQueryFrontend() = default;

SqlFrontendResult PgQueryFrontend::process(std::string_view sql,
                                            const SqlFeaturePolicy& policy) {
  SqlFrontendResult result;
  result.frontend_name = "pg_query";
  result.frontend_version = std::to_string(PG_VERSION_NUM);

  // Phase 1: Parse via libpg_query protobuf API
  PgQueryParseResultHolder parse_result(sql.data());
  if (!parse_result.ok()) {
    result.diagnostics.push_back(parse_result.diagnostic(sql));
    return result;
  }

  // Phase 2: Validate + Lower in a single protobuf tree walk.
  // The PgAstLowerer internally validates against policy during lowering,
  // so we skip the separate validateFeatures() call to avoid double traversal.
  PgAstLowerer lowerer;
  UnboundPlan unbound;
  lowerer.lower(parse_result, policy, result.statement.query, unbound,
                result.diagnostics);

  result.statement.kind = SqlStatementKind::Select;
  return result;
}

std::string PgQueryFrontend::version() const {
  return std::to_string(PG_VERSION_NUM);
}

}  // namespace sql
}  // namespace dataflow
