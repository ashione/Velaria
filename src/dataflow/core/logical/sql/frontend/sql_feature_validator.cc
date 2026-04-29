#include "src/dataflow/core/logical/sql/frontend/sql_feature_validator.h"
#include "src/dataflow/core/logical/sql/frontend/pg_query_raii.h"

extern "C" {
#include "pg_query.h"
#include "pg_query.pb-c.h"
}

#include <set>
#include <string>

namespace dataflow {
namespace sql {

namespace {

SqlDiagnostic unsupported(const std::string& feature, const std::string& msg,
                           const std::string& hint) {
  SqlDiagnostic d;
  d.phase = SqlDiagnostic::Phase::Validate;
  d.error_type = "unsupported_sql_feature";
  d.message = feature + ": " + msg;
  d.hint = hint;
  return d;
}

// Recursively check a node tree for policy violations
void checkNode(const PgQuery__Node* n, const SqlFeaturePolicy& pol,
               std::vector<SqlDiagnostic>& diags, std::set<std::string>& seen) {
  if (!n) return;

  switch (n->node_case) {
    case PG_QUERY__NODE__NODE_WINDOW_DEF:
    case PG_QUERY__NODE__NODE_WINDOW_CLAUSE:
      if (!pol.allow_window_function && !seen.count("window")) {
        seen.insert("window");
        diags.push_back(unsupported("window_function",
          "Window functions not supported in batch SQL v1.",
          "Consider stream SQL with WINDOW BY ... EVERY ... AS."));
      }
      break;

    case PG_QUERY__NODE__NODE_SUB_LINK:
      if (!pol.allow_subquery && !seen.count("subquery")) {
        seen.insert("subquery");
        diags.push_back(unsupported("subquery",
          "Subqueries (SubLink) not supported in Velaria SQL v1.",
          "Materialize the subquery as a temp view."));
      }
      break;
    case PG_QUERY__NODE__NODE_RANGE_SUBSELECT:
      if (!pol.allow_subquery && !seen.count("subquery")) {
        seen.insert("subquery");
        diags.push_back(unsupported("subquery",
          "Subquery in FROM not supported.",
          "Materialize as a temp view, then query it."));
      }
      break;

    case PG_QUERY__NODE__NODE_COMMON_TABLE_EXPR:
      if (!pol.allow_cte && !seen.count("cte")) {
        seen.insert("cte");
        diags.push_back(unsupported("cte",
          "CTE / WITH not supported in Velaria SQL v1.",
          "Materialize intermediate query as a run/artifact."));
      }
      break;

    case PG_QUERY__NODE__NODE_SET_OPERATION_STMT:
      if (!pol.allow_set_operation && !seen.count("set_operation")) {
        seen.insert("set_operation");
        diags.push_back(unsupported("set_operation",
          "UNION/INTERSECT/EXCEPT not supported.",
          "Execute queries separately and merge in client."));
      }
      break;

    case PG_QUERY__NODE__NODE_INSERT_STMT:
    case PG_QUERY__NODE__NODE_UPDATE_STMT:
    case PG_QUERY__NODE__NODE_DELETE_STMT:
      if (!pol.allow_dml && !seen.count("dml")) {
        seen.insert("dml");
        diags.push_back(unsupported("dml",
          "DML is not enabled by the current SQL policy.",
          "Use the Velaria SQL v1 INSERT subset or enable DML in the caller policy."));
      }
      break;

    case PG_QUERY__NODE__NODE_CREATE_STMT:
    case PG_QUERY__NODE__NODE_ALTER_TABLE_STMT:
    case PG_QUERY__NODE__NODE_DROP_STMT:
    case PG_QUERY__NODE__NODE_VIEW_STMT:
      if (!pol.allow_ddl && !seen.count("ddl")) {
        seen.insert("ddl");
        diags.push_back(unsupported("ddl",
          "DDL is not enabled by the current SQL policy.",
          "Use CREATE TABLE through a policy that allows Velaria SQL v1 DDL."));
      }
      break;

    case PG_QUERY__NODE__NODE_JOIN_EXPR:
      if (!pol.allow_join && !seen.count("join")) {
        seen.insert("join");
        diags.push_back(unsupported("join",
          "JOIN is not enabled by the current SQL policy.",
          "Use the Velaria SQL v1 INNER/LEFT equality JOIN subset or enable JOIN in the caller policy."));
      }
      break;

    case PG_QUERY__NODE__NODE_WITH_CLAUSE:
      if (!pol.allow_cte && !seen.count("cte")) {
        seen.insert("cte");
        diags.push_back(unsupported("cte",
          "WITH clause (CTE) not supported.",
          "Materialize intermediate results as temp views."));
      }
      break;

    default: break;
  }

  // Recursively validate sub-nodes based on type
  switch (n->node_case) {
    case PG_QUERY__NODE__NODE_SELECT_STMT: {
      auto* s = n->select_stmt; if (!s) break;
      for (size_t i = 0; i < s->n_target_list; i++) checkNode(s->target_list[i], pol, diags, seen);
      for (size_t i = 0; i < s->n_from_clause; i++) checkNode(s->from_clause[i], pol, diags, seen);
      checkNode(s->where_clause, pol, diags, seen);
      for (size_t i = 0; i < s->n_group_clause; i++) checkNode(s->group_clause[i], pol, diags, seen);
      checkNode(s->having_clause, pol, diags, seen);
      for (size_t i = 0; i < s->n_sort_clause; i++) checkNode(s->sort_clause[i], pol, diags, seen);
      for (size_t i = 0; i < s->n_window_clause; i++) checkNode(s->window_clause[i], pol, diags, seen);
      if (s->with_clause) {
        for (size_t i = 0; i < s->with_clause->n_ctes; i++)
          checkNode(s->with_clause->ctes[i], pol, diags, seen);
      }
      break;
    }
    case PG_QUERY__NODE__NODE_RES_TARGET:
      checkNode(n->res_target->val, pol, diags, seen); break;
    case PG_QUERY__NODE__NODE_FUNC_CALL:
      for (size_t i = 0; i < n->func_call->n_args; i++) checkNode(n->func_call->args[i], pol, diags, seen);
      break;
    case PG_QUERY__NODE__NODE_A_EXPR:
      checkNode(n->a_expr->lexpr, pol, diags, seen);
      checkNode(n->a_expr->rexpr, pol, diags, seen);
      break;
    case PG_QUERY__NODE__NODE_BOOL_EXPR:
      for (size_t i = 0; i < n->bool_expr->n_args; i++) checkNode(n->bool_expr->args[i], pol, diags, seen);
      break;
    case PG_QUERY__NODE__NODE_SORT_BY:
      checkNode(n->sort_by->node, pol, diags, seen); break;
    case PG_QUERY__NODE__NODE_TYPE_CAST:
      checkNode(n->type_cast->arg, pol, diags, seen); break;
    default: break;
  }
}

}  // namespace

std::vector<SqlDiagnostic> validateFeatures(
    const PgQueryParseResultHolder& parse_result,
    const SqlFeaturePolicy& policy) {
  std::vector<SqlDiagnostic> diags;

  const char* data = parse_result.parseTreeData();
  size_t len = parse_result.parseTreeLen();
  if (!data || len == 0) return diags;

  auto* parsed = pg_query__parse_result__unpack(nullptr, len, reinterpret_cast<const uint8_t*>(data));
  if (!parsed) return diags;

  // Multi-statement check
  if (parsed->n_stmts > 1 && !policy.allow_multi_statement) {
    diags.push_back(unsupported("multi_statement",
      "Multiple SQL statements detected (" + std::to_string(parsed->n_stmts) + ").",
      "Execute each statement separately."));
    pg_query__parse_result__free_unpacked(parsed, nullptr);
    return diags;
  }

  std::set<std::string> seen;
  for (size_t i = 0; i < parsed->n_stmts; i++) {
    auto* stmt = parsed->stmts[i];
    if (stmt->stmt) checkNode(stmt->stmt, policy, diags, seen);
  }

  pg_query__parse_result__free_unpacked(parsed, nullptr);
  return diags;
}

}  // namespace sql
}  // namespace dataflow
