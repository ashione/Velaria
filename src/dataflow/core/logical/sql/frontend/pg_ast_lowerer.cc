#include "src/dataflow/core/logical/sql/frontend/pg_ast_lowerer.h"
#include "src/dataflow/core/logical/sql/frontend/pg_query_raii.h"

extern "C" {
#include "pg_query.h"
#include "pg_query.pb-c.h"
}

#include <cstring>
#include <set>
#include <string_view>

namespace dataflow {
namespace sql {

namespace {

// RAII guard for protobuf-c unpack result
struct ProtobufGuard {
  PgQuery__ParseResult* p;
  ~ProtobufGuard() { if (p) pg_query__parse_result__free_unpacked(p, nullptr); }
  PgQuery__ParseResult* operator->() { return p; }
  PgQuery__ParseResult& operator*() { return *p; }
  explicit operator bool() const { return p != nullptr; }
};

// --- diagnostic helpers ---

SqlDiagnostic makeDiag(SqlDiagnostic::Phase phase, const std::string& type,
                        const std::string& msg, const std::string& hint = "") {
  SqlDiagnostic d;
  d.phase = phase;
  d.error_type = type;
  d.message = msg;
  d.hint = hint;
  return d;
}

SqlDiagnostic unsupported(const std::string& feature, const std::string& msg,
                           const std::string& hint) {
  return makeDiag(SqlDiagnostic::Phase::Validate, "unsupported_sql_feature",
                  feature + ": " + msg, hint);
}

// --- string extraction from protobuf Node ---

std::string nodeStr(const PgQuery__Node* n) {
  if (!n) return "";
  if (n->node_case == PG_QUERY__NODE__NODE_STRING && n->string)
    return n->string->sval ? n->string->sval : "";
  return "";
}

std::string funcNameStr(const PgQuery__Node* n) {
  return nodeStr(n);
}

// Extract ColumnRef from a Node
ColumnRef extractCol(const PgQuery__Node* n) {
  ColumnRef r;
  if (!n || n->node_case != PG_QUERY__NODE__NODE_COLUMN_REF || !n->column_ref) return r;
  auto* cr = n->column_ref;
  if (cr->n_fields == 1) {
    r.name = nodeStr(cr->fields[0]);
  } else if (cr->n_fields == 2) {
    r.qualifier = nodeStr(cr->fields[0]);
    r.name = nodeStr(cr->fields[1]);
  }
  return r;
}

// Extract value from A_Const. Never throws — malformed floats return 0.
Value extractVal(const PgQuery__Node* n) {
  if (!n || n->node_case != PG_QUERY__NODE__NODE_A_CONST || !n->a_const) return Value(static_cast<int64_t>(0));
  auto* ac = n->a_const;
  switch (ac->val_case) {
    case PG_QUERY__A__CONST__VAL_IVAL:
      if (ac->ival) return Value(static_cast<int64_t>(ac->ival->ival));
      break;
    case PG_QUERY__A__CONST__VAL_SVAL:
      if (ac->sval && ac->sval->sval) return Value(std::string(ac->sval->sval));
      break;
    case PG_QUERY__A__CONST__VAL_FVAL: {
      if (ac->fval && ac->fval->fval) {
        char* end = nullptr;
        double d = std::strtod(ac->fval->fval, &end);
        if (end != ac->fval->fval) return Value(d);
      }
      break;
    }
    case PG_QUERY__A__CONST__VAL_BOOLVAL:
      if (ac->boolval) return Value(ac->boolval->boolval != 0);
      break;
    default: break;
  }
  return Value(static_cast<int64_t>(0));
}

// Map operator name to BinaryOperatorKind
BinaryOperatorKind mapOp(const std::string& op) {
  if (op == "=") return BinaryOperatorKind::Eq;
  if (op == "!=" || op == "<>") return BinaryOperatorKind::Ne;
  if (op == "<") return BinaryOperatorKind::Lt;
  if (op == "<=") return BinaryOperatorKind::Lte;
  if (op == ">") return BinaryOperatorKind::Gt;
  if (op == ">=") return BinaryOperatorKind::Gte;
  return BinaryOperatorKind::Eq;
}

// --- policy checking ---

bool checkNodePolicy(const PgQuery__Node* n, const SqlFeaturePolicy& pol,
                     std::vector<SqlDiagnostic>& diags, std::set<std::string>& seen) {
  if (!n) return true;
  switch (n->node_case) {
    case PG_QUERY__NODE__NODE_WINDOW_DEF:
      if (!pol.allow_window_function && !seen.count("window")) {
        seen.insert("window");
        diags.push_back(unsupported("window_function",
          "Window functions not supported in batch SQL v1.",
          "Consider stream SQL with WINDOW BY ... EVERY ... AS."));
        return false;
      }
      break;
    case PG_QUERY__NODE__NODE_SUB_LINK:
    case PG_QUERY__NODE__NODE_RANGE_SUBSELECT:
      if (!pol.allow_subquery && !seen.count("subquery")) {
        seen.insert("subquery");
        diags.push_back(unsupported("subquery",
          "Subqueries not supported in Velaria SQL v1.",
          "Materialize the subquery as a temp view, then query it separately."));
        return false;
      }
      break;
    case PG_QUERY__NODE__NODE_SET_OPERATION_STMT:
      if (!pol.allow_set_operation && !seen.count("set_operation")) {
        seen.insert("set_operation");
        diags.push_back(unsupported("set_operation",
          "UNION/INTERSECT/EXCEPT not supported.",
          "Execute queries separately and merge results."));
        return false;
      }
      break;
    case PG_QUERY__NODE__NODE_INSERT_STMT:
    case PG_QUERY__NODE__NODE_UPDATE_STMT:
    case PG_QUERY__NODE__NODE_DELETE_STMT:
      if (!pol.allow_dml && !seen.count("dml")) {
        seen.insert("dml");
        diags.push_back(unsupported("dml", "DML via pg_query frontend not supported.",
          "Use VELARIA_SQL_FRONTEND=legacy for DML."));
        return false;
      }
      break;
    case PG_QUERY__NODE__NODE_CREATE_STMT:
    case PG_QUERY__NODE__NODE_ALTER_TABLE_STMT:
    case PG_QUERY__NODE__NODE_DROP_STMT:
    case PG_QUERY__NODE__NODE_VIEW_STMT:
      if (!pol.allow_ddl && !seen.count("ddl")) {
        seen.insert("ddl");
        diags.push_back(unsupported("ddl", "DDL via pg_query frontend not supported.",
          "Use VELARIA_SQL_FRONTEND=legacy for DDL."));
        return false;
      }
      break;
    case PG_QUERY__NODE__NODE_COMMON_TABLE_EXPR:
      if (!pol.allow_cte && !seen.count("cte")) {
        seen.insert("cte");
        diags.push_back(unsupported("cte", "CTE/WITH not supported.",
          "Materialize intermediate query as a run/artifact."));
        return false;
      }
      break;
    case PG_QUERY__NODE__NODE_SELECT_STMT: {
      // Check if this is a CTE member (WITH clause) — access the raw SelectStmt
      break;
    }
    default: break;
  }
  return true;
}

// Recursively validate a node tree against policy
void validateNode(const PgQuery__Node* n, const SqlFeaturePolicy& pol,
                  std::vector<SqlDiagnostic>& diags, std::set<std::string>& seen) {
  if (!n) return;
  checkNodePolicy(n, pol, diags, seen);

  // Recursively validate based on node type
  switch (n->node_case) {
    case PG_QUERY__NODE__NODE_SELECT_STMT: {
      auto* s = n->select_stmt;
      if (!s) break;
      for (size_t i = 0; i < s->n_target_list; i++) validateNode(s->target_list[i], pol, diags, seen);
      for (size_t i = 0; i < s->n_from_clause; i++) validateNode(s->from_clause[i], pol, diags, seen);
      validateNode(s->where_clause, pol, diags, seen);
      for (size_t i = 0; i < s->n_group_clause; i++) validateNode(s->group_clause[i], pol, diags, seen);
      validateNode(s->having_clause, pol, diags, seen);
      for (size_t i = 0; i < s->n_sort_clause; i++) validateNode(s->sort_clause[i], pol, diags, seen);
      for (size_t i = 0; i < s->n_window_clause; i++) validateNode(s->window_clause[i], pol, diags, seen);
      break;
    }
    case PG_QUERY__NODE__NODE_RES_TARGET: {
      validateNode(n->res_target->val, pol, diags, seen);
      break;
    }
    case PG_QUERY__NODE__NODE_FUNC_CALL: {
      for (size_t i = 0; i < n->func_call->n_args; i++) validateNode(n->func_call->args[i], pol, diags, seen);
      break;
    }
    case PG_QUERY__NODE__NODE_A_EXPR: {
      validateNode(n->a_expr->lexpr, pol, diags, seen);
      validateNode(n->a_expr->rexpr, pol, diags, seen);
      break;
    }
    case PG_QUERY__NODE__NODE_BOOL_EXPR: {
      for (size_t i = 0; i < n->bool_expr->n_args; i++) validateNode(n->bool_expr->args[i], pol, diags, seen);
      break;
    }
    case PG_QUERY__NODE__NODE_SORT_BY: {
      validateNode(n->sort_by->node, pol, diags, seen);
      break;
    }
    case PG_QUERY__NODE__NODE_JOIN_EXPR: {
      if (!pol.allow_join && !seen.count("join")) {
        seen.insert("join");
        diags.push_back(unsupported("join", "JOIN via pg_query frontend not yet supported.",
          "Use VELARIA_SQL_FRONTEND=legacy for JOIN."));
      }
      break;
    }
    default: break;
  }
}

// --- expression lowering ---

std::shared_ptr<PredicateExpr> lowerExpr(const PgQuery__Node* n, std::vector<SqlDiagnostic>& diags);

std::shared_ptr<PredicateExpr> lowerBoolExpr(const PgQuery__Node* n, std::vector<SqlDiagnostic>& diags) {
  if (!n || n->node_case != PG_QUERY__NODE__NODE_BOOL_EXPR || !n->bool_expr) return nullptr;
  auto* be = n->bool_expr;

  if (be->boolop == PG_QUERY__BOOL_EXPR_TYPE__NOT_EXPR) {
    if (be->n_args > 0) {
      auto ie = lowerExpr(be->args[0], diags);
      if (!ie) return nullptr;
      if (ie->kind == PredicateExprKind::Comparison) {
        switch (ie->predicate.op) {
          case BinaryOperatorKind::Eq: ie->predicate.op = BinaryOperatorKind::Ne; break;
          case BinaryOperatorKind::Ne: ie->predicate.op = BinaryOperatorKind::Eq; break;
          case BinaryOperatorKind::Lt: ie->predicate.op = BinaryOperatorKind::Gte; break;
          case BinaryOperatorKind::Lte: ie->predicate.op = BinaryOperatorKind::Gt; break;
          case BinaryOperatorKind::Gt: ie->predicate.op = BinaryOperatorKind::Lte; break;
          case BinaryOperatorKind::Gte: ie->predicate.op = BinaryOperatorKind::Lt; break;
        }
        return ie;
      }
      // NOT over AND/OR — not supported in Phase 1; return nullptr to skip this predicate
      diags.push_back(makeDiag(SqlDiagnostic::Phase::Lower, "unsupported_sql_feature",
        "NOT over compound expressions (AND/OR) not supported via pg_query frontend.",
        "Rewrite using De Morgan's law: NOT (a AND b) → (NOT a) OR (NOT b)."));
      return nullptr;
    }
    return nullptr;
  }

  // AND or OR
  std::vector<std::shared_ptr<PredicateExpr>> parts;
  for (size_t i = 0; i < be->n_args; i++) {
    auto e = lowerExpr(be->args[i], diags);
    if (e) parts.push_back(e);
  }
  if (parts.empty()) return nullptr;
  if (parts.size() == 1) return parts[0];

  auto res = parts[0];
  for (size_t i = 1; i < parts.size(); i++) {
    auto c = std::make_shared<PredicateExpr>();
    c->kind = (be->boolop == PG_QUERY__BOOL_EXPR_TYPE__AND_EXPR)
                  ? PredicateExprKind::And : PredicateExprKind::Or;
    c->left = res; c->right = parts[i]; res = c;
  }
  return res;
}

std::shared_ptr<PredicateExpr> lowerAExpr(const PgQuery__Node* n, std::vector<SqlDiagnostic>& diags) {
  if (!n || n->node_case != PG_QUERY__NODE__NODE_A_EXPR || !n->a_expr) return nullptr;
  auto* ae = n->a_expr;

  // Only support AEXPR_OP for Phase 1 — emit diagnostic for LIKE/IN/etc.
  if (ae->kind != PG_QUERY__A__EXPR__KIND__AEXPR_OP) {
    diags.push_back(makeDiag(SqlDiagnostic::Phase::Lower, "unsupported_sql_feature",
      "Unsupported WHERE operator (LIKE/IN/BETWEEN/IS NULL etc.) not supported via pg_query frontend.",
      "Use VELARIA_SQL_FRONTEND=legacy for this query."));
    return nullptr;
  }

  // Extract operator name from name[] list
  std::string op;
  for (size_t i = 0; i < ae->n_name; i++) {
    if (!op.empty()) op += ".";
    op += nodeStr(ae->name[i]);
  }

  BinaryOperatorKind mapped = mapOp(op);
  if (mapped == BinaryOperatorKind::Eq && op != "=") {
    // Unknown operator
    return nullptr;
  }

  auto e = std::make_shared<PredicateExpr>();
  e->kind = PredicateExprKind::Comparison;
  e->predicate.op = mapped;

  if (ae->lexpr) {
    if (ae->lexpr->node_case == PG_QUERY__NODE__NODE_COLUMN_REF)
      e->predicate.lhs = extractCol(ae->lexpr);
  }
  if (ae->rexpr) {
    if (ae->rexpr->node_case == PG_QUERY__NODE__NODE_A_CONST)
      e->predicate.rhs = extractVal(ae->rexpr);
    else if (ae->rexpr->node_case == PG_QUERY__NODE__NODE_COLUMN_REF) {
      e->predicate.rhs_is_column_candidate = true;
      e->predicate.rhs_column = extractCol(ae->rexpr);
    }
  }
  return e;
}

std::shared_ptr<PredicateExpr> lowerExpr(const PgQuery__Node* n, std::vector<SqlDiagnostic>& diags) {
  if (!n) return nullptr;
  switch (n->node_case) {
    case PG_QUERY__NODE__NODE_BOOL_EXPR: return lowerBoolExpr(n, diags);
    case PG_QUERY__NODE__NODE_A_EXPR: return lowerAExpr(n, diags);
    default:
      // Unsupported predicate type — emit diagnostic for fallback
      diags.push_back(makeDiag(SqlDiagnostic::Phase::Lower, "unsupported_sql_feature",
        "Unsupported WHERE expression type via pg_query frontend.",
        "Use VELARIA_SQL_FRONTEND=legacy for this query."));
      return nullptr;
  }
}

// --- target list lowering ---

void lowerTarget(const PgQuery__Node* n, SqlQuery& q, std::vector<SqlDiagnostic>& diags) {
  if (!n || n->node_case != PG_QUERY__NODE__NODE_RES_TARGET || !n->res_target) return;
  auto* rt = n->res_target;
  auto* val = rt->val;
  if (!val) return;

  SelectItem item;
  item.alias = rt->name ? rt->name : "";

  switch (val->node_case) {
    case PG_QUERY__NODE__NODE_COLUMN_REF:
      item.column = extractCol(val);
      break;

    case PG_QUERY__NODE__NODE_A_STAR:
      item.is_all = true;
      break;

    case PG_QUERY__NODE__NODE_FUNC_CALL: {
      auto* fc = val->func_call;
      // Build function name from funcname list
      std::string fn;
      for (size_t i = 0; i < fc->n_funcname; i++) {
        if (!fn.empty()) fn += ".";
        fn += funcNameStr(fc->funcname[i]);
      }
      // Normalize to lowercase for matching
      std::string fnl = fn;
      for (auto& c : fnl) c = static_cast<char>(std::tolower(c));

      static const std::set<std::string> aggs = {"sum","count","avg","min","max"};
      if (aggs.count(fnl)) {
        item.is_aggregate = true;
        if (fnl=="sum") item.aggregate.function=AggregateFunctionKind::Sum;
        else if (fnl=="count") item.aggregate.function=AggregateFunctionKind::Count;
        else if (fnl=="avg") item.aggregate.function=AggregateFunctionKind::Avg;
        else if (fnl=="min") item.aggregate.function=AggregateFunctionKind::Min;
        else item.aggregate.function=AggregateFunctionKind::Max;
        item.aggregate.alias = item.alias;

        if (fc->agg_star || fc->n_args == 0) {
          item.aggregate.count_all = true;
        } else if (fc->n_args > 0) {
          if (fc->args[0]->node_case == PG_QUERY__NODE__NODE_COLUMN_REF)
            item.aggregate.argument = extractCol(fc->args[0]);
        }
      } else {
        item.is_string_function = true;
        // Map function name
        if (fnl=="lower") item.string_function.function=StringFunctionKind::Lower;
        else if (fnl=="upper") item.string_function.function=StringFunctionKind::Upper;
        else if (fnl=="trim") item.string_function.function=StringFunctionKind::Trim;
        else if (fnl=="ltrim") item.string_function.function=StringFunctionKind::Ltrim;
        else if (fnl=="rtrim") item.string_function.function=StringFunctionKind::Rtrim;
        else if (fnl=="length"||fnl=="len"||fnl=="char_length"||fnl=="character_length")
          item.string_function.function=StringFunctionKind::Length;
        else if (fnl=="reverse") item.string_function.function=StringFunctionKind::Reverse;
        else if (fnl=="concat") item.string_function.function=StringFunctionKind::Concat;
        else if (fnl=="concat_ws") item.string_function.function=StringFunctionKind::ConcatWs;
        else if (fnl=="left") item.string_function.function=StringFunctionKind::Left;
        else if (fnl=="right") item.string_function.function=StringFunctionKind::Right;
        else if (fnl=="substr"||fnl=="substring") item.string_function.function=StringFunctionKind::Substr;
        else if (fnl=="position") item.string_function.function=StringFunctionKind::Position;
        else if (fnl=="replace") item.string_function.function=StringFunctionKind::Replace;
        else if (fnl=="abs") item.string_function.function=StringFunctionKind::Abs;
        else if (fnl=="ceil") item.string_function.function=StringFunctionKind::Ceil;
        else if (fnl=="floor") item.string_function.function=StringFunctionKind::Floor;
        else if (fnl=="round") item.string_function.function=StringFunctionKind::Round;
        else if (fnl=="year") item.string_function.function=StringFunctionKind::Year;
        else if (fnl=="month") item.string_function.function=StringFunctionKind::Month;
        else if (fnl=="day") item.string_function.function=StringFunctionKind::Day;
        else if (fnl=="iso_year") item.string_function.function=StringFunctionKind::IsoYear;
        else if (fnl=="iso_week") item.string_function.function=StringFunctionKind::IsoWeek;
        else if (fnl=="week") item.string_function.function=StringFunctionKind::Week;
        else if (fnl=="yearweek") item.string_function.function=StringFunctionKind::YearWeek;
        else if (fnl=="now") item.string_function.function=StringFunctionKind::Now;
        else if (fnl=="today") item.string_function.function=StringFunctionKind::Today;
        else if (fnl=="current_timestamp") item.string_function.function=StringFunctionKind::CurrentTimestamp;
        else if (fnl=="unix_timestamp") item.string_function.function=StringFunctionKind::UnixTimestamp;
        else if (fnl=="cast") item.string_function.function=StringFunctionKind::Cast;
        else {
          diags.push_back(makeDiag(SqlDiagnostic::Phase::Lower, "unsupported_function",
            "Unknown function: " + fn, "Check velaria_sql_capabilities for supported functions."));
          return;
        }

        // Extract arguments
        for (size_t i = 0; i < fc->n_args; i++) {
          StringFunctionArg sfa;
          auto* arg = fc->args[i];
          if (arg->node_case == PG_QUERY__NODE__NODE_COLUMN_REF) {
            sfa.is_column = true; sfa.column = extractCol(arg);
          } else if (arg->node_case == PG_QUERY__NODE__NODE_A_CONST) {
            sfa.literal = extractVal(arg);
          }
          item.string_function.args.push_back(std::move(sfa));
        }
      }
      break;
    }

    case PG_QUERY__NODE__NODE_A_CONST:
      item.is_literal = true;
      item.literal = extractVal(val);
      break;

    case PG_QUERY__NODE__NODE_TYPE_CAST:
      item.is_string_function = true;
      item.string_function.function = StringFunctionKind::Cast;
      if (val->type_cast && val->type_cast->arg) {
        StringFunctionArg sfa;
        auto* arg = val->type_cast->arg;
        if (arg->node_case == PG_QUERY__NODE__NODE_COLUMN_REF) {
          sfa.is_column = true; sfa.column = extractCol(arg);
        } else {
          sfa.literal = extractVal(arg);
        }
        item.string_function.args.push_back(std::move(sfa));
      }
      break;

    case PG_QUERY__NODE__NODE_TYPE_NAME:
      // Type reference in CAST — not a real select item, skip
      return;

    default:
      // Unsupported select item — add diagnostic but don't block
      break;
  }

  q.select_items.push_back(std::move(item));
}

}  // namespace

// --- public API ---

bool PgAstLowerer::lower(const PgQueryParseResultHolder& parse_result,
                          const SqlFeaturePolicy& policy,
                          SqlQuery& out_query,
                          UnboundPlan& /*out_unbound*/,
                          std::vector<SqlDiagnostic>& out_diagnostics) {
  const char* data = parse_result.parseTreeData();
  size_t len = parse_result.parseTreeLen();
  if (!data || len == 0) {
    out_diagnostics.push_back(makeDiag(SqlDiagnostic::Phase::Lower,
      "protobuf_error", "Empty parse tree data."));
    return false;
  }

  // Unpack protobuf (RAII-guarded against exceptions)
  ProtobufGuard parsed{pg_query__parse_result__unpack(nullptr, len, reinterpret_cast<const uint8_t*>(data))};
  if (!parsed) {
    out_diagnostics.push_back(makeDiag(SqlDiagnostic::Phase::Lower,
      "protobuf_error", "Failed to unpack protobuf parse result."));
    return false;
  }

  // Check for multi-statement
  if (parsed->n_stmts > 1 && !policy.allow_multi_statement) {
    out_diagnostics.push_back(unsupported("multi_statement",
      "Multiple SQL statements detected (" + std::to_string(parsed->n_stmts) + ").",
      "Execute each statement separately."));
    return false;
  }

  if (parsed->n_stmts == 0) {
    out_diagnostics.push_back(makeDiag(SqlDiagnostic::Phase::Lower,
      "empty_query", "No statements in query."));
    return false;
  }

  // Get first statement
  auto* raw_stmt = parsed->stmts[0];
  auto* stmt_node = raw_stmt ? raw_stmt->stmt : nullptr;
  if (!stmt_node) return false;

  // Only SELECT supported
  if (stmt_node->node_case != PG_QUERY__NODE__NODE_SELECT_STMT) {
    out_diagnostics.push_back(makeDiag(SqlDiagnostic::Phase::Lower,
      "unsupported_statement",
      "Only SELECT statements supported via pg_query frontend."));
    return false;
  }

  // --- Validate entire tree against policy ---
  std::set<std::string> seen;
  validateNode(stmt_node, policy, out_diagnostics, seen);

  // Check for blocking diagnostics from validation
  bool has_blocking = false;
  for (auto& d : out_diagnostics) {
    if (d.phase == SqlDiagnostic::Phase::Validate && !d.error_type.empty())
      has_blocking = true;
  }
  if (has_blocking) return false;

  // --- Lower SelectStmt ---
  auto* s = stmt_node->select_stmt;

  // Check WITH clause (CTE)
  if (s->with_clause && s->with_clause->n_ctes > 0 && !policy.allow_cte) {
    out_diagnostics.push_back(unsupported("cte",
      "CTE / WITH not supported in Velaria SQL v1.",
      "Materialize intermediate query as a run/artifact, then query it in a second step."));
    return false;
  }

  // targetList
  for (size_t i = 0; i < s->n_target_list; i++)
    lowerTarget(s->target_list[i], out_query, out_diagnostics);

  // fromClause
  if (s->n_from_clause > 0) {
    // Reject implicit cross-join / multi-table FROM lists (FROM a, b, ...)
    if (s->n_from_clause > 1) {
      out_diagnostics.push_back(makeDiag(SqlDiagnostic::Phase::Lower,
        "unsupported_sql_feature",
        "Multi-table FROM clauses (implicit joins) not supported via pg_query frontend.",
        "Use VELARIA_SQL_FRONTEND=legacy for this query, or rewrite as explicit JOIN."));
      return false;
    }
    auto* f0 = s->from_clause[0];
    if (f0->node_case == PG_QUERY__NODE__NODE_RANGE_VAR && f0->range_var) {
      out_query.has_from = true;
      out_query.from.name = f0->range_var->relname ? f0->range_var->relname : "";
      if (f0->range_var->alias && f0->range_var->alias->aliasname)
        out_query.from.alias = f0->range_var->alias->aliasname;
    } else {
      // JOIN, subquery, function, or other unsupported FROM type
      // Always reject in pg_query frontend — fall back to legacy parser
      const char* from_type = "unknown";
      if (f0->node_case == PG_QUERY__NODE__NODE_JOIN_EXPR) from_type = "JOIN";
      else if (f0->node_case == PG_QUERY__NODE__NODE_RANGE_SUBSELECT) from_type = "subquery";
      else if (f0->node_case == PG_QUERY__NODE__NODE_RANGE_FUNCTION) from_type = "function";
      out_diagnostics.push_back(makeDiag(SqlDiagnostic::Phase::Lower,
        "unsupported_sql_feature",
        std::string("FROM clause type not yet supported via pg_query frontend: ") + from_type,
        "Use VELARIA_SQL_FRONTEND=legacy for this query."));
      // (freed by ProtobufGuard RAII)
      return false;
    }
  }

  // whereClause
  if (s->where_clause)
    out_query.where = lowerExpr(s->where_clause, out_diagnostics);

  // havingClause — emit diagnostic and fall back to legacy
  // (aggregates in HAVING predicates require special handling not yet implemented)
  if (s->having_clause) {
    out_diagnostics.push_back(makeDiag(SqlDiagnostic::Phase::Lower,
      "unsupported_sql_feature",
      "HAVING clause not yet supported via pg_query frontend.",
      "Use VELARIA_SQL_FRONTEND=legacy for queries with HAVING."));
    return false;
  }

  // groupClause
  for (size_t i = 0; i < s->n_group_clause; i++) {
    GroupByExpr gb;
    if (s->group_clause[i]->node_case == PG_QUERY__NODE__NODE_COLUMN_REF)
      gb.column = extractCol(s->group_clause[i]);
    out_query.group_by.push_back(std::move(gb));
  }

  // sortClause
  for (size_t i = 0; i < s->n_sort_clause; i++) {
    auto* sb = s->sort_clause[i];
    if (!sb || sb->node_case != PG_QUERY__NODE__NODE_SORT_BY || !sb->sort_by) continue;
    OrderByItem ob;
    if (sb->sort_by->node)
      ob.column = extractCol(sb->sort_by->node);
    int dir = sb->sort_by->sortby_dir;
    ob.ascending = (dir == PG_QUERY__SORT_BY_DIR__SORTBY_ASC ||
                    dir == PG_QUERY__SORT_BY_DIR__SORTBY_DEFAULT);
    out_query.order_by.push_back(std::move(ob));
  }

  // limitCount / limitOffset
  if (s->limit_count && s->limit_count->node_case == PG_QUERY__NODE__NODE_INTEGER &&
      s->limit_count->integer)
    out_query.limit = static_cast<size_t>(s->limit_count->integer->ival);

  if (s->limit_offset && s->limit_offset->node_case == PG_QUERY__NODE__NODE_INTEGER &&
      s->limit_offset->integer && s->limit_offset->integer->ival != 0) {
    out_diagnostics.push_back(unsupported("offset",
      "OFFSET not supported in Velaria SQL v1.",
      "Use range-based slicing on the result set."));
  }

  return true;
}

}  // namespace sql
}  // namespace dataflow
