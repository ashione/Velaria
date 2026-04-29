#include "src/dataflow/core/logical/sql/frontend/pg_ast_lowerer.h"
#include "src/dataflow/core/logical/sql/frontend/pg_query_raii.h"

extern "C" {
#include "pg_query.h"
#include "pg_query.pb-c.h"
}

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <string_view>

namespace dataflow {
namespace sql {

namespace {

struct ProtobufGuard {
  PgQuery__ParseResult* p;
  ~ProtobufGuard() { if (p) pg_query__parse_result__free_unpacked(p, nullptr); }
  PgQuery__ParseResult* operator->() { return p; }
  explicit operator bool() const { return p != nullptr; }
};

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

std::string toLower(std::string value) {
  for (auto& c : value) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  return value;
}

std::string toUpper(std::string value) {
  for (auto& c : value) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
  return value;
}

std::string nodeStr(const PgQuery__Node* n) {
  if (!n) return "";
  if (n->node_case == PG_QUERY__NODE__NODE_STRING && n->string) {
    return n->string->sval ? n->string->sval : "";
  }
  if (n->node_case == PG_QUERY__NODE__NODE_A_STAR) return "*";
  return "";
}

std::string typeNameStr(const PgQuery__TypeName* type_name) {
  if (!type_name) return "";
  std::string out;
  for (size_t i = 0; i < type_name->n_names; ++i) {
    if (!out.empty()) out += ".";
    out += nodeStr(type_name->names[i]);
  }
  return toUpper(out);
}

std::string rangeName(const PgQuery__RangeVar* rv) {
  return rv && rv->relname ? rv->relname : "";
}

std::string rangeAlias(const PgQuery__RangeVar* rv) {
  return rv && rv->alias && rv->alias->aliasname ? rv->alias->aliasname : "";
}

ColumnRef extractCol(const PgQuery__Node* n) {
  ColumnRef r;
  if (!n || n->node_case != PG_QUERY__NODE__NODE_COLUMN_REF || !n->column_ref) return r;
  auto* cr = n->column_ref;
  if (cr->n_fields == 1) {
    r.name = nodeStr(cr->fields[0]);
  } else if (cr->n_fields >= 2) {
    r.qualifier = nodeStr(cr->fields[0]);
    r.name = nodeStr(cr->fields[1]);
  }
  return r;
}

bool isTableStar(const PgQuery__Node* n, std::string* table_name) {
  if (!n || n->node_case != PG_QUERY__NODE__NODE_COLUMN_REF || !n->column_ref) return false;
  auto* cr = n->column_ref;
  if (cr->n_fields == 1 && cr->fields[0] &&
      cr->fields[0]->node_case == PG_QUERY__NODE__NODE_A_STAR) {
    return true;
  }
  if (cr->n_fields == 2 && cr->fields[1] &&
      cr->fields[1]->node_case == PG_QUERY__NODE__NODE_A_STAR) {
    if (table_name) *table_name = nodeStr(cr->fields[0]);
    return true;
  }
  return false;
}

bool extractLiteralValue(const PgQuery__Node* n, Value* out) {
  if (!n || !out) return false;
  if (n->node_case == PG_QUERY__NODE__NODE_A_CONST && n->a_const) {
    auto* ac = n->a_const;
    if (ac->isnull) {
      *out = Value();
      return true;
    }
    switch (ac->val_case) {
      case PG_QUERY__A__CONST__VAL_IVAL:
        if (!ac->ival) return false;
        *out = Value(static_cast<int64_t>(ac->ival->ival));
        return true;
      case PG_QUERY__A__CONST__VAL_SVAL:
        if (!ac->sval || !ac->sval->sval) return false;
        *out = Value(std::string(ac->sval->sval));
        return true;
      case PG_QUERY__A__CONST__VAL_FVAL: {
        if (!ac->fval || !ac->fval->fval) return false;
        char* end = nullptr;
        const double d = std::strtod(ac->fval->fval, &end);
        if (end == ac->fval->fval) return false;
        *out = Value(d);
        return true;
      }
      case PG_QUERY__A__CONST__VAL_BOOLVAL:
        if (!ac->boolval) return false;
        *out = Value(ac->boolval->boolval != 0);
        return true;
      default:
        return false;
    }
  }
  if (n->node_case == PG_QUERY__NODE__NODE_INTEGER && n->integer) {
    *out = Value(static_cast<int64_t>(n->integer->ival));
    return true;
  }
  if (n->node_case == PG_QUERY__NODE__NODE_FLOAT && n->float_) {
    char* end = nullptr;
    const double d = std::strtod(n->float_->fval, &end);
    if (end == n->float_->fval) return false;
    *out = Value(d);
    return true;
  }
  if (n->node_case == PG_QUERY__NODE__NODE_BOOLEAN && n->boolean) {
    *out = Value(n->boolean->boolval != 0);
    return true;
  }
  if (n->node_case == PG_QUERY__NODE__NODE_STRING && n->string) {
    *out = Value(std::string(n->string->sval ? n->string->sval : ""));
    return true;
  }
  return false;
}

bool lowerLiteralValue(const PgQuery__Node* n, Value* out, std::vector<SqlDiagnostic>& diags,
                       const std::string& context) {
  if (extractLiteralValue(n, out)) return true;
  diags.push_back(makeDiag(SqlDiagnostic::Phase::Lower, "unsupported_sql_feature",
                           "Unsupported " + context + " expression.",
                           "Use a literal value in this SQL v1 position."));
  return false;
}

BinaryOperatorKind mapOp(const std::string& op) {
  if (op == "=") return BinaryOperatorKind::Eq;
  if (op == "!=" || op == "<>") return BinaryOperatorKind::Ne;
  if (op == "<") return BinaryOperatorKind::Lt;
  if (op == "<=") return BinaryOperatorKind::Lte;
  if (op == ">") return BinaryOperatorKind::Gt;
  if (op == ">=") return BinaryOperatorKind::Gte;
  return BinaryOperatorKind::Eq;
}

std::string operatorName(const PgQuery__AExpr* ae) {
  std::string op;
  if (!ae) return op;
  for (size_t i = 0; i < ae->n_name; ++i) {
    if (!op.empty()) op += ".";
    op += nodeStr(ae->name[i]);
  }
  return op;
}

AggregateFunctionKind aggregateKind(const std::string& fn) {
  const auto lower = toLower(fn);
  if (lower == "sum") return AggregateFunctionKind::Sum;
  if (lower == "count") return AggregateFunctionKind::Count;
  if (lower == "avg") return AggregateFunctionKind::Avg;
  if (lower == "min") return AggregateFunctionKind::Min;
  if (lower == "max") return AggregateFunctionKind::Max;
  return AggregateFunctionKind::StdDev;
}

bool isAggregateName(const std::string& fn) {
  const auto lower = toLower(fn);
  return lower == "sum" || lower == "count" || lower == "avg" ||
         lower == "min" || lower == "max";
}

std::string funcName(const PgQuery__FuncCall* fc) {
  std::string out;
  if (!fc) return out;
  for (size_t i = 0; i < fc->n_funcname; ++i) {
    if (!out.empty()) out += ".";
    out += nodeStr(fc->funcname[i]);
  }
  return out;
}

std::optional<StringFunctionKind> stringFunctionKind(const std::string& raw) {
  auto fn = toLower(raw);
  const auto dot = fn.rfind('.');
  if (dot != std::string::npos) {
    fn = fn.substr(dot + 1);
  }
  if (fn.rfind("velaria_", 0) == 0) {
    fn = fn.substr(std::string("velaria_").size());
  }
  if (fn == "btrim") return StringFunctionKind::Trim;
  if (fn == "lower") return StringFunctionKind::Lower;
  if (fn == "upper") return StringFunctionKind::Upper;
  if (fn == "trim") return StringFunctionKind::Trim;
  if (fn == "ltrim") return StringFunctionKind::Ltrim;
  if (fn == "rtrim") return StringFunctionKind::Rtrim;
  if (fn == "length" || fn == "len" || fn == "char_length" || fn == "character_length") {
    return StringFunctionKind::Length;
  }
  if (fn == "reverse") return StringFunctionKind::Reverse;
  if (fn == "concat") return StringFunctionKind::Concat;
  if (fn == "concat_ws") return StringFunctionKind::ConcatWs;
  if (fn == "left") return StringFunctionKind::Left;
  if (fn == "right") return StringFunctionKind::Right;
  if (fn == "substr" || fn == "substring") return StringFunctionKind::Substr;
  if (fn == "position") return StringFunctionKind::Position;
  if (fn == "replace") return StringFunctionKind::Replace;
  if (fn == "abs") return StringFunctionKind::Abs;
  if (fn == "ceil") return StringFunctionKind::Ceil;
  if (fn == "floor") return StringFunctionKind::Floor;
  if (fn == "round") return StringFunctionKind::Round;
  if (fn == "year") return StringFunctionKind::Year;
  if (fn == "month") return StringFunctionKind::Month;
  if (fn == "day") return StringFunctionKind::Day;
  if (fn == "iso_year") return StringFunctionKind::IsoYear;
  if (fn == "iso_week") return StringFunctionKind::IsoWeek;
  if (fn == "week") return StringFunctionKind::Week;
  if (fn == "yearweek") return StringFunctionKind::YearWeek;
  if (fn == "now") return StringFunctionKind::Now;
  if (fn == "today") return StringFunctionKind::Today;
  if (fn == "current_timestamp" || fn == "currenttimestamp") {
    return StringFunctionKind::CurrentTimestamp;
  }
  if (fn == "unix_timestamp") return StringFunctionKind::UnixTimestamp;
  if (fn == "cast") return StringFunctionKind::Cast;
  return std::nullopt;
}

std::optional<StringFunctionKind> sqlValueFunctionKind(const PgQuery__SQLValueFunction* fn) {
  if (!fn) return std::nullopt;
  switch (fn->op) {
    case PG_QUERY__SQLVALUE_FUNCTION_OP__SVFOP_CURRENT_DATE:
      return StringFunctionKind::Today;
    case PG_QUERY__SQLVALUE_FUNCTION_OP__SVFOP_CURRENT_TIMESTAMP:
    case PG_QUERY__SQLVALUE_FUNCTION_OP__SVFOP_CURRENT_TIMESTAMP_N:
      return StringFunctionKind::CurrentTimestamp;
    default:
      return std::nullopt;
  }
}

StringFunctionArg lowerFunctionArg(const PgQuery__Node* n, std::vector<SqlDiagnostic>& diags);
StringFunctionExpr lowerStringFunction(const PgQuery__FuncCall* fc,
                                       std::vector<SqlDiagnostic>& diags) {
  StringFunctionExpr expr;
  const auto name = funcName(fc);
  const auto kind = stringFunctionKind(name);
  if (!kind.has_value()) {
    diags.push_back(makeDiag(SqlDiagnostic::Phase::Lower, "unsupported_function",
                             "Unknown function: " + name,
                             "Check velaria_sql_capabilities for supported functions."));
    return expr;
  }
  expr.function = *kind;
  for (size_t i = 0; i < fc->n_args; ++i) {
    expr.args.push_back(lowerFunctionArg(fc->args[i], diags));
  }
  return expr;
}

StringFunctionArg lowerTypeCastArg(const PgQuery__TypeCast* tc,
                                   std::vector<SqlDiagnostic>& diags) {
  StringFunctionArg out;
  out.is_function = true;
  out.function = std::make_shared<StringFunctionExpr>();
  out.function->function = StringFunctionKind::Cast;
  if (tc && tc->arg) {
    out.function->args.push_back(lowerFunctionArg(tc->arg, diags));
  }
  StringFunctionArg target;
  target.literal = Value(typeNameStr(tc ? tc->type_name : nullptr));
  out.function->args.push_back(std::move(target));
  return out;
}

StringFunctionArg lowerFunctionArg(const PgQuery__Node* n, std::vector<SqlDiagnostic>& diags) {
  StringFunctionArg out;
  if (!n) return out;
  if (n->node_case == PG_QUERY__NODE__NODE_COLUMN_REF) {
    out.is_column = true;
    out.column = extractCol(n);
    return out;
  }
  if (n->node_case == PG_QUERY__NODE__NODE_FUNC_CALL) {
    out.is_function = true;
    out.function = std::make_shared<StringFunctionExpr>(lowerStringFunction(n->func_call, diags));
    return out;
  }
  if (n->node_case == PG_QUERY__NODE__NODE_TYPE_CAST) {
    return lowerTypeCastArg(n->type_cast, diags);
  }
  if (n->node_case == PG_QUERY__NODE__NODE_SQLVALUE_FUNCTION) {
    const auto kind = sqlValueFunctionKind(n->sqlvalue_function);
    if (kind.has_value()) {
      out.is_function = true;
      out.function = std::make_shared<StringFunctionExpr>();
      out.function->function = *kind;
      return out;
    }
  }
  lowerLiteralValue(n, &out.literal, diags, "function argument");
  return out;
}

AggregateExpr lowerAggregate(const PgQuery__FuncCall* fc, std::vector<SqlDiagnostic>& diags) {
  AggregateExpr agg;
  const auto name = funcName(fc);
  agg.function = aggregateKind(name);
  if (fc && fc->agg_distinct) {
    diags.push_back(makeDiag(SqlDiagnostic::Phase::Lower, "unsupported_sql_feature",
                             "Aggregate DISTINCT is not supported.",
                             "Use the Velaria SQL v1 aggregate subset without DISTINCT."));
  }
  if (!fc || fc->agg_star || fc->n_args == 0) {
    agg.count_all = true;
    return agg;
  }
  if (fc->args[0] && fc->args[0]->node_case == PG_QUERY__NODE__NODE_A_STAR) {
    agg.count_all = true;
  } else if (fc->args[0] && fc->args[0]->node_case == PG_QUERY__NODE__NODE_COLUMN_REF) {
    agg.argument = extractCol(fc->args[0]);
  } else {
    diags.push_back(makeDiag(SqlDiagnostic::Phase::Lower, "unsupported_sql_feature",
                             "Aggregate argument must be a column or '*'.",
                             "Project scalar expressions first, then aggregate the result column."));
  }
  return agg;
}

bool lowerPredicateOperand(const PgQuery__Node* n, Predicate* pred, bool lhs,
                           std::vector<SqlDiagnostic>& diags) {
  if (!n || !pred) return false;
  if (n->node_case == PG_QUERY__NODE__NODE_COLUMN_REF) {
    if (lhs) {
      pred->lhs = extractCol(n);
    } else {
      pred->rhs_is_column_candidate = true;
      pred->rhs_column = extractCol(n);
    }
    return true;
  }
  if (lhs && n->node_case == PG_QUERY__NODE__NODE_FUNC_CALL &&
      isAggregateName(funcName(n->func_call))) {
    pred->lhs_is_aggregate = true;
    pred->lhs_aggregate = lowerAggregate(n->func_call, diags);
    return true;
  }
  if (!lhs) {
    return lowerLiteralValue(n, &pred->rhs, diags, "predicate right-hand");
  }
  diags.push_back(makeDiag(SqlDiagnostic::Phase::Lower, "unsupported_sql_feature",
                           "Unsupported predicate left-hand expression.",
                           "Use a column or supported aggregate comparison."));
  return false;
}

std::shared_ptr<PredicateExpr> lowerExpr(const PgQuery__Node* n, std::vector<SqlDiagnostic>& diags);

std::shared_ptr<PredicateExpr> lowerBoolExpr(const PgQuery__Node* n,
                                             std::vector<SqlDiagnostic>& diags) {
  if (!n || n->node_case != PG_QUERY__NODE__NODE_BOOL_EXPR || !n->bool_expr) return nullptr;
  auto* be = n->bool_expr;
  if (be->boolop == PG_QUERY__BOOL_EXPR_TYPE__NOT_EXPR) {
    if (be->n_args == 0) return nullptr;
    auto expr = lowerExpr(be->args[0], diags);
    if (!expr || expr->kind != PredicateExprKind::Comparison) {
      diags.push_back(makeDiag(SqlDiagnostic::Phase::Lower, "unsupported_sql_feature",
                               "NOT over compound expressions is not supported.",
                               "Rewrite using explicit positive comparisons."));
      return nullptr;
    }
    switch (expr->predicate.op) {
      case BinaryOperatorKind::Eq: expr->predicate.op = BinaryOperatorKind::Ne; break;
      case BinaryOperatorKind::Ne: expr->predicate.op = BinaryOperatorKind::Eq; break;
      case BinaryOperatorKind::Lt: expr->predicate.op = BinaryOperatorKind::Gte; break;
      case BinaryOperatorKind::Lte: expr->predicate.op = BinaryOperatorKind::Gt; break;
      case BinaryOperatorKind::Gt: expr->predicate.op = BinaryOperatorKind::Lte; break;
      case BinaryOperatorKind::Gte: expr->predicate.op = BinaryOperatorKind::Lt; break;
    }
    return expr;
  }

  std::vector<std::shared_ptr<PredicateExpr>> parts;
  for (size_t i = 0; i < be->n_args; ++i) {
    auto part = lowerExpr(be->args[i], diags);
    if (part) parts.push_back(std::move(part));
  }
  if (parts.empty()) return nullptr;
  auto out = parts.front();
  for (size_t i = 1; i < parts.size(); ++i) {
    auto parent = std::make_shared<PredicateExpr>();
    parent->kind = be->boolop == PG_QUERY__BOOL_EXPR_TYPE__AND_EXPR
                       ? PredicateExprKind::And
                       : PredicateExprKind::Or;
    parent->left = std::move(out);
    parent->right = std::move(parts[i]);
    out = std::move(parent);
  }
  return out;
}

std::shared_ptr<PredicateExpr> lowerAExpr(const PgQuery__Node* n,
                                          std::vector<SqlDiagnostic>& diags) {
  if (!n || n->node_case != PG_QUERY__NODE__NODE_A_EXPR || !n->a_expr) return nullptr;
  auto* ae = n->a_expr;
  if (ae->kind != PG_QUERY__A__EXPR__KIND__AEXPR_OP) {
    diags.push_back(makeDiag(SqlDiagnostic::Phase::Lower, "unsupported_sql_feature",
                             "Unsupported WHERE operator.",
                             "Use =, !=, <, <=, >, or >=."));
    return nullptr;
  }
  const auto op = operatorName(ae);
  if (op != "=" && op != "!=" && op != "<>" && op != "<" && op != "<=" &&
      op != ">" && op != ">=") {
    diags.push_back(makeDiag(SqlDiagnostic::Phase::Lower, "unsupported_sql_feature",
                             "Unsupported WHERE operator: " + op,
                             "Use =, !=, <, <=, >, or >=."));
    return nullptr;
  }
  auto out = std::make_shared<PredicateExpr>();
  out->kind = PredicateExprKind::Comparison;
  out->predicate.op = mapOp(op);
  lowerPredicateOperand(ae->lexpr, &out->predicate, true, diags);
  lowerPredicateOperand(ae->rexpr, &out->predicate, false, diags);
  return out;
}

std::shared_ptr<PredicateExpr> lowerExpr(const PgQuery__Node* n, std::vector<SqlDiagnostic>& diags) {
  if (!n) return nullptr;
  switch (n->node_case) {
    case PG_QUERY__NODE__NODE_BOOL_EXPR: return lowerBoolExpr(n, diags);
    case PG_QUERY__NODE__NODE_A_EXPR: return lowerAExpr(n, diags);
    default:
      diags.push_back(makeDiag(SqlDiagnostic::Phase::Lower, "unsupported_sql_feature",
                               "Unsupported predicate expression type.",
                               "Use simple comparison predicates."));
      return nullptr;
  }
}

SelectItem lowerTarget(const PgQuery__Node* n, std::vector<SqlDiagnostic>& diags) {
  SelectItem item;
  if (!n || n->node_case != PG_QUERY__NODE__NODE_RES_TARGET || !n->res_target) return item;
  auto* rt = n->res_target;
  auto* val = rt->val;
  item.alias = rt->name ? rt->name : "";
  if (!val) return item;

  std::string table_star_name;
  if (isTableStar(val, &table_star_name)) {
    if (table_star_name.empty()) {
      item.is_all = true;
    } else {
      item.is_table_all = true;
      item.table_name_or_alias = table_star_name;
    }
    return item;
  }

  switch (val->node_case) {
    case PG_QUERY__NODE__NODE_COLUMN_REF:
      item.column = extractCol(val);
      break;
    case PG_QUERY__NODE__NODE_FUNC_CALL: {
      const auto name = funcName(val->func_call);
      if (isAggregateName(name)) {
        item.is_aggregate = true;
        item.aggregate = lowerAggregate(val->func_call, diags);
        item.aggregate.alias = item.alias;
      } else {
        item.is_string_function = true;
        item.string_function = lowerStringFunction(val->func_call, diags);
      }
      break;
    }
    case PG_QUERY__NODE__NODE_TYPE_CAST:
      item.is_string_function = true;
      item.string_function = *lowerTypeCastArg(val->type_cast, diags).function;
      break;
    case PG_QUERY__NODE__NODE_SQLVALUE_FUNCTION: {
      const auto kind = sqlValueFunctionKind(val->sqlvalue_function);
      if (kind.has_value()) {
        item.is_string_function = true;
        item.string_function.function = *kind;
      } else {
        diags.push_back(makeDiag(SqlDiagnostic::Phase::Lower, "unsupported_sql_feature",
                                 "Unsupported SQL value function.",
                                 "Use CURRENT_DATE or CURRENT_TIMESTAMP."));
      }
      break;
    }
    case PG_QUERY__NODE__NODE_A_CONST:
    case PG_QUERY__NODE__NODE_INTEGER:
    case PG_QUERY__NODE__NODE_FLOAT:
    case PG_QUERY__NODE__NODE_BOOLEAN:
    case PG_QUERY__NODE__NODE_STRING:
      item.is_literal = true;
      lowerLiteralValue(val, &item.literal, diags, "SELECT literal");
      break;
    default:
      diags.push_back(makeDiag(SqlDiagnostic::Phase::Lower, "unsupported_sql_feature",
                               "Unsupported SELECT item expression.",
                               "Use columns, literals, supported scalar functions, or aggregates."));
      break;
  }
  return item;
}

GroupByExpr lowerGroupBy(const PgQuery__Node* n, std::vector<SqlDiagnostic>& diags) {
  GroupByExpr out;
  if (!n) return out;
  if (n->node_case == PG_QUERY__NODE__NODE_COLUMN_REF) {
    out.column = extractCol(n);
    return out;
  }
  if (n->node_case == PG_QUERY__NODE__NODE_FUNC_CALL) {
    out.is_string_function = true;
    out.string_function = lowerStringFunction(n->func_call, diags);
    return out;
  }
  if (n->node_case == PG_QUERY__NODE__NODE_TYPE_CAST) {
    out.is_string_function = true;
    out.string_function = *lowerTypeCastArg(n->type_cast, diags).function;
    return out;
  }
  diags.push_back(makeDiag(SqlDiagnostic::Phase::Lower, "unsupported_sql_feature",
                           "GROUP BY expects a column or supported scalar function.",
                           "Rewrite GROUP BY with an explicit supported expression."));
  return out;
}

OrderByItem lowerOrderBy(const PgQuery__Node* n) {
  OrderByItem out;
  if (!n || n->node_case != PG_QUERY__NODE__NODE_SORT_BY || !n->sort_by) return out;
  out.column = extractCol(n->sort_by->node);
  out.ascending = n->sort_by->sortby_dir != PG_QUERY__SORT_BY_DIR__SORTBY_DESC;
  return out;
}

bool lowerFromRange(const PgQuery__Node* n, FromItem* out) {
  if (!n || !out || n->node_case != PG_QUERY__NODE__NODE_RANGE_VAR || !n->range_var) return false;
  out->name = rangeName(n->range_var);
  out->alias = rangeAlias(n->range_var);
  return true;
}

bool lowerJoin(const PgQuery__Node* n, SqlQuery* query, std::vector<SqlDiagnostic>& diags) {
  if (!n || !query || n->node_case != PG_QUERY__NODE__NODE_JOIN_EXPR || !n->join_expr) return false;
  auto* join = n->join_expr;
  if (join->jointype != PG_QUERY__JOIN_TYPE__JOIN_INNER &&
      join->jointype != PG_QUERY__JOIN_TYPE__JOIN_LEFT) {
    diags.push_back(makeDiag(SqlDiagnostic::Phase::Lower, "unsupported_sql_feature",
                             "Only INNER JOIN and LEFT JOIN are supported.",
                             "Rewrite the query using supported join types."));
    return false;
  }
  if (!lowerFromRange(join->larg, &query->from) || !lowerFromRange(join->rarg, &query->join.emplace().right)) {
    diags.push_back(makeDiag(SqlDiagnostic::Phase::Lower, "unsupported_sql_feature",
                             "JOIN inputs must be base tables.",
                             "Materialize complex inputs as temp views first."));
    query->join.reset();
    return false;
  }
  query->has_from = true;
  query->join->is_left = join->jointype == PG_QUERY__JOIN_TYPE__JOIN_LEFT;
  if (!join->quals || join->quals->node_case != PG_QUERY__NODE__NODE_A_EXPR ||
      !join->quals->a_expr || operatorName(join->quals->a_expr) != "=") {
    diags.push_back(makeDiag(SqlDiagnostic::Phase::Lower, "unsupported_sql_feature",
                             "Only equality JOIN predicates are supported.",
                             "Use JOIN ... ON left_key = right_key."));
    query->join.reset();
    return false;
  }
  query->join->left_key = extractCol(join->quals->a_expr->lexpr);
  query->join->right_key = extractCol(join->quals->a_expr->rexpr);
  return true;
}

void lowerLimit(const PgQuery__Node* n, SqlQuery* query, std::vector<SqlDiagnostic>& diags) {
  if (!n || !query) return;
  Value value;
  if (!lowerLiteralValue(n, &value, diags, "LIMIT")) return;
  if (!value.isNumber()) {
    diags.push_back(makeDiag(SqlDiagnostic::Phase::Lower, "unsupported_sql_feature",
                             "LIMIT expects a numeric literal.",
                             "Use LIMIT with an integer literal."));
    return;
  }
  query->limit = static_cast<std::size_t>(value.asInt64());
}

SqlQuery lowerSelect(const PgQuery__SelectStmt* s, const VelariaSqlExtensions& extensions,
                     std::vector<SqlDiagnostic>& diags) {
  SqlQuery q;
  if (!s) return q;
  if (s->op == PG_QUERY__SET_OPERATION__SETOP_UNION) {
    if (s->larg) q = lowerSelect(s->larg, VelariaSqlExtensions{}, diags);
    if (s->rarg) {
      SqlUnionTerm term;
      term.all = s->all;
      term.query = std::make_shared<SqlQuery>(lowerSelect(s->rarg, VelariaSqlExtensions{}, diags));
      q.union_terms.push_back(std::move(term));
    }
    for (size_t i = 0; i < s->n_sort_clause; ++i) {
      q.order_by.push_back(lowerOrderBy(s->sort_clause[i]));
    }
    lowerLimit(s->limit_count, &q, diags);
    return q;
  }
  if (s->op == PG_QUERY__SET_OPERATION__SETOP_INTERSECT ||
      s->op == PG_QUERY__SET_OPERATION__SETOP_EXCEPT) {
    diags.push_back(makeDiag(SqlDiagnostic::Phase::Lower, "unsupported_sql_feature",
                             "INTERSECT and EXCEPT are not supported.",
                             "Use UNION or run the queries separately."));
    return q;
  }
  if (s->n_distinct_clause > 0) {
    diags.push_back(makeDiag(SqlDiagnostic::Phase::Lower, "unsupported_sql_feature",
                             "SELECT DISTINCT is not supported.",
                             "Use GROUP BY or de-duplicate the result outside SQL v1."));
  }
  for (size_t i = 0; i < s->n_target_list; ++i) {
    q.select_items.push_back(lowerTarget(s->target_list[i], diags));
  }
  if (s->n_from_clause > 0) {
    if (s->n_from_clause > 1) {
      diags.push_back(makeDiag(SqlDiagnostic::Phase::Lower, "unsupported_sql_feature",
                               "Implicit multi-table FROM clauses are not supported.",
                               "Use explicit JOIN syntax."));
    } else if (!lowerFromRange(s->from_clause[0], &q.from)) {
      lowerJoin(s->from_clause[0], &q, diags);
    } else {
      q.has_from = true;
    }
  }
  if (s->where_clause) q.where = lowerExpr(s->where_clause, diags);
  for (size_t i = 0; i < s->n_group_clause; ++i) {
    q.group_by.push_back(lowerGroupBy(s->group_clause[i], diags));
  }
  if (s->having_clause) q.having = lowerExpr(s->having_clause, diags);
  for (size_t i = 0; i < s->n_sort_clause; ++i) {
    q.order_by.push_back(lowerOrderBy(s->sort_clause[i]));
  }
  lowerLimit(s->limit_count, &q, diags);
  if (s->limit_offset) {
    Value offset;
    if (lowerLiteralValue(s->limit_offset, &offset, diags, "OFFSET") && !offset.isNumber()) {
      diags.push_back(makeDiag(SqlDiagnostic::Phase::Lower, "unsupported_sql_feature",
                               "OFFSET expects a numeric literal.",
                               "Use OFFSET with an integer literal, or omit OFFSET."));
    } else if (offset.isNumber() && offset.asInt64() != 0) {
      diags.push_back(unsupported("offset", "OFFSET not supported in Velaria SQL v1.",
                                  "Use range-based slicing on the result set."));
    }
  }
  q.keyword_search = extensions.keyword_search;
  q.hybrid_search = extensions.hybrid_search;
  q.window = extensions.window;
  return q;
}

Row lowerValuesRow(const PgQuery__Node* n, std::vector<SqlDiagnostic>& diags) {
  Row row;
  if (!n || n->node_case != PG_QUERY__NODE__NODE_LIST || !n->list) return row;
  for (size_t i = 0; i < n->list->n_items; ++i) {
    Value value;
    lowerLiteralValue(n->list->items[i], &value, diags, "VALUES");
    row.push_back(std::move(value));
  }
  return row;
}

CreateTableStmt lowerCreate(const PgQuery__CreateStmt* create,
                            const VelariaSqlExtensions& extensions) {
  CreateTableStmt out;
  if (!create) return out;
  out.table = rangeName(create->relation);
  out.kind = extensions.has_create_kind ? extensions.create_kind : TableKind::Regular;
  out.provider = extensions.create_provider;
  out.options = extensions.create_options;
  for (size_t i = 0; i < create->n_table_elts; ++i) {
    auto* node = create->table_elts[i];
    if (!node || node->node_case != PG_QUERY__NODE__NODE_COLUMN_DEF || !node->column_def) continue;
    SqlColumnDef column;
    column.name = node->column_def->colname ? node->column_def->colname : "";
    column.type = typeNameStr(node->column_def->type_name);
    out.columns.push_back(std::move(column));
  }
  return out;
}

void lowerInsertColumns(const PgQuery__InsertStmt* insert, InsertStmt* out) {
  if (!insert || !out) return;
  for (size_t i = 0; i < insert->n_cols; ++i) {
    auto* node = insert->cols[i];
    if (!node) continue;
    if (node->node_case == PG_QUERY__NODE__NODE_RES_TARGET && node->res_target) {
      out->columns.push_back(node->res_target->name ? node->res_target->name : "");
    } else if (node->node_case == PG_QUERY__NODE__NODE_COLUMN_REF) {
      out->columns.push_back(extractCol(node).name);
    } else {
      const auto text = nodeStr(node);
      if (!text.empty()) out->columns.push_back(text);
    }
  }
}

InsertStmt lowerInsert(const PgQuery__InsertStmt* insert,
                       const VelariaSqlExtensions& extensions,
                       std::vector<SqlDiagnostic>& diags,
                       SqlStatementKind* kind) {
  InsertStmt out;
  out.select_from = false;
  out.table = insert ? rangeName(insert->relation) : "";
  lowerInsertColumns(insert, &out);
  if (!insert || !insert->select_stmt || insert->select_stmt->node_case != PG_QUERY__NODE__NODE_SELECT_STMT) {
    if (kind) *kind = SqlStatementKind::InsertValues;
    return out;
  }
  auto* select = insert->select_stmt->select_stmt;
  if (select && select->n_values_lists > 0) {
    if (kind) *kind = SqlStatementKind::InsertValues;
    for (size_t i = 0; i < select->n_values_lists; ++i) {
      out.values.push_back(lowerValuesRow(select->values_lists[i], diags));
    }
    return out;
  }
  if (kind) *kind = SqlStatementKind::InsertSelect;
  out.select_from = true;
  out.query = lowerSelect(select, extensions, diags);
  return out;
}

bool hasBlockingDiagnostics(const std::vector<SqlDiagnostic>& diagnostics) {
  return std::any_of(diagnostics.begin(), diagnostics.end(), [](const SqlDiagnostic& d) {
    return !d.error_type.empty();
  });
}

}  // namespace

bool PgAstLowerer::lower(const PgQueryParseResultHolder& parse_result,
                         const SqlFeaturePolicy& policy,
                         const VelariaSqlExtensions& extensions,
                         SqlStatement& out_statement,
                         UnboundPlan&,
                         std::vector<SqlDiagnostic>& out_diagnostics) {
  const char* data = parse_result.parseTreeData();
  const size_t len = parse_result.parseTreeLen();
  if (!data || len == 0) {
    out_diagnostics.push_back(makeDiag(SqlDiagnostic::Phase::Lower,
                                       "protobuf_error", "Empty parse tree data."));
    return false;
  }

  ProtobufGuard parsed{
      pg_query__parse_result__unpack(nullptr, len, reinterpret_cast<const uint8_t*>(data))};
  if (!parsed) {
    out_diagnostics.push_back(makeDiag(SqlDiagnostic::Phase::Lower,
                                       "protobuf_error", "Failed to unpack protobuf parse result."));
    return false;
  }
  if (parsed->n_stmts > 1 && !policy.allow_multi_statement) {
    out_diagnostics.push_back(unsupported("multi_statement",
      "Multiple SQL statements detected (" + std::to_string(parsed->n_stmts) + ").",
      "Execute each statement separately."));
    return false;
  }
  if (parsed->n_stmts == 0 || !parsed->stmts[0] || !parsed->stmts[0]->stmt) {
    out_diagnostics.push_back(makeDiag(SqlDiagnostic::Phase::Lower,
                                       "empty_query", "No statements in query."));
    return false;
  }

  const auto validation = validateFeatures(parse_result, policy);
  out_diagnostics.insert(out_diagnostics.end(), validation.begin(), validation.end());
  if (hasBlockingDiagnostics(out_diagnostics)) return false;

  auto* stmt = parsed->stmts[0]->stmt;
  switch (stmt->node_case) {
    case PG_QUERY__NODE__NODE_SELECT_STMT:
      out_statement.kind = SqlStatementKind::Select;
      out_statement.query = lowerSelect(stmt->select_stmt, extensions, out_diagnostics);
      break;
    case PG_QUERY__NODE__NODE_CREATE_STMT:
      out_statement.kind = SqlStatementKind::CreateTable;
      out_statement.create = lowerCreate(stmt->create_stmt, extensions);
      break;
    case PG_QUERY__NODE__NODE_INSERT_STMT:
      out_statement.insert = lowerInsert(stmt->insert_stmt, extensions, out_diagnostics,
                                         &out_statement.kind);
      break;
    default:
      out_diagnostics.push_back(makeDiag(SqlDiagnostic::Phase::Lower,
                                         "unsupported_statement",
                                         "not supported in SQL v1: unsupported SQL statement."));
      return false;
  }
  return !hasBlockingDiagnostics(out_diagnostics);
}

}  // namespace sql
}  // namespace dataflow
