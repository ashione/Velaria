#include "src/dataflow/core/logical/sql/sql_planner.h"

#include <algorithm>
#include <memory>
#include <sstream>
#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "src/dataflow/core/execution/columnar_batch.h"
#include "src/dataflow/core/execution/runtime/execution_optimizer.h"

namespace dataflow {
namespace sql {

namespace {

[[noreturn]] void throwUnsupportedSqlV1(const std::string& detail) {
  throw SQLUnsupportedError("not supported in SQL v1: " + detail);
}

[[noreturn]] void throwTableKindViolation(const std::string& detail) {
  throw SQLTableKindError("table-kind constraint violation: " + detail);
}

std::string opToString(BinaryOperatorKind op) {
  switch (op) {
    case BinaryOperatorKind::Eq:
      return "=";
    case BinaryOperatorKind::Ne:
      return "!=";
    case BinaryOperatorKind::Lt:
      return "<";
    case BinaryOperatorKind::Lte:
      return "<=";
    case BinaryOperatorKind::Gt:
      return ">";
    case BinaryOperatorKind::Gte:
      return ">=";
  }
  return "=";
}

AggregateFunction toPlanFunction(AggregateFunctionKind fn) {
  switch (fn) {
    case AggregateFunctionKind::Sum:
      return AggregateFunction::Sum;
    case AggregateFunctionKind::Count:
      return AggregateFunction::Count;
    case AggregateFunctionKind::Avg:
      return AggregateFunction::Avg;
    case AggregateFunctionKind::Min:
      return AggregateFunction::Min;
    case AggregateFunctionKind::Max:
      return AggregateFunction::Max;
    case AggregateFunctionKind::StdDev:
      throw SQLUnsupportedError("not supported in SQL v1: aggregate function STDDEV");
  }
  throw SQLUnsupportedError("not supported in SQL v1: aggregate function");
}

ComputedColumnKind toComputedFunctionKind(sql::StringFunctionKind function) {
  switch (function) {
    case sql::StringFunctionKind::Length:
      return ComputedColumnKind::StringLength;
    case sql::StringFunctionKind::Lower:
      return ComputedColumnKind::StringLower;
    case sql::StringFunctionKind::Upper:
      return ComputedColumnKind::StringUpper;
    case sql::StringFunctionKind::Trim:
      return ComputedColumnKind::StringTrim;
    case sql::StringFunctionKind::Concat:
      return ComputedColumnKind::StringConcat;
    case sql::StringFunctionKind::Reverse:
      return ComputedColumnKind::StringReverse;
    case sql::StringFunctionKind::ConcatWs:
      return ComputedColumnKind::StringConcatWs;
    case sql::StringFunctionKind::Left:
      return ComputedColumnKind::StringLeft;
    case sql::StringFunctionKind::Right:
      return ComputedColumnKind::StringRight;
    case sql::StringFunctionKind::Substr:
      return ComputedColumnKind::StringSubstr;
    case sql::StringFunctionKind::Ltrim:
      return ComputedColumnKind::StringLtrim;
    case sql::StringFunctionKind::Rtrim:
      return ComputedColumnKind::StringRtrim;
    case sql::StringFunctionKind::Position:
      return ComputedColumnKind::StringPosition;
    case sql::StringFunctionKind::Replace:
      return ComputedColumnKind::StringReplace;
    case sql::StringFunctionKind::Abs:
      return ComputedColumnKind::NumericAbs;
    case sql::StringFunctionKind::Ceil:
      return ComputedColumnKind::NumericCeil;
    case sql::StringFunctionKind::Floor:
      return ComputedColumnKind::NumericFloor;
    case sql::StringFunctionKind::Round:
      return ComputedColumnKind::NumericRound;
    case sql::StringFunctionKind::Year:
      return ComputedColumnKind::DateYear;
    case sql::StringFunctionKind::Month:
      return ComputedColumnKind::DateMonth;
    case sql::StringFunctionKind::Day:
      return ComputedColumnKind::DateDay;
  }
  return ComputedColumnKind::StringConcat;
}

std::string defaultStringAlias(const sql::StringFunctionExpr& expr, std::size_t position) {
  auto baseAlias = [](sql::StringFunctionKind function) {
    switch (function) {
      case sql::StringFunctionKind::Length:
        return std::string("length");
      case sql::StringFunctionKind::Reverse:
        return std::string("reverse");
      case sql::StringFunctionKind::Lower:
        return std::string("lower");
      case sql::StringFunctionKind::Upper:
        return std::string("upper");
      case sql::StringFunctionKind::Trim:
        return std::string("trim");
      case sql::StringFunctionKind::Concat:
        return std::string("concat");
      case sql::StringFunctionKind::ConcatWs:
        return std::string("concat_ws");
      case sql::StringFunctionKind::Left:
        return std::string("left");
      case sql::StringFunctionKind::Right:
        return std::string("right");
      case sql::StringFunctionKind::Substr:
        return std::string("substr");
      case sql::StringFunctionKind::Ltrim:
        return std::string("ltrim");
      case sql::StringFunctionKind::Rtrim:
        return std::string("rtrim");
      case sql::StringFunctionKind::Position:
        return std::string("position");
      case sql::StringFunctionKind::Replace:
        return std::string("replace");
      case sql::StringFunctionKind::Abs:
        return std::string("abs");
      case sql::StringFunctionKind::Ceil:
        return std::string("ceil");
      case sql::StringFunctionKind::Floor:
        return std::string("floor");
      case sql::StringFunctionKind::Round:
        return std::string("round");
      case sql::StringFunctionKind::Year:
        return std::string("year");
      case sql::StringFunctionKind::Month:
        return std::string("month");
      case sql::StringFunctionKind::Day:
        return std::string("day");
    }
    return std::string("string_func");
  }(expr.function);

  if (expr.args.empty()) {
    return baseAlias + "_" + std::to_string(position + 1);
  }
  const auto& arg = expr.args.front();
  if (arg.is_column) {
    std::string key = arg.column.name;
    if (!arg.column.qualifier.empty()) {
      key = arg.column.qualifier + "_" + key;
    }
    return key.empty() ? baseAlias + "_" + std::to_string(position + 1) : baseAlias + "_" + key;
  }
  if (arg.literal.type() == DataType::String) {
    return baseAlias + "_" + arg.literal.asString();
  }
  return baseAlias + "_" + std::to_string(position + 1);
}

std::string defaultAggregateAlias(AggregateFunctionKind fn, const std::string& arg,
                                 const std::string& userAlias) {
  if (!userAlias.empty()) return userAlias;
  if (fn == AggregateFunctionKind::Count) return "count";
  if (arg.empty()) return "count";
  switch (fn) {
    case AggregateFunctionKind::Sum:
      return "sum_" + arg;
    case AggregateFunctionKind::Avg:
      return "avg_" + arg;
    case AggregateFunctionKind::Min:
      return "min_" + arg;
    case AggregateFunctionKind::Max:
      return "max_" + arg;
    default:
      return "count";
  }
}

bool isUnaryStep(LogicalStepKind kind) {
  return kind == LogicalStepKind::Filter || kind == LogicalStepKind::PredicateFilter ||
         kind == LogicalStepKind::HybridSearch ||
         kind == LogicalStepKind::Project ||
         kind == LogicalStepKind::Limit || kind == LogicalStepKind::Having ||
         kind == LogicalStepKind::WithColumn;
}

bool isBarrierStep(LogicalStepKind kind) {
  return kind == LogicalStepKind::Scan || kind == LogicalStepKind::KeywordSearch ||
         kind == LogicalStepKind::Union ||
         kind == LogicalStepKind::Join ||
         kind == LogicalStepKind::Aggregate || kind == LogicalStepKind::OrderBy;
}

bool isAggregateQuery(const SqlQuery& query) {
  return !query.group_by.empty() ||
         std::any_of(query.select_items.begin(), query.select_items.end(),
                     [](const SelectItem& item) { return item.is_aggregate; });
}

bool predicateExprHasAggregate(const std::shared_ptr<PredicateExpr>& expr) {
  if (!expr) return false;
  if (expr->kind == PredicateExprKind::Comparison) {
    return expr->predicate.lhs_is_aggregate;
  }
  return predicateExprHasAggregate(expr->left) || predicateExprHasAggregate(expr->right);
}

bool predicateExprIsSimpleComparison(const std::shared_ptr<PredicateExpr>& expr) {
  return expr && expr->kind == PredicateExprKind::Comparison;
}

bool collectConjunctivePredicates(const std::shared_ptr<PredicateExpr>& expr,
                                  std::vector<Predicate>* out) {
  if (!expr || out == nullptr) return false;
  if (expr->kind == PredicateExprKind::Comparison) {
    out->push_back(expr->predicate);
    return true;
  }
  if (expr->kind != PredicateExprKind::And) {
    return false;
  }
  return collectConjunctivePredicates(expr->left, out) &&
         collectConjunctivePredicates(expr->right, out);
}

using PredicateRewriter = std::function<Predicate(const Predicate&)>;

std::shared_ptr<PredicateExpr> rewritePredicateExpr(const std::shared_ptr<PredicateExpr>& expr,
                                                    const PredicateRewriter& rewrite_predicate) {
  if (!expr) return nullptr;
  auto out = std::make_shared<PredicateExpr>();
  out->kind = expr->kind;
  if (expr->kind == PredicateExprKind::Comparison) {
    out->predicate = rewrite_predicate(expr->predicate);
    return out;
  }
  out->left = rewritePredicateExpr(expr->left, rewrite_predicate);
  out->right = rewritePredicateExpr(expr->right, rewrite_predicate);
  return out;
}

std::shared_ptr<::dataflow::PlanPredicateExpr> toPlanPredicateExpr(
    const std::shared_ptr<PredicateExpr>& expr) {
  if (!expr) return nullptr;
  auto out = std::make_shared<::dataflow::PlanPredicateExpr>();
  if (expr->kind == PredicateExprKind::Comparison) {
    out->kind = ::dataflow::PlanPredicateExprKind::Comparison;
    out->comparison.column_index =
        static_cast<std::size_t>(std::stoull(expr->predicate.lhs.name));
    out->comparison.value = expr->predicate.rhs;
    out->comparison.op = opToString(expr->predicate.op);
    return out;
  }
  out->kind = expr->kind == PredicateExprKind::And
                  ? ::dataflow::PlanPredicateExprKind::And
                  : ::dataflow::PlanPredicateExprKind::Or;
  out->left = toPlanPredicateExpr(expr->left);
  out->right = toPlanPredicateExpr(expr->right);
  return out;
}

VectorDistanceMetric parseHybridMetric(const std::string& metric) {
  if (metric == "cosine" || metric == "cosin") {
    return VectorDistanceMetric::Cosine;
  }
  if (metric == "dot") {
    return VectorDistanceMetric::Dot;
  }
  if (metric == "l2") {
    return VectorDistanceMetric::L2;
  }
  throw SQLSemanticError("unsupported HYBRID SEARCH metric: " + metric);
}

bool hasMixedSelectStar(const SqlQuery& query) {
  const auto star_count = std::count_if(
      query.select_items.begin(), query.select_items.end(),
      [](const SelectItem& item) { return item.is_all; });
  return star_count > 0 && query.select_items.size() > 1;
}

const char* streamNodeKindName(StreamPlanNodeKind kind) {
  switch (kind) {
    case StreamPlanNodeKind::Scan:
      return "Scan";
    case StreamPlanNodeKind::Filter:
      return "Filter";
    case StreamPlanNodeKind::Project:
      return "Project";
    case StreamPlanNodeKind::WithColumn:
      return "WithColumn";
    case StreamPlanNodeKind::Limit:
      return "Limit";
    case StreamPlanNodeKind::OrderBy:
      return "OrderBy";
    case StreamPlanNodeKind::WindowAssign:
      return "WindowAssign";
    case StreamPlanNodeKind::Aggregate:
      return "Aggregate";
    case StreamPlanNodeKind::Sink:
      return "Sink";
  }
  return "Unknown";
}

std::string joinStrings(const std::vector<std::string>& values, const std::string& delim) {
  std::ostringstream out;
  for (size_t i = 0; i < values.size(); ++i) {
    if (i > 0) out << delim;
    out << values[i];
  }
  return out.str();
}

void validateUnionCompatibility(const Schema& left, const Schema& right) {
  if (left.fields.size() != right.fields.size()) {
    throw SQLSemanticError("UNION requires the same number of projected columns");
  }
}

void validateUnionBranchQuery(const SqlQuery& query) {
  if (!query.order_by.empty()) {
    throwUnsupportedSqlV1("UNION does not support ORDER BY in branch query");
  }
  if (query.limit.has_value()) {
    throwUnsupportedSqlV1("UNION does not support LIMIT in branch query");
  }
  if (query.hybrid_search.has_value()) {
    throwUnsupportedSqlV1("UNION does not support HYBRID SEARCH in branch query");
  }
  if (query.window.has_value()) {
    throwUnsupportedSqlV1("UNION does not support WINDOW in branch query");
  }
}

void ensureSingleTableStreamQuery(const SqlQuery& query) {
  if (!query.union_terms.empty()) {
    throwUnsupportedSqlV1("stream SQL does not support UNION");
  }
  if (query.keyword_search.has_value()) {
    throwUnsupportedSqlV1("stream SQL does not support KEYWORD SEARCH");
  }
  if (!query.has_from) {
    throw SQLSemanticError("stream SQL requires FROM");
  }
  if (query.join.has_value()) {
    throwUnsupportedSqlV1("stream SQL does not support JOIN");
  }
  if (std::any_of(query.select_items.begin(), query.select_items.end(),
                  [](const SelectItem& item) { return item.is_literal; })) {
    throwUnsupportedSqlV1("stream SQL does not support literal projection");
  }
  if (query.hybrid_search.has_value()) {
    throwUnsupportedSqlV1("stream SQL does not support HYBRID SEARCH");
  }
}

void ensureQualifierMatches(const ColumnRef& ref, const FromItem& from) {
  if (ref.qualifier.empty() || ref.qualifier == from.name) return;
  if (!from.alias.empty() && ref.qualifier == from.alias) return;
  throw SQLSemanticError("stream SQL qualifier does not match source: " + ref.qualifier);
}

std::string resolveStreamColumnName(const ColumnRef& ref, const FromItem& from) {
  ensureQualifierMatches(ref, from);
  return ref.name;
}

void ensureStringFunctionSupported(const StringFunctionExpr& function) {
  switch (function.function) {
    case StringFunctionKind::Length:
      if (function.args.size() != 1) {
        throw SQLSemanticError("LENGTH expects 1 argument");
      }
      break;
    case StringFunctionKind::Lower:
      if (function.args.size() != 1) {
        throw SQLSemanticError("LOWER expects 1 argument");
      }
      break;
    case StringFunctionKind::Upper:
      if (function.args.size() != 1) {
        throw SQLSemanticError("UPPER expects 1 argument");
      }
      break;
    case StringFunctionKind::Trim:
      if (function.args.size() != 1) {
        throw SQLSemanticError("TRIM expects 1 argument");
      }
      break;
    case StringFunctionKind::Reverse:
      if (function.args.size() != 1) {
        throw SQLSemanticError("REVERSE expects 1 argument");
      }
      break;
    case StringFunctionKind::Concat:
      if (function.args.empty()) {
        throw SQLSemanticError("CONCAT expects at least 1 argument");
      }
      break;
    case StringFunctionKind::ConcatWs:
      if (function.args.size() < 2) {
        throw SQLSemanticError("CONCAT_WS requires at least 2 arguments");
      }
      break;
    case StringFunctionKind::Substr:
      if (function.args.size() < 2 || function.args.size() > 3) {
        throw SQLSemanticError("SUBSTR expects 2 or 3 arguments");
      }
      break;
    case StringFunctionKind::Replace:
      if (function.args.size() != 3) {
        throw SQLSemanticError("REPLACE requires exactly 3 arguments");
      }
      break;
    case StringFunctionKind::Left:
      if (function.args.size() != 2) {
        throw SQLSemanticError("LEFT requires 2 arguments");
      }
      break;
    case StringFunctionKind::Right:
      if (function.args.size() != 2) {
        throw SQLSemanticError("RIGHT requires 2 arguments");
      }
      break;
    case StringFunctionKind::Position:
      if (function.args.size() != 2) {
        throw SQLSemanticError("POSITION requires 2 arguments");
      }
      break;
    case StringFunctionKind::Ltrim:
    case StringFunctionKind::Rtrim:
      if (function.args.size() != 1) {
        throw SQLSemanticError("LTRIM/RTRIM requires 1 argument");
      }
      break;
    case StringFunctionKind::Abs:
      if (function.args.size() != 1) {
        throw SQLSemanticError("ABS expects 1 argument");
      }
      break;
    case StringFunctionKind::Ceil:
      if (function.args.size() != 1) {
        throw SQLSemanticError("CEIL expects 1 argument");
      }
      break;
    case StringFunctionKind::Floor:
      if (function.args.size() != 1) {
        throw SQLSemanticError("FLOOR expects 1 argument");
      }
      break;
    case StringFunctionKind::Round:
      if (function.args.size() != 1) {
        throw SQLSemanticError("ROUND expects 1 argument");
      }
      break;
    case StringFunctionKind::Year:
    case StringFunctionKind::Month:
    case StringFunctionKind::Day:
      if (function.args.size() != 1) {
        throw SQLSemanticError("YEAR/MONTH/DAY expects 1 argument");
      }
      break;
  }
}

std::string aggregateOutputName(const SelectItem& item) {
  if (!item.is_aggregate) {
    throw SQLSemanticError("expected aggregate select item");
  }
  return defaultAggregateAlias(item.aggregate.function, item.aggregate.argument.name, item.alias);
}

void validateStreamAggregate(const AggregateExpr& aggregate) {
  switch (aggregate.function) {
    case AggregateFunctionKind::Sum:
    case AggregateFunctionKind::Count:
    case AggregateFunctionKind::Avg:
    case AggregateFunctionKind::Min:
    case AggregateFunctionKind::Max:
      return;
    case AggregateFunctionKind::StdDev:
      break;
  }
  throw SQLSemanticError("unsupported stream SQL aggregate");
}

std::string streamAggregateStateLabel(const StreamAggregateSpec& aggregate) {
  switch (aggregate.function) {
    case AggregateFunction::Sum:
      return "group_sum:" + aggregate.output_column;
    case AggregateFunction::Count:
      return "group_count:" + aggregate.output_column;
    case AggregateFunction::Avg:
      return "group_avg:" + aggregate.output_column;
    case AggregateFunction::Min:
      return "group_min:" + aggregate.output_column;
    case AggregateFunction::Max:
      return "group_max:" + aggregate.output_column;
  }
  return "group_aggregate:" + aggregate.output_column;
}

StreamAggregateSpec toStreamAggregateSpec(const SelectItem& item, const FromItem& from) {
  if (!item.is_aggregate) {
    throw SQLSemanticError("expected aggregate select item");
  }
  validateStreamAggregate(item.aggregate);

  StreamAggregateSpec spec;
  spec.function = toPlanFunction(item.aggregate.function);
  spec.output_column = aggregateOutputName(item);
  spec.is_count_star = item.aggregate.function == AggregateFunctionKind::Count;
  if (!spec.is_count_star) {
    spec.value_column = resolveStreamColumnName(item.aggregate.argument, from);
  } else if (!item.aggregate.count_all) {
    throw SQLSemanticError("stream SQL only supports COUNT(*)");
  }
  spec.state_label = streamAggregateStateLabel(spec);
  return spec;
}

std::string streamAggregateExplain(const StreamAggregateSpec& aggregate) {
  switch (aggregate.function) {
    case AggregateFunction::Sum:
      return "SUM(" + aggregate.value_column + ") AS " + aggregate.output_column;
    case AggregateFunction::Count:
      return "COUNT(*) AS " + aggregate.output_column;
    case AggregateFunction::Avg:
      return "AVG(" + aggregate.value_column + ") AS " + aggregate.output_column;
    case AggregateFunction::Min:
      return "MIN(" + aggregate.value_column + ") AS " + aggregate.output_column;
    case AggregateFunction::Max:
      return "MAX(" + aggregate.value_column + ") AS " + aggregate.output_column;
  }
  return aggregate.output_column;
}

struct RelationView {
  std::string sourceName;
  std::string alias;
  std::shared_ptr<const Schema> schema;
  std::size_t offset = 0;
};

class RelationContext {
 public:
  void addRelation(const FromItem& item, const Schema& schema, std::size_t offset) {
    RelationView v;
    v.sourceName = item.name;
    v.alias = item.alias.empty() ? item.name : item.alias;
    v.schema = std::make_shared<Schema>(schema);
    v.offset = offset;
    relations_.push_back(v);
    aliases_[v.alias] = relations_.size() - 1;
    aliases_[v.sourceName] = relations_.size() - 1;
  }

  void clear() {
    relations_.clear();
    aliases_.clear();
  }

  const RelationView& getRelation(const std::string& aliasOrName) const {
    const auto it = aliases_.find(aliasOrName);
    if (it == aliases_.end()) {
      throw SQLSemanticError("unknown table or alias: " + aliasOrName);
    }
    return relations_[it->second];
  }

  std::size_t resolveColumn(const ColumnRef& col, std::string* outRelationName = nullptr) const {
    if (!col.qualifier.empty()) {
      const auto& rel = getRelation(col.qualifier);
      if (!rel.schema->has(col.name)) {
        throw SQLSemanticError("column not found in " + col.qualifier + ": " + col.name);
      }
      if (outRelationName) {
        *outRelationName = rel.alias;
      }
      return rel.offset + rel.schema->indexOf(col.name);
    }

    std::size_t found = 0;
    std::size_t index = 0;
    std::string relationName;
    for (const auto& rel : relations_) {
      if (rel.schema->has(col.name)) {
        if (found == 0) {
          index = rel.offset + rel.schema->indexOf(col.name);
          relationName = rel.alias;
        }
        ++found;
      }
    }
    if (found == 0) {
      throw SQLSemanticError("column not found: " + col.name);
    }
    if (found > 1) {
      throw SQLSemanticError("ambiguous column: " + col.name);
    }
    if (outRelationName) {
      *outRelationName = relationName;
    }
    return index;
  }

  std::vector<std::size_t> resolveAllFromRelation(const std::string& qualifier) const {
    const auto& rel = getRelation(qualifier);
    std::vector<std::size_t> indices;
    indices.reserve(rel.schema->fields.size());
    for (std::size_t i = 0; i < rel.schema->fields.size(); ++i) {
      indices.push_back(rel.offset + i);
    }
    return indices;
  }

 private:
  std::vector<RelationView> relations_;
  std::unordered_map<std::string, std::size_t> aliases_;
};

std::vector<ComputedColumnArg> resolveStringFunctionArgs(const StringFunctionExpr& function,
                                                        const RelationContext& ctx) {
  ensureStringFunctionSupported(function);
  std::vector<ComputedColumnArg> args;
  args.reserve(function.args.size());
  for (const auto& arg : function.args) {
    ComputedColumnArg out_arg;
    out_arg.is_literal = !arg.is_column;
    if (arg.is_column) {
      out_arg.source_column_index = ctx.resolveColumn(arg.column, nullptr);
      out_arg.literal = Value();
    } else {
      out_arg.source_column_index = static_cast<size_t>(-1);
      out_arg.literal = arg.literal;
    }
    args.push_back(std::move(out_arg));
  }
  return args;
}

std::vector<std::string> resolveKeywordColumns(const KeywordSearchSpec& keyword,
                                               RelationContext& ctx,
                                               const Schema& schema) {
  std::vector<std::string> columns;
  columns.reserve(keyword.columns.size());
  for (const auto& column : keyword.columns) {
    const auto idx = ctx.resolveColumn(column, nullptr);
    columns.push_back(schema.fields[idx]);
  }
  return columns;
}

std::vector<ComputedColumnArg> resolveStreamStringFunctionArgs(const StringFunctionExpr& function,
                                                              const FromItem& from) {
  ensureStringFunctionSupported(function);
  std::vector<ComputedColumnArg> args;
  args.reserve(function.args.size());
  for (const auto& arg : function.args) {
    ComputedColumnArg out_arg;
    out_arg.is_literal = !arg.is_column;
    if (arg.is_column) {
      out_arg.source_column_index = static_cast<size_t>(-1);
      out_arg.source_column_name = resolveStreamColumnName(arg.column, from);
      out_arg.literal = Value();
    } else {
      out_arg.source_column_index = static_cast<size_t>(-1);
      out_arg.literal = arg.literal;
    }
    args.push_back(std::move(out_arg));
  }
  return args;
}

LogicalPlan optimizeLogical(const LogicalPlan& logical) {
  LogicalPlan out;
  out.seed = logical.seed;
  for (const auto& step : logical.steps) {
    if (step.kind == LogicalStepKind::Limit && !out.steps.empty() &&
        out.steps.back().kind == LogicalStepKind::Limit) {
      const auto merged = std::min(out.steps.back().limit, step.limit);
      out.steps.back().limit = merged;
      continue;
    }
    if (step.kind == LogicalStepKind::Project && !out.steps.empty() &&
        out.steps.back().kind == LogicalStepKind::Project) {
      out.steps.pop_back();
    }
    out.steps.push_back(step);
  }
  return out;
}

PhysicalPlan optimizePhysical(const PhysicalPlan& physical) {
  PhysicalPlan out = physical;
  bool in_pipeline = false;
  bool had_join = false;
  for (auto& step : out.steps) {
    if (isBarrierStep(step.logical_kind)) {
      in_pipeline = false;
    }
    if (!isBarrierStep(step.logical_kind) && isUnaryStep(step.logical_kind) && in_pipeline &&
        step.logical_kind != LogicalStepKind::Aggregate && step.logical_kind != LogicalStepKind::Join) {
      step.fused = true;
      step.reason = "fused in local chain";
      step.physical_kind = PhysicalStepKind::FusedUnary;
    } else if (step.logical_kind == LogicalStepKind::Join) {
      step.physical_kind = PhysicalStepKind::Join;
      step.reason = had_join ? "joined chain uses right source" : "first join chain";
      had_join = true;
    } else if (step.logical_kind == LogicalStepKind::Aggregate) {
      step.physical_kind = PhysicalStepKind::Aggregate;
      step.reason = "aggregate barrier";
    }
    if (step.physical_kind == PhysicalStepKind::SourceOnly) {
      if (step.logical_kind == LogicalStepKind::Scan) {
        step.pipeline_barrier = true;
        step.reason = "source load";
      } else if (step.physical_kind != PhysicalStepKind::Join &&
                 step.physical_kind != PhysicalStepKind::Aggregate) {
        step.physical_kind = PhysicalStepKind::FusedUnary;
        step.reason = "single unary stage";
      }
    }
    if (!isUnaryStep(step.logical_kind)) {
      step.pipeline_barrier = true;
    } else {
      step.pipeline_barrier = false;
    }
    if (!step.pipeline_barrier) {
      in_pipeline = true;
    }
  }
  return out;
}

bool streamPlanIsPartitionLocal(const StreamPlanNode& node) {
  return node.kind == StreamPlanNodeKind::Scan || node.kind == StreamPlanNodeKind::Filter ||
         node.kind == StreamPlanNodeKind::Project || node.kind == StreamPlanNodeKind::WithColumn ||
         node.kind == StreamPlanNodeKind::WindowAssign;
}

bool isActorHotPathAggregate(const StreamPlanNode& node) {
  if (node.kind != StreamPlanNodeKind::Aggregate || !node.stateful || node.group_keys.empty() ||
      node.aggregates.empty()) {
    return false;
  }
  return std::all_of(
      node.aggregates.begin(), node.aggregates.end(), [](const StreamAggregateSpec& aggregate) {
        switch (aggregate.function) {
          case AggregateFunction::Sum:
          case AggregateFunction::Count:
          case AggregateFunction::Avg:
          case AggregateFunction::Min:
          case AggregateFunction::Max:
            return true;
        }
        return false;
      });
}

std::string computedFunctionName(ComputedColumnKind function) {
  switch (function) {
    case ComputedColumnKind::Copy:
      return "copy";
    case ComputedColumnKind::StringLength:
      return "length";
    case ComputedColumnKind::StringLower:
      return "lower";
    case ComputedColumnKind::StringUpper:
      return "upper";
    case ComputedColumnKind::StringTrim:
      return "trim";
    case ComputedColumnKind::StringConcat:
      return "concat";
    case ComputedColumnKind::StringReverse:
      return "reverse";
    case ComputedColumnKind::StringConcatWs:
      return "concat_ws";
    case ComputedColumnKind::StringLeft:
      return "left";
    case ComputedColumnKind::StringRight:
      return "right";
    case ComputedColumnKind::StringSubstr:
      return "substr";
    case ComputedColumnKind::StringLtrim:
      return "ltrim";
    case ComputedColumnKind::StringRtrim:
      return "rtrim";
    case ComputedColumnKind::StringPosition:
      return "position";
    case ComputedColumnKind::StringReplace:
      return "replace";
    case ComputedColumnKind::NumericAbs:
      return "abs";
    case ComputedColumnKind::NumericCeil:
      return "ceil";
    case ComputedColumnKind::NumericFloor:
      return "floor";
    case ComputedColumnKind::NumericRound:
      return "round";
    case ComputedColumnKind::DateYear:
      return "year";
    case ComputedColumnKind::DateMonth:
      return "month";
    case ComputedColumnKind::DateDay:
      return "day";
  }
  return "copy";
}

std::string computedArgToString(const ComputedColumnArg& arg) {
  if (arg.is_literal) {
    return "literal(" + arg.literal.toString() + ")";
  }
  if (!arg.source_column_name.empty()) {
    return "column(" + arg.source_column_name + ")";
  }
  return "column(" + std::to_string(arg.source_column_index) + ")";
}

std::string orderByOutputName(const OrderByItem& item) {
  if (!item.column.qualifier.empty()) {
    return item.column.qualifier + "." + item.column.name;
  }
  return item.column.name;
}

std::vector<std::size_t> resolveOrderColumns(const SqlQuery& query, const Schema& schema) {
  std::vector<std::size_t> indices;
  indices.reserve(query.order_by.size());
  for (const auto& item : query.order_by) {
    const auto qualified = orderByOutputName(item);
    if (schema.has(qualified)) {
      indices.push_back(schema.indexOf(qualified));
      continue;
    }
    if (schema.has(item.column.name)) {
      indices.push_back(schema.indexOf(item.column.name));
      continue;
    }
    throw SQLSemanticError("ORDER BY column must appear in SELECT output in SQL v1: " + qualified);
  }
  return indices;
}

std::vector<std::string> resolveStreamOrderColumns(const SqlQuery& query, const FromItem& from) {
  std::vector<std::string> columns;
  columns.reserve(query.order_by.size());
  for (const auto& item : query.order_by) {
    (void)from;
    columns.push_back(orderByOutputName(item));
  }
  return columns;
}

std::vector<std::string> resolveStreamOrderColumns(const SqlQuery& query,
                                                   const std::vector<std::string>& output_columns) {
  std::vector<std::string> columns;
  columns.reserve(query.order_by.size());
  for (const auto& item : query.order_by) {
    const auto qualified = orderByOutputName(item);
    auto exact = std::find(output_columns.begin(), output_columns.end(), qualified);
    if (exact != output_columns.end()) {
      columns.push_back(*exact);
      continue;
    }
    auto unqualified = std::find(output_columns.begin(), output_columns.end(), item.column.name);
    if (unqualified != output_columns.end()) {
      columns.push_back(*unqualified);
      continue;
    }
    throw SQLSemanticError("ORDER BY column must appear in SELECT output in SQL v1: " + qualified);
  }
  return columns;
}

std::vector<bool> resolveOrderDirections(const SqlQuery& query) {
  std::vector<bool> ascending;
  ascending.reserve(query.order_by.size());
  for (const auto& item : query.order_by) {
    ascending.push_back(item.ascending);
  }
  return ascending;
}

}  // namespace

LogicalPlan SqlPlanner::buildLogicalPlan(const SqlQuery& query, const ViewCatalog& catalog) const {
  if (!query.union_terms.empty()) {
    validateUnionBranchQuery(query);
    for (const auto& term : query.union_terms) {
      if (!term.query) {
        throw SQLSemanticError("UNION branch is missing query");
      }
      validateUnionBranchQuery(*term.query);
    }
  }
  if (query.window.has_value()) {
    throwUnsupportedSqlV1("WINDOW BY ... EVERY ... AS ... is only supported in stream SQL");
  }
  if (query.keyword_search.has_value() && query.join.has_value()) {
    throw SQLSemanticError("KEYWORD SEARCH does not support JOIN queries");
  }
  if (query.hybrid_search.has_value() && query.join.has_value()) {
    throw SQLSemanticError("HYBRID SEARCH does not support JOIN queries");
  }
  if (hasMixedSelectStar(query)) {
    throwUnsupportedSqlV1("SELECT * cannot be mixed with other projections");
  }
  LogicalPlan logical;

  if (!query.has_from) {
    if (query.join.has_value() || query.where || query.keyword_search.has_value() ||
        query.hybrid_search.has_value() || !query.group_by.empty() ||
        query.having) {
      throw SQLSemanticError("SELECT without FROM only supports literal projection");
    }
    if (std::any_of(query.select_items.begin(), query.select_items.end(),
                    [](const SelectItem& item) { return item.is_string_function; })) {
      throwUnsupportedSqlV1("string function is not supported without FROM");
    }
    std::vector<std::string> fields;
    Row row;
    fields.reserve(query.select_items.size());
    row.reserve(query.select_items.size());
    for (std::size_t i = 0; i < query.select_items.size(); ++i) {
      const auto& item = query.select_items[i];
      if (!item.is_literal || item.is_all || item.is_table_all || item.is_aggregate ||
          !item.column.name.empty()) {
        throwUnsupportedSqlV1("SELECT without FROM only supports literal projection");
      }
      fields.push_back(item.alias.empty() ? "expr" + std::to_string(i + 1) : item.alias);
      row.push_back(item.literal);
    }
    logical.seed = DataFrame(Table(Schema(std::move(fields)), std::vector<Row>{std::move(row)}));
    LogicalPlanStep scan;
    scan.kind = LogicalStepKind::Scan;
    scan.source_name = "__values__";
    scan.source = logical.seed;
    logical.steps.push_back(scan);
    if (!query.order_by.empty()) {
      LogicalPlanStep order;
      order.kind = LogicalStepKind::OrderBy;
      order.order_indices = resolveOrderColumns(query, logical.seed.schema());
      order.order_ascending = resolveOrderDirections(query);
      logical.steps.push_back(order);
    }
    if (query.limit.has_value()) {
      LogicalPlanStep limit;
      limit.kind = LogicalStepKind::Limit;
      limit.limit = *query.limit;
      limit.limit_set = true;
      logical.steps.push_back(limit);
    }
    return logical;
  }

  if (catalog.isSinkTable(query.from.name)) {
    throwTableKindViolation("SELECT from SINK table is not allowed: " + query.from.name);
  }

  DataFrame current = catalog.getView(query.from.name);
  logical.seed = current;
  RelationContext ctx;
  auto leftSchema = current.schema();
  const auto baseLeftSchema = leftSchema;
  ctx.addRelation(query.from, baseLeftSchema, 0);

  LogicalPlanStep scan;
  scan.kind = LogicalStepKind::Scan;
  scan.source_name = query.from.name;
  scan.source = current;
  logical.steps.push_back(scan);

  std::size_t leftWidth = leftSchema.fields.size();

  if (query.join.has_value()) {
    const auto& join = query.join.value();
    const auto& rightView = join.right;
    if (catalog.isSinkTable(rightView.name)) {
      throwTableKindViolation("JOIN with SINK table is not allowed: " + rightView.name);
    }
    DataFrame right = catalog.getView(rightView.name);
    const auto rightSchema = right.schema();

    RelationContext rightCtx;
    rightCtx.addRelation(rightView, rightSchema, 0);
    std::size_t leftKey = ctx.resolveColumn(join.left_key, nullptr);
    std::size_t leftRelIdx = leftKey;
    std::size_t rightKey = rightCtx.resolveColumn(join.right_key, nullptr);
    const std::string leftJoinCol = leftSchema.fields[leftRelIdx];
    const std::string rightJoinCol = rightSchema.fields[rightKey];

    LogicalPlanStep joinStep;
    joinStep.kind = LogicalStepKind::Join;
    joinStep.join_left_column = leftJoinCol;
    joinStep.join_right_column = rightJoinCol;
    joinStep.join_kind = join.is_left ? JoinKind::Left : JoinKind::Inner;
    joinStep.join_right = right;
    logical.steps.push_back(joinStep);

    current = current.join(right, leftJoinCol, rightJoinCol,
                          join.is_left ? JoinKind::Left : JoinKind::Inner);
    leftSchema = current.schema();
    ctx.clear();
    ctx.addRelation(query.from, baseLeftSchema, 0);
    ctx.addRelation(rightView, rightSchema, leftWidth);
  }

  if (query.where) {
    if (predicateExprHasAggregate(query.where)) {
      throw SQLSemanticError("WHERE does not support aggregate expressions");
    }
    std::vector<Predicate> conjunctive;
    if (collectConjunctivePredicates(query.where, &conjunctive)) {
      for (const auto& predicate : conjunctive) {
        std::size_t idx = ctx.resolveColumn(predicate.lhs, nullptr);
        LogicalPlanStep filter;
        filter.kind = LogicalStepKind::Filter;
        filter.filter_column = idx;
        filter.filter_op = opToString(predicate.op);
        filter.filter_value = predicate.rhs;
        logical.steps.push_back(filter);
        current = current.filterByIndex(idx, filter.filter_op, filter.filter_value);
      }
    } else {
      LogicalPlanStep filter;
      filter.kind = LogicalStepKind::PredicateFilter;
      filter.predicate_expr = rewritePredicateExpr(query.where, [&](const Predicate& predicate) {
        auto rebound = predicate;
        const auto idx = ctx.resolveColumn(predicate.lhs, nullptr);
        rebound.lhs.qualifier = "__index__";
        rebound.lhs.name = std::to_string(idx);
        return rebound;
      });
      logical.steps.push_back(filter);
      current = current.filterPredicate(toPlanPredicateExpr(filter.predicate_expr));
    }
  }

  const auto whereFilteredSchema = current.schema();
  const bool hasAggregateQuery =
      !query.group_by.empty() ||
      std::any_of(query.select_items.begin(), query.select_items.end(),
                  [](const SelectItem& item) { return item.is_aggregate; });
  if (query.keyword_search.has_value() && hasAggregateQuery) {
    throw SQLSemanticError("KEYWORD SEARCH does not support aggregate queries");
  }
  if (query.hybrid_search.has_value() && hasAggregateQuery) {
    throw SQLSemanticError("HYBRID SEARCH does not support aggregate queries");
  }

  if (std::any_of(query.select_items.begin(), query.select_items.end(),
                  [](const SelectItem& item) { return item.is_literal; })) {
    throwUnsupportedSqlV1("literal projection with FROM is not supported");
  }
  if (std::any_of(query.select_items.begin(), query.select_items.end(),
                  [](const SelectItem& item) { return item.is_string_function; }) &&
      hasAggregateQuery) {
    throwUnsupportedSqlV1("string functions are not supported in aggregate queries");
  }

  if (query.keyword_search.has_value()) {
    const auto& keyword = *query.keyword_search;
    LogicalPlanStep keyword_step;
    keyword_step.kind = LogicalStepKind::KeywordSearch;
    keyword_step.keyword_columns = resolveKeywordColumns(keyword, ctx, current.schema());
    keyword_step.keyword_query_text = keyword.query_text;
    keyword_step.keyword_top_k = keyword.top_k;
    logical.steps.push_back(keyword_step);
    current = current.keywordSearch(keyword_step.keyword_columns,
                                    keyword_step.keyword_query_text,
                                    keyword_step.keyword_top_k);
    ctx.clear();
    ctx.addRelation(query.from, current.schema(), 0);
  }

  if (query.hybrid_search.has_value()) {
    const auto& hybrid = *query.hybrid_search;
    const auto vector_index = ctx.resolveColumn(hybrid.vector_column, nullptr);
    LogicalPlanStep hybrid_step;
    hybrid_step.kind = LogicalStepKind::HybridSearch;
    hybrid_step.hybrid_vector_column = current.schema().fields[vector_index];
    hybrid_step.hybrid_query_vector = Value::parseFixedVector(hybrid.query_vector);
    hybrid_step.hybrid_options.metric = parseHybridMetric(hybrid.metric);
    hybrid_step.hybrid_options.top_k = hybrid.top_k;
    hybrid_step.hybrid_options.score_threshold = hybrid.score_threshold;
    logical.steps.push_back(hybrid_step);
    current = current.hybridSearch(hybrid_step.hybrid_vector_column,
                                   hybrid_step.hybrid_query_vector,
                                   hybrid_step.hybrid_options);
    ctx.clear();
    ctx.addRelation(query.from, current.schema(), 0);
  }

  if (hasAggregateQuery) {
    if (query.having && query.group_by.empty()) {
      throw SQLSemanticError("GROUP BY required for HAVING");
    }

    std::vector<std::size_t> groupKeys;
    std::unordered_set<std::string> groupKeySet;
    for (const auto& key : query.group_by) {
      auto idx = ctx.resolveColumn(key, nullptr);
      groupKeys.push_back(idx);
      auto name = whereFilteredSchema.fields[idx];
      groupKeySet.insert(name);
    }

    std::vector<AggregateSpec> specs;
    std::vector<std::string> aggOutputs;
    for (const auto& item : query.select_items) {
      if (!item.is_aggregate) {
        if (!item.column.name.empty()) {
          const auto name = whereFilteredSchema.fields[ctx.resolveColumn(item.column, nullptr)];
          if (groupKeySet.find(name) == groupKeySet.end()) {
            throw SQLSemanticError("non-aggregate field must appear in GROUP BY: " + name);
          }
        }
        continue;
      }
      AggregateSpec spec;
      spec.function = toPlanFunction(item.aggregate.function);
      spec.output_name =
          defaultAggregateAlias(item.aggregate.function, item.aggregate.argument.name, item.alias);
      if (item.aggregate.function == AggregateFunctionKind::Count && item.aggregate.count_all) {
        spec.value_index = static_cast<std::size_t>(-1);
      } else {
        spec.value_index = ctx.resolveColumn(item.aggregate.argument, nullptr);
      }
      if (std::find(aggOutputs.begin(), aggOutputs.end(), spec.output_name) != aggOutputs.end()) {
        throw SQLSemanticError("duplicate aggregate alias: " + spec.output_name);
      }
      specs.push_back(spec);
      aggOutputs.push_back(spec.output_name);
    }

    if (specs.empty()) {
      throw SQLSemanticError("aggregate query requires at least one aggregate function");
    }

    LogicalPlanStep aggStep;
    aggStep.kind = LogicalStepKind::Aggregate;
    aggStep.group_keys = groupKeys;
    aggStep.aggregates = specs;
    logical.steps.push_back(aggStep);
    current = current.aggregate(groupKeys, specs);

    const auto aggSchema = current.schema();
    if (query.having) {
      auto resolve_having_column = [&](const Predicate& pred) -> std::size_t {
        std::string havingColumn;
        if (pred.lhs_is_aggregate) {
          std::string default_name =
              defaultAggregateAlias(pred.lhs_aggregate.function, pred.lhs_aggregate.argument.name, "");
          if (aggSchema.has(default_name)) {
            havingColumn = default_name;
          } else {
            bool matched = false;
            for (const auto& item : query.select_items) {
              if (!item.is_aggregate) continue;
              const auto& agg = item.aggregate;
              if (agg.function != pred.lhs_aggregate.function) continue;
              if (agg.count_all != pred.lhs_aggregate.count_all) continue;
              if (!agg.count_all &&
                  (agg.argument.name != pred.lhs_aggregate.argument.name ||
                   agg.argument.qualifier != pred.lhs_aggregate.argument.qualifier)) {
                continue;
              }
              havingColumn =
                  defaultAggregateAlias(item.aggregate.function, item.aggregate.argument.name, item.alias);
              matched = true;
              break;
            }
            if (!matched) {
              throw SQLSemanticError("HAVING aggregate not found in SELECT: " + default_name);
            }
          }
        } else {
          havingColumn = pred.lhs.name;
        }
        return aggSchema.indexOf(havingColumn);
      };
      std::vector<Predicate> conjunctive;
      if (collectConjunctivePredicates(query.having, &conjunctive)) {
        for (const auto& pred : conjunctive) {
          LogicalPlanStep having;
          having.kind = LogicalStepKind::Having;
          having.having_column = aggSchema.fields[resolve_having_column(pred)];
          having.filter_op = opToString(pred.op);
          having.filter_value = pred.rhs;
          logical.steps.push_back(having);
          current = current.filterByIndex(resolve_having_column(pred), having.filter_op, having.filter_value);
        }
      } else {
        LogicalPlanStep having;
        having.kind = LogicalStepKind::PredicateFilter;
        having.predicate_expr = rewritePredicateExpr(query.having, [&](const Predicate& pred) {
          auto rebound = pred;
          rebound.lhs.qualifier = "__index__";
          rebound.lhs.name = std::to_string(resolve_having_column(pred));
          return rebound;
        });
        logical.steps.push_back(having);
        current = current.filterPredicate(toPlanPredicateExpr(having.predicate_expr));
      }
    }

    std::vector<std::size_t> finalIndices;
    std::vector<std::string> finalAliases;
    for (const auto& item : query.select_items) {
      if (item.is_all) {
        throwUnsupportedSqlV1("SELECT * is not supported when aggregate is used");
      }
      if (item.is_table_all) {
        throwUnsupportedSqlV1("table.* is not supported when aggregate is used");
      }
      if (item.is_aggregate) {
        const auto outName =
            defaultAggregateAlias(item.aggregate.function, item.aggregate.argument.name, item.alias);
        finalIndices.push_back(aggSchema.indexOf(outName));
        finalAliases.push_back(item.alias.empty() ? outName : item.alias);
      } else {
        const auto idx = aggSchema.indexOf(item.column.name);
        finalIndices.push_back(idx);
        finalAliases.push_back(item.alias.empty() ? aggSchema.fields[idx] : item.alias);
      }
    }
    LogicalPlanStep project;
    project.kind = LogicalStepKind::Project;
    project.project_indices = finalIndices;
    project.project_aliases = finalAliases;
    logical.steps.push_back(project);
    current = current.selectByIndices(finalIndices, finalAliases);
  } else {
    if (!query.group_by.empty()) {
      throw SQLSemanticError("GROUP BY used without aggregate");
    }
    if (query.having) {
      throw SQLSemanticError("HAVING used without aggregate");
    }

    bool isSelectAll = false;
    for (const auto& item : query.select_items) {
      if (item.is_all) {
        isSelectAll = true;
        break;
      }
    }

    std::vector<std::size_t> indices;
    std::vector<std::string> aliases;
    if (isSelectAll) {
      const auto& schema = current.schema();
      for (std::size_t i = 0; i < schema.fields.size(); ++i) {
        indices.push_back(i);
      }
    } else {
      std::size_t projected_width = current.schema().fields.size();
      std::size_t expr_index = 0;
      for (const auto& item : query.select_items) {
      if (item.is_string_function) {
          auto args = resolveStringFunctionArgs(item.string_function, ctx);
          const auto function = toComputedFunctionKind(item.string_function.function);
          const auto outName =
              item.alias.empty() ? defaultStringAlias(item.string_function, expr_index++) : item.alias;
          LogicalPlanStep withColumnStep;
          withColumnStep.kind = LogicalStepKind::WithColumn;
          withColumnStep.with_column_name = outName;
          withColumnStep.with_function = function;
          withColumnStep.with_args = args;
          logical.steps.push_back(withColumnStep);
          current = current.withColumn(outName, function, args);
          indices.push_back(projected_width);
          ++projected_width;
          aliases.push_back(item.alias.empty() ? outName : item.alias);
          continue;
        }
        if (item.is_table_all) {
          auto cols = ctx.resolveAllFromRelation(item.table_name_or_alias);
          const auto& schema = current.schema();
          for (auto idx : cols) {
            indices.push_back(idx);
            aliases.push_back(item.table_name_or_alias + "." + schema.fields[idx]);
          }
          continue;
        }
        const auto idx = ctx.resolveColumn(item.column, nullptr);
        indices.push_back(idx);
        aliases.push_back(item.alias.empty() ? current.schema().fields[idx] : item.alias);
      }
    }

    LogicalPlanStep project;
    project.kind = LogicalStepKind::Project;
    project.project_indices = indices;
    project.project_aliases = aliases;
    logical.steps.push_back(project);
    current = current.selectByIndices(indices, aliases);
  }

  if (!query.order_by.empty()) {
    LogicalPlanStep order;
    order.kind = LogicalStepKind::OrderBy;
    order.order_indices = resolveOrderColumns(query, current.schema());
    order.order_ascending = resolveOrderDirections(query);
    logical.steps.push_back(order);
    std::vector<std::string> order_columns;
    order_columns.reserve(order.order_indices.size());
    for (auto index : order.order_indices) {
      order_columns.push_back(current.schema().fields[index]);
    }
    current = current.orderBy(order_columns, order.order_ascending);
  }

  if (query.limit.has_value()) {
    LogicalPlanStep limit;
    limit.kind = LogicalStepKind::Limit;
    limit.limit = *query.limit;
    limit.limit_set = true;
    logical.steps.push_back(limit);
    current = current.limit(*query.limit);
  }

  for (const auto& union_term : query.union_terms) {
    if (!union_term.query) {
      throw SQLSemanticError("UNION branch is missing query");
    }
    auto right = plan(*union_term.query, catalog);
    validateUnionCompatibility(current.schema(), right.schema());
    LogicalPlanStep union_step;
    union_step.kind = LogicalStepKind::Union;
    union_step.union_right = right;
    union_step.union_distinct = !union_term.all;
    logical.steps.push_back(union_step);
    current = current.unionWith(right, union_step.union_distinct);
  }

  return optimizeLogical(logical);
}

PhysicalPlan SqlPlanner::buildPhysicalPlan(const LogicalPlan& logical) const {
  PhysicalPlan physical;
  physical.seed = logical.seed;
  for (const auto& step : logical.steps) {
    PhysicalPlanStep out;
    out.logical = step;
    out.logical_kind = step.kind;
    out.physical_kind = PhysicalStepKind::SourceOnly;
    out.pipeline_barrier = true;
    physical.steps.push_back(out);
  }
  return optimizePhysical(physical);
}

std::string SqlPlanner::explainLogicalPlan(const LogicalPlan& logical) const {
  std::ostringstream out;
  out << "seed_columns=" << logical.seed.schema().fields.size() << "\n";
  for (std::size_t i = 0; i < logical.steps.size(); ++i) {
    const auto& step = logical.steps[i];
    out << i + 1 << ". ";
    switch (step.kind) {
      case LogicalStepKind::Scan:
        out << "Scan source=" << step.source_name;
        break;
      case LogicalStepKind::Filter:
        out << "Filter column=" << step.filter_column << " op=" << step.filter_op;
        break;
      case LogicalStepKind::PredicateFilter:
        out << "PredicateFilter";
        break;
      case LogicalStepKind::KeywordSearch:
        out << "KeywordSearch columns=" << step.keyword_columns.size()
            << " top_k=" << step.keyword_top_k;
        break;
      case LogicalStepKind::HybridSearch:
        out << "HybridSearch column=" << step.hybrid_vector_column
            << " top_k=" << step.hybrid_options.top_k;
        break;
      case LogicalStepKind::Union:
        out << "Union distinct=" << (step.union_distinct ? "true" : "false");
        break;
      case LogicalStepKind::Join:
        out << "Join left=" << step.join_left_column << " right=" << step.join_right_column;
        break;
      case LogicalStepKind::Aggregate:
        out << "Aggregate keys=" << step.group_keys.size() << " aggs=" << step.aggregates.size();
        break;
      case LogicalStepKind::Having:
        out << "Having column=" << step.having_column << " op=" << step.filter_op;
        break;
      case LogicalStepKind::WithColumn:
        out << "WithColumn output=" << step.with_column_name;
        break;
      case LogicalStepKind::Project:
        out << "Project columns=" << step.project_indices.size();
        break;
      case LogicalStepKind::OrderBy:
        out << "OrderBy columns=" << step.order_indices.size();
        break;
      case LogicalStepKind::Limit:
        out << "Limit value=" << step.limit;
        break;
    }
    out << "\n";
  }
  return out.str();
}

std::string SqlPlanner::explainPhysicalPlan(const PhysicalPlan& physical) const {
  std::ostringstream out;
  for (std::size_t i = 0; i < physical.steps.size(); ++i) {
    const auto& step = physical.steps[i];
    out << i + 1 << ". ";
    switch (step.physical_kind) {
      case PhysicalStepKind::SourceOnly:
        out << "SourceOnly";
        break;
      case PhysicalStepKind::FusedUnary:
        out << "FusedUnary";
        break;
      case PhysicalStepKind::Join:
        out << "Join";
        break;
      case PhysicalStepKind::Aggregate:
        out << "Aggregate";
        break;
    }
    out << " barrier=" << (step.pipeline_barrier ? "true" : "false");
    if (!step.reason.empty()) {
      out << " reason=" << step.reason;
    }
    out << "\n";
  }
  return out.str();
}

DataFrame SqlPlanner::materializeFromPhysical(const PhysicalPlan& physical) const {
  DataFrame current = physical.seed;
  for (const auto& step : physical.steps) {
    switch (step.logical_kind) {
      case LogicalStepKind::Scan:
        break;
      case LogicalStepKind::Filter:
        current = current.filterByIndex(step.logical.filter_column, step.logical.filter_op,
                                       step.logical.filter_value);
        break;
      case LogicalStepKind::PredicateFilter:
        current = current.filterPredicate(toPlanPredicateExpr(step.logical.predicate_expr));
        break;
      case LogicalStepKind::KeywordSearch:
        current = current.keywordSearch(step.logical.keyword_columns,
                                        step.logical.keyword_query_text,
                                        step.logical.keyword_top_k);
        break;
      case LogicalStepKind::HybridSearch:
        current = current.hybridSearch(step.logical.hybrid_vector_column,
                                       step.logical.hybrid_query_vector,
                                       step.logical.hybrid_options);
        break;
      case LogicalStepKind::Union:
        current = current.unionWith(step.logical.union_right, step.logical.union_distinct);
        break;
      case LogicalStepKind::Join:
        current =
            current.join(step.logical.join_right, step.logical.join_left_column,
                         step.logical.join_right_column, step.logical.join_kind);
        break;
      case LogicalStepKind::Aggregate:
        current = current.aggregate(step.logical.group_keys, step.logical.aggregates);
        break;
      case LogicalStepKind::Having:
        {
          const auto idx = current.schema().indexOf(step.logical.having_column);
          current = current.filterByIndex(idx, step.logical.filter_op, step.logical.filter_value);
        }
        break;
      case LogicalStepKind::Project:
        current = current.selectByIndices(step.logical.project_indices,
                                          step.logical.project_aliases);
        break;
      case LogicalStepKind::OrderBy:
        {
          std::vector<std::string> columns;
          columns.reserve(step.logical.order_indices.size());
          for (auto index : step.logical.order_indices) {
            columns.push_back(current.schema().fields[index]);
          }
          current = current.orderBy(columns, step.logical.order_ascending);
        }
        break;
      case LogicalStepKind::WithColumn:
        current =
            current.withColumn(step.logical.with_column_name, step.logical.with_function,
                              step.logical.with_args);
        break;
      case LogicalStepKind::Limit:
        if (step.logical.limit_set) {
          current = current.limit(step.logical.limit);
        }
        break;
      default:
        break;
    }
  }
  return current;
}

DataFrame SqlPlanner::plan(const SqlQuery& query, const ViewCatalog& catalog) const {
  const auto logical = buildLogicalPlan(query, catalog);
  const auto physical = buildPhysicalPlan(logical);
  return materializeFromPhysical(physical);
}

StreamLogicalPlan SqlPlanner::buildStreamLogicalPlan(const SqlQuery& query,
                                                     const std::string& sink_name) const {
  ensureSingleTableStreamQuery(query);
  if (hasMixedSelectStar(query)) {
    throwUnsupportedSqlV1("stream SQL SELECT * cannot be mixed with other projections");
  }

  StreamLogicalPlan logical;
  logical.source_name = query.from.name;
  logical.sink_name = sink_name;
  logical.writes_to_sink = !sink_name.empty();
  std::vector<std::string> final_output_columns;
  bool has_final_output_columns = false;

  StreamPlanNode scan;
  scan.kind = StreamPlanNodeKind::Scan;
  scan.source_name = query.from.name;
  logical.nodes.push_back(scan);

  if (query.where) {
    if (!predicateExprIsSimpleComparison(query.where)) {
      throwUnsupportedSqlV1("stream SQL WHERE does not support AND/OR");
    }
    const auto& predicate = query.where->predicate;
    if (predicate.lhs_is_aggregate) {
      throw SQLSemanticError("WHERE does not support aggregate expressions");
    }
    StreamPlanNode filter;
    filter.kind = StreamPlanNodeKind::Filter;
    filter.column = resolveStreamColumnName(predicate.lhs, query.from);
    filter.op = opToString(predicate.op);
    filter.value = predicate.rhs;
    logical.nodes.push_back(filter);
  }

  if (query.window.has_value()) {
    StreamPlanNode window;
    window.kind = StreamPlanNodeKind::WindowAssign;
    window.column = resolveStreamColumnName(query.window->time_column, query.from);
    window.window_ms = query.window->every_ms;
    window.output_column = query.window->output_column;
    logical.nodes.push_back(window);
  }

  if (isAggregateQuery(query)) {
    if (query.having && query.group_by.empty()) {
      throw SQLSemanticError("GROUP BY required for HAVING");
    }

    std::vector<std::string> group_keys;
    group_keys.reserve(query.group_by.size());
    for (const auto& key : query.group_by) {
      group_keys.push_back(resolveStreamColumnName(key, query.from));
    }

    std::vector<StreamAggregateSpec> aggregates;
    for (const auto& item : query.select_items) {
      if (item.is_string_function) {
        throwUnsupportedSqlV1("stream SQL does not support string functions in aggregate query");
      }
      if (item.is_all || item.is_table_all) {
        throwUnsupportedSqlV1("stream SQL aggregate query does not support star projection");
      }
      if (item.is_aggregate) {
        const auto aggregate = toStreamAggregateSpec(item, query.from);
        if (std::any_of(aggregates.begin(), aggregates.end(),
                        [&](const StreamAggregateSpec& spec) {
                          return spec.output_column == aggregate.output_column;
                        })) {
          throw SQLSemanticError("duplicate aggregate alias: " + aggregate.output_column);
        }
        aggregates.push_back(aggregate);
        continue;
      }
      const auto column = resolveStreamColumnName(item.column, query.from);
      if (std::find(group_keys.begin(), group_keys.end(), column) == group_keys.end()) {
        throw SQLSemanticError("non-aggregate field must appear in GROUP BY: " + column);
      }
    }
    if (aggregates.empty()) {
      throw SQLSemanticError("stream SQL aggregate query requires at least one aggregate output");
    }

    StreamPlanNode aggregate;
    aggregate.kind = StreamPlanNodeKind::Aggregate;
    aggregate.group_keys = group_keys;
    aggregate.stateful = true;
    aggregate.aggregates = aggregates;
    logical.nodes.push_back(aggregate);

    if (query.having) {
      if (!predicateExprIsSimpleComparison(query.having)) {
        throwUnsupportedSqlV1("stream SQL HAVING does not support AND/OR");
      }
      const auto& predicate = query.having->predicate;
      StreamPlanNode filter;
      filter.kind = StreamPlanNodeKind::Filter;
      if (predicate.lhs_is_aggregate) {
        const auto target_output =
            defaultAggregateAlias(predicate.lhs_aggregate.function,
                                  predicate.lhs_aggregate.argument.name, "");
        auto it = std::find_if(aggregates.begin(), aggregates.end(),
                               [&](const StreamAggregateSpec& spec) {
                                 if (spec.function != toPlanFunction(predicate.lhs_aggregate.function)) {
                                   return false;
                                 }
                                 if (spec.is_count_star != predicate.lhs_aggregate.count_all) {
                                   return false;
                                 }
                                 if (!spec.is_count_star &&
                                     spec.value_column !=
                                         resolveStreamColumnName(predicate.lhs_aggregate.argument, query.from)) {
                                   return false;
                                 }
                                 return true;
                               });
        if (it == aggregates.end()) {
          auto alias_match = std::find_if(aggregates.begin(), aggregates.end(),
                                          [&](const StreamAggregateSpec& spec) {
                                            return spec.output_column == target_output;
                                          });
          if (alias_match == aggregates.end()) {
            throw SQLSemanticError("HAVING aggregate not found in SELECT: " + target_output);
          }
          filter.column = alias_match->output_column;
        } else {
          filter.column = it->output_column;
        }
      } else {
        auto alias_match = std::find_if(aggregates.begin(), aggregates.end(),
                                        [&](const StreamAggregateSpec& spec) {
                                          return spec.output_column == predicate.lhs.name;
                                        });
        filter.column = alias_match != aggregates.end()
                            ? alias_match->output_column
                            : resolveStreamColumnName(predicate.lhs, query.from);
      }
      filter.op = opToString(predicate.op);
      filter.value = predicate.rhs;
      logical.nodes.push_back(filter);
    }

    StreamPlanNode project;
    project.kind = StreamPlanNodeKind::Project;
    std::vector<std::pair<std::string, std::string>> aliases;
    for (const auto& item : query.select_items) {
      if (item.is_aggregate) {
        project.columns.push_back(aggregateOutputName(item));
      } else {
        const auto column = resolveStreamColumnName(item.column, query.from);
        if (!item.alias.empty() && item.alias != column) {
          aliases.push_back({item.alias, column});
          project.columns.push_back(item.alias);
        } else {
          project.columns.push_back(column);
        }
      }
    }
    std::vector<std::string> natural_columns = group_keys;
    for (const auto& agg : aggregates) {
      natural_columns.push_back(agg.output_column);
    }
    for (const auto& alias : aliases) {
      StreamPlanNode with_column;
      with_column.kind = StreamPlanNodeKind::WithColumn;
      with_column.output_column = alias.first;
      with_column.value_column = alias.second;
      logical.nodes.push_back(with_column);
    }
    if (!aliases.empty() || project.columns != natural_columns) {
      logical.nodes.push_back(project);
    }
    final_output_columns = project.columns != natural_columns ? project.columns : natural_columns;
    has_final_output_columns = true;
  } else {
    if (!query.group_by.empty()) {
      throw SQLSemanticError("GROUP BY used without aggregate");
    }
    if (query.having) {
      throw SQLSemanticError("HAVING used without aggregate");
    }

    bool select_all = false;
    StreamPlanNode project;
    project.kind = StreamPlanNodeKind::Project;
    std::size_t expr_index = 0;
    for (const auto& item : query.select_items) {
      if (item.is_all) {
        if (query.select_items.size() != 1) {
          throwUnsupportedSqlV1("stream SQL SELECT * cannot be mixed with other projections");
        }
        select_all = true;
        break;
      }
      if (item.is_string_function) {
        auto args = resolveStreamStringFunctionArgs(item.string_function, query.from);
        const auto function = toComputedFunctionKind(item.string_function.function);
        const auto outName =
            item.alias.empty() ? defaultStringAlias(item.string_function, expr_index++) : item.alias;
        StreamPlanNode with_column;
        with_column.kind = StreamPlanNodeKind::WithColumn;
        with_column.output_column = outName;
        with_column.with_function = function;
        with_column.with_args = std::move(args);
        logical.nodes.push_back(with_column);
        project.columns.push_back(outName);
        continue;
      }
      if (item.is_table_all) {
        if (query.select_items.size() != 1) {
          throwUnsupportedSqlV1("stream SQL table.* cannot be mixed with other projections");
        }
        if (item.table_name_or_alias != query.from.name &&
            (query.from.alias.empty() || item.table_name_or_alias != query.from.alias)) {
          throw SQLSemanticError("stream SQL table.* does not match source: " +
                                 item.table_name_or_alias);
        }
        select_all = true;
        break;
      }
      const auto column = resolveStreamColumnName(item.column, query.from);
      if (!item.alias.empty() && item.alias != column) {
        project.aliases.push_back({item.alias, column});
        project.columns.push_back(item.alias);
      } else {
        project.columns.push_back(column);
      }
    }
    if (!select_all) {
      for (const auto& alias : project.aliases) {
        StreamPlanNode with_column;
        with_column.kind = StreamPlanNodeKind::WithColumn;
        with_column.output_column = alias.first;
        with_column.value_column = alias.second;
        logical.nodes.push_back(with_column);
      }
      logical.nodes.push_back(project);
      final_output_columns = project.columns;
      has_final_output_columns = true;
    }
  }

  if (query.limit.has_value()) {
    StreamPlanNode limit;
    limit.kind = StreamPlanNodeKind::Limit;
    limit.limit = *query.limit;
    logical.nodes.push_back(limit);
  }

  if (!query.order_by.empty()) {
    StreamPlanNode order;
    order.kind = StreamPlanNodeKind::OrderBy;
    order.order_columns = has_final_output_columns
                              ? resolveStreamOrderColumns(query, final_output_columns)
                              : resolveStreamOrderColumns(query, query.from);
    order.order_ascending = resolveOrderDirections(query);
    const auto limit_node = query.limit.has_value() ? logical.nodes.back() : StreamPlanNode{};
    if (query.limit.has_value()) {
      logical.nodes.pop_back();
    }
    logical.nodes.push_back(order);
    if (query.limit.has_value()) {
      logical.nodes.push_back(limit_node);
    }
  }

  return logical;
}

StreamPhysicalPlan SqlPlanner::buildStreamPhysicalPlan(const StreamLogicalPlan& logical) const {
  StreamPhysicalPlan physical;
  physical.logical = logical;
  physical.partition_local_prefix_nodes = logical.nodes.size();

  for (std::size_t i = 0; i < logical.nodes.size(); ++i) {
    const auto& node = logical.nodes[i];
    if (node.kind == StreamPlanNodeKind::WindowAssign) {
      physical.has_window = true;
    }
    if (node.kind == StreamPlanNodeKind::Aggregate && node.stateful) {
      physical.has_stateful_ops = true;
    }
    if (!streamPlanIsPartitionLocal(node)) {
      physical.partition_local_prefix_nodes = i;
      break;
    }
  }

  for (std::size_t i = 0; i < logical.nodes.size(); ++i) {
    const auto& node = logical.nodes[i];
    if (!isActorHotPathAggregate(node)) {
      continue;
    }
    if (i + 1 != logical.nodes.size()) {
      physical.actor_eligibility_reason =
          "actor acceleration requires the eligible grouped aggregate to be the final stream transform";
      return physical;
    }
    physical.actor_eligible = true;
    physical.actor_eligibility_reason =
        "final stateful grouped aggregate with supported SUM/COUNT/MIN/MAX/AVG outputs is eligible for actor acceleration";
    return physical;
  }

  physical.actor_eligibility_reason = "query plan is not eligible for actor acceleration";
  return physical;
}

std::string SqlPlanner::explainStreamLogicalPlan(const StreamLogicalPlan& logical) const {
  std::ostringstream out;
  out << "source=" << logical.source_name << "\n";
  if (logical.writes_to_sink) {
    out << "sink=" << logical.sink_name << "\n";
  }
  for (std::size_t i = 0; i < logical.nodes.size(); ++i) {
    const auto& node = logical.nodes[i];
    out << i + 1 << ". " << streamNodeKindName(node.kind);
    if (node.kind == StreamPlanNodeKind::Scan) {
      out << " source=" << node.source_name;
    } else if (node.kind == StreamPlanNodeKind::Filter) {
      out << " column=" << node.column << " op=" << node.op;
    } else if (node.kind == StreamPlanNodeKind::Project) {
      out << " columns=[" << joinStrings(node.columns, ", ") << "]";
    } else if (node.kind == StreamPlanNodeKind::WithColumn) {
      out << " output=" << node.output_column;
      if (node.with_function == ComputedColumnKind::Copy) {
        out << " from=" << node.value_column;
      } else {
        out << " function=" << computedFunctionName(node.with_function) << " args=[";
        for (std::size_t i = 0; i < node.with_args.size(); ++i) {
          if (i > 0) out << ", ";
          out << computedArgToString(node.with_args[i]);
        }
        out << "]";
      }
    } else if (node.kind == StreamPlanNodeKind::OrderBy) {
      out << " columns=[";
      for (std::size_t i = 0; i < node.order_columns.size(); ++i) {
        if (i > 0) out << ", ";
        out << node.order_columns[i] << (i < node.order_ascending.size() && !node.order_ascending[i] ? " DESC" : " ASC");
      }
      out << "]";
    } else if (node.kind == StreamPlanNodeKind::Limit) {
      out << " limit=" << node.limit;
    } else if (node.kind == StreamPlanNodeKind::WindowAssign) {
      out << " time_column=" << node.column << " window_ms=" << node.window_ms
          << " output=" << node.output_column;
    } else if (node.kind == StreamPlanNodeKind::Aggregate) {
      out << " keys=[" << joinStrings(node.group_keys, ", ") << "]"
          << " aggs=[";
      for (size_t j = 0; j < node.aggregates.size(); ++j) {
        if (j > 0) out << ", ";
        out << streamAggregateExplain(node.aggregates[j]);
      }
      out << "]"
          << " stateful=" << (node.stateful ? "true" : "false");
    } else if (node.kind == StreamPlanNodeKind::Sink) {
      out << " sink=" << node.sink_name;
    }
    out << "\n";
  }
  return out.str();
}

std::string SqlPlanner::explainStreamPhysicalPlan(const StreamPhysicalPlan& physical) const {
  std::ostringstream out;
  out << "writes_to_sink=" << (physical.logical.writes_to_sink ? "true" : "false") << "\n";
  if (physical.logical.writes_to_sink) {
    out << "sink=" << physical.logical.sink_name << "\n";
  }
  out << "has_window=" << (physical.has_window ? "true" : "false") << "\n";
  out << "has_stateful_ops=" << (physical.has_stateful_ops ? "true" : "false") << "\n";
  out << "partition_local_prefix_nodes=" << physical.partition_local_prefix_nodes << "\n";
  out << "actor_eligible=" << (physical.actor_eligible ? "true" : "false") << "\n";
  out << "actor_eligibility_reason=" << physical.actor_eligibility_reason << "\n";
  for (std::size_t i = 0; i < physical.logical.nodes.size(); ++i) {
    const auto& node = physical.logical.nodes[i];
    out << i + 1 << ". " << streamNodeKindName(node.kind)
        << " partition_local=" << (streamPlanIsPartitionLocal(node) ? "true" : "false");
    if (node.kind == StreamPlanNodeKind::Aggregate) {
      out << " stateful=" << (node.stateful ? "true" : "false");
    }
    out << "\n";
  }
  return out.str();
}

StreamingDataFrame SqlPlanner::materializeStreamFromPhysical(
    const StreamPhysicalPlan& physical,
    const std::unordered_map<std::string, StreamingDataFrame>& stream_views) const {
  auto it = stream_views.find(physical.logical.source_name);
  if (it == stream_views.end()) {
    throw CatalogNotFoundError("stream view not found: " + physical.logical.source_name);
  }

  StreamingDataFrame current = it->second;
  for (const auto& node : physical.logical.nodes) {
    switch (node.kind) {
      case StreamPlanNodeKind::Scan:
        break;
      case StreamPlanNodeKind::Filter:
        current = current.filter(node.column, node.op, node.value);
        break;
      case StreamPlanNodeKind::Project:
        current = current.select(node.columns);
        break;
      case StreamPlanNodeKind::WithColumn:
        if (node.with_function == ComputedColumnKind::Copy) {
          current = current.withColumn(node.output_column, node.value_column);
        } else {
          current = current.withColumn(node.output_column, node.with_function, node.with_args);
        }
        break;
      case StreamPlanNodeKind::OrderBy:
        current = current.orderBy(node.order_columns, node.order_ascending);
        break;
      case StreamPlanNodeKind::Limit:
        current = current.limit(node.limit);
        break;
      case StreamPlanNodeKind::WindowAssign:
        current = current.window(node.column, node.window_ms, node.output_column);
        break;
      case StreamPlanNodeKind::Aggregate:
        current = current.groupBy(node.group_keys).aggregate(node.aggregates, node.stateful);
        break;
      case StreamPlanNodeKind::Sink:
        break;
    }
  }
  return current;
}

StreamingDataFrame SqlPlanner::planStream(
    const SqlQuery& query,
    const std::unordered_map<std::string, StreamingDataFrame>& stream_views) const {
  const auto logical = buildStreamLogicalPlan(query);
  const auto physical = buildStreamPhysicalPlan(logical);
  return materializeStreamFromPhysical(physical, stream_views);
}

}  // namespace sql
}  // namespace dataflow
