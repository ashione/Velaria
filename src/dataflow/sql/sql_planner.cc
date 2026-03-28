#include "src/dataflow/sql/sql_planner.h"

#include <algorithm>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace dataflow {
namespace sql {

namespace {

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
  }
  return AggregateFunction::Sum;
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
  return kind == LogicalStepKind::Filter || kind == LogicalStepKind::Project ||
         kind == LogicalStepKind::Limit || kind == LogicalStepKind::Having;
}

bool isBarrierStep(LogicalStepKind kind) {
  return kind == LogicalStepKind::Scan || kind == LogicalStepKind::Join ||
         kind == LogicalStepKind::Aggregate;
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

}  // namespace

LogicalPlan SqlPlanner::buildLogicalPlan(const SqlQuery& query, const ViewCatalog& catalog) const {
  LogicalPlan logical;

  if (!query.has_from) {
    if (query.join.has_value() || query.where.has_value() || !query.group_by.empty() ||
        query.having.has_value()) {
      throw SQLSemanticError("SELECT without FROM only supports literal projection");
    }
    std::vector<std::string> fields;
    Row row;
    fields.reserve(query.select_items.size());
    row.reserve(query.select_items.size());
    for (std::size_t i = 0; i < query.select_items.size(); ++i) {
      const auto& item = query.select_items[i];
      if (!item.is_literal || item.is_all || item.is_table_all || item.is_aggregate ||
          !item.column.name.empty()) {
        throw SQLSemanticError("SELECT without FROM only supports literal projection");
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
    throw SQLSemanticError("SELECT from SINK table is not allowed: " + query.from.name);
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
      throw SQLSemanticError("JOIN with SINK table is not allowed: " + rightView.name);
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

  if (query.where.has_value()) {
    if (query.where->lhs_is_aggregate) {
      throw SQLSemanticError("WHERE does not support aggregate expressions");
    }
    std::size_t idx = ctx.resolveColumn(query.where->lhs, nullptr);
    LogicalPlanStep filter;
    filter.kind = LogicalStepKind::Filter;
    filter.filter_column = idx;
    filter.filter_op = opToString(query.where->op);
    filter.filter_value = query.where->rhs;
    logical.steps.push_back(filter);
    current = current.filterByIndex(idx, filter.filter_op, filter.filter_value);
  }

  const auto whereFilteredSchema = current.schema();
  const bool isAggregateQuery =
      !query.group_by.empty() ||
      std::any_of(query.select_items.begin(), query.select_items.end(),
                  [](const SelectItem& item) { return item.is_aggregate; });

  if (std::any_of(query.select_items.begin(), query.select_items.end(),
                  [](const SelectItem& item) { return item.is_literal; })) {
    throw SQLSemanticError("literal projection with FROM is not supported in SQL v1");
  }

  if (isAggregateQuery) {
    if (query.having.has_value() && query.group_by.empty()) {
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
    if (query.having.has_value()) {
      const auto& pred = query.having.value();
      std::string havingColumn;
      if (pred.lhs_is_aggregate) {
        std::string default_name =
            defaultAggregateAlias(pred.lhs_aggregate.function, pred.lhs_aggregate.argument.name, "");
        if (aggSchema.has(default_name)) {
          havingColumn = default_name;
        } else {
          bool matched = false;
          for (const auto& item : query.select_items) {
            if (!item.is_aggregate) {
              continue;
            }
            const auto& agg = item.aggregate;
            if (agg.function != pred.lhs_aggregate.function) {
              continue;
            }
            if (agg.count_all != pred.lhs_aggregate.count_all) {
              continue;
            }
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
      LogicalPlanStep having;
      having.kind = LogicalStepKind::Having;
      having.having_column = havingColumn;
      having.filter_op = opToString(pred.op);
      having.filter_value = pred.rhs;
      logical.steps.push_back(having);
      const auto havingIndex = aggSchema.indexOf(havingColumn);
      current = current.filterByIndex(havingIndex, having.filter_op, having.filter_value);
    }

    std::vector<std::size_t> finalIndices;
    std::vector<std::string> finalAliases;
    for (const auto& item : query.select_items) {
      if (item.is_all) {
        throw SQLSemanticError("SELECT * is not supported when aggregate is used");
      }
      if (item.is_table_all) {
        throw SQLSemanticError("table.* is not supported when aggregate is used");
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
    if (query.having.has_value()) {
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
      for (const auto& item : query.select_items) {
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

  if (query.limit.has_value()) {
    LogicalPlanStep limit;
    limit.kind = LogicalStepKind::Limit;
    limit.limit = *query.limit;
    limit.limit_set = true;
    logical.steps.push_back(limit);
    current = current.limit(*query.limit);
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

}  // namespace sql
}  // namespace dataflow
