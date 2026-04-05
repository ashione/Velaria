#include "src/dataflow/core/execution/runtime/execution_optimizer.h"

#include <algorithm>

namespace dataflow {

FilterChainPattern analyzeFilterChain(const PlanNodePtr& plan) {
  FilterChainPattern pattern;
  PlanNodePtr current = plan;
  while (current && current->kind == PlanKind::Filter) {
    const auto* node = static_cast<const FilterPlan*>(current.get());
    pattern.filters.push_back(node);
    current = node->child;
  }
  pattern.base_child = std::move(current);
  std::reverse(pattern.filters.begin(), pattern.filters.end());
  return pattern;
}

LimitExecutionPattern analyzeLimitExecution(const LimitPlan& plan) {
  LimitExecutionPattern pattern;
  if (!plan.child) {
    return pattern;
  }
  if (plan.child->kind == PlanKind::Filter) {
    pattern.use_filter_chain = true;
    pattern.filter_chain = analyzeFilterChain(plan.child);
    return pattern;
  }
  if (plan.child->kind == PlanKind::OrderBy) {
    pattern.use_topn = true;
    pattern.order_by = static_cast<const OrderByPlan*>(plan.child.get());
  }
  return pattern;
}

AggregateExecutionShape analyzeAggregateExecutionShape(
    const std::vector<std::size_t>& key_indices,
    const std::vector<AggregateSpec>& aggregates) {
  if (aggregates.size() != 1 || aggregates.front().function != AggregateFunction::Sum) {
    return AggregateExecutionShape::Generic;
  }
  if (key_indices.empty()) {
    return AggregateExecutionShape::SumNoKey;
  }
  if (key_indices.size() == 1) {
    return AggregateExecutionShape::SumSingleInt64Key;
  }
  if (key_indices.size() == 2) {
    return AggregateExecutionShape::SumDoubleInt64Key;
  }
  return AggregateExecutionShape::Generic;
}

}  // namespace dataflow
