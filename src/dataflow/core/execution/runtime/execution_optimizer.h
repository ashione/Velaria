#pragma once

#include <vector>

#include "src/dataflow/core/logical/planner/plan.h"

namespace dataflow {

struct FilterChainPattern {
  PlanNodePtr base_child;
  std::vector<const FilterPlan*> filters;
};

enum class AggregateExecutionShape {
  Generic = 0,
  SumNoKey = 1,
  SumSingleInt64Key = 2,
  SumDoubleInt64Key = 3,
};

struct LimitExecutionPattern {
  bool use_filter_chain = false;
  FilterChainPattern filter_chain;
  bool use_topn = false;
  const OrderByPlan* order_by = nullptr;
};

FilterChainPattern analyzeFilterChain(const PlanNodePtr& plan);
LimitExecutionPattern analyzeLimitExecution(const LimitPlan& plan);
AggregateExecutionShape analyzeAggregateExecutionShape(
    const std::vector<std::size_t>& key_indices,
    const std::vector<AggregateSpec>& aggregates);

}  // namespace dataflow
