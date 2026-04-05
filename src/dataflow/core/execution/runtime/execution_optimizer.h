#pragma once

#include <vector>

#include "src/dataflow/core/logical/planner/plan.h"

namespace dataflow {

struct Table;

struct FilterChainPattern {
  PlanNodePtr base_child;
  std::vector<const FilterPlan*> filters;
};

enum class AggregateExecutionShape {
  GenericSerializedKeys = 0,
  GenericNoKey = 1,
  GenericSingleStringKey = 2,
  GenericSingleInt64Key = 3,
  GenericDoubleInt64Key = 4,
  SumNoKey = 5,
  SumSingleInt64Key = 6,
  SumDoubleInt64Key = 7,
};

struct AggregateExecutionPattern {
  AggregateExecutionShape shape = AggregateExecutionShape::GenericSerializedKeys;
};

struct LimitExecutionPattern {
  bool use_filter_chain = false;
  FilterChainPattern filter_chain;
  bool use_topn = false;
  const OrderByPlan* order_by = nullptr;
};

FilterChainPattern analyzeFilterChain(const PlanNodePtr& plan);
LimitExecutionPattern analyzeLimitExecution(const LimitPlan& plan);
AggregateExecutionPattern analyzeAggregateExecution(
    const Table& input,
    const std::vector<std::size_t>& key_indices,
    const std::vector<AggregateSpec>& aggregates);

}  // namespace dataflow
