#include "src/dataflow/core/execution/runtime/execution_optimizer.h"

#include <algorithm>

#include "src/dataflow/core/execution/columnar_batch.h"

namespace dataflow {

namespace {

enum class KeyColumnShape {
  Unknown = 0,
  String = 1,
  Int64 = 2,
};

KeyColumnShape analyzeKeyColumnShape(const ValueColumnBuffer& column) {
  if (column.arrow_backing != nullptr) {
    const auto format = column.arrow_backing->format;
    if (format == "u" || format == "U") {
      return KeyColumnShape::String;
    }
    if (format == "b" || format == "i" || format == "I" || format == "l" || format == "L") {
      return KeyColumnShape::Int64;
    }
    return KeyColumnShape::Unknown;
  }
  for (const auto& value : column.values) {
    if (value.isNull()) {
      continue;
    }
    if (value.type() == DataType::String) {
      return KeyColumnShape::String;
    }
    if (value.type() == DataType::Int64) {
      return KeyColumnShape::Int64;
    }
    return KeyColumnShape::Unknown;
  }
  return KeyColumnShape::Unknown;
}

}  // namespace

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

AggregateExecutionPattern analyzeAggregateExecution(
    const Table& input,
    const std::vector<std::size_t>& key_indices,
    const std::vector<AggregateSpec>& aggregates) {
  AggregateExecutionPattern pattern;
  const bool single_sum =
      aggregates.size() == 1 && aggregates.front().function == AggregateFunction::Sum;

  if (key_indices.empty()) {
    pattern.shape =
        single_sum ? AggregateExecutionShape::SumNoKey : AggregateExecutionShape::GenericNoKey;
    return pattern;
  }

  if (key_indices.size() == 1) {
    const auto key_column = viewValueColumn(input, key_indices.front());
    switch (analyzeKeyColumnShape(*key_column.buffer)) {
      case KeyColumnShape::String:
        pattern.shape = AggregateExecutionShape::GenericSingleStringKey;
        return pattern;
      case KeyColumnShape::Int64:
        pattern.shape =
            single_sum ? AggregateExecutionShape::SumSingleInt64Key
                       : AggregateExecutionShape::GenericSingleInt64Key;
        return pattern;
      case KeyColumnShape::Unknown:
        break;
    }
  }

  if (key_indices.size() == 2) {
    const auto first_key = viewValueColumn(input, key_indices[0]);
    const auto second_key = viewValueColumn(input, key_indices[1]);
    if (analyzeKeyColumnShape(*first_key.buffer) == KeyColumnShape::Int64 &&
        analyzeKeyColumnShape(*second_key.buffer) == KeyColumnShape::Int64) {
      pattern.shape =
          single_sum ? AggregateExecutionShape::SumDoubleInt64Key
                     : AggregateExecutionShape::GenericDoubleInt64Key;
      return pattern;
    }
  }

  pattern.shape = AggregateExecutionShape::GenericSerializedKeys;
  return pattern;
}

}  // namespace dataflow
