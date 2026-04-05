#include "src/dataflow/core/execution/runtime/execution_optimizer.h"

#include <algorithm>

#include "src/dataflow/core/execution/arrow_format.h"
#include "src/dataflow/core/execution/columnar_batch.h"

namespace dataflow {

namespace {

enum class KeyColumnShape {
  Unknown = 0,
  String = 1,
  Int64 = 2,
  MixedStringInt64OrNull = 3,
};

KeyColumnShape analyzeKeyColumnShape(const ValueColumnBuffer& column) {
  if (column.arrow_backing != nullptr) {
    const auto format = column.arrow_backing->format;
    if (isArrowUtf8Format(format)) {
      return KeyColumnShape::String;
    }
    if (isArrowIntegerLikeFormat(format)) {
      return KeyColumnShape::Int64;
    }
    return KeyColumnShape::Unknown;
  }

  bool saw_string = false;
  bool saw_int64 = false;
  for (const auto& value : column.values) {
    if (value.isNull()) {
      continue;
    }
    if (value.type() == DataType::String) {
      saw_string = true;
      continue;
    }
    if (value.type() == DataType::Int64) {
      saw_int64 = true;
      continue;
    }
    return KeyColumnShape::Unknown;
  }
  if (saw_string && saw_int64) {
    return KeyColumnShape::MixedStringInt64OrNull;
  }
  if (saw_string) {
    return KeyColumnShape::String;
  }
  if (saw_int64) {
    return KeyColumnShape::Int64;
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
      case KeyColumnShape::MixedStringInt64OrNull:
      case KeyColumnShape::Unknown:
        break;
    }
  }

  if (key_indices.size() == 2) {
    const auto first_key = viewValueColumn(input, key_indices[0]);
    const auto second_key = viewValueColumn(input, key_indices[1]);
    const auto first_shape = analyzeKeyColumnShape(*first_key.buffer);
    const auto second_shape = analyzeKeyColumnShape(*second_key.buffer);
    if (first_shape == KeyColumnShape::Int64 && second_shape == KeyColumnShape::Int64) {
      pattern.shape =
          single_sum ? AggregateExecutionShape::SumDoubleInt64Key
                     : AggregateExecutionShape::GenericDoubleInt64Key;
      return pattern;
    }
    // Multi-key mixed (String/Int64/Null only) fast-path.
    const bool supported =
        (first_shape == KeyColumnShape::String || first_shape == KeyColumnShape::Int64 ||
         first_shape == KeyColumnShape::MixedStringInt64OrNull) &&
        (second_shape == KeyColumnShape::String || second_shape == KeyColumnShape::Int64 ||
         second_shape == KeyColumnShape::MixedStringInt64OrNull);
    if (supported) {
      pattern.shape = AggregateExecutionShape::GenericPackedKeys2;
      return pattern;
    }
  }

  if (key_indices.size() == 3) {
    const auto k0 = viewValueColumn(input, key_indices[0]);
    const auto k1 = viewValueColumn(input, key_indices[1]);
    const auto k2 = viewValueColumn(input, key_indices[2]);
    const auto s0 = analyzeKeyColumnShape(*k0.buffer);
    const auto s1 = analyzeKeyColumnShape(*k1.buffer);
    const auto s2 = analyzeKeyColumnShape(*k2.buffer);
    const bool supported =
        (s0 == KeyColumnShape::String || s0 == KeyColumnShape::Int64 ||
         s0 == KeyColumnShape::MixedStringInt64OrNull) &&
        (s1 == KeyColumnShape::String || s1 == KeyColumnShape::Int64 ||
         s1 == KeyColumnShape::MixedStringInt64OrNull) &&
        (s2 == KeyColumnShape::String || s2 == KeyColumnShape::Int64 ||
         s2 == KeyColumnShape::MixedStringInt64OrNull);
    if (supported) {
      pattern.shape = AggregateExecutionShape::GenericPackedKeys3;
      return pattern;
    }
  }

  pattern.shape = AggregateExecutionShape::GenericSerializedKeys;
  return pattern;
}

}  // namespace dataflow
