#include "src/dataflow/core/execution/runtime/execution_optimizer.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <unordered_set>

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
    if (format == kArrowFormatBool) {
      return KeyColumnShape::Int64;
    }
    if (isArrowIntegerLikeFormat(format)) {
      return KeyColumnShape::Int64;
    }
    return KeyColumnShape::Unknown;
  }

  bool saw_string = false;
  bool saw_int64 = false;
  std::size_t sampled_non_null = 0;
  constexpr std::size_t kMaxTypeSamples = 256;
  for (const auto& value : column.values) {
    if (value.isNull()) {
      continue;
    }
    ++sampled_non_null;
    if (value.type() == DataType::String) {
      saw_string = true;
    } else if (value.type() == DataType::Int64 || value.type() == DataType::Bool) {
      saw_int64 = true;
    } else {
      return KeyColumnShape::Unknown;
    }
    if (saw_string && saw_int64) {
      return KeyColumnShape::MixedStringInt64OrNull;
    }
    if (sampled_non_null >= kMaxTypeSamples) {
      break;
    }
  }
  if (saw_string) {
    return KeyColumnShape::String;
  }
  if (saw_int64) {
    return KeyColumnShape::Int64;
  }
  return KeyColumnShape::Unknown;
}

bool columnHasNulls(const ValueColumnBuffer& column) {
  const auto row_count = valueColumnRowCount(column);
  for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
    if (valueColumnIsNullAt(column, row_index)) {
      return true;
    }
  }
  return false;
}

bool estimateLowCardinalitySingleInt64(const Table& input, std::size_t key_index,
                                       std::size_t* expected_groups) {
  const auto key_column = viewValueColumn(input, key_index);
  if (analyzeKeyColumnShape(*key_column.buffer) != KeyColumnShape::Int64) {
    return false;
  }
  const auto row_count = input.rowCount();
  if (row_count == 0) {
    if (expected_groups != nullptr) {
      *expected_groups = 0;
    }
    return false;
  }

  constexpr std::size_t kMaxDenseRowsToInspect = 65536;
  const std::size_t inspect_rows = std::min<std::size_t>(row_count, kMaxDenseRowsToInspect);
  int64_t min_value = std::numeric_limits<int64_t>::max();
  int64_t max_value = std::numeric_limits<int64_t>::min();
  std::unordered_set<int64_t> distinct;
  distinct.reserve(std::min<std::size_t>(inspect_rows, 4096));
  for (std::size_t row_index = 0; row_index < inspect_rows; ++row_index) {
    if (valueColumnIsNullAt(*key_column.buffer, row_index)) {
      continue;
    }
    const auto value = valueColumnInt64At(*key_column.buffer, row_index);
    min_value = std::min(min_value, value);
    max_value = std::max(max_value, value);
    distinct.insert(value);
  }
  if (distinct.empty()) {
    if (expected_groups != nullptr) {
      *expected_groups = 0;
    }
    return false;
  }

  const uint64_t domain_size = static_cast<uint64_t>(max_value - min_value) + 1;
  if (expected_groups != nullptr) {
    std::size_t projected_groups = distinct.size();
    if (row_count != inspect_rows) {
      const double ratio = static_cast<double>(row_count) / static_cast<double>(inspect_rows);
      projected_groups =
          static_cast<std::size_t>(static_cast<double>(distinct.size()) * ratio);
    }
    *expected_groups = std::min<std::size_t>(projected_groups, static_cast<std::size_t>(domain_size));
  }
  constexpr uint64_t kMaxDenseDomain = 1u << 16;
  const bool sample_looks_open_ended =
      row_count != inspect_rows && distinct.size() == inspect_rows &&
      domain_size >= inspect_rows;
  return domain_size <= kMaxDenseDomain &&
         domain_size <= std::max<uint64_t>(distinct.size() * 4ull, 64ull) &&
         !sample_looks_open_ended;
}

AggregatePropertySet buildAggregateProperties(const Table& input,
                                             const std::vector<std::size_t>& key_indices,
                                             bool ordered_input,
                                             bool partition_local,
                                             std::size_t* dense_groups) {
  AggregatePropertySet properties;
  properties.key_count = key_indices.size();
  properties.ordered_input = ordered_input;
  properties.partition_local = partition_local;
  properties.all_fixed_width = !key_indices.empty();
  properties.all_int64_like = !key_indices.empty();
  properties.all_string_like = !key_indices.empty();
  properties.packable = key_indices.size() >= 2 && key_indices.size() <= 3;

  for (const auto key_index : key_indices) {
    const auto key_column = viewValueColumn(input, key_index);
    properties.has_nullable_keys = properties.has_nullable_keys || columnHasNulls(*key_column.buffer);
    const auto shape = analyzeKeyColumnShape(*key_column.buffer);
    properties.all_fixed_width = properties.all_fixed_width && shape == KeyColumnShape::Int64;
    properties.all_int64_like = properties.all_int64_like && shape == KeyColumnShape::Int64;
    properties.all_string_like = properties.all_string_like && shape == KeyColumnShape::String;
    properties.packable = properties.packable &&
                          (shape == KeyColumnShape::String || shape == KeyColumnShape::Int64 ||
                           shape == KeyColumnShape::MixedStringInt64OrNull);
  }

  if (key_indices.size() == 1 && properties.all_int64_like) {
    properties.low_cardinality =
        estimateLowCardinalitySingleInt64(input, key_indices.front(), dense_groups);
  }
  return properties;
}

void appendRejectedCandidate(std::vector<std::string>* rejected, const std::string& value) {
  if (rejected == nullptr || value.empty()) {
    return;
  }
  rejected->push_back(value);
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

const char* aggregateExecKindName(AggImplKind kind) {
  switch (kind) {
    case AggImplKind::Dense:
      return "dense";
    case AggImplKind::HashPacked:
      return "hash-packed";
    case AggImplKind::HashFixed:
      return "hash-fixed";
    case AggImplKind::HashRef:
      return "hash-ref";
    case AggImplKind::SortStreaming:
      return "sort-streaming";
    case AggImplKind::LocalGlobal:
      return "local-global";
  }
  return "hash-ref";
}

const char* aggregatePartialLayoutName(AggregatePartialLayoutKind kind) {
  switch (kind) {
    case AggregatePartialLayoutKind::GenericTable:
      return "generic-table";
    case AggregatePartialLayoutKind::TwoKeyColumnar:
      return "two-key-columnar";
  }
  return "generic-table";
}

const char* aggregateExecutionShapeName(AggregateExecutionShape shape) {
  switch (shape) {
    case AggregateExecutionShape::GenericSerializedKeys:
      return "generic-serialized-keys";
    case AggregateExecutionShape::GenericNoKey:
      return "generic-no-key";
    case AggregateExecutionShape::GenericSingleStringKey:
      return "generic-single-string-key";
    case AggregateExecutionShape::GenericSingleInt64Key:
      return "generic-single-int64-key";
    case AggregateExecutionShape::GenericDoubleInt64Key:
      return "generic-double-int64-key";
    case AggregateExecutionShape::GenericPackedKeys2:
      return "generic-packed-keys-2";
    case AggregateExecutionShape::GenericPackedKeys3:
      return "generic-packed-keys-3";
    case AggregateExecutionShape::SumNoKey:
      return "sum-no-key";
    case AggregateExecutionShape::SumSingleInt64Key:
      return "sum-single-int64-key";
    case AggregateExecutionShape::SumDoubleInt64Key:
      return "sum-double-int64-key";
  }
  return "generic-serialized-keys";
}

AggregateExecutionPattern analyzeAggregateExecution(
    const Table& input,
    const std::vector<std::size_t>& key_indices,
    const std::vector<AggregateSpec>& aggregates,
    bool ordered_input,
    bool partition_local) {
  AggregateExecutionPattern pattern;
  const bool single_sum =
      aggregates.size() == 1 && aggregates.front().function == AggregateFunction::Sum;
  std::size_t dense_groups = 0;
  pattern.exec_spec.properties =
      buildAggregateProperties(input, key_indices, ordered_input, partition_local, &dense_groups);
  pattern.exec_spec.key_layout.normalized_key_indices = key_indices;
  pattern.exec_spec.partial_layout =
      key_indices.size() == 2 ? AggregatePartialLayoutKind::TwoKeyColumnar
                              : AggregatePartialLayoutKind::GenericTable;
  pattern.exec_spec.key_layout.nullable_encoded = pattern.exec_spec.properties.has_nullable_keys;
  pattern.exec_spec.key_layout.fixed_width = pattern.exec_spec.properties.all_fixed_width;
  pattern.exec_spec.key_layout.packed = pattern.exec_spec.properties.packable;
  pattern.exec_spec.key_layout.ordered_input = pattern.exec_spec.properties.ordered_input;
  pattern.exec_spec.key_layout.low_cardinality = pattern.exec_spec.properties.low_cardinality;
  pattern.exec_spec.key_layout.transforms.push_back("identity");
  pattern.exec_spec.expected_groups = dense_groups;
  pattern.feedback.input_rows = input.rowCount();
  pattern.feedback.observed_ordered_input = pattern.exec_spec.properties.ordered_input;

  if (key_indices.empty()) {
    pattern.exec_spec.impl_kind = AggImplKind::HashFixed;
    pattern.exec_spec.reason = "aggregate has no group keys; use the scalar accumulator path";
    pattern.shape =
        single_sum ? AggregateExecutionShape::SumNoKey : AggregateExecutionShape::GenericNoKey;
    return pattern;
  }

  if (pattern.exec_spec.properties.partition_local) {
    pattern.exec_spec.impl_kind = AggImplKind::LocalGlobal;
    pattern.exec_spec.use_local_global = true;
    pattern.exec_spec.reason =
        "input is partition-local on group keys; prefer local/global aggregate composition";
  } else if (pattern.exec_spec.properties.ordered_input) {
    pattern.exec_spec.impl_kind = AggImplKind::SortStreaming;
    pattern.exec_spec.input_requires_sort = false;
    pattern.exec_spec.reason =
        "input is already ordered on group keys; prefer ordered run aggregation";
  } else if (pattern.exec_spec.properties.low_cardinality &&
             !pattern.exec_spec.properties.has_nullable_keys) {
    pattern.exec_spec.impl_kind = AggImplKind::Dense;
    pattern.exec_spec.reason =
        "single int64 key has a compact observed domain; prefer dense indexing";
  } else if (pattern.exec_spec.properties.packable) {
    pattern.exec_spec.impl_kind = AggImplKind::HashPacked;
    pattern.exec_spec.reason =
        "group keys fit the generic packed-key path; avoid serialized-key materialization";
  } else if (pattern.exec_spec.properties.all_fixed_width) {
    pattern.exec_spec.impl_kind = AggImplKind::HashFixed;
    pattern.exec_spec.reason =
        "all group keys are fixed-width; prefer fixed-width hash aggregation";
  } else {
    pattern.exec_spec.impl_kind = AggImplKind::HashRef;
    pattern.exec_spec.reason =
        "group keys require ref-oriented handling; use generic hash aggregation";
  }

  if (!pattern.exec_spec.properties.partition_local) {
    appendRejectedCandidate(&pattern.exec_spec.rejected_candidates,
                            "local-global: input is not partition-local");
  }
  if (!pattern.exec_spec.properties.ordered_input) {
    appendRejectedCandidate(&pattern.exec_spec.rejected_candidates,
                            "sort-streaming: input is not ordered on group keys");
  }
  if (!pattern.exec_spec.properties.low_cardinality) {
    appendRejectedCandidate(&pattern.exec_spec.rejected_candidates,
                            "dense: key domain is not compact enough");
  }
  if (!pattern.exec_spec.properties.packable) {
    appendRejectedCandidate(&pattern.exec_spec.rejected_candidates,
                            "hash-packed: keys are not packable");
  }

  if (key_indices.size() == 1) {
    const auto key_column = viewValueColumn(input, key_indices.front());
    switch (analyzeKeyColumnShape(*key_column.buffer)) {
      case KeyColumnShape::String:
        if (pattern.exec_spec.impl_kind == AggImplKind::HashRef) {
          pattern.exec_spec.reserved_buckets = input.rowCount();
        }
        pattern.shape = AggregateExecutionShape::GenericSingleStringKey;
        return pattern;
      case KeyColumnShape::Int64:
        if (pattern.exec_spec.impl_kind == AggImplKind::Dense) {
          pattern.shape = AggregateExecutionShape::GenericSingleInt64Key;
          pattern.exec_spec.reserved_buckets =
              pattern.exec_spec.expected_groups == 0 ? input.rowCount() : pattern.exec_spec.expected_groups;
          return pattern;
        }
        pattern.shape =
            single_sum ? AggregateExecutionShape::SumSingleInt64Key
                       : AggregateExecutionShape::GenericSingleInt64Key;
        pattern.exec_spec.reserved_buckets = input.rowCount();
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
      if (pattern.exec_spec.impl_kind == AggImplKind::HashPacked) {
        pattern.shape = AggregateExecutionShape::GenericPackedKeys2;
      } else {
        pattern.shape =
            single_sum ? AggregateExecutionShape::SumDoubleInt64Key
                       : AggregateExecutionShape::GenericDoubleInt64Key;
      }
      pattern.exec_spec.reserved_buckets = input.rowCount();
      return pattern;
    }
    const bool supported =
        (first_shape == KeyColumnShape::String || first_shape == KeyColumnShape::Int64 ||
         first_shape == KeyColumnShape::MixedStringInt64OrNull) &&
        (second_shape == KeyColumnShape::String || second_shape == KeyColumnShape::Int64 ||
         second_shape == KeyColumnShape::MixedStringInt64OrNull);
    if (supported && pattern.exec_spec.impl_kind == AggImplKind::HashPacked) {
      pattern.shape = AggregateExecutionShape::GenericPackedKeys2;
      pattern.exec_spec.reserved_buckets = input.rowCount();
      return pattern;
    }
    if (first_shape == KeyColumnShape::Int64 && second_shape == KeyColumnShape::Int64) {
      pattern.shape =
          single_sum ? AggregateExecutionShape::SumDoubleInt64Key
                     : AggregateExecutionShape::GenericDoubleInt64Key;
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

  if (pattern.exec_spec.impl_kind == AggImplKind::SortStreaming) {
    pattern.shape = AggregateExecutionShape::GenericSerializedKeys;
    pattern.exec_spec.reserved_buckets = 0;
    return pattern;
  }

  pattern.shape = AggregateExecutionShape::GenericSerializedKeys;
  pattern.exec_spec.reserved_buckets = input.rowCount();
  return pattern;
}

}  // namespace dataflow
