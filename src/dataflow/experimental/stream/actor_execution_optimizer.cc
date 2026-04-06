#include "src/dataflow/experimental/stream/actor_execution_optimizer.h"

#include "src/dataflow/core/execution/arrow_format.h"
#include "src/dataflow/core/execution/columnar_batch.h"

namespace dataflow {

namespace {

bool canUseSingleValueSumColumnarKernel(const LocalGroupedAggregateSpec& aggregate) {
  return aggregate.group_keys.size() == 2 &&
         aggregate.aggregates.size() == 1 &&
         aggregate.aggregates[0].function == AggregateFunction::Sum &&
         !aggregate.aggregates[0].is_count_star;
}

bool isStringLikeColumn(const Table& table, std::size_t column_index) {
  if (!table.rows.empty()) {
    for (const auto& row : table.rows) {
      if (column_index >= row.size() || row[column_index].isNull()) {
        continue;
      }
      return row[column_index].type() == DataType::String;
    }
    return true;
  }
  const auto column = viewValueColumn(table, column_index);
  if (column.buffer == nullptr) {
    return false;
  }
  if (column.buffer->arrow_backing != nullptr) {
    return isArrowUtf8Format(column.buffer->arrow_backing->format);
  }
  const auto row_count = valueColumnRowCount(*column.buffer);
  for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
    const auto value = valueColumnValueAt(*column.buffer, row_index);
    if (value.isNull()) continue;
    return value.type() == DataType::String;
  }
  return true;
}

bool isInt64LikeColumn(const Table& table, std::size_t column_index) {
  if (!table.rows.empty()) {
    for (const auto& row : table.rows) {
      if (column_index >= row.size() || row[column_index].isNull()) {
        continue;
      }
      return row[column_index].type() == DataType::Int64;
    }
    return true;
  }
  const auto column = viewValueColumn(table, column_index);
  if (column.buffer == nullptr) {
    return false;
  }
  if (column.buffer->arrow_backing != nullptr) {
    return isArrowIntegerLikeFormat(column.buffer->arrow_backing->format);
  }
  const auto row_count = valueColumnRowCount(*column.buffer);
  for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
    const auto value = valueColumnValueAt(*column.buffer, row_index);
    if (value.isNull()) continue;
    return value.type() == DataType::Int64;
  }
  return true;
}

bool isPrimitiveNumericColumn(const Table& table, std::size_t column_index) {
  if (!table.rows.empty()) {
    for (const auto& row : table.rows) {
      if (column_index >= row.size() || row[column_index].isNull()) {
        continue;
      }
      return row[column_index].isNumber() || row[column_index].isBool();
    }
    return true;
  }
  const auto column = viewValueColumn(table, column_index);
  if (column.buffer == nullptr) {
    return false;
  }
  if (column.buffer->arrow_backing != nullptr) {
    return isArrowPrimitiveNumericFormat(column.buffer->arrow_backing->format);
  }
  const auto row_count = valueColumnRowCount(*column.buffer);
  for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
    const auto value = valueColumnValueAt(*column.buffer, row_index);
    if (value.isNull()) continue;
    return value.isNumber() || value.isBool();
  }
  return true;
}

bool supportsBinaryRowBatchTransport(const Table& input, const LocalGroupedAggregateSpec& aggregate) {
  for (const auto& key_name : aggregate.group_keys) {
    if (!input.schema.has(key_name)) return false;
    const auto key_index = input.schema.indexOf(key_name);
    if (!isStringLikeColumn(input, key_index) && !isInt64LikeColumn(input, key_index)) {
      return false;
    }
  }
  for (const auto& aggregate_spec : aggregate.aggregates) {
    if (aggregate_spec.function == AggregateFunction::Count && aggregate_spec.is_count_star) {
      continue;
    }
    if (aggregate_spec.value_column.empty() || !input.schema.has(aggregate_spec.value_column)) {
      return false;
    }
    if (!isPrimitiveNumericColumn(input, input.schema.indexOf(aggregate_spec.value_column))) {
      return false;
    }
  }
  return !aggregate.group_keys.empty() && !aggregate.aggregates.empty();
}

}  // namespace

const char* localGroupedAggregateShapeName(LocalGroupedAggregateShape shape) {
  switch (shape) {
    case LocalGroupedAggregateShape::Generic:
      return "generic";
    case LocalGroupedAggregateShape::KeyColumnarStringSum:
      return "key-columnar-string-sum";
    case LocalGroupedAggregateShape::KeyColumnarFixedSum:
      return "key-columnar-fixed-sum";
  }
  return "generic";
}

const char* localTransportEncodingName(LocalTransportEncoding encoding) {
  switch (encoding) {
    case LocalTransportEncoding::ArrowIpc:
      return "arrow-ipc";
    case LocalTransportEncoding::BinaryRowBatch:
      return "binary-row-batch";
  }
  return "arrow-ipc";
}

LocalGroupedAggregateExecutionPattern analyzeLocalGroupedAggregateExecution(
    const Table& input, const LocalGroupedAggregateSpec& aggregate) {
  LocalGroupedAggregateExecutionPattern pattern;
  if (!supportsBinaryRowBatchTransport(input, aggregate)) {
    return pattern;
  }
  pattern.transport_encoding = LocalTransportEncoding::BinaryRowBatch;
  pattern.use_direct_partial_merge = true;

  if (!canUseSingleValueSumColumnarKernel(aggregate)) {
    return pattern;
  }
  if (!input.schema.has(aggregate.group_keys[0]) || !input.schema.has(aggregate.group_keys[1]) ||
      !input.schema.has(aggregate.aggregates[0].value_column)) {
    return pattern;
  }

  const auto first_key_index = input.schema.indexOf(aggregate.group_keys[0]);
  const auto second_key_index = input.schema.indexOf(aggregate.group_keys[1]);
  if (isStringLikeColumn(input, first_key_index) && isStringLikeColumn(input, second_key_index)) {
    pattern.aggregate_shape = LocalGroupedAggregateShape::KeyColumnarStringSum;
    return pattern;
  }
  if (isInt64LikeColumn(input, first_key_index) && isInt64LikeColumn(input, second_key_index)) {
    pattern.aggregate_shape = LocalGroupedAggregateShape::KeyColumnarFixedSum;
    return pattern;
  }
  return pattern;
}

LocalGroupedAggregateExecutionPattern analyzeLocalGroupedAggregateExecution(
    const std::vector<Table>& batches, const LocalGroupedAggregateSpec& aggregate) {
  if (batches.empty()) {
    return {};
  }
  return analyzeLocalGroupedAggregateExecution(batches.front(), aggregate);
}

LocalExecutionDecision analyzeLocalExecutionDecision(
    const LocalExecutionDecisionInputs& inputs,
    const LocalExecutionAutoOptions& auto_options) {
  LocalExecutionDecision decision;
  decision.chosen_mode = LocalExecutionMode::SingleProcess;
  decision.sampled_batches = inputs.sampled_batches;
  decision.rows_per_batch = inputs.rows_per_batch;
  decision.average_projected_payload_bytes = inputs.average_projected_payload_bytes;
  decision.single_rows_per_s = inputs.single_rows_per_s;
  decision.actor_rows_per_s = inputs.actor_rows_per_s;
  decision.actor_speedup =
      inputs.single_rows_per_s <= 0.0 ? 0.0 : (inputs.actor_rows_per_s / inputs.single_rows_per_s);
  decision.compute_to_overhead_ratio = inputs.compute_to_overhead_ratio;

  const bool rows_ok = decision.rows_per_batch >= auto_options.min_rows_per_batch;
  const bool payload_ok =
      decision.average_projected_payload_bytes >= auto_options.min_projected_payload_bytes;
  const bool ratio_ok =
      decision.compute_to_overhead_ratio >= auto_options.min_compute_to_overhead_ratio;
  const bool speed_ok = decision.actor_speedup >= auto_options.min_actor_speedup;
  const bool strong_speed_ok = decision.actor_speedup >= auto_options.strong_actor_speedup;
  const bool heuristic_ok = rows_ok && payload_ok && speed_ok && (ratio_ok || strong_speed_ok);
  const bool measured_override_ok = speed_ok && ratio_ok;
  const bool soft_measured_override_ok = decision.actor_speedup > 1.0 && ratio_ok;

  decision.thresholds_met = heuristic_ok || measured_override_ok || soft_measured_override_ok;
  if (decision.thresholds_met) {
    decision.chosen_mode = LocalExecutionMode::ActorCredit;
    if (heuristic_ok && ratio_ok) {
      decision.reason =
          "actor sample passed rows, payload, compute/overhead, and speedup thresholds";
    } else if (heuristic_ok) {
      decision.reason =
          "actor sample speedup is strong enough to override compute/overhead threshold";
    } else if (soft_measured_override_ok) {
      decision.reason =
          "actor sample is measurably faster with healthy compute/overhead despite conservative speed threshold";
    } else {
      decision.reason =
          "actor sample throughput and compute/overhead beat single-process despite small batch heuristics";
    }
  } else if (!rows_ok) {
    decision.reason = "rows_per_batch below actor threshold";
  } else if (!payload_ok) {
    decision.reason = "projected payload below actor threshold";
  } else if (!speed_ok) {
    decision.reason = "actor sample throughput not high enough";
  } else if (!ratio_ok) {
    decision.reason = "actor sample compute/overhead ratio too low";
  } else {
    decision.reason = "actor sample did not meet selection thresholds";
  }
  return decision;
}

}  // namespace dataflow
