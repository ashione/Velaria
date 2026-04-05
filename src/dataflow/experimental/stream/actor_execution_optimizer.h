#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "src/dataflow/experimental/stream/actor_stream_runtime.h"

namespace dataflow {

enum class LocalGroupedAggregateShape {
  Generic = 0,
  WindowKeySumString = 1,
  WindowKeySumInt64 = 2,
};

enum class LocalTransportEncoding {
  ArrowIpc = 0,
  BinaryRowBatch = 1,
};

struct LocalGroupedAggregateExecutionPattern {
  LocalGroupedAggregateShape aggregate_shape = LocalGroupedAggregateShape::Generic;
  LocalTransportEncoding transport_encoding = LocalTransportEncoding::ArrowIpc;
  bool use_window_key_partial_merge = false;
};

struct LocalExecutionDecisionInputs {
  size_t sampled_batches = 0;
  size_t rows_per_batch = 0;
  size_t average_projected_payload_bytes = 0;
  double single_rows_per_s = 0.0;
  double actor_rows_per_s = 0.0;
  double compute_to_overhead_ratio = 0.0;
};

const char* localGroupedAggregateShapeName(LocalGroupedAggregateShape shape);
const char* localTransportEncodingName(LocalTransportEncoding encoding);

LocalGroupedAggregateExecutionPattern analyzeLocalGroupedAggregateExecution(
    const Table& input, const LocalGroupedAggregateSpec& aggregate);
LocalGroupedAggregateExecutionPattern analyzeLocalGroupedAggregateExecution(
    const std::vector<Table>& batches, const LocalGroupedAggregateSpec& aggregate);

LocalExecutionDecision analyzeLocalExecutionDecision(
    const LocalExecutionDecisionInputs& inputs,
    const LocalExecutionAutoOptions& auto_options);

}  // namespace dataflow
