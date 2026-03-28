#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "src/dataflow/core/table.h"

namespace dataflow {

struct LocalActorStreamOptions {
  size_t worker_count = 2;
  size_t max_inflight_partitions = 2;
  uint64_t worker_delay_ms = 0;
  size_t cpu_spin_per_row = 0;
};

struct LocalActorStreamResult {
  Table final_table;
  size_t processed_batches = 0;
  size_t processed_partitions = 0;
  size_t blocked_count = 0;
  size_t max_inflight_partitions = 0;
  uint64_t elapsed_ms = 0;
  uint64_t split_ms = 0;
  uint64_t coordinator_serialize_ms = 0;
  uint64_t coordinator_wait_ms = 0;
  uint64_t coordinator_deserialize_ms = 0;
  uint64_t coordinator_merge_ms = 0;
  uint64_t worker_deserialize_ms = 0;
  uint64_t worker_compute_ms = 0;
  uint64_t worker_serialize_ms = 0;
  uint64_t input_payload_bytes = 0;
  uint64_t output_payload_bytes = 0;
};

Table runSingleProcessWindowKeySum(const std::vector<Table>& batches,
                                   size_t cpu_spin_per_row = 0);
LocalActorStreamResult runLocalActorStreamWindowKeySum(const std::vector<Table>& batches,
                                                       const LocalActorStreamOptions& options);

}  // namespace dataflow
