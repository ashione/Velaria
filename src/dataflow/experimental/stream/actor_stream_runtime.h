#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "src/dataflow/core/logical/planner/plan.h"
#include "src/dataflow/core/execution/table.h"

namespace dataflow {

enum class LocalExecutionMode {
  SingleProcess,
  ActorCredit,
};

struct LocalActorStreamOptions {
  size_t worker_count = 2;
  size_t max_inflight_partitions = 2;
  uint64_t worker_delay_ms = 0;
  size_t cpu_spin_per_row = 0;
  bool shared_memory_transport = true;
  size_t shared_memory_min_payload_bytes = 64 * 1024;
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
  uint64_t input_shared_memory_bytes = 0;
  uint64_t output_shared_memory_bytes = 0;
  bool used_shared_memory = false;
};

struct LocalExecutionAutoOptions {
  size_t sample_batches = 2;
  size_t min_rows_per_batch = 64 * 1024;
  size_t min_projected_payload_bytes = 256 * 1024;
  double min_compute_to_overhead_ratio = 1.5;
  double min_actor_speedup = 1.05;
  double strong_actor_speedup = 1.25;
};

struct LocalExecutionDecision {
  LocalExecutionMode chosen_mode = LocalExecutionMode::SingleProcess;
  size_t sampled_batches = 0;
  size_t rows_per_batch = 0;
  size_t average_projected_payload_bytes = 0;
  double single_rows_per_s = 0.0;
  double actor_rows_per_s = 0.0;
  double actor_speedup = 0.0;
  double compute_to_overhead_ratio = 0.0;
  bool thresholds_met = false;
  std::string reason;
};

struct LocalGroupedAggregateSpec {
  struct Aggregate {
    AggregateFunction function = AggregateFunction::Sum;
    std::string value_column;
    std::string output_column;
    bool is_count_star = false;
  };

  std::vector<std::string> group_keys;
  std::vector<Aggregate> aggregates;
};

const char* localExecutionModeName(LocalExecutionMode mode);
Table runSingleProcessGroupedAggregate(const std::vector<Table>& batches,
                                      const LocalGroupedAggregateSpec& aggregate,
                                      size_t cpu_spin_per_row = 0);
LocalActorStreamResult runLocalActorStreamGroupedAggregate(
    const std::vector<Table>& batches, const LocalGroupedAggregateSpec& aggregate,
    const LocalActorStreamOptions& options);
LocalActorStreamResult runAutoLocalActorStreamGroupedAggregate(
    const std::vector<Table>& batches, const LocalGroupedAggregateSpec& aggregate,
    const LocalActorStreamOptions& actor_options,
    const LocalExecutionAutoOptions& auto_options = {},
    LocalExecutionDecision* decision = nullptr);

}  // namespace dataflow
