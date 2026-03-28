#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include <cstdlib>

#include "src/dataflow/stream/actor_stream_runtime.h"

namespace {

std::vector<dataflow::Table> makeBatches(size_t batch_count, size_t rows_per_batch) {
  std::vector<dataflow::Table> batches;
  batches.reserve(batch_count);
  for (size_t batch = 0; batch < batch_count; ++batch) {
    dataflow::Table table;
    table.schema = dataflow::Schema({"window_start", "key", "value"});
    table.rows.reserve(rows_per_batch);
    const std::string window = "2026-03-28T09:" + std::to_string(static_cast<int>(batch % 10));
    for (size_t row = 0; row < rows_per_batch; ++row) {
      table.rows.push_back({dataflow::Value(window),
                            dataflow::Value("user_" + std::to_string(row % 128)),
                            dataflow::Value(int64_t((row % 9) + 1))});
    }
    batches.push_back(std::move(table));
  }
  return batches;
}

void printMetrics(const std::string& label, const dataflow::LocalActorStreamResult& result,
                  size_t rows_per_batch) {
  const double elapsed_s = static_cast<double>(result.elapsed_ms) / 1000.0;
  const double rows_per_s =
      elapsed_s > 0.0 ? (result.processed_batches * rows_per_batch) / elapsed_s : 0.0;
  std::cout << "[actor-stream] " << label << " batches=" << result.processed_batches
            << " partitions=" << result.processed_partitions
            << " elapsed_ms=" << result.elapsed_ms
            << " rows_per_s=" << rows_per_s
            << " blocked=" << result.blocked_count
            << " max_inflight=" << result.max_inflight_partitions
            << " result_rows=" << result.final_table.rowCount()
            << " split_ms=" << result.split_ms
            << " coord_serialize_ms=" << result.coordinator_serialize_ms
            << " coord_wait_ms=" << result.coordinator_wait_ms
            << " coord_deserialize_ms=" << result.coordinator_deserialize_ms
            << " coord_merge_ms=" << result.coordinator_merge_ms
            << " worker_deserialize_ms=" << result.worker_deserialize_ms
            << " worker_compute_ms=" << result.worker_compute_ms
            << " worker_serialize_ms=" << result.worker_serialize_ms
            << " input_payload_bytes=" << result.input_payload_bytes
            << " output_payload_bytes=" << result.output_payload_bytes
            << std::endl;
}

}  // namespace

int main(int argc, char** argv) {
  size_t batch_count = 32;
  size_t rows_per_batch = 4096;
  size_t worker_count = 4;
  size_t max_inflight = 4;
  uint64_t worker_delay_ms = 5;
  size_t cpu_spin_per_row = 0;

  if (argc > 1) batch_count = static_cast<size_t>(std::strtoull(argv[1], nullptr, 10));
  if (argc > 2) rows_per_batch = static_cast<size_t>(std::strtoull(argv[2], nullptr, 10));
  if (argc > 3) worker_count = static_cast<size_t>(std::strtoull(argv[3], nullptr, 10));
  if (argc > 4) max_inflight = static_cast<size_t>(std::strtoull(argv[4], nullptr, 10));
  if (argc > 5) worker_delay_ms = static_cast<uint64_t>(std::strtoull(argv[5], nullptr, 10));
  if (argc > 6) cpu_spin_per_row = static_cast<size_t>(std::strtoull(argv[6], nullptr, 10));

  const auto batches = makeBatches(batch_count, rows_per_batch);

  const auto single_started = std::chrono::steady_clock::now();
  dataflow::LocalActorStreamResult single_result;
  single_result.final_table = dataflow::runSingleProcessWindowKeySum(batches, cpu_spin_per_row);
  single_result.processed_batches = batch_count;
  single_result.processed_partitions = batch_count;
  single_result.elapsed_ms = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - single_started)
          .count());
  printMetrics("single-process", single_result, rows_per_batch);

  dataflow::LocalActorStreamOptions actor_options;
  actor_options.worker_count = worker_count;
  actor_options.max_inflight_partitions = max_inflight;
  actor_options.worker_delay_ms = worker_delay_ms;
  actor_options.cpu_spin_per_row = cpu_spin_per_row;
  const auto actor_result = dataflow::runLocalActorStreamWindowKeySum(batches, actor_options);
  printMetrics("actor-credit", actor_result, rows_per_batch);

  return 0;
}
