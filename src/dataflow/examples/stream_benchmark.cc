#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include <cstdlib>

#include "src/dataflow/api/session.h"
#include "src/dataflow/runtime/observability.h"
#include "src/dataflow/stream/stream.h"

namespace {

std::vector<dataflow::Table> makeBatches(size_t batch_count, size_t rows_per_batch) {
  std::vector<dataflow::Table> batches;
  batches.reserve(batch_count);
  int64_t ts_base = 1711616400000LL;
  for (size_t b = 0; b < batch_count; ++b) {
    dataflow::Table batch;
    batch.schema = dataflow::Schema({"ts", "key", "value"});
    batch.rows.reserve(rows_per_batch);
    for (size_t r = 0; r < rows_per_batch; ++r) {
      const auto ts = ts_base + static_cast<int64_t>((b * rows_per_batch + r) * 1000);
      const std::string key = "user_" + std::to_string(r % 64);
      batch.rows.push_back(
          {dataflow::Value(ts), dataflow::Value(key), dataflow::Value(int64_t((r % 7) + 1))});
    }
    batches.push_back(std::move(batch));
  }
  return batches;
}

void runCase(const std::string& name, dataflow::StreamingExecutionMode mode, size_t workers,
             bool stateful, size_t batch_count, size_t rows_per_batch) {
  using namespace dataflow::observability;
  dataflow::DataflowSession& session = dataflow::DataflowSession::builder();
  auto source =
      std::make_shared<dataflow::MemoryStreamSource>(makeBatches(batch_count, rows_per_batch));

  dataflow::StreamingQueryOptions options;
  options.trigger_interval_ms = 0;
  options.execution_mode = mode;
  options.local_workers = workers;
  options.actor_workers = workers;
  options.actor_max_inflight_partitions = workers;
  options.max_inflight_batches = 4;
  options.max_queued_partitions = 16;
  options.max_retained_windows = 4;

  auto sink = std::make_shared<dataflow::MemoryStreamSink>();
  auto state = dataflow::makeMemoryStateStore();
  auto stream = session.readStream(source)
                    .withStateStore(state)
                    .window("ts", 60000, "window_start")
                    .groupBy({"window_start", "key"});

  auto query = stateful ? stream.sum("value", true, "value_sum").writeStream(sink, options)
                        : stream.sum("value", false, "value_sum").writeStream(sink, options);
  query.start();

  const auto started = std::chrono::steady_clock::now();
  const size_t processed = query.awaitTermination();
  const auto elapsed = std::chrono::steady_clock::now() - started;
  const auto progress = query.progress();

  const double seconds =
      std::chrono::duration_cast<std::chrono::duration<double>>(elapsed).count();
  const double batches_per_sec = seconds > 0.0 ? processed / seconds : 0.0;
  const double rows_per_sec =
      seconds > 0.0 ? (processed * static_cast<double>(rows_per_batch)) / seconds : 0.0;

  std::cout << "[bench] " << name << " processed=" << processed << " elapsed_s=" << seconds
            << " batches_per_s=" << batches_per_sec << " rows_per_s=" << rows_per_sec
            << " mode=" << progress.execution_mode
            << " reason=" << progress.execution_reason
            << " blocked=" << progress.blocked_count
            << " last_batch_ms=" << progress.last_batch_latency_ms
            << " last_state_ms=" << progress.last_state_latency_ms << std::endl;
  std::cout << "[bench-json] "
            << object({
                   field("name", name),
                   field("processed", processed),
                   field("elapsed_s", std::to_string(seconds), true),
                   field("batches_per_s", std::to_string(batches_per_sec), true),
                   field("rows_per_s", std::to_string(rows_per_sec), true),
                   field("mode", progress.execution_mode),
                   field("reason", progress.execution_reason),
                   field("transport_mode", progress.transport_mode),
                   field("blocked_count", progress.blocked_count),
                   field("last_batch_latency_ms", progress.last_batch_latency_ms),
                   field("last_state_latency_ms", progress.last_state_latency_ms),
                   field("actor_eligible", progress.actor_eligible),
                   field("used_actor_runtime", progress.used_actor_runtime),
                   field("used_shared_memory", progress.used_shared_memory),
               })
            << std::endl;
}

}  // namespace

int main(int argc, char** argv) {
  size_t batch_count = 32;
  size_t rows_per_batch = 8192;
  size_t worker_count = 4;

  if (argc > 1) batch_count = static_cast<size_t>(std::strtoull(argv[1], nullptr, 10));
  if (argc > 2) rows_per_batch = static_cast<size_t>(std::strtoull(argv[2], nullptr, 10));
  if (argc > 3) worker_count = static_cast<size_t>(std::strtoull(argv[3], nullptr, 10));

  runCase("stateless-single", dataflow::StreamingExecutionMode::SingleProcess, 1, false, batch_count,
          rows_per_batch);
  runCase("stateless-local-workers", dataflow::StreamingExecutionMode::LocalWorkers, worker_count,
          false, batch_count, rows_per_batch);
  runCase("stateful-single", dataflow::StreamingExecutionMode::SingleProcess, 1, true, batch_count,
          rows_per_batch);
  runCase("stateful-local-workers", dataflow::StreamingExecutionMode::LocalWorkers, worker_count, true,
          batch_count, rows_per_batch);
  runCase("stateful-actor-credit", dataflow::StreamingExecutionMode::ActorCredit, worker_count, true,
          batch_count, rows_per_batch);
  runCase("stateful-auto", dataflow::StreamingExecutionMode::Auto, worker_count, true, batch_count,
          rows_per_batch);
  return 0;
}
