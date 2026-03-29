#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "src/dataflow/api/session.h"
#include "src/dataflow/stream/stream.h"

namespace {

using dataflow::DataflowSession;
using dataflow::MemoryStreamSink;
using dataflow::MemoryStreamSource;
using dataflow::StreamingExecutionMode;
using dataflow::StreamingQueryOptions;
using dataflow::Table;
using dataflow::Value;

void expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

Table makeHotPathBatch() {
  Table table;
  table.schema = dataflow::Schema({"window_start", "key", "value"});
  table.rows = {
      {Value("2026-03-29T10:00:00"), Value("userA"), Value(int64_t(1))},
      {Value("2026-03-29T10:00:00"), Value("userA"), Value(int64_t(2))},
      {Value("2026-03-29T10:00:00"), Value("userB"), Value(int64_t(3))},
      {Value("2026-03-29T10:01:00"), Value("userA"), Value(int64_t(4))},
      {Value("2026-03-29T10:01:00"), Value("userB"), Value(int64_t(5))},
  };
  return table;
}

Table makeNonHotBatch() {
  Table table;
  table.schema = dataflow::Schema({"key", "value"});
  table.rows = {
      {Value("userA"), Value(int64_t(1))},
      {Value("userA"), Value(int64_t(2))},
      {Value("userB"), Value(int64_t(3))},
  };
  return table;
}

std::unordered_map<std::string, double> sumTableToMap(const Table& table) {
  std::unordered_map<std::string, double> out;
  const auto window_idx = table.schema.indexOf("window_start");
  const auto key_idx = table.schema.indexOf("key");
  const auto value_idx = table.schema.indexOf("value_sum");
  for (const auto& row : table.rows) {
    out[row[window_idx].toString() + "|" + row[key_idx].toString()] = row[value_idx].asDouble();
  }
  return out;
}

struct QueryRun {
  Table table;
  dataflow::StreamingQueryProgress progress;
};

QueryRun runHotPath(StreamingExecutionMode mode) {
  DataflowSession& session = DataflowSession::builder();
  auto sink = std::make_shared<MemoryStreamSink>();

  StreamingQueryOptions options;
  options.trigger_interval_ms = 0;
  options.execution_mode = mode;
  options.local_workers = 4;
  options.actor_workers = 4;
  options.actor_max_inflight_partitions = 4;
  options.actor_auto_options.sample_batches = 1;
  options.actor_auto_options.min_rows_per_batch = 0;
  options.actor_auto_options.min_projected_payload_bytes = 0;
  options.actor_auto_options.min_compute_to_overhead_ratio = 0.0;
  options.actor_auto_options.min_actor_speedup = 0.0;
  options.actor_auto_options.strong_actor_speedup = 0.0;

  auto query = session.readStream(std::make_shared<MemoryStreamSource>(std::vector<Table>{makeHotPathBatch()}))
                   .withStateStore(dataflow::makeMemoryStateStore())
                   .groupBy({"window_start", "key"})
                   .sum("value", true, "value_sum")
                   .writeStream(sink, options);
  query.start();
  expect(query.awaitTermination() == 1, "hot-path query should process one batch");
  return {sink->lastTable(), query.progress()};
}

void testExplainStreamSql() {
  DataflowSession& session = DataflowSession::builder();
  session.createTempView(
      "strategy_hot_events",
      session.readStream(std::make_shared<MemoryStreamSource>(std::vector<Table>{makeHotPathBatch()})));
  session.sql(
      "CREATE SINK TABLE strategy_hot_sink (window_start STRING, key STRING, value_sum INT) "
      "USING csv OPTIONS(path '/tmp/velaria-stream-strategy-explain.csv', delimiter ',')");

  StreamingQueryOptions options;
  options.execution_mode = StreamingExecutionMode::Auto;
  options.actor_workers = 4;
  options.actor_max_inflight_partitions = 4;
  options.actor_auto_options.sample_batches = 1;
  options.actor_auto_options.min_rows_per_batch = 0;
  options.actor_auto_options.min_projected_payload_bytes = 0;
  options.actor_auto_options.min_compute_to_overhead_ratio = 0.0;
  options.actor_auto_options.min_actor_speedup = 0.0;
  options.actor_auto_options.strong_actor_speedup = 0.0;

  const std::string explain = session.explainStreamSql(
      "INSERT INTO strategy_hot_sink "
      "SELECT window_start, key, SUM(value) AS value_sum "
      "FROM strategy_hot_events GROUP BY window_start, key",
      options);

  expect(explain.find("logical\n") != std::string::npos, "explain should contain logical section");
  expect(explain.find("physical\n") != std::string::npos, "explain should contain physical section");
  expect(explain.find("strategy\n") != std::string::npos, "explain should contain strategy section");
  expect(explain.find("Aggregate keys=[window_start, key]") != std::string::npos,
         "explain should describe aggregate keys");
  expect(explain.find("actor_eligible=true") != std::string::npos,
         "explain should expose actor eligibility");
  expect(explain.find("selected_mode=auto") != std::string::npos,
         "explain should expose the initial selected mode");
  expect(explain.find("actor_shared_memory_transport=true") != std::string::npos,
         "explain should expose shared-memory knobs");
}

void testExecutionModeConsistency() {
  const auto single = runHotPath(StreamingExecutionMode::SingleProcess);
  const auto local = runHotPath(StreamingExecutionMode::LocalWorkers);
  const auto actor = runHotPath(StreamingExecutionMode::ActorCredit);
  const auto automatic = runHotPath(StreamingExecutionMode::Auto);

  const auto baseline = sumTableToMap(single.table);
  expect(sumTableToMap(local.table) == baseline, "local-workers result should match single-process");
  expect(sumTableToMap(actor.table) == baseline, "actor-credit result should match single-process");
  expect(sumTableToMap(automatic.table) == baseline, "auto result should match single-process");

  expect(single.progress.execution_mode == "single-process",
         "single-process progress should keep single-process mode");
  expect(local.progress.execution_mode == "local-workers",
         "local-workers progress should keep local-workers mode");
  expect(actor.progress.execution_mode == "actor-credit",
         "actor-credit progress should keep actor-credit mode");
  expect(automatic.progress.execution_mode == "actor-credit",
         "auto hot path should resolve to actor-credit with permissive thresholds");
  expect(automatic.progress.transport_mode == "shared-memory" ||
             automatic.progress.transport_mode == "rpc-copy",
         "auto hot path should expose actor transport mode");
}

void testAutoFallbackForNonHotPath() {
  DataflowSession& session = DataflowSession::builder();
  auto sink = std::make_shared<MemoryStreamSink>();

  StreamingQueryOptions options;
  options.trigger_interval_ms = 0;
  options.execution_mode = StreamingExecutionMode::Auto;
  options.actor_workers = 4;
  options.actor_max_inflight_partitions = 4;
  options.actor_auto_options.sample_batches = 1;
  options.actor_auto_options.min_rows_per_batch = 0;
  options.actor_auto_options.min_projected_payload_bytes = 0;
  options.actor_auto_options.min_compute_to_overhead_ratio = 0.0;
  options.actor_auto_options.min_actor_speedup = 0.0;
  options.actor_auto_options.strong_actor_speedup = 0.0;

  auto query = session.readStream(std::make_shared<MemoryStreamSource>(std::vector<Table>{makeNonHotBatch()}))
                   .withStateStore(dataflow::makeMemoryStateStore())
                   .groupBy({"key"})
                   .count(true, "event_count")
                   .writeStream(sink, options);
  query.start();
  expect(query.awaitTermination() == 1, "non-hot-path query should process one batch");
  const auto progress = query.progress();

  expect(progress.execution_mode == "single-process",
         "non-hot-path auto query should fall back to single-process");
  expect(progress.execution_reason.find("not eligible") != std::string::npos,
         "non-hot-path auto query should expose fallback reason");
}

}  // namespace

int main() {
  try {
    testExplainStreamSql();
    testExecutionModeConsistency();
    testAutoFallbackForNonHotPath();
    std::cout << "[test] stream strategy explain ok" << std::endl;
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "[test] stream strategy explain failed: " << ex.what() << std::endl;
    return 1;
  }
}
