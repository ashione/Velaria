#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "src/dataflow/core/contract/api/session.h"
#include "src/dataflow/core/execution/stream/stream.h"

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

std::unordered_map<std::string, double> countTableToMap(const Table& table) {
  std::unordered_map<std::string, double> out;
  const auto window_idx = table.schema.indexOf("window_start");
  const auto key_idx = table.schema.indexOf("key");
  const auto value_idx = table.schema.indexOf("event_count");
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
  options.max_inflight_partitions = 4;

  auto query = session.readStream(std::make_shared<MemoryStreamSource>(std::vector<Table>{makeHotPathBatch()}))
                   .withStateStore(dataflow::makeMemoryStateStore())
                   .groupBy({"window_start", "key"})
                   .sum("value", true, "value_sum")
                   .writeStream(sink, options);
  query.start();
  expect(query.awaitTermination() == 1, "hot-path query should process one batch");
  return {sink->lastTable(), query.progress()};
}

QueryRun runCountHotPath(StreamingExecutionMode mode) {
  DataflowSession& session = DataflowSession::builder();
  auto sink = std::make_shared<MemoryStreamSink>();

  StreamingQueryOptions options;
  options.trigger_interval_ms = 0;
  options.execution_mode = mode;
  options.local_workers = 4;
  options.max_inflight_partitions = 4;

  auto query = session.readStream(std::make_shared<MemoryStreamSource>(std::vector<Table>{makeHotPathBatch()}))
                   .withStateStore(dataflow::makeMemoryStateStore())
                   .groupBy({"window_start", "key"})
                   .count(true, "event_count")
                   .writeStream(sink, options);
  query.start();
  expect(query.awaitTermination() == 1, "count hot-path query should process one batch");
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
  session.sql(
      "CREATE SINK TABLE strategy_count_sink (window_start STRING, key STRING, event_count INT) "
      "USING csv OPTIONS(path '/tmp/velaria-stream-strategy-count-explain.csv', delimiter ',')");
  session.sql(
      "CREATE SINK TABLE strategy_multi_sink "
      "(window_start STRING, key STRING, event_count INT, avg_value DOUBLE) "
      "USING csv OPTIONS(path '/tmp/velaria-stream-strategy-multi-explain.csv', delimiter ',')");

  StreamingQueryOptions options;
  options.execution_mode = StreamingExecutionMode::LocalWorkers;
  options.local_workers = 4;
  options.max_inflight_partitions = 4;

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
  expect(explain.find("selected_mode=local-workers") != std::string::npos,
         "explain should expose the selected local-workers mode");
  expect(explain.find("shared_memory_transport=true") != std::string::npos,
         "explain should expose shared-memory knobs");

  const std::string count_explain = session.explainStreamSql(
      "INSERT INTO strategy_count_sink "
      "SELECT window_start, key, COUNT(*) AS event_count "
      "FROM strategy_hot_events GROUP BY window_start, key",
      options);
  expect(count_explain.find("COUNT(*) AS event_count") != std::string::npos,
         "count explain should describe count aggregate");
  expect(count_explain.find("actor_eligible=true") != std::string::npos,
         "count explain should expose actor eligibility");

  const std::string multi_explain = session.explainStreamSql(
      "INSERT INTO strategy_multi_sink "
      "SELECT window_start, key, COUNT(*) AS event_count, AVG(value) AS avg_value "
      "FROM strategy_hot_events GROUP BY window_start, key",
      options);
  expect(multi_explain.find("AVG(value) AS avg_value") != std::string::npos,
         "multi explain should describe avg aggregate");
  expect(multi_explain.find("actor_eligible=false") != std::string::npos,
         "multi explain should disable actor eligibility for multi aggregate");

  const std::string having_explain = session.explainStreamSql(
      "INSERT INTO strategy_hot_sink "
      "SELECT window_start, key, SUM(value) AS value_sum "
      "FROM strategy_hot_events GROUP BY window_start, key HAVING value_sum > 0",
      options);
  const std::string final_transform_reason =
      "actor acceleration requires the aggregate hot path to be the final stream transform";
  expect(having_explain.find("actor_eligible=false") != std::string::npos,
         "having explain should disable actor eligibility after aggregate");
  expect(having_explain.find("actor_eligibility_reason=" + final_transform_reason) !=
             std::string::npos,
         "having explain should expose the final-transform eligibility reason");
  expect(having_explain.find("reason=" + final_transform_reason) != std::string::npos,
         "having explain strategy reason should stay aligned with physical eligibility reason");
}

void testExecutionModeConsistency() {
  const auto single = runHotPath(StreamingExecutionMode::SingleProcess);
  const auto local = runHotPath(StreamingExecutionMode::LocalWorkers);

  const auto baseline = sumTableToMap(single.table);
  expect(sumTableToMap(local.table) == baseline, "local-workers result should match single-process");

  expect(single.progress.execution_mode == "single-process",
         "single-process progress should keep single-process mode");
  expect(local.progress.execution_mode == "local-workers",
         "local-workers progress should keep local-workers mode");
  expect(local.progress.used_actor_runtime,
         "eligible local-workers hot path should use the credit accelerator");
  expect(local.progress.transport_mode == "shared-memory" ||
             local.progress.transport_mode == "rpc-copy",
         "eligible local-workers hot path should expose accelerator transport mode");
  const auto count_single = runCountHotPath(StreamingExecutionMode::SingleProcess);
  const auto count_local = runCountHotPath(StreamingExecutionMode::LocalWorkers);
  expect(countTableToMap(count_local.table) == countTableToMap(count_single.table),
         "count local-workers result should match single-process");
  expect(count_local.progress.execution_mode == "local-workers",
         "count local-workers hot path should stay in local-workers mode");
  expect(count_local.progress.used_actor_runtime,
         "count local-workers hot path should use the credit accelerator");
}

void testLocalWorkersFallbackForNonHotPath() {
  DataflowSession& session = DataflowSession::builder();
  auto sink = std::make_shared<MemoryStreamSink>();

  StreamingQueryOptions options;
  options.trigger_interval_ms = 0;
  options.execution_mode = StreamingExecutionMode::LocalWorkers;
  options.local_workers = 4;
  options.max_inflight_partitions = 4;

  auto query = session.readStream(std::make_shared<MemoryStreamSource>(std::vector<Table>{makeNonHotBatch()}))
                   .withStateStore(dataflow::makeMemoryStateStore())
                   .groupBy({"key"})
                   .count(true, "event_count")
                   .writeStream(sink, options);
  query.start();
  expect(query.awaitTermination() == 1, "non-hot-path query should process one batch");
  const auto progress = query.progress();

  expect(progress.execution_mode == "local-workers",
         "non-hot-path local-workers query should stay in local-workers mode");
  expect(!progress.used_actor_runtime,
         "non-hot-path local-workers query should fall back to generic partition workers");
  expect(progress.execution_reason.find("generic partition workers") != std::string::npos,
         "non-hot-path local-workers query should expose fallback reason");
}

void testLocalWorkersSingleWorkerDisablesCreditAcceleration() {
  DataflowSession& session = DataflowSession::builder();
  auto sink = std::make_shared<MemoryStreamSink>();

  StreamingQueryOptions options;
  options.trigger_interval_ms = 0;
  options.execution_mode = StreamingExecutionMode::LocalWorkers;
  options.local_workers = 1;
  options.max_inflight_partitions = 4;

  auto query = session.readStream(std::make_shared<MemoryStreamSource>(std::vector<Table>{makeHotPathBatch()}))
                   .withStateStore(dataflow::makeMemoryStateStore())
                   .groupBy({"window_start", "key"})
                   .sum("value", true, "value_sum")
                   .writeStream(sink, options);
  query.start();
  expect(query.awaitTermination() == 1, "single-worker local-workers query should process one batch");
  const auto progress = query.progress();

  expect(progress.execution_mode == "local-workers",
         "single-worker local-workers query should stay in local-workers mode");
  expect(!progress.used_actor_runtime,
         "single-worker local-workers query should not use credit acceleration");
  expect(progress.transport_mode == "inproc",
         "single-worker local-workers query should stay in inproc transport");
  expect(progress.execution_reason.find("local_workers > 1") != std::string::npos,
         "single-worker local-workers query should explain why credit acceleration was skipped");
}

}  // namespace

int main() {
  try {
    testExplainStreamSql();
    testExecutionModeConsistency();
    testLocalWorkersFallbackForNonHotPath();
    testLocalWorkersSingleWorkerDisablesCreditAcceleration();
    std::cout << "[test] stream strategy explain ok" << std::endl;
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "[test] stream strategy explain failed: " << ex.what() << std::endl;
    return 1;
  }
}
