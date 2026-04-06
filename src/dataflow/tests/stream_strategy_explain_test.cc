#include <filesystem>
#include <cmath>
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
namespace fs = std::filesystem;

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

Table makeGenericMultiAggregateBatch() {
  Table table;
  table.schema = dataflow::Schema({"segment", "bucket", "value"});
  table.rows = {
      {Value("alpha"), Value(int64_t(1)), Value(int64_t(10))},
      {Value("alpha"), Value(int64_t(1)), Value(int64_t(14))},
      {Value("alpha"), Value(int64_t(2)), Value(int64_t(3))},
      {Value("beta"), Value(int64_t(1)), Value(int64_t(6))},
      {Value("beta"), Value(int64_t(1)), Value(int64_t(8))},
  };
  return table;
}

std::unordered_map<std::string, double> sumTableToMap(const Table& table) {
  auto materialized = table;
  dataflow::materializeRows(&materialized);
  std::unordered_map<std::string, double> out;
  const auto window_idx = materialized.schema.indexOf("window_start");
  const auto key_idx = materialized.schema.indexOf("key");
  const auto value_idx = materialized.schema.indexOf("value_sum");
  for (const auto& row : materialized.rows) {
    out[row[window_idx].toString() + "|" + row[key_idx].toString()] = row[value_idx].asDouble();
  }
  return out;
}

std::unordered_map<std::string, double> countTableToMap(const Table& table) {
  auto materialized = table;
  dataflow::materializeRows(&materialized);
  std::unordered_map<std::string, double> out;
  const auto window_idx = materialized.schema.indexOf("window_start");
  const auto key_idx = materialized.schema.indexOf("key");
  const auto value_idx = materialized.schema.indexOf("event_count");
  for (const auto& row : materialized.rows) {
    out[row[window_idx].toString() + "|" + row[key_idx].toString()] = row[value_idx].asDouble();
  }
  return out;
}

struct AggregateRow {
  int64_t sum = 0;
  int64_t count = 0;
  int64_t min = 0;
  int64_t max = 0;
  double avg = 0.0;
};

std::unordered_map<std::string, AggregateRow> multiAggregateTableToMap(const Table& table) {
  auto materialized = table;
  dataflow::materializeRows(&materialized);
  std::unordered_map<std::string, AggregateRow> out;
  const auto segment_idx = materialized.schema.indexOf("segment");
  const auto bucket_idx = materialized.schema.indexOf("bucket");
  const auto sum_idx = materialized.schema.indexOf("value_sum");
  const auto count_idx = materialized.schema.indexOf("event_count");
  const auto min_idx = materialized.schema.indexOf("min_value");
  const auto max_idx = materialized.schema.indexOf("max_value");
  const auto avg_idx = materialized.schema.indexOf("avg_value");
  for (const auto& row : materialized.rows) {
    out[row[segment_idx].toString() + "|" + row[bucket_idx].toString()] = AggregateRow{
        row[sum_idx].asInt64(),
        row[count_idx].asInt64(),
        row[min_idx].asInt64(),
        row[max_idx].asInt64(),
        row[avg_idx].asDouble(),
    };
  }
  return out;
}

void expectAggregateMapsEqual(const std::unordered_map<std::string, AggregateRow>& lhs,
                              const std::unordered_map<std::string, AggregateRow>& rhs,
                              const std::string& message) {
  expect(lhs.size() == rhs.size(), message + ": size");
  for (const auto& entry : lhs) {
    const auto it = rhs.find(entry.first);
    expect(it != rhs.end(), message + ": missing key " + entry.first);
    expect(entry.second.sum == it->second.sum, message + ": sum mismatch for " + entry.first);
    expect(entry.second.count == it->second.count, message + ": count mismatch for " + entry.first);
    expect(entry.second.min == it->second.min, message + ": min mismatch for " + entry.first);
    expect(entry.second.max == it->second.max, message + ": max mismatch for " + entry.first);
    expect(std::fabs(entry.second.avg - it->second.avg) < 1e-9,
           message + ": avg mismatch for " + entry.first);
  }
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
  session.createTempView(
      "strategy_generic_events",
      session.readStream(
          std::make_shared<MemoryStreamSource>(std::vector<Table>{makeGenericMultiAggregateBatch()})));
  session.sql(
      "CREATE SINK TABLE strategy_hot_sink (window_start STRING, key STRING, value_sum INT) "
      "USING csv OPTIONS(path '/tmp/velaria-stream-strategy-explain.csv', delimiter ',')");
  session.sql(
      "CREATE SINK TABLE strategy_count_sink (window_start STRING, key STRING, event_count INT) "
      "USING csv OPTIONS(path '/tmp/velaria-stream-strategy-count-explain.csv', delimiter ',')");
  session.sql(
      "CREATE SINK TABLE strategy_multi_sink "
      "(segment STRING, bucket INT, value_sum INT, event_count INT, min_value INT, max_value INT, avg_value DOUBLE) "
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
  expect(explain.find("sink=strategy_hot_sink") != std::string::npos,
         "explain should include sink binding");
  expect(explain.find("writes_to_sink=true") != std::string::npos,
         "physical explain should expose sink binding");
  expect(explain.find("Aggregate keys=[window_start, key]") != std::string::npos,
         "explain should describe aggregate keys");
  expect(explain.find("actor_eligible=true") != std::string::npos,
         "explain should expose actor eligibility");
  expect(explain.find("selected_mode=local-workers") != std::string::npos,
         "explain should expose the selected local-workers mode");
  expect(explain.find("shared_memory_transport=true") != std::string::npos,
         "explain should expose shared-memory knobs");
  expect(explain.find("simd_backend=") != std::string::npos,
         "explain should expose the active simd backend");
  expect(explain.find("compiled_backends=") != std::string::npos,
         "explain should expose compiled simd backends");

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
      "SELECT segment, bucket, SUM(value) AS value_sum, COUNT(*) AS event_count, "
      "MIN(value) AS min_value, MAX(value) AS max_value, AVG(value) AS avg_value "
      "FROM strategy_generic_events GROUP BY segment, bucket",
      options);
  expect(multi_explain.find("AVG(value) AS avg_value") != std::string::npos,
         "multi explain should describe avg aggregate");
  expect(multi_explain.find("Aggregate keys=[segment, bucket]") != std::string::npos,
         "multi explain should describe generic aggregate keys");
  expect(multi_explain.find("actor_eligible=true") != std::string::npos,
         "multi explain should enable actor eligibility for grouped multi aggregate");

  const std::string having_explain = session.explainStreamSql(
      "INSERT INTO strategy_hot_sink "
      "SELECT window_start, key, SUM(value) AS value_sum "
      "FROM strategy_hot_events GROUP BY window_start, key HAVING value_sum > 0",
      options);
  const std::string final_transform_reason =
      "actor acceleration requires the eligible grouped aggregate to be the final stream transform";
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

void testMultiAggregateHotPathWithGenericKeys() {
  DataflowSession& session = DataflowSession::builder();
  fs::remove("/tmp/velaria-stream-strategy-generic-single.csv");
  fs::remove("/tmp/velaria-stream-strategy-generic-local.csv");
  session.createTempView(
      "strategy_generic_multi_events_single",
      session.readStream(
          std::make_shared<MemoryStreamSource>(std::vector<Table>{makeGenericMultiAggregateBatch()})));
  session.createTempView(
      "strategy_generic_multi_events_local",
      session.readStream(
          std::make_shared<MemoryStreamSource>(std::vector<Table>{makeGenericMultiAggregateBatch()})));
  session.sql(
      "CREATE SINK TABLE strategy_generic_multi_single "
      "(segment STRING, bucket INT, value_sum INT, event_count INT, min_value INT, max_value INT, avg_value DOUBLE) "
      "USING csv OPTIONS(path '/tmp/velaria-stream-strategy-generic-single.csv', delimiter ',')");
  session.sql(
      "CREATE SINK TABLE strategy_generic_multi_local "
      "(segment STRING, bucket INT, value_sum INT, event_count INT, min_value INT, max_value INT, avg_value DOUBLE) "
      "USING csv OPTIONS(path '/tmp/velaria-stream-strategy-generic-local.csv', delimiter ',')");

  StreamingQueryOptions single_options;
  single_options.trigger_interval_ms = 0;
  single_options.execution_mode = StreamingExecutionMode::SingleProcess;

  StreamingQueryOptions local_options;
  local_options.trigger_interval_ms = 0;
  local_options.execution_mode = StreamingExecutionMode::LocalWorkers;
  local_options.local_workers = 4;
  local_options.max_inflight_partitions = 4;

  auto single_query = session.startStreamSql(
      "INSERT INTO strategy_generic_multi_single "
      "SELECT segment, bucket, SUM(value) AS value_sum, COUNT(*) AS event_count, "
      "MIN(value) AS min_value, MAX(value) AS max_value, AVG(value) AS avg_value "
      "FROM strategy_generic_multi_events_single GROUP BY segment, bucket",
      single_options);
  auto local_query = session.startStreamSql(
      "INSERT INTO strategy_generic_multi_local "
      "SELECT segment, bucket, SUM(value) AS value_sum, COUNT(*) AS event_count, "
      "MIN(value) AS min_value, MAX(value) AS max_value, AVG(value) AS avg_value "
      "FROM strategy_generic_multi_events_local GROUP BY segment, bucket",
      local_options);
  expect(single_query.awaitTermination(1) == 1, "single-process multi aggregate should process one batch");
  expect(local_query.awaitTermination(1) == 1, "local-workers multi aggregate should process one batch");

  const auto single_table = session.read_csv("/tmp/velaria-stream-strategy-generic-single.csv").toTable();
  const auto local_table = session.read_csv("/tmp/velaria-stream-strategy-generic-local.csv").toTable();
  expectAggregateMapsEqual(multiAggregateTableToMap(local_table),
                           multiAggregateTableToMap(single_table),
                           "generic multi aggregate output should match single-process");
  expect(local_query.progress().execution_mode == "local-workers",
         "generic multi aggregate should stay in local-workers mode");
  expect(local_query.progress().used_actor_runtime,
         "generic multi aggregate should use the credit accelerator");
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
                   .filter("event_count", ">", Value(int64_t(0)))
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
    testMultiAggregateHotPathWithGenericKeys();
    testLocalWorkersFallbackForNonHotPath();
    testLocalWorkersSingleWorkerDisablesCreditAcceleration();
    std::cout << "[test] stream strategy explain ok" << std::endl;
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "[test] stream strategy explain failed: " << ex.what() << std::endl;
    return 1;
  }
}
