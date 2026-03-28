#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

#include "src/dataflow/api/session.h"
#include "src/dataflow/stream/stream.h"

namespace {

class SlowSink : public dataflow::StreamSink {
 public:
  explicit SlowSink(uint64_t delay_ms) : delay_ms_(delay_ms) {}

  void write(const dataflow::Table& table) override {
    std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms_));
    std::lock_guard<std::mutex> lock(mu_);
    batches_.push_back(table);
  }

  size_t batches() const {
    std::lock_guard<std::mutex> lock(mu_);
    return batches_.size();
  }

 private:
  uint64_t delay_ms_;
  mutable std::mutex mu_;
  std::vector<dataflow::Table> batches_;
};

class CollectSink : public dataflow::StreamSink {
 public:
  void write(const dataflow::Table& table) override {
    std::lock_guard<std::mutex> lock(mu_);
    batches_.push_back(table);
  }

  std::vector<dataflow::Table> batches() const {
    std::lock_guard<std::mutex> lock(mu_);
    return batches_;
  }

 private:
  mutable std::mutex mu_;
  std::vector<dataflow::Table> batches_;
};

dataflow::Table makeWindowBatch(const std::vector<std::tuple<const char*, const char*, int64_t>>& rows) {
  dataflow::Table table;
  table.schema = dataflow::Schema({"ts", "key", "value"});
  for (const auto& row : rows) {
    table.rows.push_back({dataflow::Value(std::get<0>(row)), dataflow::Value(std::get<1>(row)),
                          dataflow::Value(std::get<2>(row))});
  }
  return table;
}

void expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

void testBackpressure() {
  dataflow::DataflowSession& session = dataflow::DataflowSession::builder();
  std::vector<dataflow::Table> batches;
  for (int i = 0; i < 4; ++i) {
    dataflow::Table batch;
    batch.schema = dataflow::Schema({"value"});
    batch.rows = {{dataflow::Value(int64_t(i))}, {dataflow::Value(int64_t(i + 10))}};
    batches.push_back(batch);
  }

  auto sink = std::make_shared<SlowSink>(60);
  dataflow::StreamingQueryOptions options;
  options.trigger_interval_ms = 0;
  options.max_inflight_batches = 1;
  options.max_queued_partitions = 1;
  options.backpressure_high_watermark = 1;
  options.backpressure_low_watermark = 0;

  auto query = session.readStream(std::make_shared<dataflow::MemoryStreamSource>(batches))
                   .filter("value", ">", dataflow::Value(int64_t(-1)))
                   .writeStream(sink, options);
  query.start();
  const size_t processed = query.awaitTermination();
  const auto progress = query.progress();

  expect(processed == 4, "backpressure test should process all batches");
  expect(progress.blocked_count > 0, "backpressure test should record blocking");
  expect(progress.max_backlog_batches <= 1, "backpressure backlog should remain bounded");
  expect(sink->batches() == 4, "slow sink should receive all batches");
}

void testStatefulWindowCount() {
  dataflow::DataflowSession& session = dataflow::DataflowSession::builder();
  auto sink = std::make_shared<CollectSink>();
  auto state = dataflow::makeMemoryStateStore();

  std::vector<dataflow::Table> batches = {
      makeWindowBatch({{"2026-03-28T09:00:00", "userA", 1},
                       {"2026-03-28T09:00:10", "userA", 1},
                       {"2026-03-28T09:00:30", "userB", 1}}),
      makeWindowBatch({{"2026-03-28T09:00:40", "userA", 1},
                       {"2026-03-28T09:00:50", "userB", 1}}),
  };

  auto query = session.readStream(std::make_shared<dataflow::MemoryStreamSource>(batches))
                   .withStateStore(state)
                   .window("ts", 60000, "window_start")
                   .groupBy({"window_start", "key"})
                   .count(true, "event_count")
                   .writeStream(sink, dataflow::StreamingQueryOptions{});
  query.start();
  const size_t processed = query.awaitTermination();
  const auto collected = sink->batches();

  expect(processed == 2, "window count test should process all batches");
  expect(collected.size() == 2, "window count sink should contain two outputs");
  const auto& last = collected.back();
  expect(last.schema.has("window_start"), "window output should contain window_start");
  expect(last.schema.has("event_count"), "window output should contain event_count");

  int64_t user_a = -1;
  int64_t user_b = -1;
  const auto key_idx = last.schema.indexOf("key");
  const auto count_idx = last.schema.indexOf("event_count");
  for (const auto& row : last.rows) {
    if (row[key_idx].toString() == "userA") user_a = row[count_idx].asInt64();
    if (row[key_idx].toString() == "userB") user_b = row[count_idx].asInt64();
  }
  expect(user_a == 3, "userA count should accumulate to 3");
  expect(user_b == 2, "userB count should accumulate to 2");
}

void testCheckpointRestore() {
  namespace fs = std::filesystem;
  const std::string checkpoint = "/tmp/velaria-stream-runtime-test.checkpoint";
  fs::remove(checkpoint);

  dataflow::DataflowSession& session = dataflow::DataflowSession::builder();
  std::vector<dataflow::Table> batches = {
      makeWindowBatch({{"2026-03-28T09:00:00", "userA", 1}}),
      makeWindowBatch({{"2026-03-28T09:00:10", "userA", 1}}),
      makeWindowBatch({{"2026-03-28T09:00:20", "userA", 1}}),
  };

  dataflow::StreamingQueryOptions options;
  options.trigger_interval_ms = 0;
  options.checkpoint_path = checkpoint;

  auto first = session.readStream(std::make_shared<dataflow::MemoryStreamSource>(batches))
                   .writeStream(std::make_shared<CollectSink>(), options);
  first.start();
  const size_t first_processed = first.awaitTermination(1);
  expect(first_processed == 1, "first checkpoint run should stop after one batch");

  auto second = session.readStream(std::make_shared<dataflow::MemoryStreamSource>(batches))
                    .writeStream(std::make_shared<CollectSink>(), options);
  second.start();
  const size_t second_processed = second.awaitTermination();
  const auto progress = second.progress();

  expect(second_processed == 2, "restored run should skip the first completed batch");
  expect(progress.batches_processed == 3, "restored progress should preserve total batches");
}

void testWindowEviction() {
  dataflow::DataflowSession& session = dataflow::DataflowSession::builder();
  auto state = dataflow::makeMemoryStateStore();

  std::vector<dataflow::Table> batches = {
      makeWindowBatch({{"2026-03-28T09:00:00", "userA", 1}}),
      makeWindowBatch({{"2026-03-28T09:01:05", "userA", 1}}),
      makeWindowBatch({{"2026-03-28T09:02:10", "userA", 1}}),
  };

  dataflow::StreamingQueryOptions options;
  options.trigger_interval_ms = 0;
  options.max_retained_windows = 1;

  auto query = session.readStream(std::make_shared<dataflow::MemoryStreamSource>(batches))
                   .withStateStore(state)
                   .window("ts", 60000, "window_start")
                   .groupBy({"window_start", "key"})
                   .sum("value", true, "value_sum")
                   .writeStream(std::make_shared<CollectSink>(), options);
  query.start();
  const size_t processed = query.awaitTermination();
  expect(processed == 3, "window eviction should process all batches");

  std::vector<std::string> state_keys;
  state->listKeysByPrefix("group_sum:", &state_keys);
  expect(state_keys.size() == 1, "window eviction should retain only latest window state");
  expect(state_keys.front().find("2026-03-28T09:02:00") != std::string::npos,
         "window eviction should keep newest window key");
}

}  // namespace

int main() {
  try {
    testBackpressure();
    testStatefulWindowCount();
    testCheckpointRestore();
    testWindowEviction();
    std::cout << "[test] stream runtime ok" << std::endl;
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "[test] stream runtime failed: " << ex.what() << std::endl;
    return 1;
  }
}
