#include <chrono>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "src/dataflow/core/contract/api/session.h"
#include "src/dataflow/core/execution/columnar_batch.h"
#include "src/dataflow/core/execution/stream/stream.h"

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

class PersistentStateStore : public dataflow::StateStore {
 public:
  explicit PersistentStateStore(std::string path) : path_(std::move(path)) { load(); }

  bool get(const std::string& key, std::string* out) const override {
    const auto it = values_.find(key);
    if (it == values_.end()) return false;
    if (out != nullptr) *out = it->second;
    return true;
  }

  void put(const std::string& key, const std::string& value) override {
    values_[key] = value;
    flush();
  }

  bool remove(const std::string& key) override {
    const bool removed = values_.erase(key) > 0;
    if (removed) flush();
    return removed;
  }

  bool getMapField(const std::string& mapKey, const std::string& field,
                   std::string* out) const override {
    const auto map_it = maps_.find(mapKey);
    if (map_it == maps_.end()) return false;
    const auto field_it = map_it->second.find(field);
    if (field_it == map_it->second.end()) return false;
    if (out != nullptr) *out = field_it->second;
    return true;
  }

  void putMapField(const std::string& mapKey, const std::string& field,
                   const std::string& value) override {
    maps_[mapKey][field] = value;
    flush();
  }

  bool removeMapField(const std::string& mapKey, const std::string& field) override {
    const auto map_it = maps_.find(mapKey);
    if (map_it == maps_.end()) return false;
    const bool removed = map_it->second.erase(field) > 0;
    if (map_it->second.empty()) {
      maps_.erase(map_it);
    }
    if (removed) flush();
    return removed;
  }

  bool getMapFields(const std::string& mapKey, std::vector<std::string>* fields) const override {
    if (fields == nullptr) return false;
    fields->clear();
    const auto map_it = maps_.find(mapKey);
    if (map_it == maps_.end()) return false;
    for (const auto& entry : map_it->second) {
      fields->push_back(entry.first);
    }
    return !fields->empty();
  }

  bool getValueList(const std::string& listKey, std::vector<std::string>* values) const override {
    if (values == nullptr) return false;
    values->clear();
    const auto it = lists_.find(listKey);
    if (it == lists_.end()) return false;
    *values = it->second;
    return true;
  }

  bool setValueList(const std::string& listKey,
                    const std::vector<std::string>& values) override {
    lists_[listKey] = values;
    flush();
    return true;
  }

  bool appendValueToList(const std::string& listKey, const std::string& value) override {
    lists_[listKey].push_back(value);
    flush();
    return true;
  }

  bool popValueFromList(const std::string& listKey, std::string* value) override {
    const auto it = lists_.find(listKey);
    if (it == lists_.end() || it->second.empty()) return false;
    if (value != nullptr) *value = it->second.back();
    it->second.pop_back();
    if (it->second.empty()) {
      lists_.erase(it);
    }
    flush();
    return true;
  }

  bool listKeys(std::vector<std::string>* keys) const override {
    if (keys == nullptr) return false;
    keys->clear();
    for (const auto& entry : values_) {
      keys->push_back(entry.first);
    }
    return !keys->empty();
  }

  void close() override { flush(); }

 private:
  void load() {
    std::ifstream in(path_);
    if (!in) return;
    std::string tag;
    while (in >> tag) {
      if (tag == "K") {
        std::string key;
        std::string value;
        in >> std::quoted(key) >> std::quoted(value);
        values_[key] = value;
      } else if (tag == "M") {
        std::string map_key;
        std::string field;
        std::string value;
        in >> std::quoted(map_key) >> std::quoted(field) >> std::quoted(value);
        maps_[map_key][field] = value;
      } else if (tag == "L") {
        std::string list_key;
        size_t size = 0;
        in >> std::quoted(list_key) >> size;
        auto& list = lists_[list_key];
        list.clear();
        for (size_t i = 0; i < size; ++i) {
          std::string value;
          in >> std::quoted(value);
          list.push_back(value);
        }
      } else {
        throw std::runtime_error("persistent state store encountered unknown record");
      }
    }
  }

  void flush() const {
    std::ofstream out(path_, std::ios::trunc);
    if (!out) {
      throw std::runtime_error("cannot write persistent state store: " + path_);
    }
    for (const auto& entry : values_) {
      out << "K " << std::quoted(entry.first) << " " << std::quoted(entry.second) << "\n";
    }
    for (const auto& map_entry : maps_) {
      for (const auto& field_entry : map_entry.second) {
        out << "M " << std::quoted(map_entry.first) << " " << std::quoted(field_entry.first)
            << " " << std::quoted(field_entry.second) << "\n";
      }
    }
    for (const auto& list_entry : lists_) {
      out << "L " << std::quoted(list_entry.first) << " " << list_entry.second.size();
      for (const auto& value : list_entry.second) {
        out << " " << std::quoted(value);
      }
      out << "\n";
    }
  }

  std::string path_;
  std::unordered_map<std::string, std::string> values_;
  std::unordered_map<std::string, std::unordered_map<std::string, std::string>> maps_;
  std::unordered_map<std::string, std::vector<std::string>> lists_;
};

std::vector<std::string> readNonEmptyLines(const std::string& path) {
  std::ifstream in(path);
  std::vector<std::string> lines;
  std::string line;
  while (std::getline(in, line)) {
    if (!line.empty()) {
      lines.push_back(line);
    }
  }
  return lines;
}

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

void expectContains(const std::string& haystack, const std::string& needle,
                    const std::string& message) {
  if (haystack.find(needle) == std::string::npos) {
    throw std::runtime_error(message + ": missing " + needle);
  }
}

void expectNear(double actual, double expected, double tolerance, const std::string& message) {
  if (std::abs(actual - expected) > tolerance) {
    throw std::runtime_error(message);
  }
}

double lookupMetric(const dataflow::Table& table, const std::string& output_column,
                    const std::string& user) {
  const auto key_idx = table.schema.indexOf("key");
  const auto value_idx = table.schema.indexOf(output_column);
  for (const auto& row : table.rows) {
    if (row[key_idx].toString() == user) {
      return row[value_idx].asDouble();
    }
  }
  throw std::runtime_error("missing metric row for user " + user);
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
  expect(progress.backpressure_high_watermark == 1, "backpressure snapshot should expose high watermark");
  expect(progress.backpressure_low_watermark == 0, "backpressure snapshot should expose low watermark");
  expect(progress.inflight_batches == 0, "backpressure test should drain queued inflight batches");
  expect(progress.inflight_partitions == 0, "backpressure test should drain queued inflight partitions");
  expect(!progress.backpressure_active, "backpressure should clear after backlog drains");
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
  expect(last.columnar_cache != nullptr, "window output should retain columnar cache");

  int64_t user_a = -1;
  int64_t user_b = -1;
  const auto key_idx = last.schema.indexOf("key");
  const auto count_idx = last.schema.indexOf("event_count");
  const auto cached_counts = dataflow::materializeValueColumn(last, count_idx);
  expect(cached_counts.values.size() == last.rows.size(), "cached event_count size mismatch");
  for (const auto& row : last.rows) {
    if (row[key_idx].toString() == "userA") user_a = row[count_idx].asInt64();
    if (row[key_idx].toString() == "userB") user_b = row[count_idx].asInt64();
  }
  expect(user_a == 3, "userA count should accumulate to 3");
  expect(user_b == 2, "userB count should accumulate to 2");
}

void testAggregateVariants() {
  dataflow::DataflowSession& session = dataflow::DataflowSession::builder();
  std::vector<dataflow::Table> batches = {
      makeWindowBatch({{"2026-03-28T09:00:00", "userA", 10},
                       {"2026-03-28T09:00:10", "userA", 5},
                       {"2026-03-28T09:00:20", "userB", 7}}),
      makeWindowBatch({{"2026-03-28T09:00:30", "userA", 3},
                       {"2026-03-28T09:00:40", "userB", 9}}),
  };

  const auto run_and_capture =
      [&](const std::function<dataflow::StreamingDataFrame(const dataflow::StreamingDataFrame&)>& build) {
        auto sink = std::make_shared<CollectSink>();
        auto query = build(session.readStream(std::make_shared<dataflow::MemoryStreamSource>(batches))
                               .withStateStore(dataflow::makeMemoryStateStore())
                               .window("ts", 60000, "window_start"))
                         .writeStream(sink, dataflow::StreamingQueryOptions{});
        query.start();
        expect(query.awaitTermination() == 2, "aggregate variant query should process all batches");
        const auto outputs = sink->batches();
        expect(outputs.size() == 2, "aggregate variant query should emit two batches");
        return outputs.back();
      };

  const auto sum_table = run_and_capture([](const dataflow::StreamingDataFrame& df) {
    return df.groupBy({"window_start", "key"}).sum("value", true, "value_sum");
  });
  const auto count_table = run_and_capture([](const dataflow::StreamingDataFrame& df) {
    return df.groupBy({"window_start", "key"}).count(true, "event_count");
  });
  const auto min_table = run_and_capture([](const dataflow::StreamingDataFrame& df) {
    return df.groupBy({"window_start", "key"}).min("value", true, "min_value");
  });
  const auto max_table = run_and_capture([](const dataflow::StreamingDataFrame& df) {
    return df.groupBy({"window_start", "key"}).max("value", true, "max_value");
  });
  const auto avg_table = run_and_capture([](const dataflow::StreamingDataFrame& df) {
    return df.groupBy({"window_start", "key"}).avg("value", true, "avg_value");
  });

  expect(lookupMetric(sum_table, "value_sum", "userA") == 18.0,
         "sum aggregate should keep userA running sum");
  expect(lookupMetric(sum_table, "value_sum", "userB") == 16.0,
         "sum aggregate should keep userB running sum");
  expect(lookupMetric(count_table, "event_count", "userA") == 3.0,
         "count aggregate should keep userA running count");
  expect(lookupMetric(count_table, "event_count", "userB") == 2.0,
         "count aggregate should keep userB running count");
  expect(lookupMetric(min_table, "min_value", "userA") == 3.0,
         "min aggregate should keep userA running min");
  expect(lookupMetric(min_table, "min_value", "userB") == 7.0,
         "min aggregate should keep userB running min");
  expect(lookupMetric(max_table, "max_value", "userA") == 10.0,
         "max aggregate should keep userA running max");
  expect(lookupMetric(max_table, "max_value", "userB") == 9.0,
         "max aggregate should keep userB running max");
  expect(lookupMetric(avg_table, "avg_value", "userA") == 6.0,
         "avg aggregate should keep userA running avg");
  expect(lookupMetric(avg_table, "avg_value", "userB") == 8.0,
         "avg aggregate should keep userB running avg");
}

void testCheckpointRestoreAggregateVariants() {
  namespace fs = std::filesystem;
  dataflow::DataflowSession& session = dataflow::DataflowSession::builder();
  const std::vector<dataflow::Table> batches = {
      makeWindowBatch({{"2026-03-28T12:00:00", "userA", 10}}),
      makeWindowBatch({{"2026-03-28T12:00:10", "userA", 4}}),
      makeWindowBatch({{"2026-03-28T12:00:20", "userA", 8}}),
  };

  const auto run_checkpoint_restore =
      [&](const std::string& checkpoint, const std::string& state_path,
          const std::function<dataflow::StreamingDataFrame(const dataflow::StreamingDataFrame&)>& build) {
        fs::remove(checkpoint);
        fs::remove(state_path);
        dataflow::StreamingQueryOptions options;
        options.trigger_interval_ms = 0;
        options.checkpoint_path = checkpoint;
        options.checkpoint_delivery_mode = dataflow::CheckpointDeliveryMode::BestEffort;

        auto first_state = std::make_shared<PersistentStateStore>(state_path);
        auto first = build(session.readStream(std::make_shared<dataflow::MemoryStreamSource>(batches))
                               .withStateStore(first_state)
                               .window("ts", 60000, "window_start"))
                         .writeStream(std::make_shared<CollectSink>(), options);
        first.start();
        expect(first.awaitTermination(1) == 1,
               "aggregate restore first run should stop after one batch");
        first_state->close();
        first_state.reset();

        auto second_sink = std::make_shared<CollectSink>();
        auto second_state = std::make_shared<PersistentStateStore>(state_path);
        auto second = build(session.readStream(std::make_shared<dataflow::MemoryStreamSource>(batches))
                                .withStateStore(second_state)
                                .window("ts", 60000, "window_start"))
                          .writeStream(second_sink, options);
        second.start();
        expect(second.awaitTermination() == 2,
               "aggregate restore second run should resume remaining batches");
        second_state->close();
        const auto outputs = second_sink->batches();
        expect(!outputs.empty(), "aggregate restore should emit output after resume");
        return outputs.back();
      };

  const auto min_table = run_checkpoint_restore(
      "/tmp/velaria-stream-runtime-test-min.checkpoint",
      "/tmp/velaria-stream-runtime-test-min.state",
      [](const dataflow::StreamingDataFrame& df) {
        return df.groupBy({"window_start", "key"}).min("value", true, "min_value");
      });
  expect(lookupMetric(min_table, "min_value", "userA") == 4.0,
         "min aggregate should restore its running minimum");

  const auto max_table = run_checkpoint_restore(
      "/tmp/velaria-stream-runtime-test-max.checkpoint",
      "/tmp/velaria-stream-runtime-test-max.state",
      [](const dataflow::StreamingDataFrame& df) {
        return df.groupBy({"window_start", "key"}).max("value", true, "max_value");
      });
  expect(lookupMetric(max_table, "max_value", "userA") == 10.0,
         "max aggregate should restore its running maximum");

  const auto avg_table = run_checkpoint_restore(
      "/tmp/velaria-stream-runtime-test-avg.checkpoint",
      "/tmp/velaria-stream-runtime-test-avg.state",
      [](const dataflow::StreamingDataFrame& df) {
        return df.groupBy({"window_start", "key"}).avg("value", true, "avg_value");
      });
  expectNear(lookupMetric(avg_table, "avg_value", "userA"), 22.0 / 3.0, 1e-9,
             "avg aggregate should restore its running average");
}

void testCheckpointRestoreAtLeastOnceDefault() {
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

  expect(second_processed == 3, "at-least-once restore should replay from source");
  expect(progress.batches_processed >= 4, "at-least-once progress should include replayed batches");
  expect(progress.last_source_offset == "3", "at-least-once restore should finish at the final source offset");
}


void testCheckpointRestoreBestEffort() {
  namespace fs = std::filesystem;
  const std::string checkpoint = "/tmp/velaria-stream-runtime-test-best-effort.checkpoint";
  fs::remove(checkpoint);

  dataflow::DataflowSession& session = dataflow::DataflowSession::builder();
  std::vector<dataflow::Table> batches = {
      makeWindowBatch({{"2026-03-28T10:00:00", "userA", 1}}),
      makeWindowBatch({{"2026-03-28T10:00:10", "userA", 1}}),
      makeWindowBatch({{"2026-03-28T10:00:20", "userA", 1}}),
  };

  dataflow::StreamingQueryOptions options;
  options.trigger_interval_ms = 0;
  options.checkpoint_path = checkpoint;
  options.checkpoint_delivery_mode = dataflow::CheckpointDeliveryMode::BestEffort;

  auto first = session.readStream(std::make_shared<dataflow::MemoryStreamSource>(batches))
                   .writeStream(std::make_shared<CollectSink>(), options);
  first.start();
  const size_t first_processed = first.awaitTermination(1);
  expect(first_processed == 1, "best-effort first run should stop after one batch");

  auto second = session.readStream(std::make_shared<dataflow::MemoryStreamSource>(batches))
                    .writeStream(std::make_shared<CollectSink>(), options);
  second.start();
  const size_t second_processed = second.awaitTermination();
  const auto progress = second.progress();

  expect(second_processed == 2, "best-effort restore should skip first completed batch");
  expect(progress.checkpoint_delivery_mode == "best-effort",
         "best-effort progress should expose checkpoint mode");
  expect(progress.last_source_offset == "3", "best-effort restore should finish at the final source offset");
}

void testCheckpointRestoreDuplicatesSinkOutputAtLeastOnce() {
  namespace fs = std::filesystem;
  const std::string checkpoint = "/tmp/velaria-stream-runtime-test-duplicates.checkpoint";
  const std::string sink_path = "/tmp/velaria-stream-runtime-test-duplicates.csv";
  fs::remove(checkpoint);
  fs::remove(sink_path);

  dataflow::DataflowSession& session = dataflow::DataflowSession::builder();
  std::vector<dataflow::Table> batches = {
      makeWindowBatch({{"2026-03-28T11:00:00", "userA", 1}}),
      makeWindowBatch({{"2026-03-28T11:00:10", "userB", 2}}),
      makeWindowBatch({{"2026-03-28T11:00:20", "userC", 3}}),
  };

  dataflow::StreamingQueryOptions options;
  options.trigger_interval_ms = 0;
  options.checkpoint_path = checkpoint;

  auto first = session.readStream(std::make_shared<dataflow::MemoryStreamSource>(batches))
                   .writeStream(std::make_shared<dataflow::FileAppendStreamSink>(sink_path), options);
  first.start();
  expect(first.awaitTermination(1) == 1, "duplicate-output first run should stop after one batch");

  auto second = session.readStream(std::make_shared<dataflow::MemoryStreamSource>(batches))
                    .writeStream(std::make_shared<dataflow::FileAppendStreamSink>(sink_path), options);
  second.start();
  expect(second.awaitTermination() == 3, "duplicate-output second run should replay all batches");

  const auto lines = readNonEmptyLines(sink_path);
  expect(lines.size() == 5, "at-least-once replay should leave header plus four data rows");
  expect(lines[1] == lines[2], "at-least-once replay should duplicate the first sink output");
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

void testWindowEvictionAggregateVariants() {
  dataflow::DataflowSession& session = dataflow::DataflowSession::builder();
  const std::vector<dataflow::Table> batches = {
      makeWindowBatch({{"2026-03-28T13:00:00", "userA", 10}}),
      makeWindowBatch({{"2026-03-28T13:01:05", "userA", 4}}),
      makeWindowBatch({{"2026-03-28T13:02:10", "userA", 8}}),
  };

  const auto run_eviction =
      [&](const std::shared_ptr<dataflow::StateStore>& state,
          const std::function<dataflow::StreamingDataFrame(const dataflow::StreamingDataFrame&)>& build,
          const std::string& prefix) {
        dataflow::StreamingQueryOptions options;
        options.trigger_interval_ms = 0;
        options.max_retained_windows = 1;

        auto query = build(session.readStream(std::make_shared<dataflow::MemoryStreamSource>(batches))
                               .withStateStore(state)
                               .window("ts", 60000, "window_start"))
                         .writeStream(std::make_shared<CollectSink>(), options);
        query.start();
        expect(query.awaitTermination() == 3, "aggregate eviction query should process all batches");

        std::vector<std::string> state_keys;
        state->listKeysByPrefix(prefix, &state_keys);
        expect(state_keys.size() == 1,
               "aggregate eviction should retain only the latest window state");
        expect(state_keys.front().find("2026-03-28T13:02:00") != std::string::npos,
               "aggregate eviction should keep the newest window key");
      };

  run_eviction(dataflow::makeMemoryStateStore(),
               [](const dataflow::StreamingDataFrame& df) {
                 return df.groupBy({"window_start", "key"}).min("value", true, "min_value");
               },
               "group_min:min_value");
  run_eviction(dataflow::makeMemoryStateStore(),
               [](const dataflow::StreamingDataFrame& df) {
                 return df.groupBy({"window_start", "key"}).max("value", true, "max_value");
               },
               "group_max:max_value");
  run_eviction(dataflow::makeMemoryStateStore(),
               [](const dataflow::StreamingDataFrame& df) {
                 return df.groupBy({"window_start", "key"}).avg("value", true, "avg_value");
               },
               "group_avg:avg_value");
}

void testSnapshotJsonContract() {
  dataflow::DataflowSession& session = dataflow::DataflowSession::builder();
  auto sink = std::make_shared<CollectSink>();

  dataflow::Table batch;
  batch.schema = dataflow::Schema({"ts", "key", "value"});
  batch.rows = {
      {dataflow::Value("2026-03-28T12:00:00"), dataflow::Value("userA"), dataflow::Value(int64_t(1))},
      {dataflow::Value("2026-03-28T12:00:10"), dataflow::Value("userB"), dataflow::Value(int64_t(2))},
  };

  dataflow::StreamingQueryOptions options;
  options.trigger_interval_ms = 0;
  options.checkpoint_delivery_mode = dataflow::CheckpointDeliveryMode::BestEffort;

  auto query = session.readStream(std::make_shared<dataflow::MemoryStreamSource>(std::vector<dataflow::Table>{batch}))
                   .writeStream(sink, options);
  query.start();
  expect(query.awaitTermination() == 1, "snapshot contract query should process one batch");

  const std::string snapshot = query.snapshotJson();
  expectContains(snapshot, "\"query_id\":", "snapshot should expose query_id");
  expectContains(snapshot, "\"status\":", "snapshot should expose status");
  expectContains(snapshot, "\"requested_execution_mode\":",
                 "snapshot should expose requested_execution_mode");
  expectContains(snapshot, "\"execution_mode\":", "snapshot should expose execution_mode");
  expectContains(snapshot, "\"execution_reason\":", "snapshot should expose execution_reason");
  expectContains(snapshot, "\"transport_mode\":", "snapshot should expose transport_mode");
  expectContains(snapshot, "\"blocked_count\":", "snapshot should expose blocked_count");
  expectContains(snapshot, "\"max_backlog_batches\":", "snapshot should expose max_backlog_batches");
  expectContains(snapshot, "\"inflight_batches\":", "snapshot should expose inflight_batches");
  expectContains(snapshot, "\"inflight_partitions\":", "snapshot should expose inflight_partitions");
  expectContains(snapshot, "\"last_batch_latency_ms\":", "snapshot should expose last_batch_latency_ms");
  expectContains(snapshot, "\"last_sink_latency_ms\":", "snapshot should expose last_sink_latency_ms");
  expectContains(snapshot, "\"last_state_latency_ms\":", "snapshot should expose last_state_latency_ms");
  expectContains(snapshot, "\"last_source_offset\":", "snapshot should expose last_source_offset");
  expectContains(snapshot, "\"backpressure_active\":", "snapshot should expose backpressure_active");
  expectContains(snapshot, "\"actor_eligible\":", "snapshot should expose actor_eligible");
  expectContains(snapshot, "\"used_actor_runtime\":", "snapshot should expose used_actor_runtime");
  expectContains(snapshot, "\"used_shared_memory\":", "snapshot should expose used_shared_memory");
  expectContains(snapshot, "\"has_stateful_ops\":", "snapshot should expose has_stateful_ops");
  expectContains(snapshot, "\"has_window\":", "snapshot should expose has_window");
  expectContains(snapshot, "\"sink_is_blocking\":", "snapshot should expose sink_is_blocking");
  expectContains(snapshot, "\"source_is_bounded\":", "snapshot should expose source_is_bounded");
  expectContains(snapshot, "\"estimated_partitions\":", "snapshot should expose estimated_partitions");
  expectContains(snapshot, "\"projected_payload_bytes\":", "snapshot should expose projected_payload_bytes");
  expectContains(snapshot, "\"sampled_batches\":", "snapshot should expose sampled_batches");
  expectContains(snapshot, "\"sampled_rows_per_batch\":",
                 "snapshot should expose sampled_rows_per_batch");
  expectContains(snapshot, "\"average_projected_payload_bytes\":",
                 "snapshot should expose average_projected_payload_bytes");
  expectContains(snapshot, "\"actor_speedup\":", "snapshot should expose actor_speedup");
  expectContains(snapshot, "\"compute_to_overhead_ratio\":",
                 "snapshot should expose compute_to_overhead_ratio");
  expectContains(snapshot, "\"estimated_state_size_bytes\":",
                 "snapshot should expose estimated_state_size_bytes");
  expectContains(snapshot, "\"estimated_batch_cost\":", "snapshot should expose estimated_batch_cost");
  expectContains(snapshot, "\"backpressure_max_queue_batches\":",
                 "snapshot should expose backpressure_max_queue_batches");
  expectContains(snapshot, "\"backpressure_high_watermark\":",
                 "snapshot should expose backpressure_high_watermark");
  expectContains(snapshot, "\"backpressure_low_watermark\":",
                 "snapshot should expose backpressure_low_watermark");
  expectContains(snapshot, "\"checkpoint_delivery_mode\":\"best-effort\"",
                 "snapshot should expose checkpoint_delivery_mode");
}

void testLocalWorkersRetainColumnarCacheAcrossPartitions() {
  dataflow::DataflowSession& session = dataflow::DataflowSession::builder();
  auto sink = std::make_shared<CollectSink>();

  const std::vector<dataflow::Table> batches = {
      makeWindowBatch({{"2026-03-28T14:00:00", "userA", 1},
                       {"2026-03-28T14:00:05", "userA", 1},
                       {"2026-03-28T14:00:20", "userB", 1},
                       {"2026-03-28T14:00:25", "userB", 1}}),
  };

  dataflow::StreamingQueryOptions options;
  options.trigger_interval_ms = 0;
  options.local_workers = 2;

  auto query = session.readStream(std::make_shared<dataflow::MemoryStreamSource>(batches))
                   .withColumn("key_copy", "key")
                   .window("ts", 60000, "window_start")
                   .groupBy({"window_start", "key_copy"})
                   .count(true, "event_count")
                   .writeStream(sink, options);
  query.start();
  expect(query.awaitTermination() == 1,
         "local worker cache test should process one batch");

  const auto outputs = sink->batches();
  expect(outputs.size() == 1, "local worker cache test should emit one batch");
  const auto& table = outputs.front();
  expect(table.schema.has("window_start"),
         "local worker cache test should keep window_start column");
  expect(table.schema.has("key_copy"),
         "local worker cache test should keep copied key column");
  expect(table.schema.has("event_count"),
         "local worker cache test should produce event_count column");
  expect(table.columnar_cache != nullptr,
         "local worker cache test should retain output columnar cache");

  const auto key_idx = table.schema.indexOf("key_copy");
  const auto count_idx = table.schema.indexOf("event_count");
  const auto cached_counts = dataflow::materializeValueColumn(table, count_idx);
  expect(cached_counts.values.size() == table.rows.size(),
         "local worker cached event_count size mismatch");
  int64_t user_a = -1;
  int64_t user_b = -1;
  for (const auto& row : table.rows) {
    if (row[key_idx].toString() == "userA") user_a = row[count_idx].asInt64();
    if (row[key_idx].toString() == "userB") user_b = row[count_idx].asInt64();
  }
  expect(user_a == 2, "local worker cache test should keep userA count");
  expect(user_b == 2, "local worker cache test should keep userB count");
}

}  // namespace

int main() {
  try {
    testBackpressure();
    testStatefulWindowCount();
    testAggregateVariants();
    testCheckpointRestoreAggregateVariants();
    testCheckpointRestoreAtLeastOnceDefault();
    testCheckpointRestoreBestEffort();
    testCheckpointRestoreDuplicatesSinkOutputAtLeastOnce();
    testWindowEviction();
    testWindowEvictionAggregateVariants();
    testSnapshotJsonContract();
    testLocalWorkersRetainColumnarCacheAcrossPartitions();
    std::cout << "[test] stream runtime ok" << std::endl;
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "[test] stream runtime failed: " << ex.what() << std::endl;
    return 1;
  }
}
