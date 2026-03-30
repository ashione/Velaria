#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "src/dataflow/experimental/stream/actor_stream_runtime.h"

namespace {

std::vector<dataflow::Table> makeBatches() {
  std::vector<dataflow::Table> batches;
  for (size_t batch = 0; batch < 6; ++batch) {
    dataflow::Table table;
    table.schema = dataflow::Schema({"window_start", "key", "value"});
    const std::string window = "2026-03-28T09:0" + std::to_string(static_cast<int>(batch % 3));
    for (size_t row = 0; row < 64; ++row) {
      table.rows.push_back({dataflow::Value(window),
                            dataflow::Value("user_" + std::to_string(row % 8)),
                            dataflow::Value(int64_t((row % 5) + 1))});
    }
    batches.push_back(std::move(table));
  }
  return batches;
}

std::unordered_map<std::string, double> toMap(const dataflow::Table& table) {
  std::unordered_map<std::string, double> out;
  const auto window_idx = table.schema.indexOf("window_start");
  const auto key_idx = table.schema.indexOf("key");
  const auto value_idx = table.schema.indexOf("value_sum");
  for (const auto& row : table.rows) {
    out[row[window_idx].toString() + "|" + row[key_idx].toString()] = row[value_idx].asDouble();
  }
  return out;
}

void expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

}  // namespace

int main() {
  try {
    const auto batches = makeBatches();
    const auto baseline = dataflow::runSingleProcessWindowKeySum(batches);

    dataflow::LocalActorStreamOptions options;
    options.worker_count = 3;
    options.max_inflight_partitions = 2;
    options.worker_delay_ms = 20;
    const auto actor = dataflow::runLocalActorStreamWindowKeySum(batches, options);

    expect(actor.processed_batches == batches.size(), "actor result should process all batches");
    expect(actor.processed_partitions == batches.size() * options.worker_count,
           "actor result should process all partitions");
    expect(actor.max_inflight_partitions <= options.max_inflight_partitions,
           "credit controller should bound inflight partitions");
    expect(actor.blocked_count > 0, "credit controller should observe blocking under delay");
    expect(toMap(actor.final_table) == toMap(baseline), "actor result should match baseline aggregate");

    std::cout << "[test] stream actor credit ok" << std::endl;
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "[test] stream actor credit failed: " << ex.what() << std::endl;
    return 1;
  }
}
