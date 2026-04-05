#include <cmath>
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
    table.schema = dataflow::Schema({"segment", "bucket", "value"});
    const int64_t bucket = static_cast<int64_t>((batch % 3) + 1);
    for (size_t row = 0; row < 64; ++row) {
      table.rows.push_back({dataflow::Value("group_" + std::to_string(row % 4)),
                            dataflow::Value(bucket),
                            dataflow::Value(int64_t((row % 5) + 1))});
    }
    batches.push_back(std::move(table));
  }
  return batches;
}

struct AggregateRow {
  int64_t sum = 0;
  int64_t count = 0;
  int64_t min = 0;
  int64_t max = 0;
  double avg = 0.0;
};

std::unordered_map<std::string, AggregateRow> toMap(const dataflow::Table& table) {
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

void expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

void expectAggregateMapsEqual(const std::unordered_map<std::string, AggregateRow>& lhs,
                              const std::unordered_map<std::string, AggregateRow>& rhs,
                              const std::string& message) {
  expect(lhs.size() == rhs.size(), message + ": size");
  for (const auto& entry : lhs) {
    const auto it = rhs.find(entry.first);
    expect(it != rhs.end(), message + ": missing key");
    expect(entry.second.sum == it->second.sum, message + ": sum");
    expect(entry.second.count == it->second.count, message + ": count");
    expect(entry.second.min == it->second.min, message + ": min");
    expect(entry.second.max == it->second.max, message + ": max");
    expect(std::fabs(entry.second.avg - it->second.avg) < 1e-9, message + ": avg");
  }
}

}  // namespace

int main() {
  try {
    const auto batches = makeBatches();
    dataflow::LocalGroupedAggregateSpec aggregate;
    aggregate.group_keys = {"segment", "bucket"};
    aggregate.aggregates = {
        {dataflow::AggregateFunction::Sum, "value", "value_sum", false},
        {dataflow::AggregateFunction::Count, "", "event_count", true},
        {dataflow::AggregateFunction::Min, "value", "min_value", false},
        {dataflow::AggregateFunction::Max, "value", "max_value", false},
        {dataflow::AggregateFunction::Avg, "value", "avg_value", false},
    };

    const auto baseline = dataflow::runSingleProcessGroupedAggregate(batches, aggregate);

    dataflow::LocalActorStreamOptions options;
    options.worker_count = 3;
    options.max_inflight_partitions = 2;
    options.worker_delay_ms = 20;
    const auto actor = dataflow::runLocalActorStreamGroupedAggregate(batches, aggregate, options);

    expect(actor.processed_batches == batches.size(), "actor result should process all batches");
    expect(actor.processed_partitions == batches.size() * options.worker_count,
           "actor result should process all partitions");
    expect(actor.max_inflight_partitions <= options.max_inflight_partitions,
           "credit controller should bound inflight partitions");
    expect(actor.blocked_count > 0, "credit controller should observe blocking under delay");
    expectAggregateMapsEqual(toMap(actor.final_table), toMap(baseline),
                             "actor result should match grouped aggregate baseline");

    std::cout << "[test] stream actor credit ok" << std::endl;
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "[test] stream actor credit failed: " << ex.what() << std::endl;
    return 1;
  }
}
