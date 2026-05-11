#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include "src/dataflow/core/execution/runtime/execution_optimizer.h"
#include "src/dataflow/core/execution/runtime/executor.h"

namespace {

using dataflow::AggregateFunction;
using dataflow::AggregateSpec;
using dataflow::Schema;
using dataflow::Table;
using dataflow::Value;

Table makeTwoKeyMixTable(std::size_t rows) {
  // int64 + string mixed keys — exercises PackedKeys2
  Table table;
  table.schema = Schema({"ki", "ks", "v"});
  table.rows.reserve(rows);
  for (std::size_t i = 0; i < rows; ++i) {
    table.rows.push_back(
        {Value(static_cast<int64_t>(i % 512)),
         Value("cat_" + std::to_string((i / 16) % 256)),
         Value(static_cast<int64_t>((i % 23) + 1))});
  }
  return table;
}

Table makeThreeKeyMixTable(std::size_t rows) {
  // int64 + string + double — exercises PackedKeys3 with new double support
  Table table;
  table.schema = Schema({"ki", "ks", "kd", "v"});
  table.rows.reserve(rows);
  for (std::size_t i = 0; i < rows; ++i) {
    table.rows.push_back(
        {Value(static_cast<int64_t>(i % 256)),
         Value("bucket_" + std::to_string((i / 32) % 128)),
         Value(static_cast<double>((i % 50)) * 0.25),
         Value(static_cast<int64_t>((i % 19) + 1))});
  }
  return table;
}

Table makeTwoKeyStringTable(std::size_t rows) {
  // Two string keys — exercises PackedKeys2 with string+string
  Table table;
  table.schema = Schema({"ks1", "ks2", "v"});
  table.rows.reserve(rows);
  for (std::size_t i = 0; i < rows; ++i) {
    table.rows.push_back(
        {Value("region_" + std::to_string(i % 64)),
         Value("city_" + std::to_string((i / 8) % 256)),
         Value(static_cast<int64_t>((i % 31) + 1))});
  }
  return table;
}

Table makeSerializedKeyFallbackTable(std::size_t rows) {
  // Large cardinality string keys — exercises GenericSerializedKeys path
  Table table;
  table.schema = Schema({"k1", "k2", "k3", "v"});
  table.rows.reserve(rows);
  for (std::size_t i = 0; i < rows; ++i) {
    table.rows.push_back(
        {Value("product_" + std::to_string(i % 2048)),
         Value("warehouse_" + std::to_string((i / 16) % 1024)),
         Value("batch_" + std::to_string((i / 256) % 128)),
         Value(static_cast<int64_t>((i % 37) + 1))});
  }
  return table;
}

void runAggKeyBench(const std::string& scenario, const Table& input,
                    const std::vector<std::size_t>& key_indices,
                    const std::vector<AggregateSpec>& aggs,
                    std::size_t rounds) {
  auto pattern =
      dataflow::analyzeAggregateExecution(input, key_indices, aggs, false, false);

  uint64_t best_ms = std::numeric_limits<uint64_t>::max();
  std::size_t output_groups = 0;
  for (std::size_t round = 0; round < rounds; ++round) {
    const auto started = std::chrono::steady_clock::now();
    const Table out = dataflow::executeAggregateTable(input, key_indices, aggs,
                                                       &pattern.exec_spec);
    const auto elapsed_ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - started)
            .count());
    best_ms = std::min(best_ms, elapsed_ms);
    output_groups = out.rowCount();
  }

  const double rows_per_s =
      best_ms == 0 ? 0.0
                   : (static_cast<double>(input.rowCount()) /
                      (static_cast<double>(best_ms) / 1000.0));
  std::cout << "[agg-key-bench] scenario=" << scenario
            << " rows=" << input.rowCount()
            << " key_count=" << key_indices.size()
            << " selected_impl="
            << dataflow::aggregateExecKindName(pattern.exec_spec.impl_kind)
            << " runtime_shape="
            << dataflow::aggregateExecutionShapeName(pattern.shape)
            << " elapsed_ms=" << best_ms
            << " rows_per_s=" << rows_per_s
            << " output_groups=" << output_groups
            << std::endl;
}

}  // namespace

int main(int argc, char** argv) {
  std::size_t rows = 1 << 20;
  std::size_t rounds = 3;
  if (argc > 1)
    rows = static_cast<std::size_t>(std::strtoull(argv[1], nullptr, 10));
  if (argc > 2)
    rounds = static_cast<std::size_t>(std::strtoull(argv[2], nullptr, 10));

  // PackedKeys2: int64 + string
  runAggKeyBench("packed2-int-string", makeTwoKeyMixTable(rows), {0, 1},
                 {AggregateSpec{AggregateFunction::Sum, 2, "sum_v"}}, rounds);

  // PackedKeys3: int64 + string + double
  runAggKeyBench("packed3-int-string-double", makeThreeKeyMixTable(rows), {0, 1, 2},
                 {AggregateSpec{AggregateFunction::Sum, 3, "sum_v"}}, rounds);

  // PackedKeys2: string + string
  runAggKeyBench("packed2-string-string", makeTwoKeyStringTable(rows), {0, 1},
                 {AggregateSpec{AggregateFunction::Sum, 2, "sum_v"}}, rounds);

  // GenericSerializedKeys fallback: 3 high-cardinality strings
  runAggKeyBench(
      "serialized-3-string", makeSerializedKeyFallbackTable(rows), {0, 1, 2},
      {AggregateSpec{AggregateFunction::Sum, 3, "sum_v"}}, rounds);

  return 0;
}