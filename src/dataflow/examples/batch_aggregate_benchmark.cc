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

using dataflow::AggregateExecutionPattern;
using dataflow::AggregateFunction;
using dataflow::AggregateSpec;
using dataflow::Schema;
using dataflow::Table;
using dataflow::Value;

Table makeSingleInt64LowDomainTable(std::size_t rows) {
  Table table;
  table.schema = Schema({"k", "v"});
  table.rows.reserve(rows);
  for (std::size_t i = 0; i < rows; ++i) {
    table.rows.push_back({Value(static_cast<int64_t>(i % 1024)),
                          Value(static_cast<int64_t>((i % 13) + 1))});
  }
  return table;
}

Table makeSingleInt64HighDomainTable(std::size_t rows) {
  Table table;
  table.schema = Schema({"k", "v"});
  table.rows.reserve(rows);
  for (std::size_t i = 0; i < rows; ++i) {
    table.rows.push_back({Value(static_cast<int64_t>(i)),
                          Value(static_cast<int64_t>((i % 17) + 1))});
  }
  return table;
}

Table makeDoubleInt64Table(std::size_t rows) {
  Table table;
  table.schema = Schema({"k1", "k2", "v"});
  table.rows.reserve(rows);
  for (std::size_t i = 0; i < rows; ++i) {
    table.rows.push_back({Value(static_cast<int64_t>(i % 4096)),
                          Value(static_cast<int64_t>((i / 8) % 256)),
                          Value(static_cast<int64_t>((i % 11) + 1))});
  }
  return table;
}

Table makeMixedStringInt64Table(std::size_t rows) {
  Table table;
  table.schema = Schema({"ks", "ki", "v"});
  table.rows.reserve(rows);
  for (std::size_t i = 0; i < rows; ++i) {
    table.rows.push_back({Value("seg_" + std::to_string(i % 512)),
                          Value(static_cast<int64_t>((i / 16) % 1024)),
                          Value(static_cast<int64_t>((i % 7) + 1))});
  }
  return table;
}

Table makeOrderedStringTable(std::size_t rows) {
  Table table;
  table.schema = Schema({"k", "v"});
  table.rows.reserve(rows);
  for (std::size_t group = 0; group < rows / 32; ++group) {
    for (std::size_t i = 0; i < 32; ++i) {
      table.rows.push_back({Value("grp_" + std::to_string(group)),
                            Value(static_cast<int64_t>((i % 5) + 1))});
    }
  }
  return table;
}

void runScenario(const std::string& name, const Table& input, const std::vector<std::size_t>& keys,
                 const std::vector<AggregateSpec>& aggs, bool ordered_input, std::size_t rounds) {
  const AggregateExecutionPattern pattern =
      dataflow::analyzeAggregateExecution(input, keys, aggs, ordered_input, false);

  uint64_t best_ms = std::numeric_limits<uint64_t>::max();
  std::size_t output_groups = 0;
  for (std::size_t round = 0; round < rounds; ++round) {
    const auto started = std::chrono::steady_clock::now();
    const Table out = dataflow::executeAggregateTable(input, keys, aggs, &pattern.exec_spec);
    const auto elapsed_ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - started)
            .count());
    best_ms = std::min(best_ms, elapsed_ms);
    output_groups = out.rowCount();
  }

  const double rows_per_s =
      best_ms == 0 ? 0.0 : (static_cast<double>(input.rowCount()) / (static_cast<double>(best_ms) / 1000.0));
  std::cout << "[batch-aggregate-bench] scenario=" << name
            << " rows=" << input.rowCount()
            << " selected_impl=" << dataflow::aggregateExecKindName(pattern.exec_spec.impl_kind)
            << " runtime_shape=" << dataflow::aggregateExecutionShapeName(pattern.shape)
            << " ordered_input=" << (ordered_input ? "true" : "false")
            << " elapsed_ms=" << best_ms
            << " rows_per_s=" << rows_per_s
            << " output_groups=" << output_groups
            << std::endl;
}

}  // namespace

int main(int argc, char** argv) {
  std::size_t rows = 1 << 20;
  std::size_t rounds = 3;
  if (argc > 1) rows = static_cast<std::size_t>(std::strtoull(argv[1], nullptr, 10));
  if (argc > 2) rounds = static_cast<std::size_t>(std::strtoull(argv[2], nullptr, 10));

  runScenario("single-int64-low-domain", makeSingleInt64LowDomainTable(rows), {0},
              {AggregateSpec{AggregateFunction::Sum, 1, "sum_v"}}, false, rounds);
  runScenario("single-int64-high-domain", makeSingleInt64HighDomainTable(rows), {0},
              {AggregateSpec{AggregateFunction::Sum, 1, "sum_v"}}, false, rounds);
  runScenario("double-int64", makeDoubleInt64Table(rows), {0, 1},
              {AggregateSpec{AggregateFunction::Sum, 2, "sum_v"}}, false, rounds);
  runScenario("mixed-string-int64", makeMixedStringInt64Table(rows), {0, 1},
              {AggregateSpec{AggregateFunction::Sum, 2, "sum_v"}}, false, rounds);
  runScenario("ordered-string", makeOrderedStringTable(rows), {0},
              {AggregateSpec{AggregateFunction::Sum, 1, "sum_v"}}, true, rounds);
  return 0;
}
