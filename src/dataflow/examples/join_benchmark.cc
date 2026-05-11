#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include "src/dataflow/core/execution/runtime/executor.h"
#include "src/dataflow/core/logical/planner/plan.h"

namespace {

using dataflow::JoinKind;
using dataflow::JoinPlan;
using dataflow::Schema;
using dataflow::Table;
using dataflow::Value;

Table makeIntTable(std::size_t rows, std::size_t key_mod) {
  Table table;
  table.schema = Schema({"key", "value"});
  table.rows.reserve(rows);
  for (std::size_t i = 0; i < rows; ++i) {
    table.rows.push_back(
        {Value(static_cast<int64_t>(i % key_mod)),
         Value(static_cast<int64_t>((i * 7 + 3) % 1000))});
  }
  return table;
}

Table makeStringTable(std::size_t rows, std::size_t key_mod) {
  Table table;
  table.schema = Schema({"key", "value"});
  table.rows.reserve(rows);
  for (std::size_t i = 0; i < rows; ++i) {
    table.rows.push_back(
        {Value("key_" + std::to_string(i % key_mod)),
         Value(static_cast<double>((i * 13 + 7) % 500) * 0.5)});
  }
  return table;
}

void runJoinBench(const std::string& scenario, const Table& left, const Table& right,
                  std::size_t rounds) {
  auto plan = std::make_shared<JoinPlan>(
      std::make_shared<dataflow::SourcePlan>("left", left),
      std::make_shared<dataflow::SourcePlan>("right", right),
      0, 0, JoinKind::Inner);

  dataflow::LocalExecutor executor;
  uint64_t best_ms = std::numeric_limits<uint64_t>::max();
  std::size_t result_rows = 0;
  for (std::size_t round = 0; round < rounds; ++round) {
    const auto started = std::chrono::steady_clock::now();
    auto output = executor.execute(plan);
    const auto elapsed_ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - started)
            .count());
    best_ms = std::min(best_ms, elapsed_ms);
    result_rows = output.rowCount();
  }

  const std::size_t total_rows = left.rowCount() + right.rowCount();
  const double rows_per_s =
      best_ms == 0 ? 0.0
                   : (static_cast<double>(total_rows) /
                      (static_cast<double>(best_ms) / 1000.0));
  std::cout << "[join-bench] scenario=" << scenario
            << " left_rows=" << left.rowCount()
            << " right_rows=" << right.rowCount()
            << " result_rows=" << result_rows
            << " elapsed_ms=" << best_ms
            << " rows_per_s=" << rows_per_s
            << std::endl;
}

}  // namespace

int main(int argc, char** argv) {
  std::size_t rounds = 3;
  if (argc > 1) rounds = static_cast<std::size_t>(std::strtoull(argv[1], nullptr, 10));

  // Scenario 1: Small left, large right (swap benefits most)
  runJoinBench("small-left-large-right-int",
               makeIntTable(100, 10),       // 100 rows, 10 distinct keys
               makeIntTable(100000, 1000),  // 100K rows, 1K distinct keys
               rounds);

  // Scenario 2: Large left, small right (no swap, already optimal)
  runJoinBench("large-left-small-right-int",
               makeIntTable(100000, 1000),
               makeIntTable(100, 10),
               rounds);

  // Scenario 3: Equal size int-key join
  runJoinBench("equal-size-int",
               makeIntTable(50000, 1000),
               makeIntTable(50000, 1000),
               rounds);

  // Scenario 4: String key join (small right)
  runJoinBench("string-key-small-right",
               makeStringTable(50000, 2000),
               makeStringTable(1000, 200),
               rounds);

  // Scenario 5: String key join (small left, swap case)
  runJoinBench("string-key-small-left",
               makeStringTable(1000, 200),
               makeStringTable(50000, 2000),
               rounds);

  // Scenario 6: High-cardinality int join
  runJoinBench("high-cardinality-int",
               makeIntTable(10000, 100000),
               makeIntTable(5000, 50000),
               rounds);

  return 0;
}