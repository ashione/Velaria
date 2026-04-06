#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "src/dataflow/core/contract/api/session.h"
#include "src/dataflow/experimental/stream/actor_stream_runtime.h"

namespace {

std::vector<dataflow::Table> makeStringKeyBatches(size_t batch_count, size_t rows_per_batch) {
  std::vector<dataflow::Table> batches;
  batches.reserve(batch_count);

  static const char* return_flags[] = {"A", "N", "R"};
  static const char* line_statuses[] = {"F", "O"};

  for (size_t batch = 0; batch < batch_count; ++batch) {
    dataflow::Table table;
    table.schema = dataflow::Schema(
        {"window_start", "key", "value", "extendedprice", "discount", "tax", "comment"});
    table.rows.reserve(rows_per_batch);
    for (size_t row = 0; row < rows_per_batch; ++row) {
      const std::string return_flag = return_flags[(batch + row) % 3];
      const std::string line_status = line_statuses[(row / 7) % 2];
      const int64_t quantity = static_cast<int64_t>((row % 50) + 1);
      const double extendedprice = 100.0 + static_cast<double>(row % 1000) * 0.25;
      const double discount = static_cast<double>(row % 10) / 100.0;
      const double tax = static_cast<double>(row % 8) / 100.0;
      const std::string comment = "lineitem-comment-" + std::to_string(batch) + "-" +
                                  std::to_string(row % 4096);
      table.rows.push_back(
          {dataflow::Value(return_flag), dataflow::Value(line_status), dataflow::Value(quantity),
           dataflow::Value(extendedprice), dataflow::Value(discount), dataflow::Value(tax),
           dataflow::Value(comment)});
    }
    batches.push_back(std::move(table));
  }
  return batches;
}

std::vector<dataflow::Table> makeNumericKeyBatches(size_t batch_count, size_t rows_per_batch) {
  std::vector<dataflow::Table> batches;
  batches.reserve(batch_count);

  for (size_t batch = 0; batch < batch_count; ++batch) {
    dataflow::Table table;
    table.schema = dataflow::Schema(
        {"window_start", "key", "value", "extendedprice", "discount", "tax", "comment"});
    table.rows.reserve(rows_per_batch);
    for (size_t row = 0; row < rows_per_batch; ++row) {
      const int64_t window_start =
          static_cast<int64_t>(1710000000000LL + static_cast<int64_t>(((batch / 2) + (row / 512)) * 60000));
      const int64_t key = static_cast<int64_t>((batch + row) % 128);
      const double value = 1.0 + static_cast<double>(row % 50);
      const double extendedprice = 100.0 + static_cast<double>(row % 1000) * 0.25;
      const double discount = static_cast<double>(row % 10) / 100.0;
      const double tax = static_cast<double>(row % 8) / 100.0;
      const std::string comment = "numeric-bench-" + std::to_string(batch) + "-" +
                                  std::to_string(row % 4096);
      table.rows.push_back(
          {dataflow::Value(window_start), dataflow::Value(key), dataflow::Value(value),
           dataflow::Value(extendedprice), dataflow::Value(discount), dataflow::Value(tax),
           dataflow::Value(comment)});
    }
    batches.push_back(std::move(table));
  }
  return batches;
}

std::vector<dataflow::Table> makeLineitemQ1ShapeBatches(const std::string& scenario,
                                                        size_t batch_count,
                                                        size_t rows_per_batch) {
  if (scenario == "numeric-keys") {
    return makeNumericKeyBatches(batch_count, rows_per_batch);
  }
  return makeStringKeyBatches(batch_count, rows_per_batch);
}

dataflow::Table makeQ6LikeLineitemTable(size_t total_rows) {
  dataflow::Table table;
  table.schema =
      dataflow::Schema({"shipdate", "discount", "quantity", "revenue", "tax", "comment"});
  table.rows.reserve(total_rows);
  for (size_t row = 0; row < total_rows; ++row) {
    const int64_t shipdate = static_cast<int64_t>(19940101 + (row % 365));
    const double discount = 0.02 + static_cast<double>(row % 8) * 0.01;
    const int64_t quantity = static_cast<int64_t>((row % 40) + 1);
    const double revenue = 50.0 + static_cast<double>(row % 2000) * 0.75;
    const double tax = static_cast<double>(row % 6) * 0.01;
    table.rows.push_back({dataflow::Value(shipdate), dataflow::Value(discount),
                          dataflow::Value(quantity), dataflow::Value(revenue),
                          dataflow::Value(tax),
                          dataflow::Value("q6-lineitem-" + std::to_string(row % 8192))});
  }
  return table;
}

dataflow::Table makeQ3LikeOrdersTable(size_t order_rows) {
  dataflow::Table table;
  table.schema = dataflow::Schema({"orderkey", "custkey", "orderdate", "shippriority"});
  table.rows.reserve(order_rows);
  for (size_t row = 0; row < order_rows; ++row) {
    const int64_t orderkey = static_cast<int64_t>(row + 1);
    const int64_t custkey = static_cast<int64_t>((row % 16384) + 1);
    const int64_t orderdate = static_cast<int64_t>(19950201 + (row % 180));
    const int64_t shippriority = static_cast<int64_t>(row % 8);
    table.rows.push_back({dataflow::Value(orderkey), dataflow::Value(custkey),
                          dataflow::Value(orderdate), dataflow::Value(shippriority)});
  }
  return table;
}

dataflow::Table makeQ3LikeLineitemTable(size_t lineitem_rows, size_t order_rows) {
  dataflow::Table table;
  table.schema = dataflow::Schema({"orderkey", "shipdate", "revenue", "discount", "quantity"});
  table.rows.reserve(lineitem_rows);
  for (size_t row = 0; row < lineitem_rows; ++row) {
    const int64_t orderkey = static_cast<int64_t>((row % order_rows) + 1);
    const int64_t shipdate = static_cast<int64_t>(19950320 + (row % 180));
    const double revenue = 10.0 + static_cast<double>(row % 4096) * 0.5;
    const double discount = 0.01 + static_cast<double>(row % 10) * 0.01;
    const int64_t quantity = static_cast<int64_t>((row % 32) + 1);
    table.rows.push_back({dataflow::Value(orderkey), dataflow::Value(shipdate),
                          dataflow::Value(revenue), dataflow::Value(discount),
                          dataflow::Value(quantity)});
  }
  return table;
}

dataflow::Table makeQ18LikeOrdersTable(size_t total_rows) {
  dataflow::Table table;
  table.schema = dataflow::Schema({"custkey", "orderkey", "quantity", "totalprice"});
  table.rows.reserve(total_rows);
  for (size_t row = 0; row < total_rows; ++row) {
    const int64_t custkey = static_cast<int64_t>((row % 32768) + 1);
    const int64_t orderkey = static_cast<int64_t>(row + 1);
    const int64_t quantity = static_cast<int64_t>((row % 64) + 1);
    const double totalprice = 100.0 + static_cast<double>(row % 8192) * 0.25;
    table.rows.push_back({dataflow::Value(custkey), dataflow::Value(orderkey),
                          dataflow::Value(quantity), dataflow::Value(totalprice)});
  }
  return table;
}

void printMetrics(const std::string& label, const dataflow::LocalActorStreamResult& result,
                  const std::string& scenario, size_t rows_per_batch) {
  const double elapsed_s = static_cast<double>(result.elapsed_ms) / 1000.0;
  const double rows_per_s =
      elapsed_s > 0.0 ? (result.processed_batches * rows_per_batch) / elapsed_s : 0.0;
  std::cout << "[tpch-q1-shape] " << label
            << " scenario=" << scenario
            << " batches=" << result.processed_batches
            << " partitions=" << result.processed_partitions
            << " elapsed_ms=" << result.elapsed_ms
            << " rows_per_s=" << rows_per_s
            << " blocked=" << result.blocked_count
            << " max_inflight=" << result.max_inflight_partitions
            << " result_rows=" << result.final_table.rowCount()
            << " split_ms=" << result.split_ms
            << " coord_serialize_ms=" << result.coordinator_serialize_ms
            << " coord_wait_ms=" << result.coordinator_wait_ms
            << " coord_deserialize_ms=" << result.coordinator_deserialize_ms
            << " coord_merge_ms=" << result.coordinator_merge_ms
            << " worker_deserialize_ms=" << result.worker_deserialize_ms
            << " worker_compute_ms=" << result.worker_compute_ms
            << " worker_serialize_ms=" << result.worker_serialize_ms
            << " input_payload_bytes=" << result.input_payload_bytes
            << " output_payload_bytes=" << result.output_payload_bytes
            << " input_shm_bytes=" << result.input_shared_memory_bytes
            << " output_shm_bytes=" << result.output_shared_memory_bytes
            << " used_shm=" << (result.used_shared_memory ? "true" : "false")
            << std::endl;
}

void printDecision(const dataflow::LocalExecutionDecision& decision) {
  std::cout << "[tpch-q1-shape] auto-decision chosen="
            << dataflow::localExecutionModeName(decision.chosen_mode)
            << " sampled_batches=" << decision.sampled_batches
            << " rows_per_batch=" << decision.rows_per_batch
            << " avg_projected_payload_bytes=" << decision.average_projected_payload_bytes
            << " single_rows_per_s=" << decision.single_rows_per_s
            << " actor_rows_per_s=" << decision.actor_rows_per_s
            << " actor_speedup=" << decision.actor_speedup
            << " compute_to_overhead=" << decision.compute_to_overhead_ratio
            << " thresholds_met=" << (decision.thresholds_met ? "true" : "false")
            << " reason=" << decision.reason << std::endl;
}

void printBatchMetrics(const std::string& case_name, std::size_t input_rows, std::size_t result_rows,
                       uint64_t elapsed_ms) {
  const double elapsed_s = static_cast<double>(elapsed_ms) / 1000.0;
  const double rows_per_s = elapsed_s > 0.0 ? static_cast<double>(input_rows) / elapsed_s : 0.0;
  std::cout << "[tpch-batch] case=" << case_name
            << " input_rows=" << input_rows
            << " elapsed_ms=" << elapsed_ms
            << " rows_per_s=" << rows_per_s
            << " result_rows=" << result_rows
            << std::endl;
}

void runQ6LikeBatchBenchmark(std::size_t batch_count, std::size_t rows_per_batch) {
  const std::size_t total_rows = batch_count * rows_per_batch;
  auto& session = dataflow::DataflowSession::builder();
  auto lineitem = session.createDataFrame(makeQ6LikeLineitemTable(total_rows));
  const auto revenue_index = lineitem.schema().indexOf("revenue");
  dataflow::AggregateSpec revenue_sum{dataflow::AggregateFunction::Sum, revenue_index, "revenue_sum"};

  const auto started = std::chrono::steady_clock::now();
  auto result = lineitem.filter("shipdate", ">=", dataflow::Value(int64_t(19940101)))
                    .filter("shipdate", "<", dataflow::Value(int64_t(19950101)))
                    .filter("discount", ">=", dataflow::Value(0.05))
                    .filter("discount", "<=", dataflow::Value(0.07))
                    .filter("quantity", "<", dataflow::Value(int64_t(24)))
                    .aggregate({}, {revenue_sum});
  const auto& output = result.materializedTable();
  const auto elapsed_ms = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started)
          .count());
  printBatchMetrics("q6-like-scan-filter-sum", total_rows, output.rowCount(), elapsed_ms);
}

void runQ3LikeBatchBenchmark(std::size_t batch_count, std::size_t rows_per_batch) {
  const std::size_t lineitem_rows = batch_count * rows_per_batch;
  const std::size_t order_rows = std::max<std::size_t>(1, lineitem_rows / 8);
  auto& session = dataflow::DataflowSession::builder();
  auto orders = session.createDataFrame(makeQ3LikeOrdersTable(order_rows));
  auto lineitem = session.createDataFrame(makeQ3LikeLineitemTable(lineitem_rows, order_rows));

  const auto started = std::chrono::steady_clock::now();
  auto result = orders.filter("orderdate", "<", dataflow::Value(int64_t(19950315)))
                    .join(lineitem.filter("shipdate", ">", dataflow::Value(int64_t(19950315))),
                          "orderkey", "orderkey", dataflow::JoinKind::Inner)
                    .groupBy({"shippriority"})
                    .sum("revenue", "revenue_sum")
                    .orderBy({"revenue_sum"}, {false})
                    .limit(10);
  const auto& output = result.materializedTable();
  const auto elapsed_ms = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started)
          .count());
  printBatchMetrics("q3-like-join-group-order", lineitem_rows + order_rows, output.rowCount(),
                    elapsed_ms);
}

void runQ18LikeBatchBenchmark(std::size_t batch_count, std::size_t rows_per_batch) {
  const std::size_t total_rows = batch_count * rows_per_batch;
  auto& session = dataflow::DataflowSession::builder();
  auto orders = session.createDataFrame(makeQ18LikeOrdersTable(total_rows));

  const auto started = std::chrono::steady_clock::now();
  auto result = orders.filter("quantity", ">", dataflow::Value(int64_t(15)))
                    .groupBy({"custkey"})
                    .sum("totalprice", "totalprice_sum")
                    .orderBy({"totalprice_sum"}, {false})
                    .limit(100);
  const auto& output = result.materializedTable();
  const auto elapsed_ms = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started)
          .count());
  printBatchMetrics("q18-like-high-card-group-order", total_rows, output.rowCount(), elapsed_ms);
}

}  // namespace

int main(int argc, char** argv) {
  size_t batch_count = 32;
  size_t rows_per_batch = 4096;
  size_t worker_count = 4;
  size_t max_inflight = 4;
  uint64_t worker_delay_ms = 0;
  size_t cpu_spin_per_row = 0;
  std::string mode = "all";
  std::string scenario = "string-keys";
  std::string benchmark_case = "q1";

  if (argc > 1) batch_count = static_cast<size_t>(std::strtoull(argv[1], nullptr, 10));
  if (argc > 2) rows_per_batch = static_cast<size_t>(std::strtoull(argv[2], nullptr, 10));
  if (argc > 3) worker_count = static_cast<size_t>(std::strtoull(argv[3], nullptr, 10));
  if (argc > 4) max_inflight = static_cast<size_t>(std::strtoull(argv[4], nullptr, 10));
  if (argc > 5) worker_delay_ms = static_cast<uint64_t>(std::strtoull(argv[5], nullptr, 10));
  if (argc > 6) cpu_spin_per_row = static_cast<size_t>(std::strtoull(argv[6], nullptr, 10));
  if (argc > 7) mode = argv[7];
  if (argc > 8) scenario = argv[8];
  if (argc > 9) benchmark_case = argv[9];

  if (benchmark_case == "q1" || benchmark_case == "suite") {
    const auto batches = makeLineitemQ1ShapeBatches(scenario, batch_count, rows_per_batch);
    dataflow::LocalGroupedAggregateSpec aggregate;
    aggregate.group_keys = {"window_start", "key"};
    aggregate.aggregates.push_back(
        {dataflow::AggregateFunction::Sum, "value", "value_sum", false});

    dataflow::LocalActorStreamOptions actor_options;
    actor_options.worker_count = worker_count;
    actor_options.max_inflight_partitions = max_inflight;
    actor_options.worker_delay_ms = worker_delay_ms;
    actor_options.cpu_spin_per_row = cpu_spin_per_row;

    if (mode == "single" || mode == "all") {
      const auto single_started = std::chrono::steady_clock::now();
      dataflow::LocalActorStreamResult single_result;
      single_result.final_table =
          dataflow::runSingleProcessGroupedAggregate(batches, aggregate, cpu_spin_per_row);
      single_result.processed_batches = batch_count;
      single_result.processed_partitions = batch_count;
      single_result.elapsed_ms = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::steady_clock::now() - single_started)
              .count());
      printMetrics("single-process", single_result, scenario, rows_per_batch);
    }

    if (mode == "actor" || mode == "all") {
      const auto actor_result =
          dataflow::runLocalActorStreamGroupedAggregate(batches, aggregate, actor_options);
      printMetrics("actor-credit", actor_result, scenario, rows_per_batch);
    }

    if (mode == "auto" || mode == "all") {
      dataflow::LocalExecutionDecision decision;
      const auto auto_result =
          dataflow::runAutoLocalActorStreamGroupedAggregate(batches, aggregate, actor_options, {},
                                                            &decision);
      printDecision(decision);
      printMetrics("auto-selected", auto_result, scenario, rows_per_batch);
    }
  }

  if (benchmark_case == "q6" || benchmark_case == "suite") {
    runQ6LikeBatchBenchmark(batch_count, rows_per_batch);
  }
  if (benchmark_case == "q3" || benchmark_case == "suite") {
    runQ3LikeBatchBenchmark(batch_count, rows_per_batch);
  }
  if (benchmark_case == "q18" || benchmark_case == "suite") {
    runQ18LikeBatchBenchmark(batch_count, rows_per_batch);
  }

  return 0;
}
