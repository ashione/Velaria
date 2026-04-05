#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "src/dataflow/core/contract/api/session.h"

namespace {

using Clock = std::chrono::steady_clock;

dataflow::ComputedColumnArg columnArg(std::size_t index) {
  dataflow::ComputedColumnArg arg;
  arg.is_literal = false;
  arg.source_column_index = index;
  return arg;
}

dataflow::ComputedColumnArg literalArg(dataflow::Value value) {
  dataflow::ComputedColumnArg arg;
  arg.is_literal = true;
  arg.literal = std::move(value);
  arg.source_column_index = static_cast<std::size_t>(-1);
  return arg;
}

dataflow::Table makeStringTable(std::size_t rows) {
  dataflow::Table table;
  table.schema = dataflow::Schema({"id", "region", "payload", "user_id"});
  table.rows.reserve(rows);
  static const char* kRegions[] = {"APAC", "EMEA", "AMER", "LATAM"};
  for (std::size_t i = 0; i < rows; ++i) {
    const std::string region = kRegions[i % 4];
    const std::string payload = "  order-" + std::to_string(i % 1024) + "-" + region + "-segment-" +
                                std::to_string((i * 7) % 97) + "  ";
    const std::string user_id = "user_" + std::to_string(i % 4096);
    if ((i % 53) == 0) {
      table.rows.push_back({dataflow::Value(static_cast<int64_t>(i)), dataflow::Value(region),
                            dataflow::Value(), dataflow::Value(user_id)});
      continue;
    }
    table.rows.push_back({dataflow::Value(static_cast<int64_t>(i)), dataflow::Value(region),
                          dataflow::Value(payload), dataflow::Value(user_id)});
  }
  return table;
}

long long microsBetween(Clock::time_point begin, Clock::time_point end) {
  return std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
}

std::size_t stringChecksum(const dataflow::Table& table, std::size_t column_index) {
  std::size_t checksum = 0;
  if (column_index >= table.schema.fields.size()) {
    return checksum;
  }
  const auto column = dataflow::materializeValueColumn(table, column_index);
  const auto row_count = dataflow::valueColumnRowCount(column);
  for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
    const auto value = dataflow::valueColumnValueAt(column, row_index);
    if (value.isNull()) continue;
    checksum += value.toString().size();
  }
  return checksum;
}

void runCase(const std::string& name, std::size_t rows, std::size_t rounds,
             const std::function<dataflow::DataFrame(dataflow::DataflowSession&, const dataflow::DataFrame&)>&
                 builder,
             std::size_t checksum_column) {
  auto& session = dataflow::DataflowSession::builder();
  const auto base = session.createDataFrame(makeStringTable(rows));

  long long total_us = 0;
  std::size_t last_checksum = 0;
  std::size_t last_columns = 0;
  for (std::size_t round = 0; round < rounds; ++round) {
    const auto started = Clock::now();
    const auto out = builder(session, base).toTable();
    const auto ended = Clock::now();
    total_us += microsBetween(started, ended);
    last_checksum = stringChecksum(out, checksum_column);
    last_columns = out.schema.fields.size();
    if (out.rowCount() != rows) {
      throw std::runtime_error("unexpected row count for case: " + name);
    }
  }

  const double avg_us = static_cast<double>(total_us) / static_cast<double>(rounds);
  const double rows_per_s = avg_us > 0.0 ? (rows * 1000000.0) / avg_us : 0.0;
  std::cout << "{"
            << "\"bench\":\"string-builtins\","
            << "\"case\":\"" << name << "\","
            << "\"rows\":" << rows << ","
            << "\"rounds\":" << rounds << ","
            << "\"avg_us\":" << avg_us << ","
            << "\"rows_per_s\":" << rows_per_s << ","
            << "\"checksum\":" << last_checksum << ","
            << "\"result_columns\":" << last_columns
            << "}" << std::endl;
}

void runPrebuiltCase(const std::string& name, std::size_t rows, std::size_t rounds,
                     const dataflow::DataFrame& plan, std::size_t checksum_column) {
  long long total_us = 0;
  std::size_t last_checksum = 0;
  std::size_t last_columns = 0;
  for (std::size_t round = 0; round < rounds; ++round) {
    const auto started = Clock::now();
    const auto out = plan.toTable();
    const auto ended = Clock::now();
    total_us += microsBetween(started, ended);
    last_checksum = stringChecksum(out, checksum_column);
    last_columns = out.schema.fields.size();
    if (out.rowCount() != rows) {
      throw std::runtime_error("unexpected row count for case: " + name);
    }
  }

  const double avg_us = static_cast<double>(total_us) / static_cast<double>(rounds);
  const double rows_per_s = avg_us > 0.0 ? (rows * 1000000.0) / avg_us : 0.0;
  std::cout << "{"
            << "\"bench\":\"string-builtins\","
            << "\"case\":\"" << name << "\","
            << "\"rows\":" << rows << ","
            << "\"rounds\":" << rounds << ","
            << "\"avg_us\":" << avg_us << ","
            << "\"rows_per_s\":" << rows_per_s << ","
            << "\"checksum\":" << last_checksum << ","
            << "\"result_columns\":" << last_columns
            << "}" << std::endl;
}

}  // namespace

int main(int argc, char** argv) {
  std::size_t rows = 100000;
  std::size_t rounds = 5;

  if (argc > 1) rows = static_cast<std::size_t>(std::strtoull(argv[1], nullptr, 10));
  if (argc > 2) rounds = static_cast<std::size_t>(std::strtoull(argv[2], nullptr, 10));

  std::cout << "[string-benchmark] rows=" << rows << " rounds=" << rounds << std::endl;

  auto& session = dataflow::DataflowSession::builder();
  const std::string sql_view_name = "string_builtin_bench_" + std::to_string(rows);
  session.createTempView(sql_view_name, session.createDataFrame(makeStringTable(rows)));
  const auto sql_reused_plan =
      session.sql("SELECT LOWER(region) AS region_lower, "
                  "TRIM(payload) AS payload_trimmed, "
                  "SUBSTR(payload, 2, 12) AS payload_slice, "
                  "REPLACE(payload, '-', '_') AS payload_clean, "
                  "CONCAT_WS('|', region, user_id) AS region_user "
                  "FROM " +
                  sql_view_name);

  runCase(
      "copy-column", rows, rounds,
      [](dataflow::DataflowSession&, const dataflow::DataFrame& base) {
        return base.withColumn("region_copy", "region");
      },
      4);

  runCase(
      "single-arg-functions", rows, rounds,
      [](dataflow::DataflowSession&, const dataflow::DataFrame& base) {
        return base.withColumn(
                       "region_lower", dataflow::ComputedColumnKind::StringLower,
                       std::vector<dataflow::ComputedColumnArg>{columnArg(1)})
            .withColumn("payload_trimmed", dataflow::ComputedColumnKind::StringTrim,
                        std::vector<dataflow::ComputedColumnArg>{columnArg(2)});
      },
      5);

  runCase(
      "multi-arg-functions", rows, rounds,
      [](dataflow::DataflowSession&, const dataflow::DataFrame& base) {
        return base.withColumn(
                       "payload_slice", dataflow::ComputedColumnKind::StringSubstr,
                       std::vector<dataflow::ComputedColumnArg>{
                           columnArg(2), literalArg(dataflow::Value(int64_t(2))),
                           literalArg(dataflow::Value(int64_t(12)))})
            .withColumn(
                "payload_clean", dataflow::ComputedColumnKind::StringReplace,
                std::vector<dataflow::ComputedColumnArg>{
                    columnArg(2), literalArg(dataflow::Value("-")),
                    literalArg(dataflow::Value("_"))})
            .withColumn(
                "region_user", dataflow::ComputedColumnKind::StringConcatWs,
                std::vector<dataflow::ComputedColumnArg>{
                    literalArg(dataflow::Value("|")), columnArg(1), columnArg(3)});
      },
      6);

  runCase(
      "dependent-chain", rows, rounds,
      [](dataflow::DataflowSession&, const dataflow::DataFrame& base) {
        return base.withColumn("payload_trimmed", dataflow::ComputedColumnKind::StringTrim,
                               std::vector<dataflow::ComputedColumnArg>{columnArg(2)})
            .withColumn("payload_lower", dataflow::ComputedColumnKind::StringLower,
                        std::vector<dataflow::ComputedColumnArg>{columnArg(4)})
            .withColumn(
                "payload_clean", dataflow::ComputedColumnKind::StringReplace,
                std::vector<dataflow::ComputedColumnArg>{
                    columnArg(5), literalArg(dataflow::Value("-")),
                    literalArg(dataflow::Value("_"))})
            .withColumn(
                "payload_slice", dataflow::ComputedColumnKind::StringSubstr,
                std::vector<dataflow::ComputedColumnArg>{
                    columnArg(6), literalArg(dataflow::Value(int64_t(2))),
                    literalArg(dataflow::Value(int64_t(10)))});
      },
      7);

  runCase(
      "sql-plan-and-execute", rows, rounds,
      [sql_view_name](dataflow::DataflowSession& session, const dataflow::DataFrame&) {
        return session.sql("SELECT LOWER(region) AS region_lower, "
                           "TRIM(payload) AS payload_trimmed, "
                           "SUBSTR(payload, 2, 12) AS payload_slice, "
                           "REPLACE(payload, '-', '_') AS payload_clean, "
                           "CONCAT_WS('|', region, user_id) AS region_user "
                           "FROM " +
                           sql_view_name);
      },
      4);

  runPrebuiltCase("sql-reused-plan", rows, rounds, sql_reused_plan, 4);

  return 0;
}
