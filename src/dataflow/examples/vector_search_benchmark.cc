#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "src/dataflow/api/session.h"

namespace {

dataflow::Table makeSyntheticTable(std::size_t rows, std::size_t dim, uint32_t seed) {
  std::mt19937 rng(seed);
  std::uniform_real_distribution<float> dist(-1.0f, 1.0f);

  dataflow::Table table;
  table.schema = dataflow::Schema({"id", "embedding"});
  table.rows.reserve(rows);
  for (std::size_t i = 0; i < rows; ++i) {
    std::vector<float> vec(dim);
    for (std::size_t d = 0; d < dim; ++d) vec[d] = dist(rng);
    dataflow::Row row;
    row.emplace_back(static_cast<int64_t>(i));
    row.emplace_back(dataflow::Value(std::move(vec)));
    table.rows.push_back(std::move(row));
  }
  return table;
}

std::vector<float> makeQuery(std::size_t dim, uint32_t seed) {
  std::mt19937 rng(seed);
  std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
  std::vector<float> q(dim);
  for (std::size_t i = 0; i < dim; ++i) q[i] = dist(rng);
  return q;
}

void runCase(std::size_t rows, std::size_t dim, dataflow::VectorDistanceMetric metric,
             const std::string& metric_name) {
  auto table = makeSyntheticTable(rows, dim, static_cast<uint32_t>(rows + dim));
  auto query = makeQuery(dim, static_cast<uint32_t>(dim));

  auto& session = dataflow::DataflowSession::builder();
  const std::string view_name = "vec_bench_" + std::to_string(rows) + "_" + std::to_string(dim);
  session.createTempView(view_name, session.createDataFrame(table));

  const auto begin = std::chrono::steady_clock::now();
  auto out = session.vectorQuery(view_name, "embedding", query, 10, metric).toTable();
  const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - begin);

  std::cout << "{"
            << "\"rows\":" << rows << ","
            << "\"dimension\":" << dim << ","
            << "\"top_k\":10,"
            << "\"metric\":\"" << metric_name << "\","
            << "\"elapsed_ms\":" << elapsed.count() << ","
            << "\"result_rows\":" << out.rows.size() << ""
            << "}" << std::endl;
}

}  // namespace

int main() {
  std::cout << "[vector-benchmark] exact scan regression baseline" << std::endl;
  for (std::size_t rows : {10000ULL, 100000ULL}) {
    for (std::size_t dim : {128ULL, 768ULL}) {
      runCase(rows, dim, dataflow::VectorDistanceMetric::Cosine, "cosine");
      runCase(rows, dim, dataflow::VectorDistanceMetric::Dot, "dot");
      runCase(rows, dim, dataflow::VectorDistanceMetric::L2, "l2");
    }
  }
  return 0;
}
