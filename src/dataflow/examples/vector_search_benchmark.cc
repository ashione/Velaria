#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <random>
#include <string>
#include <vector>

#include "src/dataflow/core/contract/api/session.h"
#include "src/dataflow/core/execution/nanoarrow_ipc_codec.h"
#include "src/dataflow/experimental/rpc/actor_rpc_codec.h"
#include "src/dataflow/core/execution/serial/serializer.h"
#include "src/dataflow/core/execution/stream/binary_row_batch.h"

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

long long microsBetween(std::chrono::steady_clock::time_point begin,
                        std::chrono::steady_clock::time_point end) {
  return std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
}

void runCase(std::size_t rows, std::size_t dim, dataflow::VectorDistanceMetric metric,
             const std::string& metric_name) {
  auto table = makeSyntheticTable(rows, dim, static_cast<uint32_t>(rows + dim));
  auto query = makeQuery(dim, static_cast<uint32_t>(dim));

  auto& session = dataflow::DataflowSession::builder();
  const std::string view_name = "vec_bench_" + std::to_string(rows) + "_" + std::to_string(dim);
  session.createTempView(view_name, session.createDataFrame(table));

  const auto cold_begin = std::chrono::steady_clock::now();
  auto out = session.vectorQuery(view_name, "embedding", query, 10, metric).toTable();
  const auto cold_end = std::chrono::steady_clock::now();

  constexpr std::size_t kWarmIterations = 5;
  long long warm_query_us = 0;
  for (std::size_t i = 0; i < kWarmIterations; ++i) {
    const auto begin = std::chrono::steady_clock::now();
    auto warm = session.vectorQuery(view_name, "embedding", query, 10, metric).toTable();
    const auto end = std::chrono::steady_clock::now();
    warm_query_us += microsBetween(begin, end);
    if (warm.rows.size() != out.rows.size()) {
      std::cerr << "[vector-benchmark] warm query cardinality mismatch" << std::endl;
      std::exit(1);
    }
  }

  long long explain_us = 0;
  for (std::size_t i = 0; i < kWarmIterations; ++i) {
    const auto begin = std::chrono::steady_clock::now();
    const auto explain = session.explainVectorQuery(view_name, "embedding", query, 10, metric);
    const auto end = std::chrono::steady_clock::now();
    explain_us += microsBetween(begin, end);
    if (explain.find("mode=exact-scan") == std::string::npos) {
      std::cerr << "[vector-benchmark] explain output mismatch" << std::endl;
      std::exit(1);
    }
  }

  std::cout << "{"
            << "\"bench\":\"vector-query\","
            << "\"rows\":" << rows << ","
            << "\"dimension\":" << dim << ","
            << "\"top_k\":10,"
            << "\"metric\":\"" << metric_name << "\","
            << "\"cold_query_us\":" << microsBetween(cold_begin, cold_end) << ","
            << "\"warm_query_avg_us\":" << (warm_query_us / static_cast<long long>(kWarmIterations)) << ","
            << "\"warm_explain_avg_us\":" << (explain_us / static_cast<long long>(kWarmIterations)) << ","
            << "\"result_rows\":" << out.rows.size() << ""
            << "}" << std::endl;
}

void runTransportCase(std::size_t rows, std::size_t dim) {
  const auto table = makeSyntheticTable(rows, dim, static_cast<uint32_t>(rows * 17 + dim));

  dataflow::ProtoLikeSerializer proto_codec;
  const auto proto_serialize_begin = std::chrono::steady_clock::now();
  const auto proto_payload = proto_codec.serialize(table);
  const auto proto_serialize_end = std::chrono::steady_clock::now();
  const auto proto_deserialize_begin = std::chrono::steady_clock::now();
  const auto proto_roundtrip = proto_codec.deserialize(proto_payload);
  const auto proto_deserialize_end = std::chrono::steady_clock::now();

  dataflow::BinaryRowBatchCodec batch_codec;
  std::vector<uint8_t> binary_payload;
  const auto binary_serialize_begin = std::chrono::steady_clock::now();
  batch_codec.serialize(table, &binary_payload);
  const auto binary_serialize_end = std::chrono::steady_clock::now();
  const auto binary_deserialize_begin = std::chrono::steady_clock::now();
  const auto binary_roundtrip = batch_codec.deserialize(binary_payload);
  const auto binary_deserialize_end = std::chrono::steady_clock::now();

  const auto arrow_serialize_begin = std::chrono::steady_clock::now();
  const auto arrow_payload = dataflow::serialize_nanoarrow_ipc_table(table);
  const auto arrow_serialize_end = std::chrono::steady_clock::now();
  const auto arrow_deserialize_begin = std::chrono::steady_clock::now();
  const auto arrow_roundtrip = dataflow::deserialize_nanoarrow_ipc_table(arrow_payload, false);
  const auto arrow_deserialize_end = std::chrono::steady_clock::now();

  dataflow::ActorRpcMessage actor_payload;
  actor_payload.action = dataflow::ActorRpcAction::Result;
  actor_payload.job_id = "bench-job";
  actor_payload.chain_id = "bench-chain";
  actor_payload.task_id = "bench-task";
  actor_payload.node_id = "bench-node";
  actor_payload.ok = true;
  actor_payload.state = "FINISHED";
  actor_payload.summary = "vector transport benchmark";
  actor_payload.result_location = "inline://bench-job/bench-task";
  actor_payload.payload = actor_payload.summary;

  const auto actor_encode_begin = std::chrono::steady_clock::now();
  const auto actor_wire = encodeActorRpcMessage(actor_payload);
  const auto actor_encode_end = std::chrono::steady_clock::now();
  const auto actor_decode_begin = std::chrono::steady_clock::now();
  dataflow::ActorRpcMessage actor_roundtrip;
  const bool actor_decode_ok = decodeActorRpcMessage(actor_wire, &actor_roundtrip);
  const auto actor_decode_end = std::chrono::steady_clock::now();

  if (proto_roundtrip.rowCount() != table.rowCount() || binary_roundtrip.rowCount() != table.rowCount() ||
      arrow_roundtrip.rowCount() != table.rowCount()) {
    std::cerr << "[vector-benchmark] transport roundtrip row count mismatch" << std::endl;
    std::exit(1);
  }
  if (!actor_decode_ok || actor_roundtrip.summary != actor_payload.summary ||
      actor_roundtrip.result_location != actor_payload.result_location) {
    std::cerr << "[vector-benchmark] actor rpc control roundtrip mismatch" << std::endl;
    std::exit(1);
  }

  std::cout << "{"
            << "\"bench\":\"vector-transport\","
            << "\"rows\":" << rows << ","
            << "\"dimension\":" << dim << ","
            << "\"proto_serialize_us\":" << microsBetween(proto_serialize_begin, proto_serialize_end) << ","
            << "\"proto_deserialize_us\":" << microsBetween(proto_deserialize_begin, proto_deserialize_end) << ","
            << "\"proto_payload_bytes\":" << proto_payload.size() << ","
            << "\"binary_serialize_us\":" << microsBetween(binary_serialize_begin, binary_serialize_end) << ","
            << "\"binary_deserialize_us\":" << microsBetween(binary_deserialize_begin, binary_deserialize_end) << ","
            << "\"binary_payload_bytes\":" << binary_payload.size() << ","
            << "\"arrow_ipc_serialize_us\":" << microsBetween(arrow_serialize_begin, arrow_serialize_end) << ","
            << "\"arrow_ipc_deserialize_us\":" << microsBetween(arrow_deserialize_begin, arrow_deserialize_end) << ","
            << "\"arrow_ipc_payload_bytes\":" << arrow_payload.size() << ","
            << "\"actor_rpc_encode_us\":" << microsBetween(actor_encode_begin, actor_encode_end) << ","
            << "\"actor_rpc_decode_us\":" << microsBetween(actor_decode_begin, actor_decode_end) << ","
            << "\"actor_rpc_control_bytes\":" << actor_wire.size()
            << "}" << std::endl;
}

}  // namespace

int main(int argc, char** argv) {
  bool quick = false;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--quick") {
      quick = true;
      continue;
    }
    if (arg == "-h" || arg == "--help") {
      std::cout << "Usage: " << argv[0] << " [--quick]\n";
      std::cout << "  --quick  run a smaller exact-scan baseline for repo verification\n";
      return 0;
    }
    throw std::runtime_error("unknown argument: " + arg);
  }

  const std::vector<std::size_t> rows_cases = quick ? std::vector<std::size_t>{10000ULL}
                                                    : std::vector<std::size_t>{10000ULL, 100000ULL};
  const std::vector<std::size_t> dim_cases = quick ? std::vector<std::size_t>{128ULL}
                                                   : std::vector<std::size_t>{128ULL, 768ULL};

  std::cout << "[vector-benchmark] exact scan regression baseline"
            << (quick ? " (quick)" : " (full)") << std::endl;
  for (std::size_t rows : rows_cases) {
    for (std::size_t dim : dim_cases) {
      runCase(rows, dim, dataflow::VectorDistanceMetric::Cosine, "cosine");
      runCase(rows, dim, dataflow::VectorDistanceMetric::Dot, "dot");
      runCase(rows, dim, dataflow::VectorDistanceMetric::L2, "l2");
      runTransportCase(rows, dim);
    }
  }
  return 0;
}
