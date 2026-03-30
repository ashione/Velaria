#include <cmath>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "src/dataflow/api/session.h"
#include "src/dataflow/serial/serializer.h"
#include "src/dataflow/stream/binary_row_batch.h"

namespace {

void expect(bool cond, const std::string& msg) {
  if (!cond) throw std::runtime_error(msg);
}

bool nearlyEqual(double lhs, double rhs, double eps = 1e-6) {
  return std::fabs(lhs - rhs) <= eps;
}

}  // namespace

int main() {
  try {
    dataflow::Table table;
    table.schema = dataflow::Schema({"id", "embedding"});
    table.rows = {
        {dataflow::Value(int64_t(1)), dataflow::Value(std::vector<float>{1.0f, 0.0f, 0.0f})},
        {dataflow::Value(int64_t(2)), dataflow::Value(std::vector<float>{0.9f, 0.1f, 0.0f})},
        {dataflow::Value(int64_t(3)), dataflow::Value(std::vector<float>{0.0f, 1.0f, 0.0f})},
    };

    dataflow::ProtoLikeSerializer text_codec;
    auto text_payload = text_codec.serialize(table);
    auto text_roundtrip = text_codec.deserialize(text_payload);
    expect(text_roundtrip.rows.size() == 3, "proto-like row count mismatch");
    expect(text_roundtrip.rows[0][1].type() == dataflow::DataType::FixedVector,
           "proto-like should keep vector type");
    expect(text_roundtrip.rows[0][1].asFixedVector().size() == 3,
           "proto-like vector length mismatch");

    dataflow::BinaryRowBatchCodec batch_codec;
    std::vector<uint8_t> binary_payload;
    batch_codec.serialize(table, &binary_payload);
    auto binary_roundtrip = batch_codec.deserialize(binary_payload);
    expect(binary_roundtrip.rows.size() == 3, "binary row batch row count mismatch");
    expect(binary_roundtrip.rows[1][1].type() == dataflow::DataType::FixedVector,
           "binary row batch should keep vector type");
    expect(binary_roundtrip.rows[1][1].asFixedVector()[1] == 0.1f,
           "binary row batch vector content mismatch");

    auto& session = dataflow::DataflowSession::builder();
    auto df = session.createDataFrame(table);
    session.createTempView("vec_src", df);

    auto cosine = session.vectorQuery("vec_src", "embedding", {1.0f, 0.0f, 0.0f}, 2,
                                      dataflow::VectorDistanceMetric::Cosine)
                      .toTable();
    expect(cosine.rows.size() == 2, "cosine vector query top-k mismatch");
    expect(cosine.schema.fields[0] == "row_id", "cosine result schema mismatch");
    expect(cosine.rows[0][0].asInt64() == 0, "cosine nearest should be row 0");

    auto l2 = session.vectorQuery("vec_src", "embedding", {0.0f, 1.0f, 0.0f}, 1,
                                  dataflow::VectorDistanceMetric::L2)
                  .toTable();
    expect(l2.rows.size() == 1, "l2 vector query top-k mismatch");
    expect(l2.rows[0][0].asInt64() == 2, "l2 nearest should be row 2");

    auto dot = session.vectorQuery("vec_src", "embedding", {1.0f, 0.0f, 0.0f}, 1,
                                   dataflow::VectorDistanceMetric::Dot)
                   .toTable();
    expect(dot.rows[0][0].asInt64() == 0, "dot nearest should be row 0");

    const auto explain = session.explainVectorQuery("vec_src", "embedding", {1.0f, 0.0f, 0.0f}, 2,
                                                    dataflow::VectorDistanceMetric::Cosine);
    expect(explain.find("mode=exact-scan") != std::string::npos, "explain mode missing");
    expect(explain.find("metric=cosine") != std::string::npos, "explain metric missing");
    expect(explain.find("dimension=3") != std::string::npos, "explain dimension missing");
    expect(explain.find("top_k=2") != std::string::npos, "explain top_k missing");
    expect(explain.find("acceleration=flat-buffer+heap-topk") != std::string::npos,
           "explain acceleration hint missing");

    std::cout << "[test] vector runtime query and transport ok" << std::endl;
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "[test] vector runtime query and transport failed: " << ex.what() << std::endl;
    return 1;
  }
}
