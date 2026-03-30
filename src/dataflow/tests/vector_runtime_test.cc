#include <cstdint>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

#include "src/dataflow/core/contract/api/session.h"
#include "src/dataflow/experimental/rpc/actor_rpc_codec.h"
#include "src/dataflow/experimental/rpc/rpc_codec.h"
#include "src/dataflow/core/execution/serial/serializer.h"
#include "src/dataflow/core/execution/stream/binary_row_batch.h"

namespace {

void expect(bool cond, const std::string& msg) {
  if (!cond) throw std::runtime_error(msg);
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

    dataflow::RpcEnvelope rpc_envelope;
    rpc_envelope.type = dataflow::RpcMessageType::DataBatch;
    rpc_envelope.codec_id = "table-bin-v1";
    auto table_serializer = dataflow::makeTableRpcSerializer();
    dataflow::RpcDataBatchMessage batch_message{table};
    const auto rpc_payload = table_serializer->serialize(rpc_envelope, &batch_message);
    expect(!rpc_payload.empty(), "rpc table payload should not be empty");
    dataflow::RpcDataBatchMessage decoded_batch;
    expect(table_serializer->deserialize(rpc_envelope, rpc_payload, &decoded_batch),
           "rpc table batch deserialize failed");
    expect(decoded_batch.table.rows.size() == 3, "rpc table batch row count mismatch");
    expect(decoded_batch.table.rows[2][1].asFixedVector()[1] == 1.0f,
           "rpc table batch vector content mismatch");

    dataflow::ActorRpcMessage actor_origin;
    actor_origin.action = dataflow::ActorRpcAction::Result;
    actor_origin.job_id = "vector-job";
    actor_origin.chain_id = "vector-chain";
    actor_origin.task_id = "vector-task";
    actor_origin.node_id = "vector-worker";
    actor_origin.ok = true;
    actor_origin.state = "FINISHED";
    actor_origin.summary = "vector payload";
    actor_origin.result_location = "inline://vector-job/vector-task";
    actor_origin.payload = actor_origin.summary;
    const auto actor_wire = dataflow::encodeActorRpcMessage(actor_origin);
    dataflow::ActorRpcMessage actor_copy;
    expect(dataflow::decodeActorRpcMessage(actor_wire, &actor_copy),
           "actor rpc roundtrip failed for control payload");
    expect(actor_copy.summary == actor_origin.summary,
           "actor rpc should preserve control summary");

    dataflow::LengthPrefixedFrameCodec frame_codec;
    dataflow::RpcFrame control_frame;
    control_frame.header.protocol_version = 1;
    control_frame.header.type = dataflow::RpcMessageType::Control;
    control_frame.header.message_id = 17;
    control_frame.header.correlation_id = 0;
    control_frame.header.codec_id = "actor-rpc-v1";
    control_frame.header.source = "vector-worker";
    control_frame.header.target = "vector-client";
    control_frame.payload = actor_wire;

    std::size_t control_consumed = 0;
    dataflow::RpcFrame decoded_control_frame;
    expect(frame_codec.decode(frame_codec.encode(control_frame), &decoded_control_frame, &control_consumed),
           "frame codec should decode control frame");
    expect(decoded_control_frame.header.message_id == control_frame.header.message_id,
           "control frame message id mismatch");
    expect(decoded_control_frame.header.codec_id == "actor-rpc-v1",
           "control frame codec id mismatch");

    dataflow::RpcFrame data_frame;
    data_frame.header.protocol_version = 1;
    data_frame.header.type = dataflow::RpcMessageType::DataBatch;
    data_frame.header.message_id = 18;
    data_frame.header.correlation_id = control_frame.header.message_id;
    data_frame.header.codec_id = "table-bin-v1";
    data_frame.header.source = "vector-worker";
    data_frame.header.target = "vector-client";
    data_frame.payload = rpc_payload;

    std::size_t data_consumed = 0;
    dataflow::RpcFrame decoded_data_frame;
    expect(frame_codec.decode(frame_codec.encode(data_frame), &decoded_data_frame, &data_consumed),
           "frame codec should decode data frame");
    expect(decoded_data_frame.header.correlation_id == control_frame.header.message_id,
           "data frame correlation id mismatch");
    dataflow::RpcDataBatchMessage framed_batch;
    expect(table_serializer->deserialize(decoded_data_frame.header, decoded_data_frame.payload, &framed_batch),
           "framed data batch deserialize failed");
    expect(framed_batch.table.rows.size() == table.rows.size(),
           "framed data batch row count mismatch");
    expect(framed_batch.table.rows[0][1].asFixedVector()[0] == 1.0f,
           "framed data batch vector content mismatch");

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
    expect(explain.find("candidate_rows=3") != std::string::npos, "explain candidate_rows missing");
    expect(explain.find("filter_pushdown=false") != std::string::npos,
           "explain filter_pushdown contract missing");
    expect(explain.find("acceleration=flat-buffer+heap-topk") != std::string::npos,
           "explain acceleration hint missing");

    dataflow::Table sparse_table;
    sparse_table.schema = dataflow::Schema({"id", "embedding"});
    sparse_table.rows = {
        {dataflow::Value(int64_t(10))},
        {dataflow::Value(int64_t(20)), dataflow::Value(std::vector<float>{0.0f, 1.0f, 0.0f})},
        {dataflow::Value(int64_t(30)), dataflow::Value(std::vector<float>{1.0f, 0.0f, 0.0f})},
    };
    session.createTempView("vec_sparse", session.createDataFrame(sparse_table));
    auto sparse = session.vectorQuery("vec_sparse", "embedding", {1.0f, 0.0f, 0.0f}, 1,
                                      dataflow::VectorDistanceMetric::Cosine)
                      .toTable();
    expect(sparse.rows.size() == 1, "sparse vector query top-k mismatch");
    expect(sparse.rows[0][0].asInt64() == 2, "sparse vector query should preserve source row id");

    const auto parsed_space = dataflow::Value::parseFixedVector("[1 0 0]");
    expect(parsed_space.size() == 3, "space-separated vector parse should keep dimension");
    expect(parsed_space[0] == 1.0f && parsed_space[1] == 0.0f && parsed_space[2] == 0.0f,
           "space-separated vector parse content mismatch");

    const auto parsed_comma = dataflow::Value::parseFixedVector("[0.9,0.1,0]");
    expect(parsed_comma.size() == 3, "comma-separated vector parse should keep dimension");
    expect(parsed_comma[0] == 0.9f && parsed_comma[1] == 0.1f && parsed_comma[2] == 0.0f,
           "comma-separated vector parse content mismatch");

    char csv_path_template[] = "/tmp/velaria-vector-runtime-XXXXXX";
    const int csv_fd = mkstemp(csv_path_template);
    expect(csv_fd != -1, "mkstemp failed for vector csv test");
    close(csv_fd);
    const std::string csv_path = csv_path_template;
    {
      std::ofstream csv(csv_path);
      expect(csv.good(), "failed to open vector csv temp file");
      csv << "id,embedding\n";
      csv << "1,[1 0 0]\n";
      csv << "2,[0.9 0.1 0]\n";
      csv << "3,[0 1 0]\n";
    }

    auto csv_df = session.read_csv(csv_path);
    session.createTempView("vec_csv_src", csv_df);
    const auto csv_cosine = session.vectorQuery("vec_csv_src", "embedding", {1.0f, 0.0f, 0.0f}, 2,
                                                dataflow::VectorDistanceMetric::Cosine)
                                .toTable();
    std::remove(csv_path.c_str());
    expect(csv_cosine.rows.size() == 2, "csv vector query top-k mismatch");
    expect(csv_cosine.rows[0][0].asInt64() == 0, "csv vector query nearest should be row 0");

    const auto csv_explain = session.explainVectorQuery("vec_csv_src", "embedding",
                                                        {1.0f, 0.0f, 0.0f}, 2,
                                                        dataflow::VectorDistanceMetric::Cosine);
    expect(csv_explain.find("mode=exact-scan") != std::string::npos,
           "csv explain mode missing");
    expect(csv_explain.find("candidate_rows=3") != std::string::npos,
           "csv explain candidate_rows missing");

    std::cout << "[test] vector runtime query and transport ok" << std::endl;
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "[test] vector runtime query and transport failed: " << ex.what() << std::endl;
    return 1;
  }
}
