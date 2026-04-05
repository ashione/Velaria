#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "src/dataflow/core/execution/table.h"
#include "src/dataflow/experimental/rpc/rpc_codec.h"
#include "src/dataflow/experimental/rpc/rpc_codec_ids.h"
#include "src/dataflow/experimental/runtime/rpc_contract.h"
#include "src/dataflow/experimental/runtime/byte_transport.h"

namespace dataflow {

struct RpcRunnerCodecConfig {
  std::string control_codec_id = kRpcCodecIdJsonControlV1;
  std::string data_codec_id = kRpcCodecIdTableArrowIpcV1;
  uint8_t protocol_version = 1;
};

class RpcRunner {
 public:
  explicit RpcRunner(std::shared_ptr<IByteTransport> transport,
                     std::unique_ptr<IRpcFrameCodec> frame_codec = std::make_unique<LengthPrefixedFrameCodec>(),
                     RpcRunnerCodecConfig codec_config = RpcRunnerCodecConfig(),
                     const std::string& node_id = "node-local");

  uint64_t nextMessageId();
  uint64_t nextCorrelationId();

  RpcStatus sendControlEx(uint64_t message_id,
                          uint64_t correlation_id,
                          const std::string& source,
                          const std::string& target,
                          RpcMessageType type,
                          const std::string& name,
                          const std::string& body,
                          const std::string& codec_id = {});

  RpcStatus sendDataBatchEx(uint64_t message_id,
                            uint64_t correlation_id,
                            const std::string& source,
                            const std::string& target,
                            const Table& table,
                            const std::string& codec_id = {});

  RpcStatus receiveFrameEx(RpcFrame* frame, int timeout_ms = 0);

  RpcStatus receiveControlEx(uint64_t correlation_id,
                            RpcControlMessage* message,
                            int timeout_ms = 1000);

  RpcStatus receiveDataBatchEx(uint64_t correlation_id, Table* out_table, int timeout_ms = 1000);

  bool sendControl(uint64_t message_id,
                   uint64_t correlation_id,
                   const std::string& source,
                   const std::string& target,
                   RpcMessageType type,
                   const std::string& name,
                   const std::string& body,
                   const std::string& codec_id = {});

  bool sendDataBatch(uint64_t message_id,
                     uint64_t correlation_id,
                     const std::string& source,
                     const std::string& target,
                     const Table& table,
                     const std::string& codec_id = {});

  bool receiveFrame(RpcFrame* frame, int timeout_ms = 0);
  bool receiveControl(uint64_t correlation_id,
                     RpcControlMessage* message,
                     int timeout_ms = 1000);
  bool receiveDataBatch(uint64_t correlation_id, Table* out_table, int timeout_ms = 1000);

 private:
  template <typename T>
  RpcStatus sendTypedMessageEx(const RpcEnvelope& envelope, const T& message);
  RpcStatus receiveOneFrameEx(RpcFrame* frame, int timeout_ms);
  static RpcStatus deserializeTyped(const RpcFrame& frame, RpcControlMessage* control);
  static RpcStatus deserializeTyped(const RpcFrame& frame, RpcDataBatchMessage* data_batch);

  std::shared_ptr<IByteTransport> transport_;
  std::unique_ptr<IRpcFrameCodec> frame_codec_;
  RpcRunnerCodecConfig codec_config_;
  std::string local_node_id_;
  mutable std::vector<uint8_t> receive_buffer_;
  mutable uint64_t message_seq_ = 0;
  mutable uint64_t correlation_seq_ = 0;
};

}  // namespace dataflow
