#include "src/dataflow/runtime/rpc_runner.h"

#include <string>

namespace dataflow {

namespace {

const std::string& pickCodecId(const std::string& user_value,
                               const std::string& default_value) {
  return user_value.empty() ? default_value : user_value;
}

RpcStatus statusTimeout(int timeout_ms, const char* op) {
  if (timeout_ms == 0) {
    return {RpcStatusCode::Timeout, std::string(op) + " timeout"};
  }
  return {RpcStatusCode::Timeout, std::string(op) + " timeout"};
}

}  // namespace

RpcRunner::RpcRunner(std::shared_ptr<IByteTransport> transport,
                     std::unique_ptr<IRpcFrameCodec> frame_codec,
                     RpcRunnerCodecConfig codec_config,
                     const std::string& node_id)
    : transport_(std::move(transport)),
      frame_codec_(std::move(frame_codec)),
      codec_config_(std::move(codec_config)),
      local_node_id_(node_id) {
  registerBuiltinRpcSerializers();
}

uint64_t RpcRunner::nextMessageId() {
  return ++message_seq_;
}

uint64_t RpcRunner::nextCorrelationId() {
  return ++correlation_seq_;
}

RpcStatus RpcRunner::sendControlEx(uint64_t message_id,
                                   uint64_t correlation_id,
                                   const std::string& source,
                                   const std::string& target,
                                   RpcMessageType type,
                                   const std::string& name,
                                   const std::string& body,
                                   const std::string& codec_id) {
  RpcEnvelope env;
  env.protocol_version = codec_config_.protocol_version;
  env.type = type;
  env.message_id = message_id;
  env.correlation_id = correlation_id;
  env.codec_id = pickCodecId(codec_id, codec_config_.control_codec_id);
  env.source = source.empty() ? local_node_id_ : source;
  env.target = target;

  RpcControlMessage message{name, body};
  return sendTypedMessageEx(env, message);
}

RpcStatus RpcRunner::sendDataBatchEx(uint64_t message_id,
                                     uint64_t correlation_id,
                                     const std::string& source,
                                     const std::string& target,
                                     const Table& table,
                                     const std::string& codec_id) {
  RpcEnvelope env;
  env.protocol_version = codec_config_.protocol_version;
  env.type = RpcMessageType::DataBatch;
  env.message_id = message_id;
  env.correlation_id = correlation_id;
  env.codec_id = pickCodecId(codec_id, codec_config_.data_codec_id);
  env.source = source.empty() ? local_node_id_ : source;
  env.target = target;

  RpcDataBatchMessage message{table};
  return sendTypedMessageEx(env, message);
}

bool RpcRunner::sendControl(uint64_t message_id,
                            uint64_t correlation_id,
                            const std::string& source,
                            const std::string& target,
                            RpcMessageType type,
                            const std::string& name,
                            const std::string& body,
                            const std::string& codec_id) {
  return sendControlEx(message_id, correlation_id, source, target, type, name, body,
                      codec_id)
      .ok();
}

bool RpcRunner::sendDataBatch(uint64_t message_id,
                              uint64_t correlation_id,
                              const std::string& source,
                              const std::string& target,
                              const Table& table,
                              const std::string& codec_id) {
  return sendDataBatchEx(message_id, correlation_id, source, target, table, codec_id)
      .ok();
}

template <typename T>
RpcStatus RpcRunner::sendTypedMessageEx(const RpcEnvelope& envelope, const T& message) {
  if (!transport_) {
    return {RpcStatusCode::TransportClosed, "transport not configured"};
  }
  if (!frame_codec_) {
    return {RpcStatusCode::UnknownError, "frame codec not configured"};
  }

  const auto* codec = RpcSerializerRegistry::instance().find(envelope.codec_id);
  if (!codec) {
    return {RpcStatusCode::CodecMissing, "codec missing: " + envelope.codec_id};
  }

  RpcFrame frame;
  frame.header = envelope;
  frame.payload = codec->serialize(envelope, &message);
  if (frame.payload.empty()) {
    return {RpcStatusCode::SerializeError, "serializer output empty"};
  }

  const auto encoded = frame_codec_->encode(frame);
  if (encoded.empty()) {
    return {RpcStatusCode::FrameError, "frame encode failed"};
  }
  if (!transport_->writeBytes(encoded)) {
    return {RpcStatusCode::TransportError, "transport write failed"};
  }
  return {RpcStatusCode::Ok, ""};
}

RpcStatus RpcRunner::receiveOneFrameEx(RpcFrame* frame, int timeout_ms) {
  if (!frame) {
    return {RpcStatusCode::ArgumentError, "output frame is null"};
  }
  if (!transport_) {
    return {RpcStatusCode::TransportClosed, "transport not configured"};
  }
  if (!frame_codec_) {
    return {RpcStatusCode::UnknownError, "frame codec not configured"};
  }

  while (true) {
    size_t consumed = 0;
    if (frame_codec_->decode(receive_buffer_, frame, &consumed)) {
      if (frame->header.protocol_version != codec_config_.protocol_version) {
        return {RpcStatusCode::VersionUnsupported, "unsupported protocol version"};
      }
      if (consumed > 0 && consumed <= receive_buffer_.size()) {
        receive_buffer_.erase(receive_buffer_.begin(),
                             receive_buffer_.begin() +
                                 static_cast<std::vector<uint8_t>::difference_type>(consumed));
      }
      return {RpcStatusCode::Ok, ""};
    }

    std::vector<uint8_t> chunk;
    if (!transport_->readBytes(&chunk, timeout_ms)) {
      return statusTimeout(timeout_ms, "receive");
    }
    receive_buffer_.insert(receive_buffer_.end(), chunk.begin(), chunk.end());
  }
}

RpcStatus RpcRunner::receiveFrameEx(RpcFrame* frame, int timeout_ms) {
  return receiveOneFrameEx(frame, timeout_ms);
}

RpcStatus RpcRunner::deserializeTyped(const RpcFrame& frame, RpcControlMessage* control) {
  if (!control) {
    return {RpcStatusCode::ArgumentError, "output control message is null"};
  }
  const auto* codec = RpcSerializerRegistry::instance().find(frame.header.codec_id);
  if (!codec) {
    return {RpcStatusCode::CodecMissing, "codec missing: " + frame.header.codec_id};
  }
  if (!codec->deserialize(frame.header, frame.payload, control)) {
    return {RpcStatusCode::DeserializeError, "control deserialize failed"};
  }
  return {RpcStatusCode::Ok, ""};
}

RpcStatus RpcRunner::deserializeTyped(const RpcFrame& frame, RpcDataBatchMessage* data_batch) {
  if (!data_batch) {
    return {RpcStatusCode::ArgumentError, "output table message is null"};
  }
  const auto* codec = RpcSerializerRegistry::instance().find(frame.header.codec_id);
  if (!codec) {
    return {RpcStatusCode::CodecMissing, "codec missing: " + frame.header.codec_id};
  }
  if (!codec->deserialize(frame.header, frame.payload, data_batch)) {
    return {RpcStatusCode::DeserializeError, "data batch deserialize failed"};
  }
  return {RpcStatusCode::Ok, ""};
}

RpcStatus RpcRunner::receiveControlEx(uint64_t correlation_id,
                                      RpcControlMessage* message,
                                      int timeout_ms) {
  if (!message) {
    return {RpcStatusCode::ArgumentError, "output control is null"};
  }

  RpcFrame frame;
  while (true) {
    const auto frame_status = receiveOneFrameEx(&frame, timeout_ms);
    if (!frame_status.ok()) {
      return frame_status;
    }
    if (frame.header.type != RpcMessageType::Control) {
      continue;
    }
    if (frame.header.correlation_id != correlation_id) {
      continue;
    }
    const auto deserialize_status = deserializeTyped(frame, message);
    if (!deserialize_status.ok()) {
      return deserialize_status;
    }
    return {RpcStatusCode::Ok, ""};
  }
}

bool RpcRunner::receiveControl(uint64_t correlation_id,
                               RpcControlMessage* message,
                               int timeout_ms) {
  return receiveControlEx(correlation_id, message, timeout_ms).ok();
}

RpcStatus RpcRunner::receiveDataBatchEx(uint64_t correlation_id, Table* out_table, int timeout_ms) {
  if (!out_table) {
    return {RpcStatusCode::ArgumentError, "output table is null"};
  }

  RpcFrame frame;
  while (true) {
    const auto frame_status = receiveOneFrameEx(&frame, timeout_ms);
    if (!frame_status.ok()) {
      return frame_status;
    }
    if (frame.header.type != RpcMessageType::DataBatch) {
      continue;
    }
    if (frame.header.correlation_id != correlation_id) {
      continue;
    }
    RpcDataBatchMessage batch;
    const auto deserialize_status = deserializeTyped(frame, &batch);
    if (!deserialize_status.ok()) {
      return deserialize_status;
    }
    *out_table = std::move(batch.table);
    return {RpcStatusCode::Ok, ""};
  }
}

bool RpcRunner::receiveDataBatch(uint64_t correlation_id, Table* out_table, int timeout_ms) {
  return receiveDataBatchEx(correlation_id, out_table, timeout_ms).ok();
}

bool RpcRunner::receiveFrame(RpcFrame* frame, int timeout_ms) {
  return receiveFrameEx(frame, timeout_ms).ok();
}

}  // namespace dataflow
