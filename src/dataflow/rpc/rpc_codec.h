#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "src/dataflow/core/table.h"

namespace dataflow {

enum class RpcMessageType : uint8_t {
  Control = 0,
  DataBatch = 1,
  DataAck = 2,
  Heartbeat = 3,
  Error = 4,
};

struct RpcEnvelope {
  uint8_t protocol_version = 1;
  RpcMessageType type = RpcMessageType::Control;
  uint64_t message_id = 0;
  uint64_t correlation_id = 0;
  std::string codec_id;
  std::string source;
  std::string target;
};

struct RpcFrame {
  RpcEnvelope header;
  std::vector<uint8_t> payload;
};

struct RpcControlMessage {
  std::string name;
  std::string body;
};

struct RpcDataBatchMessage {
  Table table;
};

class IRpcSerializer {
 public:
  virtual ~IRpcSerializer() = default;
  virtual std::string codec_id() const = 0;
  virtual std::vector<uint8_t> serialize(const RpcEnvelope& envelope,
                                        const void* message) const = 0;
  virtual bool deserialize(const RpcEnvelope& envelope,
                          const std::vector<uint8_t>& payload,
                          void* out_message) const = 0;
};

class RpcSerializerRegistry {
 public:
  static RpcSerializerRegistry& instance();

  void registerSerializer(std::unique_ptr<IRpcSerializer> serializer);
  const IRpcSerializer* find(const std::string& codec_id) const;
  std::vector<std::string> codecIds() const;

 private:
  RpcSerializerRegistry() = default;
  std::unordered_map<std::string, std::unique_ptr<IRpcSerializer>> codecs_;
};

class IRpcFrameCodec {
 public:
  virtual ~IRpcFrameCodec() = default;
  virtual std::vector<uint8_t> encode(const RpcFrame& frame) const = 0;
  virtual bool decode(const std::vector<uint8_t>& bytes,
                      RpcFrame* frame,
                      size_t* consumed_bytes) const = 0;
};

class LengthPrefixedFrameCodec : public IRpcFrameCodec {
 public:
  std::vector<uint8_t> encode(const RpcFrame& frame) const override;
  bool decode(const std::vector<uint8_t>& bytes,
              RpcFrame* frame,
              size_t* consumed_bytes) const override;
};

void registerBuiltinRpcSerializers();
std::unique_ptr<IRpcSerializer> makeJsonControlRpcSerializer();
std::unique_ptr<IRpcSerializer> makeTableRpcSerializer();

}  // namespace dataflow
