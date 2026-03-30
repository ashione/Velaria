#include "src/dataflow/rpc/rpc_codec.h"

#include <cstddef>
#include <cstdint>
#include <sstream>
#include <string>

#include "src/dataflow/stream/binary_row_batch.h"

namespace dataflow {

namespace {

void appendByte(std::vector<uint8_t>* out, uint8_t value) { out->push_back(value); }

void writeU32(std::vector<uint8_t>* out, uint32_t value) {
  out->push_back(static_cast<uint8_t>(value & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 16) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 24) & 0xFF));
}

void writeU64(std::vector<uint8_t>* out, uint64_t value) {
  out->push_back(static_cast<uint8_t>(value & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 16) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 24) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 32) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 40) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 48) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 56) & 0xFF));
}

bool readU32(const std::vector<uint8_t>& src, size_t offset, uint32_t* value) {
  if (offset + 4 > src.size()) return false;
  *value = static_cast<uint32_t>(src[offset]) |
           static_cast<uint32_t>(src[offset + 1]) << 8 |
           static_cast<uint32_t>(src[offset + 2]) << 16 |
           static_cast<uint32_t>(src[offset + 3]) << 24;
  return true;
}

bool readU64(const std::vector<uint8_t>& src, size_t offset, uint64_t* value) {
  if (offset + 8 > src.size()) return false;
  *value = static_cast<uint64_t>(src[offset]) |
           static_cast<uint64_t>(src[offset + 1]) << 8 |
           static_cast<uint64_t>(src[offset + 2]) << 16 |
           static_cast<uint64_t>(src[offset + 3]) << 24 |
           static_cast<uint64_t>(src[offset + 4]) << 32 |
           static_cast<uint64_t>(src[offset + 5]) << 40 |
           static_cast<uint64_t>(src[offset + 6]) << 48 |
           static_cast<uint64_t>(src[offset + 7]) << 56;
  return true;
}

void writeString(std::vector<uint8_t>* out, const std::string& value) {
  writeU32(out, static_cast<uint32_t>(value.size()));
  out->insert(out->end(), value.begin(), value.end());
}

bool readString(const std::vector<uint8_t>& src,
                size_t offset,
                std::string* value,
                size_t* next_offset) {
  uint32_t size = 0;
  if (!readU32(src, offset, &size)) return false;
  offset += 4;
  if (offset + size > src.size()) return false;
  value->assign(reinterpret_cast<const char*>(src.data() + offset), size);
  *next_offset = offset + size;
  return true;
}

std::string escapeJson(const std::string& input) {
  std::ostringstream out;
  for (char ch : input) {
    switch (ch) {
      case '\\':
        out << "\\\\";
        break;
      case '"':
        out << "\\\"";
        break;
      case '\n':
        out << "\\n";
        break;
      case '\r':
        out << "\\r";
        break;
      default:
        out << ch;
        break;
    }
  }
  return out.str();
}

std::string extractJsonString(const std::string& source,
                             const std::string& key,
                             bool* ok) {
  *ok = false;
  const std::string key_token = "\"" + key + "\":\"";
  const size_t key_pos = source.find(key_token);
  if (key_pos == std::string::npos) return {};
  size_t start = key_pos + key_token.size();
  size_t end = start;
  while (end < source.size()) {
    if (source[end] == '\\') {
      end += 2;
      continue;
    }
    if (source[end] == '"') break;
    ++end;
  }
  if (end >= source.size()) return {};
  std::string raw = source.substr(start, end - start);
  std::string result;
  for (size_t i = 0; i < raw.size(); ++i) {
    if (raw[i] == '\\' && i + 1 < raw.size()) {
      const char next = raw[i + 1];
      if (next == '\\' || next == '"') {
        result.push_back(next);
        ++i;
        continue;
      }
      if (next == 'n') {
        result.push_back('\n');
        ++i;
        continue;
      }
      if (next == 'r') {
        result.push_back('\r');
        ++i;
        continue;
      }
    }
    result.push_back(raw[i]);
  }
  *ok = true;
  return result;
}

class JsonControlRpcSerializer : public IRpcSerializer {
 public:
  std::string codec_id() const override { return "json-control-v1"; }

  std::vector<uint8_t> serialize(const RpcEnvelope& envelope,
                                const void* message) const override {
    if (envelope.type != RpcMessageType::Control) return {};
    const auto* control = static_cast<const RpcControlMessage*>(message);
    if (control == nullptr) return {};
    const std::string content =
        "{\"name\":\"" + escapeJson(control->name) + "\",\"body\":\"" +
        escapeJson(control->body) + "\"}";
    return std::vector<uint8_t>(content.begin(), content.end());
  }

  bool deserialize(const RpcEnvelope& envelope,
                   const std::vector<uint8_t>& payload,
                   void* out_message) const override {
    if (envelope.type != RpcMessageType::Control) return false;
    auto* control = static_cast<RpcControlMessage*>(out_message);
    if (control == nullptr) return false;
    const std::string payload_str(reinterpret_cast<const char*>(payload.data()),
                                 payload.size());
    bool ok_name = false;
    bool ok_body = false;
    control->name = extractJsonString(payload_str, "name", &ok_name);
    control->body = extractJsonString(payload_str, "body", &ok_body);
    return ok_name && ok_body;
  }
};

class TableBatchRpcSerializer : public IRpcSerializer {
 public:
  std::string codec_id() const override { return "table-bin-v1"; }

  std::vector<uint8_t> serialize(const RpcEnvelope& envelope,
                                const void* message) const override {
    if (envelope.type != RpcMessageType::DataBatch) return {};
    const auto* batch = static_cast<const RpcDataBatchMessage*>(message);
    if (batch == nullptr) return {};
    BinaryRowBatchCodec serializer;
    std::vector<uint8_t> payload;
    serializer.serialize(batch->table, &payload);
    return payload;
  }

  bool deserialize(const RpcEnvelope& envelope,
                   const std::vector<uint8_t>& payload,
                   void* out_message) const override {
    if (envelope.type != RpcMessageType::DataBatch) return false;
    auto* batch = static_cast<RpcDataBatchMessage*>(out_message);
    if (batch == nullptr) return false;
    BinaryRowBatchCodec serializer;
    batch->table = serializer.deserialize(payload);
    return true;
  }
};

}  // namespace

RpcSerializerRegistry& RpcSerializerRegistry::instance() {
  static RpcSerializerRegistry registry;
  return registry;
}

void RpcSerializerRegistry::registerSerializer(
    std::unique_ptr<IRpcSerializer> serializer) {
  if (!serializer) return;
  codecs_[serializer->codec_id()] = std::move(serializer);
}

const IRpcSerializer* RpcSerializerRegistry::find(
    const std::string& codec_id) const {
  auto it = codecs_.find(codec_id);
  if (it == codecs_.end()) return nullptr;
  return it->second.get();
}

std::vector<std::string> RpcSerializerRegistry::codecIds() const {
  std::vector<std::string> ids;
  ids.reserve(codecs_.size());
  for (const auto& kv : codecs_) {
    ids.push_back(kv.first);
  }
  return ids;
}

std::vector<uint8_t> LengthPrefixedFrameCodec::encode(const RpcFrame& frame) const {
  std::vector<uint8_t> bytes;
  encodeInto(frame, &bytes);
  return bytes;
}

void LengthPrefixedFrameCodec::encodeInto(const RpcFrame& frame, std::vector<uint8_t>* bytes) const {
  if (bytes == nullptr) return;
  std::vector<uint8_t> body;
  body.reserve(64 + frame.payload.size());
  appendByte(&body, frame.header.protocol_version);
  appendByte(&body, static_cast<uint8_t>(frame.header.type));
  writeU64(&body, frame.header.message_id);
  writeU64(&body, frame.header.correlation_id);
  writeString(&body, frame.header.codec_id);
  writeString(&body, frame.header.source);
  writeString(&body, frame.header.target);
  writeU32(&body, static_cast<uint32_t>(frame.payload.size()));
  body.insert(body.end(), frame.payload.begin(), frame.payload.end());

  bytes->clear();
  bytes->reserve(4 + body.size());
  writeU32(bytes, static_cast<uint32_t>(body.size()));
  bytes->insert(bytes->end(), body.begin(), body.end());
}

bool LengthPrefixedFrameCodec::decode(const std::vector<uint8_t>& bytes,
                                     RpcFrame* frame,
                                     size_t* consumed_bytes) const {
  if (consumed_bytes) *consumed_bytes = 0;
  if (!frame) return false;
  if (bytes.size() < 4) return false;

  uint32_t total = 0;
  if (!readU32(bytes, 0, &total)) return false;
  if (bytes.size() < 4 + total) return false;

  size_t offset = 4;
  if (offset >= bytes.size()) return false;
  frame->header.protocol_version = bytes[offset++];
  if (offset >= bytes.size()) return false;
  frame->header.type = static_cast<RpcMessageType>(bytes[offset++]);
  if (!readU64(bytes, offset, &frame->header.message_id)) return false;
  offset += 8;
  if (!readU64(bytes, offset, &frame->header.correlation_id)) return false;
  offset += 8;

  size_t next = 0;
  if (!readString(bytes, offset, &frame->header.codec_id, &next)) return false;
  offset = next;
  if (!readString(bytes, offset, &frame->header.source, &next)) return false;
  offset = next;
  if (!readString(bytes, offset, &frame->header.target, &next)) return false;
  offset = next;

  uint32_t payload_size = 0;
  if (!readU32(bytes, offset, &payload_size)) return false;
  offset += 4;
  if (offset + payload_size > bytes.size()) return false;
  frame->payload.assign(
      bytes.begin() + static_cast<std::vector<uint8_t>::difference_type>(offset),
      bytes.begin() +
          static_cast<std::vector<uint8_t>::difference_type>(offset + payload_size));
  if (consumed_bytes) *consumed_bytes = 4 + total;
  return true;
}

void registerBuiltinRpcSerializers() {
  auto& registry = RpcSerializerRegistry::instance();
  if (registry.find("json-control-v1") == nullptr) {
    registry.registerSerializer(makeJsonControlRpcSerializer());
  }
  if (registry.find("table-bin-v1") == nullptr) {
    registry.registerSerializer(makeTableRpcSerializer());
  }
}

std::unique_ptr<IRpcSerializer> makeJsonControlRpcSerializer() {
  return std::unique_ptr<IRpcSerializer>(new JsonControlRpcSerializer());
}

std::unique_ptr<IRpcSerializer> makeTableRpcSerializer() {
  return std::unique_ptr<IRpcSerializer>(new TableBatchRpcSerializer());
}

}  // namespace dataflow
