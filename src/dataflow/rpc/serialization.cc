#include "src/dataflow/rpc/serialization.h"

#include <cctype>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "src/dataflow/rpc/actor_rpc_codec.h"

namespace dataflow {

namespace {

std::string jsonEscape(const std::string& value) {
  std::string out;
  out.reserve(value.size());
  for (char ch : value) {
    switch (ch) {
      case '\\':
        out += "\\\\";
        break;
      case '"':
        out += "\\\"";
        break;
      case '\n':
        out += "\\n";
        break;
      case '\r':
        out += "\\r";
        break;
      case '\t':
        out += "\\t";
        break;
      default:
        out.push_back(ch);
        break;
    }
  }
  return out;
}

void skipWs(const std::string& text, size_t* pos) {
  while (*pos < text.size() &&
         std::isspace(static_cast<unsigned char>(text[*pos])) != 0) {
    ++(*pos);
  }
}

bool consume(const std::string& text, size_t* pos, char expected) {
  skipWs(text, pos);
  if (*pos >= text.size() || text[*pos] != expected) return false;
  ++(*pos);
  return true;
}

bool parseJsonString(const std::string& text, size_t* pos, std::string* out) {
  if (out == nullptr) return false;
  skipWs(text, pos);
  if (*pos >= text.size() || text[*pos] != '"') return false;
  ++(*pos);

  std::string value;
  while (*pos < text.size()) {
    const char ch = text[*pos];
    ++(*pos);
    if (ch == '"') {
      *out = value;
      return true;
    }
    if (ch == '\\') {
      if (*pos >= text.size()) return false;
      const char escaped = text[*pos];
      ++(*pos);
      switch (escaped) {
        case '\\':
        case '"':
        case '/':
          value.push_back(escaped);
          break;
        case 'n':
          value.push_back('\n');
          break;
        case 'r':
          value.push_back('\r');
          break;
        case 't':
          value.push_back('\t');
          break;
        default:
          return false;
      }
      continue;
    }
    value.push_back(ch);
  }
  return false;
}

bool parseJsonBool(const std::string& text, size_t* pos, bool* out) {
  if (out == nullptr) return false;
  skipWs(text, pos);
  if (text.compare(*pos, 4, "true") == 0) {
    *out = true;
    *pos += 4;
    return true;
  }
  if (text.compare(*pos, 5, "false") == 0) {
    *out = false;
    *pos += 5;
    return true;
  }
  return false;
}

bool parseFlatJsonObject(const std::string& text,
                         std::unordered_map<std::string, std::string>* string_values,
                         std::unordered_map<std::string, bool>* bool_values) {
  if (string_values == nullptr || bool_values == nullptr) return false;
  size_t pos = 0;
  if (!consume(text, &pos, '{')) return false;
  skipWs(text, &pos);
  if (pos < text.size() && text[pos] == '}') {
    ++pos;
    skipWs(text, &pos);
    return pos == text.size();
  }

  while (pos < text.size()) {
    std::string key;
    if (!parseJsonString(text, &pos, &key)) return false;
    if (!consume(text, &pos, ':')) return false;

    skipWs(text, &pos);
    if (pos >= text.size()) return false;
    if (text[pos] == '"') {
      std::string value;
      if (!parseJsonString(text, &pos, &value)) return false;
      (*string_values)[key] = std::move(value);
    } else {
      bool value = false;
      if (!parseJsonBool(text, &pos, &value)) return false;
      (*bool_values)[key] = value;
    }

    skipWs(text, &pos);
    if (pos >= text.size()) return false;
    if (text[pos] == '}') {
      ++pos;
      skipWs(text, &pos);
      return pos == text.size();
    }
    if (text[pos] != ',') return false;
    ++pos;
  }
  return false;
}

std::string getString(const std::unordered_map<std::string, std::string>& values,
                      const std::string& key) {
  const auto it = values.find(key);
  return it == values.end() ? std::string() : it->second;
}

bool getBool(const std::unordered_map<std::string, bool>& values,
             const std::string& key,
             bool fallback) {
  const auto it = values.find(key);
  return it == values.end() ? fallback : it->second;
}

class JsonMessageSerializer : public IMessageSerializer {
 public:
  std::string serializerId() const override { return "actor-rpc-json-v1"; }

  std::string messageType() const override { return "actor-rpc"; }

  RpcStatus encode(const ActorRpcMessage& message,
                   std::vector<uint8_t>* out) const override {
    if (out == nullptr) {
      return makeRpcStatus(RpcStatusCode::ArgumentError,
                           "actor rpc encode output is null");
    }

    const RpcStatus status = actorRpcMessageStatus(message);
    std::ostringstream encoded;
    encoded << "{"
            << "\"message_type\":\"" << jsonEscape(messageType()) << "\"," 
            << "\"serializer_id\":\"" << jsonEscape(serializerId()) << "\"," 
            << "\"action\":\"" << jsonEscape(actorRpcActionToString(message.action)) << "\"," 
            << "\"job_id\":\"" << jsonEscape(message.job_id) << "\"," 
            << "\"chain_id\":\"" << jsonEscape(message.chain_id) << "\"," 
            << "\"task_id\":\"" << jsonEscape(message.task_id) << "\"," 
            << "\"node_id\":\"" << jsonEscape(message.node_id) << "\"," 
            << "\"attempt\":\"" << message.attempt << "\"," 
            << "\"heartbeat_seq\":\"" << message.heartbeat_seq << "\"," 
            << "\"output_rows\":\"" << message.output_rows << "\"," 
            << "\"ok\":" << (status.ok() ? "true" : "false") << ","
            << "\"status_code\":\"" << jsonEscape(rpcStatusCodeName(status.code)) << "\"," 
            << "\"status_message\":\"" << jsonEscape(status.reason) << "\"," 
            << "\"state\":\"" << jsonEscape(message.state) << "\"," 
            << "\"summary\":\"" << jsonEscape(message.summary) << "\"," 
            << "\"result_location\":\"" << jsonEscape(message.result_location) << "\"," 
            << "\"reason\":\"" << jsonEscape(message.reason) << "\"," 
            << "\"payload\":\"" << jsonEscape(message.payload) << "\""
            << "}";

    const std::string payload = encoded.str();
    out->assign(payload.begin(), payload.end());
    return rpcOk();
  }

  RpcStatus decode(const std::vector<uint8_t>& bytes,
                   ActorRpcMessage* message) const override {
    if (message == nullptr) {
      return makeRpcStatus(RpcStatusCode::ArgumentError,
                           "actor rpc decode output is null");
    }

    const std::string raw(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    std::unordered_map<std::string, std::string> string_values;
    std::unordered_map<std::string, bool> bool_values;
    if (!parseFlatJsonObject(raw, &string_values, &bool_values)) {
      return makeRpcStatus(RpcStatusCode::InvalidMessage,
                           "actor rpc payload is not valid json");
    }
    if (getString(string_values, "message_type") != messageType()) {
      return makeRpcStatus(RpcStatusCode::TypeMismatch,
                           "unexpected actor rpc message type");
    }

    ActorRpcMessage decoded;
    if (!actorRpcActionFromString(getString(string_values, "action"), &decoded.action)) {
      return makeRpcStatus(RpcStatusCode::InvalidMessage,
                           "actor rpc action is invalid");
    }
    decoded.job_id = getString(string_values, "job_id");
    decoded.chain_id = getString(string_values, "chain_id");
    decoded.task_id = getString(string_values, "task_id");
    decoded.node_id = getString(string_values, "node_id");
    decoded.attempt = static_cast<uint32_t>(
        getString(string_values, "attempt").empty() ? 0 : std::stoul(getString(string_values, "attempt")));
    decoded.heartbeat_seq = static_cast<uint64_t>(
        getString(string_values, "heartbeat_seq").empty() ? 0 : std::stoull(getString(string_values, "heartbeat_seq")));
    decoded.output_rows = static_cast<uint64_t>(
        getString(string_values, "output_rows").empty() ? 0 : std::stoull(getString(string_values, "output_rows")));
    decoded.state = getString(string_values, "state");
    decoded.summary = getString(string_values, "summary");
    decoded.result_location = getString(string_values, "result_location");
    decoded.payload = getString(string_values, "payload");
    decoded.reason = getString(string_values, "reason");

    const std::string status_text = getString(string_values, "status_code");
    RpcStatusCode status_code = RpcStatusCode::Ok;
    if (!status_text.empty()) {
      if (!rpcStatusCodeFromString(status_text, &status_code)) {
        return makeRpcStatus(RpcStatusCode::InvalidMessage,
                             "actor rpc status_code is invalid");
      }
    } else if (!getBool(bool_values, "ok", true)) {
      status_code = RpcStatusCode::UnknownError;
    }

    setActorRpcMessageStatus(
        &decoded,
        makeRpcStatus(status_code, getString(string_values, "status_message")));
    if (status_code == RpcStatusCode::Ok &&
        bool_values.find("ok") != bool_values.end() &&
        !getBool(bool_values, "ok", true)) {
      setActorRpcMessageStatus(
          &decoded,
          makeRpcStatus(RpcStatusCode::UnknownError,
                        getString(string_values, "status_message")));
    }

    *message = decoded;
    return rpcOk();
  }
};

}  // namespace

std::unique_ptr<IMessageSerializer> makeJsonMessageSerializer() {
  return std::unique_ptr<IMessageSerializer>(new JsonMessageSerializer());
}

}  // namespace dataflow
