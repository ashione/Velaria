#pragma once

#include <cstdint>
#include <exception>
#include <sstream>
#include <string>
#include <utility>

#include "src/dataflow/core/execution/table.h"
#include "src/dataflow/core/logical/sql/sql_errors.h"

namespace dataflow {

enum class RpcStatusCode : uint8_t {
  Ok = 0,
  Timeout = 1,
  TransportClosed = 2,
  TransportError = 3,
  CodecMissing = 4,
  SerializeError = 5,
  DeserializeError = 6,
  FrameError = 7,
  VersionUnsupported = 8,
  TypeMismatch = 9,
  ArgumentError = 10,
  MissingInput = 11,
  NoWorkerAvailable = 12,
  SQLSyntaxError = 13,
  SQLSemanticError = 14,
  CatalogNotFound = 15,
  InvalidMessage = 16,
  UnknownError = 255,
};

struct RpcStatus {
  RpcStatusCode code = RpcStatusCode::Ok;
  std::string reason;

  bool ok() const { return code == RpcStatusCode::Ok; }
  bool isTimeout() const { return code == RpcStatusCode::Timeout; }
};

inline RpcStatus makeRpcStatus(RpcStatusCode code, std::string reason = {}) {
  return RpcStatus{code, std::move(reason)};
}

inline RpcStatus rpcOk() {
  return RpcStatus{};
}

inline const char* rpcStatusCodeName(RpcStatusCode code) {
  switch (code) {
    case RpcStatusCode::Ok:
      return "ok";
    case RpcStatusCode::Timeout:
      return "timeout";
    case RpcStatusCode::TransportClosed:
      return "transport-closed";
    case RpcStatusCode::TransportError:
      return "transport-error";
    case RpcStatusCode::CodecMissing:
      return "codec-missing";
    case RpcStatusCode::SerializeError:
      return "serialize-error";
    case RpcStatusCode::DeserializeError:
      return "deserialize-error";
    case RpcStatusCode::FrameError:
      return "frame-error";
    case RpcStatusCode::VersionUnsupported:
      return "version-unsupported";
    case RpcStatusCode::TypeMismatch:
      return "type-mismatch";
    case RpcStatusCode::ArgumentError:
      return "argument-error";
    case RpcStatusCode::MissingInput:
      return "missing-input";
    case RpcStatusCode::NoWorkerAvailable:
      return "no-worker-available";
    case RpcStatusCode::SQLSyntaxError:
      return "sql-syntax-error";
    case RpcStatusCode::SQLSemanticError:
      return "sql-semantic-error";
    case RpcStatusCode::CatalogNotFound:
      return "catalog-not-found";
    case RpcStatusCode::InvalidMessage:
      return "invalid-message";
    case RpcStatusCode::UnknownError:
      return "unknown-error";
    default:
      return "unknown-error";
  }
}

inline bool rpcStatusCodeFromString(const std::string& value, RpcStatusCode* code) {
  if (code == nullptr) return false;
  if (value == "ok") {
    *code = RpcStatusCode::Ok;
    return true;
  }
  if (value == "timeout") {
    *code = RpcStatusCode::Timeout;
    return true;
  }
  if (value == "transport-closed") {
    *code = RpcStatusCode::TransportClosed;
    return true;
  }
  if (value == "transport-error") {
    *code = RpcStatusCode::TransportError;
    return true;
  }
  if (value == "codec-missing") {
    *code = RpcStatusCode::CodecMissing;
    return true;
  }
  if (value == "serialize-error") {
    *code = RpcStatusCode::SerializeError;
    return true;
  }
  if (value == "deserialize-error") {
    *code = RpcStatusCode::DeserializeError;
    return true;
  }
  if (value == "frame-error") {
    *code = RpcStatusCode::FrameError;
    return true;
  }
  if (value == "version-unsupported") {
    *code = RpcStatusCode::VersionUnsupported;
    return true;
  }
  if (value == "type-mismatch") {
    *code = RpcStatusCode::TypeMismatch;
    return true;
  }
  if (value == "argument-error") {
    *code = RpcStatusCode::ArgumentError;
    return true;
  }
  if (value == "missing-input") {
    *code = RpcStatusCode::MissingInput;
    return true;
  }
  if (value == "no-worker-available") {
    *code = RpcStatusCode::NoWorkerAvailable;
    return true;
  }
  if (value == "sql-syntax-error") {
    *code = RpcStatusCode::SQLSyntaxError;
    return true;
  }
  if (value == "sql-semantic-error") {
    *code = RpcStatusCode::SQLSemanticError;
    return true;
  }
  if (value == "catalog-not-found") {
    *code = RpcStatusCode::CatalogNotFound;
    return true;
  }
  if (value == "invalid-message") {
    *code = RpcStatusCode::InvalidMessage;
    return true;
  }
  if (value == "unknown-error") {
    *code = RpcStatusCode::UnknownError;
    return true;
  }
  return false;
}

inline RpcStatus rpcStatusFromException(const std::exception& ex) {
  if (dynamic_cast<const SQLSyntaxError*>(&ex) != nullptr) {
    return makeRpcStatus(RpcStatusCode::SQLSyntaxError, ex.what());
  }
  if (dynamic_cast<const SQLSemanticError*>(&ex) != nullptr) {
    return makeRpcStatus(RpcStatusCode::SQLSemanticError, ex.what());
  }
  if (dynamic_cast<const CatalogNotFoundError*>(&ex) != nullptr) {
    return makeRpcStatus(RpcStatusCode::CatalogNotFound, ex.what());
  }
  return makeRpcStatus(RpcStatusCode::UnknownError, ex.what());
}

inline bool rpcStatusToBool(const RpcStatus& status) {
  return status.ok();
}

struct ActorTaskCommand {
  static constexpr const char* kRunTask = "RunTask";
  static constexpr const char* kTaskAck = "TaskAck";
  static constexpr const char* kTaskResult = "TaskResult";
  static constexpr const char* kTaskError = "TaskError";
  static constexpr const char* kHeartbeat = "Heartbeat";
};

struct ActorTaskStatus {
  static constexpr const char* kOk = "ok";
  static constexpr const char* kError = "error";
  static constexpr const char* kIdentity = "identity";
};

struct ActorTaskMessage {
  std::string task_id;
  std::string status;
  std::string detail;
  size_t rows = 0;

  std::string encode() const;
  static ActorTaskMessage decode(const std::string& body);
};

inline std::string ActorTaskMessage::encode() const {
  std::ostringstream out;
  out << "task_id=" << task_id;
  out << ";status=" << status;
  out << ";rows=" << rows;
  if (!detail.empty()) out << ";detail=" << detail;
  return out.str();
}

inline ActorTaskMessage ActorTaskMessage::decode(const std::string& body) {
  ActorTaskMessage msg;
  size_t pos = 0;
  while (pos < body.size()) {
    const size_t sep = body.find(';', pos);
    const auto token = body.substr(pos, sep == std::string::npos ? std::string::npos : sep - pos);
    const size_t kv = token.find('=');
    if (kv == std::string::npos) break;
    const std::string key = token.substr(0, kv);
    const std::string val = token.substr(kv + 1);
    if (key == "task_id") msg.task_id = val;
    if (key == "status") msg.status = val;
    if (key == "detail") msg.detail = val;
    if (key == "rows") {
      try {
        msg.rows = static_cast<size_t>(std::stoull(val));
      } catch (...) {
        msg.rows = 0;
      }
    }
    if (sep == std::string::npos) break;
    pos = sep + 1;
  }
  return msg;
}

}  // namespace dataflow
