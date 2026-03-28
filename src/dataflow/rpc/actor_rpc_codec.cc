#include "src/dataflow/rpc/actor_rpc_codec.h"

#include <vector>

#include "src/dataflow/rpc/serialization.h"

namespace dataflow {

const char* actorRpcActionToString(ActorRpcAction action) {
  switch (action) {
    case ActorRpcAction::RegisterWorker:
      return "register";
    case ActorRpcAction::SubmitJob:
      return "submit";
    case ActorRpcAction::Ack:
      return "ack";
    case ActorRpcAction::Result:
      return "result";
    case ActorRpcAction::Heartbeat:
      return "heartbeat";
    default:
      return "unknown";
  }
}

bool actorRpcActionFromString(const std::string& value, ActorRpcAction* action) {
  if (!action) return false;
  if (value == "register") {
    *action = ActorRpcAction::RegisterWorker;
    return true;
  }
  if (value == "submit") {
    *action = ActorRpcAction::SubmitJob;
    return true;
  }
  if (value == "ack") {
    *action = ActorRpcAction::Ack;
    return true;
  }
  if (value == "result") {
    *action = ActorRpcAction::Result;
    return true;
  }
  if (value == "heartbeat") {
    *action = ActorRpcAction::Heartbeat;
    return true;
  }
  return false;
}

RpcStatus actorRpcMessageStatus(const ActorRpcMessage& message) {
  if (message.status.code != RpcStatusCode::Ok || !message.status.reason.empty()) {
    return message.status;
  }
  if (message.ok) {
    return makeRpcStatus(RpcStatusCode::Ok, message.reason);
  }
  if (message.reason == "no-worker-available") {
    return makeRpcStatus(RpcStatusCode::NoWorkerAvailable, message.reason);
  }
  return makeRpcStatus(RpcStatusCode::UnknownError, message.reason);
}

void setActorRpcMessageStatus(ActorRpcMessage* message, const RpcStatus& status) {
  if (message == nullptr) return;
  message->status = status;
  message->ok = status.ok();
  message->reason = status.reason;
}

RpcStatus encodeActorRpcMessage(const IMessageSerializer& serializer,
                                const ActorRpcMessage& message,
                                std::vector<uint8_t>* out) {
  if (out == nullptr) {
    return makeRpcStatus(RpcStatusCode::ArgumentError,
                         "actor rpc encode output is null");
  }
  ActorRpcMessage normalized = message;
  setActorRpcMessageStatus(&normalized, actorRpcMessageStatus(message));
  return serializer.encode(normalized, out);
}

RpcStatus decodeActorRpcMessage(const IMessageSerializer& serializer,
                                const std::vector<uint8_t>& bytes,
                                ActorRpcMessage* message) {
  if (message == nullptr) {
    return makeRpcStatus(RpcStatusCode::ArgumentError,
                         "actor rpc decode output is null");
  }
  const RpcStatus status = serializer.decode(bytes, message);
  if (!status.ok()) return status;
  setActorRpcMessageStatus(message, actorRpcMessageStatus(*message));
  return rpcOk();
}

std::unique_ptr<IMessageSerializer> makeDefaultActorRpcSerializer() {
  return makeJsonMessageSerializer();
}

std::vector<uint8_t> encodeActorRpcMessage(const ActorRpcMessage& message) {
  auto serializer = makeDefaultActorRpcSerializer();
  if (!serializer) return {};
  std::vector<uint8_t> encoded;
  if (!encodeActorRpcMessage(*serializer, message, &encoded).ok()) {
    return {};
  }
  return encoded;
}

bool decodeActorRpcMessage(const std::vector<uint8_t>& bytes,
                           ActorRpcMessage* message) {
  auto serializer = makeDefaultActorRpcSerializer();
  if (!serializer) return false;
  return decodeActorRpcMessage(*serializer, bytes, message).ok();
}

}  // namespace dataflow
