#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "src/dataflow/experimental/runtime/rpc_contract.h"

namespace dataflow {

class IMessageSerializer;

enum class ActorRpcAction : uint8_t {
  RegisterWorker = 0,
  SubmitJob = 1,
  Ack = 2,
  Result = 3,
  Heartbeat = 4,
};

struct ActorRpcMessage {
  ActorRpcAction action = ActorRpcAction::SubmitJob;
  std::string job_id;
  std::string chain_id;
  std::string task_id;
  std::string node_id;
  uint32_t attempt = 0;
  uint64_t heartbeat_seq = 0;
  uint64_t output_rows = 0;
  RpcStatus status = rpcOk();
  bool ok = true;
  std::string state;
  std::string summary;
  std::string result_location;
  std::string payload;
  std::string reason;
};

RpcStatus actorRpcMessageStatus(const ActorRpcMessage& message);
void setActorRpcMessageStatus(ActorRpcMessage* message, const RpcStatus& status);

RpcStatus encodeActorRpcMessage(const IMessageSerializer& serializer,
                                const ActorRpcMessage& message,
                                std::vector<uint8_t>* out);
RpcStatus decodeActorRpcMessage(const IMessageSerializer& serializer,
                                const std::vector<uint8_t>& bytes,
                                ActorRpcMessage* message);
std::vector<uint8_t> encodeActorRpcMessage(const ActorRpcMessage& message);
bool decodeActorRpcMessage(const std::vector<uint8_t>& bytes,
                           ActorRpcMessage* message);

std::unique_ptr<IMessageSerializer> makeDefaultActorRpcSerializer();

const char* actorRpcActionToString(ActorRpcAction action);
bool actorRpcActionFromString(const std::string& value, ActorRpcAction* action);

}  // namespace dataflow
