#pragma once

#include <memory>
#include <string>
#include <vector>

#include "src/dataflow/experimental/runtime/rpc_contract.h"

namespace dataflow {

struct ActorRpcMessage;

class IMessageSerializer {
 public:
  virtual ~IMessageSerializer() = default;

  virtual std::string serializerId() const = 0;
  virtual std::string messageType() const = 0;
  virtual RpcStatus encode(const ActorRpcMessage& message,
                           std::vector<uint8_t>* out) const = 0;
  virtual RpcStatus decode(const std::vector<uint8_t>& bytes,
                           ActorRpcMessage* message) const = 0;
};

std::unique_ptr<IMessageSerializer> makeJsonMessageSerializer();

}  // namespace dataflow
