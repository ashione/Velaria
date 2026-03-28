#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "src/dataflow/rpc/rpc_codec.h"

namespace dataflow {

bool parseEndpoint(const std::string& endpoint, std::string* host, uint16_t* port);

int createServerSocket(const std::string& host, uint16_t port);
int createClientSocket(const std::string& host, uint16_t port);

bool sendAllBytes(int fd, const uint8_t* data, size_t size);
bool sendAllBytes(int fd, const std::vector<uint8_t>& data);
bool recvAllBytes(int fd, uint8_t* data, size_t size);

bool sendFrameOverSocket(int fd,
                         const LengthPrefixedFrameCodec& codec,
                         const RpcFrame& frame);
bool recvFrameOverSocket(int fd,
                         const LengthPrefixedFrameCodec& codec,
                         RpcFrame* frame);

}  // namespace dataflow
