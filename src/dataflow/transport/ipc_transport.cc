#include "src/dataflow/transport/ipc_transport.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <cctype>
#include <sstream>

#include "src/dataflow/rpc/rpc_codec.h"

namespace dataflow {

namespace {

constexpr uint32_t kMaxFramePayloadBytes = 512u * 1024u * 1024u;

}  // namespace

bool parseEndpoint(const std::string& endpoint, std::string* host, uint16_t* port) {
  if (host == nullptr || port == nullptr) return false;
  const std::string::size_type sep = endpoint.find(':');
  if (sep == std::string::npos || sep + 1 >= endpoint.size()) return false;
  *host = endpoint.substr(0, sep);
  if (host->empty()) {
    *host = "127.0.0.1";
  }
  const std::string port_text = endpoint.substr(sep + 1);
  for (char ch : port_text) {
    if (!std::isdigit(static_cast<unsigned char>(ch))) return false;
  }
  const int raw_port = std::stoi(port_text);
  if (raw_port <= 0 || raw_port > 65535) return false;
  *port = static_cast<uint16_t>(raw_port);
  return true;
}

int createServerSocket(const std::string& host, uint16_t port) {
  const int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) return -1;

  const int opt = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
    ::close(server_fd);
    return -1;
  }

  sockaddr_in addr {};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
    ::close(server_fd);
    return -1;
  }
  if (bind(server_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    ::close(server_fd);
    return -1;
  }
  if (listen(server_fd, 16) < 0) {
    ::close(server_fd);
    return -1;
  }
  return server_fd;
}

int createClientSocket(const std::string& host, uint16_t port) {
  const int client_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (client_fd < 0) return -1;

  sockaddr_in addr {};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
    ::close(client_fd);
    return -1;
  }

  if (connect(client_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    ::close(client_fd);
    return -1;
  }
  return client_fd;
}

bool sendAllBytes(int fd, const uint8_t* data, size_t size) {
  while (size > 0) {
    const ssize_t written = ::send(fd, data, size, 0);
    if (written < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (written == 0) return false;
    data += static_cast<size_t>(written);
    size -= static_cast<size_t>(written);
  }
  return true;
}

bool sendAllBytes(int fd, const std::vector<uint8_t>& data) {
  return sendAllBytes(fd, data.data(), data.size());
}

bool recvAllBytes(int fd, uint8_t* data, size_t size) {
  while (size > 0) {
    const ssize_t read = ::recv(fd, data, size, 0);
    if (read < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (read == 0) return false;
    data += static_cast<size_t>(read);
    size -= static_cast<size_t>(read);
  }
  return true;
}

bool sendFrameOverSocket(int fd,
                        const LengthPrefixedFrameCodec& codec,
                        const RpcFrame& frame) {
  std::vector<uint8_t> bytes = codec.encode(frame);
  return sendAllBytes(fd, bytes);
}

bool sendFrameOverSocket(int fd,
                        const LengthPrefixedFrameCodec& codec,
                        const RpcFrame& frame,
                        std::vector<uint8_t>* scratch) {
  if (scratch == nullptr) {
    return sendFrameOverSocket(fd, codec, frame);
  }
  codec.encodeInto(frame, scratch);
  return sendAllBytes(fd, *scratch);
}

bool recvFrameOverSocket(int fd,
                        const LengthPrefixedFrameCodec& codec,
                        RpcFrame* frame) {
  if (frame == nullptr) return false;
  uint8_t length_prefix[4];
  if (!recvAllBytes(fd, length_prefix, 4)) return false;

  const uint32_t payload_bytes = static_cast<uint32_t>(length_prefix[0]) |
                                (static_cast<uint32_t>(length_prefix[1]) << 8) |
                                (static_cast<uint32_t>(length_prefix[2]) << 16) |
                                (static_cast<uint32_t>(length_prefix[3]) << 24);
  if (payload_bytes == 0 || payload_bytes > kMaxFramePayloadBytes) return false;

  std::vector<uint8_t> bytes(4 + payload_bytes);
  memcpy(&bytes[0], length_prefix, 4);
  if (!recvAllBytes(fd, &bytes[4], payload_bytes)) return false;

  size_t consumed = 0;
  return codec.decode(bytes, frame, &consumed);
}

}  // namespace dataflow
