#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <memory>
#include <utility>
#include <vector>

namespace dataflow {

class IByteTransport {
 public:
  virtual ~IByteTransport() = default;

  virtual bool writeBytes(const std::vector<uint8_t>& bytes) = 0;
  virtual bool readBytes(std::vector<uint8_t>* out, int timeout_ms = -1) = 0;
  virtual void close() = 0;
};

class InMemoryByteTransport : public IByteTransport {
 public:
  static std::pair<std::shared_ptr<InMemoryByteTransport>, std::shared_ptr<InMemoryByteTransport>> makePipePair(
      const std::string& left_name = "left",
      const std::string& right_name = "right");

  explicit InMemoryByteTransport(std::string name);

  void attachPeer(const std::shared_ptr<InMemoryByteTransport>& peer);

  bool writeBytes(const std::vector<uint8_t>& bytes) override;
  bool readBytes(std::vector<uint8_t>* out, int timeout_ms = -1) override;
  void close() override;
  bool isClosed() const;

 private:
  bool pushToPeer(const std::vector<uint8_t>& bytes);
  std::vector<uint8_t> popAllLocked();

  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::deque<uint8_t> incoming_;
  bool closed_ = false;
  std::weak_ptr<InMemoryByteTransport> peer_;
  std::string name_;
};

}  // namespace dataflow
