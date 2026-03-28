#include "src/dataflow/runtime/byte_transport.h"

#include <algorithm>

namespace dataflow {

InMemoryByteTransport::InMemoryByteTransport(std::string name) : name_(std::move(name)) {}

std::pair<std::shared_ptr<InMemoryByteTransport>, std::shared_ptr<InMemoryByteTransport>>
InMemoryByteTransport::makePipePair(const std::string& left_name, const std::string& right_name) {
  auto left = std::make_shared<InMemoryByteTransport>(left_name);
  auto right = std::make_shared<InMemoryByteTransport>(right_name);
  left->attachPeer(right);
  right->attachPeer(left);
  return {left, right};
}

void InMemoryByteTransport::attachPeer(const std::shared_ptr<InMemoryByteTransport>& peer) {
  std::lock_guard<std::mutex> lock(mutex_);
  peer_ = peer;
}

bool InMemoryByteTransport::pushToPeer(const std::vector<uint8_t>& bytes) {
  auto peer = peer_.lock();
  if (!peer || peer->isClosed()) return false;
  {
    std::lock_guard<std::mutex> peer_lock(peer->mutex_);
    if (peer->closed_) return false;
    peer->incoming_.insert(peer->incoming_.end(), bytes.begin(), bytes.end());
  }
  peer->cv_.notify_one();
  return true;
}

bool InMemoryByteTransport::writeBytes(const std::vector<uint8_t>& bytes) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_) return false;
  }
  return pushToPeer(bytes);
}

std::vector<uint8_t> InMemoryByteTransport::popAllLocked() {
  std::vector<uint8_t> out;
  out.reserve(incoming_.size());
  while (!incoming_.empty()) {
    out.push_back(incoming_.front());
    incoming_.pop_front();
  }
  return out;
}

bool InMemoryByteTransport::readBytes(std::vector<uint8_t>* out, int timeout_ms) {
  if (!out) return false;
  std::unique_lock<std::mutex> lock(mutex_);
  if (incoming_.empty()) {
    if (closed_) return false;
    if (timeout_ms == 0) return false;

    if (timeout_ms < 0) {
      cv_.wait(lock, [this]() { return !incoming_.empty() || closed_; });
    } else {
      cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms),
                   [this]() { return !incoming_.empty() || closed_; });
    }
    if (incoming_.empty()) return false;
  }

  *out = popAllLocked();
  return true;
}

void InMemoryByteTransport::close() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (closed_) return;
  closed_ = true;
  cv_.notify_all();
}

bool InMemoryByteTransport::isClosed() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return closed_;
}

}  // namespace dataflow
