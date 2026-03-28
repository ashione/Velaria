#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "src/dataflow/core/table.h"

namespace dataflow {

struct BinaryRowBatchOptions {
  std::vector<std::string> projected_columns;
};

class BinaryRowBatchCodec {
 public:
  void serialize(const Table& table, std::vector<uint8_t>* out,
                 const BinaryRowBatchOptions& options = {}) const;
  Table deserialize(const std::vector<uint8_t>& payload) const;
};

class ByteBufferPool {
 public:
  explicit ByteBufferPool(size_t max_cached = 32) : max_cached_(max_cached) {}

  std::vector<uint8_t> acquire(size_t min_capacity = 0);
  void release(std::vector<uint8_t>&& buffer);

 private:
  size_t max_cached_ = 32;
  std::vector<std::vector<uint8_t>> cached_;
};

}  // namespace dataflow
