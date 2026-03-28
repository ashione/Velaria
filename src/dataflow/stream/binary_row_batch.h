#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <new>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "src/dataflow/core/table.h"

namespace dataflow {

template <typename T, std::size_t Alignment>
class AlignedAllocator {
 public:
  using value_type = T;

  AlignedAllocator() noexcept = default;

  template <typename U>
  AlignedAllocator(const AlignedAllocator<U, Alignment>&) noexcept {}

  T* allocate(std::size_t n) {
    if (n > static_cast<std::size_t>(-1) / sizeof(T)) {
      throw std::bad_alloc();
    }
    void* ptr = nullptr;
    const std::size_t bytes = n * sizeof(T);
    const std::size_t aligned_bytes =
        bytes == 0 ? Alignment : ((bytes + Alignment - 1) / Alignment) * Alignment;
    if (::posix_memalign(&ptr, Alignment, aligned_bytes) != 0 || ptr == nullptr) {
      throw std::bad_alloc();
    }
    return static_cast<T*>(ptr);
  }

  void deallocate(T* ptr, std::size_t) noexcept { std::free(ptr); }

  template <typename U>
  struct rebind {
    using other = AlignedAllocator<U, Alignment>;
  };

  using propagate_on_container_move_assignment = std::true_type;
  using is_always_equal = std::true_type;
};

template <typename T, typename U, std::size_t Alignment>
bool operator==(const AlignedAllocator<T, Alignment>&,
                const AlignedAllocator<U, Alignment>&) noexcept {
  return true;
}

template <typename T, typename U, std::size_t Alignment>
bool operator!=(const AlignedAllocator<T, Alignment>&,
                const AlignedAllocator<U, Alignment>&) noexcept {
  return false;
}

struct BinaryRowBatchOptions {
  std::vector<std::string> projected_columns;
};

template <typename T>
using CacheAlignedVector = std::vector<T, AlignedAllocator<T, 64>>;

struct StringBlobStorage {
  CacheAlignedVector<uint32_t> offsets;
  std::vector<char> blob;

  void clear();
  void reserve(size_t count, size_t bytes);
  uint32_t append(const std::string& value);
  uint32_t append(std::string_view value);
  std::string_view view(size_t index) const;
  size_t size() const;
};

struct BinaryStringColumn {
  bool dictionary_encoded = false;
  StringBlobStorage dictionary;
  CacheAlignedVector<uint32_t> indices;
  StringBlobStorage values;
  CacheAlignedVector<uint8_t> is_null;
};

struct BinaryDoubleColumn {
  CacheAlignedVector<double> values;
  CacheAlignedVector<uint8_t> is_null;
};

struct WindowKeyValueColumnarBatch {
  size_t row_count = 0;
  BinaryStringColumn window_start;
  BinaryStringColumn key;
  BinaryDoubleColumn value;
};

struct PreparedBinaryRowColumn {
  size_t column_index = 0;
  DataType type = DataType::String;
  uint8_t encoding = 0;
  std::vector<std::string> dictionary;
  std::unordered_map<std::string, uint32_t> dictionary_index;
};

struct PreparedBinaryRowBatch {
  std::vector<size_t> projected_columns;
  std::vector<PreparedBinaryRowColumn> encoded_columns;
  size_t estimated_size = 0;
};

class BinaryRowBatchCodec {
 public:
  PreparedBinaryRowBatch prepare(const Table& table,
                                 const BinaryRowBatchOptions& options = {}) const;
  PreparedBinaryRowBatch prepareRange(const Table& table, size_t row_begin, size_t row_end,
                                      const BinaryRowBatchOptions& options = {}) const;
  size_t estimateSerializedSize(const Table& table,
                                const BinaryRowBatchOptions& options = {}) const;
  size_t estimateSerializedSizeRange(const Table& table, size_t row_begin, size_t row_end,
                                     const BinaryRowBatchOptions& options = {}) const;
  void serialize(const Table& table, std::vector<uint8_t>* out,
                 const BinaryRowBatchOptions& options = {}) const;
  void serializeRange(const Table& table, size_t row_begin, size_t row_end,
                      std::vector<uint8_t>* out,
                      const BinaryRowBatchOptions& options = {}) const;
  void serializePreparedRange(const Table& table, size_t row_begin, size_t row_end,
                              const PreparedBinaryRowBatch& prepared,
                              std::vector<uint8_t>* out) const;
  size_t serializeRangeToBuffer(const Table& table, size_t row_begin, size_t row_end,
                                uint8_t* out, size_t capacity,
                                const BinaryRowBatchOptions& options = {}) const;
  size_t serializePreparedRangeToBuffer(const Table& table, size_t row_begin, size_t row_end,
                                        const PreparedBinaryRowBatch& prepared, uint8_t* out,
                                        size_t capacity) const;
  Table deserialize(const std::vector<uint8_t>& payload) const;
  Table deserializeFromBuffer(const uint8_t* payload, size_t size) const;
  bool deserializeWindowKeyValue(const std::vector<uint8_t>& payload,
                                 WindowKeyValueColumnarBatch* out) const;
  bool deserializeWindowKeyValueFromBuffer(const uint8_t* payload, size_t size,
                                           WindowKeyValueColumnarBatch* out) const;
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
