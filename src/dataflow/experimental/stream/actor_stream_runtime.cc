#include "src/dataflow/experimental/stream/actor_stream_runtime.h"

#include <chrono>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <atomic>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <thread>
#include <unordered_map>
#include <unistd.h>
#include <utility>
#include <vector>

#include "src/dataflow/core/contract/api/dataframe.h"
#include "src/dataflow/core/execution/stream/binary_row_batch.h"
#include "src/dataflow/experimental/transport/ipc_transport.h"

namespace dataflow {

namespace {

constexpr char kKeySep = '\x1f';
constexpr uint8_t kRequestKind = 1;
constexpr uint8_t kResponseKind = 2;
constexpr uint8_t kEnvelopeMagic[4] = {'A', 'S', 'B', '1'};
constexpr uint8_t kPayloadInline = 0;
constexpr uint8_t kPayloadSharedMemory = 1;

uint64_t toMillis(std::chrono::steady_clock::duration value) {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(value).count());
}

double toRowsPerSecond(const LocalActorStreamResult& result, size_t rows_per_batch) {
  if (result.elapsed_ms == 0 || result.processed_batches == 0) return 0.0;
  return static_cast<double>(result.processed_batches * rows_per_batch) /
         (static_cast<double>(result.elapsed_ms) / 1000.0);
}

uint64_t actorOverheadMs(const LocalActorStreamResult& result) {
  return result.coordinator_serialize_ms + result.coordinator_wait_ms +
         result.coordinator_deserialize_ms + result.coordinator_merge_ms +
         result.worker_deserialize_ms + result.worker_serialize_ms;
}

LocalActorStreamResult measureSingleProcessWindowKeySum(const std::vector<Table>& batches,
                                                        size_t cpu_spin_per_row) {
  LocalActorStreamResult result;
  auto started = std::chrono::steady_clock::now();
  result.final_table = runSingleProcessWindowKeySum(batches, cpu_spin_per_row);
  result.processed_batches = batches.size();
  result.processed_partitions = batches.size();
  result.elapsed_ms = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - started)
          .count());
  return result;
}

void appendU8(std::vector<uint8_t>* out, uint8_t value) { out->push_back(value); }

void appendU32(std::vector<uint8_t>* out, uint32_t value) {
  out->push_back(static_cast<uint8_t>(value & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 16) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 24) & 0xFF));
}

void appendU64(std::vector<uint8_t>* out, uint64_t value) {
  out->push_back(static_cast<uint8_t>(value & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 16) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 24) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 32) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 40) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 48) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 56) & 0xFF));
}

bool readU8(const std::vector<uint8_t>& src, size_t* offset, uint8_t* out) {
  if (offset == nullptr || out == nullptr || *offset >= src.size()) return false;
  *out = src[*offset];
  ++(*offset);
  return true;
}

bool readU32(const std::vector<uint8_t>& src, size_t* offset, uint32_t* out) {
  if (offset == nullptr || out == nullptr || *offset + 4 > src.size()) return false;
  *out = static_cast<uint32_t>(src[*offset]) |
         (static_cast<uint32_t>(src[*offset + 1]) << 8) |
         (static_cast<uint32_t>(src[*offset + 2]) << 16) |
         (static_cast<uint32_t>(src[*offset + 3]) << 24);
  *offset += 4;
  return true;
}

bool readU64(const std::vector<uint8_t>& src, size_t* offset, uint64_t* out) {
  if (offset == nullptr || out == nullptr || *offset + 8 > src.size()) return false;
  *out = static_cast<uint64_t>(src[*offset]) |
         (static_cast<uint64_t>(src[*offset + 1]) << 8) |
         (static_cast<uint64_t>(src[*offset + 2]) << 16) |
         (static_cast<uint64_t>(src[*offset + 3]) << 24) |
         (static_cast<uint64_t>(src[*offset + 4]) << 32) |
         (static_cast<uint64_t>(src[*offset + 5]) << 40) |
         (static_cast<uint64_t>(src[*offset + 6]) << 48) |
         (static_cast<uint64_t>(src[*offset + 7]) << 56);
  *offset += 8;
  return true;
}

void appendBytes(std::vector<uint8_t>* out, const std::vector<uint8_t>& bytes) {
  appendU32(out, static_cast<uint32_t>(bytes.size()));
  out->insert(out->end(), bytes.begin(), bytes.end());
}

void appendString(std::vector<uint8_t>* out, const std::string& value) {
  appendU32(out, static_cast<uint32_t>(value.size()));
  out->insert(out->end(), value.begin(), value.end());
}

bool readBytes(const std::vector<uint8_t>& src, size_t* offset, std::vector<uint8_t>* out) {
  uint32_t size = 0;
  if (offset == nullptr || out == nullptr || !readU32(src, offset, &size)) return false;
  if (*offset + size > src.size()) return false;
  out->assign(src.begin() + static_cast<std::vector<uint8_t>::difference_type>(*offset),
              src.begin() + static_cast<std::vector<uint8_t>::difference_type>(*offset + size));
  *offset += size;
  return true;
}

bool readString(const std::vector<uint8_t>& src, size_t* offset, std::string* out) {
  uint32_t size = 0;
  if (offset == nullptr || out == nullptr || !readU32(src, offset, &size)) return false;
  if (*offset + size > src.size()) return false;
  out->assign(reinterpret_cast<const char*>(src.data() + *offset), size);
  *offset += size;
  return true;
}

struct WorkerHandle {
  pid_t pid = -1;
  int fd = -1;
  bool busy = false;
  std::string node_id;
  uint64_t task_id = 0;
};

struct SharedMemoryPayload {
  std::string name;
  uint64_t size = 0;
};

struct PendingPartition {
  uint64_t task_id = 0;
  std::vector<uint8_t> payload;
  bool shared_memory = false;
  SharedMemoryPayload shared_memory_ref;
};

struct WorkerMetrics {
  uint64_t deserialize_ms = 0;
  uint64_t compute_ms = 0;
  uint64_t serialize_ms = 0;
};

struct ActorStreamRequest {
  uint64_t task_id = 0;
  std::vector<uint8_t> table_payload;
  bool shared_memory = false;
  std::string shared_memory_name;
  uint64_t shared_memory_size = 0;
};

struct ActorStreamResponse {
  uint64_t task_id = 0;
  bool ok = false;
  WorkerMetrics metrics;
  std::vector<uint8_t> table_payload;
  std::string reason;
  bool shared_memory = false;
  std::string shared_memory_name;
  uint64_t shared_memory_size = 0;
};

struct SharedMemoryWriteRegion {
  SharedMemoryPayload ref;
  int fd = -1;
  void* mapped = nullptr;
};

struct SharedMemoryReadRegion {
  int fd = -1;
  const void* mapped = nullptr;
  size_t size = 0;
};

std::atomic<uint64_t> g_shared_memory_seq{1};

std::string makeSharedMemoryName() {
  return "/velaria-stream-" + std::to_string(static_cast<uint64_t>(::getpid())) + "-" +
         std::to_string(g_shared_memory_seq.fetch_add(1, std::memory_order_relaxed));
}

SharedMemoryWriteRegion createSharedMemoryWriteRegion(size_t size) {
  SharedMemoryWriteRegion region;
  region.ref.name = makeSharedMemoryName();
  region.ref.size = static_cast<uint64_t>(size);
  region.fd = ::shm_open(region.ref.name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
  if (region.fd < 0) {
    throw std::runtime_error("shm_open create failed");
  }
  if (::ftruncate(region.fd, static_cast<off_t>(size)) != 0) {
    ::close(region.fd);
    ::shm_unlink(region.ref.name.c_str());
    throw std::runtime_error("ftruncate failed");
  }
  if (size > 0) {
    region.mapped = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, region.fd, 0);
    if (region.mapped == MAP_FAILED) {
      ::close(region.fd);
      ::shm_unlink(region.ref.name.c_str());
      throw std::runtime_error("mmap failed");
    }
  }
  return region;
}

void closeSharedMemoryWriteRegion(SharedMemoryWriteRegion* region) {
  if (region == nullptr) return;
  if (region->mapped != nullptr && region->mapped != MAP_FAILED) {
    ::munmap(region->mapped, static_cast<size_t>(region->ref.size));
    region->mapped = nullptr;
  }
  if (region->fd >= 0) {
    ::close(region->fd);
    region->fd = -1;
  }
}

SharedMemoryReadRegion openSharedMemoryReadRegion(const SharedMemoryPayload& ref) {
  SharedMemoryReadRegion region;
  region.size = static_cast<size_t>(ref.size);
  if (region.size == 0) {
    return region;
  }
  const int fd = ::shm_open(ref.name.c_str(), O_RDONLY, 0600);
  if (fd < 0) {
    throw std::runtime_error("shm_open read failed");
  }
  void* mapped = ::mmap(nullptr, region.size, PROT_READ, MAP_SHARED, fd, 0);
  if (mapped == MAP_FAILED) {
    ::close(fd);
    throw std::runtime_error("mmap read failed");
  }
  region.fd = fd;
  region.mapped = mapped;
  return region;
}

void closeSharedMemoryReadRegion(SharedMemoryReadRegion* region) {
  if (region == nullptr) return;
  if (region->mapped != nullptr && region->mapped != MAP_FAILED) {
    ::munmap(const_cast<void*>(region->mapped), region->size);
    region->mapped = nullptr;
  }
  if (region->fd >= 0) {
    ::close(region->fd);
    region->fd = -1;
  }
}

void unlinkSharedMemoryPayload(const SharedMemoryPayload& ref) {
  if (!ref.name.empty()) {
    ::shm_unlink(ref.name.c_str());
  }
}

void encodeActorStreamRequest(const ActorStreamRequest& request, std::vector<uint8_t>* out) {
  if (out == nullptr) return;
  out->clear();
  out->insert(out->end(), kEnvelopeMagic, kEnvelopeMagic + 4);
  appendU8(out, kRequestKind);
  appendU64(out, request.task_id);
  if (request.shared_memory) {
    appendU8(out, kPayloadSharedMemory);
    appendString(out, request.shared_memory_name);
    appendU64(out, request.shared_memory_size);
  } else {
    appendU8(out, kPayloadInline);
    appendBytes(out, request.table_payload);
  }
}

bool decodeActorStreamRequest(const std::vector<uint8_t>& payload, ActorStreamRequest* out) {
  if (out == nullptr || payload.size() < 5 ||
      std::memcmp(payload.data(), kEnvelopeMagic, 4) != 0) {
    return false;
  }
  size_t offset = 4;
  uint8_t kind = 0;
  if (!readU8(payload, &offset, &kind) || kind != kRequestKind) return false;
  if (!readU64(payload, &offset, &out->task_id)) return false;
  uint8_t mode = 0;
  if (!readU8(payload, &offset, &mode)) return false;
  out->shared_memory = mode == kPayloadSharedMemory;
  if (out->shared_memory) {
    return readString(payload, &offset, &out->shared_memory_name) &&
           readU64(payload, &offset, &out->shared_memory_size);
  }
  return readBytes(payload, &offset, &out->table_payload);
}

void encodeActorStreamResponse(const ActorStreamResponse& response, std::vector<uint8_t>* out) {
  if (out == nullptr) return;
  out->clear();
  out->insert(out->end(), kEnvelopeMagic, kEnvelopeMagic + 4);
  appendU8(out, kResponseKind);
  appendU64(out, response.task_id);
  appendU8(out, response.ok ? 1 : 0);
  appendU64(out, response.metrics.deserialize_ms);
  appendU64(out, response.metrics.compute_ms);
  appendU64(out, response.metrics.serialize_ms);
  if (response.ok) {
    if (response.shared_memory) {
      appendU8(out, kPayloadSharedMemory);
      appendString(out, response.shared_memory_name);
      appendU64(out, response.shared_memory_size);
    } else {
      appendU8(out, kPayloadInline);
      appendBytes(out, response.table_payload);
    }
  } else {
    appendString(out, response.reason);
  }
}

bool decodeActorStreamResponse(const std::vector<uint8_t>& payload, ActorStreamResponse* out) {
  if (out == nullptr || payload.size() < 5 ||
      std::memcmp(payload.data(), kEnvelopeMagic, 4) != 0) {
    return false;
  }
  size_t offset = 4;
  uint8_t kind = 0;
  uint8_t ok = 0;
  if (!readU8(payload, &offset, &kind) || kind != kResponseKind) return false;
  if (!readU64(payload, &offset, &out->task_id) || !readU8(payload, &offset, &ok)) return false;
  out->ok = ok != 0;
  if (!readU64(payload, &offset, &out->metrics.deserialize_ms) ||
      !readU64(payload, &offset, &out->metrics.compute_ms) ||
      !readU64(payload, &offset, &out->metrics.serialize_ms)) {
    return false;
  }
  if (out->ok) {
    uint8_t mode = 0;
    if (!readU8(payload, &offset, &mode)) return false;
    out->shared_memory = mode == kPayloadSharedMemory;
    if (out->shared_memory) {
      return readString(payload, &offset, &out->shared_memory_name) &&
             readU64(payload, &offset, &out->shared_memory_size);
    }
    return readBytes(payload, &offset, &out->table_payload);
  }
  return readString(payload, &offset, &out->reason);
}

std::string makeStateKey(const Row& row, size_t window_idx, size_t key_idx) {
  return row[window_idx].toString() + kKeySep + row[key_idx].toString();
}

std::vector<std::pair<size_t, size_t>> splitTableRanges(const Table& table, size_t parts) {
  if (parts <= 1 || table.rows.size() <= 1) {
    return {{0, table.rows.size()}};
  }
  parts = std::min(parts, table.rows.size());
  std::vector<std::pair<size_t, size_t>> out;
  out.reserve(parts);
  const size_t rows_per_part = (table.rows.size() + parts - 1) / parts;
  size_t begin = 0;
  while (begin < table.rows.size()) {
    const size_t end = std::min(table.rows.size(), begin + rows_per_part);
    out.push_back({begin, end});
    begin = end;
  }
  return out;
}

void mergePartialTable(const Table& partial,
                       std::unordered_map<std::string, double>* sums,
                       std::vector<std::string>* ordered_keys) {
  if (sums == nullptr || ordered_keys == nullptr) return;
  if (!partial.schema.has("window_start") || !partial.schema.has("key") ||
      !partial.schema.has("partial_sum")) {
    throw std::runtime_error("partial aggregate schema mismatch");
  }
  const auto window_idx = partial.schema.indexOf("window_start");
  const auto key_idx = partial.schema.indexOf("key");
  const auto sum_idx = partial.schema.indexOf("partial_sum");
  for (const auto& row : partial.rows) {
    const auto state_key = makeStateKey(row, window_idx, key_idx);
    auto it = sums->find(state_key);
    if (it == sums->end()) {
      ordered_keys->push_back(state_key);
      it = sums->emplace(state_key, 0.0).first;
    }
    it->second += row[sum_idx].asDouble();
  }
}

Table materializeStateTable(const std::unordered_map<std::string, double>& sums,
                            const std::vector<std::string>& ordered_keys) {
  Table out(Schema({"window_start", "key", "value_sum"}), {});
  for (const auto& state_key : ordered_keys) {
    const auto split = state_key.find(kKeySep);
    if (split == std::string::npos) continue;
    Row row;
    row.emplace_back(state_key.substr(0, split));
    row.emplace_back(state_key.substr(split + 1));
    row.emplace_back(sums.at(state_key));
    out.rows.push_back(std::move(row));
  }
  return out;
}

std::string_view stringAt(const BinaryStringColumn& column, size_t row_idx) {
  if (column.dictionary_encoded) {
    return column.dictionary.view(column.indices[row_idx]);
  }
  return column.values.view(row_idx);
}

double spinValue(double value, size_t cpu_spin_per_row) {
  double out = value;
  for (size_t i = 0; i < cpu_spin_per_row; ++i) {
    out = out * 1.0000001 + static_cast<double>(i % 7);
    out = out / 1.0000001;
  }
  return out;
}

Table aggregateVectorizedBatch(const WindowKeyValueColumnarBatch& batch, size_t cpu_spin_per_row) {
  Table out(Schema({"window_start", "key", "partial_sum"}), {});
  if (batch.row_count == 0) {
    return out;
  }

  if (batch.window_start.dictionary_encoded && batch.key.dictionary_encoded) {
    std::unordered_map<uint64_t, double> sums;
    std::vector<uint64_t> ordered;
    ordered.reserve(batch.row_count);
    for (size_t i = 0; i < batch.row_count; ++i) {
      if (batch.window_start.is_null[i] || batch.key.is_null[i] || batch.value.is_null[i]) continue;
      const uint64_t composite =
          (static_cast<uint64_t>(batch.window_start.indices[i]) << 32) |
          static_cast<uint64_t>(batch.key.indices[i]);
      auto it = sums.find(composite);
      if (it == sums.end()) {
        ordered.push_back(composite);
        it = sums.emplace(composite, 0.0).first;
      }
      const double value = cpu_spin_per_row == 0 ? batch.value.values[i]
                                                 : spinValue(batch.value.values[i], cpu_spin_per_row);
      it->second += value;
    }
    out.rows.reserve(ordered.size());
    for (uint64_t composite : ordered) {
      const uint32_t window_id = static_cast<uint32_t>(composite >> 32);
      const uint32_t key_id = static_cast<uint32_t>(composite & 0xFFFFFFFFu);
      Row row;
      const auto window = batch.window_start.dictionary.view(window_id);
      const auto key = batch.key.dictionary.view(key_id);
      row.emplace_back(std::string(window.data(), window.size()));
      row.emplace_back(std::string(key.data(), key.size()));
      row.emplace_back(sums.at(composite));
      out.rows.push_back(std::move(row));
    }
    return out;
  }

  std::unordered_map<std::string, double> sums;
  std::vector<std::string> ordered;
  ordered.reserve(batch.row_count);
  for (size_t i = 0; i < batch.row_count; ++i) {
    if (batch.window_start.is_null[i] || batch.key.is_null[i] || batch.value.is_null[i]) continue;
    const auto window = stringAt(batch.window_start, i);
    const auto key = stringAt(batch.key, i);
    std::string state_key;
    state_key.reserve(window.size() + 1 + key.size());
    state_key.append(window.data(), window.size());
    state_key.push_back(kKeySep);
    state_key.append(key.data(), key.size());
    auto it = sums.find(state_key);
    if (it == sums.end()) {
      ordered.push_back(state_key);
      it = sums.emplace(state_key, 0.0).first;
    }
    const double value = cpu_spin_per_row == 0 ? batch.value.values[i]
                                               : spinValue(batch.value.values[i], cpu_spin_per_row);
    it->second += value;
  }
  out.rows.reserve(ordered.size());
  for (const auto& state_key : ordered) {
    const auto split = state_key.find(kKeySep);
    Row row;
    row.emplace_back(state_key.substr(0, split));
    row.emplace_back(state_key.substr(split + 1));
    row.emplace_back(sums.at(state_key));
    out.rows.push_back(std::move(row));
  }
  return out;
}

WindowKeyValueColumnarBatch buildColumnarFromTable(const Table& input) {
  if (!input.schema.has("window_start") || !input.schema.has("key") || !input.schema.has("value")) {
    throw std::runtime_error("buildColumnarFromTable schema mismatch");
  }

  const size_t window_idx = input.schema.indexOf("window_start");
  const size_t key_idx = input.schema.indexOf("key");
  const size_t value_idx = input.schema.indexOf("value");
  WindowKeyValueColumnarBatch batch;
  batch.row_count = input.rowCount();
  batch.window_start.dictionary_encoded = true;
  batch.key.dictionary_encoded = true;
  batch.window_start.indices.assign(batch.row_count, 0);
  batch.key.indices.assign(batch.row_count, 0);
  batch.window_start.is_null.assign(batch.row_count, 0);
  batch.key.is_null.assign(batch.row_count, 0);
  batch.value.is_null.assign(batch.row_count, 0);
  batch.value.values.assign(batch.row_count, 0.0);

  std::unordered_map<std::string, uint32_t> window_dict;
  std::unordered_map<std::string, uint32_t> key_dict;
  for (size_t row_idx = 0; row_idx < input.rows.size(); ++row_idx) {
    const auto& row = input.rows[row_idx];
    if (window_idx >= row.size() || row[window_idx].isNull()) {
      batch.window_start.is_null[row_idx] = 1;
    } else {
      const auto text = row[window_idx].toString();
      auto it = window_dict.find(text);
      if (it == window_dict.end()) {
        const uint32_t id = static_cast<uint32_t>(batch.window_start.dictionary.size());
        batch.window_start.dictionary.append(text);
        it = window_dict.emplace(text, id).first;
      }
      batch.window_start.indices[row_idx] = it->second;
    }

    if (key_idx >= row.size() || row[key_idx].isNull()) {
      batch.key.is_null[row_idx] = 1;
    } else {
      const auto text = row[key_idx].toString();
      auto it = key_dict.find(text);
      if (it == key_dict.end()) {
        const uint32_t id = static_cast<uint32_t>(batch.key.dictionary.size());
        batch.key.dictionary.append(text);
        it = key_dict.emplace(text, id).first;
      }
      batch.key.indices[row_idx] = it->second;
    }

    if (value_idx >= row.size() || row[value_idx].isNull()) {
      batch.value.is_null[row_idx] = 1;
    } else {
      batch.value.values[row_idx] = row[value_idx].asDouble();
    }
  }
  return batch;
}

Table aggregatePartitionWithWork(const Table& input, size_t cpu_spin_per_row) {
  return aggregateVectorizedBatch(buildColumnarFromTable(input), cpu_spin_per_row);
}

void workerLoop(int fd, uint64_t delay_ms, size_t cpu_spin_per_row,
                size_t shared_memory_min_payload_bytes) {
  LengthPrefixedFrameCodec codec;
  BinaryRowBatchCodec batch_codec;
  BinaryRowBatchOptions output_projection;
  output_projection.projected_columns = {"window_start", "key", "partial_sum"};
  ByteBufferPool payload_pool;
  ByteBufferPool frame_pool;

  while (true) {
    RpcFrame frame;
    if (!recvFrameOverSocket(fd, codec, &frame)) {
      break;
    }
    ActorStreamRequest request;
    if (!decodeActorStreamRequest(frame.payload, &request)) {
      continue;
    }

    if (delay_ms > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
    }

    ActorStreamResponse result;
    result.task_id = request.task_id;
    try {
      const auto deserialize_started = std::chrono::steady_clock::now();
      SharedMemoryReadRegion request_region;
      if (request.shared_memory) {
        request_region = openSharedMemoryReadRegion(
            SharedMemoryPayload{request.shared_memory_name, request.shared_memory_size});
      }
      WindowKeyValueColumnarBatch input;
      const bool decoded =
          request.shared_memory
              ? batch_codec.deserializeWindowKeyValueFromBuffer(
                    static_cast<const uint8_t*>(request_region.mapped), request_region.size, &input)
              : batch_codec.deserializeWindowKeyValue(request.table_payload, &input);
      closeSharedMemoryReadRegion(&request_region);
      if (!decoded) {
        throw std::runtime_error("deserializeWindowKeyValue failed");
      }
      const auto compute_started = std::chrono::steady_clock::now();
      const Table partial = aggregateVectorizedBatch(input, cpu_spin_per_row);
      const auto serialize_started = std::chrono::steady_clock::now();

      result.ok = true;
      result.metrics.deserialize_ms = toMillis(compute_started - deserialize_started);
      result.metrics.compute_ms = toMillis(serialize_started - compute_started);

      const PreparedBinaryRowBatch prepared_output =
          batch_codec.prepare(partial, output_projection);
      const size_t estimated_payload = prepared_output.estimated_size;
      if (estimated_payload >= shared_memory_min_payload_bytes) {
        auto region = createSharedMemoryWriteRegion(estimated_payload);
        const size_t written = batch_codec.serializePreparedRangeToBuffer(
            partial, 0, partial.rowCount(), prepared_output, static_cast<uint8_t*>(region.mapped),
            static_cast<size_t>(region.ref.size));
        closeSharedMemoryWriteRegion(&region);
        region.ref.size = written;
        result.shared_memory = true;
        result.shared_memory_name = region.ref.name;
        result.shared_memory_size = region.ref.size;
      } else {
        auto table_payload = payload_pool.acquire(std::max<size_t>(estimated_payload, 64));
        batch_codec.serializePreparedRange(partial, 0, partial.rowCount(), prepared_output,
                                           &table_payload);
        result.table_payload = std::move(table_payload);
      }
      result.metrics.serialize_ms = toMillis(std::chrono::steady_clock::now() - serialize_started);
    } catch (const std::exception& ex) {
      result.ok = false;
      result.reason = ex.what();
    }

    RpcFrame reply;
    reply.header.protocol_version = 1;
    reply.header.type = RpcMessageType::DataBatch;
    reply.header.message_id = result.task_id;
    reply.header.correlation_id = result.task_id;
    reply.header.codec_id = "actor-stream-bin-v1";
    reply.header.source = "worker";
    reply.header.target = "coordinator";
    reply.payload = payload_pool.acquire(result.table_payload.size() + 64);
    encodeActorStreamResponse(result, &reply.payload);
    auto scratch = frame_pool.acquire(reply.payload.size() + 128);
    sendFrameOverSocket(fd, codec, reply, &scratch);
    frame_pool.release(std::move(scratch));
    payload_pool.release(std::move(reply.payload));
    payload_pool.release(std::move(result.table_payload));
    if (request.shared_memory) {
      unlinkSharedMemoryPayload(SharedMemoryPayload{request.shared_memory_name,
                                                    request.shared_memory_size});
    }
  }
  ::close(fd);
  _exit(0);
}

std::vector<WorkerHandle> startWorkers(const LocalActorStreamOptions& options) {
  std::vector<WorkerHandle> workers;
  workers.reserve(options.worker_count);
  for (size_t i = 0; i < std::max<size_t>(1, options.worker_count); ++i) {
    int fds[2];
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
      throw std::runtime_error("socketpair failed");
    }
    const pid_t pid = ::fork();
    if (pid < 0) {
      ::close(fds[0]);
      ::close(fds[1]);
      throw std::runtime_error("fork failed");
    }
    if (pid == 0) {
      ::close(fds[0]);
      workerLoop(fds[1], options.worker_delay_ms, options.cpu_spin_per_row,
                 options.shared_memory_min_payload_bytes);
    }
    ::close(fds[1]);
    WorkerHandle worker;
    worker.pid = pid;
    worker.fd = fds[0];
    worker.node_id = "local-worker-" + std::to_string(i + 1);
    workers.push_back(worker);
  }
  return workers;
}

void stopWorkers(std::vector<WorkerHandle>* workers) {
  if (workers == nullptr) return;
  for (auto& worker : *workers) {
    if (worker.fd >= 0) {
      ::close(worker.fd);
      worker.fd = -1;
    }
  }
  for (auto& worker : *workers) {
    if (worker.pid > 0) {
      int status = 0;
      ::waitpid(worker.pid, &status, 0);
    }
  }
}

}  // namespace

const char* localExecutionModeName(LocalExecutionMode mode) {
  switch (mode) {
    case LocalExecutionMode::SingleProcess:
      return "single-process";
    case LocalExecutionMode::ActorCredit:
      return "actor-credit";
  }
  return "single-process";
}

Table runSingleProcessWindowKeySum(const std::vector<Table>& batches, size_t cpu_spin_per_row) {
  std::unordered_map<std::string, double> sums;
  std::vector<std::string> ordered_keys;
  for (const auto& batch : batches) {
    mergePartialTable(aggregatePartitionWithWork(batch, cpu_spin_per_row), &sums, &ordered_keys);
  }
  return materializeStateTable(sums, ordered_keys);
}

LocalActorStreamResult runLocalActorStreamWindowKeySum(const std::vector<Table>& batches,
                                                       const LocalActorStreamOptions& options) {
  if (options.max_inflight_partitions == 0) {
    throw std::invalid_argument("max_inflight_partitions must be positive");
  }

  LocalActorStreamResult result;
  auto started = std::chrono::steady_clock::now();
  auto workers = startWorkers(options);
  LengthPrefixedFrameCodec codec;
  BinaryRowBatchCodec batch_codec;
  BinaryRowBatchOptions input_projection;
  input_projection.projected_columns = {"window_start", "key", "value"};
  ByteBufferPool payload_pool;
  ByteBufferPool frame_pool;

  const bool can_use_shared_memory = options.shared_memory_transport;

  std::vector<PendingPartition> pending;
  uint64_t task_seq = 1;
  for (const auto& batch : batches) {
    ++result.processed_batches;
    const auto split_started = std::chrono::steady_clock::now();
    const auto ranges = splitTableRanges(batch, std::max<size_t>(1, workers.size()));
    result.split_ms += toMillis(std::chrono::steady_clock::now() - split_started);
    for (const auto& range : ranges) {
      PendingPartition pending_part;
      pending_part.task_id = task_seq++;
      const auto serialize_started = std::chrono::steady_clock::now();
      const PreparedBinaryRowBatch prepared_input =
          batch_codec.prepareRange(batch, range.first, range.second, input_projection);
      const size_t estimated = prepared_input.estimated_size;
      if (can_use_shared_memory && estimated >= options.shared_memory_min_payload_bytes) {
        auto region = createSharedMemoryWriteRegion(estimated);
        const size_t written = batch_codec.serializePreparedRangeToBuffer(
            batch, range.first, range.second, prepared_input,
            static_cast<uint8_t*>(region.mapped), static_cast<size_t>(region.ref.size));
        closeSharedMemoryWriteRegion(&region);
        region.ref.size = written;
        pending_part.shared_memory = true;
        pending_part.shared_memory_ref = region.ref;
        result.input_shared_memory_bytes += written;
        result.used_shared_memory = true;
      } else {
        pending_part.payload = payload_pool.acquire(std::max<size_t>(estimated, 64));
        batch_codec.serializePreparedRange(batch, range.first, range.second, prepared_input,
                                           &pending_part.payload);
        result.input_payload_bytes += pending_part.payload.size();
      }
      result.coordinator_serialize_ms +=
          toMillis(std::chrono::steady_clock::now() - serialize_started);
      pending.push_back(std::move(pending_part));
    }
  }

  std::unordered_map<std::string, double> sums;
  std::vector<std::string> ordered_keys;
  size_t next_pending = 0;
  size_t inflight = 0;
  std::unordered_map<uint64_t, SharedMemoryPayload> inflight_input_shared_memory;

  auto findIdleWorker = [&]() -> WorkerHandle* {
    for (auto& worker : workers) {
      if (!worker.busy) return &worker;
    }
    return nullptr;
  };

  auto dispatchOne = [&](WorkerHandle* worker) {
    if (worker == nullptr || next_pending >= pending.size()) return false;
    auto& task = pending[next_pending++];

    worker->busy = true;
    worker->task_id = task.task_id;
    ++inflight;
    result.max_inflight_partitions = std::max(result.max_inflight_partitions, inflight);

    RpcFrame frame;
    frame.header.protocol_version = 1;
    frame.header.type = RpcMessageType::DataBatch;
    frame.header.message_id = task.task_id;
    frame.header.correlation_id = task.task_id;
    frame.header.codec_id = "actor-stream-bin-v1";
    frame.header.source = "coordinator";
    frame.header.target = worker->node_id;
    frame.payload = payload_pool.acquire(task.payload.size() + 64);

    ActorStreamRequest request;
    request.task_id = task.task_id;
    if (task.shared_memory) {
      request.shared_memory = true;
      request.shared_memory_name = task.shared_memory_ref.name;
      request.shared_memory_size = task.shared_memory_ref.size;
      inflight_input_shared_memory.emplace(task.task_id, task.shared_memory_ref);
    } else {
      request.table_payload = std::move(task.payload);
    }
    encodeActorStreamRequest(request, &frame.payload);

    auto scratch = frame_pool.acquire(frame.payload.size() + 128);
    const bool ok = sendFrameOverSocket(worker->fd, codec, frame, &scratch);
    frame_pool.release(std::move(scratch));
    payload_pool.release(std::move(task.payload));
    payload_pool.release(std::move(request.table_payload));
    payload_pool.release(std::move(frame.payload));
    if (!ok) {
      throw std::runtime_error("sendFrameOverSocket failed");
    }
    return true;
  };

  try {
    while (next_pending < pending.size() || inflight > 0) {
      while (inflight < options.max_inflight_partitions) {
        WorkerHandle* worker = findIdleWorker();
        if (worker == nullptr || next_pending >= pending.size()) break;
        dispatchOne(worker);
      }

      if (next_pending < pending.size() && inflight >= options.max_inflight_partitions) {
        ++result.blocked_count;
      }

      fd_set reads;
      FD_ZERO(&reads);
      int max_fd = -1;
      for (const auto& worker : workers) {
        if (!worker.busy) continue;
        FD_SET(worker.fd, &reads);
        if (worker.fd > max_fd) max_fd = worker.fd;
      }
      if (max_fd < 0) {
        continue;
      }

      const auto wait_started = std::chrono::steady_clock::now();
      const int ready = ::select(max_fd + 1, &reads, nullptr, nullptr, nullptr);
      result.coordinator_wait_ms += toMillis(std::chrono::steady_clock::now() - wait_started);
      if (ready <= 0) {
        throw std::runtime_error("select failed");
      }

      for (auto& worker : workers) {
        if (!worker.busy || !FD_ISSET(worker.fd, &reads)) continue;
        RpcFrame frame;
        if (!recvFrameOverSocket(worker.fd, codec, &frame)) {
          throw std::runtime_error("recvFrameOverSocket failed");
        }
        ActorStreamResponse msg;
        if (!decodeActorStreamResponse(frame.payload, &msg)) {
          throw std::runtime_error("decodeActorStreamResponse failed");
        }
        if (msg.task_id != worker.task_id) {
          throw std::runtime_error("unexpected actor stream reply");
        }
        auto input_shared = inflight_input_shared_memory.find(msg.task_id);
        if (input_shared != inflight_input_shared_memory.end()) {
          unlinkSharedMemoryPayload(input_shared->second);
          inflight_input_shared_memory.erase(input_shared);
        }
        worker.busy = false;
        worker.task_id = 0;
        --inflight;
        if (!msg.ok) {
          throw std::runtime_error(msg.reason.empty() ? "worker failed" : msg.reason);
        }

        SharedMemoryReadRegion output_region;
        if (msg.shared_memory) {
          const SharedMemoryPayload shared{msg.shared_memory_name, msg.shared_memory_size};
          result.output_shared_memory_bytes += shared.size;
          result.used_shared_memory = true;
          output_region = openSharedMemoryReadRegion(shared);
          const auto deserialize_started = std::chrono::steady_clock::now();
          const Table partial = batch_codec.deserializeFromBuffer(
              static_cast<const uint8_t*>(output_region.mapped), output_region.size);
          result.coordinator_deserialize_ms +=
              toMillis(std::chrono::steady_clock::now() - deserialize_started);
          closeSharedMemoryReadRegion(&output_region);
          unlinkSharedMemoryPayload(shared);
          result.output_payload_bytes += shared.size;

          result.worker_deserialize_ms += msg.metrics.deserialize_ms;
          result.worker_compute_ms += msg.metrics.compute_ms;
          result.worker_serialize_ms += msg.metrics.serialize_ms;

          const auto merge_started = std::chrono::steady_clock::now();
          mergePartialTable(partial, &sums, &ordered_keys);
          result.coordinator_merge_ms += toMillis(std::chrono::steady_clock::now() - merge_started);
          ++result.processed_partitions;
        } else {
          result.output_payload_bytes += msg.table_payload.size();
          const auto deserialize_started = std::chrono::steady_clock::now();
          const Table partial = batch_codec.deserialize(msg.table_payload);
          result.coordinator_deserialize_ms +=
              toMillis(std::chrono::steady_clock::now() - deserialize_started);
          result.worker_deserialize_ms += msg.metrics.deserialize_ms;
          result.worker_compute_ms += msg.metrics.compute_ms;
          result.worker_serialize_ms += msg.metrics.serialize_ms;

          const auto merge_started = std::chrono::steady_clock::now();
          mergePartialTable(partial, &sums, &ordered_keys);
          result.coordinator_merge_ms += toMillis(std::chrono::steady_clock::now() - merge_started);
          ++result.processed_partitions;
        }
      }
    }
  } catch (...) {
    for (size_t i = next_pending; i < pending.size(); ++i) {
      if (pending[i].shared_memory) {
        unlinkSharedMemoryPayload(pending[i].shared_memory_ref);
      }
    }
    for (const auto& entry : inflight_input_shared_memory) {
      unlinkSharedMemoryPayload(entry.second);
    }
    stopWorkers(&workers);
    throw;
  }

  stopWorkers(&workers);
  result.final_table = materializeStateTable(sums, ordered_keys);
  result.elapsed_ms = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - started)
          .count());
  return result;
}

LocalActorStreamResult runAutoLocalActorStreamWindowKeySum(
    const std::vector<Table>& batches, const LocalActorStreamOptions& actor_options,
    const LocalExecutionAutoOptions& auto_options, LocalExecutionDecision* decision) {
  LocalExecutionDecision local_decision;
  local_decision.chosen_mode = LocalExecutionMode::SingleProcess;
  if (batches.empty()) {
    local_decision.reason = "no batches";
    if (decision != nullptr) *decision = local_decision;
    return LocalActorStreamResult{};
  }

  local_decision.rows_per_batch = batches.front().rowCount();
  if (actor_options.worker_count < 2 || actor_options.max_inflight_partitions == 0) {
    local_decision.reason = "actor workers unavailable";
    if (decision != nullptr) *decision = local_decision;
    return measureSingleProcessWindowKeySum(batches, actor_options.cpu_spin_per_row);
  }

  const size_t sampled_batches =
      std::min(std::max<size_t>(1, auto_options.sample_batches), batches.size());
  local_decision.sampled_batches = sampled_batches;
  const std::vector<Table> sample_batches_vec(batches.begin(),
                                              batches.begin() + sampled_batches);

  const LocalActorStreamResult sample_single =
      measureSingleProcessWindowKeySum(sample_batches_vec, actor_options.cpu_spin_per_row);
  const LocalActorStreamResult sample_actor =
      runLocalActorStreamWindowKeySum(sample_batches_vec, actor_options);

  const uint64_t total_projected_bytes =
      sample_actor.input_payload_bytes + sample_actor.input_shared_memory_bytes;
  local_decision.average_projected_payload_bytes =
      sample_actor.processed_batches == 0
          ? 0
          : static_cast<size_t>(total_projected_bytes / sample_actor.processed_batches);
  local_decision.single_rows_per_s =
      toRowsPerSecond(sample_single, local_decision.rows_per_batch);
  local_decision.actor_rows_per_s = toRowsPerSecond(sample_actor, local_decision.rows_per_batch);
  local_decision.actor_speedup =
      local_decision.single_rows_per_s <= 0.0
          ? 0.0
          : (local_decision.actor_rows_per_s / local_decision.single_rows_per_s);
  const uint64_t overhead_ms = actorOverheadMs(sample_actor);
  local_decision.compute_to_overhead_ratio =
      overhead_ms == 0 ? static_cast<double>(sample_actor.worker_compute_ms)
                       : static_cast<double>(sample_actor.worker_compute_ms) /
                             static_cast<double>(overhead_ms);

  const bool rows_ok = local_decision.rows_per_batch >= auto_options.min_rows_per_batch;
  const bool payload_ok =
      local_decision.average_projected_payload_bytes >= auto_options.min_projected_payload_bytes;
  const bool ratio_ok =
      local_decision.compute_to_overhead_ratio >= auto_options.min_compute_to_overhead_ratio;
  const bool speed_ok = local_decision.actor_speedup >= auto_options.min_actor_speedup;
  const bool strong_speed_ok =
      local_decision.actor_speedup >= auto_options.strong_actor_speedup;

  local_decision.thresholds_met = rows_ok && payload_ok && speed_ok && (ratio_ok || strong_speed_ok);
  if (local_decision.thresholds_met) {
    local_decision.chosen_mode = LocalExecutionMode::ActorCredit;
    if (ratio_ok) {
      local_decision.reason =
          "actor sample passed rows, payload, compute/overhead, and speedup thresholds";
    } else {
      local_decision.reason =
          "actor sample speedup is strong enough to override compute/overhead threshold";
    }
  } else if (!rows_ok) {
    local_decision.reason = "rows_per_batch below actor threshold";
  } else if (!payload_ok) {
    local_decision.reason = "projected payload below actor threshold";
  } else if (!speed_ok) {
    local_decision.reason = "actor sample throughput not high enough";
  } else if (!ratio_ok) {
    local_decision.reason = "worker compute does not dominate actor overhead";
  } else {
    local_decision.reason = "actor sample did not satisfy selection thresholds";
  }

  if (decision != nullptr) *decision = local_decision;
  if (sampled_batches == batches.size()) {
    return local_decision.chosen_mode == LocalExecutionMode::ActorCredit ? sample_actor
                                                                         : sample_single;
  }
  if (local_decision.chosen_mode == LocalExecutionMode::ActorCredit) {
    return runLocalActorStreamWindowKeySum(batches, actor_options);
  }
  return measureSingleProcessWindowKeySum(batches, actor_options.cpu_spin_per_row);
}

}  // namespace dataflow
