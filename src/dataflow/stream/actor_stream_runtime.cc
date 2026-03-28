#include "src/dataflow/stream/actor_stream_runtime.h"

#include <chrono>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <thread>
#include <unordered_map>
#include <unistd.h>
#include <utility>
#include <vector>

#include "src/dataflow/api/dataframe.h"
#include "src/dataflow/stream/binary_row_batch.h"
#include "src/dataflow/transport/ipc_transport.h"

namespace dataflow {

namespace {

constexpr char kKeySep = '\x1f';
constexpr uint8_t kRequestKind = 1;
constexpr uint8_t kResponseKind = 2;
constexpr uint8_t kEnvelopeMagic[4] = {'A', 'S', 'B', '1'};

uint64_t toMillis(std::chrono::steady_clock::duration value) {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(value).count());
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

struct PendingPartition {
  uint64_t task_id = 0;
  Table partition;
};

struct WorkerMetrics {
  uint64_t deserialize_ms = 0;
  uint64_t compute_ms = 0;
  uint64_t serialize_ms = 0;
};

struct ActorStreamRequest {
  uint64_t task_id = 0;
  std::vector<uint8_t> table_payload;
};

struct ActorStreamResponse {
  uint64_t task_id = 0;
  bool ok = false;
  WorkerMetrics metrics;
  std::vector<uint8_t> table_payload;
  std::string reason;
};

void encodeActorStreamRequest(const ActorStreamRequest& request, std::vector<uint8_t>* out) {
  if (out == nullptr) return;
  out->clear();
  out->insert(out->end(), kEnvelopeMagic, kEnvelopeMagic + 4);
  appendU8(out, kRequestKind);
  appendU64(out, request.task_id);
  appendBytes(out, request.table_payload);
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
    appendBytes(out, response.table_payload);
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
    return readBytes(payload, &offset, &out->table_payload);
  }
  return readString(payload, &offset, &out->reason);
}

std::string makeStateKey(const Row& row, size_t window_idx, size_t key_idx) {
  return row[window_idx].toString() + kKeySep + row[key_idx].toString();
}

std::vector<Table> splitTable(const Table& table, size_t parts) {
  if (parts <= 1 || table.rows.size() <= 1) {
    return {table};
  }
  parts = std::min(parts, table.rows.size());
  std::vector<Table> out(parts, Table(table.schema, {}));
  const size_t rows_per_part = (table.rows.size() + parts - 1) / parts;
  size_t part = 0;
  size_t assigned = 0;
  for (const auto& row : table.rows) {
    out[part].rows.push_back(row);
    ++assigned;
    if (assigned >= rows_per_part && part + 1 < parts) {
      ++part;
      assigned = 0;
    }
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

Table aggregatePartition(const Table& input) {
  return DataFrame(input).groupBy({"window_start", "key"}).sum("value", "partial_sum").toTable();
}

double spinValue(double value, size_t cpu_spin_per_row) {
  double out = value;
  for (size_t i = 0; i < cpu_spin_per_row; ++i) {
    out = out * 1.0000001 + static_cast<double>(i % 7);
    out = out / 1.0000001;
  }
  return out;
}

Table aggregatePartitionWithWork(const Table& input, size_t cpu_spin_per_row) {
  if (cpu_spin_per_row == 0) {
    return aggregatePartition(input);
  }
  if (!input.schema.has("window_start") || !input.schema.has("key") || !input.schema.has("value")) {
    throw std::runtime_error("aggregatePartitionWithWork schema mismatch");
  }
  const auto window_idx = input.schema.indexOf("window_start");
  const auto key_idx = input.schema.indexOf("key");
  const auto value_idx = input.schema.indexOf("value");

  std::unordered_map<std::string, double> sums;
  std::vector<std::string> ordered_keys;
  for (const auto& row : input.rows) {
    const auto state_key = makeStateKey(row, window_idx, key_idx);
    auto it = sums.find(state_key);
    if (it == sums.end()) {
      ordered_keys.push_back(state_key);
      it = sums.emplace(state_key, 0.0).first;
    }
    it->second += spinValue(row[value_idx].asDouble(), cpu_spin_per_row);
  }

  Table out(Schema({"window_start", "key", "partial_sum"}), {});
  for (const auto& state_key : ordered_keys) {
    const auto split = state_key.find(kKeySep);
    Row row;
    row.emplace_back(state_key.substr(0, split));
    row.emplace_back(state_key.substr(split + 1));
    row.emplace_back(sums[state_key]);
    out.rows.push_back(std::move(row));
  }
  return out;
}

void workerLoop(int fd, uint64_t delay_ms, size_t cpu_spin_per_row) {
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
      const Table input = batch_codec.deserialize(request.table_payload);
      const auto compute_started = std::chrono::steady_clock::now();
      const Table partial = aggregatePartitionWithWork(input, cpu_spin_per_row);
      const auto serialize_started = std::chrono::steady_clock::now();

      result.ok = true;
      result.metrics.deserialize_ms = toMillis(compute_started - deserialize_started);
      result.metrics.compute_ms = toMillis(serialize_started - compute_started);

      auto table_payload = payload_pool.acquire(partial.rowCount() * 32);
      batch_codec.serialize(partial, &table_payload, output_projection);
      result.metrics.serialize_ms = toMillis(std::chrono::steady_clock::now() - serialize_started);
      result.table_payload = std::move(table_payload);
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
      workerLoop(fds[1], options.worker_delay_ms, options.cpu_spin_per_row);
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

  std::vector<PendingPartition> pending;
  uint64_t task_seq = 1;
  const auto split_started = std::chrono::steady_clock::now();
  for (const auto& batch : batches) {
    ++result.processed_batches;
    auto parts = splitTable(batch, std::max<size_t>(1, workers.size()));
    for (auto& part : parts) {
      PendingPartition pending_part;
      pending_part.task_id = task_seq++;
      pending_part.partition = std::move(part);
      pending.push_back(std::move(pending_part));
    }
  }
  result.split_ms = toMillis(std::chrono::steady_clock::now() - split_started);

  std::unordered_map<std::string, double> sums;
  std::vector<std::string> ordered_keys;
  size_t next_pending = 0;
  size_t inflight = 0;

  auto findIdleWorker = [&]() -> WorkerHandle* {
    for (auto& worker : workers) {
      if (!worker.busy) return &worker;
    }
    return nullptr;
  };

  auto dispatchOne = [&](WorkerHandle* worker) {
    if (worker == nullptr || next_pending >= pending.size()) return false;
    const auto& task = pending[next_pending++];
    const auto serialize_started = std::chrono::steady_clock::now();
    std::vector<uint8_t> table_payload = payload_pool.acquire(task.partition.rowCount() * 32);
    batch_codec.serialize(task.partition, &table_payload, input_projection);
    result.coordinator_serialize_ms +=
        toMillis(std::chrono::steady_clock::now() - serialize_started);
    result.input_payload_bytes += table_payload.size();

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
    frame.payload = payload_pool.acquire(table_payload.size() + 64);

    ActorStreamRequest request;
    request.task_id = task.task_id;
    request.table_payload = std::move(table_payload);
    encodeActorStreamRequest(request, &frame.payload);

    auto scratch = frame_pool.acquire(frame.payload.size() + 128);
    const bool ok = sendFrameOverSocket(worker->fd, codec, frame, &scratch);
    frame_pool.release(std::move(scratch));
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
        worker.busy = false;
        worker.task_id = 0;
        --inflight;
        if (!msg.ok) {
          throw std::runtime_error(msg.reason.empty() ? "worker failed" : msg.reason);
        }

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
  } catch (...) {
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

}  // namespace dataflow
