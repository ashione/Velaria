#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "src/dataflow/core/execution/table.h"

namespace dataflow {

struct SourceBatchToken {
  std::string value;
};

enum class SourceStatus {
  Ok,
  EndOfStream,
  Blocked,
};

enum class SinkStatus {
  Ok,
  Blocked,
  Failed,
};

struct RuntimeSourceContext {
  std::string query_id;
  uint64_t trigger_interval_ms = 0;
  std::size_t backlog_batches = 0;
  std::size_t inflight_batches = 0;
  std::size_t max_inflight_batches = 0;
  std::string checkpoint_path;
};

struct RuntimeSinkContext {
  std::string query_id;
  std::string checkpoint_path;
  std::string execution_mode;
  std::string transport_mode;
};

struct RuntimeCheckpointMarker {
  std::string query_id;
  std::string source_offset;
  std::size_t batches_processed = 0;
};

class RuntimeSource {
 public:
  virtual ~RuntimeSource() = default;
  virtual Schema schema() const = 0;
  virtual void open(const RuntimeSourceContext& context) = 0;
  virtual SourceStatus nextBatch(Table* out, SourceBatchToken* token) = 0;
  virtual void ack(const SourceBatchToken& token) { (void)token; }
  virtual void checkpoint(const RuntimeCheckpointMarker& marker) { (void)marker; }
  virtual void close() = 0;
};

class RuntimeSink {
 public:
  virtual ~RuntimeSink() = default;
  virtual void open(const RuntimeSinkContext& context) = 0;
  virtual SinkStatus write(const Table& batch) = 0;
  virtual void flush() {}
  virtual void checkpoint(const RuntimeCheckpointMarker& marker) { (void)marker; }
  virtual void close() = 0;
};

}  // namespace dataflow
