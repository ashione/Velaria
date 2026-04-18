#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <string>
#include <unordered_map>
#include <vector>
#include <deque>

#include "src/dataflow/core/logical/planner/plan.h"
#include "src/dataflow/core/execution/table.h"
#include "src/dataflow/core/contract/source_sink_abi.h"

namespace dataflow {

namespace sql {
class SqlPlanner;
}

using StreamId = std::uint64_t;

class StreamingDataFrame;
class StreamingQuery;

class StateStore {
 public:
  virtual ~StateStore() = default;

  virtual bool get(const std::string& key, std::string* out) const = 0;
  virtual void put(const std::string& key, const std::string& value) = 0;
  virtual bool remove(const std::string& key) = 0;
  virtual void close() {}

  virtual bool getMapField(const std::string& mapKey, const std::string& field,
                           std::string* out) const;
  virtual void putMapField(const std::string& mapKey, const std::string& field,
                           const std::string& value);
  virtual bool removeMapField(const std::string& mapKey, const std::string& field);
  virtual bool getMapFields(const std::string& mapKey,
                            std::vector<std::string>* fields) const;

  virtual bool getValueList(const std::string& listKey, std::vector<std::string>* values) const;
  virtual bool setValueList(const std::string& listKey, const std::vector<std::string>& values);
  virtual bool appendValueToList(const std::string& listKey, const std::string& value);
  virtual bool popValueFromList(const std::string& listKey, std::string* value);

  virtual bool listKeys(std::vector<std::string>* keys) const = 0;
  virtual bool listKeysByPrefix(const std::string& prefix, std::vector<std::string>* keys) const;
  virtual bool removeKeysByPrefix(const std::string& prefix);

  double getDouble(const std::string& key, double defaultValue = 0.0) const;
  double addDouble(const std::string& key, double delta);
};

class MemoryStateStore : public StateStore {
 public:
  bool get(const std::string& key, std::string* out) const override;
  void put(const std::string& key, const std::string& value) override;
  bool remove(const std::string& key) override;

  bool getMapField(const std::string& mapKey, const std::string& field,
                   std::string* out) const override;
  void putMapField(const std::string& mapKey, const std::string& field,
                   const std::string& value) override;
  bool removeMapField(const std::string& mapKey, const std::string& field) override;
  bool getMapFields(const std::string& mapKey,
                    std::vector<std::string>* fields) const override;

  bool getValueList(const std::string& listKey, std::vector<std::string>* values) const override;
  bool setValueList(const std::string& listKey,
                    const std::vector<std::string>& values) override;
  bool appendValueToList(const std::string& listKey, const std::string& value) override;
  bool popValueFromList(const std::string& listKey, std::string* value) override;

  bool listKeys(std::vector<std::string>* keys) const override;

 private:
  std::unordered_map<std::string, std::string> values_;
  std::unordered_map<std::string, std::unordered_map<std::string, std::string>> maps_;
  std::unordered_map<std::string, std::vector<std::string>> lists_;
};

#if __has_include(<rocksdb/db.h>)
class RocksDbStateStore : public StateStore {
 public:
  static std::shared_ptr<RocksDbStateStore> create(const std::string& dbPath);

  explicit RocksDbStateStore(std::string dbPath);
  ~RocksDbStateStore() override;

  bool get(const std::string& key, std::string* out) const override;
  void put(const std::string& key, const std::string& value) override;
  bool remove(const std::string& key) override;
  bool listKeys(std::vector<std::string>* keys) const override;

  bool getMapField(const std::string& mapKey, const std::string& field,
                   std::string* out) const override;
  void putMapField(const std::string& mapKey, const std::string& field,
                   const std::string& value) override;
  bool removeMapField(const std::string& mapKey, const std::string& field) override;
  bool getMapFields(const std::string& mapKey,
                    std::vector<std::string>* fields) const override;

  bool getValueList(const std::string& listKey, std::vector<std::string>* values) const override;
  bool setValueList(const std::string& listKey,
                    const std::vector<std::string>& values) override;
  bool appendValueToList(const std::string& listKey, const std::string& value) override;
  bool popValueFromList(const std::string& listKey, std::string* value) override;
  void close() override;

 private:
  void ensureOpen() const;
  std::string encodeList(const std::vector<std::string>& values) const;
  bool decodeList(const std::string& raw, std::vector<std::string>* values) const;
  std::string makeMapFieldKey(const std::string& mapKey, const std::string& field) const;
  std::string makeListKey(const std::string& listKey) const;

  std::string dbPath_;
  mutable void* db_ = nullptr;
};
#endif

struct StateStoreConfig {
  std::string backend = "memory";
  std::string defaultPath = "/tmp/dataflow-state";
  std::unordered_map<std::string, std::string> options;
};

std::shared_ptr<StateStore> makeMemoryStateStore();
std::shared_ptr<StateStore> makeRocksDbStateStore(const std::string& dbPath);
std::shared_ptr<StateStore> makeStateStore(const StateStoreConfig& config);
std::shared_ptr<StateStore> makeStateStore(
    const std::string& backend,
    const std::unordered_map<std::string, std::string>& options = {});

enum class StreamingExecutionMode { SingleProcess, LocalWorkers };
enum class StreamingTransportMode { InProcess, RpcCopy, SharedMemory, Auto };

enum class CheckpointDeliveryMode { AtLeastOnce, BestEffort };

struct StreamingQueryOptions {
  uint64_t trigger_interval_ms = 1000;
  size_t max_inflight_batches = 2;
  size_t max_queued_partitions = 8;
  size_t backpressure_high_watermark = 2;
  size_t backpressure_low_watermark = 1;
  std::string checkpoint_path;
  StreamingExecutionMode execution_mode = StreamingExecutionMode::SingleProcess;
  size_t local_workers = 1;
  size_t max_inflight_partitions = 0;
  bool shared_memory_transport = true;
  size_t shared_memory_min_payload_bytes = 64 * 1024;
  uint64_t idle_wait_ms = 100;
  size_t max_retained_windows = 0;
  CheckpointDeliveryMode checkpoint_delivery_mode = CheckpointDeliveryMode::AtLeastOnce;

  size_t effectiveLocalWorkers() const {
    return execution_mode == StreamingExecutionMode::LocalWorkers && local_workers > 1
               ? local_workers
               : 1;
  }
};

struct StreamPullContext {
  std::string query_id;
  // `backlog_batches` and `inflight_batches` both represent the number of
  // queued batches that have been pulled but not yet consumed by the query.
  size_t backlog_batches = 0;
  size_t inflight_batches = 0;
  // `queued_partitions` tracks the queued partitions implied by the current
  // backlog, not the partitions already being processed by the consumer.
  size_t queued_partitions = 0;
  size_t max_inflight_batches = 0;
  bool backpressure_active = false;
};

struct StreamSourceContext {
  std::string query_id;
  uint64_t trigger_interval_ms = 0;
  size_t max_inflight_batches = 0;
  size_t max_queued_partitions = 0;
  std::string checkpoint_path;
  bool source_is_bounded = true;
};

struct StreamSinkContext {
  std::string query_id;
  std::string checkpoint_path;
  std::string requested_execution_mode;
};

struct StreamCheckpointMarker {
  std::string query_id;
  std::string source_offset;
  size_t batches_processed = 0;
};

struct StreamingStrategyDecision {
  std::string requested_execution_mode = "single-process";
  std::string resolved_execution_mode = "single-process";
  std::string transport_mode = "inproc";
  std::string aggregate_impl = "not-applicable";
  std::string aggregate_reason;
  std::string reason;
  bool actor_eligible = false;
  bool used_actor_runtime = false;
  bool used_shared_memory = false;
  bool has_stateful_ops = false;
  bool has_window = false;
  bool sink_is_blocking = false;
  bool source_is_bounded = true;
  size_t estimated_partitions = 1;
  size_t projected_payload_bytes = 0;
  size_t sampled_batches = 0;
  size_t sampled_rows_per_batch = 0;
  size_t average_projected_payload_bytes = 0;
  double actor_speedup = 0.0;
  double compute_to_overhead_ratio = 0.0;
  size_t estimated_state_size_bytes = 0;
  size_t estimated_batch_cost = 0;
  size_t backpressure_max_queue_batches = 0;
  size_t backpressure_high_watermark = 0;
  size_t backpressure_low_watermark = 0;
  std::string checkpoint_delivery_mode = "at-least-once";
};

class StreamSink;
StreamingStrategyDecision describeStreamingStrategy(const StreamingDataFrame& root,
                                                    const std::shared_ptr<StreamSink>& sink,
                                                    const StreamingQueryOptions& options);
void validateStreamingOrderRequirements(const StreamingDataFrame& root);

struct StreamingQueryProgress {
  std::string query_id;
  std::string status = "created";
  std::string requested_execution_mode = "single-process";
  std::string execution_mode = "single-process";
  std::string execution_reason;
  std::string transport_mode = "inproc";
  std::string aggregate_impl = "not-applicable";
  std::string aggregate_reason;
  size_t batches_pulled = 0;
  size_t batches_processed = 0;
  // `blocked_count` counts distinct producer-side waits caused by bounded
  // backlog or partition pressure. It does not count spin loops.
  size_t blocked_count = 0;
  // `max_backlog_batches` is the largest observed queued backlog immediately
  // after enqueueing a pulled batch.
  size_t max_backlog_batches = 0;
  // `inflight_*` represent the queued backlog still waiting to be consumed.
  size_t inflight_batches = 0;
  size_t inflight_partitions = 0;
  // `last_batch_latency_ms` covers one batch from execution start through sink
  // flush completion. `last_sink_latency_ms` is the sink write/flush portion.
  // `last_state_latency_ms` is state/window finalize time and is zero for
  // stateless batches.
  uint64_t last_batch_latency_ms = 0;
  uint64_t last_sink_latency_ms = 0;
  uint64_t last_state_latency_ms = 0;
  std::string last_source_offset;
  bool backpressure_active = false;
  bool actor_eligible = false;
  bool used_actor_runtime = false;
  bool used_shared_memory = false;
  bool has_stateful_ops = false;
  bool has_window = false;
  bool sink_is_blocking = false;
  bool source_is_bounded = true;
  size_t estimated_partitions = 1;
  size_t projected_payload_bytes = 0;
  size_t sampled_batches = 0;
  size_t sampled_rows_per_batch = 0;
  size_t average_projected_payload_bytes = 0;
  double actor_speedup = 0.0;
  double compute_to_overhead_ratio = 0.0;
  size_t estimated_state_size_bytes = 0;
  size_t estimated_batch_cost = 0;
  size_t backpressure_max_queue_batches = 0;
  size_t backpressure_high_watermark = 0;
  size_t backpressure_low_watermark = 0;
  std::string checkpoint_delivery_mode = "at-least-once";
};

class StreamSource {
 public:
  virtual ~StreamSource() = default;
  virtual void open(const StreamSourceContext& context) { (void)context; }
  virtual bool nextBatch(const StreamPullContext& context, Table& batch) = 0;
  virtual bool bounded() const { return true; }
  virtual std::string currentOffsetToken() const { return ""; }
  virtual bool restoreOffsetToken(const std::string& token) {
    (void)token;
    return false;
  }
  virtual void checkpoint(const StreamCheckpointMarker& marker) { (void)marker; }
  virtual void close() {}
  virtual std::string describe() const { return "stream-source"; }
};

class MemoryStreamSource : public StreamSource {
 public:
  explicit MemoryStreamSource(std::vector<Table> batches);

  void open(const StreamSourceContext& context) override;
  bool nextBatch(const StreamPullContext& context, Table& batch) override;
  std::string currentOffsetToken() const override;
  bool restoreOffsetToken(const std::string& token) override;
  std::string describe() const override { return "memory"; }

 private:
  std::vector<Table> batches_;
  size_t index_ = 0;
};

class DirectoryCsvStreamSource : public StreamSource {
 public:
  explicit DirectoryCsvStreamSource(std::string directory, char delimiter = ',');

  void open(const StreamSourceContext& context) override;
  bool nextBatch(const StreamPullContext& context, Table& batch) override;
  bool bounded() const override { return false; }
  std::string currentOffsetToken() const override;
  bool restoreOffsetToken(const std::string& token) override;
  std::string describe() const override;

 private:
  std::string directory_;
  char delimiter_ = ',';
  std::string resume_after_;
  std::string last_processed_;
};

class QueueStreamSource : public StreamSource {
 public:
  explicit QueueStreamSource(Schema schema);

  void open(const StreamSourceContext& context) override;
  bool nextBatch(const StreamPullContext& context, Table& batch) override;
  bool bounded() const override;
  std::string currentOffsetToken() const override;
  bool restoreOffsetToken(const std::string& token) override;
  void close() override;
  std::string describe() const override { return "queue"; }

  void push(Table batch);
  void closeInput();
  Schema schema() const { return schema_; }

 private:
  Schema schema_;
  mutable std::mutex mu_;
  std::deque<std::pair<std::size_t, Table>> queue_;
  std::size_t next_offset_ = 0;
  std::size_t consumed_offset_ = 0;
  bool closed_ = false;
};

class StreamSink {
 public:
  virtual ~StreamSink() = default;
  virtual void open(const StreamSinkContext& context) { (void)context; }
  virtual void write(const Table& table) = 0;
  virtual void flush() {}
  virtual void checkpoint(const StreamCheckpointMarker& marker) { (void)marker; }
  virtual void close() {}
  virtual std::string name() const { return "sink"; }
};

class ConsoleStreamSink : public StreamSink {
 public:
  explicit ConsoleStreamSink(std::string name = "console") : name_(std::move(name)) {}
  void write(const Table& table) override;
  std::string name() const override { return name_; }

 private:
  std::string name_;
};

class FileAppendStreamSink : public StreamSink {
 public:
  explicit FileAppendStreamSink(std::string path, char delimiter = ',');

  void open(const StreamSinkContext& context) override;
  void write(const Table& table) override;
  void flush() override;
  std::string name() const override { return "file_append"; }

 private:
  std::string path_;
  char delimiter_ = ',';
  bool wrote_schema_ = false;
};

class MemoryStreamSink : public StreamSink {
 public:
  void write(const Table& table) override;
  std::string name() const override { return "memory"; }

  const Table& lastTable() const { return last_table_; }
  size_t batchesWritten() const { return batches_written_; }
  size_t rowsWritten() const { return rows_written_; }

 private:
  Table last_table_;
  size_t batches_written_ = 0;
  size_t rows_written_ = 0;
};

class QueueStreamSink : public StreamSink {
 public:
  void write(const Table& table) override;
  void close() override;
  std::string name() const override { return "queue"; }

  bool pop(Table* table);
  bool closed() const;

 private:
  mutable std::mutex mu_;
  std::deque<Table> queue_;
  bool closed_ = false;
};

enum class StreamTransformMode { PartitionLocal, GlobalBarrier };

enum class StreamAcceleratorKind { None, GroupedAggregate };

struct StreamAggregateSpec {
  AggregateFunction function = AggregateFunction::Sum;
  std::string value_column;
  std::string output_column;
  bool is_count_star = false;
  std::string state_label;
};

struct StreamAcceleratorSpec {
  StreamAcceleratorKind kind = StreamAcceleratorKind::None;
  bool stateful = false;
  std::vector<std::string> group_keys;
  std::vector<StreamAggregateSpec> aggregates;
};

class RuntimeSourceAdapter : public StreamSource {
 public:
  explicit RuntimeSourceAdapter(std::shared_ptr<RuntimeSource> source);

  void open(const StreamSourceContext& context) override;
  bool nextBatch(const StreamPullContext& context, Table& batch) override;
  std::string currentOffsetToken() const override;
  bool restoreOffsetToken(const std::string& token) override;
  void checkpoint(const StreamCheckpointMarker& marker) override;
  void close() override;
  std::string describe() const override;

 private:
  std::shared_ptr<RuntimeSource> source_;
  std::string last_token_;
};

class RuntimeSinkAdapter : public StreamSink {
 public:
  explicit RuntimeSinkAdapter(std::shared_ptr<RuntimeSink> sink);

  void open(const StreamSinkContext& context) override;
  void write(const Table& table) override;
  void flush() override;
  void checkpoint(const StreamCheckpointMarker& marker) override;
  void close() override;
  std::string name() const override;

 private:
  std::shared_ptr<RuntimeSink> sink_;
};

std::shared_ptr<StreamSource> makeRuntimeSourceAdapter(std::shared_ptr<RuntimeSource> source);
std::shared_ptr<StreamSink> makeRuntimeSinkAdapter(std::shared_ptr<RuntimeSink> sink);

class StreamTransform {
 public:
  using Fn = std::function<Table(const Table&, const StreamingQueryOptions&)>;

  StreamTransform(Fn f, StreamTransformMode mode = StreamTransformMode::PartitionLocal,
                  bool touches_state = false, std::string label = {},
                  StreamAcceleratorSpec accelerator = {});

  Table operator()(const Table& in, const StreamingQueryOptions& options) const {
    return fn_(in, options);
  }
  StreamTransformMode mode() const { return mode_; }
  bool touchesState() const { return touches_state_; }
  const std::string& label() const { return label_; }
  const StreamAcceleratorSpec& accelerator() const { return accelerator_; }

 private:
  Fn fn_;
  StreamTransformMode mode_;
  bool touches_state_ = false;
  std::string label_;
  StreamAcceleratorSpec accelerator_;
};

class GroupedStreamingDataFrame;

class StreamingDataFrame {
 public:
  explicit StreamingDataFrame(std::shared_ptr<StreamSource> source);
  explicit StreamingDataFrame(std::shared_ptr<StreamSource> source,
                              std::vector<StreamTransform> transforms,
                              std::shared_ptr<StateStore> state);

  StreamingDataFrame select(const std::vector<std::string>& columns) const;
  StreamingDataFrame filter(const std::string& column, const std::string& op,
                            const Value& value) const;
  StreamingDataFrame withColumn(const std::string& name, const std::string& sourceColumn) const;
  StreamingDataFrame withColumn(const std::string& name, ComputedColumnKind function,
                               const std::vector<ComputedColumnArg>& args) const;
  StreamingDataFrame drop(const std::string& column) const;
  StreamingDataFrame orderBy(const std::vector<std::string>& columns,
                             const std::vector<bool>& ascending = {}) const;
  StreamingDataFrame limit(size_t n) const;
  StreamingDataFrame window(const std::string& timeColumn, uint64_t windowMs,
                            const std::string& outputColumn = "window_start",
                            uint64_t slideMs = 0) const;

  StreamingDataFrame withStateStore(std::shared_ptr<StateStore> state) const;
  GroupedStreamingDataFrame groupBy(const std::vector<std::string>& keys) const;

  StreamingQuery writeStream(std::shared_ptr<StreamSink> sink,
                             StreamingQueryOptions options = {}) const;
  StreamingQuery writeStream(std::shared_ptr<StreamSink> sink, uint64_t triggerIntervalMs) const;
  StreamingQuery writeStreamToConsole(StreamingQueryOptions options = {}) const;
  StreamingQuery writeStreamToConsole(uint64_t triggerIntervalMs) const;

  Table applyTransforms(const Table& batch, const StreamingQueryOptions& options,
                        uint64_t* state_ms = nullptr, size_t* partitions = nullptr) const;

 private:
  friend class StreamingQuery;
  friend StreamingStrategyDecision describeStreamingStrategy(
      const StreamingDataFrame& root, const std::shared_ptr<StreamSink>& sink,
      const StreamingQueryOptions& options);
  friend void validateStreamingOrderRequirements(const StreamingDataFrame& root);
  std::shared_ptr<StreamSource> source_;
  std::vector<StreamTransform> transforms_;
  std::shared_ptr<StateStore> state_;
};

class GroupedStreamingDataFrame {
 public:
  GroupedStreamingDataFrame(std::shared_ptr<StreamSource> source,
                            std::vector<StreamTransform> transforms,
                            std::vector<std::string> keys,
                            std::shared_ptr<StateStore> state);

  StreamingDataFrame sum(const std::string& valueColumn, bool stateful = false,
                         const std::string& outputColumn = "sum") const;
  StreamingDataFrame count(bool stateful = false,
                           const std::string& outputColumn = "count") const;
  StreamingDataFrame min(const std::string& valueColumn, bool stateful = false,
                         const std::string& outputColumn = "min") const;
  StreamingDataFrame max(const std::string& valueColumn, bool stateful = false,
                         const std::string& outputColumn = "max") const;
  StreamingDataFrame avg(const std::string& valueColumn, bool stateful = false,
                         const std::string& outputColumn = "avg") const;

 private:
  friend class sql::SqlPlanner;
  StreamingDataFrame aggregate(const std::vector<StreamAggregateSpec>& specs,
                               bool stateful) const;

  std::shared_ptr<StreamSource> source_;
  std::vector<StreamTransform> transforms_;
  std::vector<std::string> keys_;
  std::shared_ptr<StateStore> state_;
};

class StreamingQuery {
 public:
  StreamingQuery(StreamingDataFrame root, std::shared_ptr<StreamSink> sink,
                 StreamingQueryOptions options);
  StreamingQuery(StreamingQuery&& other) noexcept;
  StreamingQuery& operator=(StreamingQuery&& other) noexcept;
  StreamingQuery(const StreamingQuery&) = delete;
  StreamingQuery& operator=(const StreamingQuery&) = delete;

  StreamingQuery& trigger(uint64_t triggerIntervalMs);
  StreamingQuery& start();
  size_t awaitTermination(size_t maxBatches = 0);
  void stop();

  StreamingQueryProgress progress() const;
  std::string snapshotJson() const;
  const StreamingStrategyDecision& strategyDecision() const { return strategy_decision_; }

 private:
  void applyStrategyDecision(const StreamingStrategyDecision& decision);
  void updateProgress(const std::function<void(StreamingQueryProgress&)>& update);
  bool loadCheckpoint();
  void persistCheckpoint(const StreamingQueryProgress& progress) const;

  StreamingDataFrame root_;
  std::shared_ptr<StreamSink> sink_;
  StreamingQueryOptions options_;
  mutable std::mutex progress_mu_;
  StreamingQueryProgress progress_;
  std::atomic<bool> running_{false};
  bool started_ = false;
  bool execution_decided_ = false;
  StreamingExecutionMode resolved_execution_mode_ = StreamingExecutionMode::SingleProcess;
  std::string execution_reason_;
  StreamingStrategyDecision strategy_decision_;
};

}  // namespace dataflow
