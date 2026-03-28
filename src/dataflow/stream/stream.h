#pragma once

#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "src/dataflow/core/table.h"

namespace dataflow {

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
  virtual bool appendValueToList(const std::string& listKey, const std::string& value);
  virtual bool popValueFromList(const std::string& listKey, std::string* value);

  virtual bool listKeys(std::vector<std::string>* keys) const = 0;

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
  bool appendValueToList(const std::string& listKey, const std::string& value) override;
  bool popValueFromList(const std::string& listKey, std::string* value) override;

  bool listKeys(std::vector<std::string>* keys) const override;

 private:
  std::unordered_map<std::string, std::string> values_;
  std::unordered_map<std::string, std::unordered_map<std::string, std::string>> maps_;
  std::unordered_map<std::string, std::vector<std::string>> lists_;
};

// RocksDB implementation is optional and auto-detected by build environment.
// If the build does not contain rocksdb headers, requesting backend="rocksdb"
// will fail at runtime with a clear error.
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
std::shared_ptr<StateStore> makeStateStore(const std::string& backend,
                                          const std::unordered_map<std::string, std::string>& options = {});

class StreamSource {
 public:
  virtual ~StreamSource() = default;
  virtual bool nextBatch(Table& batch) = 0;
};

class MemoryStreamSource : public StreamSource {
 public:
  explicit MemoryStreamSource(std::vector<Table> batches);

  bool nextBatch(Table& batch) override;

 private:
  std::vector<Table> batches_;
  size_t index_ = 0;
};

class StreamSink {
 public:
  virtual ~StreamSink() = default;
  virtual void write(const Table& table) = 0;
};

class ConsoleStreamSink : public StreamSink {
 public:
  explicit ConsoleStreamSink(std::string name = "console") : name_(std::move(name)) {}
  void write(const Table& table) override;

 private:
  std::string name_;
};

class StreamTransform {
 public:
  using Fn = std::function<Table(const Table&)>;
  explicit StreamTransform(Fn f) : fn_(std::move(f)) {}
  Table operator()(const Table& in) const { return fn_(in); }

 private:
  Fn fn_;
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
  StreamingDataFrame drop(const std::string& column) const;
  StreamingDataFrame limit(size_t n) const;

  StreamingDataFrame withStateStore(std::shared_ptr<StateStore> state) const;
  GroupedStreamingDataFrame groupBy(const std::vector<std::string>& keys) const;

  StreamingQuery writeStream(std::shared_ptr<StreamSink> sink, uint64_t triggerIntervalMs = 1000) const;
  StreamingQuery writeStreamToConsole(uint64_t triggerIntervalMs = 1000) const;

  Table applyTransforms(const Table& batch) const;

 private:
  friend class StreamingQuery;
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

  StreamingDataFrame sum(const std::string& valueColumn, bool stateful = false) const;

 private:
  std::shared_ptr<StreamSource> source_;
  std::vector<StreamTransform> transforms_;
  std::vector<std::string> keys_;
  std::shared_ptr<StateStore> state_;
};

class StreamingQuery {
 public:
  StreamingQuery(StreamingDataFrame root, std::shared_ptr<StreamSink> sink, uint64_t triggerIntervalMs);

  size_t awaitTermination(size_t maxBatches = 0);
  void stop();

 private:
  StreamingDataFrame root_;
  std::shared_ptr<StreamSink> sink_;
  uint64_t intervalMs_;
  bool running_ = false;
};

}  // namespace dataflow
