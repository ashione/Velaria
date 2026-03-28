#include "src/dataflow/stream/stream.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_set>

#if __has_include(<rocksdb/db.h>)
#include <rocksdb/db.h>
#define DATAFLOW_HAS_ROCKSDB_BACKEND 1
#else
#define DATAFLOW_HAS_ROCKSDB_BACKEND 0
#endif

#include "src/dataflow/ai/plugin_runtime.h"
#include "src/dataflow/api/dataframe.h"
#include "src/dataflow/core/csv.h"
#include "src/dataflow/stream/actor_stream_runtime.h"

namespace dataflow {

namespace {

bool eqPred(const Value& lhs, const Value& rhs) { return lhs == rhs; }
bool nePred(const Value& lhs, const Value& rhs) { return lhs != rhs; }
bool ltPred(const Value& lhs, const Value& rhs) { return lhs < rhs; }
bool gtPred(const Value& lhs, const Value& rhs) { return lhs > rhs; }
bool ltePred(const Value& lhs, const Value& rhs) { return lhs < rhs || lhs == rhs; }
bool gtePred(const Value& lhs, const Value& rhs) { return lhs > rhs || lhs == rhs; }

using Pred = bool (*)(const Value&, const Value&);

Pred resolvePred(const std::string& op) {
  if (op == "==" || op == "=") return &eqPred;
  if (op == "!=") return &nePred;
  if (op == "<") return &ltPred;
  if (op == ">") return &gtPred;
  if (op == "<=") return &ltePred;
  if (op == ">=") return &gtePred;
  throw std::invalid_argument("unsupported filter op: " + op);
}

std::string makeStateKey(const Row& row, const std::vector<size_t>& keyIdx,
                         const std::string& prefix) {
  std::string key = prefix;
  for (size_t i = 0; i < keyIdx.size(); ++i) {
    if (i > 0) key.push_back('\x1f');
    const auto idx = keyIdx[i];
    if (idx < row.size()) key += row[idx].toString();
  }
  return key;
}

#if DATAFLOW_HAS_ROCKSDB_BACKEND
std::string encodeToken(const std::string& token) {
  return std::to_string(token.size()) + ":" + token;
}

bool parseToken(const std::string& raw, size_t& pos, std::string* token) {
  if (pos >= raw.size()) return false;
  auto sep = raw.find(':', pos);
  if (sep == std::string::npos || sep == pos) return false;
  size_t len = 0;
  for (size_t i = pos; i < sep; ++i) {
    char c = raw[i];
    if (c < '0' || c > '9') return false;
    len = len * 10 + static_cast<size_t>(c - '0');
  }
  pos = sep + 1;
  if (pos + len > raw.size()) return false;
  *token = raw.substr(pos, len);
  pos += len;
  return true;
}

std::string encodeStringList(const std::vector<std::string>& values) {
  std::string encoded;
  for (size_t i = 0; i < values.size(); ++i) {
    if (i > 0) encoded.push_back('|');
    encoded += encodeToken(values[i]);
  }
  return encoded;
}

bool decodeStringList(const std::string& raw, std::vector<std::string>* values) {
  values->clear();
  size_t pos = 0;
  while (pos < raw.size()) {
    std::string token;
    if (!parseToken(raw, pos, &token)) return false;
    values->push_back(std::move(token));
    if (pos == raw.size()) break;
    if (raw[pos] != '|') return false;
    ++pos;
  }
  return true;
}
#endif

std::string nextStreamingQueryId() {
  static std::atomic<std::uint64_t> id{1};
  std::ostringstream oss;
  oss << "stream-query-" << id.fetch_add(1);
  return oss.str();
}

uint64_t toMillis(const std::chrono::steady_clock::duration& d) {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(d).count());
}

std::string jsonEscape(const std::string& input) {
  std::string out;
  out.reserve(input.size());
  for (char c : input) {
    switch (c) {
      case '"':
        out += "\\\"";
        break;
      case '\\':
        out += "\\\\";
        break;
      case '\n':
        out += "\\n";
        break;
      default:
        out.push_back(c);
        break;
    }
  }
  return out;
}

uint64_t parseTimestampMillis(const Value& value) {
  if (value.isNumber()) {
    return static_cast<uint64_t>(value.asInt64());
  }

  const std::string& raw = value.asString();
  if (raw.empty()) return 0;
  bool numeric = std::all_of(raw.begin(), raw.end(), [](char c) { return c >= '0' && c <= '9'; });
  if (numeric) {
    return static_cast<uint64_t>(std::stoll(raw));
  }

  std::tm tm = {};
  std::istringstream in(raw);
  in >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
  if (in.fail()) {
    in.clear();
    in.str(raw);
    in >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
  }
  if (in.fail()) {
    throw std::runtime_error("unsupported timestamp format: " + raw);
  }
  const std::time_t seconds = timegm(&tm);
  return static_cast<uint64_t>(seconds) * 1000;
}

std::string formatTimestampMillis(uint64_t millis) {
  const std::time_t seconds = static_cast<std::time_t>(millis / 1000);
  std::tm tm = {};
  gmtime_r(&seconds, &tm);
  char buf[32];
  std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", &tm);
  return std::string(buf);
}

size_t safeWorkerCount(const StreamingQueryOptions& options) {
  return std::max<size_t>(1, options.effectiveLocalWorkers());
}

size_t partitionCountForBatch(const Table& table, const StreamingQueryOptions& options) {
  if (table.rows.empty()) return 1;
  return std::min<size_t>(safeWorkerCount(options), table.rows.size());
}

std::vector<Table> splitTable(const Table& table, size_t parts) {
  if (parts <= 1 || table.rows.size() <= 1) {
    return {table};
  }

  parts = std::min<size_t>(parts, table.rows.size());
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

Table mergeTables(const std::vector<Table>& tables);

Table executePartitionStage(const Table& input, const std::vector<StreamTransform>& stage,
                            const StreamingQueryOptions& options, size_t worker_count) {
  if (stage.empty()) return input;
  if (worker_count <= 1 || input.rows.size() <= 1) {
    Table table = input;
    for (const auto& transform : stage) {
      table = transform(table, options);
    }
    return table;
  }

  auto splits = splitTable(input, worker_count);
  std::vector<Table> outputs(splits.size());
  std::vector<std::thread> threads;
  threads.reserve(splits.size());
  for (size_t i = 0; i < splits.size(); ++i) {
    threads.emplace_back([&, i] {
      Table part = splits[i];
      for (const auto& transform : stage) {
        part = transform(part, options);
      }
      outputs[i] = std::move(part);
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  return mergeTables(outputs);
}

Table mergeTables(const std::vector<Table>& tables) {
  if (tables.empty()) return Table();
  Table out(tables.front().schema, {});
  for (const auto& table : tables) {
    if (table.schema.fields != out.schema.fields) {
      throw std::runtime_error("cannot merge stream partitions with different schema");
    }
    out.rows.insert(out.rows.end(), table.rows.begin(), table.rows.end());
  }
  return out;
}

bool findWindowKeyPosition(const std::vector<std::string>& keys, size_t* pos) {
  if (pos == nullptr) return false;
  for (size_t i = 0; i < keys.size(); ++i) {
    if (keys[i].find("window") != std::string::npos) {
      *pos = i;
      return true;
    }
  }
  return false;
}

std::string windowRegistryKey(const std::string& label) {
  return "__window_registry__:" + label;
}

std::string windowOrderKey(const std::string& label) {
  return "__window_order__:" + label;
}

std::string windowMembersKey(const std::string& label, const std::string& window_value) {
  return "__window_members__:" + label + ":" + window_value;
}

void registerWindowState(StateStore* state_store, const std::string& label,
                         const std::string& window_value, const std::string& state_key) {
  if (state_store == nullptr || label.empty() || window_value.empty() || state_key.empty()) {
    return;
  }
  std::string exists;
  const auto registry = windowRegistryKey(label);
  if (!state_store->getMapField(registry, window_value, &exists)) {
    state_store->putMapField(registry, window_value, "1");
    state_store->appendValueToList(windowOrderKey(label), window_value);
  }
  state_store->appendValueToList(windowMembersKey(label, window_value), state_key);
}

void evictExpiredWindows(StateStore* state_store, const std::string& label,
                         const StreamingQueryOptions& options) {
  if (state_store == nullptr || label.empty() || options.max_retained_windows == 0) {
    return;
  }

  std::vector<std::string> windows;
  if (!state_store->getValueList(windowOrderKey(label), &windows)) {
    return;
  }
  if (windows.size() <= options.max_retained_windows) {
    return;
  }

  const size_t evict_count = windows.size() - options.max_retained_windows;
  for (size_t i = 0; i < evict_count; ++i) {
    const auto& victim = windows[i];
    std::vector<std::string> members;
    if (state_store->getValueList(windowMembersKey(label, victim), &members)) {
      for (const auto& state_key : members) {
        state_store->remove(state_key);
      }
    }
    state_store->remove(windowMembersKey(label, victim));
    state_store->removeMapField(windowRegistryKey(label), victim);
  }

  windows.erase(windows.begin(), windows.begin() + static_cast<std::ptrdiff_t>(evict_count));
  state_store->setValueList(windowOrderKey(label), windows);
}

struct BatchEnvelope {
  Table table;
  std::string offset;
  size_t partition_count = 1;
};

const char* streamingExecutionModeName(StreamingExecutionMode mode) {
  switch (mode) {
    case StreamingExecutionMode::SingleProcess:
      return "single-process";
    case StreamingExecutionMode::LocalWorkers:
      return "local-workers";
    case StreamingExecutionMode::ActorCredit:
      return "actor-credit";
    case StreamingExecutionMode::Auto:
      return "auto";
  }
  return "single-process";
}

LocalActorStreamOptions makeActorOptions(const StreamingQueryOptions& options) {
  LocalActorStreamOptions actor;
  actor.worker_count = std::max<size_t>(2, options.effectiveActorWorkers());
  actor.max_inflight_partitions =
      std::max<size_t>(1, options.actor_max_inflight_partitions > 0
                              ? options.actor_max_inflight_partitions
                              : actor.worker_count);
  actor.shared_memory_transport = options.actor_shared_memory_transport;
  actor.shared_memory_min_payload_bytes = options.actor_shared_memory_min_payload_bytes;
  return actor;
}

LocalExecutionAutoOptions makeActorAutoOptions(const StreamingAutoExecutionOptions& options) {
  LocalExecutionAutoOptions out;
  out.sample_batches = options.sample_batches;
  out.min_rows_per_batch = options.min_rows_per_batch;
  out.min_projected_payload_bytes = options.min_projected_payload_bytes;
  out.min_compute_to_overhead_ratio = options.min_compute_to_overhead_ratio;
  out.min_actor_speedup = options.min_actor_speedup;
  out.strong_actor_speedup = options.strong_actor_speedup;
  return out;
}

bool findActorAcceleratorTransform(const std::vector<StreamTransform>& transforms, size_t* index,
                                   StreamAcceleratorSpec* accelerator) {
  if (index == nullptr || accelerator == nullptr) return false;
  bool found = false;
  for (size_t i = 0; i < transforms.size(); ++i) {
    if (transforms[i].accelerator().kind == StreamAcceleratorKind::None) continue;
    if (found) return false;
    *index = i;
    *accelerator = transforms[i].accelerator();
    found = true;
  }
  if (!found) return false;
  for (size_t i = 0; i < *index; ++i) {
    if (transforms[i].mode() != StreamTransformMode::PartitionLocal) {
      return false;
    }
  }
  return *index + 1 == transforms.size();
}

bool canRunActorWindowKeySum(const Table& table, const StreamAcceleratorSpec& accelerator) {
  return accelerator.kind == StreamAcceleratorKind::WindowKeySum && table.schema.has("window_start") &&
         table.schema.has("key") && table.schema.has("value");
}

void renameColumn(Table* table, const std::string& from, const std::string& to) {
  if (table == nullptr || from == to || !table->schema.has(from)) return;
  const auto idx = table->schema.indexOf(from);
  table->schema.fields[idx] = to;
  table->schema.index.erase(from);
  table->schema.index[to] = idx;
}

Table finalizeActorWindowKeySumOutput(Table aggregated, const StreamAcceleratorSpec& accelerator,
                                      std::shared_ptr<StateStore> state,
                                      const StreamingQueryOptions& options, uint64_t* state_ms) {
  auto started = std::chrono::steady_clock::now();
  renameColumn(&aggregated, "value_sum", accelerator.output_column.empty() ? "value_sum"
                                                                            : accelerator.output_column);
  if (!accelerator.stateful) {
    if (state_ms != nullptr) *state_ms = 0;
    return aggregated;
  }

  auto state_store = state ? state : makeMemoryStateStore();
  const size_t window_idx = aggregated.schema.indexOf("window_start");
  const size_t key_idx = aggregated.schema.indexOf("key");
  const size_t sum_idx =
      aggregated.schema.indexOf(accelerator.output_column.empty() ? "value_sum" : accelerator.output_column);
  for (auto& row : aggregated.rows) {
    const auto state_key = makeStateKey(row, {window_idx, key_idx}, "group_sum:");
    const double delta = row[sum_idx].asDouble();
    row[sum_idx] = Value(state_store->addDouble(state_key, delta));
    registerWindowState(state_store.get(), "group_sum", row[window_idx].toString(), state_key);
  }
  evictExpiredWindows(state_store.get(), "group_sum", options);
  if (state_ms != nullptr) {
    *state_ms = toMillis(std::chrono::steady_clock::now() - started);
  }
  return aggregated;
}

}  // namespace

// ---------- StateStore ----------

double StateStore::getDouble(const std::string& key, double defaultValue) const {
  std::string raw;
  if (!get(key, &raw)) return defaultValue;
  try {
    return std::stod(raw);
  } catch (...) {
    return defaultValue;
  }
}

double StateStore::addDouble(const std::string& key, double delta) {
  double base = getDouble(key, 0.0);
  double next = base + delta;
  std::ostringstream out;
  out << next;
  put(key, out.str());
  return next;
}

bool StateStore::getMapField(const std::string&, const std::string&, std::string*) const {
  return false;
}

void StateStore::putMapField(const std::string&, const std::string&, const std::string&) {}

bool StateStore::removeMapField(const std::string&, const std::string&) {
  return false;
}

bool StateStore::getMapFields(const std::string&, std::vector<std::string>*) const {
  return false;
}

bool StateStore::getValueList(const std::string&, std::vector<std::string>*) const {
  return false;
}

bool StateStore::setValueList(const std::string&, const std::vector<std::string>&) {
  return false;
}

bool StateStore::appendValueToList(const std::string&, const std::string&) {
  return false;
}

bool StateStore::popValueFromList(const std::string&, std::string*) {
  return false;
}

bool StateStore::listKeysByPrefix(const std::string& prefix, std::vector<std::string>* keys) const {
  if (keys == nullptr) return false;
  std::vector<std::string> all;
  if (!listKeys(&all)) {
    keys->clear();
    return false;
  }
  keys->clear();
  for (const auto& key : all) {
    if (key.rfind(prefix, 0) == 0) {
      keys->push_back(key);
    }
  }
  return !keys->empty();
}

bool StateStore::removeKeysByPrefix(const std::string& prefix) {
  std::vector<std::string> keys;
  if (!listKeysByPrefix(prefix, &keys)) {
    return false;
  }
  bool removed = false;
  for (const auto& key : keys) {
    removed = remove(key) || removed;
  }
  return removed;
}

// ---------- MemoryStateStore ----------

bool MemoryStateStore::get(const std::string& key, std::string* out) const {
  auto it = values_.find(key);
  if (it == values_.end()) return false;
  *out = it->second;
  return true;
}

void MemoryStateStore::put(const std::string& key, const std::string& value) {
  values_[key] = value;
}

bool MemoryStateStore::remove(const std::string& key) {
  bool removed = false;
  removed |= values_.erase(key) > 0;
  removed |= maps_.erase(key) > 0;
  removed |= lists_.erase(key) > 0;
  return removed;
}

bool MemoryStateStore::getMapField(const std::string& mapKey, const std::string& field,
                                   std::string* out) const {
  auto it = maps_.find(mapKey);
  if (it == maps_.end()) return false;
  auto fieldIt = it->second.find(field);
  if (fieldIt == it->second.end()) return false;
  *out = fieldIt->second;
  return true;
}

void MemoryStateStore::putMapField(const std::string& mapKey, const std::string& field,
                                   const std::string& value) {
  maps_[mapKey][field] = value;
}

bool MemoryStateStore::removeMapField(const std::string& mapKey, const std::string& field) {
  auto it = maps_.find(mapKey);
  if (it == maps_.end()) return false;
  auto removed = it->second.erase(field) > 0;
  if (it->second.empty()) {
    maps_.erase(it);
  }
  return removed;
}

bool MemoryStateStore::getMapFields(const std::string& mapKey,
                                    std::vector<std::string>* fields) const {
  auto it = maps_.find(mapKey);
  if (it == maps_.end()) return false;
  fields->clear();
  fields->reserve(it->second.size());
  for (const auto& kv : it->second) {
    fields->push_back(kv.first);
  }
  return true;
}

bool MemoryStateStore::getValueList(const std::string& listKey,
                                    std::vector<std::string>* values) const {
  auto it = lists_.find(listKey);
  if (it == lists_.end()) return false;
  *values = it->second;
  return true;
}

bool MemoryStateStore::setValueList(const std::string& listKey,
                                    const std::vector<std::string>& values) {
  lists_[listKey] = values;
  return true;
}

bool MemoryStateStore::appendValueToList(const std::string& listKey, const std::string& value) {
  lists_[listKey].push_back(value);
  return true;
}

bool MemoryStateStore::popValueFromList(const std::string& listKey, std::string* value) {
  auto it = lists_.find(listKey);
  if (it == lists_.end() || it->second.empty()) return false;
  *value = it->second.back();
  it->second.pop_back();
  if (it->second.empty()) {
    lists_.erase(it);
  }
  return true;
}

bool MemoryStateStore::listKeys(std::vector<std::string>* keys) const {
  std::unordered_set<std::string> merged;
  for (const auto& pair : values_) merged.insert(pair.first);
  for (const auto& pair : maps_) merged.insert(pair.first);
  for (const auto& pair : lists_) merged.insert(pair.first);
  keys->assign(merged.begin(), merged.end());
  return !keys->empty();
}

#if DATAFLOW_HAS_ROCKSDB_BACKEND
std::shared_ptr<StateStore> makeRocksDbStateStore(const std::string& dbPath) {
  return RocksDbStateStore::create(dbPath);
}
#else
std::shared_ptr<StateStore> makeRocksDbStateStore(const std::string& dbPath) {
  (void)dbPath;
  throw std::runtime_error("RocksDB backend is not linked in this build");
}
#endif

#if DATAFLOW_HAS_ROCKSDB_BACKEND

namespace {
constexpr char kKvPrefix[] = "kv:";
constexpr char kMapPrefix[] = "map:";
constexpr char kListPrefix[] = "list:";

std::string makeKVKey(const std::string& key) { return std::string(kKvPrefix) + key; }

}  // namespace

std::shared_ptr<RocksDbStateStore> RocksDbStateStore::create(const std::string& dbPath) {
  return std::make_shared<RocksDbStateStore>(dbPath);
}

RocksDbStateStore::RocksDbStateStore(std::string dbPath) : dbPath_(std::move(dbPath)) {
  ensureOpen();
}

RocksDbStateStore::~RocksDbStateStore() {
  close();
}

void RocksDbStateStore::ensureOpen() const {
  if (db_ != nullptr) return;
  rocksdb::DB* db = nullptr;
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status status = rocksdb::DB::Open(options, dbPath_, &db);
  if (!status.ok()) {
    throw std::runtime_error("cannot open rocksdb at " + dbPath_ + ": " + status.ToString());
  }
  db_ = db;
}

bool RocksDbStateStore::get(const std::string& key, std::string* out) const {
  ensureOpen();
  rocksdb::DB* db = static_cast<rocksdb::DB*>(db_);
  auto s = db->Get(rocksdb::ReadOptions(), makeKVKey(key), out);
  return s.ok();
}

void RocksDbStateStore::put(const std::string& key, const std::string& value) {
  ensureOpen();
  rocksdb::DB* db = static_cast<rocksdb::DB*>(db_);
  auto s = db->Put(rocksdb::WriteOptions(), makeKVKey(key), value);
  if (!s.ok()) {
    throw std::runtime_error("rocksdb put failed: " + s.ToString());
  }
}

bool RocksDbStateStore::remove(const std::string& key) {
  ensureOpen();
  rocksdb::DB* db = static_cast<rocksdb::DB*>(db_);
  rocksdb::Status s = db->Delete(rocksdb::WriteOptions(), makeKVKey(key));
  bool removed = s.ok();

  auto mapPrefix = std::string(kMapPrefix) + encodeToken(key) + ":";
  auto* iter = db->NewIterator(rocksdb::ReadOptions());
  for (iter->Seek(mapPrefix); iter->Valid(); iter->Next()) {
    if (!iter->key().starts_with(mapPrefix)) break;
    removed = true;
    db->Delete(rocksdb::WriteOptions(), iter->key());
  }
  delete iter;

  auto listPrefix = std::string(kListPrefix) + encodeToken(key);
  auto* listIt = db->NewIterator(rocksdb::ReadOptions());
  for (listIt->Seek(listPrefix); listIt->Valid(); listIt->Next()) {
    if (!listIt->key().starts_with(listPrefix)) break;
    removed = true;
    db->Delete(rocksdb::WriteOptions(), listIt->key());
  }
  delete listIt;

  return removed;
}

bool RocksDbStateStore::listKeys(std::vector<std::string>* keys) const {
  ensureOpen();
  auto* db = static_cast<rocksdb::DB*>(db_);
  auto* iter = db->NewIterator(rocksdb::ReadOptions());
  std::unordered_set<std::string> merged;

  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    auto raw = iter->key().ToString();
    if (raw.rfind(std::string(kKvPrefix), 0) == 0) {
      merged.insert(raw.substr(sizeof(kKvPrefix) - 1));
      continue;
    }
    if (raw.rfind(std::string(kListPrefix), 0) == 0) {
      std::string encoded = raw.substr(sizeof(kListPrefix) - 1);
      size_t pos = 0;
      std::string listKey;
      if (parseToken(encoded, pos, &listKey) && pos == encoded.size()) {
        merged.insert(listKey);
      }
      continue;
    }
    if (raw.rfind(std::string(kMapPrefix), 0) == 0) {
      std::string encoded = raw.substr(sizeof(kMapPrefix) - 1);
      size_t pos = 0;
      std::string mapKey;
      if (parseToken(encoded, pos, &mapKey)) {
        merged.insert(mapKey);
      }
    }
  }

  delete iter;
  keys->assign(merged.begin(), merged.end());
  return !keys->empty();
}

std::string RocksDbStateStore::encodeList(const std::vector<std::string>& values) const {
  return encodeStringList(values);
}

bool RocksDbStateStore::decodeList(const std::string& raw,
                                   std::vector<std::string>* values) const {
  return decodeStringList(raw, values);
}

std::string RocksDbStateStore::makeMapFieldKey(const std::string& mapKey,
                                               const std::string& field) const {
  return std::string(kMapPrefix) + encodeToken(mapKey) + ":" + encodeToken(field);
}

std::string RocksDbStateStore::makeListKey(const std::string& listKey) const {
  return std::string(kListPrefix) + encodeToken(listKey);
}

bool RocksDbStateStore::getMapField(const std::string& mapKey, const std::string& field,
                                    std::string* out) const {
  ensureOpen();
  auto* db = static_cast<rocksdb::DB*>(db_);
  auto s = db->Get(rocksdb::ReadOptions(), makeMapFieldKey(mapKey, field), out);
  return s.ok();
}

void RocksDbStateStore::putMapField(const std::string& mapKey, const std::string& field,
                                    const std::string& value) {
  ensureOpen();
  auto* db = static_cast<rocksdb::DB*>(db_);
  auto s = db->Put(rocksdb::WriteOptions(), makeMapFieldKey(mapKey, field), value);
  if (!s.ok()) {
    throw std::runtime_error("rocksdb put map field failed: " + s.ToString());
  }
}

bool RocksDbStateStore::removeMapField(const std::string& mapKey, const std::string& field) {
  ensureOpen();
  auto* db = static_cast<rocksdb::DB*>(db_);
  auto s = db->Delete(rocksdb::WriteOptions(), makeMapFieldKey(mapKey, field));
  return s.ok();
}

bool RocksDbStateStore::getMapFields(const std::string& mapKey,
                                     std::vector<std::string>* fields) const {
  ensureOpen();
  auto* db = static_cast<rocksdb::DB*>(db_);
  auto prefix = std::string(kMapPrefix) + encodeToken(mapKey) + ":";
  auto* iter = db->NewIterator(rocksdb::ReadOptions());
  fields->clear();
  for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
    auto key = iter->key().ToString();
    if (!key.starts_with(prefix)) break;
    size_t pos = prefix.size();
    std::string field;
    if (parseToken(key, pos, &field)) {
      fields->push_back(field);
    }
  }
  delete iter;
  return !fields->empty();
}

bool RocksDbStateStore::getValueList(const std::string& listKey,
                                     std::vector<std::string>* values) const {
  std::string raw;
  ensureOpen();
  auto* db = static_cast<rocksdb::DB*>(db_);
  auto s = db->Get(rocksdb::ReadOptions(), makeListKey(listKey), &raw);
  if (!s.ok()) return false;
  return decodeList(raw, values);
}

bool RocksDbStateStore::setValueList(const std::string& listKey,
                                     const std::vector<std::string>& values) {
  ensureOpen();
  auto* db = static_cast<rocksdb::DB*>(db_);
  auto s =
      db->Put(rocksdb::WriteOptions(), makeListKey(listKey), encodeList(values));
  return s.ok();
}

bool RocksDbStateStore::appendValueToList(const std::string& listKey, const std::string& value) {
  ensureOpen();
  auto* db = static_cast<rocksdb::DB*>(db_);
  std::vector<std::string> list;
  std::string raw;
  auto s = db->Get(rocksdb::ReadOptions(), makeListKey(listKey), &raw);
  if (s.ok()) {
    if (!decodeList(raw, &list)) {
      throw std::runtime_error("broken state list encoding: " + listKey);
    }
  }
  list.push_back(value);
  auto putStatus = db->Put(rocksdb::WriteOptions(), makeListKey(listKey), encodeList(list));
  return putStatus.ok();
}

bool RocksDbStateStore::popValueFromList(const std::string& listKey, std::string* value) {
  ensureOpen();
  auto* db = static_cast<rocksdb::DB*>(db_);
  std::vector<std::string> list;
  std::string raw;
  auto s = db->Get(rocksdb::ReadOptions(), makeListKey(listKey), &raw);
  if (!s.ok()) return false;

  if (!decodeList(raw, &list) || list.empty()) return false;
  *value = list.back();
  list.pop_back();

  auto w = db->Put(rocksdb::WriteOptions(), makeListKey(listKey), encodeList(list));
  return w.ok();
}

void RocksDbStateStore::close() {
  if (db_ == nullptr) return;
  auto* db = static_cast<rocksdb::DB*>(db_);
  delete db;
  db_ = nullptr;
}

#endif

std::shared_ptr<StateStore> makeMemoryStateStore() {
  return std::make_shared<MemoryStateStore>();
}

std::shared_ptr<StateStore> makeStateStore(const StateStoreConfig& config) {
  auto backend = config.backend.empty() ? std::string("memory") : config.backend;
  if (backend == "memory") return makeMemoryStateStore();
  if (backend == "rocksdb") {
    std::string path = config.defaultPath;
    auto it = config.options.find("path");
    if (it != config.options.end()) path = it->second;
    return makeRocksDbStateStore(path);
  }
  throw std::invalid_argument("unsupported state store backend: " + backend);
}

std::shared_ptr<StateStore> makeStateStore(
    const std::string& backend,
    const std::unordered_map<std::string, std::string>& options) {
  StateStoreConfig config;
  config.backend = backend;
  config.options = options;
  return makeStateStore(config);
}

// ---------- Source / Sink ----------

MemoryStreamSource::MemoryStreamSource(std::vector<Table> batches) : batches_(std::move(batches)) {}

bool MemoryStreamSource::nextBatch(const StreamPullContext&, Table& batch) {
  if (index_ >= batches_.size()) return false;
  batch = batches_[index_++];
  return true;
}

std::string MemoryStreamSource::currentOffsetToken() const {
  return std::to_string(index_);
}

bool MemoryStreamSource::restoreOffsetToken(const std::string& token) {
  if (token.empty()) return false;
  const auto next = static_cast<size_t>(std::stoull(token));
  index_ = std::min(next, batches_.size());
  return true;
}

DirectoryCsvStreamSource::DirectoryCsvStreamSource(std::string directory, char delimiter)
    : directory_(std::move(directory)), delimiter_(delimiter) {}

bool DirectoryCsvStreamSource::nextBatch(const StreamPullContext&, Table& batch) {
  namespace fs = std::filesystem;
  if (!fs::exists(directory_)) {
    return false;
  }

  std::vector<fs::path> files;
  for (const auto& entry : fs::directory_iterator(directory_)) {
    if (!entry.is_regular_file()) continue;
    files.push_back(entry.path());
  }
  std::sort(files.begin(), files.end());

  for (const auto& path : files) {
    const auto token = path.filename().string();
    if (!resume_after_.empty() && token <= resume_after_) continue;
    batch = load_csv(path.string(), delimiter_);
    last_processed_ = token;
    resume_after_ = token;
    return true;
  }
  return false;
}

std::string DirectoryCsvStreamSource::currentOffsetToken() const {
  return last_processed_;
}

bool DirectoryCsvStreamSource::restoreOffsetToken(const std::string& token) {
  if (token.empty()) return false;
  resume_after_ = token;
  last_processed_ = token;
  return true;
}

std::string DirectoryCsvStreamSource::describe() const {
  return "directory_csv:" + directory_;
}

void ConsoleStreamSink::write(const Table& table) {
  std::cout << "[console:" << name_ << "] batch" << std::endl;
  for (size_t i = 0; i < table.schema.fields.size(); ++i) {
    if (i > 0) std::cout << "\t";
    std::cout << table.schema.fields[i];
  }
  if (!table.schema.fields.empty()) std::cout << "\n";
  for (const auto& row : table.rows) {
    for (size_t i = 0; i < row.size(); ++i) {
      if (i > 0) std::cout << "\t";
      std::cout << row[i].toString();
    }
    std::cout << "\n";
  }
}

FileAppendStreamSink::FileAppendStreamSink(std::string path, char delimiter)
    : path_(std::move(path)), delimiter_(delimiter) {}

void FileAppendStreamSink::write(const Table& table) {
  namespace fs = std::filesystem;
  fs::path path(path_);
  if (path.has_parent_path()) {
    fs::create_directories(path.parent_path());
  }

  std::ofstream out(path_, std::ios::app);
  if (!out) {
    throw std::runtime_error("cannot open stream sink file: " + path_);
  }
  if (!wrote_schema_ && !table.schema.fields.empty()) {
    for (size_t i = 0; i < table.schema.fields.size(); ++i) {
      if (i > 0) out << delimiter_;
      out << table.schema.fields[i];
    }
    out << "\n";
    wrote_schema_ = true;
  }
  for (const auto& row : table.rows) {
    for (size_t i = 0; i < row.size(); ++i) {
      if (i > 0) out << delimiter_;
      out << row[i].toString();
    }
    out << "\n";
  }
}

void FileAppendStreamSink::flush() {}

void MemoryStreamSink::write(const Table& table) {
  last_table_ = table;
  ++batches_written_;
  rows_written_ += table.rowCount();
}

// ---------- Transform ----------

StreamTransform::StreamTransform(Fn f, StreamTransformMode mode, bool touches_state,
                                 std::string label, StreamAcceleratorSpec accelerator)
    : fn_(std::move(f)),
      mode_(mode),
      touches_state_(touches_state),
      label_(std::move(label)),
      accelerator_(std::move(accelerator)) {}

// ---------- StreamingDataFrame ----------

StreamingDataFrame::StreamingDataFrame(std::shared_ptr<StreamSource> source)
    : source_(std::move(source)), state_(nullptr) {}

StreamingDataFrame::StreamingDataFrame(std::shared_ptr<StreamSource> source,
                                       std::vector<StreamTransform> transforms,
                                       std::shared_ptr<StateStore> state)
    : source_(std::move(source)), transforms_(std::move(transforms)), state_(std::move(state)) {}

StreamingDataFrame StreamingDataFrame::select(const std::vector<std::string>& columns) const {
  auto t = transforms_;
  t.emplace_back(
      [columns](const Table& input, const StreamingQueryOptions&) {
        return DataFrame(input).select(columns).toTable();
      },
      StreamTransformMode::PartitionLocal, false, "select");
  return StreamingDataFrame(source_, std::move(t), state_);
}

StreamingDataFrame StreamingDataFrame::filter(const std::string& column, const std::string& op,
                                              const Value& value) const {
  (void)resolvePred(op);
  auto t = transforms_;
  t.emplace_back(
      [column, op, value](const Table& input, const StreamingQueryOptions&) {
        return DataFrame(input).filter(column, op, value).toTable();
      },
      StreamTransformMode::PartitionLocal, false, "filter");
  return StreamingDataFrame(source_, std::move(t), state_);
}

StreamingDataFrame StreamingDataFrame::withColumn(const std::string& name,
                                                  const std::string& sourceColumn) const {
  auto t = transforms_;
  t.emplace_back(
      [name, sourceColumn](const Table& input, const StreamingQueryOptions&) {
        return DataFrame(input).withColumn(name, sourceColumn).toTable();
      },
      StreamTransformMode::PartitionLocal, false, "withColumn");
  return StreamingDataFrame(source_, std::move(t), state_);
}

StreamingDataFrame StreamingDataFrame::drop(const std::string& column) const {
  auto t = transforms_;
  t.emplace_back([column](const Table& input, const StreamingQueryOptions&) {
                   return DataFrame(input).drop(column).toTable();
                 },
                 StreamTransformMode::PartitionLocal, false, "drop");
  return StreamingDataFrame(source_, std::move(t), state_);
}

StreamingDataFrame StreamingDataFrame::limit(size_t n) const {
  auto t = transforms_;
  t.emplace_back([n](const Table& input, const StreamingQueryOptions&) {
                   return DataFrame(input).limit(n).toTable();
                 },
                 StreamTransformMode::GlobalBarrier, false, "limit");
  return StreamingDataFrame(source_, std::move(t), state_);
}

StreamingDataFrame StreamingDataFrame::window(const std::string& timeColumn, uint64_t windowMs,
                                              const std::string& outputColumn) const {
  if (windowMs == 0) {
    throw std::invalid_argument("window size must be positive");
  }
  auto t = transforms_;
  t.emplace_back(
      [timeColumn, windowMs, outputColumn](const Table& input, const StreamingQueryOptions&) {
        Table out = input;
        if (!out.schema.has(timeColumn)) {
          throw std::runtime_error("window column not found: " + timeColumn);
        }
        const auto idx = out.schema.indexOf(timeColumn);
        if (!out.schema.has(outputColumn)) {
          out.schema.fields.push_back(outputColumn);
          out.schema.index[outputColumn] = out.schema.fields.size() - 1;
          for (auto& row : out.rows) {
            const auto ts_ms = parseTimestampMillis(row[idx]);
            const auto bucket = (ts_ms / windowMs) * windowMs;
            row.emplace_back(formatTimestampMillis(bucket));
          }
        }
        return out;
      },
      StreamTransformMode::PartitionLocal, false, "window");
  return StreamingDataFrame(source_, std::move(t), state_);
}

StreamingDataFrame StreamingDataFrame::withStateStore(std::shared_ptr<StateStore> state) const {
  return StreamingDataFrame(source_, transforms_, std::move(state));
}

GroupedStreamingDataFrame StreamingDataFrame::groupBy(const std::vector<std::string>& keys) const {
  return GroupedStreamingDataFrame(source_, transforms_, keys, state_);
}

StreamingQuery StreamingDataFrame::writeStream(std::shared_ptr<StreamSink> sink,
                                               StreamingQueryOptions options) const {
  return StreamingQuery(*this, std::move(sink), std::move(options));
}

StreamingQuery StreamingDataFrame::writeStream(std::shared_ptr<StreamSink> sink,
                                               uint64_t triggerIntervalMs) const {
  StreamingQueryOptions options;
  options.trigger_interval_ms = triggerIntervalMs;
  return writeStream(std::move(sink), std::move(options));
}

StreamingQuery StreamingDataFrame::writeStreamToConsole(StreamingQueryOptions options) const {
  return writeStream(std::make_shared<ConsoleStreamSink>(), std::move(options));
}

StreamingQuery StreamingDataFrame::writeStreamToConsole(uint64_t triggerIntervalMs) const {
  StreamingQueryOptions options;
  options.trigger_interval_ms = triggerIntervalMs;
  return writeStreamToConsole(std::move(options));
}

Table StreamingDataFrame::applyTransforms(const Table& batch, const StreamingQueryOptions& options,
                                          uint64_t* state_ms, size_t* partitions) const {
  Table current = batch;
  std::vector<StreamTransform> pending_partition_local;
  uint64_t state_time = 0;
  size_t max_partitions = partitionCountForBatch(batch, options);

  for (const auto& transform : transforms_) {
    if (transform.mode() == StreamTransformMode::PartitionLocal) {
      pending_partition_local.push_back(transform);
      continue;
    }

    current = executePartitionStage(current, pending_partition_local, options, safeWorkerCount(options));
    pending_partition_local.clear();

    auto started = std::chrono::steady_clock::now();
    current = transform(current, options);
    auto elapsed = std::chrono::steady_clock::now() - started;
    if (transform.touchesState()) {
      state_time += toMillis(elapsed);
    }
    max_partitions = std::max<size_t>(1, max_partitions);
  }

  current = executePartitionStage(current, pending_partition_local, options, safeWorkerCount(options));

  if (state_ms != nullptr) {
    *state_ms = state_time;
  }
  if (partitions != nullptr) {
    *partitions = max_partitions;
  }
  return current;
}

// ---------- GroupedStreamingDataFrame ----------

GroupedStreamingDataFrame::GroupedStreamingDataFrame(std::shared_ptr<StreamSource> source,
                                                     std::vector<StreamTransform> transforms,
                                                     std::vector<std::string> keys,
                                                     std::shared_ptr<StateStore> state)
    : source_(std::move(source)),
      transforms_(std::move(transforms)),
      keys_(std::move(keys)),
      state_(std::move(state)) {}

StreamingDataFrame GroupedStreamingDataFrame::sum(const std::string& valueColumn, bool stateful,
                                                  const std::string& outputColumn) const {
  auto t = transforms_;
  StreamAcceleratorSpec accelerator;
  if (keys_ == std::vector<std::string>{"window_start", "key"} && valueColumn == "value") {
    accelerator.kind = StreamAcceleratorKind::WindowKeySum;
    accelerator.stateful = stateful;
    accelerator.output_column = outputColumn;
  }
  if (!stateful) {
    t.emplace_back(
        [keys = keys_, valueColumn, outputColumn](const Table& input,
                                                  const StreamingQueryOptions&) {
          auto grouped = DataFrame(input).groupBy(keys).sum(valueColumn, outputColumn);
          return grouped.toTable();
        },
        StreamTransformMode::GlobalBarrier, false, "group_sum", accelerator);
    return StreamingDataFrame(source_, std::move(t), nullptr);
  }

  auto state = state_ ? state_ : makeMemoryStateStore();
  auto stateStore = state;
  t.emplace_back(
      [keys = keys_, valueColumn, outputColumn, stateStore](const Table& input,
                                                            const StreamingQueryOptions& options) {
        auto grouped = DataFrame(input).groupBy(keys).sum(valueColumn, outputColumn);
        Table out = grouped.toTable();

        const size_t keyCols = keys.size();
        const size_t sumIndex = out.schema.indexOf(outputColumn);
        size_t window_pos = 0;
        const bool has_window = findWindowKeyPosition(keys, &window_pos);
        for (auto& row : out.rows) {
          std::vector<size_t> idx;
          idx.reserve(keyCols);
          for (size_t k = 0; k < keyCols && k < row.size(); ++k) idx.push_back(k);
          const auto stateKey = makeStateKey(row, idx, "group_sum:");
          const double delta = sumIndex < row.size() ? row[sumIndex].asDouble() : 0.0;
          row[sumIndex] = Value(stateStore->addDouble(stateKey, delta));
          if (has_window && window_pos < row.size()) {
            registerWindowState(stateStore.get(), "group_sum", row[window_pos].toString(), stateKey);
          }
        }
        evictExpiredWindows(stateStore.get(), "group_sum", options);
        return out;
      },
      StreamTransformMode::GlobalBarrier, true, "stateful_group_sum", accelerator);

  return StreamingDataFrame(source_, std::move(t), state);
}

StreamingDataFrame GroupedStreamingDataFrame::count(bool stateful,
                                                    const std::string& outputColumn) const {
  auto t = transforms_;
  if (!stateful) {
    t.emplace_back(
        [keys = keys_, outputColumn](const Table& input, const StreamingQueryOptions&) {
          std::vector<size_t> key_indices;
          key_indices.reserve(keys.size());
          for (const auto& key : keys) {
            key_indices.push_back(input.schema.indexOf(key));
          }
          AggregateSpec spec{AggregateFunction::Count, 0, outputColumn};
          return DataFrame(input).aggregate(key_indices, {spec}).toTable();
        },
        StreamTransformMode::GlobalBarrier, false, "group_count");
    return StreamingDataFrame(source_, std::move(t), nullptr);
  }

  auto state = state_ ? state_ : makeMemoryStateStore();
  auto stateStore = state;
  t.emplace_back(
      [keys = keys_, outputColumn, stateStore](const Table& input,
                                               const StreamingQueryOptions& options) {
        std::vector<size_t> key_indices;
        key_indices.reserve(keys.size());
        for (const auto& key : keys) {
          key_indices.push_back(input.schema.indexOf(key));
        }
        AggregateSpec spec{AggregateFunction::Count, 0, outputColumn};
        Table out = DataFrame(input).aggregate(key_indices, {spec}).toTable();
        const size_t keyCols = keys.size();
        const size_t countIndex = out.schema.indexOf(outputColumn);
        size_t window_pos = 0;
        const bool has_window = findWindowKeyPosition(keys, &window_pos);
        for (auto& row : out.rows) {
          std::vector<size_t> idx;
          idx.reserve(keyCols);
          for (size_t k = 0; k < keyCols && k < row.size(); ++k) idx.push_back(k);
          const auto stateKey = makeStateKey(row, idx, "group_count:");
          const double delta =
              countIndex < row.size() ? static_cast<double>(row[countIndex].asInt64()) : 0.0;
          row[countIndex] = Value(static_cast<int64_t>(stateStore->addDouble(stateKey, delta)));
          if (has_window && window_pos < row.size()) {
            registerWindowState(stateStore.get(), "group_count", row[window_pos].toString(), stateKey);
          }
        }
        evictExpiredWindows(stateStore.get(), "group_count", options);
        return out;
      },
      StreamTransformMode::GlobalBarrier, true, "stateful_group_count");
  return StreamingDataFrame(source_, std::move(t), state);
}

// ---------- Query ----------

StreamingQuery::StreamingQuery(StreamingDataFrame root, std::shared_ptr<StreamSink> sink,
                               StreamingQueryOptions options)
    : root_(std::move(root)), sink_(std::move(sink)), options_(std::move(options)) {
  progress_.query_id = nextStreamingQueryId();
}

StreamingQuery::StreamingQuery(StreamingQuery&& other) noexcept
    : root_(std::move(other.root_)),
      sink_(std::move(other.sink_)),
      options_(std::move(other.options_)),
      progress_(std::move(other.progress_)),
      running_(other.running_.load()),
      started_(other.started_),
      execution_decided_(other.execution_decided_),
      resolved_execution_mode_(other.resolved_execution_mode_),
      execution_reason_(std::move(other.execution_reason_)) {
  other.running_ = false;
  other.started_ = false;
  other.execution_decided_ = false;
  other.resolved_execution_mode_ = StreamingExecutionMode::SingleProcess;
}

StreamingQuery& StreamingQuery::operator=(StreamingQuery&& other) noexcept {
  if (this == &other) {
    return *this;
  }
  root_ = std::move(other.root_);
  sink_ = std::move(other.sink_);
  options_ = std::move(other.options_);
  {
    std::lock_guard<std::mutex> lock(progress_mu_);
    progress_ = std::move(other.progress_);
  }
  running_ = other.running_.load();
  started_ = other.started_;
  execution_decided_ = other.execution_decided_;
  resolved_execution_mode_ = other.resolved_execution_mode_;
  execution_reason_ = std::move(other.execution_reason_);

  other.running_ = false;
  other.started_ = false;
  other.execution_decided_ = false;
  other.resolved_execution_mode_ = StreamingExecutionMode::SingleProcess;
  return *this;
}

StreamingQuery& StreamingQuery::trigger(uint64_t triggerIntervalMs) {
  options_.trigger_interval_ms = triggerIntervalMs;
  return *this;
}

StreamingQuery& StreamingQuery::start() {
  started_ = true;
  running_ = true;
  updateProgress([&](StreamingQueryProgress& progress) {
    progress.status = "running";
    progress.execution_mode = streamingExecutionModeName(options_.execution_mode);
    progress.execution_reason.clear();
  });
  return *this;
}

size_t StreamingQuery::awaitTermination(size_t maxBatches) {
  if (!started_) {
    start();
  }
  loadCheckpoint();

  std::deque<BatchEnvelope> queue;
  std::mutex queue_mu;
  std::condition_variable queue_cv;
  bool source_done = false;
  size_t actor_transform_index = 0;
  StreamAcceleratorSpec actor_accelerator;
  const bool actor_pipeline_supported =
      findActorAcceleratorTransform(root_.transforms_, &actor_transform_index, &actor_accelerator);
  std::vector<StreamTransform> actor_prefix_transforms;
  if (actor_pipeline_supported) {
    actor_prefix_transforms.assign(root_.transforms_.begin(),
                                   root_.transforms_.begin() +
                                       static_cast<std::ptrdiff_t>(actor_transform_index));
  }

  auto queuedPartitions = [&]() -> size_t {
    size_t total = 0;
    for (const auto& batch : queue) total += batch.partition_count;
    return total;
  };

  auto producer = std::thread([&] {
    while (running_) {
      {
        std::unique_lock<std::mutex> lock(queue_mu);
        const auto queue_partitions = queuedPartitions();
        const bool over_batches =
            queue.size() >= std::max<size_t>(1, options_.max_inflight_batches);
        const bool over_partitions =
            queue_partitions >= std::max<size_t>(1, options_.max_queued_partitions);
        if (over_batches || over_partitions) {
          updateProgress([&](StreamingQueryProgress& progress) {
            progress.blocked_count += 1;
            progress.backpressure_active = true;
            progress.status = "backpressure";
          });
          queue_cv.wait(lock, [&] {
            const auto current_backlog = queue.size();
            const auto current_partitions = queuedPartitions();
            return !running_ ||
                   (current_backlog <= options_.backpressure_low_watermark &&
                    current_partitions < std::max<size_t>(1, options_.max_queued_partitions));
          });
          if (!running_) break;
        }
      }

      Table batch;
      auto current = progress();
      StreamPullContext pull;
      pull.query_id = current.query_id;
      pull.backlog_batches = current.inflight_batches;
      pull.inflight_batches = current.inflight_batches;
      pull.queued_partitions = current.inflight_partitions;
      pull.max_inflight_batches = options_.max_inflight_batches;
      pull.backpressure_active = current.backpressure_active;

      const bool has_batch = root_.source_->nextBatch(pull, batch);
      if (!has_batch) {
        if (root_.source_->bounded()) {
          std::lock_guard<std::mutex> lock(queue_mu);
          source_done = true;
          queue_cv.notify_all();
          break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(options_.idle_wait_ms));
        continue;
      }

      BatchEnvelope envelope;
      envelope.table = std::move(batch);
      envelope.offset = root_.source_->currentOffsetToken();
      envelope.partition_count = partitionCountForBatch(envelope.table, options_);

      {
        std::lock_guard<std::mutex> lock(queue_mu);
        queue.push_back(std::move(envelope));
        const auto backlog = queue.size();
        const auto partitions = queuedPartitions();
        updateProgress([&](StreamingQueryProgress& progress) {
          progress.batches_pulled += 1;
          progress.inflight_batches = backlog;
          progress.inflight_partitions = partitions;
          progress.max_backlog_batches = std::max(progress.max_backlog_batches, backlog);
          progress.backpressure_active =
              backlog >= std::max<size_t>(1, options_.backpressure_high_watermark);
          if (progress.backpressure_active) {
            progress.status = "backpressure";
          } else {
            progress.status = "running";
          }
        });
      }
      queue_cv.notify_all();

      if (options_.trigger_interval_ms > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(options_.trigger_interval_ms));
      }
    }
  });

  size_t processed_this_run = 0;
  while (running_) {
    BatchEnvelope envelope;
    {
      std::unique_lock<std::mutex> lock(queue_mu);
      queue_cv.wait(lock, [&] { return !running_ || !queue.empty() || source_done; });
      if (queue.empty()) {
        if (source_done) break;
        continue;
      }
      envelope = std::move(queue.front());
      queue.pop_front();
      const auto backlog = queue.size();
      const auto partitions = queuedPartitions();
      updateProgress([&](StreamingQueryProgress& progress) {
        progress.inflight_batches = backlog;
        progress.inflight_partitions = partitions;
        if (backlog < options_.backpressure_low_watermark) {
          progress.backpressure_active = false;
          progress.status = "running";
        }
      });
    }
    queue_cv.notify_all();

    ai::PluginContext ctx;
    ctx.trace_id = progress().query_id;
    ctx.labels["api"] = "StreamingQuery::awaitTermination";
    ai::PluginPayload payload;
    payload.summary = "batch start";
    payload.row_count = envelope.table.rows.size();

    auto batch_hook =
        ai::PluginManager::instance().runHook(ai::HookPoint::kStreamingBatchStart, ctx, &payload);
    if (!batch_hook.continue_execution()) {
      running_ = false;
      break;
    }

    const auto batch_started = std::chrono::steady_clock::now();
    uint64_t state_ms = 0;
    size_t partitions_used = 1;
    Table out;
    bool used_actor_runtime = false;
    const bool wants_actor_mode =
        options_.execution_mode == StreamingExecutionMode::ActorCredit ||
        options_.execution_mode == StreamingExecutionMode::Auto;
    if (wants_actor_mode && actor_pipeline_supported) {
      Table actor_input =
          executePartitionStage(envelope.table, actor_prefix_transforms, options_, 1);
      if (canRunActorWindowKeySum(actor_input, actor_accelerator)) {
        const auto actor_options = makeActorOptions(options_);
        if (options_.execution_mode == StreamingExecutionMode::ActorCredit) {
          auto actor_result =
              runLocalActorStreamWindowKeySum(std::vector<Table>{actor_input}, actor_options);
          out = finalizeActorWindowKeySumOutput(std::move(actor_result.final_table), actor_accelerator,
                                               root_.state_, options_, &state_ms);
          partitions_used = std::max<size_t>(1, actor_result.processed_partitions);
          used_actor_runtime = true;
          execution_decided_ = true;
          resolved_execution_mode_ = StreamingExecutionMode::ActorCredit;
          execution_reason_ = "configured actor-credit execution";
        } else if (!execution_decided_) {
          LocalExecutionDecision decision;
          auto actor_result = runAutoLocalActorStreamWindowKeySum(std::vector<Table>{actor_input},
                                                                  actor_options,
                                                                  makeActorAutoOptions(options_.actor_auto_options),
                                                                  &decision);
          resolved_execution_mode_ =
              decision.chosen_mode == LocalExecutionMode::ActorCredit
                  ? StreamingExecutionMode::ActorCredit
                  : StreamingExecutionMode::SingleProcess;
          execution_reason_ = decision.reason;
          execution_decided_ = true;
          updateProgress([&](StreamingQueryProgress& progress) {
            progress.execution_mode = streamingExecutionModeName(resolved_execution_mode_);
            progress.execution_reason = execution_reason_;
          });
          if (resolved_execution_mode_ == StreamingExecutionMode::ActorCredit) {
            out = finalizeActorWindowKeySumOutput(std::move(actor_result.final_table),
                                                  actor_accelerator, root_.state_, options_,
                                                  &state_ms);
            partitions_used = std::max<size_t>(1, actor_result.processed_partitions);
            used_actor_runtime = true;
          }
        } else if (resolved_execution_mode_ == StreamingExecutionMode::ActorCredit) {
          auto actor_result =
              runLocalActorStreamWindowKeySum(std::vector<Table>{actor_input}, actor_options);
          out = finalizeActorWindowKeySumOutput(std::move(actor_result.final_table), actor_accelerator,
                                               root_.state_, options_, &state_ms);
          partitions_used = std::max<size_t>(1, actor_result.processed_partitions);
          used_actor_runtime = true;
        }
      } else if (options_.execution_mode == StreamingExecutionMode::Auto && !execution_decided_) {
        execution_decided_ = true;
        resolved_execution_mode_ = StreamingExecutionMode::SingleProcess;
        execution_reason_ = "actor path requires window_start/key/value schema after partition-local transforms";
        updateProgress([&](StreamingQueryProgress& progress) {
          progress.execution_mode = streamingExecutionModeName(resolved_execution_mode_);
          progress.execution_reason = execution_reason_;
        });
      }
    } else if (wants_actor_mode && !actor_pipeline_supported && !execution_decided_) {
      execution_decided_ = true;
      resolved_execution_mode_ = StreamingExecutionMode::SingleProcess;
      execution_reason_ = "query plan is not eligible for actor acceleration";
      updateProgress([&](StreamingQueryProgress& progress) {
        progress.execution_mode = streamingExecutionModeName(resolved_execution_mode_);
        progress.execution_reason = execution_reason_;
      });
    }

    if (!used_actor_runtime) {
      out = root_.applyTransforms(envelope.table, options_, &state_ms, &partitions_used);
    }
    const auto before_sink = std::chrono::steady_clock::now();
    if (sink_) {
      sink_->write(out);
      sink_->flush();
    }
    const auto batch_finished = std::chrono::steady_clock::now();

    const auto sink_ms = toMillis(batch_finished - before_sink);
    const auto batch_ms = toMillis(batch_finished - batch_started);

    updateProgress([&](StreamingQueryProgress& progress) {
      progress.batches_processed += 1;
      progress.last_batch_latency_ms = batch_ms;
      progress.last_sink_latency_ms = sink_ms;
      progress.last_state_latency_ms = state_ms;
      progress.last_source_offset = envelope.offset;
      progress.inflight_partitions = partitions_used;
      progress.status = "running";
    });
    persistCheckpoint(progress());

    payload.summary = "batch end";
    payload.row_count = out.rows.size();
    payload.attributes["batch_out_rows"] = std::to_string(out.rows.size());
    payload.attributes["batch_latency_ms"] = std::to_string(batch_ms);
    payload.attributes["sink_latency_ms"] = std::to_string(sink_ms);
    payload.attributes["state_latency_ms"] = std::to_string(state_ms);
    auto end_hook =
        ai::PluginManager::instance().runHook(ai::HookPoint::kStreamingBatchEnd, ctx, &payload);
    if (!end_hook.continue_execution()) {
      running_ = false;
      break;
    }

    ++processed_this_run;
    if (maxBatches != 0 && processed_this_run >= maxBatches) {
      running_ = false;
      break;
    }
  }

  running_ = false;
  queue_cv.notify_all();
  if (producer.joinable()) {
    producer.join();
  }

  if (root_.state_) {
    root_.state_->close();
  }
  updateProgress([](StreamingQueryProgress& progress) { progress.status = "stopped"; });
  return processed_this_run;
}

void StreamingQuery::stop() {
  running_ = false;
  updateProgress([](StreamingQueryProgress& progress) { progress.status = "stopped"; });
  if (root_.state_) {
    root_.state_->close();
  }
}

StreamingQueryProgress StreamingQuery::progress() const {
  std::lock_guard<std::mutex> lock(progress_mu_);
  return progress_;
}

std::string StreamingQuery::snapshotJson() const {
  const auto current = progress();
  std::ostringstream out;
  out << "{"
      << "\"query_id\":\"" << jsonEscape(current.query_id) << "\","
      << "\"status\":\"" << jsonEscape(current.status) << "\","
      << "\"execution_mode\":\"" << jsonEscape(current.execution_mode) << "\","
      << "\"execution_reason\":\"" << jsonEscape(current.execution_reason) << "\","
      << "\"batches_pulled\":" << current.batches_pulled << ","
      << "\"batches_processed\":" << current.batches_processed << ","
      << "\"blocked_count\":" << current.blocked_count << ","
      << "\"max_backlog_batches\":" << current.max_backlog_batches << ","
      << "\"inflight_batches\":" << current.inflight_batches << ","
      << "\"inflight_partitions\":" << current.inflight_partitions << ","
      << "\"last_batch_latency_ms\":" << current.last_batch_latency_ms << ","
      << "\"last_sink_latency_ms\":" << current.last_sink_latency_ms << ","
      << "\"last_state_latency_ms\":" << current.last_state_latency_ms << ","
      << "\"last_source_offset\":\"" << jsonEscape(current.last_source_offset) << "\","
      << "\"backpressure_active\":" << (current.backpressure_active ? "true" : "false")
      << "}";
  return out.str();
}

void StreamingQuery::updateProgress(
    const std::function<void(StreamingQueryProgress&)>& update) {
  std::lock_guard<std::mutex> lock(progress_mu_);
  update(progress_);
}

bool StreamingQuery::loadCheckpoint() {
  if (options_.checkpoint_path.empty()) {
    return false;
  }
  std::ifstream in(options_.checkpoint_path);
  if (!in) {
    return false;
  }

  StreamingQueryProgress restored = progress();
  std::string line;
  while (std::getline(in, line)) {
    const auto eq = line.find('=');
    if (eq == std::string::npos) continue;
    const auto key = line.substr(0, eq);
    const auto value = line.substr(eq + 1);
    if (key == "batches_pulled") restored.batches_pulled = std::stoull(value);
    if (key == "batches_processed") restored.batches_processed = std::stoull(value);
    if (key == "blocked_count") restored.blocked_count = std::stoull(value);
    if (key == "max_backlog_batches") restored.max_backlog_batches = std::stoull(value);
    if (key == "last_source_offset") restored.last_source_offset = value;
  }

  if (!restored.last_source_offset.empty()) {
    root_.source_->restoreOffsetToken(restored.last_source_offset);
  }
  restored.status = "restored";
  updateProgress([&](StreamingQueryProgress& progress) { progress = restored; });
  return true;
}

void StreamingQuery::persistCheckpoint(const StreamingQueryProgress& progress) const {
  if (options_.checkpoint_path.empty()) {
    return;
  }
  namespace fs = std::filesystem;
  fs::path path(options_.checkpoint_path);
  if (path.has_parent_path()) {
    fs::create_directories(path.parent_path());
  }
  std::ofstream out(options_.checkpoint_path, std::ios::trunc);
  if (!out) {
    throw std::runtime_error("cannot write checkpoint: " + options_.checkpoint_path);
  }
  out << "batches_pulled=" << progress.batches_pulled << "\n";
  out << "batches_processed=" << progress.batches_processed << "\n";
  out << "blocked_count=" << progress.blocked_count << "\n";
  out << "max_backlog_batches=" << progress.max_backlog_batches << "\n";
  out << "last_source_offset=" << progress.last_source_offset << "\n";
}

}  // namespace dataflow
