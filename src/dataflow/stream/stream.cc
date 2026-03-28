#include "src/dataflow/stream/stream.h"

#include <algorithm>
#include <chrono>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_set>

#include "src/dataflow/api/dataframe.h"

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

// tiny length-prefix codec to avoid delimiter conflict for map/list keys and values
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

bool StateStore::appendValueToList(const std::string&, const std::string&) {
  return false;
}

bool StateStore::popValueFromList(const std::string&, std::string*) {
  return false;
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
  fields->reserve(fields->size() + it->second.size());
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
  for (const auto& pair : values_) {
    merged.insert(pair.first);
  }
  for (const auto& pair : maps_) {
    merged.insert(pair.first);
  }
  for (const auto& pair : lists_) {
    merged.insert(pair.first);
  }
  keys->assign(merged.begin(), merged.end());
  return !keys->empty();
}

#ifdef DATAFLOW_USE_ROCKSDB
#include <rocksdb/db.h>

std::shared_ptr<StateStore> makeRocksDbStateStore(const std::string& dbPath) {
  return RocksDbStateStore::create(dbPath);
}

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
  if (db_ != nullptr) {
    return;
  }
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
  rocksdb::Status s = db->Put(rocksdb::WriteOptions(), makeKVKey(key), value);
  if (!s.ok()) {
    throw std::runtime_error("rocksdb put failed: " + s.ToString());
  }
}

bool RocksDbStateStore::remove(const std::string& key) {
  ensureOpen();
  rocksdb::DB* db = static_cast<rocksdb::DB*>(db_);
  rocksdb::Status s = db->Delete(rocksdb::WriteOptions(), makeKVKey(key));
  bool removed = s.ok();

  // Remove map/list entries under same top-level key as best-effort cleanup.
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

  if (!removed) return false;
  return true;
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
  auto key = makeMapFieldKey(mapKey, field);
  auto s = db->Get(rocksdb::ReadOptions(), key, out);
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
  rocksdb::Status s = db->Delete(rocksdb::WriteOptions(), makeMapFieldKey(mapKey, field));
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
    if (!key.starts_with(prefix)) {
      break;
    }
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
  rocksdb::Status s = db->Get(rocksdb::ReadOptions(), makeListKey(listKey), &raw);
  if (!s.ok()) {
    return false;
  }
  return decodeList(raw, values);
}

bool RocksDbStateStore::appendValueToList(const std::string& listKey, const std::string& value) {
  ensureOpen();
  auto* db = static_cast<rocksdb::DB*>(db_);
  std::vector<std::string> list;
  std::string raw;
  rocksdb::Status s = db->Get(rocksdb::ReadOptions(), makeListKey(listKey), &raw);
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
  rocksdb::Status s = db->Get(rocksdb::ReadOptions(), makeListKey(listKey), &raw);
  if (!s.ok()) return false;

  if (!decodeList(raw, &list) || list.empty()) return false;
  *value = list.back();
  list.pop_back();

  rocksdb::Status w =
      db->Put(rocksdb::WriteOptions(), makeListKey(listKey), encodeList(list));
  if (!w.ok()) {
    return false;
  }
  return true;
}

void RocksDbStateStore::close() {
  if (db_ != nullptr) {
    auto* db = static_cast<rocksdb::DB*>(db_);
    delete db;
    db_ = nullptr;
  }
}

#endif

std::shared_ptr<StateStore> makeMemoryStateStore() {
  return std::make_shared<MemoryStateStore>();
}

// ---------- Source / Sink ----------

MemoryStreamSource::MemoryStreamSource(std::vector<Table> batches) : batches_(std::move(batches)) {}

bool MemoryStreamSource::nextBatch(Table& batch) {
  if (index_ >= batches_.size()) return false;
  batch = batches_[index_++];
  return true;
}

void ConsoleStreamSink::write(const Table& table) {
  std::cout << "[console:" << name_ << "] batch" << std::endl;
  for (const auto& row : table.rows) {
    for (size_t i = 0; i < row.size(); ++i) {
      if (i > 0) std::cout << "\t";
      std::cout << row[i].toString();
    }
    std::cout << "\n";
  }
}

// ---------- StreamingDataFrame ----------

StreamingDataFrame::StreamingDataFrame(std::shared_ptr<StreamSource> source)
    : source_(std::move(source)), state_(nullptr) {}

StreamingDataFrame::StreamingDataFrame(std::shared_ptr<StreamSource> source,
                                     std::vector<StreamTransform> transforms,
                                     std::shared_ptr<StateStore> state)
    : source_(std::move(source)), transforms_(std::move(transforms)), state_(std::move(state)) {}

Table StreamingDataFrame::applyTransforms(const Table& batch) const {
  Table table = batch;
  for (const auto& tr : transforms_) {
    table = tr(table);
  }
  return table;
}

StreamingDataFrame StreamingDataFrame::select(const std::vector<std::string>& columns) const {
  auto t = transforms_;
  t.emplace_back([columns](const Table& input) {
    auto df = DataFrame(input).select(columns);
    return df.toTable();
  });
  return StreamingDataFrame(source_, std::move(t), state_);
}

StreamingDataFrame StreamingDataFrame::filter(const std::string& column, const std::string& op,
                                            const Value& value) const {
  (void)resolvePred(op);
  auto t = transforms_;
  auto columnCopy = column;
  auto opCopy = op;
  auto valueCopy = value;
  t.emplace_back([columnCopy, opCopy, valueCopy](const Table& input) {
    auto df = DataFrame(input).filter(columnCopy, opCopy, valueCopy);
    return df.toTable();
  });
  return StreamingDataFrame(source_, std::move(t), state_);
}

StreamingDataFrame StreamingDataFrame::withColumn(const std::string& name,
                                                 const std::string& sourceColumn) const {
  auto t = transforms_;
  t.emplace_back([name, sourceColumn](const Table& input) {
    auto df = DataFrame(input).withColumn(name, sourceColumn);
    return df.toTable();
  });
  return StreamingDataFrame(source_, std::move(t), state_);
}

StreamingDataFrame StreamingDataFrame::drop(const std::string& column) const {
  auto t = transforms_;
  t.emplace_back([column](const Table& input) {
    auto df = DataFrame(input).drop(column);
    return df.toTable();
  });
  return StreamingDataFrame(source_, std::move(t), state_);
}

StreamingDataFrame StreamingDataFrame::limit(size_t n) const {
  auto t = transforms_;
  t.emplace_back([n](const Table& input) {
    auto df = DataFrame(input).limit(n);
    return df.toTable();
  });
  return StreamingDataFrame(source_, std::move(t), state_);
}

StreamingDataFrame StreamingDataFrame::withStateStore(std::shared_ptr<StateStore> state) const {
  return StreamingDataFrame(source_, transforms_, std::move(state));
}

GroupedStreamingDataFrame StreamingDataFrame::groupBy(const std::vector<std::string>& keys) const {
  return GroupedStreamingDataFrame(source_, transforms_, keys, state_);
}

StreamingQuery StreamingDataFrame::writeStream(std::shared_ptr<StreamSink> sink,
                                             uint64_t triggerIntervalMs) const {
  return StreamingQuery(*this, std::move(sink), triggerIntervalMs);
}

StreamingQuery StreamingDataFrame::writeStreamToConsole(uint64_t triggerIntervalMs) const {
  return writeStream(std::make_shared<ConsoleStreamSink>(), triggerIntervalMs);
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

StreamingDataFrame GroupedStreamingDataFrame::sum(const std::string& valueColumn, bool stateful) const {
  auto t = transforms_;
  auto keys = keys_;
  auto valueColumnCopy = valueColumn;

  if (!stateful) {
    t.emplace_back([keys, valueColumnCopy](const Table& input) {
      auto transformed = DataFrame(input);
      auto grouped = transformed.groupBy(keys).sum(valueColumnCopy);
      return grouped.toTable();
    });
    return StreamingDataFrame(source_, std::move(t), nullptr);
  }

  auto state = state_ ? state_ : makeMemoryStateStore();
  auto stateStore = state;
  t.emplace_back([keys, valueColumnCopy, stateStore](const Table& input) {
    auto transformed = DataFrame(input);
    auto grouped = transformed.groupBy(keys).sum(valueColumnCopy);
    Table out = grouped.toTable();

    size_t keyCols = keys.size();
    size_t sumIndex = out.schema.fields.empty() ? 0 : out.schema.fields.size() - 1;
    size_t rowCount = out.rows.size();

    for (size_t i = 0; i < rowCount; ++i) {
      Row& row = out.rows[i];
      std::vector<size_t> idx;
      idx.reserve(keyCols);
      for (size_t k = 0; k < keyCols && k < row.size(); ++k) {
        idx.push_back(k);
      }
      auto stateKey = makeStateKey(row, idx, "group:");
      double currentSum = (sumIndex < row.size()) ? row[sumIndex].asDouble() : 0.0;
      double nextSum = stateStore->addDouble(stateKey, currentSum);
      if (sumIndex < row.size()) {
        row[sumIndex] = Value(nextSum);
      }
    }
    return out;
  });

  return StreamingDataFrame(source_, std::move(t), state);
}

// ---------- Query ----------

StreamingQuery::StreamingQuery(StreamingDataFrame root, std::shared_ptr<StreamSink> sink,
                             uint64_t triggerIntervalMs)
    : root_(std::move(root)), sink_(std::move(sink)), intervalMs_(triggerIntervalMs) {}

size_t StreamingQuery::awaitTermination(size_t maxBatches) {
  running_ = true;
  size_t processed = 0;

  while (running_) {
    Table in;
    if (!root_.source_->nextBatch(in)) {
      break;
    }

    Table out = root_.applyTransforms(in);
    if (sink_) {
      sink_->write(out);
    }
    ++processed;

    if (maxBatches != 0 && processed >= maxBatches) {
      break;
    }
    if (intervalMs_ > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs_));
    }
  }

  if (root_.state_) {
    root_.state_->close();
  }

  running_ = false;
  return processed;
}

void StreamingQuery::stop() {
  running_ = false;
  if (root_.state_) {
    root_.state_->close();
  }
}

}  // namespace dataflow
