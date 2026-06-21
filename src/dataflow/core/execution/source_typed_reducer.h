#pragma once

#include <array>
#include <cstddef>
#include <string>
#include <unordered_map>
#include <vector>

#include "src/dataflow/core/execution/table.h"
#include "src/dataflow/core/logical/planner/plan.h"

namespace dataflow {

class SourceTypedReducer {
 public:
  SourceTypedReducer(const Schema& schema, const SourcePushdownSpec& pushdown);

  bool supported() const { return supported_; }
  void add(const std::vector<Value>& keys, const Value* aggregate_value);
  bool hasEncodedKey(const std::string& encoded_key) const;
  void addEncodedExisting(const std::string& encoded_key, const Value* aggregate_value);
  void addEncodedNew(std::string encoded_key, const std::vector<Value>& keys,
                     const Value* aggregate_value);
  Table finalize(bool materialize_rows = false) const;

 private:
  struct Key {
    std::array<Value, 3> values;
    std::size_t size = 0;
  };

  struct KeyHash {
    std::size_t operator()(const Key& key) const;
  };

  struct KeyEq {
    bool operator()(const Key& lhs, const Key& rhs) const;
  };

  struct State {
    std::array<Value, 3> keys;
    std::size_t key_size = 0;
    double sum = 0.0;
    std::size_t count = 0;
  };

  const Schema* schema_ = nullptr;
  const SourcePushdownSpec* pushdown_ = nullptr;
  bool supported_ = false;
  AggregateFunction function_ = AggregateFunction::Count;
  std::unordered_map<Key, std::size_t, KeyHash, KeyEq> key_groups_;
  std::unordered_map<std::string, std::size_t> encoded_groups_;
  std::vector<State> states_;

  void update(State* state, const Value* aggregate_value);
};

bool sourceTypedReducerSupports(const SourcePushdownSpec& pushdown);

}  // namespace dataflow
