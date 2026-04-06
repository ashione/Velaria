#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "src/dataflow/core/execution/stream/binary_row_batch.h"
#include "src/dataflow/core/execution/table.h"
#include "src/dataflow/core/logical/planner/plan.h"

namespace dataflow {

struct AggregatePartialBatch {
  std::size_t row_count = 0;
  std::vector<std::string> key_names;
  std::vector<BinaryKeyColumn> key_columns;
  std::vector<std::string> state_names;
  std::vector<BinaryDoubleColumn> sum_state_columns;
};

struct AggregateStringKeyTuple {
  std::vector<uint32_t> ids;

  bool operator==(const AggregateStringKeyTuple& other) const {
    return ids == other.ids;
  }
};

struct AggregateStringKeyTupleHash {
  std::size_t operator()(const AggregateStringKeyTuple& value) const;
};

struct AggregateStringKeyState {
  std::vector<std::unordered_map<std::string, uint32_t>> index_by_value;
  std::vector<std::vector<std::string>> values_by_key;
  std::unordered_map<AggregateStringKeyTuple, std::size_t, AggregateStringKeyTupleHash> index_by_key;
  std::vector<AggregateStringKeyTuple> keys;
  std::vector<double> sums;
};

struct AggregateFixedKeyTuple {
  std::vector<int64_t> values;
  std::vector<uint8_t> is_null;

  bool operator==(const AggregateFixedKeyTuple& other) const {
    return values == other.values && is_null == other.is_null;
  }
};

struct AggregateFixedKeyTupleHash {
  std::size_t operator()(const AggregateFixedKeyTuple& value) const;
};

struct AggregateFixedKeyState {
  std::size_t key_count = 0;
  std::unordered_map<AggregateFixedKeyTuple, std::size_t, AggregateFixedKeyTupleHash> index_by_key;
  std::vector<AggregateFixedKeyTuple> keys;
  std::vector<double> sums;
};

struct TwoKeyStringStateKey {
  uint32_t first_id = 0;
  uint32_t second_id = 0;

  bool operator==(const TwoKeyStringStateKey& other) const {
    return first_id == other.first_id && second_id == other.second_id;
  }
};

struct TwoKeyStringStateKeyHash {
  std::size_t operator()(const TwoKeyStringStateKey& value) const;
};

struct TwoKeyStringState {
  std::unordered_map<std::string, uint32_t> first_index_by_value;
  std::unordered_map<std::string, uint32_t> second_index_by_value;
  std::vector<std::string> first_values;
  std::vector<std::string> second_values;
  std::unordered_map<TwoKeyStringStateKey, std::size_t, TwoKeyStringStateKeyHash> index_by_key;
  std::vector<TwoKeyStringStateKey> keys;
  std::vector<double> sums;
};

struct TwoKeyInt64Key {
  bool first_is_null = true;
  int64_t first = 0;
  bool second_is_null = true;
  int64_t second = 0;

  bool operator==(const TwoKeyInt64Key& other) const {
    return first_is_null == other.first_is_null && first == other.first &&
           second_is_null == other.second_is_null && second == other.second;
  }
};

struct TwoKeyInt64KeyHash {
  std::size_t operator()(const TwoKeyInt64Key& value) const;
};

struct TwoKeyInt64State {
  std::unordered_map<TwoKeyInt64Key, std::size_t, TwoKeyInt64KeyHash> index_by_key;
  std::vector<TwoKeyInt64Key> keys;
  std::vector<double> sums;
};

bool usesTwoKeyColumnarLayout(const AggregateExecSpec& spec);
AggregatePartialBatch makeAggregatePartialBatch(const TwoKeyValueColumnarBatch& batch,
                                                const std::vector<std::string>& key_names,
                                                const std::vector<std::string>& state_names);
void mergeAggregatePartialBatch(const AggregatePartialBatch& partial,
                                AggregateStringKeyState* string_state,
                                AggregateFixedKeyState* fixed_state);
Table materializeAggregateStringKeyState(const AggregateStringKeyState& state,
                                         const std::vector<std::string>& key_names,
                                         const std::string& output_column);
Table materializeAggregateFixedKeyState(const AggregateFixedKeyState& state,
                                        const std::vector<std::string>& key_names,
                                        const std::string& output_column);

void mergeTwoKeyPartialBatch(const TwoKeyValueColumnarBatch& partial,
                             TwoKeyStringState* string_state,
                             TwoKeyInt64State* int64_state);
Table materializeTwoKeyStringState(const TwoKeyStringState& state,
                                   const std::string& first_key_name,
                                   const std::string& second_key_name,
                                   const std::string& output_column);
Table materializeTwoKeyInt64State(const TwoKeyInt64State& state,
                                  const std::string& first_key_name,
                                  const std::string& second_key_name,
                                  const std::string& output_column);

}  // namespace dataflow
