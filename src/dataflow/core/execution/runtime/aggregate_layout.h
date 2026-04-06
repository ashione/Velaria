#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "src/dataflow/core/execution/stream/binary_row_batch.h"
#include "src/dataflow/core/execution/table.h"
#include "src/dataflow/core/logical/planner/plan.h"

namespace dataflow {

enum class AggregateStateMergeOp {
  Sum,
  Min,
  Max,
};

struct AggregatePartialBatch {
  struct StateColumn {
    std::string name;
    AggregateStateMergeOp merge_op = AggregateStateMergeOp::Sum;
    BinaryDoubleColumn values;
  };

  std::size_t row_count = 0;
  std::vector<std::string> key_names;
  std::vector<BinaryKeyColumn> key_columns;
  std::vector<StateColumn> state_columns;
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
  std::size_t state_count = 0;
  std::vector<std::unordered_map<std::string, uint32_t>> index_by_value;
  std::vector<std::vector<Value>> values_by_key;
  std::unordered_map<AggregateStringKeyTuple, std::size_t, AggregateStringKeyTupleHash> index_by_key;
  std::vector<AggregateStringKeyTuple> keys;
  std::vector<double> state_values;
};

struct AggregateFixedKeyTuple {
  uint8_t arity = 0;
  std::array<int64_t, 4> values{};
  std::array<uint8_t, 4> is_null{};

  bool operator==(const AggregateFixedKeyTuple& other) const {
    if (arity != other.arity) return false;
    for (uint8_t i = 0; i < arity; ++i) {
      if (values[i] != other.values[i] || is_null[i] != other.is_null[i]) {
        return false;
      }
    }
    return true;
  }
};

struct AggregateFixedKeyTupleHash {
  std::size_t operator()(const AggregateFixedKeyTuple& value) const;
};

struct AggregateFixedKeyState {
  std::size_t key_count = 0;
  std::size_t state_count = 0;
  std::unordered_map<AggregateFixedKeyTuple, std::size_t, AggregateFixedKeyTupleHash> index_by_key;
  std::vector<AggregateFixedKeyTuple> keys;
  std::vector<double> state_values;
};

AggregatePartialBatch makeAggregatePartialBatch(const TwoKeyValueColumnarBatch& batch,
                                                const std::vector<std::string>& key_names,
                                                const std::vector<std::string>& state_names);
AggregatePartialBatch makeAggregatePartialBatchFromTable(
    const Table& partials, const std::vector<std::string>& key_names,
    const std::vector<std::pair<std::string, AggregateStateMergeOp>>& state_columns);
void mergeAggregatePartialBatch(const AggregatePartialBatch& partial,
                                AggregateStringKeyState* string_state,
                                AggregateFixedKeyState* fixed_state);
Table materializeAggregateStringKeyState(const AggregateStringKeyState& state,
                                         const std::vector<std::string>& key_names,
                                         const std::vector<std::string>& state_names);
Table materializeAggregateFixedKeyState(const AggregateFixedKeyState& state,
                                        const std::vector<std::string>& key_names,
                                        const std::vector<std::string>& state_names);

}  // namespace dataflow
