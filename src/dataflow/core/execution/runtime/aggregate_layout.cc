#include "src/dataflow/core/execution/runtime/aggregate_layout.h"

#include <stdexcept>
#include <string_view>

namespace dataflow {

namespace {

std::string_view keyStringAt(const BinaryKeyColumn& column, std::size_t row_idx) {
  if (column.dictionary_encoded) {
    return column.dictionary.view(column.indices[row_idx]);
  }
  return column.string_values.view(row_idx);
}

uint32_t internStringKey(std::string_view value, std::unordered_map<std::string, uint32_t>* index,
                         std::vector<std::string>* values) {
  if (index == nullptr || values == nullptr) {
    throw std::invalid_argument("internStringKey requires state dictionaries");
  }
  std::string owned(value.data(), value.size());
  auto it = index->find(owned);
  if (it != index->end()) {
    return it->second;
  }
  const uint32_t id = static_cast<uint32_t>(values->size());
  values->push_back(owned);
  index->emplace(std::move(owned), id);
  return id;
}

}  // namespace

std::size_t AggregateStringKeyTupleHash::operator()(const AggregateStringKeyTuple& value) const {
  std::size_t seed = 0;
  for (const auto id : value.ids) {
    seed ^= std::hash<uint32_t>{}(id) + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
  }
  return seed;
}

std::size_t AggregateFixedKeyTupleHash::operator()(const AggregateFixedKeyTuple& value) const {
  std::size_t seed = 0;
  for (std::size_t i = 0; i < value.values.size(); ++i) {
    seed ^= std::hash<uint8_t>{}(value.is_null[i]) + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
    seed ^= std::hash<int64_t>{}(value.values[i]) + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
  }
  return seed;
}

std::size_t TwoKeyStringStateKeyHash::operator()(const TwoKeyStringStateKey& value) const {
  std::size_t seed = std::hash<uint32_t>{}(value.first_id);
  seed ^= std::hash<uint32_t>{}(value.second_id) + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
  return seed;
}

std::size_t TwoKeyInt64KeyHash::operator()(const TwoKeyInt64Key& value) const {
  std::size_t seed = std::hash<bool>{}(value.first_is_null);
  seed ^= std::hash<int64_t>{}(value.first) + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
  seed ^= std::hash<bool>{}(value.second_is_null) + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
  seed ^= std::hash<int64_t>{}(value.second) + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
  return seed;
}

bool usesTwoKeyColumnarLayout(const AggregateExecSpec& spec) {
  return spec.partial_layout == AggregatePartialLayoutKind::TwoKeyColumnar;
}

AggregatePartialBatch makeAggregatePartialBatch(const TwoKeyValueColumnarBatch& batch,
                                                const std::vector<std::string>& key_names,
                                                const std::vector<std::string>& state_names) {
  AggregatePartialBatch out;
  out.row_count = batch.row_count;
  out.key_names = key_names;
  out.key_columns.push_back(batch.first_key);
  out.key_columns.push_back(batch.second_key);
  out.state_names = state_names;
  out.sum_state_columns.push_back(batch.value);
  return out;
}

void mergeAggregatePartialBatch(const AggregatePartialBatch& partial,
                                AggregateStringKeyState* string_state,
                                AggregateFixedKeyState* fixed_state) {
  if (partial.key_columns.empty() || partial.sum_state_columns.empty()) {
    return;
  }
  bool all_fixed = true;
  for (const auto& key_column : partial.key_columns) {
    if (key_column.type != BinaryKeyColumnType::Int64) {
      all_fixed = false;
      break;
    }
  }
  const auto& values = partial.sum_state_columns.front();
  for (std::size_t row_idx = 0; row_idx < partial.row_count; ++row_idx) {
    bool skip = values.is_null[row_idx] != 0;
    if (!skip) {
      for (const auto& key_column : partial.key_columns) {
        if (key_column.is_null[row_idx] != 0) {
          skip = true;
          break;
        }
      }
    }
    if (skip) continue;

    if (all_fixed) {
      if (fixed_state == nullptr) continue;
      fixed_state->key_count = partial.key_columns.size();
      AggregateFixedKeyTuple key;
      key.values.reserve(partial.key_columns.size());
      key.is_null.reserve(partial.key_columns.size());
      for (const auto& key_column : partial.key_columns) {
        key.values.push_back(key_column.int64_values[row_idx]);
        key.is_null.push_back(0);
      }
      auto it = fixed_state->index_by_key.find(key);
      if (it == fixed_state->index_by_key.end()) {
        const std::size_t index = fixed_state->sums.size();
        fixed_state->index_by_key.emplace(key, index);
        fixed_state->keys.push_back(key);
        fixed_state->sums.push_back(0.0);
        it = fixed_state->index_by_key.find(key);
      }
      fixed_state->sums[it->second] += values.values[row_idx];
      continue;
    }

    if (string_state == nullptr) continue;
    if (string_state->index_by_value.empty()) {
      string_state->index_by_value.resize(partial.key_columns.size());
      string_state->values_by_key.resize(partial.key_columns.size());
    }
    AggregateStringKeyTuple key;
    key.ids.reserve(partial.key_columns.size());
    for (std::size_t key_index = 0; key_index < partial.key_columns.size(); ++key_index) {
      const auto& key_column = partial.key_columns[key_index];
      std::string_view view = key_column.type == BinaryKeyColumnType::Int64
                                  ? std::string_view()
                                  : keyStringAt(key_column, row_idx);
      uint32_t id = 0;
      if (key_column.type == BinaryKeyColumnType::Int64) {
        const std::string encoded = std::to_string(key_column.int64_values[row_idx]);
        id = internStringKey(encoded, &string_state->index_by_value[key_index],
                             &string_state->values_by_key[key_index]);
      } else {
        id = internStringKey(view, &string_state->index_by_value[key_index],
                             &string_state->values_by_key[key_index]);
      }
      key.ids.push_back(id);
    }
    auto it = string_state->index_by_key.find(key);
    if (it == string_state->index_by_key.end()) {
      const std::size_t index = string_state->sums.size();
      string_state->index_by_key.emplace(key, index);
      string_state->keys.push_back(key);
      string_state->sums.push_back(0.0);
      it = string_state->index_by_key.find(key);
    }
    string_state->sums[it->second] += values.values[row_idx];
  }
}

Table materializeAggregateStringKeyState(const AggregateStringKeyState& state,
                                         const std::vector<std::string>& key_names,
                                         const std::string& output_column) {
  std::vector<std::string> fields = key_names;
  fields.push_back(output_column);
  Table out(Schema(fields), {});
  out.rows.reserve(state.sums.size());
  for (std::size_t i = 0; i < state.sums.size(); ++i) {
    Row row;
    for (std::size_t key_index = 0; key_index < state.keys[i].ids.size(); ++key_index) {
      row.emplace_back(state.values_by_key[key_index][state.keys[i].ids[key_index]]);
    }
    row.emplace_back(state.sums[i]);
    out.rows.push_back(std::move(row));
  }
  return out;
}

Table materializeAggregateFixedKeyState(const AggregateFixedKeyState& state,
                                        const std::vector<std::string>& key_names,
                                        const std::string& output_column) {
  std::vector<std::string> fields = key_names;
  fields.push_back(output_column);
  Table out(Schema(fields), {});
  out.rows.reserve(state.sums.size());
  for (std::size_t i = 0; i < state.sums.size(); ++i) {
    Row row;
    for (std::size_t key_index = 0; key_index < state.keys[i].values.size(); ++key_index) {
      row.emplace_back(state.keys[i].is_null[key_index] != 0 ? Value()
                                                             : Value(state.keys[i].values[key_index]));
    }
    row.emplace_back(state.sums[i]);
    out.rows.push_back(std::move(row));
  }
  return out;
}

void mergeTwoKeyPartialBatch(const TwoKeyValueColumnarBatch& partial,
                             TwoKeyStringState* string_state,
                             TwoKeyInt64State* int64_state) {
  const bool int64_keys = partial.first_key.type == BinaryKeyColumnType::Int64 &&
                          partial.second_key.type == BinaryKeyColumnType::Int64;
  for (std::size_t row_idx = 0; row_idx < partial.row_count; ++row_idx) {
    if (partial.first_key.is_null[row_idx] || partial.second_key.is_null[row_idx] ||
        partial.value.is_null[row_idx]) {
      continue;
    }
    if (int64_keys) {
      if (int64_state == nullptr) continue;
      const TwoKeyInt64Key key{false, partial.first_key.int64_values[row_idx], false,
                               partial.second_key.int64_values[row_idx]};
      auto it = int64_state->index_by_key.find(key);
      if (it == int64_state->index_by_key.end()) {
        const std::size_t index = int64_state->sums.size();
        int64_state->index_by_key.emplace(key, index);
        int64_state->keys.push_back(key);
        int64_state->sums.push_back(0.0);
        it = int64_state->index_by_key.find(key);
      }
      int64_state->sums[it->second] += partial.value.values[row_idx];
      continue;
    }

    if (string_state == nullptr) continue;
    const uint32_t first_id = internStringKey(keyStringAt(partial.first_key, row_idx),
                                              &string_state->first_index_by_value,
                                              &string_state->first_values);
    const uint32_t second_id = internStringKey(keyStringAt(partial.second_key, row_idx),
                                               &string_state->second_index_by_value,
                                               &string_state->second_values);
    const TwoKeyStringStateKey key{first_id, second_id};
    auto it = string_state->index_by_key.find(key);
    if (it == string_state->index_by_key.end()) {
      const std::size_t index = string_state->sums.size();
      string_state->index_by_key.emplace(key, index);
      string_state->keys.push_back(key);
      string_state->sums.push_back(0.0);
      it = string_state->index_by_key.find(key);
    }
    string_state->sums[it->second] += partial.value.values[row_idx];
  }
}

Table materializeTwoKeyStringState(const TwoKeyStringState& state,
                                   const std::string& first_key_name,
                                   const std::string& second_key_name,
                                   const std::string& output_column) {
  Table out(Schema({first_key_name, second_key_name, output_column}), {});
  out.rows.reserve(state.sums.size());
  for (std::size_t i = 0; i < state.sums.size(); ++i) {
    Row row;
    row.emplace_back(state.first_values[state.keys[i].first_id]);
    row.emplace_back(state.second_values[state.keys[i].second_id]);
    row.emplace_back(state.sums[i]);
    out.rows.push_back(std::move(row));
  }
  return out;
}

Table materializeTwoKeyInt64State(const TwoKeyInt64State& state,
                                  const std::string& first_key_name,
                                  const std::string& second_key_name,
                                  const std::string& output_column) {
  Table out(Schema({first_key_name, second_key_name, output_column}), {});
  out.rows.reserve(state.sums.size());
  for (std::size_t i = 0; i < state.sums.size(); ++i) {
    Row row;
    row.emplace_back(state.keys[i].first_is_null ? Value() : Value(state.keys[i].first));
    row.emplace_back(state.keys[i].second_is_null ? Value() : Value(state.keys[i].second));
    row.emplace_back(state.sums[i]);
    out.rows.push_back(std::move(row));
  }
  return out;
}

}  // namespace dataflow
