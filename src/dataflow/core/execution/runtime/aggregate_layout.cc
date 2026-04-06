#include "src/dataflow/core/execution/runtime/aggregate_layout.h"

#include "src/dataflow/core/execution/runtime/simd_dispatch.h"

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

AggregatePartialBatch makeAggregatePartialBatch(const TwoKeyValueColumnarBatch& batch,
                                                const std::vector<std::string>& key_names,
                                                const std::vector<std::string>& state_names) {
  AggregatePartialBatch out;
  out.row_count = batch.row_count;
  out.key_names = key_names;
  out.key_columns.push_back(batch.first_key);
  out.key_columns.push_back(batch.second_key);
  AggregatePartialBatch::StateColumn state;
  state.name = state_names.empty() ? "sum" : state_names.front();
  state.values = batch.value;
  out.state_columns.push_back(std::move(state));
  return out;
}

void mergeAggregatePartialBatch(const AggregatePartialBatch& partial,
                                AggregateStringKeyState* string_state,
                                AggregateFixedKeyState* fixed_state) {
  if (partial.key_columns.empty() || partial.state_columns.empty()) {
    return;
  }
  bool all_fixed = true;
  for (const auto& key_column : partial.key_columns) {
    if (key_column.type != BinaryKeyColumnType::Int64) {
      all_fixed = false;
      break;
    }
  }
  const std::size_t state_count = partial.state_columns.size();
  for (std::size_t row_idx = 0; row_idx < partial.row_count; ++row_idx) {
    bool skip = false;
    for (const auto& state_column : partial.state_columns) {
      if (state_column.values.is_null[row_idx] != 0) {
        skip = true;
        break;
      }
    }
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
      fixed_state->state_count = state_count;
      AggregateFixedKeyTuple key;
      key.values.reserve(partial.key_columns.size());
      key.is_null.reserve(partial.key_columns.size());
      for (const auto& key_column : partial.key_columns) {
        key.values.push_back(key_column.int64_values[row_idx]);
        key.is_null.push_back(0);
      }
      auto it = fixed_state->index_by_key.find(key);
      if (it == fixed_state->index_by_key.end()) {
        const std::size_t index = fixed_state->keys.size();
        fixed_state->index_by_key.emplace(key, index);
        fixed_state->keys.push_back(key);
        fixed_state->state_values.resize((index + 1) * state_count, 0.0);
        it = fixed_state->index_by_key.find(key);
      }
      std::vector<double> row_values(state_count, 0.0);
      for (std::size_t state_index = 0; state_index < state_count; ++state_index) {
        row_values[state_index] = partial.state_columns[state_index].values.values[row_idx];
      }
      simdDispatch().accumulate_double(
          fixed_state->state_values.data() + (it->second * state_count), row_values.data(),
          state_count);
      continue;
    }

    if (string_state == nullptr) continue;
    if (string_state->index_by_value.empty()) {
      string_state->index_by_value.resize(partial.key_columns.size());
      string_state->values_by_key.resize(partial.key_columns.size());
    }
    string_state->state_count = state_count;
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
      const std::size_t index = string_state->keys.size();
      string_state->index_by_key.emplace(key, index);
      string_state->keys.push_back(key);
      string_state->state_values.resize((index + 1) * state_count, 0.0);
      it = string_state->index_by_key.find(key);
    }
    std::vector<double> row_values(state_count, 0.0);
    for (std::size_t state_index = 0; state_index < state_count; ++state_index) {
      row_values[state_index] = partial.state_columns[state_index].values.values[row_idx];
    }
    simdDispatch().accumulate_double(
        string_state->state_values.data() + (it->second * state_count), row_values.data(),
        state_count);
  }
}

Table materializeAggregateStringKeyState(const AggregateStringKeyState& state,
                                         const std::vector<std::string>& key_names,
                                         const std::string& output_column) {
  std::vector<std::string> fields = key_names;
  fields.push_back(output_column);
  Table out(Schema(fields), {});
  out.rows.reserve(state.keys.size());
  for (std::size_t i = 0; i < state.keys.size(); ++i) {
    Row row;
    for (std::size_t key_index = 0; key_index < state.keys[i].ids.size(); ++key_index) {
      row.emplace_back(state.values_by_key[key_index][state.keys[i].ids[key_index]]);
    }
    row.emplace_back(state.state_values[i * state.state_count]);
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
  out.rows.reserve(state.keys.size());
  for (std::size_t i = 0; i < state.keys.size(); ++i) {
    Row row;
    for (std::size_t key_index = 0; key_index < state.keys[i].values.size(); ++key_index) {
      row.emplace_back(state.keys[i].is_null[key_index] != 0 ? Value()
                                                             : Value(state.keys[i].values[key_index]));
    }
    row.emplace_back(state.state_values[i * state.state_count]);
    out.rows.push_back(std::move(row));
  }
  return out;
}

}  // namespace dataflow
