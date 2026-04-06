#include "src/dataflow/core/execution/runtime/aggregate_layout.h"

#include <algorithm>
#include "src/dataflow/core/execution/arrow_format.h"
#include "src/dataflow/core/execution/columnar_batch.h"
#include "src/dataflow/core/execution/runtime/simd_dispatch.h"

#include <stdexcept>
#include <string_view>

namespace dataflow {

namespace {

constexpr char kTypedKeyPrefix[] = {'\0', 'V', 'K'};
constexpr std::size_t kTypedKeyPrefixSize = sizeof(kTypedKeyPrefix);

std::string_view keyStringAt(const BinaryKeyColumn& column, std::size_t row_idx) {
  if (column.dictionary_encoded) {
    return column.dictionary.view(column.indices[row_idx]);
  }
  return column.string_values.view(row_idx);
}

std::string encodeTaggedKeyValue(const Value& value) {
  std::string out(kTypedKeyPrefix, kTypedKeyPrefixSize);
  out.push_back(static_cast<char>(value.type()));
  switch (value.type()) {
    case DataType::Nil:
      return out;
    case DataType::Bool:
      out.push_back(value.asBool() ? '1' : '0');
      return out;
    case DataType::Int64:
      out += std::to_string(value.asInt64());
      return out;
    case DataType::Double:
      out += value.toString();
      return out;
    case DataType::String:
      out += value.asString();
      return out;
    case DataType::FixedVector:
      out += value.toString();
      return out;
  }
  return out;
}

Value decodeTaggedKeyValue(std::string_view encoded) {
  if (encoded.size() < kTypedKeyPrefixSize + 1 ||
      !std::equal(std::begin(kTypedKeyPrefix), std::end(kTypedKeyPrefix), encoded.begin())) {
    return Value(std::string(encoded));
  }
  const auto type = static_cast<DataType>(static_cast<uint8_t>(encoded[kTypedKeyPrefixSize]));
  const std::string payload(encoded.substr(kTypedKeyPrefixSize + 1));
  switch (type) {
    case DataType::Nil:
      return Value();
    case DataType::Bool:
      return Value(!payload.empty() && payload[0] == '1');
    case DataType::Int64:
      return Value(payload.empty() ? int64_t(0)
                                   : static_cast<int64_t>(std::stoll(payload)));
    case DataType::Double:
      return Value(payload.empty() ? 0.0 : std::stod(payload));
    case DataType::String:
      return Value(payload);
    case DataType::FixedVector:
      return Value(Value::parseFixedVector(payload));
  }
  return Value(payload);
}

std::string typedLookupKey(const Value& value) {
  switch (value.type()) {
    case DataType::Nil:
      return "n:";
    case DataType::Bool:
      return value.asBool() ? "b:1" : "b:0";
    case DataType::Int64:
      return "i:" + std::to_string(value.asInt64());
    case DataType::Double:
      return "d:" + value.toString();
    case DataType::String:
      return "s:" + value.asString();
    case DataType::FixedVector:
      return "v:" + value.toString();
  }
  return "";
}

uint32_t internStringKey(const std::string& lookup_key,
                         std::unordered_map<std::string, uint32_t>* index,
                         const Value& stored_value, std::vector<Value>* values) {
  if (index == nullptr || values == nullptr) {
    throw std::invalid_argument("internStringKey requires state dictionaries");
  }
  auto it = index->find(lookup_key);
  if (it != index->end()) {
    return it->second;
  }
  const uint32_t id = static_cast<uint32_t>(values->size());
  values->push_back(stored_value);
  index->emplace(lookup_key, id);
  return id;
}

}  // namespace

std::size_t AggregateStringKeyTupleHash::operator()(const AggregateStringKeyTuple& value) const {
  std::size_t seed = 0;
  for (std::size_t i = 0; i < value.ids.size(); ++i) {
    seed ^= std::hash<uint32_t>{}(value.ids[i]) + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
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
  state.merge_op = AggregateStateMergeOp::Sum;
  state.values = batch.value;
  out.state_columns.push_back(std::move(state));
  return out;
}

AggregatePartialBatch makeAggregatePartialBatch(
    const KeyStateColumnarBatch& batch,
    const std::vector<std::pair<std::string, AggregateStateMergeOp>>& state_columns) {
  if (batch.state_columns.size() != state_columns.size()) {
    throw std::invalid_argument("key-state columnar batch state arity mismatch");
  }
  AggregatePartialBatch out;
  out.row_count = batch.row_count;
  out.key_names = batch.key_names;
  out.key_columns = batch.key_columns;
  out.state_columns.reserve(state_columns.size());
  for (std::size_t i = 0; i < state_columns.size(); ++i) {
    AggregatePartialBatch::StateColumn state;
    state.name = state_columns[i].first;
    state.merge_op = state_columns[i].second;
    state.values = batch.state_columns[i];
    out.state_columns.push_back(std::move(state));
  }
  return out;
}

AggregatePartialBatch makeAggregatePartialBatchFromTable(
    const Table& partials, const std::vector<std::string>& key_names,
    const std::vector<std::pair<std::string, AggregateStateMergeOp>>& state_columns) {
  AggregatePartialBatch out;
  out.row_count = partials.rowCount();
  out.key_names = key_names;

  for (const auto& key_name : key_names) {
    if (!partials.schema.has(key_name)) {
      throw std::runtime_error("partial batch key column not found: " + key_name);
    }
    const auto column = viewValueColumn(partials, partials.schema.indexOf(key_name));
    BinaryKeyColumn key_column;
    key_column.is_null.assign(out.row_count, 0);
    bool fixed_width = false;
    if (column.buffer != nullptr) {
      if (column.buffer->arrow_backing != nullptr) {
        fixed_width = column.buffer->arrow_backing->format == kArrowFormatBool ||
                      isArrowIntegerLikeFormat(column.buffer->arrow_backing->format);
      } else {
        fixed_width = std::all_of(
            column.buffer->values.begin(), column.buffer->values.end(),
            [](const Value& value) {
              return value.isNull() || value.type() == DataType::Int64 ||
                     value.type() == DataType::Bool;
            });
      }
    }
    key_column.type = fixed_width ? BinaryKeyColumnType::Int64 : BinaryKeyColumnType::String;
    key_column.dictionary_encoded = !fixed_width;
    if (fixed_width) {
      key_column.int64_values.assign(out.row_count, 0);
    } else {
      key_column.dictionary_encoded = false;
      key_column.string_values.reserve(out.row_count, out.row_count * 16);
    }
    for (std::size_t row_idx = 0; row_idx < out.row_count; ++row_idx) {
      const Value value = valueColumnValueAt(*column.buffer, row_idx);
      if (value.isNull()) {
        key_column.is_null[row_idx] = 1;
        continue;
      }
      if (fixed_width) {
        key_column.int64_values[row_idx] =
            value.type() == DataType::Bool ? (value.asBool() ? 1 : 0) : value.asInt64();
      } else {
        key_column.string_values.append(encodeTaggedKeyValue(value));
      }
    }
    out.key_columns.push_back(std::move(key_column));
  }

  for (const auto& state_spec : state_columns) {
    if (!partials.schema.has(state_spec.first)) {
      std::string available = "[";
      for (std::size_t i = 0; i < partials.schema.fields.size(); ++i) {
        if (i > 0) available += ", ";
        available += partials.schema.fields[i];
      }
      available += "]";
      throw std::runtime_error("partial batch state column not found: " + state_spec.first +
                               ", available=" + available);
    }
    const auto column = viewValueColumn(partials, partials.schema.indexOf(state_spec.first));
    AggregatePartialBatch::StateColumn state;
    state.name = state_spec.first;
    state.merge_op = state_spec.second;
    state.values.is_null.assign(out.row_count, 0);
    state.values.values.assign(out.row_count, 0.0);
    for (std::size_t row_idx = 0; row_idx < out.row_count; ++row_idx) {
      if (valueColumnIsNullAt(*column.buffer, row_idx)) {
        state.values.is_null[row_idx] = 1;
        continue;
      }
      state.values.values[row_idx] = valueColumnDoubleAt(*column.buffer, row_idx);
    }
    out.state_columns.push_back(std::move(state));
  }
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
      std::vector<double> row_values(state_count, 0.0);
      for (std::size_t state_index = 0; state_index < state_count; ++state_index) {
        row_values[state_index] = partial.state_columns[state_index].values.values[row_idx];
      }
      AggregateFixedKeyTuple key;
      key.values.resize(partial.key_columns.size(), 0);
      key.is_null.resize(partial.key_columns.size(), 0);
      for (std::size_t key_index = 0; key_index < partial.key_columns.size(); ++key_index) {
        const auto& key_column = partial.key_columns[key_index];
        key.values[key_index] = key_column.int64_values[row_idx];
        key.is_null[key_index] = 0;
      }
      auto it = fixed_state->index_by_key.find(key);
      if (it == fixed_state->index_by_key.end()) {
        const std::size_t index = fixed_state->keys.size();
        fixed_state->index_by_key.emplace(key, index);
        fixed_state->keys.push_back(key);
        fixed_state->state_values.resize((index + 1) * state_count, 0.0);
        auto* dst_init = fixed_state->state_values.data() + (index * state_count);
        for (std::size_t state_index = 0; state_index < state_count; ++state_index) {
          dst_init[state_index] = row_values[state_index];
        }
        it = fixed_state->index_by_key.find(key);
        continue;
      }
      auto* dst = fixed_state->state_values.data() + (it->second * state_count);
      std::size_t begin = 0;
      while (begin < state_count) {
        const auto op = partial.state_columns[begin].merge_op;
        std::size_t end = begin + 1;
        while (end < state_count && partial.state_columns[end].merge_op == op) {
          ++end;
        }
        simdDispatch().combine_double(dst + begin, row_values.data() + begin, end - begin,
                                      static_cast<NumericCombineOp>(op));
        begin = end;
      }
      continue;
    }

    if (string_state == nullptr) continue;
    if (string_state->index_by_value.empty()) {
      string_state->index_by_value.resize(partial.key_columns.size());
      string_state->values_by_key.resize(partial.key_columns.size());
    }
    string_state->state_count = state_count;
    std::vector<double> row_values(state_count, 0.0);
    for (std::size_t state_index = 0; state_index < state_count; ++state_index) {
      row_values[state_index] = partial.state_columns[state_index].values.values[row_idx];
    }
    AggregateStringKeyTuple key;
    key.ids.resize(partial.key_columns.size(), 0);
    for (std::size_t key_index = 0; key_index < partial.key_columns.size(); ++key_index) {
      const auto& key_column = partial.key_columns[key_index];
      std::string_view view = key_column.type == BinaryKeyColumnType::Int64
                                  ? std::string_view()
                                  : keyStringAt(key_column, row_idx);
      uint32_t id = 0;
      if (key_column.type == BinaryKeyColumnType::Int64) {
        const Value key_value(key_column.int64_values[row_idx]);
        id = internStringKey(typedLookupKey(key_value), &string_state->index_by_value[key_index], key_value,
                             &string_state->values_by_key[key_index]);
      } else {
        const Value key_value = decodeTaggedKeyValue(view);
        id = internStringKey(typedLookupKey(key_value), &string_state->index_by_value[key_index], key_value,
                             &string_state->values_by_key[key_index]);
      }
      key.ids[key_index] = id;
    }
    auto it = string_state->index_by_key.find(key);
    if (it == string_state->index_by_key.end()) {
      const std::size_t index = string_state->keys.size();
      string_state->index_by_key.emplace(key, index);
      string_state->keys.push_back(key);
      string_state->state_values.resize((index + 1) * state_count, 0.0);
      auto* dst_init = string_state->state_values.data() + (index * state_count);
      for (std::size_t state_index = 0; state_index < state_count; ++state_index) {
        dst_init[state_index] = row_values[state_index];
      }
      it = string_state->index_by_key.find(key);
      continue;
    }
    auto* dst = string_state->state_values.data() + (it->second * state_count);
    std::size_t begin = 0;
    while (begin < state_count) {
      const auto op = partial.state_columns[begin].merge_op;
      std::size_t end = begin + 1;
      while (end < state_count && partial.state_columns[end].merge_op == op) {
        ++end;
      }
      simdDispatch().combine_double(dst + begin, row_values.data() + begin, end - begin,
                                    static_cast<NumericCombineOp>(op));
      begin = end;
    }
  }
}

Table materializeAggregateStringKeyState(const AggregateStringKeyState& state,
                                         const std::vector<std::string>& key_names,
                                         const std::vector<std::string>& state_names) {
  std::vector<std::string> fields = key_names;
  fields.insert(fields.end(), state_names.begin(), state_names.end());
  Table out(Schema(fields), {});
  out.rows.reserve(state.keys.size());
  for (std::size_t i = 0; i < state.keys.size(); ++i) {
    Row row;
    for (std::size_t key_index = 0; key_index < state.keys[i].ids.size(); ++key_index) {
      row.push_back(state.values_by_key.at(key_index).at(state.keys[i].ids[key_index]));
    }
    for (std::size_t state_index = 0; state_index < state.state_count; ++state_index) {
      row.emplace_back(state.state_values[i * state.state_count + state_index]);
    }
    out.rows.push_back(std::move(row));
  }
  return out;
}

Table materializeAggregateFixedKeyState(const AggregateFixedKeyState& state,
                                        const std::vector<std::string>& key_names,
                                        const std::vector<std::string>& state_names) {
  std::vector<std::string> fields = key_names;
  fields.insert(fields.end(), state_names.begin(), state_names.end());
  Table out(Schema(fields), {});
  out.rows.reserve(state.keys.size());
  for (std::size_t i = 0; i < state.keys.size(); ++i) {
    Row row;
    for (std::size_t key_index = 0; key_index < state.keys[i].values.size(); ++key_index) {
      row.emplace_back(state.keys[i].is_null[key_index] != 0 ? Value()
                                                             : Value(state.keys[i].values[key_index]));
    }
    for (std::size_t state_index = 0; state_index < state.state_count; ++state_index) {
      row.emplace_back(state.state_values[i * state.state_count + state_index]);
    }
    out.rows.push_back(std::move(row));
  }
  return out;
}

}  // namespace dataflow
