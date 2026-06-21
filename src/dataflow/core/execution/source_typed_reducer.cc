#include "src/dataflow/core/execution/source_typed_reducer.h"

#include <memory>
#include <stdexcept>
#include <utility>

#include "src/dataflow/core/execution/columnar_batch.h"

namespace dataflow {
namespace {

std::size_t hashCombine(std::size_t seed, std::size_t value) {
  return seed ^ (value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2));
}

bool valuesEqualForTypedKey(const Value& lhs, const Value& rhs) {
  if (lhs.type() == rhs.type()) {
    return lhs == rhs;
  }
  if (lhs.isNumber() && rhs.isNumber()) {
    return lhs.asDouble() == rhs.asDouble();
  }
  return false;
}

std::size_t hashValueForTypedKey(const Value& value) {
  if (value.isNumber()) {
    return hashCombine(17, std::hash<double>{}(value.asDouble()));
  }
  std::size_t seed = static_cast<std::size_t>(value.type());
  switch (value.type()) {
    case DataType::Nil:
      return hashCombine(seed, 0);
    case DataType::Bool:
      return hashCombine(seed, std::hash<bool>{}(value.asBool()));
    case DataType::Int64:
    case DataType::Double:
    case DataType::Float32:
      return hashCombine(17, std::hash<double>{}(value.asDouble()));
    case DataType::String:
      return hashCombine(seed, std::hash<std::string>{}(value.asString()));
    case DataType::FixedVector: {
      std::size_t out = seed;
      for (const auto item : value.asFixedVector()) {
        out = hashCombine(out, std::hash<float>{}(item));
      }
      return out;
    }
  }
  return seed;
}

Table makeColumnFirstTable(std::vector<std::string> columns) {
  Table table;
  table.schema = Schema(std::move(columns));
  auto cache = std::make_shared<ColumnarTable>();
  cache->schema = table.schema;
  cache->columns.resize(table.schema.fields.size());
  table.columnar_cache = std::move(cache);
  return table;
}

}  // namespace

bool sourceTypedReducerSupports(const SourcePushdownSpec& pushdown) {
  if (!pushdown.has_aggregate || pushdown.aggregate.keys.empty() ||
      pushdown.aggregate.keys.size() > 3 || pushdown.aggregate.aggregates.size() != 1) {
    return false;
  }
  const auto function = pushdown.aggregate.aggregates.front().function;
  return function == AggregateFunction::Count || function == AggregateFunction::Sum ||
         function == AggregateFunction::Avg;
}

SourceTypedReducer::SourceTypedReducer(const Schema& schema,
                                       const SourcePushdownSpec& pushdown)
    : schema_(&schema), pushdown_(&pushdown), supported_(sourceTypedReducerSupports(pushdown)) {
  if (pushdown.aggregate.aggregates.size() == 1) {
    function_ = pushdown.aggregate.aggregates.front().function;
  }
  key_groups_.reserve(64);
  encoded_groups_.reserve(64);
  states_.reserve(64);
}

std::size_t SourceTypedReducer::KeyHash::operator()(const Key& key) const {
  std::size_t seed = key.size;
  for (std::size_t i = 0; i < key.size; ++i) {
    seed = hashCombine(seed, hashValueForTypedKey(key.values[i]));
  }
  return seed;
}

bool SourceTypedReducer::KeyEq::operator()(const Key& lhs, const Key& rhs) const {
  if (lhs.size != rhs.size) {
    return false;
  }
  for (std::size_t i = 0; i < lhs.size; ++i) {
    if (!valuesEqualForTypedKey(lhs.values[i], rhs.values[i])) {
      return false;
    }
  }
  return true;
}

void SourceTypedReducer::add(const std::vector<Value>& keys, const Value* aggregate_value) {
  if (!supported_) {
    throw std::runtime_error("source typed reducer does not support this aggregate shape");
  }
  if (keys.empty() || keys.size() > 3) {
    throw std::runtime_error("source typed reducer key count out of range");
  }
  Key key;
  key.size = keys.size();
  for (std::size_t i = 0; i < keys.size(); ++i) {
    key.values[i] = keys[i];
  }
  auto it = key_groups_.find(key);
  if (it == key_groups_.end()) {
    State state;
    state.keys = key.values;
    state.key_size = key.size;
    states_.push_back(std::move(state));
    const std::size_t index = states_.size() - 1;
    it = key_groups_.emplace(std::move(key), index).first;
  }
  update(&states_[it->second], aggregate_value);
}

bool SourceTypedReducer::hasEncodedKey(const std::string& encoded_key) const {
  return encoded_groups_.find(encoded_key) != encoded_groups_.end();
}

void SourceTypedReducer::addEncodedExisting(const std::string& encoded_key,
                                            const Value* aggregate_value) {
  const auto it = encoded_groups_.find(encoded_key);
  if (it == encoded_groups_.end()) {
    throw std::runtime_error("source typed reducer encoded key is missing");
  }
  update(&states_[it->second], aggregate_value);
}

void SourceTypedReducer::addEncodedNew(std::string encoded_key, const std::vector<Value>& keys,
                                       const Value* aggregate_value) {
  if (!supported_) {
    throw std::runtime_error("source typed reducer does not support this aggregate shape");
  }
  if (keys.empty() || keys.size() > 3) {
    throw std::runtime_error("source typed reducer key count out of range");
  }
  State state;
  state.key_size = keys.size();
  for (std::size_t i = 0; i < keys.size(); ++i) {
    state.keys[i] = keys[i];
  }
  states_.push_back(std::move(state));
  const std::size_t index = states_.size() - 1;
  encoded_groups_.emplace(std::move(encoded_key), index);
  update(&states_[index], aggregate_value);
}

void SourceTypedReducer::update(State* state, const Value* aggregate_value) {
  if (state == nullptr) {
    throw std::runtime_error("source typed reducer state is null");
  }
  switch (function_) {
    case AggregateFunction::Count:
      state->count += 1;
      break;
    case AggregateFunction::Sum:
      if (aggregate_value != nullptr && aggregate_value->isNumber()) {
        state->sum += aggregate_value->asDouble();
      }
      break;
    case AggregateFunction::Avg:
      if (aggregate_value != nullptr && aggregate_value->isNumber()) {
        state->sum += aggregate_value->asDouble();
        state->count += 1;
      }
      break;
    case AggregateFunction::Min:
    case AggregateFunction::Max:
      break;
  }
}

Table SourceTypedReducer::finalize(bool materialize_rows) const {
  if (!supported_ || schema_ == nullptr || pushdown_ == nullptr) {
    throw std::runtime_error("source typed reducer cannot finalize unsupported shape");
  }
  std::vector<std::string> out_fields;
  out_fields.reserve(pushdown_->aggregate.keys.size() + 1);
  for (const auto key_index : pushdown_->aggregate.keys) {
    if (key_index >= schema_->fields.size()) {
      throw std::runtime_error("source typed reducer key index out of range");
    }
    out_fields.push_back(schema_->fields[key_index]);
  }
  out_fields.push_back(pushdown_->aggregate.aggregates.front().output_name);

  Table out = makeColumnFirstTable(std::move(out_fields));
  out.columnar_cache->row_count = states_.size();
  for (auto& column : out.columnar_cache->columns) {
    column.values.reserve(states_.size());
  }
  if (materialize_rows) {
    out.rows.reserve(states_.size());
  }
  for (const auto& state : states_) {
    Row row;
    if (materialize_rows) {
      row.reserve(pushdown_->aggregate.keys.size() + 1);
    }
    for (std::size_t i = 0; i < pushdown_->aggregate.keys.size(); ++i) {
      out.columnar_cache->columns[i].values.push_back(state.keys[i]);
      if (materialize_rows) {
        row.push_back(state.keys[i]);
      }
    }
    Value aggregate_output;
    switch (function_) {
      case AggregateFunction::Count:
        aggregate_output = Value(static_cast<int64_t>(state.count));
        break;
      case AggregateFunction::Sum:
        aggregate_output = Value(state.sum);
        break;
      case AggregateFunction::Avg:
        aggregate_output = state.count == 0
                               ? Value()
                               : Value(state.sum / static_cast<double>(state.count));
        break;
      case AggregateFunction::Min:
      case AggregateFunction::Max:
        aggregate_output = Value();
        break;
    }
    out.columnar_cache->columns.back().values.push_back(aggregate_output);
    if (materialize_rows) {
      row.push_back(std::move(aggregate_output));
      out.rows.push_back(std::move(row));
    }
  }
  if (out.columnar_cache->row_count > 0) {
    out.columnar_cache->batch_row_counts.push_back(out.columnar_cache->row_count);
  }
  return out;
}

}  // namespace dataflow
