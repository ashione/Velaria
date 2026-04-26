#include "src/dataflow/core/execution/runtime/executor.h"

#include <memory>
#include <algorithm>
#include <array>
#include <cstdint>
#include <functional>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "src/dataflow/core/execution/columnar_batch.h"
#include "src/dataflow/core/execution/arrow_format.h"
#include "src/dataflow/core/execution/csv.h"
#include "src/dataflow/core/execution/file_source.h"
#include "src/dataflow/core/execution/source_materialization.h"
#include "src/dataflow/core/execution/runtime/execution_optimizer.h"
#include "src/dataflow/experimental/runtime/rpc_runner.h"

namespace dataflow {

namespace {

constexpr char kGroupDelim = '\x1f';
constexpr char kValueMetaDelim = '\x1e';

enum class FilterCompareOp : uint8_t { Eq, Ne, Lt, Gt, Le, Ge };

FilterCompareOp parseFilterCompareOp(std::string_view op) {
  if (op == "=" || op == "==") return FilterCompareOp::Eq;
  if (op == "!=") return FilterCompareOp::Ne;
  if (op == "<") return FilterCompareOp::Lt;
  if (op == ">") return FilterCompareOp::Gt;
  if (op == "<=") return FilterCompareOp::Le;
  if (op == ">=") return FilterCompareOp::Ge;
  throw std::runtime_error("unsupported filter op: " + std::string(op));
}

bool matchesFilterCompareOp(int compare_result, FilterCompareOp op) {
  switch (op) {
    case FilterCompareOp::Eq:
      return compare_result == 0;
    case FilterCompareOp::Ne:
      return compare_result != 0;
    case FilterCompareOp::Lt:
      return compare_result < 0;
    case FilterCompareOp::Gt:
      return compare_result > 0;
    case FilterCompareOp::Le:
      return compare_result <= 0;
    case FilterCompareOp::Ge:
      return compare_result >= 0;
  }
  return false;
}

struct BoundFilter {
  const ValueColumnBuffer* column = nullptr;
  const Value* rhs = nullptr;
  FilterCompareOp op = FilterCompareOp::Eq;
};

template <typename T>
int compareFilterScalar(const T& lhs, const T& rhs) {
  return lhs < rhs ? -1 : (lhs > rhs ? 1 : 0);
}

bool rowMatchesFilter(const BoundFilter& filter, std::size_t row_index) {
  const auto& column = *filter.column;
  const auto& rhs = *filter.rhs;
  const auto op = filter.op;
  if (valueColumnIsNullAt(column, row_index)) {
    if (op == FilterCompareOp::Eq) {
      return rhs.isNull();
    }
    if (op == FilterCompareOp::Ne) {
      return !rhs.isNull();
    }
    return false;
  }
  if (rhs.isNull()) {
    return op == FilterCompareOp::Ne;
  }

  if (rhs.isNumber()) {
    if (!column.values.empty()) {
      const auto& lhs = column.values[row_index];
      if (!lhs.isNumber()) {
        return false;
      }
      return matchesFilterCompareOp(compareFilterScalar(lhs.asDouble(), rhs.asDouble()), op);
    }
    if (column.arrow_backing != nullptr) {
      const auto format = column.arrow_backing->format;
      if (isArrowPrimitiveNumericFormat(format)) {
        return matchesFilterCompareOp(
            compareFilterScalar(valueColumnDoubleAt(column, row_index), rhs.asDouble()), op);
      }
    }
  }

  if (rhs.type() == DataType::String) {
    if (!column.values.empty()) {
      const auto& lhs = column.values[row_index];
      if (lhs.type() != DataType::String) {
        return false;
      }
      return matchesFilterCompareOp(compareFilterScalar(lhs.asString(), rhs.asString()), op);
    }
    if (column.arrow_backing != nullptr && isArrowUtf8Format(column.arrow_backing->format)) {
      return matchesFilterCompareOp(
          compareFilterScalar(valueColumnStringViewAt(column, row_index),
                              std::string_view(rhs.asString())),
          op);
    }
  }

  const Value lhs = column.values.empty() ? valueColumnValueAt(column, row_index)
                                          : column.values[row_index];
  try {
    return matchesFilterCompareOp(lhs == rhs ? 0 : (lhs < rhs ? -1 : 1), op);
  } catch (...) {
    return false;
  }
}

bool rowMatchesAllFilters(std::span<const BoundFilter> filters, std::size_t row_index) {
  for (const auto& filter : filters) {
    if (!rowMatchesFilter(filter, row_index)) {
      return false;
    }
  }
  return true;
}

std::vector<std::string_view> splitKeyViews(std::string_view key) {
  std::vector<std::string_view> out;
  std::size_t begin = 0;
  for (std::size_t index = 0; index < key.size(); ++index) {
    if (key[index] != kGroupDelim) {
      continue;
    }
    out.push_back(key.substr(begin, index - begin));
    begin = index + 1;
  }
  out.push_back(key.substr(begin));
  return out;
}

std::string_view singleStringKeyAt(const ValueColumnBuffer& column, std::size_t row_index) {
  if (valueColumnIsNullAt(column, row_index)) {
    return std::string_view("null");
  }
  const auto view = valueColumnStringViewAt(column, row_index);
  if (!view.empty()) {
    return view;
  }
  return std::string_view();
}

void appendLengthPrefixedUnionValue(std::string* key, const Value& value) {
  const auto encoded = value.toString();
  key->append(std::to_string(static_cast<int>(value.type())));
  key->push_back(kValueMetaDelim);
  key->append(std::to_string(encoded.size()));
  key->push_back(kValueMetaDelim);
  key->append(encoded);
  key->push_back(kGroupDelim);
}

std::string serializeUnionRow(const Row& row) {
  std::string key;
  for (const auto& value : row) {
    appendLengthPrefixedUnionValue(&key, value);
  }
  return key;
}

Table executeUnionTable(Table left, Table right, bool distinct) {
  if (left.schema.fields.size() != right.schema.fields.size()) {
    throw std::runtime_error("UNION requires the same number of columns on both sides");
  }
  materializeRows(&left);
  materializeRows(&right);

  Table out;
  out.schema = left.schema;
  out.rows.reserve(left.rows.size() + right.rows.size());
  if (!distinct) {
    out.rows.insert(out.rows.end(), left.rows.begin(), left.rows.end());
    out.rows.insert(out.rows.end(), right.rows.begin(), right.rows.end());
    return out;
  }

  std::unordered_set<std::string> seen;
  seen.reserve(left.rows.size() + right.rows.size());
  auto append_unique = [&](const std::vector<Row>& rows) {
    for (const auto& row : rows) {
      auto key = serializeUnionRow(row);
      if (seen.insert(std::move(key)).second) {
        out.rows.push_back(row);
      }
    }
  };
  append_unique(left.rows);
  append_unique(right.rows);
  return out;
}

struct AggregateAccumulator {
  std::vector<double> sum;
  std::vector<std::size_t> count;
  std::vector<Value> minVal;
  std::vector<uint8_t> hasMin;
  std::vector<Value> maxVal;
  std::vector<uint8_t> hasMax;
};

std::size_t reserveAggregateGroupCount(const AggregateExecutionPattern& aggregate_pattern,
                                       std::size_t row_count) {
  if (aggregate_pattern.shape == AggregateExecutionShape::GenericNoKey) {
    return 1;
  }
  if (aggregate_pattern.exec_spec.expected_groups != 0) {
    return std::max<std::size_t>(aggregate_pattern.exec_spec.expected_groups, 64);
  }
  if (aggregate_pattern.exec_spec.properties.low_cardinality) {
    return std::min<std::size_t>(row_count, 4096);
  }
  return std::min<std::size_t>(row_count, 16384);
}

struct Int64AggregateKey {
  bool is_null = true;
  int64_t value = 0;

  bool operator==(const Int64AggregateKey& other) const {
    return is_null == other.is_null && value == other.value;
  }
};

struct Int64AggregateKeyHash {
  std::size_t operator()(const Int64AggregateKey& key) const {
    std::size_t seed = std::hash<bool>{}(key.is_null);
    seed ^= std::hash<int64_t>{}(key.value) + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
    return seed;
  }
};

struct Int64PairAggregateKey {
  Int64AggregateKey first;
  Int64AggregateKey second;

  bool operator==(const Int64PairAggregateKey& other) const {
    return first == other.first && second == other.second;
  }
};

struct Int64PairAggregateKeyHash {
  std::size_t operator()(const Int64PairAggregateKey& key) const {
    std::size_t seed = Int64AggregateKeyHash{}(key.first);
    const auto second_hash = Int64AggregateKeyHash{}(key.second);
    seed ^= second_hash + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
    return seed;
  }
};

AggregateAccumulator initAccumulator(const std::vector<AggregateSpec>& aggs) {
  AggregateAccumulator acc;
  const auto aggregate_count = aggs.size();
  bool needs_min = false;
  bool needs_max = false;
  for (const auto& agg : aggs) {
    switch (agg.function) {
      case AggregateFunction::Sum:
      case AggregateFunction::Count:
      case AggregateFunction::Avg:
        break;
      case AggregateFunction::Min:
        needs_min = true;
        break;
      case AggregateFunction::Max:
        needs_max = true;
        break;
      }
  }
  acc.sum.assign(aggregate_count, 0.0);
  acc.count.assign(aggregate_count, 0);
  if (needs_min) {
    acc.minVal.resize(aggregate_count);
    acc.hasMin.assign(aggregate_count, static_cast<uint8_t>(0));
  }
  if (needs_max) {
    acc.maxVal.resize(aggregate_count);
    acc.hasMax.assign(aggregate_count, static_cast<uint8_t>(0));
  }
  return acc;
}

void updateAccumulator(AggregateAccumulator* acc, const std::vector<AggregateSpec>& aggs,
                       const std::vector<const ValueColumnView*>& aggregate_columns,
                       std::size_t row_index) {
  if (acc == nullptr) {
    throw std::invalid_argument("aggregate accumulator is null");
  }
  for (std::size_t i = 0; i < aggs.size(); ++i) {
    const auto& agg = aggs[i];
    if (agg.function == AggregateFunction::Count) {
      acc->count[i] += 1;
      continue;
    }
    const auto& column = *aggregate_columns[i]->buffer;
    if (valueColumnIsNullAt(column, row_index)) {
      continue;
    }
    if (agg.function == AggregateFunction::Sum || agg.function == AggregateFunction::Avg) {
      acc->sum[i] += valueColumnDoubleAt(column, row_index);
      acc->count[i] += 1;
    } else if (agg.function == AggregateFunction::Min) {
      const auto v = valueColumnValueAt(column, row_index);
      if (!acc->hasMin[i] || v < acc->minVal[i]) {
        acc->minVal[i] = v;
        acc->hasMin[i] = true;
      }
    } else if (agg.function == AggregateFunction::Max) {
      const auto v = valueColumnValueAt(column, row_index);
      if (!acc->hasMax[i] || v > acc->maxVal[i]) {
        acc->maxVal[i] = v;
        acc->hasMax[i] = true;
      }
    }
  }
}

Value finalizeAggregateValue(const AggregateAccumulator& acc, const AggregateSpec& agg,
                             std::size_t aggregate_index) {
  switch (agg.function) {
    case AggregateFunction::Sum:
      return Value(acc.sum[aggregate_index]);
    case AggregateFunction::Count:
      return Value(static_cast<int64_t>(acc.count[aggregate_index]));
    case AggregateFunction::Avg:
      return Value(acc.count[aggregate_index] == 0
                       ? 0.0
                       : acc.sum[aggregate_index] /
                             static_cast<double>(acc.count[aggregate_index]));
    case AggregateFunction::Min:
      return acc.minVal[aggregate_index];
    case AggregateFunction::Max:
      return acc.maxVal[aggregate_index];
  }
  return Value();
}

Table makeAggregateOutputSchema(const Table& input, const std::vector<size_t>& key_indices,
                                const std::vector<AggregateSpec>& aggs) {
  Table out;
  for (const auto idx : key_indices) {
    out.schema.fields.push_back(input.schema.fields[idx]);
  }
  for (const auto& agg : aggs) {
    out.schema.fields.push_back(agg.output_name);
  }
  for (std::size_t i = 0; i < out.schema.fields.size(); ++i) {
    out.schema.index[out.schema.fields[i]] = i;
  }
  return out;
}

void appendAggregateValues(ColumnarTable* cache, std::size_t* out_column_index,
                           const AggregateAccumulator& acc,
                           const std::vector<AggregateSpec>& aggs) {
  if (cache == nullptr || out_column_index == nullptr) {
    throw std::invalid_argument("appendAggregateValues requires cache and column index");
  }
  for (std::size_t i = 0; i < aggs.size(); ++i) {
    cache->columns[(*out_column_index)++].values.push_back(finalizeAggregateValue(acc, aggs[i], i));
  }
}

Table executeSingleSumNoKeyTable(const Table& input, const AggregateSpec& agg) {
  Table out = makeAggregateOutputSchema(input, {}, std::vector<AggregateSpec>{agg});
  auto cache = std::make_shared<ColumnarTable>();
  cache->schema = out.schema;
  cache->columns.resize(1);
  cache->arrow_formats.resize(1);
  if (input.rowCount() == 0) {
    cache->row_count = 0;
    out.columnar_cache = std::move(cache);
    return out;
  }
  const auto column = viewValueColumn(input, agg.value_index);
  double sum = 0.0;
  for (std::size_t row_index = 0; row_index < input.rowCount(); ++row_index) {
    if (valueColumnIsNullAt(*column.buffer, row_index)) {
      continue;
    }
    sum += valueColumnDoubleAt(*column.buffer, row_index);
  }
  cache->row_count = 1;
  cache->batch_row_counts.push_back(1);
  cache->columns[0].values.push_back(Value(sum));
  out.columnar_cache = std::move(cache);
  return out;
}

Table executeSingleSumSingleInt64KeyTable(const Table& input, std::size_t key_index,
                                          const AggregateSpec& agg) {
  const auto key_column = viewValueColumn(input, key_index);
  const auto value_column = viewValueColumn(input, agg.value_index);
  std::unordered_map<Int64AggregateKey, std::size_t, Int64AggregateKeyHash> key_to_index;
  std::vector<double> ordered_sums;
  std::vector<Int64AggregateKey> ordered_keys;
  key_to_index.reserve(input.rowCount());
  ordered_keys.reserve(input.rowCount());
  ordered_sums.reserve(input.rowCount());
  for (std::size_t row_index = 0; row_index < input.rowCount(); ++row_index) {
    const Int64AggregateKey key{valueColumnIsNullAt(*key_column.buffer, row_index),
                                valueColumnIsNullAt(*key_column.buffer, row_index)
                                    ? 0
                                    : valueColumnInt64At(*key_column.buffer, row_index)};
    auto it = key_to_index.find(key);
    if (it == key_to_index.end()) {
      ordered_keys.push_back(key);
      ordered_sums.push_back(0.0);
      const auto index = ordered_sums.size() - 1;
      it = key_to_index.emplace(key, index).first;
    }
    if (!valueColumnIsNullAt(*value_column.buffer, row_index)) {
      ordered_sums[it->second] += valueColumnDoubleAt(*value_column.buffer, row_index);
    }
  }

  Table out = makeAggregateOutputSchema(input, {key_index}, std::vector<AggregateSpec>{agg});
  auto cache = std::make_shared<ColumnarTable>();
  cache->schema = out.schema;
  cache->columns.resize(2);
  cache->arrow_formats.resize(2);
  cache->row_count = ordered_keys.size();
  cache->columns[0].values.reserve(ordered_keys.size());
  cache->columns[1].values.reserve(ordered_keys.size());
  for (std::size_t index = 0; index < ordered_keys.size(); ++index) {
    const auto& key = ordered_keys[index];
    cache->columns[0].values.push_back(key.is_null ? Value() : Value(key.value));
    cache->columns[1].values.push_back(Value(ordered_sums[index]));
  }
  if (!ordered_keys.empty()) {
    cache->batch_row_counts.push_back(ordered_keys.size());
  }
  out.columnar_cache = std::move(cache);
  return out;
}

Table executeSingleSumDoubleInt64KeyTable(const Table& input, std::size_t first_key_index,
                                          std::size_t second_key_index,
                                          const AggregateSpec& agg) {
  const auto first_key = viewValueColumn(input, first_key_index);
  const auto second_key = viewValueColumn(input, second_key_index);
  const auto value_column = viewValueColumn(input, agg.value_index);
  std::unordered_map<Int64PairAggregateKey, std::size_t, Int64PairAggregateKeyHash> key_to_index;
  std::vector<double> ordered_sums;
  std::vector<Int64PairAggregateKey> ordered_keys;
  key_to_index.reserve(input.rowCount());
  ordered_keys.reserve(input.rowCount());
  ordered_sums.reserve(input.rowCount());
  for (std::size_t row_index = 0; row_index < input.rowCount(); ++row_index) {
    const Int64PairAggregateKey key{
        {valueColumnIsNullAt(*first_key.buffer, row_index),
         valueColumnIsNullAt(*first_key.buffer, row_index)
             ? 0
             : valueColumnInt64At(*first_key.buffer, row_index)},
        {valueColumnIsNullAt(*second_key.buffer, row_index),
         valueColumnIsNullAt(*second_key.buffer, row_index)
             ? 0
             : valueColumnInt64At(*second_key.buffer, row_index)}};
    auto it = key_to_index.find(key);
    if (it == key_to_index.end()) {
      ordered_keys.push_back(key);
      ordered_sums.push_back(0.0);
      const auto index = ordered_sums.size() - 1;
      it = key_to_index.emplace(key, index).first;
    }
    if (!valueColumnIsNullAt(*value_column.buffer, row_index)) {
      ordered_sums[it->second] += valueColumnDoubleAt(*value_column.buffer, row_index);
    }
  }

  Table out = makeAggregateOutputSchema(input, {first_key_index, second_key_index},
                                        std::vector<AggregateSpec>{agg});
  auto cache = std::make_shared<ColumnarTable>();
  cache->schema = out.schema;
  cache->columns.resize(3);
  cache->arrow_formats.resize(3);
  cache->row_count = ordered_keys.size();
  for (auto& column : cache->columns) {
    column.values.reserve(ordered_keys.size());
  }
  for (std::size_t index = 0; index < ordered_keys.size(); ++index) {
    const auto& key = ordered_keys[index];
    cache->columns[0].values.push_back(key.first.is_null ? Value() : Value(key.first.value));
    cache->columns[1].values.push_back(key.second.is_null ? Value() : Value(key.second.value));
    cache->columns[2].values.push_back(Value(ordered_sums[index]));
  }
  if (!ordered_keys.empty()) {
    cache->batch_row_counts.push_back(ordered_keys.size());
  }
  out.columnar_cache = std::move(cache);
  return out;
}

bool aggregateKeysEqualAt(const Table& input, const std::vector<size_t>& key_indices,
                          std::size_t lhs_row, std::size_t rhs_row) {
  for (const auto key_index : key_indices) {
    const auto key_column = viewValueColumn(input, key_index);
    const auto lhs = valueColumnValueAt(*key_column.buffer, lhs_row);
    const auto rhs = valueColumnValueAt(*key_column.buffer, rhs_row);
    if (!(lhs == rhs)) {
      return false;
    }
  }
  return true;
}

Row buildAggregateOutputRow(const Table& input, const std::vector<size_t>& key_indices,
                            std::size_t row_index, const AggregateAccumulator& acc,
                            const std::vector<AggregateSpec>& aggs) {
  Row row;
  row.reserve(key_indices.size() + aggs.size());
  for (const auto key_index : key_indices) {
    const auto key_column = viewValueColumn(input, key_index);
    row.push_back(valueColumnValueAt(*key_column.buffer, row_index));
  }
  for (std::size_t i = 0; i < aggs.size(); ++i) {
    row.push_back(finalizeAggregateValue(acc, aggs[i], i));
  }
  return row;
}

Table executeOrderedAggregateTable(const Table& input, const std::vector<size_t>& key_indices,
                                   const std::vector<AggregateSpec>& aggs,
                                   const std::vector<const ValueColumnView*>& aggregate_columns) {
  Table out = makeAggregateOutputSchema(input, key_indices, aggs);
  if (input.rowCount() == 0) {
    return out;
  }

  AggregateAccumulator accumulator = initAccumulator(aggs);
  std::size_t group_start = 0;
  for (std::size_t row_index = 0; row_index < input.rowCount(); ++row_index) {
    if (row_index > group_start && !aggregateKeysEqualAt(input, key_indices, group_start, row_index)) {
      out.rows.push_back(buildAggregateOutputRow(input, key_indices, group_start, accumulator, aggs));
      accumulator = initAccumulator(aggs);
      group_start = row_index;
    }
    updateAccumulator(&accumulator, aggs, aggregate_columns, row_index);
  }
  out.rows.push_back(buildAggregateOutputRow(input, key_indices, group_start, accumulator, aggs));
  return out;
}

Table executeDenseSingleInt64KeyAggregateTable(
    const Table& input, std::size_t key_index, const std::vector<AggregateSpec>& aggs,
    const std::vector<const ValueColumnView*>& aggregate_columns) {
  Table out = makeAggregateOutputSchema(input, {key_index}, aggs);
  if (input.rowCount() == 0) {
    return out;
  }

  const auto key_column = viewValueColumn(input, key_index);
  int64_t min_value = std::numeric_limits<int64_t>::max();
  int64_t max_value = std::numeric_limits<int64_t>::min();
  bool saw_value = false;
  for (std::size_t row_index = 0; row_index < input.rowCount(); ++row_index) {
    if (valueColumnIsNullAt(*key_column.buffer, row_index)) {
      return Table();
    }
    const auto value = valueColumnInt64At(*key_column.buffer, row_index);
    min_value = std::min(min_value, value);
    max_value = std::max(max_value, value);
    saw_value = true;
  }
  if (!saw_value) {
    return out;
  }

  const uint64_t domain_size = static_cast<uint64_t>(max_value - min_value) + 1;
  if (domain_size > (1u << 16)) {
    return Table();
  }

  std::vector<AggregateAccumulator> slots(static_cast<std::size_t>(domain_size), initAccumulator(aggs));
  std::vector<uint8_t> seen(static_cast<std::size_t>(domain_size), 0);
  for (std::size_t row_index = 0; row_index < input.rowCount(); ++row_index) {
    const auto value = valueColumnInt64At(*key_column.buffer, row_index);
    const auto slot = static_cast<std::size_t>(value - min_value);
    seen[slot] = 1;
    updateAccumulator(&slots[slot], aggs, aggregate_columns, row_index);
  }

  out.rows.reserve(input.rowCount());
  for (std::size_t slot = 0; slot < seen.size(); ++slot) {
    if (!seen[slot]) continue;
    Row row;
    row.reserve(1 + aggs.size());
    row.emplace_back(static_cast<int64_t>(min_value + static_cast<int64_t>(slot)));
    for (std::size_t i = 0; i < aggs.size(); ++i) {
      row.push_back(finalizeAggregateValue(slots[slot], aggs[i], i));
    }
    out.rows.push_back(std::move(row));
  }
  return out;
}

RowSelection combinedFilterSelection(const Table& input,
                                     const std::vector<SourceFilterPushdownSpec>& filters,
                                     std::size_t max_selected = 0) {
  RowSelection out;
  const auto row_count = input.rowCount();
  out.input_row_count = row_count;
  out.selected.assign(row_count, filters.empty() ? 0 : 1);
  out.indices.reserve(max_selected == 0 ? row_count : max_selected);
  out.selected_count = filters.empty() ? 0 : row_count;
  if (filters.empty()) {
    return out;
  }
  if (filters.size() == 1) {
    return vectorizedFilterSelection(viewValueColumn(input, filters.front().column_index),
                                     filters.front().value, filters.front().op, max_selected);
  }
  if (filters.size() > 1) {
    std::vector<ValueColumnView> filter_columns;
    std::vector<BoundFilter> bound_filters;
    filter_columns.reserve(filters.size());
    bound_filters.reserve(filters.size());
    for (const auto& filter : filters) {
      filter_columns.push_back(viewValueColumn(input, filter.column_index));
      bound_filters.push_back(
          BoundFilter{filter_columns.back().buffer, &filter.value, parseFilterCompareOp(filter.op)});
    }
    out.selected.assign(row_count, 0);
    out.selected_count = 0;
    for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
      if (!rowMatchesAllFilters(bound_filters, row_index)) {
        continue;
      }
      out.selected[row_index] = 1;
      out.indices.push_back(row_index);
      ++out.selected_count;
      if (max_selected != 0 && out.selected_count >= max_selected) {
        break;
      }
    }
    return out;
  }
  return out;
}

std::vector<std::size_t> allColumnIndices(const Schema& schema) {
  std::vector<std::size_t> indices;
  indices.reserve(schema.fields.size());
  for (std::size_t i = 0; i < schema.fields.size(); ++i) {
    indices.push_back(i);
  }
  return indices;
}

std::vector<std::size_t> allColumnIndices(std::size_t width) {
  std::vector<std::size_t> indices;
  indices.reserve(width);
  for (std::size_t i = 0; i < width; ++i) {
    indices.push_back(i);
  }
  return indices;
}

struct BorrowedOrOwnedTable {
  const Table* table = nullptr;
  std::shared_ptr<Table> retained;
};

using SourceRequirementMap = std::unordered_map<const SourcePlan*, std::vector<std::size_t>>;

void collectPredicateColumns(const std::shared_ptr<PlanPredicateExpr>& expr,
                             std::vector<std::size_t>* columns);
bool predicateHasColumnComparison(const std::shared_ptr<PlanPredicateExpr>& expr);

std::vector<std::size_t> normalizeRequiredColumns(std::vector<std::size_t> indices) {
  std::sort(indices.begin(), indices.end());
  indices.erase(std::unique(indices.begin(), indices.end()), indices.end());
  return indices;
}

void appendRequiredColumn(std::vector<std::size_t>* target, std::size_t column_index) {
  if (target == nullptr) {
    throw std::invalid_argument("required column target is null");
  }
  target->push_back(column_index);
}

void appendRequiredColumns(std::vector<std::size_t>* target,
                           const std::vector<std::size_t>& columns) {
  if (target == nullptr) {
    throw std::invalid_argument("required column target is null");
  }
  target->insert(target->end(), columns.begin(), columns.end());
}

void collectComputedArgColumns(const ComputedColumnArg& arg,
                               std::vector<std::size_t>* columns) {
  if (columns == nullptr || arg.is_literal) {
    return;
  }
  if (arg.is_function) {
    for (const auto& nested : arg.args) {
      collectComputedArgColumns(nested, columns);
    }
    return;
  }
  columns->push_back(arg.source_column_index);
}

std::size_t planOutputWidth(const PlanNodePtr& plan) {
  if (!plan) {
    return 0;
  }
  switch (plan->kind) {
    case PlanKind::Source:
      return static_cast<const SourcePlan*>(plan.get())->schema.fields.size();
    case PlanKind::Select:
      return static_cast<const SelectPlan*>(plan.get())->indices.size();
    case PlanKind::Filter:
      return planOutputWidth(static_cast<const FilterPlan*>(plan.get())->child);
    case PlanKind::Union:
      return planOutputWidth(static_cast<const UnionPlan*>(plan.get())->left);
    case PlanKind::WithColumn:
      return planOutputWidth(static_cast<const WithColumnPlan*>(plan.get())->child) + 1;
    case PlanKind::Drop:
      return static_cast<const DropPlan*>(plan.get())->keep_indices.size();
    case PlanKind::Limit:
      return planOutputWidth(static_cast<const LimitPlan*>(plan.get())->child);
    case PlanKind::OrderBy:
      return planOutputWidth(static_cast<const OrderByPlan*>(plan.get())->child);
    case PlanKind::WindowAssign:
      return planOutputWidth(static_cast<const WindowAssignPlan*>(plan.get())->child) + 1;
    case PlanKind::Aggregate: {
      const auto* node = static_cast<const AggregatePlan*>(plan.get());
      return node->keys.size() + node->aggregates.size();
    }
    case PlanKind::Join: {
      const auto* node = static_cast<const JoinPlan*>(plan.get());
      return planOutputWidth(node->left) + planOutputWidth(node->right);
    }
    case PlanKind::Sink:
      return planOutputWidth(static_cast<const SinkPlan*>(plan.get())->child);
  }
  return 0;
}

void collectSourceRequirements(const PlanNodePtr& plan, const std::vector<std::size_t>& required_output,
                               SourceRequirementMap* requirements) {
  if (!plan || requirements == nullptr) {
    return;
  }
  switch (plan->kind) {
    case PlanKind::Source: {
      auto* node = static_cast<const SourcePlan*>(plan.get());
      auto& required = (*requirements)[node];
      appendRequiredColumns(&required, required_output);
      required = normalizeRequiredColumns(std::move(required));
      return;
    }
    case PlanKind::Select: {
      const auto* node = static_cast<const SelectPlan*>(plan.get());
      std::vector<std::size_t> child_required;
      child_required.reserve(required_output.size());
      for (const auto index : required_output) {
        if (index < node->indices.size()) {
          child_required.push_back(node->indices[index]);
        }
      }
      collectSourceRequirements(node->child, normalizeRequiredColumns(std::move(child_required)),
                                requirements);
      return;
    }
    case PlanKind::Filter: {
      const auto* node = static_cast<const FilterPlan*>(plan.get());
      auto child_required = required_output;
      collectPredicateColumns(node->predicate, &child_required);
      collectSourceRequirements(node->child, normalizeRequiredColumns(std::move(child_required)),
                                requirements);
      return;
    }
    case PlanKind::Union: {
      const auto* node = static_cast<const UnionPlan*>(plan.get());
      collectSourceRequirements(node->left, required_output, requirements);
      collectSourceRequirements(node->right, required_output, requirements);
      return;
    }
    case PlanKind::WithColumn: {
      const auto* node = static_cast<const WithColumnPlan*>(plan.get());
      const auto child_width = planOutputWidth(node->child);
      std::vector<std::size_t> child_required;
      child_required.reserve(required_output.size() + node->args.size() + 1);
      bool needs_added_column = false;
      for (const auto index : required_output) {
        if (index < child_width) {
          child_required.push_back(index);
        } else if (index == child_width) {
          needs_added_column = true;
        }
      }
      if (needs_added_column) {
        if (node->function == ComputedColumnKind::Copy) {
          appendRequiredColumn(&child_required, node->source_column_index);
        } else {
          for (const auto& arg : node->args) {
            collectComputedArgColumns(arg, &child_required);
          }
        }
      }
      collectSourceRequirements(node->child, normalizeRequiredColumns(std::move(child_required)),
                                requirements);
      return;
    }
    case PlanKind::Drop: {
      const auto* node = static_cast<const DropPlan*>(plan.get());
      std::vector<std::size_t> child_required;
      child_required.reserve(required_output.size());
      for (const auto index : required_output) {
        if (index < node->keep_indices.size()) {
          child_required.push_back(node->keep_indices[index]);
        }
      }
      collectSourceRequirements(node->child, normalizeRequiredColumns(std::move(child_required)),
                                requirements);
      return;
    }
    case PlanKind::Limit: {
      const auto* node = static_cast<const LimitPlan*>(plan.get());
      collectSourceRequirements(node->child, required_output, requirements);
      return;
    }
    case PlanKind::OrderBy: {
      const auto* node = static_cast<const OrderByPlan*>(plan.get());
      auto child_required = required_output;
      appendRequiredColumns(&child_required, node->indices);
      collectSourceRequirements(node->child, normalizeRequiredColumns(std::move(child_required)),
                                requirements);
      return;
    }
    case PlanKind::WindowAssign: {
      const auto* node = static_cast<const WindowAssignPlan*>(plan.get());
      const auto child_width = planOutputWidth(node->child);
      std::vector<std::size_t> child_required;
      child_required.reserve(required_output.size() + 1);
      bool needs_window_output = false;
      for (const auto index : required_output) {
        if (index < child_width) {
          child_required.push_back(index);
        } else if (index == child_width) {
          needs_window_output = true;
        }
      }
      if (needs_window_output) {
        appendRequiredColumn(&child_required, node->time_column_index);
      }
      collectSourceRequirements(node->child, normalizeRequiredColumns(std::move(child_required)),
                                requirements);
      return;
    }
    case PlanKind::Aggregate: {
      const auto* node = static_cast<const AggregatePlan*>(plan.get());
      std::vector<std::size_t> child_required = node->keys;
      for (const auto& aggregate : node->aggregates) {
        if (aggregate.function != AggregateFunction::Count) {
          appendRequiredColumn(&child_required, aggregate.value_index);
        }
      }
      collectSourceRequirements(node->child, normalizeRequiredColumns(std::move(child_required)),
                                requirements);
      return;
    }
    case PlanKind::Join: {
      const auto* node = static_cast<const JoinPlan*>(plan.get());
      const auto left_width = planOutputWidth(node->left);
      std::vector<std::size_t> left_required;
      std::vector<std::size_t> right_required;
      left_required.reserve(required_output.size() + 1);
      right_required.reserve(required_output.size() + 1);
      for (const auto index : required_output) {
        if (index < left_width) {
          left_required.push_back(index);
        } else {
          right_required.push_back(index - left_width);
        }
      }
      appendRequiredColumn(&left_required, node->left_key);
      appendRequiredColumn(&right_required, node->right_key);
      collectSourceRequirements(node->left, normalizeRequiredColumns(std::move(left_required)),
                                requirements);
      collectSourceRequirements(node->right, normalizeRequiredColumns(std::move(right_required)),
                                requirements);
      return;
    }
    case PlanKind::Sink: {
      const auto* node = static_cast<const SinkPlan*>(plan.get());
      collectSourceRequirements(node->child, required_output, requirements);
      return;
    }
  }
}

bool cacheCoversProjectedColumns(const std::vector<std::size_t>& cached_columns,
                                 const std::vector<std::size_t>& required_columns) {
  if (required_columns.empty()) {
    return true;
  }
  for (const auto index : required_columns) {
    if (!std::binary_search(cached_columns.begin(), cached_columns.end(), index)) {
      return false;
    }
  }
  return true;
}

bool sourceCacheCoversPushdown(const SourcePlan& node, const SourcePushdownSpec& pushdown) {
  std::lock_guard<std::mutex> lock(node.cached_table_mu);
  if (!node.cached_table) {
    return false;
  }
  return cacheCoversProjectedColumns(node.cached_projected_indices, pushdown.projected_columns);
}

bool shouldUseSourcePushdown(const SourcePlan& node, const SourcePushdownSpec& pushdown) {
  if (node.options.materialization.enabled || node.options.cache_in_memory) {
    return false;
  }
  // Once the current session already holds a compatible source snapshot, prefer
  // in-memory execution over rescanning the file through source pushdown.
  return !sourceCacheCoversPushdown(node, pushdown);
}

std::vector<std::size_t> requestedSourceColumns(const SourcePlan& node,
                                                const SourceRequirementMap& requirements) {
  auto it = requirements.find(&node);
  if (it == requirements.end() || it->second.empty()) {
    return allColumnIndices(node.schema);
  }
  return it->second;
}

bool tryExecuteSourcePushdown(const SourcePlan& node, const SourcePushdownSpec& pushdown,
                              bool materialize_rows, Table* out) {
  switch (node.storage_kind) {
    case SourceStorageKind::InMemory:
      return false;
    case SourceStorageKind::CsvFile: {
      FileSourceConnectorSpec spec;
      spec.kind = node.file_source_kind;
      spec.path = node.csv_path;
      spec.csv_delimiter = node.csv_delimiter;
      spec.line_options = node.line_options;
      spec.json_options = node.json_options;
      return execute_file_source_pushdown(spec, node.schema, pushdown, materialize_rows, out);
    }
  }
  return false;
}

std::shared_ptr<PlanPredicateExpr> makePredicateComparisonExpr(const SourceFilterPushdownSpec& filter) {
  auto expr = std::make_shared<PlanPredicateExpr>();
  expr->kind = PlanPredicateExprKind::Comparison;
  expr->comparison = filter;
  return expr;
}

std::shared_ptr<PlanPredicateExpr> makePredicateAndExpr(std::shared_ptr<PlanPredicateExpr> left,
                                                        std::shared_ptr<PlanPredicateExpr> right) {
  if (!left) return right;
  if (!right) return left;
  auto expr = std::make_shared<PlanPredicateExpr>();
  expr->kind = PlanPredicateExprKind::And;
  expr->left = std::move(left);
  expr->right = std::move(right);
  return expr;
}

std::shared_ptr<PlanPredicateExpr> predicateExprFromFilters(
    const std::vector<SourceFilterPushdownSpec>& filters) {
  std::shared_ptr<PlanPredicateExpr> expr;
  for (const auto& filter : filters) {
    expr = makePredicateAndExpr(std::move(expr), makePredicateComparisonExpr(filter));
  }
  return expr;
}

bool collectConjunctivePredicateFilters(const std::shared_ptr<PlanPredicateExpr>& expr,
                                        std::vector<SourceFilterPushdownSpec>* out) {
  if (!expr || out == nullptr) return false;
  if (expr->kind == PlanPredicateExprKind::Comparison) {
    if (expr->comparison.rhs_is_column) {
      return false;
    }
    out->push_back(expr->comparison);
    return true;
  }
  if (expr->kind != PlanPredicateExprKind::And) {
    return false;
  }
  return collectConjunctivePredicateFilters(expr->left, out) &&
         collectConjunctivePredicateFilters(expr->right, out);
}

SourcePushdownShape classifySourcePushdownShape(const SourcePushdownSpec& spec) {
  if (spec.has_aggregate) {
    if (spec.aggregate.keys.size() == 1 && spec.aggregate.aggregates.size() == 1 &&
        !spec.predicate_expr) {
      const auto& agg = spec.aggregate.aggregates.front();
      if (agg.function == AggregateFunction::Count) {
        return SourcePushdownShape::SingleKeyCount;
      }
      if (agg.function == AggregateFunction::Sum || agg.function == AggregateFunction::Avg ||
          agg.function == AggregateFunction::Min || agg.function == AggregateFunction::Max) {
        return SourcePushdownShape::SingleKeyNumericAggregate;
      }
    }
    return SourcePushdownShape::Generic;
  }
  if (!spec.filters.empty() && !spec.predicate_expr) {
    return SourcePushdownShape::ConjunctiveFilterOnly;
  }
  return SourcePushdownShape::Generic;
}

void collectPredicateColumns(const std::shared_ptr<PlanPredicateExpr>& expr,
                             std::vector<std::size_t>* columns) {
  if (!expr || columns == nullptr) {
    return;
  }
  if (expr->kind == PlanPredicateExprKind::Comparison) {
    columns->push_back(expr->comparison.column_index);
    if (expr->comparison.rhs_is_column) {
      columns->push_back(expr->comparison.rhs_column_index);
    }
    return;
  }
  collectPredicateColumns(expr->left, columns);
  collectPredicateColumns(expr->right, columns);
}

bool predicateHasColumnComparison(const std::shared_ptr<PlanPredicateExpr>& expr) {
  if (!expr) {
    return false;
  }
  if (expr->kind == PlanPredicateExprKind::Comparison) {
    return expr->comparison.rhs_is_column;
  }
  return predicateHasColumnComparison(expr->left) ||
         predicateHasColumnComparison(expr->right);
}

RowSelection intersectSelections(const RowSelection& lhs, const RowSelection& rhs) {
  if (lhs.input_row_count != rhs.input_row_count) {
    throw std::runtime_error("predicate selection input size mismatch");
  }
  RowSelection out;
  out.input_row_count = lhs.input_row_count;
  out.selected.assign(out.input_row_count, 0);
  out.indices.reserve(std::min(lhs.selected_count, rhs.selected_count));
  for (std::size_t i = 0; i < out.input_row_count; ++i) {
    if (lhs.selected[i] != 0 && rhs.selected[i] != 0) {
      out.selected[i] = 1;
      out.indices.push_back(i);
    }
  }
  out.selected_count = out.indices.size();
  return out;
}

RowSelection unionSelections(const RowSelection& lhs, const RowSelection& rhs) {
  if (lhs.input_row_count != rhs.input_row_count) {
    throw std::runtime_error("predicate selection input size mismatch");
  }
  RowSelection out;
  out.input_row_count = lhs.input_row_count;
  out.selected.assign(out.input_row_count, 0);
  out.indices.reserve(lhs.selected_count + rhs.selected_count);
  for (std::size_t i = 0; i < out.input_row_count; ++i) {
    if (lhs.selected[i] != 0 || rhs.selected[i] != 0) {
      out.selected[i] = 1;
      out.indices.push_back(i);
    }
  }
  out.selected_count = out.indices.size();
  return out;
}

RowSelection evaluatePlanPredicateExpr(const Table& input,
                                       const std::shared_ptr<PlanPredicateExpr>& expr) {
  if (!expr) {
    RowSelection out;
    out.input_row_count = input.rowCount();
    out.selected.assign(out.input_row_count, 1);
    out.indices.reserve(out.input_row_count);
    for (std::size_t i = 0; i < out.input_row_count; ++i) {
      out.indices.push_back(i);
    }
    out.selected_count = out.input_row_count;
    return out;
  }
  if (expr->kind == PlanPredicateExprKind::Comparison) {
    if (expr->comparison.rhs_is_column) {
      return vectorizedColumnCompareSelection(
          viewValueColumn(input, expr->comparison.column_index),
          viewValueColumn(input, expr->comparison.rhs_column_index),
          expr->comparison.op);
    }
    return vectorizedFilterSelection(viewValueColumn(input, expr->comparison.column_index),
                                     expr->comparison.value, expr->comparison.op);
  }
  const auto left = evaluatePlanPredicateExpr(input, expr->left);
  const auto right = evaluatePlanPredicateExpr(input, expr->right);
  if (expr->kind == PlanPredicateExprKind::And) {
    return intersectSelections(left, right);
  }
  return unionSelections(left, right);
}

bool buildSourcePushdownSpec(const PlanNodePtr& plan, const SourceRequirementMap& requirements,
                             const SourcePlan** source_out, SourcePushdownSpec* pushdown_out) {
  if (!plan || source_out == nullptr || pushdown_out == nullptr) {
    return false;
  }
  switch (plan->kind) {
    case PlanKind::Source: {
      const auto* source = static_cast<const SourcePlan*>(plan.get());
      if (source->storage_kind != SourceStorageKind::CsvFile) {
        return false;
      }
      SourcePushdownSpec spec;
      spec.projected_columns = requestedSourceColumns(*source, requirements);
      spec.shape = classifySourcePushdownShape(spec);
      *source_out = source;
      *pushdown_out = std::move(spec);
      return true;
    }
    case PlanKind::Filter: {
      const auto* node = static_cast<const FilterPlan*>(plan.get());
      if (predicateHasColumnComparison(node->predicate)) {
        return false;
      }
      SourcePushdownSpec spec;
      const SourcePlan* source = nullptr;
      if (!buildSourcePushdownSpec(node->child, requirements, &source, &spec)) {
        return false;
      }
      std::vector<SourceFilterPushdownSpec> conjunctive_filters;
      if (collectConjunctivePredicateFilters(node->predicate, &conjunctive_filters) &&
          !spec.predicate_expr) {
        spec.filters.insert(spec.filters.end(), conjunctive_filters.begin(), conjunctive_filters.end());
      } else {
        auto combined = node->predicate;
        if (spec.predicate_expr) {
          combined = makePredicateAndExpr(spec.predicate_expr, combined);
        }
        if (!spec.filters.empty()) {
          combined = makePredicateAndExpr(predicateExprFromFilters(spec.filters), combined);
          spec.filters.clear();
        }
        spec.predicate_expr = std::move(combined);
      }
      spec.shape = classifySourcePushdownShape(spec);
      *source_out = source;
      *pushdown_out = std::move(spec);
      return true;
    }
    case PlanKind::Limit: {
      const auto* node = static_cast<const LimitPlan*>(plan.get());
      SourcePushdownSpec spec;
      const SourcePlan* source = nullptr;
      if (!buildSourcePushdownSpec(node->child, requirements, &source, &spec)) {
        return false;
      }
      spec.limit = spec.limit == 0 ? node->n : std::min(spec.limit, node->n);
      spec.shape = classifySourcePushdownShape(spec);
      *source_out = source;
      *pushdown_out = std::move(spec);
      return true;
    }
    case PlanKind::Aggregate: {
      const auto* node = static_cast<const AggregatePlan*>(plan.get());
      SourcePushdownSpec spec;
      const SourcePlan* source = nullptr;
      if (!buildSourcePushdownSpec(node->child, requirements, &source, &spec)) {
        return false;
      }
      if (spec.limit != 0 || spec.has_aggregate) {
        return false;
      }
      spec.has_aggregate = true;
      spec.aggregate.keys = node->keys;
      spec.aggregate.aggregates = node->aggregates;
      spec.shape = classifySourcePushdownShape(spec);
      *source_out = source;
      *pushdown_out = std::move(spec);
      return true;
    }
    default:
      return false;
  }
}

std::shared_ptr<Table> loadSourceConnector(const SourcePlan& node,
                                           const SourceRequirementMap& requirements) {
  const auto required_columns = requestedSourceColumns(node, requirements);
  {
    std::lock_guard<std::mutex> lock(node.cached_table_mu);
    if (node.cached_table &&
        cacheCoversProjectedColumns(node.cached_projected_indices, required_columns)) {
      return node.cached_table;
    }
  }

  Table loaded;
  const bool require_all_columns = required_columns.size() >= node.schema.fields.size();
  SourcePushdownSpec pushdown;
  pushdown.projected_columns = required_columns;
  FileSourceConnectorSpec spec;
  spec.kind = node.file_source_kind;
  spec.path = node.csv_path;
  spec.csv_delimiter = node.csv_delimiter;
  spec.line_options = node.line_options;
  spec.json_options = node.json_options;
  if (node.options.materialization.enabled) {
    const auto fingerprint =
        capture_file_source_fingerprint(node.csv_path, file_source_format_name(spec),
                                        file_source_options_signature(spec));
    const std::string materialization_root =
        node.options.materialization.root.empty() ? default_source_materialization_root()
                                                  : node.options.materialization.root;
    const SourceMaterializationStore store(materialization_root,
                                           node.options.materialization.data_format);
    if (require_all_columns) {
      if (const auto entry = store.lookup(fingerprint); entry.has_value()) {
        try {
          loaded = store.load(*entry, false);
        } catch (...) {
          loaded = node.file_source_kind == FileSourceKind::Csv
                       ? load_csv(fingerprint.abs_path, node.csv_delimiter, false)
                       : (node.file_source_kind == FileSourceKind::Line
                              ? load_line_file(fingerprint.abs_path, node.line_options)
                              : load_json_file(fingerprint.abs_path, node.json_options));
        }
      } else {
        loaded = node.file_source_kind == FileSourceKind::Csv
                     ? load_csv(fingerprint.abs_path, node.csv_delimiter, false)
                     : (node.file_source_kind == FileSourceKind::Line
                            ? load_line_file(fingerprint.abs_path, node.line_options)
                            : load_json_file(fingerprint.abs_path, node.json_options));
        try {
          store.save(fingerprint, loaded);
        } catch (...) {
        }
      }
    } else {
      if (!tryExecuteSourcePushdown(node, pushdown, false, &loaded)) {
        loaded = load_csv_projected(node.csv_path, node.schema, required_columns,
                                    node.csv_delimiter, false);
      }
      if (!store.lookup(fingerprint).has_value()) {
        try {
          auto full = node.file_source_kind == FileSourceKind::Csv
                          ? load_csv(fingerprint.abs_path, node.csv_delimiter, false)
                          : (node.file_source_kind == FileSourceKind::Line
                                 ? load_line_file(fingerprint.abs_path, node.line_options)
                                 : load_json_file(fingerprint.abs_path, node.json_options));
          store.save(fingerprint, full);
        } catch (...) {
        }
      }
    }
  } else {
    if (!tryExecuteSourcePushdown(node, pushdown, false, &loaded)) {
      loaded = node.file_source_kind == FileSourceKind::Csv
                   ? (require_all_columns
                          ? load_csv(node.csv_path, node.csv_delimiter, false)
                          : load_csv_projected(node.csv_path, node.schema, required_columns,
                                               node.csv_delimiter, false))
                   : (node.file_source_kind == FileSourceKind::Line
                          ? load_line_file(node.csv_path, node.line_options)
                          : load_json_file(node.csv_path, node.json_options));
    }
  }

  auto retained = std::make_shared<Table>(std::move(loaded));
  {
    std::lock_guard<std::mutex> lock(node.cached_table_mu);
    node.cached_table = retained;
    node.cached_projected_indices = required_columns;
  }
  return retained;
}

Table executePlanWithRequirements(const LocalExecutor& executor, const PlanNodePtr& plan,
                                  const SourceRequirementMap& requirements);

BorrowedOrOwnedTable borrowOrExecute(const LocalExecutor& executor, const PlanNodePtr& plan,
                                     const SourceRequirementMap& requirements) {
  if (!plan) {
    auto owned = std::make_shared<Table>();
    return BorrowedOrOwnedTable{owned.get(), std::move(owned)};
  }
  if (plan->kind == PlanKind::Source) {
    const auto* node = static_cast<const SourcePlan*>(plan.get());
    if (node->storage_kind == SourceStorageKind::InMemory) {
      return BorrowedOrOwnedTable{&node->table, nullptr};
    }
    auto retained = loadSourceConnector(*node, requirements);
    return BorrowedOrOwnedTable{retained.get(), std::move(retained)};
  }
  const SourcePlan* pushed_source = nullptr;
  SourcePushdownSpec pushed_spec;
  if (buildSourcePushdownSpec(plan, requirements, &pushed_source, &pushed_spec) &&
      shouldUseSourcePushdown(*pushed_source, pushed_spec)) {
    Table loaded;
    if (tryExecuteSourcePushdown(*pushed_source, pushed_spec, false, &loaded)) {
      auto owned = std::make_shared<Table>(std::move(loaded));
      return BorrowedOrOwnedTable{owned.get(), std::move(owned)};
    }
  }
  auto owned = std::make_shared<Table>(executePlanWithRequirements(executor, plan, requirements));
  return BorrowedOrOwnedTable{owned.get(), std::move(owned)};
}

}  // namespace

Table executeAggregateTable(const Table& input, const std::vector<size_t>& key_indices,
                            const std::vector<AggregateSpec>& aggs) {
  return executeAggregateTable(input, key_indices, aggs, nullptr);
}

Table executeAggregateTable(const Table& input, const std::vector<size_t>& key_indices,
                            const std::vector<AggregateSpec>& aggs,
                            const AggregateExecSpec* preferred_exec_spec) {
  auto aggregate_pattern = analyzeAggregateExecution(input, key_indices, aggs);
  if (preferred_exec_spec != nullptr) {
    aggregate_pattern.exec_spec = *preferred_exec_spec;
    if (preferred_exec_spec->impl_kind == AggImplKind::SortStreaming && !key_indices.empty()) {
      aggregate_pattern.shape = AggregateExecutionShape::GenericSerializedKeys;
    } else if (preferred_exec_spec->impl_kind == AggImplKind::Dense &&
               key_indices.size() == 1) {
      aggregate_pattern.shape = AggregateExecutionShape::GenericSingleInt64Key;
    }
  }
  switch (aggregate_pattern.shape) {
    case AggregateExecutionShape::SumNoKey:
      return executeSingleSumNoKeyTable(input, aggs.front());
    case AggregateExecutionShape::SumSingleInt64Key:
      return executeSingleSumSingleInt64KeyTable(input, key_indices[0], aggs.front());
    case AggregateExecutionShape::SumDoubleInt64Key:
      return executeSingleSumDoubleInt64KeyTable(input, key_indices[0], key_indices[1],
                                                 aggs.front());
    case AggregateExecutionShape::GenericNoKey:
    case AggregateExecutionShape::GenericSingleStringKey:
    case AggregateExecutionShape::GenericSingleInt64Key:
    case AggregateExecutionShape::GenericDoubleInt64Key:
    case AggregateExecutionShape::GenericPackedKeys2:
    case AggregateExecutionShape::GenericPackedKeys3:
    case AggregateExecutionShape::GenericSerializedKeys:
      break;
  }

  std::vector<std::size_t> aggregate_value_indices;
  aggregate_value_indices.reserve(aggs.size());
  for (const auto& agg : aggs) {
    if (agg.function != AggregateFunction::Count) {
      aggregate_value_indices.push_back(agg.value_index);
    }
  }
  const auto aggregate_inputs = viewValueColumns(input, aggregate_value_indices);
  std::vector<const ValueColumnView*> aggregate_columns;
  aggregate_columns.reserve(aggs.size());
  std::size_t aggregate_input_index = 0;
  for (const auto& agg : aggs) {
    if (agg.function == AggregateFunction::Count) {
      aggregate_columns.push_back(nullptr);
    } else {
      aggregate_columns.push_back(&aggregate_inputs[aggregate_input_index++]);
    }
  }

  if (aggregate_pattern.exec_spec.impl_kind == AggImplKind::SortStreaming && !key_indices.empty()) {
    return executeOrderedAggregateTable(input, key_indices, aggs, aggregate_columns);
  }

  if (aggregate_pattern.exec_spec.impl_kind == AggImplKind::Dense && key_indices.size() == 1) {
    Table dense = executeDenseSingleInt64KeyAggregateTable(input, key_indices.front(), aggs,
                                                           aggregate_columns);
    if (!dense.schema.fields.empty() && (dense.rowCount() != 0 || input.rowCount() == 0)) {
      return dense;
    }
  }

  if (aggregate_pattern.shape == AggregateExecutionShape::GenericNoKey) {
    auto out = makeAggregateOutputSchema(input, key_indices, aggs);
    auto cache = std::make_shared<ColumnarTable>();
    cache->schema = out.schema;
    cache->columns.resize(out.schema.fields.size());
    cache->arrow_formats.resize(out.schema.fields.size());
    if (input.rowCount() > 0) {
      auto acc = initAccumulator(aggs);
      for (std::size_t row_index = 0; row_index < input.rowCount(); ++row_index) {
        updateAccumulator(&acc, aggs, aggregate_columns, row_index);
      }
      cache->row_count = 1;
      cache->batch_row_counts.push_back(1);
      std::size_t out_column_index = 0;
      appendAggregateValues(cache.get(), &out_column_index, acc, aggs);
    }
    out.columnar_cache = std::move(cache);
    return out;
  }

  if (aggregate_pattern.shape == AggregateExecutionShape::GenericSingleStringKey) {
    const auto key_column = viewValueColumn(input, key_indices[0]);
    const auto reserve_groups =
        reserveAggregateGroupCount(aggregate_pattern, input.rowCount());
    std::unordered_map<std::string_view, std::size_t, std::hash<std::string_view>> key_to_index;
    std::vector<AggregateAccumulator> ordered_states;
    std::vector<std::string_view> ordered_key_views;
    key_to_index.reserve(reserve_groups);
    ordered_key_views.reserve(reserve_groups);
    ordered_states.reserve(reserve_groups);
    for (std::size_t row_index = 0; row_index < input.rowCount(); ++row_index) {
      const auto key = singleStringKeyAt(*key_column.buffer, row_index);
      auto it = key_to_index.find(key);
      if (it == key_to_index.end()) {
        ordered_key_views.push_back(key);
        ordered_states.push_back(initAccumulator(aggs));
        const auto index = ordered_states.size() - 1;
        it = key_to_index.emplace(key, index).first;
      }
      updateAccumulator(&ordered_states[it->second], aggs, aggregate_columns, row_index);
    }

    Table out = makeAggregateOutputSchema(input, key_indices, aggs);
    auto cache = std::make_shared<ColumnarTable>();
    cache->schema = out.schema;
    cache->columns.resize(out.schema.fields.size());
    cache->arrow_formats.resize(out.schema.fields.size());
    cache->row_count = ordered_key_views.size();
    for (auto& column : cache->columns) {
      column.values.reserve(ordered_key_views.size());
    }
    for (std::size_t index = 0; index < ordered_key_views.size(); ++index) {
      const auto key_view = ordered_key_views[index];
      const auto& acc = ordered_states[index];
      std::size_t out_column_index = 0;
      cache->columns[out_column_index++].values.push_back(Value(std::string(key_view)));
      appendAggregateValues(cache.get(), &out_column_index, acc, aggs);
    }
    out.columnar_cache = std::move(cache);
    return out;
  }

  if (aggregate_pattern.shape == AggregateExecutionShape::GenericSingleInt64Key) {
    const auto key_column = viewValueColumn(input, key_indices[0]);
    const auto reserve_groups =
        reserveAggregateGroupCount(aggregate_pattern, input.rowCount());
    std::unordered_map<Int64AggregateKey, std::size_t, Int64AggregateKeyHash> key_to_index;
    std::vector<AggregateAccumulator> ordered_states;
    std::vector<Int64AggregateKey> ordered_key_values;
    key_to_index.reserve(reserve_groups);
    ordered_key_values.reserve(reserve_groups);
    ordered_states.reserve(reserve_groups);
    for (std::size_t row_index = 0; row_index < input.rowCount(); ++row_index) {
      const Int64AggregateKey key{valueColumnIsNullAt(*key_column.buffer, row_index),
                                  valueColumnIsNullAt(*key_column.buffer, row_index)
                                      ? 0
                                      : valueColumnInt64At(*key_column.buffer, row_index)};
      auto it = key_to_index.find(key);
      if (it == key_to_index.end()) {
        ordered_key_values.push_back(key);
        ordered_states.push_back(initAccumulator(aggs));
        const auto index = ordered_states.size() - 1;
        it = key_to_index.emplace(key, index).first;
      }
      updateAccumulator(&ordered_states[it->second], aggs, aggregate_columns, row_index);
    }

    Table out = makeAggregateOutputSchema(input, key_indices, aggs);
    auto cache = std::make_shared<ColumnarTable>();
    cache->schema = out.schema;
    cache->columns.resize(out.schema.fields.size());
    cache->arrow_formats.resize(out.schema.fields.size());
    cache->row_count = ordered_key_values.size();
    for (auto& column : cache->columns) {
      column.values.reserve(ordered_key_values.size());
    }
    for (std::size_t index = 0; index < ordered_key_values.size(); ++index) {
      const auto& key = ordered_key_values[index];
      const auto& acc = ordered_states[index];
      std::size_t out_column_index = 0;
      cache->columns[out_column_index++].values.push_back(key.is_null ? Value() : Value(key.value));
      appendAggregateValues(cache.get(), &out_column_index, acc, aggs);
    }
    out.columnar_cache = std::move(cache);
    return out;
  }

  if (aggregate_pattern.shape == AggregateExecutionShape::GenericDoubleInt64Key) {
    const auto first_key = viewValueColumn(input, key_indices[0]);
    const auto second_key = viewValueColumn(input, key_indices[1]);
    const auto reserve_groups =
        reserveAggregateGroupCount(aggregate_pattern, input.rowCount());
    std::unordered_map<Int64PairAggregateKey, std::size_t, Int64PairAggregateKeyHash> key_to_index;
    std::vector<AggregateAccumulator> ordered_states;
    std::vector<Int64PairAggregateKey> ordered_key_values;
    key_to_index.reserve(reserve_groups);
    ordered_key_values.reserve(reserve_groups);
    ordered_states.reserve(reserve_groups);
    for (std::size_t row_index = 0; row_index < input.rowCount(); ++row_index) {
      const Int64PairAggregateKey key{
          {valueColumnIsNullAt(*first_key.buffer, row_index),
           valueColumnIsNullAt(*first_key.buffer, row_index)
               ? 0
               : valueColumnInt64At(*first_key.buffer, row_index)},
          {valueColumnIsNullAt(*second_key.buffer, row_index),
           valueColumnIsNullAt(*second_key.buffer, row_index)
               ? 0
               : valueColumnInt64At(*second_key.buffer, row_index)}};
      auto it = key_to_index.find(key);
      if (it == key_to_index.end()) {
        ordered_key_values.push_back(key);
        ordered_states.push_back(initAccumulator(aggs));
        const auto index = ordered_states.size() - 1;
        it = key_to_index.emplace(key, index).first;
      }
      updateAccumulator(&ordered_states[it->second], aggs, aggregate_columns, row_index);
    }

    Table out = makeAggregateOutputSchema(input, key_indices, aggs);
    auto cache = std::make_shared<ColumnarTable>();
    cache->schema = out.schema;
    cache->columns.resize(out.schema.fields.size());
    cache->arrow_formats.resize(out.schema.fields.size());
    cache->row_count = ordered_key_values.size();
    for (auto& column : cache->columns) {
      column.values.reserve(ordered_key_values.size());
    }
    for (std::size_t index = 0; index < ordered_key_values.size(); ++index) {
      const auto& key = ordered_key_values[index];
      const auto& acc = ordered_states[index];
      std::size_t out_column_index = 0;
      cache->columns[out_column_index++].values.push_back(
          key.first.is_null ? Value() : Value(key.first.value));
      cache->columns[out_column_index++].values.push_back(
          key.second.is_null ? Value() : Value(key.second.value));
      appendAggregateValues(cache.get(), &out_column_index, acc, aggs);
    }
    out.columnar_cache = std::move(cache);
    return out;
  }

  struct PackedAggregateKey {
    // Up to 3 components; only first (arity) elements are meaningful.
    uint8_t arity = 0;
    std::array<uint8_t, 3> tag{};  // 0=null, 1=int64, 2=string
    std::array<int64_t, 3> i64{};
    std::array<std::string, 3> sv{};

    bool operator==(const PackedAggregateKey& other) const {
      if (arity != other.arity) return false;
      for (uint8_t i = 0; i < arity; ++i) {
        if (tag[i] != other.tag[i]) return false;
        switch (tag[i]) {
          case 0:
            break;
          case 1:
            if (i64[i] != other.i64[i]) return false;
            break;
          case 2:
            if (sv[i] != other.sv[i]) return false;
            break;
          default:
            return false;
        }
      }
      return true;
    }
  };

  struct PackedAggregateKeyHash {
    std::size_t operator()(const PackedAggregateKey& key) const noexcept {
      std::size_t h = 1469598103934665603ull;  // FNV offset
      auto mix = [&](uint64_t v) {
        h ^= static_cast<std::size_t>(v);
        h *= 1099511628211ull;
      };
      mix(key.arity);
      for (uint8_t i = 0; i < key.arity; ++i) {
        mix(key.tag[i]);
        switch (key.tag[i]) {
          case 0:
            break;
          case 1:
            mix(static_cast<uint64_t>(key.i64[i]));
            break;
          case 2: {
            const auto& sv = key.sv[i];
            mix(std::hash<std::string>{}(sv));
            break;
          }
          default:
            break;
        }
      }
      return h;
    }
  };

  auto executePackedKeys = [&](uint8_t arity) -> Table {
    const auto k0 = viewValueColumn(input, key_indices[0]);
    const auto k1 = viewValueColumn(input, key_indices[1]);
    ValueColumnView k2;
    bool has_k2 = false;
    if (arity == 3) {
      k2 = viewValueColumn(input, key_indices[2]);
      has_k2 = true;
    }

    const auto reserve_groups =
        reserveAggregateGroupCount(aggregate_pattern, input.rowCount());
    std::unordered_map<PackedAggregateKey, std::size_t, PackedAggregateKeyHash> key_to_index;
    std::vector<AggregateAccumulator> ordered_states;
    std::vector<PackedAggregateKey> ordered_keys;
    key_to_index.reserve(reserve_groups);
    ordered_states.reserve(reserve_groups);
    ordered_keys.reserve(reserve_groups);

    auto packAt = [&](const ValueColumnView& col, std::size_t row, uint8_t idx,
                      PackedAggregateKey* out_key) -> bool {
      if (valueColumnIsNullAt(*col.buffer, row)) {
        out_key->tag[idx] = 0;
        return true;
      }

      const auto& buffer = *col.buffer;
      if (!buffer.values.empty()) {
        const Value value = valueColumnValueAt(buffer, row);
        if (value.isNull()) {
          out_key->tag[idx] = 0;
          return true;
        }
        if (value.type() == DataType::Int64) {
          out_key->tag[idx] = 1;
          out_key->i64[idx] = value.asInt64();
          return true;
        }
        if (value.type() == DataType::Bool) {
          out_key->tag[idx] = 1;
          out_key->i64[idx] = value.asBool() ? 1 : 0;
          return true;
        }
        if (value.type() == DataType::String) {
          out_key->tag[idx] = 2;
          out_key->sv[idx] = value.asString();
          return true;
        }
        return false;
      }

      if (buffer.arrow_backing != nullptr) {
        const auto& format = buffer.arrow_backing->format;
        if (isArrowUtf8Format(format)) {
          out_key->tag[idx] = 2;
          out_key->sv[idx] = std::string(valueColumnStringViewAt(buffer, row));
          return true;
        }
        if (isArrowIntegerLikeFormat(format)) {
          out_key->tag[idx] = 1;
          out_key->i64[idx] = valueColumnInt64At(buffer, row);
          return true;
        }
      }

      const Value value = valueColumnValueAt(buffer, row);
      if (value.isNull()) {
        out_key->tag[idx] = 0;
        return true;
      }
      if (value.type() == DataType::Int64) {
        out_key->tag[idx] = 1;
        out_key->i64[idx] = value.asInt64();
        return true;
      }
      if (value.type() == DataType::Bool) {
        out_key->tag[idx] = 1;
        out_key->i64[idx] = value.asBool() ? 1 : 0;
        return true;
      }
      if (value.type() == DataType::String) {
        out_key->tag[idx] = 2;
        out_key->sv[idx] = value.asString();
        return true;
      }
      return false;
    };

    for (std::size_t row_index = 0; row_index < input.rowCount(); ++row_index) {
      PackedAggregateKey key;
      key.arity = arity;
      if (!packAt(k0, row_index, 0, &key) || !packAt(k1, row_index, 1, &key) ||
          (arity == 3 && (!has_k2 || !packAt(k2, row_index, 2, &key)))) {
        // Fallback to safe slow path if we hit an unsupported value type.
        return Table();
      }

      auto it = key_to_index.find(key);
      if (it == key_to_index.end()) {
        ordered_keys.push_back(key);
        ordered_states.push_back(initAccumulator(aggs));
        const auto index = ordered_states.size() - 1;
        it = key_to_index.emplace(key, index).first;
      }
      updateAccumulator(&ordered_states[it->second], aggs, aggregate_columns, row_index);
    }

    Table out = makeAggregateOutputSchema(input, key_indices, aggs);
    auto cache = std::make_shared<ColumnarTable>();
    cache->schema = out.schema;
    cache->columns.resize(out.schema.fields.size());
    cache->arrow_formats.resize(out.schema.fields.size());
    cache->row_count = ordered_keys.size();
    for (auto& column : cache->columns) {
      column.values.reserve(ordered_keys.size());
    }

    for (std::size_t index = 0; index < ordered_keys.size(); ++index) {
      const auto& key = ordered_keys[index];
      const auto& acc = ordered_states[index];
      std::size_t out_column_index = 0;
      for (uint8_t i = 0; i < arity; ++i) {
        switch (key.tag[i]) {
          case 0:
            cache->columns[out_column_index++].values.push_back(Value());
            break;
          case 1:
            cache->columns[out_column_index++].values.push_back(Value(key.i64[i]));
            break;
          case 2:
            cache->columns[out_column_index++].values.push_back(Value(key.sv[i]));
            break;
          default:
            cache->columns[out_column_index++].values.push_back(Value());
            break;
        }
      }
      appendAggregateValues(cache.get(), &out_column_index, acc, aggs);
    }
    out.columnar_cache = std::move(cache);
    return out;
  };

  if (aggregate_pattern.shape == AggregateExecutionShape::GenericPackedKeys2) {
    Table packed = executePackedKeys(2);
    if (packed.rowCount() != 0 || input.rowCount() == 0) {
      // If packed succeeded or input empty, return it.
      // executePackedKeys returns empty table only on fallback; empty input is OK.
      if (packed.schema.fields.size() != 0 || input.rowCount() == 0) {
        return packed;
      }
    }
    // else fall through to serialized key path
  }

  if (aggregate_pattern.shape == AggregateExecutionShape::GenericPackedKeys3) {
    Table packed = executePackedKeys(3);
    if (packed.rowCount() != 0 || input.rowCount() == 0) {
      if (packed.schema.fields.size() != 0 || input.rowCount() == 0) {
        return packed;
      }
    }
  }

  std::vector<std::string_view> ordered_key_views;
  std::unordered_map<std::string_view, std::size_t, std::hash<std::string_view>> key_to_index;
  std::vector<AggregateAccumulator> ordered_states;
  const auto reserve_groups =
      reserveAggregateGroupCount(aggregate_pattern, input.rowCount());
  key_to_index.reserve(reserve_groups);
  ordered_key_views.reserve(reserve_groups);
  ordered_states.reserve(reserve_groups);
  const auto serialized_keys = materializeSerializedKeys(input, key_indices);
  for (std::size_t row_index = 0; row_index < serialized_keys.size(); ++row_index) {
    const std::string_view key = serialized_keys[row_index];
    auto it = key_to_index.find(key);
    if (it == key_to_index.end()) {
      ordered_key_views.push_back(key);
      ordered_states.push_back(initAccumulator(aggs));
      const auto index = ordered_states.size() - 1;
      it = key_to_index.emplace(key, index).first;
    }
    updateAccumulator(&ordered_states[it->second], aggs, aggregate_columns, row_index);
  }

  Table out = makeAggregateOutputSchema(input, key_indices, aggs);
  auto cache = std::make_shared<ColumnarTable>();
  cache->schema = out.schema;
  cache->columns.resize(out.schema.fields.size());
  cache->arrow_formats.resize(out.schema.fields.size());
  cache->row_count = ordered_key_views.size();
  for (auto& column : cache->columns) {
    column.values.reserve(ordered_key_views.size());
  }

  for (std::size_t index = 0; index < ordered_key_views.size(); ++index) {
    const auto key = ordered_key_views[index];
    const auto& acc = ordered_states[index];
    std::size_t out_column_index = 0;
    for (const auto key_part : splitKeyViews(key)) {
      cache->columns[out_column_index++].values.push_back(Value(std::string(key_part)));
    }
    appendAggregateValues(cache.get(), &out_column_index, acc, aggs);
  }
  out.columnar_cache = std::move(cache);
  return out;
}

RunnerExecutor::RunnerExecutor(std::shared_ptr<RpcRunner> runner,
                             std::shared_ptr<Executor> fallback,
                             bool dual_path)
    : runner_(std::move(runner)),
      fallback_(std::move(fallback)),
      dual_path_(dual_path) {
  if (!fallback_) {
    fallback_ = std::make_shared<LocalExecutor>();
  }
}

Table RunnerExecutor::execute(const PlanNodePtr& plan) const {
  Table local_result = fallback_->execute(plan);
  if (!runner_ || !dual_path_) {
    return local_result;
  }
  const uint64_t message_id = next_message_id_.fetch_add(1, std::memory_order_relaxed);
  const uint64_t correlation_id = message_id;
  if (!runner_->sendControl(message_id, correlation_id, "local", "remote", RpcMessageType::Control,
                           "RunRequest", "local_execute")) {
    return local_result;
  }
  if (!runner_->sendDataBatch(message_id + 1, correlation_id, "local", "remote", local_result)) {
    return local_result;
  }
  return local_result;
}

namespace {

Table executePlanWithRequirements(const LocalExecutor& executor, const PlanNodePtr& plan,
                                  const SourceRequirementMap& requirements) {
  if (!plan) return Table();

  switch (plan->kind) {
    case PlanKind::Source: {
      const auto* node = static_cast<const SourcePlan*>(plan.get());
      if (node->storage_kind == SourceStorageKind::InMemory) {
        return node->table;
      }
      return *loadSourceConnector(*node, requirements);
    }
    case PlanKind::Select: {
      const auto* node = static_cast<const SelectPlan*>(plan.get());
      const auto input = borrowOrExecute(executor, node->child, requirements);
      return projectTable(*input.table, node->indices, node->aliases, false);
    }
    case PlanKind::Filter: {
      const auto filter_pattern = analyzePredicatePattern(plan);
      const SourcePlan* pushed_source = nullptr;
      SourcePushdownSpec pushed_spec;
      if (buildSourcePushdownSpec(plan, requirements, &pushed_source, &pushed_spec) &&
          shouldUseSourcePushdown(*pushed_source, pushed_spec)) {
        Table pushed;
        if (tryExecuteSourcePushdown(*pushed_source, pushed_spec, false, &pushed)) {
          return pushed;
        }
      }
      const auto child_input = borrowOrExecute(executor, filter_pattern.base_child, requirements);
      const auto selection = filter_pattern.is_conjunctive
                                 ? combinedFilterSelection(*child_input.table,
                                                           filter_pattern.conjunctive_filters)
                                 : evaluatePlanPredicateExpr(*child_input.table,
                                                             filter_pattern.predicate);
      return filterTable(*child_input.table, selection, false);
    }
    case PlanKind::Union: {
      const auto* node = static_cast<const UnionPlan*>(plan.get());
      const auto left_input = borrowOrExecute(executor, node->left, requirements);
      const auto right_input = borrowOrExecute(executor, node->right, requirements);
      return executeUnionTable(*left_input.table, *right_input.table, node->distinct);
    }
    case PlanKind::WithColumn: {
      const auto* node = static_cast<const WithColumnPlan*>(plan.get());
      const auto table_input = borrowOrExecute(executor, node->child, requirements);
      Table out = *table_input.table;
      if (node->function == ComputedColumnKind::Copy) {
        appendNamedColumn(&out, node->added_column,
                          materializeValueColumn(out, node->source_column_index), false);
      } else {
        appendNamedColumn(&out, node->added_column,
                          computeComputedColumnValues(&out, node->function, node->args), false);
      }
      return out;
    }
    case PlanKind::Drop: {
      const auto* node = static_cast<const DropPlan*>(plan.get());
      const auto input = borrowOrExecute(executor, node->child, requirements);
      return projectTable(*input.table, node->keep_indices, {}, false);
    }
    case PlanKind::Limit: {
      const auto* node = static_cast<const LimitPlan*>(plan.get());
      const SourcePlan* pushed_source = nullptr;
      SourcePushdownSpec pushed_spec;
      if (buildSourcePushdownSpec(plan, requirements, &pushed_source, &pushed_spec) &&
          shouldUseSourcePushdown(*pushed_source, pushed_spec)) {
        Table pushed;
        if (tryExecuteSourcePushdown(*pushed_source, pushed_spec, false, &pushed)) {
          return pushed;
        }
      }
      const auto limit_pattern = analyzeLimitExecution(*node);
      if (limit_pattern.use_predicate_filter) {
        const auto table_input =
            borrowOrExecute(executor, limit_pattern.predicate_filter.base_child, requirements);
        if (limit_pattern.predicate_filter.is_conjunctive) {
          const auto selection = combinedFilterSelection(
              *table_input.table, limit_pattern.predicate_filter.conjunctive_filters, node->n);
          return filterTable(*table_input.table, selection, false);
        }
        auto filtered = filterTable(*table_input.table,
                                    evaluatePlanPredicateExpr(
                                        *table_input.table, limit_pattern.predicate_filter.predicate),
                                    false);
        return limitTable(filtered, node->n, false);
      }
      if (limit_pattern.use_topn) {
        const auto input = borrowOrExecute(executor, limit_pattern.order_by->child, requirements);
        return topNTable(*input.table, limit_pattern.order_by->indices,
                         limit_pattern.order_by->ascending, node->n, false);
      }
      const auto input = borrowOrExecute(executor, node->child, requirements);
      return limitTable(*input.table, node->n, false);
    }
    case PlanKind::OrderBy: {
      const auto* node = static_cast<const OrderByPlan*>(plan.get());
      const auto input = borrowOrExecute(executor, node->child, requirements);
      Table in = *input.table;
      return sortTable(in, node->indices, node->ascending);
    }
    case PlanKind::WindowAssign: {
      const auto* node = static_cast<const WindowAssignPlan*>(plan.get());
      const auto table_input = borrowOrExecute(executor, node->child, requirements);
      return assignSlidingWindow(*table_input.table, node->time_column_index, node->window_ms,
                                 node->slide_ms, node->output_column, false, false);
    }
    case PlanKind::Aggregate: {
      const SourcePlan* pushed_source = nullptr;
      SourcePushdownSpec pushed_spec;
      if (buildSourcePushdownSpec(plan, requirements, &pushed_source, &pushed_spec) &&
          shouldUseSourcePushdown(*pushed_source, pushed_spec)) {
        Table pushed;
        if (tryExecuteSourcePushdown(*pushed_source, pushed_spec, false, &pushed)) {
          return pushed;
        }
      }
      const auto* node = static_cast<const AggregatePlan*>(plan.get());
      const auto input = borrowOrExecute(executor, node->child, requirements);
      const auto pattern = analyzeAggregateExecution(*input.table, node->keys, node->aggregates);
      auto* mutable_node = const_cast<AggregatePlan*>(node);
      if (!mutable_node->has_exec_spec) {
        mutable_node->exec_spec = pattern.exec_spec;
        mutable_node->has_exec_spec = true;
      }
      mutable_node->runtime_feedback = pattern.feedback;
      Table out = executeAggregateTable(*input.table, node->keys, node->aggregates,
                                       mutable_node->has_exec_spec ? &mutable_node->exec_spec
                                                                   : nullptr);
      mutable_node->runtime_feedback.input_rows = input.table->rowCount();
      mutable_node->runtime_feedback.output_groups = out.rowCount();
      mutable_node->runtime_feedback.observed_ordered_input =
          pattern.exec_spec.properties.ordered_input;
      mutable_node->has_runtime_feedback = true;
      return out;
    }
    case PlanKind::Join: {
      const auto* node = static_cast<const JoinPlan*>(plan.get());
      const auto left_input = borrowOrExecute(executor, node->left, requirements);
      const auto right_input = borrowOrExecute(executor, node->right, requirements);
      const auto left_keys = materializeSerializedKeys(*left_input.table, {node->left_key});
      const auto right_keys = materializeSerializedKeys(*right_input.table, {node->right_key});
      const auto left_columns = viewValueColumns(*left_input.table, allColumnIndices(left_input.table->schema));
      const auto right_columns =
          viewValueColumns(*right_input.table, allColumnIndices(right_input.table->schema));
      Table out;
      out.schema.fields = left_input.table->schema.fields;
      out.schema.fields.insert(out.schema.fields.end(), right_input.table->schema.fields.begin(),
                               right_input.table->schema.fields.end());
      for (size_t i = 0; i < out.schema.fields.size(); ++i) out.schema.index[out.schema.fields[i]] = i;
      auto cache = std::make_shared<ColumnarTable>();
      cache->schema = out.schema;
      cache->columns.resize(out.schema.fields.size());
      cache->arrow_formats.resize(out.schema.fields.size());
      std::size_t output_row_count = 0;

      const auto rightBuckets = buildHashBuckets(right_keys);

      for (std::size_t left_index = 0; left_index < left_input.table->rowCount(); ++left_index) {
        auto hit = rightBuckets.find(left_keys[left_index]);
        if (hit != rightBuckets.end()) {
          for (const auto right_index : hit->second) {
            std::size_t out_column_index = 0;
            for (const auto& column : left_columns) {
              const auto value = valueColumnValueAt(*column.buffer, left_index);
              cache->columns[out_column_index++].values.push_back(value);
            }
            for (const auto& column : right_columns) {
              const auto value = valueColumnValueAt(*column.buffer, right_index);
              cache->columns[out_column_index++].values.push_back(value);
            }
            ++output_row_count;
          }
        } else if (node->kind == JoinKind::Left) {
          std::size_t out_column_index = 0;
          for (const auto& column : left_columns) {
            const auto value = valueColumnValueAt(*column.buffer, left_index);
            cache->columns[out_column_index++].values.push_back(value);
          }
          for (size_t i = 0; i < right_input.table->schema.fields.size(); ++i) {
            Value value;
            cache->columns[out_column_index++].values.push_back(std::move(value));
          }
          ++output_row_count;
        }
      }
      cache->row_count = output_row_count;
      if (output_row_count > 0) {
        cache->batch_row_counts.push_back(output_row_count);
      }
      out.columnar_cache = std::move(cache);
      return out;
    }
    case PlanKind::Sink: {
      const auto* node = static_cast<const SinkPlan*>(plan.get());
      return executePlanWithRequirements(executor, node->child, requirements);
    }
    default:
      return Table();
  }
}

}  // namespace

Table LocalExecutor::execute(const PlanNodePtr& plan) const {
  SourceRequirementMap requirements;
  collectSourceRequirements(plan, allColumnIndices(planOutputWidth(plan)), &requirements);
  return executePlanWithRequirements(*this, plan, requirements);
}

}  // namespace dataflow
