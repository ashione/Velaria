#include "src/dataflow/experimental/stream/actor_stream_runtime.h"

#include <chrono>
#include <cstdint>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <string>
#include <atomic>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <unistd.h>
#include <utility>
#include <vector>

#include "src/dataflow/core/contract/api/dataframe.h"
#include "src/dataflow/core/execution/arrow_format.h"
#include "src/dataflow/core/execution/columnar_batch.h"
#include "src/dataflow/core/execution/nanoarrow_ipc_codec.h"
#include "src/dataflow/core/execution/runtime/aggregate_layout.h"
#include "src/dataflow/core/execution/stream/binary_row_batch.h"
#include "src/dataflow/experimental/stream/actor_execution_optimizer.h"
#include "src/dataflow/experimental/transport/ipc_transport.h"

namespace dataflow {

namespace {

constexpr char kKeySep = '\x1f';
constexpr uint8_t kRequestKind = 1;
constexpr uint8_t kResponseKind = 2;
constexpr uint8_t kEnvelopeMagic[4] = {'A', 'S', 'B', '1'};
constexpr uint8_t kPayloadInline = 0;
constexpr uint8_t kPayloadSharedMemory = 1;

uint64_t toMillis(std::chrono::steady_clock::duration value) {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(value).count());
}

double toRowsPerSecond(const LocalActorStreamResult& result, size_t rows_per_batch) {
  if (result.elapsed_ms == 0 || result.processed_batches == 0) return 0.0;
  return static_cast<double>(result.processed_batches * rows_per_batch) /
         (static_cast<double>(result.elapsed_ms) / 1000.0);
}

uint64_t actorOverheadMs(const LocalActorStreamResult& result) {
  return result.coordinator_serialize_ms + result.coordinator_wait_ms +
         result.coordinator_deserialize_ms + result.coordinator_merge_ms +
         result.worker_deserialize_ms + result.worker_serialize_ms;
}

std::string partialValueColumnName(const std::string& output_column) {
  return "__partial_" + output_column;
}

std::string partialAverageSumColumnName(const std::string& output_column) {
  return "__partial_sum_" + output_column;
}

std::string partialAverageCountColumnName(const std::string& output_column) {
  return "__partial_count_" + output_column;
}

std::string stateAverageSumColumnName(const std::string& output_column) {
  return "__state_sum_" + output_column;
}

std::string stateAverageCountColumnName(const std::string& output_column) {
  return "__state_count_" + output_column;
}

struct RuntimeAggregateLayout {
  LocalGroupedAggregateSpec::Aggregate aggregate;
  std::string partial_value_column;
  std::string partial_count_column;
};

using WindowKeySumState = AggregateStringKeyState;

struct Int64WindowKey {
  bool window_is_null = true;
  int64_t window = 0;
  bool key_is_null = true;
  int64_t key = 0;

  bool operator==(const Int64WindowKey& other) const {
    return window_is_null == other.window_is_null && window == other.window &&
           key_is_null == other.key_is_null && key == other.key;
  }
};

struct Int64WindowKeyHash {
  std::size_t operator()(const Int64WindowKey& value) const {
    std::size_t seed = std::hash<bool>{}(value.window_is_null);
    seed ^= std::hash<int64_t>{}(value.window) + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
    seed ^= std::hash<bool>{}(value.key_is_null) + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
    seed ^= std::hash<int64_t>{}(value.key) + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
    return seed;
  }
};

using Int64TwoKeySumState = AggregateFixedKeyState;

double spinValue(double value, size_t cpu_spin_per_row);
std::string_view stringAt(const BinaryKeyColumn& column, size_t row_idx);
Value binaryKeyValueAt(const BinaryKeyColumn& column, size_t row_idx);

Table aggregatePartitionWithWork(const Table& input, const LocalGroupedAggregateSpec& aggregate,
                                 size_t cpu_spin_per_row);
Table aggregateInt64WindowKeySumPartial(const Table& input, const LocalGroupedAggregateSpec& aggregate,
                                        size_t cpu_spin_per_row);

bool isBinaryRowBatchPayload(const uint8_t* payload, std::size_t size) {
  static constexpr uint8_t kMagic[4] = {'B', 'R', 'B', '1'};
  return payload != nullptr && size >= 4 && std::memcmp(payload, kMagic, 4) == 0;
}

std::string encodeGenericTwoKeyStateKey(const Value& first_value, const Value& second_value) {
  return std::to_string(static_cast<int>(first_value.type())) + kKeySep +
         first_value.toString() + kKeySep +
         std::to_string(static_cast<int>(second_value.type())) + kKeySep +
         second_value.toString();
}

std::vector<size_t> resolveGroupKeyIndices(const Table& input,
                                           const std::vector<std::string>& group_keys) {
  std::vector<size_t> indices;
  indices.reserve(group_keys.size());
  for (const auto& key : group_keys) {
    indices.push_back(input.schema.indexOf(key));
  }
  return indices;
}

std::vector<AggregateSpec> buildRuntimePartialAggregateSpecs(
    const Table& input, const LocalGroupedAggregateSpec& aggregate,
    std::vector<RuntimeAggregateLayout>* layouts) {
  if (layouts == nullptr) {
    throw std::invalid_argument("layouts cannot be null");
  }
  layouts->clear();

  std::vector<AggregateSpec> specs;
  specs.reserve(aggregate.aggregates.size() * 2);
  for (const auto& aggregate_spec : aggregate.aggregates) {
    RuntimeAggregateLayout layout;
    layout.aggregate = aggregate_spec;
    switch (aggregate_spec.function) {
      case AggregateFunction::Sum:
      case AggregateFunction::Min:
      case AggregateFunction::Max: {
        layout.partial_value_column = partialValueColumnName(aggregate_spec.output_column);
        specs.push_back(AggregateSpec{aggregate_spec.function,
                                      input.schema.indexOf(aggregate_spec.value_column),
                                      layout.partial_value_column});
        break;
      }
      case AggregateFunction::Count: {
        layout.partial_count_column = partialValueColumnName(aggregate_spec.output_column);
        specs.push_back(AggregateSpec{AggregateFunction::Count, static_cast<size_t>(-1),
                                      layout.partial_count_column});
        break;
      }
      case AggregateFunction::Avg: {
        layout.partial_value_column = partialAverageSumColumnName(aggregate_spec.output_column);
        layout.partial_count_column = partialAverageCountColumnName(aggregate_spec.output_column);
        specs.push_back(AggregateSpec{AggregateFunction::Sum,
                                      input.schema.indexOf(aggregate_spec.value_column),
                                      layout.partial_value_column});
        specs.push_back(AggregateSpec{AggregateFunction::Count, static_cast<size_t>(-1),
                                      layout.partial_count_column});
        break;
      }
    }
    layouts->push_back(std::move(layout));
  }
  return specs;
}

std::vector<AggregateSpec> buildRuntimeMergeAggregateSpecs(
    const Table& partials, const std::vector<RuntimeAggregateLayout>& layouts) {
  std::vector<AggregateSpec> specs;
  specs.reserve(layouts.size() * 2);
  for (const auto& layout : layouts) {
    switch (layout.aggregate.function) {
      case AggregateFunction::Sum:
        specs.push_back(AggregateSpec{AggregateFunction::Sum,
                                      partials.schema.indexOf(layout.partial_value_column),
                                      layout.partial_value_column});
        break;
      case AggregateFunction::Count:
        specs.push_back(AggregateSpec{AggregateFunction::Sum,
                                      partials.schema.indexOf(layout.partial_count_column),
                                      layout.partial_count_column});
        break;
      case AggregateFunction::Min:
        specs.push_back(AggregateSpec{AggregateFunction::Min,
                                      partials.schema.indexOf(layout.partial_value_column),
                                      layout.partial_value_column});
        break;
      case AggregateFunction::Max:
        specs.push_back(AggregateSpec{AggregateFunction::Max,
                                      partials.schema.indexOf(layout.partial_value_column),
                                      layout.partial_value_column});
        break;
      case AggregateFunction::Avg:
        specs.push_back(AggregateSpec{AggregateFunction::Sum,
                                      partials.schema.indexOf(layout.partial_value_column),
                                      layout.partial_value_column});
        specs.push_back(AggregateSpec{AggregateFunction::Sum,
                                      partials.schema.indexOf(layout.partial_count_column),
                                      layout.partial_count_column});
        break;
    }
  }
  return specs;
}

Table makeAggregateOutputSkeleton(const LocalGroupedAggregateSpec& aggregate,
                                  bool include_avg_state_helpers) {
  Table table;
  table.schema.fields = aggregate.group_keys;
  for (const auto& aggregate_spec : aggregate.aggregates) {
    table.schema.fields.push_back(aggregate_spec.output_column);
    if (include_avg_state_helpers && aggregate_spec.function == AggregateFunction::Avg) {
      table.schema.fields.push_back(stateAverageSumColumnName(aggregate_spec.output_column));
      table.schema.fields.push_back(stateAverageCountColumnName(aggregate_spec.output_column));
    }
  }
  for (size_t i = 0; i < table.schema.fields.size(); ++i) {
    table.schema.index[table.schema.fields[i]] = i;
  }
  return table;
}

Table applyCpuSpin(Table input, const LocalGroupedAggregateSpec& aggregate, size_t cpu_spin_per_row) {
  if (cpu_spin_per_row == 0) {
    return input;
  }
  materializeRows(&input);

  std::unordered_set<std::string> value_columns;
  for (const auto& aggregate_spec : aggregate.aggregates) {
    if (aggregate_spec.function != AggregateFunction::Count) {
      value_columns.insert(aggregate_spec.value_column);
    }
  }
  for (const auto& column : value_columns) {
    if (!input.schema.has(column)) continue;
    const auto column_index = input.schema.indexOf(column);
    for (auto& row : input.rows) {
      if (column_index >= row.size() || row[column_index].isNull()) continue;
      row[column_index] = Value(spinValue(row[column_index].asDouble(), cpu_spin_per_row));
    }
  }
  return input;
}

std::vector<std::string> buildInputProjectionColumns(const LocalGroupedAggregateSpec& aggregate) {
  std::vector<std::string> columns = aggregate.group_keys;
  std::unordered_set<std::string> seen(columns.begin(), columns.end());
  for (const auto& aggregate_spec : aggregate.aggregates) {
    if (aggregate_spec.function == AggregateFunction::Count) continue;
    if (seen.insert(aggregate_spec.value_column).second) {
      columns.push_back(aggregate_spec.value_column);
    }
  }
  return columns;
}

Table appendTables(const std::vector<Table>& tables) {
  return concatenateTables(tables, false);
}

Table aggregateInputToPartial(const Table& input, const LocalGroupedAggregateSpec& aggregate,
                              size_t cpu_spin_per_row) {
  if (aggregate.group_keys.empty() || aggregate.aggregates.empty()) {
    return makeAggregateOutputSkeleton(aggregate, false);
  }
  switch (analyzeLocalGroupedAggregateExecution(input, aggregate).aggregate_shape) {
    case LocalGroupedAggregateShape::TwoKeyStringSum:
      return aggregatePartitionWithWork(input, aggregate, cpu_spin_per_row);
    case LocalGroupedAggregateShape::TwoKeyInt64Sum:
      return aggregateInt64WindowKeySumPartial(input, aggregate, cpu_spin_per_row);
    case LocalGroupedAggregateShape::Generic:
      break;
  }
  Table spun = applyCpuSpin(input, aggregate, cpu_spin_per_row);
  std::vector<RuntimeAggregateLayout> layouts;
  const auto partial_specs = buildRuntimePartialAggregateSpecs(spun, aggregate, &layouts);
  const auto key_indices = resolveGroupKeyIndices(spun, aggregate.group_keys);
  return DataFrame(spun).aggregate(key_indices, partial_specs).toTable();
}

Table aggregateInt64WindowKeySumPartial(const Table& input, const LocalGroupedAggregateSpec& aggregate,
                                        size_t cpu_spin_per_row) {
  const auto window_column = viewValueColumn(input, input.schema.indexOf(aggregate.group_keys[0]));
  const auto key_column = viewValueColumn(input, input.schema.indexOf(aggregate.group_keys[1]));
  const auto value_column =
      viewValueColumn(input, input.schema.indexOf(aggregate.aggregates[0].value_column));
  std::unordered_map<Int64WindowKey, std::size_t, Int64WindowKeyHash> key_to_index;
  std::vector<Int64WindowKey> ordered_keys;
  std::vector<double> ordered_sums;
  key_to_index.reserve(input.rowCount());
  ordered_keys.reserve(input.rowCount());
  ordered_sums.reserve(input.rowCount());
  for (std::size_t row_index = 0; row_index < input.rowCount(); ++row_index) {
    if (valueColumnIsNullAt(*value_column.buffer, row_index)) {
      continue;
    }
    const Int64WindowKey state_key{
        valueColumnIsNullAt(*window_column.buffer, row_index),
        valueColumnIsNullAt(*window_column.buffer, row_index)
            ? 0
            : valueColumnInt64At(*window_column.buffer, row_index),
        valueColumnIsNullAt(*key_column.buffer, row_index),
        valueColumnIsNullAt(*key_column.buffer, row_index)
            ? 0
            : valueColumnInt64At(*key_column.buffer, row_index),
    };
    auto it = key_to_index.find(state_key);
    if (it == key_to_index.end()) {
      ordered_keys.push_back(state_key);
      ordered_sums.push_back(0.0);
      const auto index = ordered_sums.size() - 1;
      it = key_to_index.emplace(state_key, index).first;
    }
    const double value = valueColumnDoubleAt(*value_column.buffer, row_index);
    ordered_sums[it->second] +=
        cpu_spin_per_row == 0 ? value : spinValue(value, cpu_spin_per_row);
  }

  Table out(Schema({aggregate.group_keys[0], aggregate.group_keys[1],
                    partialValueColumnName(aggregate.aggregates[0].output_column)}), {});
  out.rows.reserve(ordered_keys.size());
  for (std::size_t index = 0; index < ordered_keys.size(); ++index) {
    Row row;
    row.emplace_back(ordered_keys[index].window_is_null ? Value() : Value(ordered_keys[index].window));
    row.emplace_back(ordered_keys[index].key_is_null ? Value() : Value(ordered_keys[index].key));
    row.emplace_back(ordered_sums[index]);
    out.rows.push_back(std::move(row));
  }
  return out;
}

Table finalizeMergedPartialAggregates(const Table& partials,
                                      const LocalGroupedAggregateSpec& aggregate) {
  if (aggregate.group_keys.empty() || aggregate.aggregates.empty() || partials.rowCount() == 0) {
    return makeAggregateOutputSkeleton(aggregate, true);
  }

  std::vector<RuntimeAggregateLayout> layouts;
  for (const auto& aggregate_spec : aggregate.aggregates) {
    RuntimeAggregateLayout layout;
    layout.aggregate = aggregate_spec;
    switch (aggregate_spec.function) {
      case AggregateFunction::Sum:
      case AggregateFunction::Min:
      case AggregateFunction::Max:
        layout.partial_value_column = partialValueColumnName(aggregate_spec.output_column);
        break;
      case AggregateFunction::Count:
        layout.partial_count_column = partialValueColumnName(aggregate_spec.output_column);
        break;
      case AggregateFunction::Avg:
        layout.partial_value_column = partialAverageSumColumnName(aggregate_spec.output_column);
        layout.partial_count_column = partialAverageCountColumnName(aggregate_spec.output_column);
        break;
    }
    layouts.push_back(std::move(layout));
  }

  const auto key_indices = resolveGroupKeyIndices(partials, aggregate.group_keys);
  const auto merge_specs = buildRuntimeMergeAggregateSpecs(partials, layouts);
  Table merged = DataFrame(partials).aggregate(key_indices, merge_specs).toTable();
  materializeRows(&merged);

  Table output = makeAggregateOutputSkeleton(aggregate, true);
  output.rows.reserve(merged.rows.size());
  for (const auto& merged_row : merged.rows) {
    Row output_row;
    output_row.reserve(output.schema.fields.size());
    for (size_t i = 0; i < aggregate.group_keys.size(); ++i) {
      output_row.push_back(merged_row[i]);
    }
    for (const auto& layout : layouts) {
      switch (layout.aggregate.function) {
        case AggregateFunction::Sum:
        case AggregateFunction::Min:
        case AggregateFunction::Max:
          output_row.push_back(
              merged_row[merged.schema.indexOf(layout.partial_value_column)]);
          break;
        case AggregateFunction::Count:
          output_row.push_back(Value(static_cast<int64_t>(
              merged_row[merged.schema.indexOf(layout.partial_count_column)].asDouble())));
          break;
        case AggregateFunction::Avg: {
          const double sum =
              merged_row[merged.schema.indexOf(layout.partial_value_column)].asDouble();
          const int64_t count = static_cast<int64_t>(
              merged_row[merged.schema.indexOf(layout.partial_count_column)].asDouble());
          output_row.push_back(Value(count == 0 ? 0.0 : sum / static_cast<double>(count)));
          output_row.push_back(Value(sum));
          output_row.push_back(Value(count));
          break;
        }
      }
    }
    output.rows.push_back(std::move(output_row));
  }
  return output;
}

void mergeWindowKeySumPartial(const Table& partial, const LocalGroupedAggregateSpec& aggregate,
                              const std::string& partial_column, WindowKeySumState* state) {
  if (state == nullptr) return;
  if (!partial.schema.has(aggregate.group_keys[0]) || !partial.schema.has(aggregate.group_keys[1]) ||
      !partial.schema.has(partial_column)) {
    throw std::runtime_error("two-key partial aggregate schema mismatch");
  }
  Table materialized = partial;
  if (materialized.rows.empty() && materialized.rowCount() > 0) {
    materializeRows(&materialized);
  }
  const auto window_idx = materialized.schema.indexOf(aggregate.group_keys[0]);
  const auto key_idx = materialized.schema.indexOf(aggregate.group_keys[1]);
  const auto sum_idx = materialized.schema.indexOf(partial_column);
  TwoKeyValueColumnarBatch batch;
  batch.row_count = materialized.rows.size();
  batch.first_key.type = BinaryKeyColumnType::String;
  batch.second_key.type = BinaryKeyColumnType::String;
  batch.first_key.dictionary_encoded = false;
  batch.second_key.dictionary_encoded = false;
  batch.first_key.is_null.assign(batch.row_count, 0);
  batch.second_key.is_null.assign(batch.row_count, 0);
  batch.first_key.string_values.reserve(batch.row_count, batch.row_count * 16);
  batch.second_key.string_values.reserve(batch.row_count, batch.row_count * 16);
  batch.value.is_null.assign(batch.row_count, 0);
  batch.value.values.assign(batch.row_count, 0.0);
  for (const auto& row : materialized.rows) {
    const std::size_t row_idx = &row - materialized.rows.data();
    if (row[window_idx].isNull()) batch.first_key.is_null[row_idx] = 1;
    else batch.first_key.string_values.append(row[window_idx].toString());
    if (row[key_idx].isNull()) batch.second_key.is_null[row_idx] = 1;
    else batch.second_key.string_values.append(row[key_idx].toString());
    if (row[sum_idx].isNull()) batch.value.is_null[row_idx] = 1;
    else batch.value.values[row_idx] = row[sum_idx].asDouble();
  }
  mergeAggregatePartialBatch(makeAggregatePartialBatch(
                                 batch, {aggregate.group_keys[0], aggregate.group_keys[1]},
                                 {partial_column}),
                             state, nullptr);
}

void mergeTwoKeyValuePartial(const TwoKeyValueColumnarBatch& partial,
                             WindowKeySumState* string_state,
                             Int64TwoKeySumState* int64_state) {
  mergeAggregatePartialBatch(makeAggregatePartialBatch(partial, {"k0", "k1"}, {"sum"}),
                             string_state, int64_state);
}

Table materializeWindowKeySumState(const WindowKeySumState& state,
                                   const LocalGroupedAggregateSpec& aggregate) {
  return materializeAggregateStringKeyState(
      state, {aggregate.group_keys[0], aggregate.group_keys[1]},
      aggregate.aggregates[0].output_column);
}

Table materializeInt64TwoKeySumState(const Int64TwoKeySumState& state,
                                     const LocalGroupedAggregateSpec& aggregate) {
  return materializeAggregateFixedKeyState(
      state, {aggregate.group_keys[0], aggregate.group_keys[1]},
      aggregate.aggregates[0].output_column);
}

Table runSingleProcessGroupedAggregateImpl(const std::vector<Table>& batches,
                                           const LocalGroupedAggregateSpec& aggregate,
                                           size_t cpu_spin_per_row) {
  const auto execution_pattern = analyzeLocalGroupedAggregateExecution(batches, aggregate);
  if (execution_pattern.use_two_key_partial_merge) {
    WindowKeySumState state;
    const auto partial_column = partialValueColumnName(aggregate.aggregates[0].output_column);
    for (const auto& batch : batches) {
      Table partial = aggregateInputToPartial(batch, aggregate, cpu_spin_per_row);
      mergeWindowKeySumPartial(partial, aggregate, partial_column, &state);
    }
    return materializeWindowKeySumState(state, aggregate);
  }

  std::vector<Table> partials;
  partials.reserve(batches.size());
  for (const auto& batch : batches) {
    partials.push_back(aggregateInputToPartial(batch, aggregate, cpu_spin_per_row));
  }
  return finalizeMergedPartialAggregates(appendTables(partials), aggregate);
}

LocalActorStreamResult measureSingleProcessGroupedAggregate(
    const std::vector<Table>& batches, const LocalGroupedAggregateSpec& aggregate,
    size_t cpu_spin_per_row) {
  LocalActorStreamResult result;
  auto started = std::chrono::steady_clock::now();
  result.final_table = runSingleProcessGroupedAggregateImpl(batches, aggregate, cpu_spin_per_row);
  result.processed_batches = batches.size();
  result.processed_partitions = batches.size();
  result.elapsed_ms = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - started)
          .count());
  return result;
}

void appendU8(std::vector<uint8_t>* out, uint8_t value) { out->push_back(value); }

void appendU32(std::vector<uint8_t>* out, uint32_t value) {
  out->push_back(static_cast<uint8_t>(value & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 16) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 24) & 0xFF));
}

void appendU64(std::vector<uint8_t>* out, uint64_t value) {
  out->push_back(static_cast<uint8_t>(value & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 16) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 24) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 32) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 40) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 48) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 56) & 0xFF));
}

bool readU8(const std::vector<uint8_t>& src, size_t* offset, uint8_t* out) {
  if (offset == nullptr || out == nullptr || *offset >= src.size()) return false;
  *out = src[*offset];
  ++(*offset);
  return true;
}

bool readU32(const std::vector<uint8_t>& src, size_t* offset, uint32_t* out) {
  if (offset == nullptr || out == nullptr || *offset + 4 > src.size()) return false;
  *out = static_cast<uint32_t>(src[*offset]) |
         (static_cast<uint32_t>(src[*offset + 1]) << 8) |
         (static_cast<uint32_t>(src[*offset + 2]) << 16) |
         (static_cast<uint32_t>(src[*offset + 3]) << 24);
  *offset += 4;
  return true;
}

bool readU64(const std::vector<uint8_t>& src, size_t* offset, uint64_t* out) {
  if (offset == nullptr || out == nullptr || *offset + 8 > src.size()) return false;
  *out = static_cast<uint64_t>(src[*offset]) |
         (static_cast<uint64_t>(src[*offset + 1]) << 8) |
         (static_cast<uint64_t>(src[*offset + 2]) << 16) |
         (static_cast<uint64_t>(src[*offset + 3]) << 24) |
         (static_cast<uint64_t>(src[*offset + 4]) << 32) |
         (static_cast<uint64_t>(src[*offset + 5]) << 40) |
         (static_cast<uint64_t>(src[*offset + 6]) << 48) |
         (static_cast<uint64_t>(src[*offset + 7]) << 56);
  *offset += 8;
  return true;
}

void appendBytes(std::vector<uint8_t>* out, const std::vector<uint8_t>& bytes) {
  appendU32(out, static_cast<uint32_t>(bytes.size()));
  out->insert(out->end(), bytes.begin(), bytes.end());
}

void appendString(std::vector<uint8_t>* out, const std::string& value) {
  appendU32(out, static_cast<uint32_t>(value.size()));
  out->insert(out->end(), value.begin(), value.end());
}

bool readBytes(const std::vector<uint8_t>& src, size_t* offset, std::vector<uint8_t>* out) {
  uint32_t size = 0;
  if (offset == nullptr || out == nullptr || !readU32(src, offset, &size)) return false;
  if (*offset + size > src.size()) return false;
  out->assign(src.begin() + static_cast<std::vector<uint8_t>::difference_type>(*offset),
              src.begin() + static_cast<std::vector<uint8_t>::difference_type>(*offset + size));
  *offset += size;
  return true;
}

bool readString(const std::vector<uint8_t>& src, size_t* offset, std::string* out) {
  uint32_t size = 0;
  if (offset == nullptr || out == nullptr || !readU32(src, offset, &size)) return false;
  if (*offset + size > src.size()) return false;
  out->assign(reinterpret_cast<const char*>(src.data() + *offset), size);
  *offset += size;
  return true;
}

struct WorkerHandle {
  pid_t pid = -1;
  int fd = -1;
  bool busy = false;
  std::string node_id;
  uint64_t task_id = 0;
};

struct SharedMemoryPayload {
  std::string name;
  uint64_t size = 0;
};

struct PendingPartition {
  uint64_t task_id = 0;
  std::vector<uint8_t> payload;
  bool shared_memory = false;
  SharedMemoryPayload shared_memory_ref;
};

struct WorkerMetrics {
  uint64_t deserialize_ms = 0;
  uint64_t compute_ms = 0;
  uint64_t serialize_ms = 0;
};

struct ActorStreamRequest {
  uint64_t task_id = 0;
  std::vector<uint8_t> table_payload;
  bool shared_memory = false;
  std::string shared_memory_name;
  uint64_t shared_memory_size = 0;
};

struct ActorStreamResponse {
  uint64_t task_id = 0;
  bool ok = false;
  WorkerMetrics metrics;
  std::vector<uint8_t> table_payload;
  std::string reason;
  bool shared_memory = false;
  std::string shared_memory_name;
  uint64_t shared_memory_size = 0;
};

struct SharedMemoryWriteRegion {
  SharedMemoryPayload ref;
  int fd = -1;
  void* mapped = nullptr;
};

struct SharedMemoryReadRegion {
  int fd = -1;
  const void* mapped = nullptr;
  size_t size = 0;
};

std::atomic<uint64_t> g_shared_memory_seq{1};

std::string makeSharedMemoryName() {
  return "/velaria-stream-" + std::to_string(static_cast<uint64_t>(::getpid())) + "-" +
         std::to_string(g_shared_memory_seq.fetch_add(1, std::memory_order_relaxed));
}

SharedMemoryWriteRegion createSharedMemoryWriteRegion(size_t size) {
  SharedMemoryWriteRegion region;
  region.ref.name = makeSharedMemoryName();
  region.ref.size = static_cast<uint64_t>(size);
  region.fd = ::shm_open(region.ref.name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
  if (region.fd < 0) {
    throw std::runtime_error("shm_open create failed");
  }
  if (::ftruncate(region.fd, static_cast<off_t>(size)) != 0) {
    ::close(region.fd);
    ::shm_unlink(region.ref.name.c_str());
    throw std::runtime_error("ftruncate failed");
  }
  if (size > 0) {
    region.mapped = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, region.fd, 0);
    if (region.mapped == MAP_FAILED) {
      ::close(region.fd);
      ::shm_unlink(region.ref.name.c_str());
      throw std::runtime_error("mmap failed");
    }
  }
  return region;
}

void closeSharedMemoryWriteRegion(SharedMemoryWriteRegion* region) {
  if (region == nullptr) return;
  if (region->mapped != nullptr && region->mapped != MAP_FAILED) {
    ::munmap(region->mapped, static_cast<size_t>(region->ref.size));
    region->mapped = nullptr;
  }
  if (region->fd >= 0) {
    ::close(region->fd);
    region->fd = -1;
  }
}

SharedMemoryReadRegion openSharedMemoryReadRegion(const SharedMemoryPayload& ref) {
  SharedMemoryReadRegion region;
  region.size = static_cast<size_t>(ref.size);
  if (region.size == 0) {
    return region;
  }
  const int fd = ::shm_open(ref.name.c_str(), O_RDONLY, 0600);
  if (fd < 0) {
    throw std::runtime_error("shm_open read failed");
  }
  void* mapped = ::mmap(nullptr, region.size, PROT_READ, MAP_SHARED, fd, 0);
  if (mapped == MAP_FAILED) {
    ::close(fd);
    throw std::runtime_error("mmap read failed");
  }
  region.fd = fd;
  region.mapped = mapped;
  return region;
}

void closeSharedMemoryReadRegion(SharedMemoryReadRegion* region) {
  if (region == nullptr) return;
  if (region->mapped != nullptr && region->mapped != MAP_FAILED) {
    ::munmap(const_cast<void*>(region->mapped), region->size);
    region->mapped = nullptr;
  }
  if (region->fd >= 0) {
    ::close(region->fd);
    region->fd = -1;
  }
}

void unlinkSharedMemoryPayload(const SharedMemoryPayload& ref) {
  if (!ref.name.empty()) {
    ::shm_unlink(ref.name.c_str());
  }
}

void encodeActorStreamRequest(const ActorStreamRequest& request, std::vector<uint8_t>* out) {
  if (out == nullptr) return;
  out->clear();
  out->insert(out->end(), kEnvelopeMagic, kEnvelopeMagic + 4);
  appendU8(out, kRequestKind);
  appendU64(out, request.task_id);
  if (request.shared_memory) {
    appendU8(out, kPayloadSharedMemory);
    appendString(out, request.shared_memory_name);
    appendU64(out, request.shared_memory_size);
  } else {
    appendU8(out, kPayloadInline);
    appendBytes(out, request.table_payload);
  }
}

bool decodeActorStreamRequest(const std::vector<uint8_t>& payload, ActorStreamRequest* out) {
  if (out == nullptr || payload.size() < 5 ||
      std::memcmp(payload.data(), kEnvelopeMagic, 4) != 0) {
    return false;
  }
  size_t offset = 4;
  uint8_t kind = 0;
  if (!readU8(payload, &offset, &kind) || kind != kRequestKind) return false;
  if (!readU64(payload, &offset, &out->task_id)) return false;
  uint8_t mode = 0;
  if (!readU8(payload, &offset, &mode)) return false;
  out->shared_memory = mode == kPayloadSharedMemory;
  if (out->shared_memory) {
    return readString(payload, &offset, &out->shared_memory_name) &&
           readU64(payload, &offset, &out->shared_memory_size);
  }
  return readBytes(payload, &offset, &out->table_payload);
}

void encodeActorStreamResponse(const ActorStreamResponse& response, std::vector<uint8_t>* out) {
  if (out == nullptr) return;
  out->clear();
  out->insert(out->end(), kEnvelopeMagic, kEnvelopeMagic + 4);
  appendU8(out, kResponseKind);
  appendU64(out, response.task_id);
  appendU8(out, response.ok ? 1 : 0);
  appendU64(out, response.metrics.deserialize_ms);
  appendU64(out, response.metrics.compute_ms);
  appendU64(out, response.metrics.serialize_ms);
  if (response.ok) {
    if (response.shared_memory) {
      appendU8(out, kPayloadSharedMemory);
      appendString(out, response.shared_memory_name);
      appendU64(out, response.shared_memory_size);
    } else {
      appendU8(out, kPayloadInline);
      appendBytes(out, response.table_payload);
    }
  } else {
    appendString(out, response.reason);
  }
}

bool decodeActorStreamResponse(const std::vector<uint8_t>& payload, ActorStreamResponse* out) {
  if (out == nullptr || payload.size() < 5 ||
      std::memcmp(payload.data(), kEnvelopeMagic, 4) != 0) {
    return false;
  }
  size_t offset = 4;
  uint8_t kind = 0;
  uint8_t ok = 0;
  if (!readU8(payload, &offset, &kind) || kind != kResponseKind) return false;
  if (!readU64(payload, &offset, &out->task_id) || !readU8(payload, &offset, &ok)) return false;
  out->ok = ok != 0;
  if (!readU64(payload, &offset, &out->metrics.deserialize_ms) ||
      !readU64(payload, &offset, &out->metrics.compute_ms) ||
      !readU64(payload, &offset, &out->metrics.serialize_ms)) {
    return false;
  }
  if (out->ok) {
    uint8_t mode = 0;
    if (!readU8(payload, &offset, &mode)) return false;
    out->shared_memory = mode == kPayloadSharedMemory;
    if (out->shared_memory) {
      return readString(payload, &offset, &out->shared_memory_name) &&
             readU64(payload, &offset, &out->shared_memory_size);
    }
    return readBytes(payload, &offset, &out->table_payload);
  }
  return readString(payload, &offset, &out->reason);
}

std::vector<std::pair<size_t, size_t>> splitTableRanges(const Table& table, size_t parts) {
  const size_t row_count = table.rowCount();
  if (parts <= 1 || row_count <= 1) {
    return {{0, row_count}};
  }
  parts = std::min(parts, row_count);
  std::vector<std::pair<size_t, size_t>> out;
  out.reserve(parts);
  const size_t rows_per_part = (row_count + parts - 1) / parts;
  size_t begin = 0;
  while (begin < row_count) {
    const size_t end = std::min(row_count, begin + rows_per_part);
    out.push_back({begin, end});
    begin = end;
  }
  return out;
}

size_t chooseLocalAggregatePartitionCount(
    size_t row_count, size_t worker_count,
    const LocalGroupedAggregateExecutionPattern& execution_pattern) {
  if (worker_count <= 1 || row_count <= 1) {
    return 1;
  }
  size_t target_rows_per_partition = 2048;
  if (execution_pattern.use_two_key_partial_merge) {
    target_rows_per_partition = 8192;
  } else if (execution_pattern.transport_encoding == LocalTransportEncoding::ArrowIpc) {
    target_rows_per_partition = 4096;
  }
  const size_t desired_partitions =
      std::max<size_t>(1, (row_count + target_rows_per_partition - 1) / target_rows_per_partition);
  return std::max<size_t>(1, std::min(worker_count, desired_partitions));
}

Table projectAndSliceRange(const Table& table, size_t begin, size_t end,
                           const BinaryRowBatchOptions& projection) {
  const size_t row_count = table.rowCount();
  begin = std::min(begin, row_count);
  end = std::min(end, row_count);
  if (begin >= end) {
    Table empty;
    if (projection.projected_columns.empty()) {
      empty.schema = table.schema;
    } else {
      empty.schema.fields = projection.projected_columns;
      for (size_t i = 0; i < empty.schema.fields.size(); ++i) {
        empty.schema.index[empty.schema.fields[i]] = i;
      }
    }
    return empty;
  }

  std::vector<size_t> indices;
  if (projection.projected_columns.empty()) {
    indices.reserve(table.schema.fields.size());
    for (size_t i = 0; i < table.schema.fields.size(); ++i) {
      indices.push_back(i);
    }
  } else {
    indices.reserve(projection.projected_columns.size());
    for (const auto& column : projection.projected_columns) {
      indices.push_back(table.schema.indexOf(column));
    }
  }

  Table out;
  out.schema.fields.reserve(indices.size());
  for (size_t i = 0; i < indices.size(); ++i) {
    out.schema.fields.push_back(table.schema.fields[indices[i]]);
    out.schema.index[out.schema.fields.back()] = i;
  }

  if (const auto cache_in = ensureColumnarCache(&table)) {
    auto cache = std::make_shared<ColumnarTable>();
    cache->schema = out.schema;
    cache->columns.reserve(indices.size());
    cache->arrow_formats.reserve(indices.size());
    cache->row_count = end - begin;
    cache->batch_row_counts.push_back(end - begin);
    for (const auto index : indices) {
      ValueColumnBuffer output_column;
      output_column.values.reserve(end - begin);
      const auto& input_column = cache_in->columns[index];
      for (size_t row = begin; row < end; ++row) {
        output_column.values.push_back(valueColumnValueAt(input_column, row));
      }
      cache->columns.push_back(std::move(output_column));
      if (index < cache_in->arrow_formats.size()) {
        cache->arrow_formats.push_back(cache_in->arrow_formats[index]);
      } else {
        cache->arrow_formats.emplace_back();
      }
    }
    out.columnar_cache = std::move(cache);
    return out;
  }

  out.rows.reserve(end - begin);
  for (size_t row = begin; row < end; ++row) {
    Row projected;
    projected.reserve(indices.size());
    for (const auto index : indices) {
      projected.push_back(table.rows[row][index]);
    }
    out.rows.push_back(std::move(projected));
  }
  return out;
}

std::string_view stringAt(const BinaryKeyColumn& column, size_t row_idx) {
  if (column.dictionary_encoded) {
    return column.dictionary.view(column.indices[row_idx]);
  }
  return column.string_values.view(row_idx);
}

bool valueColumnIsInt64Like(const ValueColumnBuffer& column) {
  if (column.arrow_backing != nullptr) {
    return isArrowIntegerLikeFormat(column.arrow_backing->format);
  }
  const auto row_count = valueColumnRowCount(column);
  for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
    if (valueColumnIsNullAt(column, row_index)) continue;
    return valueColumnValueAt(column, row_index).type() == DataType::Int64;
  }
  return false;
}

std::string stringKeyAt(const ValueColumnBuffer& column, size_t row_idx) {
  if (valueColumnIsNullAt(column, row_idx)) {
    return std::string();
  }
  if (column.arrow_backing != nullptr &&
      isArrowUtf8Format(column.arrow_backing->format)) {
    const auto view = valueColumnStringViewAt(column, row_idx);
    return std::string(view.data(), view.size());
  }
  const Value value = valueColumnValueAt(column, row_idx);
  if (value.type() == DataType::String) {
    return value.asString();
  }
  return value.toString();
}

Value binaryKeyValueAt(const BinaryKeyColumn& column, size_t row_idx) {
  if (column.is_null[row_idx] != 0) {
    return Value();
  }
  if (column.type == BinaryKeyColumnType::Int64) {
    return Value(column.int64_values[row_idx]);
  }
  const auto view = stringAt(column, row_idx);
  return Value(std::string(view.data(), view.size()));
}

double spinValue(double value, size_t cpu_spin_per_row) {
  double out = value;
  for (size_t i = 0; i < cpu_spin_per_row; ++i) {
    out = out * 1.0000001 + static_cast<double>(i % 7);
    out = out / 1.0000001;
  }
  return out;
}

Table aggregateVectorizedBatch(const TwoKeyValueColumnarBatch& batch,
                               const std::string& first_key_name,
                               const std::string& second_key_name,
                               const std::string& output_column,
                               size_t cpu_spin_per_row) {
  Table out(Schema({first_key_name, second_key_name, output_column}), {});
  if (batch.row_count == 0) {
    return out;
  }

  if (batch.first_key.type == BinaryKeyColumnType::String &&
      batch.second_key.type == BinaryKeyColumnType::String &&
      batch.first_key.dictionary_encoded && batch.second_key.dictionary_encoded) {
    std::unordered_map<uint64_t, double> sums;
    std::vector<uint64_t> ordered;
    ordered.reserve(batch.row_count);
    for (size_t i = 0; i < batch.row_count; ++i) {
      if (batch.first_key.is_null[i] || batch.second_key.is_null[i] || batch.value.is_null[i]) continue;
      const uint64_t composite =
          (static_cast<uint64_t>(batch.first_key.indices[i]) << 32) |
          static_cast<uint64_t>(batch.second_key.indices[i]);
      auto it = sums.find(composite);
      if (it == sums.end()) {
        ordered.push_back(composite);
        it = sums.emplace(composite, 0.0).first;
      }
      const double value = cpu_spin_per_row == 0 ? batch.value.values[i]
                                                 : spinValue(batch.value.values[i], cpu_spin_per_row);
      it->second += value;
    }
    out.rows.reserve(ordered.size());
    for (uint64_t composite : ordered) {
      const uint32_t window_id = static_cast<uint32_t>(composite >> 32);
      const uint32_t key_id = static_cast<uint32_t>(composite & 0xFFFFFFFFu);
      Row row;
      const auto window = batch.first_key.dictionary.view(window_id);
      const auto key = batch.second_key.dictionary.view(key_id);
      row.emplace_back(std::string(window.data(), window.size()));
      row.emplace_back(std::string(key.data(), key.size()));
      row.emplace_back(sums.at(composite));
      out.rows.push_back(std::move(row));
    }
    return out;
  }

  if (batch.first_key.type == BinaryKeyColumnType::Int64 &&
      batch.second_key.type == BinaryKeyColumnType::Int64) {
    std::unordered_map<Int64WindowKey, std::size_t, Int64WindowKeyHash> sums_index;
    std::vector<Int64WindowKey> ordered;
    std::vector<double> sums;
    ordered.reserve(batch.row_count);
    sums.reserve(batch.row_count);
    for (size_t i = 0; i < batch.row_count; ++i) {
      if (batch.first_key.is_null[i] || batch.second_key.is_null[i] || batch.value.is_null[i]) continue;
      const Int64WindowKey composite{
          batch.first_key.is_null[i] != 0, batch.first_key.int64_values[i],
          batch.second_key.is_null[i] != 0, batch.second_key.int64_values[i]};
      auto it = sums_index.find(composite);
      if (it == sums_index.end()) {
        ordered.push_back(composite);
        sums.push_back(0.0);
        it = sums_index.emplace(composite, sums.size() - 1).first;
      }
      const double value = cpu_spin_per_row == 0 ? batch.value.values[i]
                                                 : spinValue(batch.value.values[i], cpu_spin_per_row);
      sums[it->second] += value;
    }
    out.rows.reserve(ordered.size());
    for (std::size_t i = 0; i < ordered.size(); ++i) {
      Row row;
      row.emplace_back(ordered[i].window_is_null ? Value() : Value(ordered[i].window));
      row.emplace_back(ordered[i].key_is_null ? Value() : Value(ordered[i].key));
      row.emplace_back(sums[i]);
      out.rows.push_back(std::move(row));
    }
    return out;
  }

  std::unordered_map<std::string, double> sums;
  std::vector<std::string> ordered;
  ordered.reserve(batch.row_count);
  for (size_t i = 0; i < batch.row_count; ++i) {
    if (batch.first_key.is_null[i] || batch.second_key.is_null[i] || batch.value.is_null[i]) continue;
    const auto window_value = binaryKeyValueAt(batch.first_key, i);
    const auto key_value = binaryKeyValueAt(batch.second_key, i);
    std::string state_key = encodeGenericTwoKeyStateKey(window_value, key_value);
    auto it = sums.find(state_key);
    if (it == sums.end()) {
      ordered.push_back(state_key);
      it = sums.emplace(state_key, 0.0).first;
    }
    const double value = cpu_spin_per_row == 0 ? batch.value.values[i]
                                               : spinValue(batch.value.values[i], cpu_spin_per_row);
    it->second += value;
  }
  out.rows.reserve(ordered.size());
  for (const auto& state_key : ordered) {
    const auto split = state_key.find(kKeySep);
    Row row;
    row.emplace_back(state_key.substr(0, split));
    row.emplace_back(state_key.substr(split + 1));
    row.emplace_back(sums.at(state_key));
    out.rows.push_back(std::move(row));
  }
  return out;
}

TwoKeyValueColumnarBatch buildColumnarFromTable(const Table& input,
                                                const LocalGroupedAggregateSpec& aggregate) {
  if (!input.schema.has(aggregate.group_keys[0]) || !input.schema.has(aggregate.group_keys[1]) ||
      !input.schema.has(aggregate.aggregates[0].value_column)) {
    throw std::runtime_error("buildColumnarFromTable schema mismatch");
  }

  const size_t window_idx = input.schema.indexOf(aggregate.group_keys[0]);
  const size_t key_idx = input.schema.indexOf(aggregate.group_keys[1]);
  const size_t value_idx = input.schema.indexOf(aggregate.aggregates[0].value_column);
  const auto window_column = viewValueColumn(input, window_idx);
  const auto key_column = viewValueColumn(input, key_idx);
  const auto value_column = viewValueColumn(input, value_idx);
  TwoKeyValueColumnarBatch batch;
  batch.row_count = input.rowCount();
  const bool first_is_int64 = valueColumnIsInt64Like(*window_column.buffer);
  const bool second_is_int64 = valueColumnIsInt64Like(*key_column.buffer);
  batch.first_key.type = first_is_int64 ? BinaryKeyColumnType::Int64 : BinaryKeyColumnType::String;
  batch.second_key.type = second_is_int64 ? BinaryKeyColumnType::Int64 : BinaryKeyColumnType::String;
  batch.first_key.dictionary_encoded = !first_is_int64;
  batch.second_key.dictionary_encoded = !second_is_int64;
  batch.first_key.is_null.assign(batch.row_count, 0);
  batch.second_key.is_null.assign(batch.row_count, 0);
  batch.value.is_null.assign(batch.row_count, 0);
  batch.value.values.assign(batch.row_count, 0.0);
  if (first_is_int64) {
    batch.first_key.int64_values.assign(batch.row_count, 0);
  } else {
    batch.first_key.indices.assign(batch.row_count, 0);
  }
  if (second_is_int64) {
    batch.second_key.int64_values.assign(batch.row_count, 0);
  } else {
    batch.second_key.indices.assign(batch.row_count, 0);
  }

  std::unordered_map<std::string, uint32_t> window_dict;
  std::unordered_map<std::string, uint32_t> key_dict;
  for (size_t row_idx = 0; row_idx < batch.row_count; ++row_idx) {
    if (valueColumnIsNullAt(*window_column.buffer, row_idx)) {
      batch.first_key.is_null[row_idx] = 1;
    } else {
      if (first_is_int64) {
        batch.first_key.int64_values[row_idx] = valueColumnInt64At(*window_column.buffer, row_idx);
      } else {
        const std::string text = stringKeyAt(*window_column.buffer, row_idx);
        auto it = window_dict.find(text);
        if (it == window_dict.end()) {
          const uint32_t id = static_cast<uint32_t>(batch.first_key.dictionary.size());
          batch.first_key.dictionary.append(text);
          it = window_dict.emplace(text, id).first;
        }
        batch.first_key.indices[row_idx] = it->second;
      }
    }

    if (valueColumnIsNullAt(*key_column.buffer, row_idx)) {
      batch.second_key.is_null[row_idx] = 1;
    } else {
      if (second_is_int64) {
        batch.second_key.int64_values[row_idx] = valueColumnInt64At(*key_column.buffer, row_idx);
      } else {
        const std::string text = stringKeyAt(*key_column.buffer, row_idx);
        auto it = key_dict.find(text);
        if (it == key_dict.end()) {
          const uint32_t id = static_cast<uint32_t>(batch.second_key.dictionary.size());
          batch.second_key.dictionary.append(text);
          it = key_dict.emplace(text, id).first;
        }
        batch.second_key.indices[row_idx] = it->second;
      }
    }

    if (valueColumnIsNullAt(*value_column.buffer, row_idx)) {
      batch.value.is_null[row_idx] = 1;
    } else {
      batch.value.values[row_idx] = valueColumnDoubleAt(*value_column.buffer, row_idx);
    }
  }
  return batch;
}

Table aggregatePartitionWithWork(const Table& input, const LocalGroupedAggregateSpec& aggregate,
                                 size_t cpu_spin_per_row) {
  return aggregateVectorizedBatch(buildColumnarFromTable(input, aggregate),
                                  aggregate.group_keys[0], aggregate.group_keys[1],
                                  partialValueColumnName(aggregate.aggregates[0].output_column),
                                  cpu_spin_per_row);
}

void workerLoop(int fd, const LocalGroupedAggregateSpec& aggregate, uint64_t delay_ms,
                size_t cpu_spin_per_row, size_t shared_memory_min_payload_bytes) {
  LengthPrefixedFrameCodec codec;
  BinaryRowBatchCodec batch_codec;
  ByteBufferPool payload_pool;
  ByteBufferPool frame_pool;

  while (true) {
    RpcFrame frame;
    if (!recvFrameOverSocket(fd, codec, &frame)) {
      break;
    }
    ActorStreamRequest request;
    if (!decodeActorStreamRequest(frame.payload, &request)) {
      continue;
    }

    if (delay_ms > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
    }

    ActorStreamResponse result;
    result.task_id = request.task_id;
    try {
      const auto deserialize_started = std::chrono::steady_clock::now();
      bool used_binary_row_request = false;
      SharedMemoryReadRegion request_region;
      const uint8_t* request_bytes = nullptr;
      std::size_t request_size = 0;
      if (request.shared_memory) {
        request_region = openSharedMemoryReadRegion(
            SharedMemoryPayload{request.shared_memory_name, request.shared_memory_size});
        request_bytes = static_cast<const uint8_t*>(request_region.mapped);
        request_size = request_region.size;
      } else {
        request_bytes = request.table_payload.data();
        request_size = request.table_payload.size();
      }
      const auto compute_started = std::chrono::steady_clock::now();
      Table partial;
      if (isBinaryRowBatchPayload(request_bytes, request_size)) {
        used_binary_row_request = true;
        TwoKeyValueColumnarBatch batch;
        const bool decoded_fast =
            request.shared_memory
                ? batch_codec.deserializeTwoKeyValueFromBuffer(
                      request_bytes, request_size, aggregate.group_keys[0], aggregate.group_keys[1],
                      aggregate.aggregates[0].value_column, &batch)
                : batch_codec.deserializeTwoKeyValue(request.table_payload, aggregate.group_keys[0],
                                                    aggregate.group_keys[1],
                                                    aggregate.aggregates[0].value_column, &batch);
        if (decoded_fast) {
          partial = aggregateVectorizedBatch(
              batch, aggregate.group_keys[0], aggregate.group_keys[1],
              partialValueColumnName(aggregate.aggregates[0].output_column), cpu_spin_per_row);
        } else {
          const Table input =
              request.shared_memory
                  ? batch_codec.deserializeFromBuffer(request_bytes, request_size)
                  : batch_codec.deserialize(request.table_payload);
          partial = aggregateInputToPartial(input, aggregate, cpu_spin_per_row);
        }
      } else {
        const Table input =
            request.shared_memory
                ? deserialize_nanoarrow_ipc_table(request_bytes, request_size, false)
                : deserialize_nanoarrow_ipc_table(request.table_payload, false);
        partial = aggregateInputToPartial(input, aggregate, cpu_spin_per_row);
      }
      closeSharedMemoryReadRegion(&request_region);
      const auto serialize_started = std::chrono::steady_clock::now();

      result.ok = true;
      result.metrics.deserialize_ms = toMillis(compute_started - deserialize_started);
      result.metrics.compute_ms = toMillis(serialize_started - compute_started);

      std::vector<uint8_t> table_payload;
      if (used_binary_row_request) {
        batch_codec.serialize(partial, &table_payload);
      } else {
        table_payload = serialize_nanoarrow_ipc_table(partial);
      }
      const size_t payload_size = table_payload.size();
      if (payload_size >= shared_memory_min_payload_bytes) {
        auto region = createSharedMemoryWriteRegion(payload_size);
        std::memcpy(region.mapped, table_payload.data(), payload_size);
        closeSharedMemoryWriteRegion(&region);
        region.ref.size = payload_size;
        result.shared_memory = true;
        result.shared_memory_name = region.ref.name;
        result.shared_memory_size = region.ref.size;
      } else {
        result.table_payload = std::move(table_payload);
      }
      result.metrics.serialize_ms = toMillis(std::chrono::steady_clock::now() - serialize_started);
    } catch (const std::exception& ex) {
      result.ok = false;
      result.reason = ex.what();
    }

    RpcFrame reply;
    reply.header.protocol_version = 1;
    reply.header.type = RpcMessageType::DataBatch;
    reply.header.message_id = result.task_id;
    reply.header.correlation_id = result.task_id;
    reply.header.codec_id = "actor-stream-bin-v1";
    reply.header.source = "worker";
    reply.header.target = "coordinator";
    reply.payload = payload_pool.acquire(result.table_payload.size() + 64);
    encodeActorStreamResponse(result, &reply.payload);
    auto scratch = frame_pool.acquire(reply.payload.size() + 128);
    sendFrameOverSocket(fd, codec, reply, &scratch);
    frame_pool.release(std::move(scratch));
    payload_pool.release(std::move(reply.payload));
    payload_pool.release(std::move(result.table_payload));
    if (request.shared_memory) {
      unlinkSharedMemoryPayload(SharedMemoryPayload{request.shared_memory_name,
                                                    request.shared_memory_size});
    }
  }
  ::close(fd);
  _exit(0);
}

std::vector<WorkerHandle> startWorkers(const LocalGroupedAggregateSpec& aggregate,
                                       const LocalActorStreamOptions& options) {
  std::vector<WorkerHandle> workers;
  workers.reserve(options.worker_count);
  for (size_t i = 0; i < std::max<size_t>(1, options.worker_count); ++i) {
    int fds[2];
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
      throw std::runtime_error("socketpair failed");
    }
    const pid_t pid = ::fork();
    if (pid < 0) {
      ::close(fds[0]);
      ::close(fds[1]);
      throw std::runtime_error("fork failed");
    }
    if (pid == 0) {
      ::close(fds[0]);
      workerLoop(fds[1], aggregate, options.worker_delay_ms, options.cpu_spin_per_row,
                 options.shared_memory_min_payload_bytes);
    }
    ::close(fds[1]);
    WorkerHandle worker;
    worker.pid = pid;
    worker.fd = fds[0];
    worker.node_id = "local-worker-" + std::to_string(i + 1);
    workers.push_back(worker);
  }
  return workers;
}

void stopWorkers(std::vector<WorkerHandle>* workers) {
  if (workers == nullptr) return;
  for (auto& worker : *workers) {
    if (worker.fd >= 0) {
      ::close(worker.fd);
      worker.fd = -1;
    }
  }
  for (auto& worker : *workers) {
    if (worker.pid > 0) {
      int status = 0;
      ::waitpid(worker.pid, &status, 0);
    }
  }
}

}  // namespace

const char* localExecutionModeName(LocalExecutionMode mode) {
  switch (mode) {
    case LocalExecutionMode::SingleProcess:
      return "single-process";
    case LocalExecutionMode::ActorCredit:
      return "actor-credit";
  }
  return "single-process";
}

Table runSingleProcessGroupedAggregate(const std::vector<Table>& batches,
                                      const LocalGroupedAggregateSpec& aggregate,
                                      size_t cpu_spin_per_row) {
  return runSingleProcessGroupedAggregateImpl(batches, aggregate, cpu_spin_per_row);
}

LocalActorStreamResult runLocalActorStreamGroupedAggregate(
    const std::vector<Table>& batches, const LocalGroupedAggregateSpec& aggregate,
    const LocalActorStreamOptions& options) {
  if (options.max_inflight_partitions == 0) {
    throw std::invalid_argument("max_inflight_partitions must be positive");
  }
  if (aggregate.group_keys.empty() || aggregate.aggregates.empty()) {
    throw std::invalid_argument("grouped aggregate spec cannot be empty");
  }

  LocalActorStreamResult result;
  auto started = std::chrono::steady_clock::now();
  auto workers = startWorkers(aggregate, options);
  LengthPrefixedFrameCodec codec;
  BinaryRowBatchCodec batch_codec;
  BinaryRowBatchOptions input_projection;
  input_projection.projected_columns = buildInputProjectionColumns(aggregate);
  ByteBufferPool payload_pool;
  ByteBufferPool frame_pool;

  const bool can_use_shared_memory = options.shared_memory_transport;
  const auto execution_pattern = analyzeLocalGroupedAggregateExecution(batches, aggregate);
  const bool use_binary_row_batch_transport =
      execution_pattern.transport_encoding == LocalTransportEncoding::BinaryRowBatch;
  const size_t effective_shared_memory_min_payload_bytes =
      execution_pattern.use_two_key_partial_merge
          ? std::min<size_t>(options.shared_memory_min_payload_bytes, 16 * 1024)
          : options.shared_memory_min_payload_bytes;

  std::vector<PendingPartition> pending;
  uint64_t task_seq = 1;
  for (const auto& batch : batches) {
    ++result.processed_batches;
    const auto split_started = std::chrono::steady_clock::now();
    std::optional<PreparedBinaryRowBatch> prepared_binary_batch;
    if (use_binary_row_batch_transport) {
      prepared_binary_batch = batch_codec.prepare(batch, input_projection);
    }
    const auto ranges = splitTableRanges(
        batch,
        chooseLocalAggregatePartitionCount(batch.rowCount(), std::max<size_t>(1, workers.size()),
                                           execution_pattern));
    result.split_ms += toMillis(std::chrono::steady_clock::now() - split_started);
    for (const auto& range : ranges) {
      PendingPartition pending_part;
      pending_part.task_id = task_seq++;
      const auto serialize_started = std::chrono::steady_clock::now();
      std::vector<uint8_t> payload;
      bool wrote_shared_memory_directly = false;
      if (use_binary_row_batch_transport) {
        const size_t estimated_size = prepared_binary_batch->estimated_size;
        if (can_use_shared_memory && estimated_size >= effective_shared_memory_min_payload_bytes) {
          auto region = createSharedMemoryWriteRegion(estimated_size);
          const size_t actual_size = batch_codec.serializePreparedRangeToBuffer(
              batch, range.first, range.second, *prepared_binary_batch,
              static_cast<uint8_t*>(region.mapped), static_cast<size_t>(region.ref.size));
          closeSharedMemoryWriteRegion(&region);
          region.ref.size = actual_size;
          pending_part.shared_memory = true;
          pending_part.shared_memory_ref = region.ref;
          result.input_shared_memory_bytes += actual_size;
          result.used_shared_memory = true;
          wrote_shared_memory_directly = true;
        } else {
          batch_codec.serializePreparedRange(batch, range.first, range.second,
                                            *prepared_binary_batch, &payload);
        }
      } else {
        auto projected_range = projectAndSliceRange(batch, range.first, range.second, input_projection);
        payload = serialize_nanoarrow_ipc_table(projected_range);
      }
      const size_t payload_size = payload.size();
      if (!wrote_shared_memory_directly &&
          can_use_shared_memory &&
          payload_size >= effective_shared_memory_min_payload_bytes) {
        auto region = createSharedMemoryWriteRegion(payload_size);
        std::memcpy(region.mapped, payload.data(), payload_size);
        closeSharedMemoryWriteRegion(&region);
        region.ref.size = payload_size;
        pending_part.shared_memory = true;
        pending_part.shared_memory_ref = region.ref;
        result.input_shared_memory_bytes += payload_size;
        result.used_shared_memory = true;
      } else {
        pending_part.payload = std::move(payload);
        result.input_payload_bytes += pending_part.payload.size();
      }
      result.coordinator_serialize_ms +=
          toMillis(std::chrono::steady_clock::now() - serialize_started);
      pending.push_back(std::move(pending_part));
    }
  }

  Table merged_partials;
  WindowKeySumState window_key_sum_state;
  Int64TwoKeySumState int64_two_key_sum_state;
  size_t next_pending = 0;
  size_t inflight = 0;
  std::unordered_map<uint64_t, SharedMemoryPayload> inflight_input_shared_memory;

  auto findIdleWorker = [&]() -> WorkerHandle* {
    for (auto& worker : workers) {
      if (!worker.busy) return &worker;
    }
    return nullptr;
  };

  auto dispatchOne = [&](WorkerHandle* worker) {
    if (worker == nullptr || next_pending >= pending.size()) return false;
    auto& task = pending[next_pending++];

    worker->busy = true;
    worker->task_id = task.task_id;
    ++inflight;
    result.max_inflight_partitions = std::max(result.max_inflight_partitions, inflight);

    RpcFrame frame;
    frame.header.protocol_version = 1;
    frame.header.type = RpcMessageType::DataBatch;
    frame.header.message_id = task.task_id;
    frame.header.correlation_id = task.task_id;
    frame.header.codec_id = "actor-stream-bin-v1";
    frame.header.source = "coordinator";
    frame.header.target = worker->node_id;
    frame.payload = payload_pool.acquire(task.payload.size() + 64);

    ActorStreamRequest request;
    request.task_id = task.task_id;
    if (task.shared_memory) {
      request.shared_memory = true;
      request.shared_memory_name = task.shared_memory_ref.name;
      request.shared_memory_size = task.shared_memory_ref.size;
      inflight_input_shared_memory.emplace(task.task_id, task.shared_memory_ref);
    } else {
      request.table_payload = std::move(task.payload);
    }
    encodeActorStreamRequest(request, &frame.payload);

    auto scratch = frame_pool.acquire(frame.payload.size() + 128);
    const bool ok = sendFrameOverSocket(worker->fd, codec, frame, &scratch);
    frame_pool.release(std::move(scratch));
    payload_pool.release(std::move(task.payload));
    payload_pool.release(std::move(request.table_payload));
    payload_pool.release(std::move(frame.payload));
    if (!ok) {
      throw std::runtime_error("sendFrameOverSocket failed");
    }
    return true;
  };

  try {
    while (next_pending < pending.size() || inflight > 0) {
      while (inflight < options.max_inflight_partitions) {
        WorkerHandle* worker = findIdleWorker();
        if (worker == nullptr || next_pending >= pending.size()) break;
        dispatchOne(worker);
      }

      if (next_pending < pending.size() && inflight >= options.max_inflight_partitions) {
        ++result.blocked_count;
      }

      fd_set reads;
      FD_ZERO(&reads);
      int max_fd = -1;
      for (const auto& worker : workers) {
        if (!worker.busy) continue;
        FD_SET(worker.fd, &reads);
        if (worker.fd > max_fd) max_fd = worker.fd;
      }
      if (max_fd < 0) {
        continue;
      }

      const auto wait_started = std::chrono::steady_clock::now();
      const int ready = ::select(max_fd + 1, &reads, nullptr, nullptr, nullptr);
      result.coordinator_wait_ms += toMillis(std::chrono::steady_clock::now() - wait_started);
      if (ready <= 0) {
        throw std::runtime_error("select failed");
      }

      for (auto& worker : workers) {
        if (!worker.busy || !FD_ISSET(worker.fd, &reads)) continue;
        RpcFrame frame;
        if (!recvFrameOverSocket(worker.fd, codec, &frame)) {
          throw std::runtime_error("recvFrameOverSocket failed");
        }
        ActorStreamResponse msg;
        if (!decodeActorStreamResponse(frame.payload, &msg)) {
          throw std::runtime_error("decodeActorStreamResponse failed");
        }
        if (msg.task_id != worker.task_id) {
          throw std::runtime_error("unexpected actor stream reply");
        }
        auto input_shared = inflight_input_shared_memory.find(msg.task_id);
        if (input_shared != inflight_input_shared_memory.end()) {
          unlinkSharedMemoryPayload(input_shared->second);
          inflight_input_shared_memory.erase(input_shared);
        }
        worker.busy = false;
        worker.task_id = 0;
        --inflight;
        if (!msg.ok) {
          throw std::runtime_error(msg.reason.empty() ? "worker failed" : msg.reason);
        }

        SharedMemoryReadRegion output_region;
        if (msg.shared_memory) {
          const SharedMemoryPayload shared{msg.shared_memory_name, msg.shared_memory_size};
          result.output_shared_memory_bytes += shared.size;
          result.used_shared_memory = true;
          output_region = openSharedMemoryReadRegion(shared);
          const auto deserialize_started = std::chrono::steady_clock::now();
          const auto* output_bytes = static_cast<const uint8_t*>(output_region.mapped);
          bool merged_directly = false;
          Table partial;
          if (use_binary_row_batch_transport &&
              isBinaryRowBatchPayload(output_bytes, output_region.size) &&
              execution_pattern.use_two_key_partial_merge) {
            TwoKeyValueColumnarBatch partial_batch;
            const bool decoded = batch_codec.deserializeTwoKeyValueFromBuffer(
                output_bytes, output_region.size, aggregate.group_keys[0], aggregate.group_keys[1],
                partialValueColumnName(aggregate.aggregates[0].output_column), &partial_batch);
            if (decoded) {
              mergeTwoKeyValuePartial(partial_batch, &window_key_sum_state,
                                      &int64_two_key_sum_state);
              merged_directly = true;
            }
          }
          if (!merged_directly) {
            if (use_binary_row_batch_transport &&
                isBinaryRowBatchPayload(output_bytes, output_region.size)) {
              partial = batch_codec.deserializeFromBuffer(output_bytes, output_region.size);
            } else {
              partial = deserialize_nanoarrow_ipc_table(output_bytes, output_region.size, false);
            }
          }
          result.coordinator_deserialize_ms +=
              toMillis(std::chrono::steady_clock::now() - deserialize_started);
          closeSharedMemoryReadRegion(&output_region);
          unlinkSharedMemoryPayload(shared);
          result.output_payload_bytes += shared.size;

          result.worker_deserialize_ms += msg.metrics.deserialize_ms;
          result.worker_compute_ms += msg.metrics.compute_ms;
          result.worker_serialize_ms += msg.metrics.serialize_ms;

          const auto merge_started = std::chrono::steady_clock::now();
          if (merged_directly) {
          } else if (execution_pattern.use_two_key_partial_merge) {
            mergeWindowKeySumPartial(
                partial, aggregate,
                partialValueColumnName(aggregate.aggregates[0].output_column),
                &window_key_sum_state);
          } else {
            merged_partials = concatenateTables({merged_partials, partial}, false);
          }
          result.coordinator_merge_ms += toMillis(std::chrono::steady_clock::now() - merge_started);
          ++result.processed_partitions;
        } else {
          result.output_payload_bytes += msg.table_payload.size();
          const auto deserialize_started = std::chrono::steady_clock::now();
          bool merged_directly = false;
          Table partial;
          if (use_binary_row_batch_transport &&
              isBinaryRowBatchPayload(msg.table_payload.data(), msg.table_payload.size()) &&
              execution_pattern.use_two_key_partial_merge) {
            TwoKeyValueColumnarBatch partial_batch;
            const bool decoded = batch_codec.deserializeTwoKeyValue(
                msg.table_payload, aggregate.group_keys[0], aggregate.group_keys[1],
                partialValueColumnName(aggregate.aggregates[0].output_column), &partial_batch);
            if (decoded) {
              mergeTwoKeyValuePartial(partial_batch, &window_key_sum_state,
                                      &int64_two_key_sum_state);
              merged_directly = true;
            }
          }
          if (!merged_directly) {
            if (use_binary_row_batch_transport &&
                isBinaryRowBatchPayload(msg.table_payload.data(), msg.table_payload.size())) {
              partial = batch_codec.deserialize(msg.table_payload);
            } else {
              partial = deserialize_nanoarrow_ipc_table(msg.table_payload, false);
            }
          }
          result.coordinator_deserialize_ms +=
              toMillis(std::chrono::steady_clock::now() - deserialize_started);
          result.worker_deserialize_ms += msg.metrics.deserialize_ms;
          result.worker_compute_ms += msg.metrics.compute_ms;
          result.worker_serialize_ms += msg.metrics.serialize_ms;

          const auto merge_started = std::chrono::steady_clock::now();
          if (merged_directly) {
          } else if (execution_pattern.use_two_key_partial_merge) {
            mergeWindowKeySumPartial(
                partial, aggregate,
                partialValueColumnName(aggregate.aggregates[0].output_column),
                &window_key_sum_state);
          } else {
            merged_partials = concatenateTables({merged_partials, partial}, false);
          }
          result.coordinator_merge_ms += toMillis(std::chrono::steady_clock::now() - merge_started);
          ++result.processed_partitions;
        }
      }
    }
  } catch (...) {
    for (size_t i = next_pending; i < pending.size(); ++i) {
      if (pending[i].shared_memory) {
        unlinkSharedMemoryPayload(pending[i].shared_memory_ref);
      }
    }
    for (const auto& entry : inflight_input_shared_memory) {
      unlinkSharedMemoryPayload(entry.second);
    }
    stopWorkers(&workers);
    throw;
  }

  stopWorkers(&workers);
  if (execution_pattern.use_two_key_partial_merge) {
    if (!int64_two_key_sum_state.keys.empty()) {
      result.final_table = materializeInt64TwoKeySumState(int64_two_key_sum_state, aggregate);
    } else {
      result.final_table = materializeWindowKeySumState(window_key_sum_state, aggregate);
    }
  } else {
    result.final_table = finalizeMergedPartialAggregates(merged_partials, aggregate);
  }
  result.elapsed_ms = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - started)
          .count());
  return result;
}

LocalActorStreamResult runAutoLocalActorStreamGroupedAggregate(
    const std::vector<Table>& batches, const LocalGroupedAggregateSpec& aggregate,
    const LocalActorStreamOptions& actor_options,
    const LocalExecutionAutoOptions& auto_options, LocalExecutionDecision* decision) {
  LocalExecutionDecision local_decision;
  local_decision.chosen_mode = LocalExecutionMode::SingleProcess;
  if (batches.empty()) {
    local_decision.reason = "no batches";
    if (decision != nullptr) *decision = local_decision;
    return LocalActorStreamResult{};
  }

  local_decision.rows_per_batch = batches.front().rowCount();
  if (actor_options.worker_count < 2 || actor_options.max_inflight_partitions == 0) {
    local_decision.reason = "actor workers unavailable";
    if (decision != nullptr) *decision = local_decision;
    return measureSingleProcessGroupedAggregate(batches, aggregate, actor_options.cpu_spin_per_row);
  }

  const size_t sampled_batches =
      std::min(std::max<size_t>(1, auto_options.sample_batches), batches.size());
  local_decision.sampled_batches = sampled_batches;
  const std::vector<Table> sample_batches_vec(batches.begin(),
                                              batches.begin() + sampled_batches);

  const LocalActorStreamResult sample_single =
      measureSingleProcessGroupedAggregate(sample_batches_vec, aggregate,
                                           actor_options.cpu_spin_per_row);
  const LocalActorStreamResult sample_actor =
      runLocalActorStreamGroupedAggregate(sample_batches_vec, aggregate, actor_options);

  const uint64_t total_projected_bytes =
      sample_actor.input_payload_bytes + sample_actor.input_shared_memory_bytes;
  local_decision.average_projected_payload_bytes =
      sample_actor.processed_batches == 0
          ? 0
          : static_cast<size_t>(total_projected_bytes / sample_actor.processed_batches);
  const uint64_t overhead_ms = actorOverheadMs(sample_actor);
  local_decision = analyzeLocalExecutionDecision(
      LocalExecutionDecisionInputs{
          sampled_batches,
          local_decision.rows_per_batch,
          local_decision.average_projected_payload_bytes,
          toRowsPerSecond(sample_single, local_decision.rows_per_batch),
          toRowsPerSecond(sample_actor, local_decision.rows_per_batch),
          overhead_ms == 0 ? static_cast<double>(sample_actor.worker_compute_ms)
                           : static_cast<double>(sample_actor.worker_compute_ms) /
                                 static_cast<double>(overhead_ms),
      },
      auto_options);

  if (decision != nullptr) *decision = local_decision;
  if (sampled_batches == batches.size()) {
    return local_decision.chosen_mode == LocalExecutionMode::ActorCredit ? sample_actor
                                                                         : sample_single;
  }
  if (local_decision.chosen_mode == LocalExecutionMode::ActorCredit) {
    return runLocalActorStreamGroupedAggregate(batches, aggregate, actor_options);
  }
  return measureSingleProcessGroupedAggregate(batches, aggregate, actor_options.cpu_spin_per_row);
}

}  // namespace dataflow
