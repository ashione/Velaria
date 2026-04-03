#include "src/dataflow/core/execution/runtime/executor.h"

#include <memory>
#include <algorithm>
#include <sstream>
#include <stdexcept>
#include <string>
#include <cstdint>
#include <unordered_map>
#include <vector>

#include "src/dataflow/core/execution/columnar_batch.h"
#include "src/dataflow/experimental/runtime/rpc_runner.h"

namespace dataflow {

namespace {

constexpr char kGroupDelim = '\x1f';

struct BoundComputedArg {
  bool is_literal = false;
  size_t source_column_index = 0;
  bool literal_is_null = false;
  bool literal_has_int64 = false;
  int64_t literal_int64 = 0;
  std::string literal_string;
};

std::vector<BoundComputedArg> bindComputedArgs(const std::vector<ComputedColumnArg>& args) {
  std::vector<BoundComputedArg> bound;
  bound.reserve(args.size());
  for (const auto& arg : args) {
    BoundComputedArg item;
    item.is_literal = arg.is_literal;
    item.source_column_index = arg.source_column_index;
    if (arg.is_literal) {
      item.literal_is_null = arg.literal.isNull();
      if (!item.literal_is_null) {
        item.literal_string =
            arg.literal.type() == DataType::String ? arg.literal.asString() : arg.literal.toString();
        if (arg.literal.isNumber()) {
          item.literal_has_int64 = true;
          item.literal_int64 = arg.literal.asInt64();
        }
      }
    }
    bound.push_back(std::move(item));
  }
  return bound;
}

StringColumnBuffer materializeStringArgColumn(const Table& table, const BoundComputedArg& arg) {
  const auto row_count = table.rows.size();
  if (arg.is_literal) {
    if (arg.literal_is_null) {
      return makeNullStringColumn(row_count);
    }
    return makeConstantStringColumn(row_count, arg.literal_string);
  }
  return dataflow::materializeStringColumn(table, arg.source_column_index);
}

Int64ColumnBuffer materializeInt64ArgColumn(const Table& table, const BoundComputedArg& arg) {
  const auto row_count = table.rows.size();
  if (arg.is_literal) {
    if (arg.literal_is_null) {
      return makeNullInt64Column(row_count);
    }
    if (!arg.literal_has_int64) {
      throw std::runtime_error("string function argument must be numeric");
    }
    return makeConstantInt64Column(row_count, arg.literal_int64);
  }
  return dataflow::materializeInt64Column(table, arg.source_column_index);
}

std::vector<Value> computeStringColumnValues(Table* table, ComputedColumnKind function,
                                             const std::vector<ComputedColumnArg>& args) {
  const auto bound_args = bindComputedArgs(args);
  auto materialize_string_args = [&]() {
    std::vector<StringColumnBuffer> columns;
    columns.reserve(bound_args.size());
    for (const auto& arg : bound_args) {
      columns.push_back(materializeStringArgColumn(*table, arg));
    }
    return columns;
  };
  switch (function) {
    case ComputedColumnKind::StringLength:
      if (bound_args.size() != 1) throw std::runtime_error("LENGTH expects 1 argument");
      return vectorizedStringLength(materializeStringArgColumn(*table, bound_args[0]));
    case ComputedColumnKind::StringLower:
      if (bound_args.size() != 1) throw std::runtime_error("LOWER expects 1 argument");
      return vectorizedStringLower(materializeStringArgColumn(*table, bound_args[0]));
    case ComputedColumnKind::StringUpper:
      if (bound_args.size() != 1) throw std::runtime_error("UPPER expects 1 argument");
      return vectorizedStringUpper(materializeStringArgColumn(*table, bound_args[0]));
    case ComputedColumnKind::StringTrim:
      if (bound_args.size() != 1) throw std::runtime_error("TRIM expects 1 argument");
      return vectorizedStringTrim(materializeStringArgColumn(*table, bound_args[0]));
    case ComputedColumnKind::StringConcat:
      return vectorizedStringConcat(materialize_string_args());
    case ComputedColumnKind::StringReverse:
      if (bound_args.size() != 1) throw std::runtime_error("REVERSE expects 1 argument");
      return vectorizedStringReverse(materializeStringArgColumn(*table, bound_args[0]));
    case ComputedColumnKind::StringConcatWs:
      return vectorizedStringConcatWs(materialize_string_args());
    case ComputedColumnKind::StringLeft:
      if (bound_args.size() != 2) throw std::runtime_error("LEFT requires 2 arguments");
      return vectorizedStringLeft(materializeStringArgColumn(*table, bound_args[0]),
                                  materializeInt64ArgColumn(*table, bound_args[1]));
    case ComputedColumnKind::StringRight:
      if (bound_args.size() != 2) throw std::runtime_error("RIGHT requires 2 arguments");
      return vectorizedStringRight(materializeStringArgColumn(*table, bound_args[0]),
                                   materializeInt64ArgColumn(*table, bound_args[1]));
    case ComputedColumnKind::StringPosition:
      if (bound_args.size() != 2) throw std::runtime_error("POSITION requires 2 arguments");
      return vectorizedStringPosition(materializeStringArgColumn(*table, bound_args[0]),
                                      materializeStringArgColumn(*table, bound_args[1]));
    case ComputedColumnKind::StringSubstr:
      if (bound_args.size() < 2 || bound_args.size() > 3) {
        throw std::runtime_error("SUBSTR expects 2 or 3 arguments");
      }
      if (bound_args.size() == 2) {
        return vectorizedStringSubstr(materializeStringArgColumn(*table, bound_args[0]),
                                      materializeInt64ArgColumn(*table, bound_args[1]), nullptr);
      } else {
        const auto length = materializeInt64ArgColumn(*table, bound_args[2]);
        return vectorizedStringSubstr(materializeStringArgColumn(*table, bound_args[0]),
                                      materializeInt64ArgColumn(*table, bound_args[1]), &length);
      }
    case ComputedColumnKind::StringLtrim:
      if (bound_args.size() != 1) throw std::runtime_error("LTRIM requires 1 argument");
      return vectorizedStringLtrim(materializeStringArgColumn(*table, bound_args[0]));
    case ComputedColumnKind::StringRtrim:
      if (bound_args.size() != 1) throw std::runtime_error("RTRIM requires 1 argument");
      return vectorizedStringRtrim(materializeStringArgColumn(*table, bound_args[0]));
    case ComputedColumnKind::StringReplace:
      if (bound_args.size() != 3) throw std::runtime_error("REPLACE requires 3 arguments");
      return vectorizedStringReplace(materializeStringArgColumn(*table, bound_args[0]),
                                     materializeStringArgColumn(*table, bound_args[1]),
                                     materializeStringArgColumn(*table, bound_args[2]));
    case ComputedColumnKind::Copy:
      break;
  }
  throw std::runtime_error("unsupported computed column function");
}

std::vector<std::string> splitKey(const std::string& key) {
  std::vector<std::string> out;
  std::string cur;
  for (char c : key) {
    if (c == kGroupDelim) {
      out.push_back(cur);
      cur.clear();
    } else {
      cur.push_back(c);
    }
  }
  out.push_back(cur);
  return out;
}

struct AggregateAccumulator {
  std::vector<double> sum;
  std::vector<std::size_t> count;
  std::vector<Value> minVal;
  std::vector<bool> hasMin;
  std::vector<Value> maxVal;
  std::vector<bool> hasMax;
};

std::vector<std::size_t> allColumnIndices(const Schema& schema) {
  std::vector<std::size_t> indices;
  indices.reserve(schema.fields.size());
  for (std::size_t i = 0; i < schema.fields.size(); ++i) {
    indices.push_back(i);
  }
  return indices;
}

}  // namespace

Table executeAggregateTable(const Table& input, const std::vector<size_t>& key_indices,
                            const std::vector<AggregateSpec>& aggs) {
  std::unordered_map<std::string, AggregateAccumulator> states;
  std::vector<std::string> ordered_keys;
  const auto serialized_keys = materializeSerializedKeys(input, key_indices);

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

  for (std::size_t row_index = 0; row_index < input.rows.size(); ++row_index) {
    const std::string& key = serialized_keys[row_index];
    auto it = states.find(key);
    if (it == states.end()) {
      AggregateAccumulator acc;
      acc.sum.assign(aggs.size(), 0.0);
      acc.count.assign(aggs.size(), 0);
      acc.minVal.resize(aggs.size());
      acc.hasMin.assign(aggs.size(), false);
      acc.maxVal.resize(aggs.size());
      acc.hasMax.assign(aggs.size(), false);
      ordered_keys.push_back(key);
      it = states.emplace(key, std::move(acc)).first;
    }
    auto& acc = it->second;

    for (std::size_t i = 0; i < aggs.size(); ++i) {
      const auto& agg = aggs[i];
      if (agg.function == AggregateFunction::Count) {
        acc.count[i] += 1;
        continue;
      }
      const auto& v = aggregate_columns[i]->values()[row_index];
      if (agg.function == AggregateFunction::Sum || agg.function == AggregateFunction::Avg) {
        acc.sum[i] += v.asDouble();
        acc.count[i] += 1;
      } else if (agg.function == AggregateFunction::Min) {
        if (!acc.hasMin[i] || v < acc.minVal[i]) {
          acc.minVal[i] = v;
          acc.hasMin[i] = true;
        }
      } else if (agg.function == AggregateFunction::Max) {
        if (!acc.hasMax[i] || v > acc.maxVal[i]) {
          acc.maxVal[i] = v;
          acc.hasMax[i] = true;
        }
      }
    }
  }

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
  out.rows.reserve(ordered_keys.size());
  auto cache = std::make_shared<ColumnarTable>();
  cache->schema = out.schema;
  cache->columns.resize(out.schema.fields.size());
  for (auto& column : cache->columns) {
    column.values.reserve(ordered_keys.size());
  }

  for (const auto& key : ordered_keys) {
    const auto& acc = states.at(key);
    Row row;
    row.reserve(out.schema.fields.size());
    std::size_t out_column_index = 0;
    if (!key_indices.empty()) {
      for (const auto& k : splitKey(key)) {
        Value value(k);
        row.push_back(value);
        cache->columns[out_column_index++].values.push_back(std::move(value));
      }
    }
    for (std::size_t i = 0; i < aggs.size(); ++i) {
      const auto& agg = aggs[i];
      Value value;
      switch (agg.function) {
        case AggregateFunction::Sum:
          value = Value(acc.sum[i]);
          break;
        case AggregateFunction::Count:
          value = Value(static_cast<int64_t>(acc.count[i]));
          break;
        case AggregateFunction::Avg:
          value = Value(acc.count[i] == 0 ? 0.0
                                          : acc.sum[i] / static_cast<double>(acc.count[i]));
          break;
        case AggregateFunction::Min:
          value = acc.minVal[i];
          break;
        case AggregateFunction::Max:
          value = acc.maxVal[i];
          break;
      }
      row.push_back(value);
      cache->columns[out_column_index++].values.push_back(std::move(value));
    }
    out.rows.push_back(std::move(row));
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

Table LocalExecutor::execute(const PlanNodePtr& plan) const {
  if (!plan) return Table();

  switch (plan->kind) {
    case PlanKind::Source: {
      const auto* node = static_cast<SourcePlan*>(plan.get());
      return node->table;
    }
    case PlanKind::Select: {
      const auto* node = static_cast<SelectPlan*>(plan.get());
      Table in = execute(node->child);
      return projectTable(in, node->indices, node->aliases);
    }
    case PlanKind::Filter: {
      const auto* node = static_cast<FilterPlan*>(plan.get());
      Table in = execute(node->child);
      const auto input = viewValueColumn(in, node->column_index);
      const auto selection = vectorizedFilterSelection(input, node->value, node->pred);
      return filterTable(in, selection);
    }
    case PlanKind::WithColumn: {
      const auto* node = static_cast<WithColumnPlan*>(plan.get());
      Table in = execute(node->child);
      Table out = std::move(in);
      if (node->function == ComputedColumnKind::Copy) {
        appendNamedColumn(&out, node->added_column,
                          materializeValueColumn(out, node->source_column_index).values);
      } else {
        // Batch-resolve arguments once per node so string builtins keep the same bulk path.
        appendNamedColumn(&out, node->added_column,
                          computeStringColumnValues(&out, node->function, node->args));
      }
      return out;
    }
    case PlanKind::Drop: {
      const auto* node = static_cast<DropPlan*>(plan.get());
      Table in = execute(node->child);
      return projectTable(in, node->keep_indices);
    }
    case PlanKind::Limit: {
      const auto* node = static_cast<LimitPlan*>(plan.get());
      Table in = execute(node->child);
      return limitTable(in, node->n);
    }
    case PlanKind::WindowAssign: {
      const auto* node = static_cast<WindowAssignPlan*>(plan.get());
      Table in = execute(node->child);
      Table out = std::move(in);
      const auto input = viewValueColumn(out, node->time_column_index);
      appendNamedColumn(&out, node->output_column,
                        vectorizedWindowStart(input, node->window_ms));
      return out;
    }
    case PlanKind::Aggregate:
    case PlanKind::GroupBySum: {
      std::vector<size_t> keyIdx;
      std::vector<AggregateSpec> aggs;
      Table aggregateInput;

      if (plan->kind == PlanKind::Aggregate) {
        const auto* node = static_cast<AggregatePlan*>(plan.get());
        keyIdx = node->keys;
        aggs = node->aggregates;
        aggregateInput = execute(node->child);
      } else {
        const auto* node = static_cast<GroupBySumPlan*>(plan.get());
        keyIdx = node->keys;
        AggregateSpec sumSpec;
        sumSpec.function = AggregateFunction::Sum;
        sumSpec.value_index = node->value_index;
        sumSpec.output_name = "sum";
        aggs.push_back(sumSpec);
        aggregateInput = execute(node->child);
      }
      return executeAggregateTable(aggregateInput, keyIdx, aggs);
    }
    case PlanKind::Join: {
      const auto* node = static_cast<JoinPlan*>(plan.get());
      Table left = execute(node->left);
      Table right = execute(node->right);
      const auto left_keys = materializeSerializedKeys(left, {node->left_key});
      const auto right_keys = materializeSerializedKeys(right, {node->right_key});
      const auto left_columns = viewValueColumns(left, allColumnIndices(left.schema));
      const auto right_columns = viewValueColumns(right, allColumnIndices(right.schema));
      Table out;
      out.schema.fields = left.schema.fields;
      out.schema.fields.insert(out.schema.fields.end(), right.schema.fields.begin(), right.schema.fields.end());
      for (size_t i = 0; i < out.schema.fields.size(); ++i) out.schema.index[out.schema.fields[i]] = i;
      auto cache = std::make_shared<ColumnarTable>();
      cache->schema = out.schema;
      cache->columns.resize(out.schema.fields.size());

      const auto rightBuckets = buildHashBuckets(right_keys);

      for (std::size_t left_index = 0; left_index < left.rows.size(); ++left_index) {
        auto hit = rightBuckets.find(left_keys[left_index]);
        if (hit != rightBuckets.end()) {
          for (const auto right_index : hit->second) {
            Row merged;
            merged.reserve(out.schema.fields.size());
            std::size_t out_column_index = 0;
            for (const auto& column : left_columns) {
              const auto& value = column.values()[left_index];
              merged.push_back(value);
              cache->columns[out_column_index++].values.push_back(value);
            }
            for (const auto& column : right_columns) {
              const auto& value = column.values()[right_index];
              merged.push_back(value);
              cache->columns[out_column_index++].values.push_back(value);
            }
            out.rows.push_back(std::move(merged));
          }
        } else if (node->kind == JoinKind::Left) {
          Row merged;
          merged.reserve(out.schema.fields.size());
          std::size_t out_column_index = 0;
          for (const auto& column : left_columns) {
            const auto& value = column.values()[left_index];
            merged.push_back(value);
            cache->columns[out_column_index++].values.push_back(value);
          }
          for (size_t i = 0; i < right.schema.fields.size(); ++i) {
            Value value;
            merged.push_back(value);
            cache->columns[out_column_index++].values.push_back(std::move(value));
          }
          out.rows.push_back(std::move(merged));
        }
      }
      out.columnar_cache = std::move(cache);
      return out;
    }
    case PlanKind::Sink: {
      const auto* node = static_cast<SinkPlan*>(plan.get());
      return execute(node->child);
    }
    default:
      return Table();
  }
}

}  // namespace dataflow
