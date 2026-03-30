#include "src/dataflow/core/execution/runtime/executor.h"

#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <cstdint>
#include <unordered_map>
#include <vector>

#include "src/dataflow/experimental/runtime/rpc_runner.h"

namespace dataflow {

namespace {

constexpr char kGroupDelim = '\x1f';

std::string makeKey(const Table& table, const Row& row, const std::vector<std::size_t>& keys) {
  std::string key;
  for (std::size_t i = 0; i < keys.size(); ++i) {
    if (i > 0) key.push_back(kGroupDelim);
    key += row[keys[i]].toString();
  }
  return key;
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

int64_t parseEpochMillis(const Value& value) {
  if (value.isNumber()) {
    return value.asInt64();
  }
  return static_cast<int64_t>(std::stoll(value.toString()));
}

struct AggregateAccumulator {
  std::vector<double> sum;
  std::vector<std::size_t> count;
  std::vector<Value> minVal;
  std::vector<bool> hasMin;
  std::vector<Value> maxVal;
  std::vector<bool> hasMax;
};

}  // namespace

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
      Table out;
      for (size_t idx : node->indices) {
        out.schema.fields.push_back(idx < in.schema.fields.size() ? in.schema.fields[idx] : std::string());
      }
      if (!node->aliases.empty() && node->aliases.size() == node->indices.size()) {
        for (size_t i = 0; i < node->aliases.size(); ++i) {
          out.schema.fields[i] = node->aliases[i];
        }
      }
      for (const auto& row : in.rows) {
        Row projected;
        projected.reserve(node->indices.size());
        for (size_t idx : node->indices) {
          if (idx >= row.size()) {
            throw std::runtime_error("select index out of range");
          }
          projected.push_back(row[idx]);
        }
        out.rows.push_back(std::move(projected));
      }
      for (size_t i = 0; i < out.schema.fields.size(); ++i) {
        out.schema.index[out.schema.fields[i]] = i;
      }
      return out;
    }
    case PlanKind::Filter: {
      const auto* node = static_cast<FilterPlan*>(plan.get());
      Table in = execute(node->child);
      Table out(in.schema, {});
      for (const auto& row : in.rows) {
        if (node->column_index < row.size() && node->pred(row[node->column_index], node->value)) {
          out.rows.push_back(row);
        }
      }
      return out;
    }
    case PlanKind::WithColumn: {
      const auto* node = static_cast<WithColumnPlan*>(plan.get());
      Table in = execute(node->child);
      Table out = in;
      out.schema.fields.push_back(node->added_column);
      out.schema.index[node->added_column] = out.schema.fields.size() - 1;
      for (auto& row : out.rows) {
        row.push_back(row[node->source_column_index]);
      }
      return out;
    }
    case PlanKind::Drop: {
      const auto* node = static_cast<DropPlan*>(plan.get());
      Table in = execute(node->child);
      Table out;
      for (size_t idx : node->keep_indices) {
        out.schema.fields.push_back(in.schema.fields[idx]);
      }
      for (const auto& row : in.rows) {
        Row projected;
        projected.reserve(node->keep_indices.size());
        for (size_t idx : node->keep_indices) {
          projected.push_back(row[idx]);
        }
        out.rows.push_back(std::move(projected));
      }
      for (size_t i = 0; i < out.schema.fields.size(); ++i) {
        out.schema.index[out.schema.fields[i]] = i;
      }
      return out;
    }
    case PlanKind::Limit: {
      const auto* node = static_cast<LimitPlan*>(plan.get());
      Table in = execute(node->child);
      Table out = in;
      if (out.rows.size() > node->n) {
        out.rows.resize(node->n);
      }
      return out;
    }
    case PlanKind::WindowAssign: {
      const auto* node = static_cast<WindowAssignPlan*>(plan.get());
      Table in = execute(node->child);
      if (node->window_ms == 0) {
        throw std::runtime_error("window size cannot be zero");
      }
      Table out = in;
      out.schema.fields.push_back(node->output_column);
      out.schema.index[node->output_column] = out.schema.fields.size() - 1;
      const auto window = static_cast<int64_t>(node->window_ms);
      for (auto& row : out.rows) {
        if (node->time_column_index >= row.size()) {
          throw std::runtime_error("window assign source column out of range");
        }
        const int64_t ts_ms = parseEpochMillis(row[node->time_column_index]);
        const int64_t window_start = (ts_ms / window) * window;
        row.push_back(Value(window_start));
      }
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

      std::unordered_map<std::string, AggregateAccumulator> states;
      std::vector<std::string> orderedKeys;

      for (const auto& row : aggregateInput.rows) {
        std::string key = makeKey(aggregateInput, row, keyIdx);
        auto it = states.find(key);
        if (it == states.end()) {
          AggregateAccumulator acc;
          acc.sum.assign(aggs.size(), 0.0);
          acc.count.assign(aggs.size(), 0);
          acc.minVal.resize(aggs.size());
          acc.hasMin.assign(aggs.size(), false);
          acc.maxVal.resize(aggs.size());
          acc.hasMax.assign(aggs.size(), false);
          orderedKeys.push_back(key);
          it = states.emplace(key, std::move(acc)).first;
        }
        auto& acc = it->second;

        for (size_t i = 0; i < aggs.size(); ++i) {
          const auto& agg = aggs[i];
          if (agg.function == AggregateFunction::Count) {
            acc.count[i] += 1;
            continue;
          }
          if (agg.value_index >= row.size()) {
            throw std::runtime_error("aggregate source column out of range");
          }
          const auto& v = row[agg.value_index];
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
      for (size_t idx : keyIdx) {
        out.schema.fields.push_back(aggregateInput.schema.fields[idx]);
      }
      for (const auto& agg : aggs) out.schema.fields.push_back(agg.output_name);
      for (size_t i = 0; i < out.schema.fields.size(); ++i) {
        out.schema.index[out.schema.fields[i]] = i;
      }

      for (const auto& key : orderedKeys) {
        const auto& acc = states.at(key);
        Row row;
        if (!keyIdx.empty()) {
          for (const auto& k : splitKey(key)) {
            row.push_back(Value(k));
          }
        }
        for (size_t i = 0; i < aggs.size(); ++i) {
          const auto& agg = aggs[i];
          switch (agg.function) {
            case AggregateFunction::Sum:
              row.push_back(Value(acc.sum[i]));
              break;
            case AggregateFunction::Count:
              row.push_back(Value(static_cast<int64_t>(acc.count[i])));
              break;
            case AggregateFunction::Avg:
              if (acc.count[i] == 0) {
                row.push_back(Value(static_cast<double>(0.0)));
              } else {
                row.push_back(Value(acc.sum[i] / static_cast<double>(acc.count[i])));
              }
              break;
            case AggregateFunction::Min:
              row.push_back(acc.minVal[i]);
              break;
            case AggregateFunction::Max:
              row.push_back(acc.maxVal[i]);
              break;
          }
        }
        out.rows.push_back(std::move(row));
      }
      return out;
    }
    case PlanKind::Join: {
      const auto* node = static_cast<JoinPlan*>(plan.get());
      Table left = execute(node->left);
      Table right = execute(node->right);
      Table out;
      out.schema.fields = left.schema.fields;
      out.schema.fields.insert(out.schema.fields.end(), right.schema.fields.begin(), right.schema.fields.end());
      for (size_t i = 0; i < out.schema.fields.size(); ++i) out.schema.index[out.schema.fields[i]] = i;

      std::unordered_map<std::string, std::vector<Row>> rightBuckets;
      for (const auto& row : right.rows) {
        rightBuckets[row[node->right_key].toString()].push_back(row);
      }

      for (const auto& l : left.rows) {
        auto hit = rightBuckets.find(l[node->left_key].toString());
        if (hit != rightBuckets.end()) {
          for (const auto& r : hit->second) {
            Row merged = l;
            merged.insert(merged.end(), r.begin(), r.end());
            out.rows.push_back(std::move(merged));
          }
        } else if (node->kind == JoinKind::Left) {
          Row merged = l;
          for (size_t i = 0; i < right.schema.fields.size(); ++i) merged.push_back(Value());
          out.rows.push_back(std::move(merged));
        }
      }
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
