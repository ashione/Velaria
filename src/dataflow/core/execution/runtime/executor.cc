#include "src/dataflow/core/execution/runtime/executor.h"

#include <memory>
#include <algorithm>
#include <cctype>
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

std::string argumentAsString(const Value& value) {
  if (value.type() == DataType::Nil) {
    return std::string();
  }
  if (value.isNumber()) return value.toString();
  if (value.type() == DataType::String) return value.asString();
  return value.toString();
}

int64_t argumentAsInt64(const Value& value) {
  if (value.type() == DataType::Nil) {
    throw std::runtime_error("string function argument cannot be null");
  }
  if (!value.isNumber()) {
    throw std::runtime_error("string function argument must be numeric");
  }
  return value.asInt64();
}

bool hasNullArgument(const std::vector<Value>& values) {
  for (const auto& value : values) {
    if (value.type() == DataType::Nil) {
      return true;
    }
  }
  return false;
}

std::string trimString(std::string input) {
  auto isSpace = [](char ch) { return std::isspace(static_cast<unsigned char>(ch)) != 0; };
  auto start = input.begin();
  while (start != input.end() && isSpace(*start)) ++start;
  auto end = input.end();
  while (end != start && isSpace(*(end - 1))) --end;
  return std::string(start, end);
}

std::string ltrimString(std::string input) {
  auto isSpace = [](char ch) { return std::isspace(static_cast<unsigned char>(ch)) != 0; };
  auto start = input.begin();
  while (start != input.end() && isSpace(*start)) ++start;
  return std::string(start, input.end());
}

std::string rtrimString(std::string input) {
  auto isSpace = [](char ch) { return std::isspace(static_cast<unsigned char>(ch)) != 0; };
  auto end = input.end();
  while (end != input.begin() && isSpace(*(end - 1))) --end;
  return std::string(input.begin(), end);
}

std::string replaceString(std::string source, const std::string& from, const std::string& to) {
  if (from.empty()) {
    return source;
  }
  std::string out = std::move(source);
  std::size_t pos = 0;
  while ((pos = out.find(from, pos)) != std::string::npos) {
    out.replace(pos, from.size(), to);
    pos += to.size();
  }
  return out;
}

std::string lowerString(std::string input) {
  for (char& ch : input) {
    ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
  }
  return input;
}

std::string upperString(std::string input) {
  for (char& ch : input) {
    ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
  }
  return input;
}

std::string reverseString(std::string input) {
  std::reverse(input.begin(), input.end());
  return input;
}

Value evalStringFunction(ComputedColumnKind function, const std::vector<ComputedColumnArg>& args,
                        const Row& row) {
  std::vector<Value> values;
  values.reserve(args.size());
  for (const auto& arg : args) {
    if (arg.is_literal) {
      values.push_back(arg.literal);
    } else {
      if (arg.source_column_index >= row.size()) {
        throw std::runtime_error("computed column source index out of range");
      }
      values.push_back(row[arg.source_column_index]);
    }
  }
  if (hasNullArgument(values)) {
    return Value();
  }
  if (function == ComputedColumnKind::StringLength) {
    if (values.size() != 1) {
      throw std::runtime_error("LENGTH expects 1 argument");
    }
    return Value(static_cast<int64_t>(argumentAsString(values[0]).size()));
  }
  if (function == ComputedColumnKind::StringReverse) {
    if (values.size() != 1) {
      throw std::runtime_error("REVERSE expects 1 argument");
    }
    return Value(reverseString(argumentAsString(values[0])));
  }
  if (function == ComputedColumnKind::StringLower) {
    if (values.size() != 1) {
      throw std::runtime_error("LOWER expects 1 argument");
    }
    return Value(lowerString(argumentAsString(values[0])));
  }
  if (function == ComputedColumnKind::StringUpper) {
    if (values.size() != 1) {
      throw std::runtime_error("UPPER expects 1 argument");
    }
    return Value(upperString(argumentAsString(values[0])));
  }
  if (function == ComputedColumnKind::StringTrim) {
    if (values.size() != 1) {
      throw std::runtime_error("TRIM expects 1 argument");
    }
    return Value(trimString(argumentAsString(values[0])));
  }
  if (function == ComputedColumnKind::StringLtrim) {
    if (values.size() != 1) {
      throw std::runtime_error("LTRIM requires 1 argument");
    }
    return Value(ltrimString(argumentAsString(values[0])));
  }
  if (function == ComputedColumnKind::StringRtrim) {
    if (values.size() != 1) {
      throw std::runtime_error("RTRIM requires 1 argument");
    }
    return Value(rtrimString(argumentAsString(values[0])));
  }
  if (function == ComputedColumnKind::StringConcat) {
    if (values.empty()) {
      throw std::runtime_error("CONCAT expects at least 1 argument");
    }
    std::string out;
    for (const auto& value : values) {
      out += argumentAsString(value);
    }
    return Value(out);
  }
  if (function == ComputedColumnKind::StringConcatWs) {
    if (values.size() < 2) {
      throw std::runtime_error("CONCAT_WS expects at least 2 arguments");
    }
    const auto delim = argumentAsString(values[0]);
    std::string out;
    for (std::size_t i = 1; i < values.size(); ++i) {
      if (i > 1) {
        out += delim;
      }
      out += argumentAsString(values[i]);
    }
    return Value(out);
  }
  if (function == ComputedColumnKind::StringLeft) {
    if (values.size() != 2) {
      throw std::runtime_error("LEFT requires 2 arguments");
    }
    const auto source = argumentAsString(values[0]);
    const auto length = argumentAsInt64(values[1]);
    if (length <= 0) {
      return Value(std::string());
    }
    auto byte_length = static_cast<size_t>(std::max<int64_t>(0, length));
    if (byte_length >= source.size()) {
      return Value(source);
    }
    return Value(source.substr(0, byte_length));
  }
  if (function == ComputedColumnKind::StringRight) {
    if (values.size() != 2) {
      throw std::runtime_error("RIGHT requires 2 arguments");
    }
    const auto source = argumentAsString(values[0]);
    const auto length = argumentAsInt64(values[1]);
    if (length <= 0) {
      return Value(std::string());
    }
    auto byte_length = static_cast<size_t>(std::max<int64_t>(0, length));
    if (byte_length >= source.size()) {
      return Value(source);
    }
    return Value(source.substr(source.size() - byte_length));
  }
  if (function == ComputedColumnKind::StringSubstr) {
    if (values.size() < 2 || values.size() > 3) {
      throw std::runtime_error("SUBSTR expects 2 or 3 arguments");
    }
    const auto source = argumentAsString(values[0]);
    const auto start_one_based = argumentAsInt64(values[1]);
    const auto start = std::max<int64_t>(1, start_one_based);
    if (start > static_cast<int64_t>(source.size())) {
      return Value(std::string());
    }
    auto byte_start = static_cast<size_t>(start - 1);
    if (values.size() == 2) {
      return Value(source.substr(byte_start));
    }
    auto length = argumentAsInt64(values[2]);
    if (length <= 0) {
      return Value(std::string());
    }
    auto end = std::min<size_t>(source.size(), byte_start + static_cast<size_t>(length));
    return Value(source.substr(byte_start, end - byte_start));
  }
  if (function == ComputedColumnKind::StringReplace) {
    if (values.size() != 3) {
      throw std::runtime_error("REPLACE requires 3 arguments");
    }
    return Value(replaceString(argumentAsString(values[0]), argumentAsString(values[1]),
                              argumentAsString(values[2])));
  }
  if (function == ComputedColumnKind::StringPosition) {
    if (values.size() != 2) {
      throw std::runtime_error("POSITION requires 2 arguments");
    }
    const auto needle = argumentAsString(values[0]);
    const auto source = argumentAsString(values[1]);
    const auto position = source.find(needle);
    if (position == std::string::npos) {
      return Value(static_cast<int64_t>(0));
    }
    return Value(static_cast<int64_t>(position + 1));
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
      if (node->function == ComputedColumnKind::Copy) {
        for (auto& row : out.rows) {
          row.push_back(row[node->source_column_index]);
        }
      } else {
        for (auto& row : out.rows) {
          row.push_back(evalStringFunction(node->function, node->args, row));
        }
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
