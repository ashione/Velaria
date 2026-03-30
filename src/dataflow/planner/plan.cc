#include "src/dataflow/planner/plan.h"

#include <cstdint>
#include <cstring>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>

#include "src/dataflow/serial/serializer.h"

namespace dataflow {

namespace {

void appendToken(std::string* out, const std::string& value) {
  out->append(std::to_string(value.size()));
  out->push_back(':');
  out->append(value);
}

std::string readToken(const std::string& payload, std::size_t* offset) {
  if (!offset) {
    throw std::runtime_error("plan decode: offset is null");
  }
  const auto sep = payload.find(':', *offset);
  if (sep == std::string::npos) {
    throw std::runtime_error("plan decode: malformed token");
  }
  const std::size_t len =
      static_cast<std::size_t>(std::stoull(payload.substr(*offset, sep - *offset)));
  const std::size_t begin = sep + 1;
  const std::size_t end = begin + len;
  if (end > payload.size()) {
    throw std::runtime_error("plan decode: truncated token");
  }
  *offset = end;
  return payload.substr(begin, len);
}

void appendSize(std::string* out, std::size_t value) {
  appendToken(out, std::to_string(value));
}

std::size_t readSize(const std::string& payload, std::size_t* offset) {
  return static_cast<std::size_t>(std::stoull(readToken(payload, offset)));
}

void appendInt(std::string* out, int value) {
  appendToken(out, std::to_string(value));
}

int readInt(const std::string& payload, std::size_t* offset) {
  return std::stoi(readToken(payload, offset));
}

bool eqPred(const Value& lhs, const Value& rhs) { return lhs == rhs; }
bool nePred(const Value& lhs, const Value& rhs) { return lhs != rhs; }
bool ltPred(const Value& lhs, const Value& rhs) { return lhs < rhs; }
bool gtPred(const Value& lhs, const Value& rhs) { return lhs > rhs; }
bool ltePred(const Value& lhs, const Value& rhs) { return lhs < rhs || lhs == rhs; }
bool gtePred(const Value& lhs, const Value& rhs) { return lhs > rhs || lhs == rhs; }

bool (*predicateForOp(const std::string& op))(const Value&, const Value&) {
  if (op == "==" || op == "=") return &eqPred;
  if (op == "!=") return &nePred;
  if (op == "<") return &ltPred;
  if (op == ">") return &gtPred;
  if (op == "<=") return &ltePred;
  if (op == ">=") return &gtePred;
  throw std::runtime_error("plan decode: unsupported filter op: " + op);
}

std::string serializeValue(const Value& value) {
  std::string out;
  appendInt(&out, static_cast<int>(value.type()));
  switch (value.type()) {
    case DataType::Nil:
      appendToken(&out, "");
      break;
    case DataType::Int64:
      appendToken(&out, std::to_string(value.asInt64()));
      break;
    case DataType::Double: {
      std::ostringstream oss;
      oss.precision(std::numeric_limits<double>::max_digits10);
      oss << value.asDouble();
      appendToken(&out, oss.str());
      break;
    }
    case DataType::String:
      appendToken(&out, value.asString());
      break;
    case DataType::FixedVector: {
      std::ostringstream oss;
      const auto& vec = value.asFixedVector();
      oss << vec.size();
      for (float v : vec) {
        uint32_t bits = 0;
        std::memcpy(&bits, &v, sizeof(bits));
        oss << ";" << bits;
      }
      appendToken(&out, oss.str());
      break;
    }
  }
  return out;
}

Value deserializeValue(const std::string& payload) {
  std::size_t offset = 0;
  const auto type = static_cast<DataType>(readInt(payload, &offset));
  const std::string raw = readToken(payload, &offset);
  switch (type) {
    case DataType::Nil:
      return Value();
    case DataType::Int64:
      return Value(static_cast<int64_t>(std::stoll(raw)));
    case DataType::Double:
      return Value(std::stod(raw));
    case DataType::String:
      return Value(raw);
    case DataType::FixedVector: {
      std::vector<float> vec;
      std::stringstream ss(raw);
      std::string token;
      if (!std::getline(ss, token, ';')) return Value(vec);
      const std::size_t n = static_cast<std::size_t>(std::stoull(token));
      vec.reserve(n);
      for (std::size_t i = 0; i < n; ++i) {
        if (!std::getline(ss, token, ';')) {
          throw std::runtime_error("plan decode: invalid fixed vector payload");
        }
        const uint32_t bits = static_cast<uint32_t>(std::stoul(token));
        float v = 0.0f;
        std::memcpy(&v, &bits, sizeof(v));
        vec.push_back(v);
      }
      return Value(std::move(vec));
    }
  }
  throw std::runtime_error("plan decode: unsupported value type");
}

void serializeNode(const PlanNodePtr& plan, std::string* out) {
  if (!out) {
    throw std::runtime_error("plan encode: output is null");
  }
  if (!plan) {
    appendInt(out, -1);
    return;
  }

  appendInt(out, static_cast<int>(plan->kind));
  switch (plan->kind) {
    case PlanKind::Source: {
      const auto* node = static_cast<const SourcePlan*>(plan.get());
      const ProtoLikeSerializer serializer;
      appendToken(out, node->source_name);
      appendToken(out, serializer.serialize(node->table));
      return;
    }
    case PlanKind::Select: {
      const auto* node = static_cast<const SelectPlan*>(plan.get());
      serializeNode(node->child, out);
      appendSize(out, node->indices.size());
      for (auto index : node->indices) appendSize(out, index);
      appendSize(out, node->aliases.size());
      for (const auto& alias : node->aliases) appendToken(out, alias);
      return;
    }
    case PlanKind::Filter: {
      const auto* node = static_cast<const FilterPlan*>(plan.get());
      serializeNode(node->child, out);
      appendSize(out, node->column_index);
      appendToken(out, node->op);
      appendToken(out, serializeValue(node->value));
      return;
    }
    case PlanKind::WithColumn: {
      const auto* node = static_cast<const WithColumnPlan*>(plan.get());
      serializeNode(node->child, out);
      appendToken(out, node->added_column);
      appendSize(out, node->source_column_index);
      return;
    }
    case PlanKind::Drop: {
      const auto* node = static_cast<const DropPlan*>(plan.get());
      serializeNode(node->child, out);
      appendSize(out, node->keep_indices.size());
      for (auto index : node->keep_indices) appendSize(out, index);
      return;
    }
    case PlanKind::Limit: {
      const auto* node = static_cast<const LimitPlan*>(plan.get());
      serializeNode(node->child, out);
      appendSize(out, node->n);
      return;
    }
    case PlanKind::WindowAssign: {
      const auto* node = static_cast<const WindowAssignPlan*>(plan.get());
      serializeNode(node->child, out);
      appendSize(out, node->time_column_index);
      appendSize(out, static_cast<std::size_t>(node->window_ms));
      appendToken(out, node->output_column);
      return;
    }
    case PlanKind::GroupBySum: {
      const auto* node = static_cast<const GroupBySumPlan*>(plan.get());
      serializeNode(node->child, out);
      appendSize(out, node->keys.size());
      for (auto key : node->keys) appendSize(out, key);
      appendSize(out, node->value_index);
      return;
    }
    case PlanKind::Aggregate: {
      const auto* node = static_cast<const AggregatePlan*>(plan.get());
      serializeNode(node->child, out);
      appendSize(out, node->keys.size());
      for (auto key : node->keys) appendSize(out, key);
      appendSize(out, node->aggregates.size());
      for (const auto& agg : node->aggregates) {
        appendInt(out, static_cast<int>(agg.function));
        appendSize(out, agg.value_index);
        appendToken(out, agg.output_name);
      }
      return;
    }
    case PlanKind::Join: {
      const auto* node = static_cast<const JoinPlan*>(plan.get());
      serializeNode(node->left, out);
      serializeNode(node->right, out);
      appendSize(out, node->left_key);
      appendSize(out, node->right_key);
      appendInt(out, static_cast<int>(node->kind));
      return;
    }
    case PlanKind::Sink: {
      const auto* node = static_cast<const SinkPlan*>(plan.get());
      serializeNode(node->child, out);
      appendToken(out, node->sink_name);
      return;
    }
  }
  throw std::runtime_error("plan encode: unsupported plan kind");
}

PlanNodePtr deserializeNode(const std::string& payload, std::size_t* offset) {
  const int raw_kind = readInt(payload, offset);
  if (raw_kind < 0) return nullptr;
  const auto kind = static_cast<PlanKind>(raw_kind);
  switch (kind) {
    case PlanKind::Source: {
      const ProtoLikeSerializer serializer;
      const auto source_name = readToken(payload, offset);
      const auto table_payload = readToken(payload, offset);
      return std::make_shared<SourcePlan>(source_name, serializer.deserialize(table_payload));
    }
    case PlanKind::Select: {
      auto child = deserializeNode(payload, offset);
      std::vector<std::size_t> indices;
      const auto index_count = readSize(payload, offset);
      indices.reserve(index_count);
      for (std::size_t i = 0; i < index_count; ++i) indices.push_back(readSize(payload, offset));
      std::vector<std::string> aliases;
      const auto alias_count = readSize(payload, offset);
      aliases.reserve(alias_count);
      for (std::size_t i = 0; i < alias_count; ++i) aliases.push_back(readToken(payload, offset));
      return std::make_shared<SelectPlan>(std::move(child), std::move(indices), std::move(aliases));
    }
    case PlanKind::Filter: {
      auto child = deserializeNode(payload, offset);
      const auto column_index = readSize(payload, offset);
      const auto op = readToken(payload, offset);
      const auto value = deserializeValue(readToken(payload, offset));
      return std::make_shared<FilterPlan>(std::move(child), column_index, value, op, predicateForOp(op));
    }
    case PlanKind::WithColumn: {
      auto child = deserializeNode(payload, offset);
      const auto added_column = readToken(payload, offset);
      const auto source_column_index = readSize(payload, offset);
      return std::make_shared<WithColumnPlan>(std::move(child), added_column, source_column_index);
    }
    case PlanKind::Drop: {
      auto child = deserializeNode(payload, offset);
      std::vector<std::size_t> keep_indices;
      const auto keep_count = readSize(payload, offset);
      keep_indices.reserve(keep_count);
      for (std::size_t i = 0; i < keep_count; ++i) keep_indices.push_back(readSize(payload, offset));
      return std::make_shared<DropPlan>(std::move(child), std::move(keep_indices));
    }
    case PlanKind::Limit: {
      auto child = deserializeNode(payload, offset);
      return std::make_shared<LimitPlan>(std::move(child), readSize(payload, offset));
    }
    case PlanKind::WindowAssign: {
      auto child = deserializeNode(payload, offset);
      const auto time_column_index = readSize(payload, offset);
      const auto window_ms = static_cast<uint64_t>(readSize(payload, offset));
      const auto output_column = readToken(payload, offset);
      return std::make_shared<WindowAssignPlan>(std::move(child), time_column_index, window_ms,
                                                output_column);
    }
    case PlanKind::GroupBySum: {
      auto child = deserializeNode(payload, offset);
      std::vector<std::size_t> keys;
      const auto key_count = readSize(payload, offset);
      keys.reserve(key_count);
      for (std::size_t i = 0; i < key_count; ++i) keys.push_back(readSize(payload, offset));
      const auto value_index = readSize(payload, offset);
      return std::make_shared<GroupBySumPlan>(std::move(child), std::move(keys), value_index);
    }
    case PlanKind::Aggregate: {
      auto child = deserializeNode(payload, offset);
      std::vector<std::size_t> keys;
      const auto key_count = readSize(payload, offset);
      keys.reserve(key_count);
      for (std::size_t i = 0; i < key_count; ++i) keys.push_back(readSize(payload, offset));
      std::vector<AggregateSpec> aggs;
      const auto agg_count = readSize(payload, offset);
      aggs.reserve(agg_count);
      for (std::size_t i = 0; i < agg_count; ++i) {
        AggregateSpec spec;
        spec.function = static_cast<AggregateFunction>(readInt(payload, offset));
        spec.value_index = readSize(payload, offset);
        spec.output_name = readToken(payload, offset);
        aggs.push_back(std::move(spec));
      }
      return std::make_shared<AggregatePlan>(std::move(child), std::move(keys), std::move(aggs));
    }
    case PlanKind::Join: {
      auto left = deserializeNode(payload, offset);
      auto right = deserializeNode(payload, offset);
      const auto left_key = readSize(payload, offset);
      const auto right_key = readSize(payload, offset);
      const auto join_kind = static_cast<JoinKind>(readInt(payload, offset));
      return std::make_shared<JoinPlan>(std::move(left), std::move(right), left_key, right_key, join_kind);
    }
    case PlanKind::Sink: {
      auto child = deserializeNode(payload, offset);
      const auto sink_name = readToken(payload, offset);
      return std::make_shared<SinkPlan>(std::move(child), sink_name);
    }
  }
  throw std::runtime_error("plan decode: unsupported plan kind");
}

}  // namespace

std::string serializePlan(const PlanNodePtr& plan) {
  std::string payload;
  serializeNode(plan, &payload);
  return payload;
}

PlanNodePtr deserializePlan(const std::string& payload) {
  std::size_t offset = 0;
  auto plan = deserializeNode(payload, &offset);
  if (offset != payload.size()) {
    throw std::runtime_error("plan decode: trailing bytes detected");
  }
  return plan;
}

}  // namespace dataflow
