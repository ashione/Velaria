#include "src/dataflow/core/logical/planner/plan.h"

#include <cstdint>
#include <cstring>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>

#include "src/dataflow/core/execution/serial/serializer.h"

namespace dataflow {

namespace {
constexpr int kPlanFormatVersion = 1005;

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

void appendSchema(std::string* out, const Schema& schema) {
  appendSize(out, schema.fields.size());
  for (const auto& field : schema.fields) {
    appendToken(out, field);
  }
}

Schema readSchema(const std::string& payload, std::size_t* offset) {
  std::vector<std::string> fields;
  const auto field_count = readSize(payload, offset);
  fields.reserve(field_count);
  for (std::size_t i = 0; i < field_count; ++i) {
    fields.push_back(readToken(payload, offset));
  }
  return Schema(std::move(fields));
}

std::string serializeLineFileOptions(const LineFileOptions& options) {
  std::string out;
  appendInt(&out, static_cast<int>(options.mode));
  appendInt(&out, static_cast<unsigned char>(options.split_delimiter));
  appendToken(&out, options.regex_pattern);
  appendInt(&out, options.skip_empty_lines ? 1 : 0);
  appendSize(&out, options.mappings.size());
  for (const auto& mapping : options.mappings) {
    appendToken(&out, mapping.column);
    appendSize(&out, mapping.source_index);
  }
  return out;
}

LineFileOptions deserializeLineFileOptions(const std::string& payload) {
  std::size_t offset = 0;
  LineFileOptions options;
  options.mode = static_cast<LineParseMode>(readInt(payload, &offset));
  options.split_delimiter = static_cast<char>(readInt(payload, &offset));
  options.regex_pattern = readToken(payload, &offset);
  options.skip_empty_lines = (readInt(payload, &offset) != 0);
  const auto mapping_count = readSize(payload, &offset);
  options.mappings.reserve(mapping_count);
  for (std::size_t i = 0; i < mapping_count; ++i) {
    LineColumnMapping mapping;
    mapping.column = readToken(payload, &offset);
    mapping.source_index = readSize(payload, &offset);
    options.mappings.push_back(std::move(mapping));
  }
  return options;
}

std::string serializeJsonFileOptions(const JsonFileOptions& options) {
  std::string out;
  appendInt(&out, static_cast<int>(options.format));
  appendSize(&out, options.columns.size());
  for (const auto& col : options.columns) appendToken(&out, col);
  return out;
}

JsonFileOptions deserializeJsonFileOptions(const std::string& payload) {
  std::size_t offset = 0;
  JsonFileOptions options;
  options.format = static_cast<JsonFileFormat>(readInt(payload, &offset));
  const auto count = readSize(payload, &offset);
  options.columns.reserve(count);
  for (std::size_t i = 0; i < count; ++i) {
    options.columns.push_back(readToken(payload, &offset));
  }
  return options;
}

std::string serializeValue(const Value& value) {
  std::string out;
  appendInt(&out, static_cast<int>(value.type()));
  switch (value.type()) {
    case DataType::Nil:
      appendToken(&out, "");
      break;
    case DataType::Bool:
      appendToken(&out, value.asBool() ? "1" : "0");
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
    case DataType::Bool:
      return Value(raw == "1");
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

void serializePredicateExpr(const std::shared_ptr<PlanPredicateExpr>& expr, std::string* out) {
  if (!out) {
    throw std::runtime_error("plan encode: predicate output is null");
  }
  if (!expr) {
    appendInt(out, -1);
    return;
  }
  appendInt(out, static_cast<int>(expr->kind));
  if (expr->kind == PlanPredicateExprKind::Comparison) {
    appendSize(out, expr->comparison.column_index);
    appendToken(out, expr->comparison.op);
    appendInt(out, expr->comparison.rhs_is_column ? 1 : 0);
    appendSize(out, expr->comparison.rhs_column_index);
    appendToken(out, serializeValue(expr->comparison.value));
    return;
  }
  serializePredicateExpr(expr->left, out);
  serializePredicateExpr(expr->right, out);
}

std::shared_ptr<PlanPredicateExpr> deserializePredicateExpr(const std::string& payload,
                                                            std::size_t* offset) {
  const int raw_kind = readInt(payload, offset);
  if (raw_kind < 0) {
    return nullptr;
  }
  auto expr = std::make_shared<PlanPredicateExpr>();
  expr->kind = static_cast<PlanPredicateExprKind>(raw_kind);
  if (expr->kind == PlanPredicateExprKind::Comparison) {
    expr->comparison.column_index = readSize(payload, offset);
    expr->comparison.op = readToken(payload, offset);
    expr->comparison.rhs_is_column = readInt(payload, offset) != 0;
    expr->comparison.rhs_column_index = readSize(payload, offset);
    expr->comparison.value = deserializeValue(readToken(payload, offset));
    return expr;
  }
  expr->left = deserializePredicateExpr(payload, offset);
  expr->right = deserializePredicateExpr(payload, offset);
  return expr;
}

void serializeComputedArg(const ComputedColumnArg& arg, std::string* out) {
  appendInt(out, arg.is_literal ? 1 : 0);
  appendInt(out, arg.is_function ? 1 : 0);
  appendSize(out, arg.source_column_index);
  appendToken(out, serializeValue(arg.literal));
  appendToken(out, arg.source_column_name);
  appendInt(out, static_cast<int>(arg.function));
  appendSize(out, arg.args.size());
  for (const auto& nested : arg.args) {
    serializeComputedArg(nested, out);
  }
}

ComputedColumnArg deserializeComputedArg(const std::string& payload, std::size_t* offset) {
  ComputedColumnArg arg;
  arg.is_literal = (readInt(payload, offset) != 0);
  arg.is_function = (readInt(payload, offset) != 0);
  arg.source_column_index = readSize(payload, offset);
  arg.literal = deserializeValue(readToken(payload, offset));
  arg.source_column_name = readToken(payload, offset);
  arg.function = static_cast<ComputedColumnKind>(readInt(payload, offset));
  const auto arg_count = readSize(payload, offset);
  arg.args.reserve(arg_count);
  for (std::size_t i = 0; i < arg_count; ++i) {
    arg.args.push_back(deserializeComputedArg(payload, offset));
  }
  return arg;
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
      appendToken(out, node->source_name);
      appendInt(out, static_cast<int>(node->storage_kind));
      appendSchema(out, node->schema);
      if (node->storage_kind == SourceStorageKind::InMemory) {
        const ProtoLikeSerializer serializer;
        appendToken(out, serializer.serialize(node->table));
      } else {
        appendToken(out, node->csv_path);
        appendInt(out, static_cast<int>(node->file_source_kind));
        if (node->file_source_kind == FileSourceKind::Csv) {
          appendInt(out, static_cast<unsigned char>(node->csv_delimiter));
        } else if (node->file_source_kind == FileSourceKind::Line) {
          appendToken(out, serializeLineFileOptions(node->line_options));
        } else {
          appendToken(out, serializeJsonFileOptions(node->json_options));
        }
      }
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
      serializePredicateExpr(node->predicate, out);
      return;
    }
    case PlanKind::Union: {
      const auto* node = static_cast<const UnionPlan*>(plan.get());
      serializeNode(node->left, out);
      serializeNode(node->right, out);
      appendInt(out, node->distinct ? 1 : 0);
      return;
    }
    case PlanKind::WithColumn: {
      const auto* node = static_cast<const WithColumnPlan*>(plan.get());
      serializeNode(node->child, out);
      appendToken(out, node->added_column);
      appendSize(out, node->source_column_index);
      appendInt(out, static_cast<int>(node->function));
      appendSize(out, node->args.size());
      for (const auto& arg : node->args) {
        serializeComputedArg(arg, out);
      }
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
    case PlanKind::OrderBy: {
      const auto* node = static_cast<const OrderByPlan*>(plan.get());
      serializeNode(node->child, out);
      appendSize(out, node->indices.size());
      for (auto index : node->indices) appendSize(out, index);
      appendSize(out, node->ascending.size());
      for (bool asc : node->ascending) appendInt(out, asc ? 1 : 0);
      return;
    }
    case PlanKind::WindowAssign: {
      const auto* node = static_cast<const WindowAssignPlan*>(plan.get());
      serializeNode(node->child, out);
      appendSize(out, node->time_column_index);
      appendSize(out, static_cast<std::size_t>(node->window_ms));
      appendSize(out, static_cast<std::size_t>(node->slide_ms));
      appendToken(out, node->output_column);
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

PlanNodePtr deserializeNode(const std::string& payload, std::size_t* offset, int format_version) {
  const int raw_kind = readInt(payload, offset);
  if (raw_kind < 0) return nullptr;
  const auto kind = static_cast<PlanKind>(raw_kind);
  switch (kind) {
    case PlanKind::Source: {
      const auto source_name = readToken(payload, offset);
      const auto storage_kind = static_cast<SourceStorageKind>(readInt(payload, offset));
      const auto schema = readSchema(payload, offset);
      if (storage_kind == SourceStorageKind::InMemory) {
        const ProtoLikeSerializer serializer;
        const auto table_payload = readToken(payload, offset);
        return std::make_shared<SourcePlan>(source_name, serializer.deserialize(table_payload));
      }
      const auto csv_path = readToken(payload, offset);
      const int source_tag = readInt(payload, offset);
      if (source_tag < static_cast<int>(FileSourceKind::Csv) ||
          source_tag > static_cast<int>(FileSourceKind::Json)) {
        const auto csv_delimiter = static_cast<char>(source_tag);
        auto plan = std::make_shared<SourcePlan>(source_name, csv_path, csv_delimiter, schema);
        plan->file_source_kind = FileSourceKind::Csv;
        return plan;
      }
      const auto file_kind = static_cast<FileSourceKind>(source_tag);
      if (file_kind == FileSourceKind::Csv) {
        const auto csv_delimiter = static_cast<char>(readInt(payload, offset));
        auto plan = std::make_shared<SourcePlan>(source_name, csv_path, csv_delimiter, schema);
        plan->file_source_kind = file_kind;
        return plan;
      }
      if (file_kind == FileSourceKind::Line) {
        auto plan = std::make_shared<SourcePlan>(
            source_name, csv_path, schema, deserializeLineFileOptions(readToken(payload, offset)));
        plan->file_source_kind = file_kind;
        return plan;
      }
      auto plan = std::make_shared<SourcePlan>(
          source_name, csv_path, schema, deserializeJsonFileOptions(readToken(payload, offset)));
      plan->file_source_kind = file_kind;
      return plan;
    }
    case PlanKind::Select: {
      auto child = deserializeNode(payload, offset, format_version);
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
      auto child = deserializeNode(payload, offset, format_version);
      auto predicate = deserializePredicateExpr(payload, offset);
      return std::make_shared<FilterPlan>(std::move(child), std::move(predicate));
    }
    case PlanKind::Union: {
      auto left = deserializeNode(payload, offset, format_version);
      auto right = deserializeNode(payload, offset, format_version);
      const bool distinct = readInt(payload, offset) != 0;
      return std::make_shared<UnionPlan>(std::move(left), std::move(right), distinct);
    }
    case PlanKind::WithColumn: {
      auto child = deserializeNode(payload, offset, format_version);
      const auto added_column = readToken(payload, offset);
      const auto source_column_index = readSize(payload, offset);
      const auto function = static_cast<ComputedColumnKind>(readInt(payload, offset));
      const auto arg_count = readSize(payload, offset);
      std::vector<ComputedColumnArg> args;
      args.reserve(arg_count);
      for (std::size_t i = 0; i < arg_count; ++i) {
        args.push_back(deserializeComputedArg(payload, offset));
      }
      if (function == ComputedColumnKind::Copy) {
        auto node = std::make_shared<WithColumnPlan>(std::move(child), added_column, source_column_index);
        node->function = function;
        node->args = std::move(args);
        return node;
      }
      auto node =
          std::make_shared<WithColumnPlan>(std::move(child), added_column, function, std::move(args));
      return node;
    }
    case PlanKind::Drop: {
      auto child = deserializeNode(payload, offset, format_version);
      std::vector<std::size_t> keep_indices;
      const auto keep_count = readSize(payload, offset);
      keep_indices.reserve(keep_count);
      for (std::size_t i = 0; i < keep_count; ++i) keep_indices.push_back(readSize(payload, offset));
      return std::make_shared<DropPlan>(std::move(child), std::move(keep_indices));
    }
    case PlanKind::Limit: {
      auto child = deserializeNode(payload, offset, format_version);
      return std::make_shared<LimitPlan>(std::move(child), readSize(payload, offset));
    }
    case PlanKind::OrderBy: {
      auto child = deserializeNode(payload, offset, format_version);
      std::vector<std::size_t> indices;
      const auto index_count = readSize(payload, offset);
      indices.reserve(index_count);
      for (std::size_t i = 0; i < index_count; ++i) indices.push_back(readSize(payload, offset));
      std::vector<bool> ascending;
      const auto asc_count = readSize(payload, offset);
      ascending.reserve(asc_count);
      for (std::size_t i = 0; i < asc_count; ++i) ascending.push_back(readInt(payload, offset) != 0);
      return std::make_shared<OrderByPlan>(std::move(child), std::move(indices), std::move(ascending));
    }
    case PlanKind::WindowAssign: {
      auto child = deserializeNode(payload, offset, format_version);
      const auto time_column_index = readSize(payload, offset);
      const auto window_ms = static_cast<uint64_t>(readSize(payload, offset));
      const auto slide_ms =
          format_version >= 1004 ? static_cast<uint64_t>(readSize(payload, offset)) : window_ms;
      const auto output_column = readToken(payload, offset);
      return std::make_shared<WindowAssignPlan>(std::move(child), time_column_index, window_ms,
                                                slide_ms, output_column);
    }
    case PlanKind::Aggregate: {
      auto child = deserializeNode(payload, offset, format_version);
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
      auto left = deserializeNode(payload, offset, format_version);
      auto right = deserializeNode(payload, offset, format_version);
      const auto left_key = readSize(payload, offset);
      const auto right_key = readSize(payload, offset);
      const auto join_kind = static_cast<JoinKind>(readInt(payload, offset));
      return std::make_shared<JoinPlan>(std::move(left), std::move(right), left_key, right_key, join_kind);
    }
    case PlanKind::Sink: {
      auto child = deserializeNode(payload, offset, format_version);
      const auto sink_name = readToken(payload, offset);
      return std::make_shared<SinkPlan>(std::move(child), sink_name);
    }
  }
  throw std::runtime_error("plan decode: unsupported plan kind");
}

}  // namespace

std::string serializePlan(const PlanNodePtr& plan) {
  std::string payload;
  appendInt(&payload, kPlanFormatVersion);
  serializeNode(plan, &payload);
  return payload;
}

PlanNodePtr deserializePlan(const std::string& payload) {
  std::size_t offset = 0;
  const auto marker = readInt(payload, &offset);
  if (marker != kPlanFormatVersion) {
    throw std::runtime_error("plan decode: unsupported plan format version: " +
                             std::to_string(marker));
  }

  auto plan = deserializeNode(payload, &offset, marker);
  if (offset != payload.size()) {
    throw std::runtime_error("plan decode: trailing bytes detected");
  }
  return plan;
}

}  // namespace dataflow
