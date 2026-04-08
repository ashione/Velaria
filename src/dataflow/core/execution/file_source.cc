#include "src/dataflow/core/execution/file_source.h"

#include <array>
#include <charconv>
#include <cctype>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <optional>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <unordered_map>

#include "src/dataflow/core/execution/columnar_batch.h"
#include "src/dataflow/core/execution/csv.h"
#include "src/dataflow/core/logical/planner/plan.h"

namespace dataflow {
namespace {

Value parseScalarCell(std::string raw) {
  if (raw.empty() || raw == "null" || raw == "NULL") return Value();
  if (raw == "true" || raw == "TRUE") return Value(true);
  if (raw == "false" || raw == "FALSE") return Value(false);

  int64_t int_value = 0;
  auto int_result = std::from_chars(raw.data(), raw.data() + raw.size(), int_value);
  if (int_result.ec == std::errc() && int_result.ptr == raw.data() + raw.size()) {
    return Value(int_value);
  }

  char* end = nullptr;
  const double double_value = std::strtod(raw.c_str(), &end);
  if (end != raw.c_str() && *end == '\0') {
    return Value(double_value);
  }
  if (raw.size() >= 2 && raw.front() == '[' && raw.back() == ']') {
    const auto vec = Value::parseFixedVector(raw);
    if (!vec.empty()) {
      return Value(vec);
    }
  }
  return Value(std::move(raw));
}

void appendRow(Table* table, Row row) {
  if (table == nullptr || table->columnar_cache == nullptr) {
    throw std::invalid_argument("table cache is not initialized");
  }
  if (row.size() != table->schema.fields.size()) {
    throw std::runtime_error("row width does not match schema");
  }
  table->rows.push_back(row);
  for (std::size_t i = 0; i < row.size(); ++i) {
    table->columnar_cache->columns[i].values.push_back(row[i]);
  }
  table->columnar_cache->row_count += 1;
}

Table makeColumnFirstTable(const std::vector<std::string>& columns) {
  Table table;
  table.schema = Schema(columns);
  table.columnar_cache = std::make_shared<ColumnarTable>();
  table.columnar_cache->schema = table.schema;
  table.columnar_cache->columns.resize(columns.size());
  table.columnar_cache->arrow_formats.resize(columns.size());
  return table;
}

std::vector<std::string_view> splitByDelimiter(std::string_view line, char delimiter) {
  std::vector<std::string_view> tokens;
  std::size_t start = 0;
  while (start <= line.size()) {
    const auto pos = line.find(delimiter, start);
    if (pos == std::string_view::npos) {
      tokens.push_back(line.substr(start));
      break;
    }
    tokens.push_back(line.substr(start, pos - start));
    start = pos + 1;
  }
  return tokens;
}

class JsonCursor {
 public:
  explicit JsonCursor(std::string_view payload) : payload_(payload) {}

  void skipWhitespace() {
    while (pos_ < payload_.size() && std::isspace(static_cast<unsigned char>(payload_[pos_]))) {
      ++pos_;
    }
  }

  bool consume(char expected) {
    skipWhitespace();
    if (pos_ < payload_.size() && payload_[pos_] == expected) {
      ++pos_;
      return true;
    }
    return false;
  }

  char peek() {
    skipWhitespace();
    if (pos_ >= payload_.size()) return '\0';
    return payload_[pos_];
  }

  std::string parseString() {
    skipWhitespace();
    if (pos_ >= payload_.size() || payload_[pos_] != '"') {
      throw std::runtime_error("json string expected");
    }
    ++pos_;
    std::string out;
    while (pos_ < payload_.size()) {
      const char ch = payload_[pos_++];
      if (ch == '"') {
        return out;
      }
      if (ch == '\\') {
        if (pos_ >= payload_.size()) break;
        const char esc = payload_[pos_++];
        switch (esc) {
          case '"':
          case '\\':
          case '/':
            out.push_back(esc);
            break;
          case 'b':
            out.push_back('\b');
            break;
          case 'f':
            out.push_back('\f');
            break;
          case 'n':
            out.push_back('\n');
            break;
          case 'r':
            out.push_back('\r');
            break;
          case 't':
            out.push_back('\t');
            break;
          default:
            throw std::runtime_error("unsupported json escape");
        }
      } else {
        out.push_back(ch);
      }
    }
    throw std::runtime_error("unterminated json string");
  }

  Value parseValue() {
    skipWhitespace();
    const char ch = peek();
    if (ch == '"') {
      return Value(parseString());
    }
    if (ch == '-' || std::isdigit(static_cast<unsigned char>(ch))) {
      return parseNumber();
    }
    if (ch == 't') {
      expectKeyword("true");
      return Value(true);
    }
    if (ch == 'f') {
      expectKeyword("false");
      return Value(false);
    }
    if (ch == 'n') {
      expectKeyword("null");
      return Value();
    }
    if (ch == '[') {
      return parseNumericArray();
    }
    throw std::runtime_error("unsupported json value kind");
  }

  std::vector<std::pair<std::string, Value>> parseObjectEntries() {
    if (!consume('{')) {
      throw std::runtime_error("json object expected");
    }
    std::vector<std::pair<std::string, Value>> out;
    skipWhitespace();
    if (consume('}')) {
      return out;
    }
    while (true) {
      const std::string key = parseString();
      if (!consume(':')) {
        throw std::runtime_error("json object missing ':'");
      }
      out.push_back({key, parseValue()});
      skipWhitespace();
      if (consume('}')) {
        break;
      }
      if (!consume(',')) {
        throw std::runtime_error("json object missing ','");
      }
    }
    return out;
  }

  std::unordered_map<std::string, Value> parseObject() {
    std::unordered_map<std::string, Value> out;
    for (auto& entry : parseObjectEntries()) {
      out[entry.first] = std::move(entry.second);
    }
    return out;
  }

 private:
  Value parseNumber() {
    skipWhitespace();
    const std::size_t start = pos_;
    if (payload_[pos_] == '-') ++pos_;
    while (pos_ < payload_.size() && std::isdigit(static_cast<unsigned char>(payload_[pos_]))) {
      ++pos_;
    }
    bool is_double = false;
    if (pos_ < payload_.size() && payload_[pos_] == '.') {
      is_double = true;
      ++pos_;
      while (pos_ < payload_.size() && std::isdigit(static_cast<unsigned char>(payload_[pos_]))) {
        ++pos_;
      }
    }
    if (pos_ < payload_.size() && (payload_[pos_] == 'e' || payload_[pos_] == 'E')) {
      is_double = true;
      ++pos_;
      if (pos_ < payload_.size() && (payload_[pos_] == '+' || payload_[pos_] == '-')) ++pos_;
      while (pos_ < payload_.size() && std::isdigit(static_cast<unsigned char>(payload_[pos_]))) {
        ++pos_;
      }
    }
    const std::string token(payload_.substr(start, pos_ - start));
    if (!is_double) {
      int64_t value = 0;
      const auto result = std::from_chars(token.data(), token.data() + token.size(), value);
      if (result.ec == std::errc() && result.ptr == token.data() + token.size()) {
        return Value(value);
      }
    }
    char* end = nullptr;
    const double value = std::strtod(token.c_str(), &end);
    if (end == token.c_str() || *end != '\0') {
      throw std::runtime_error("invalid json number");
    }
    return Value(value);
  }

  Value parseNumericArray() {
    if (!consume('[')) {
      throw std::runtime_error("json array expected");
    }
    std::vector<float> values;
    skipWhitespace();
    if (consume(']')) {
      return Value(values);
    }
    while (true) {
      Value cell = parseValue();
      if (!cell.isNumber()) {
        throw std::runtime_error("json array only supports numeric vectors");
      }
      values.push_back(static_cast<float>(cell.asDouble()));
      skipWhitespace();
      if (consume(']')) {
        break;
      }
      if (!consume(',')) {
        throw std::runtime_error("json array missing ','");
      }
    }
    return Value(values);
  }

  void expectKeyword(const char* keyword) {
    skipWhitespace();
    const std::size_t n = std::char_traits<char>::length(keyword);
    if (payload_.substr(pos_, n) != std::string_view(keyword, n)) {
      throw std::runtime_error("invalid json keyword");
    }
    pos_ += n;
  }

  std::string_view payload_;
  std::size_t pos_ = 0;
};

std::string toLowerCopy(std::string value) {
  for (char& ch : value) {
    ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
  }
  return value;
}

std::string trimCopy(std::string value) {
  const auto begin = value.find_first_not_of(" \t\r\n");
  if (begin == std::string::npos) {
    return "";
  }
  const auto end = value.find_last_not_of(" \t\r\n");
  return value.substr(begin, end - begin + 1);
}

std::string readFilePayload(const std::string& path) {
  std::ifstream input(path, std::ios::binary);
  if (!input.is_open()) {
    throw std::runtime_error("cannot open file: " + path);
  }
  return std::string((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
}

std::vector<std::string> readSampleLines(const std::string& path, std::size_t limit = 8) {
  std::ifstream input(path);
  if (!input.is_open()) {
    throw std::runtime_error("cannot open file: " + path);
  }
  std::vector<std::string> lines;
  std::string line;
  while (lines.size() < limit && std::getline(input, line)) {
    const auto trimmed = trimCopy(line);
    if (!trimmed.empty()) {
      lines.push_back(trimmed);
    }
  }
  return lines;
}

std::string defaultTableNameFromPath(const std::string& path) {
  const auto stem = std::filesystem::path(path).stem().string();
  return stem.empty() ? "input_table" : stem;
}

bool looksLikeIdentifierList(const std::vector<std::string_view>& tokens) {
  if (tokens.empty()) {
    return false;
  }
  for (const auto token_view : tokens) {
    const auto token = trimCopy(std::string(token_view));
    if (token.empty()) {
      return false;
    }
    for (char ch : token) {
      if (!(std::isalnum(static_cast<unsigned char>(ch)) || ch == '_')) {
        return false;
      }
    }
  }
  return true;
}

std::optional<char> chooseDelimitedFormat(const std::vector<std::string>& lines, bool* csv_header) {
  if (csv_header != nullptr) {
    *csv_header = false;
  }
  if (lines.empty()) {
    return std::nullopt;
  }
  constexpr std::array<char, 5> kCandidates = {',', '\t', '|', ';', ' '};
  char best_delimiter = '\0';
  std::size_t best_columns = 0;
  for (const auto delimiter : kCandidates) {
    const auto first_tokens = splitByDelimiter(lines.front(), delimiter);
    if (first_tokens.size() <= 1) {
      continue;
    }
    bool consistent = true;
    for (std::size_t i = 1; i < lines.size(); ++i) {
      if (splitByDelimiter(lines[i], delimiter).size() != first_tokens.size()) {
        consistent = false;
        break;
      }
    }
    if (!consistent) {
      continue;
    }
    if (first_tokens.size() > best_columns) {
      best_columns = first_tokens.size();
      best_delimiter = delimiter;
    }
  }
  if (best_delimiter == '\0') {
    return std::nullopt;
  }
  if (csv_header != nullptr && lines.size() >= 2) {
    const auto first_tokens = splitByDelimiter(lines.front(), best_delimiter);
    const auto second_tokens = splitByDelimiter(lines[1], best_delimiter);
    *csv_header = first_tokens.size() == second_tokens.size() && looksLikeIdentifierList(first_tokens);
  }
  return best_delimiter;
}

JsonFileOptions probeJsonOptions(const std::string& path) {
  JsonFileOptions options;
  const auto payload = readFilePayload(path);
  std::size_t pos = 0;
  while (pos < payload.size() && std::isspace(static_cast<unsigned char>(payload[pos]))) {
    ++pos;
  }
  if (pos >= payload.size()) {
    throw std::runtime_error("json probe found empty input: " + path);
  }
  if (payload[pos] == '[') {
    options.format = JsonFileFormat::JsonArray;
    JsonCursor cursor(payload);
    if (!cursor.consume('[')) {
      throw std::runtime_error("json array source should start with '['");
    }
    if (cursor.consume(']')) {
      return options;
    }
    for (const auto& [key, _] : cursor.parseObjectEntries()) {
      options.columns.push_back(key);
    }
    return options;
  }
  options.format = JsonFileFormat::JsonLines;
  const auto lines = readSampleLines(path, 1);
  if (lines.empty()) {
    throw std::runtime_error("json probe found no objects: " + path);
  }
  JsonCursor cursor(lines.front());
  for (const auto& [key, _] : cursor.parseObjectEntries()) {
    options.columns.push_back(key);
  }
  return options;
}

LineFileOptions probeLineOptions(const std::string& path) {
  const auto lines = readSampleLines(path, 2);
  if (lines.empty()) {
    throw std::runtime_error("line probe found empty input: " + path);
  }
  bool csv_header = false;
  const auto delimiter = chooseDelimitedFormat(lines, &csv_header);
  LineFileOptions options;
  options.mode = LineParseMode::Split;
  options.split_delimiter = delimiter.value_or(' ');
  const auto tokens = splitByDelimiter(lines.front(), options.split_delimiter);
  options.mappings.reserve(tokens.size());
  for (std::size_t i = 0; i < tokens.size(); ++i) {
    options.mappings.push_back(LineColumnMapping{"c" + std::to_string(i), i});
  }
  return options;
}

Row mapJsonObjectToRow(const std::unordered_map<std::string, Value>& object,
                       const JsonFileOptions& options) {
  Row row;
  row.reserve(options.columns.size());
  for (const auto& column : options.columns) {
    const auto it = object.find(column);
    if (it == object.end()) {
      row.emplace_back(Value());
    } else {
      row.emplace_back(it->second);
    }
  }
  return row;
}

Table projectTable(const Table& input, const std::vector<std::size_t>& projected_columns,
                   bool materialize_rows) {
  if (projected_columns.empty() || projected_columns.size() >= input.schema.fields.size()) {
    return input;
  }
  Table projected;
  projected.schema = input.schema;
  projected.columnar_cache = std::make_shared<ColumnarTable>();
  projected.columnar_cache->schema = projected.schema;
  projected.columnar_cache->columns.resize(projected.schema.fields.size());
  projected.columnar_cache->arrow_formats.resize(projected.schema.fields.size());

  std::vector<uint8_t> keep(projected.schema.fields.size(), 0);
  for (const auto index : projected_columns) {
    if (index >= keep.size()) {
      throw std::runtime_error("projected column out of range");
    }
    keep[index] = 1;
  }
  const std::size_t row_count =
      input.columnar_cache ? input.columnar_cache->row_count : input.rowCount();
  for (std::size_t i = 0; i < keep.size(); ++i) {
    if (keep[i] == 0) continue;
    if (input.columnar_cache) {
      projected.columnar_cache->columns[i].values = input.columnar_cache->columns[i].values;
    } else {
      projected.columnar_cache->columns[i].values.reserve(row_count);
      for (const auto& row : input.rows) {
        projected.columnar_cache->columns[i].values.push_back(row[i]);
      }
    }
  }
  projected.columnar_cache->row_count = row_count;
  if (row_count > 0) {
    projected.columnar_cache->batch_row_counts.push_back(row_count);
  }
  if (materialize_rows) {
    projected.rows.reserve(row_count);
    for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
      Row row;
      row.reserve(projected_columns.size());
      for (const auto column_index : projected_columns) {
        row.push_back(projected.columnar_cache->columns[column_index].values[row_index]);
      }
      projected.rows.push_back(std::move(row));
    }
  }
  return projected;
}

bool matchesCompareOp(int compare_result, const std::string& op) {
  if (op == "=" || op == "==") return compare_result == 0;
  if (op == "!=") return compare_result != 0;
  if (op == "<") return compare_result < 0;
  if (op == ">") return compare_result > 0;
  if (op == "<=") return compare_result <= 0;
  if (op == ">=") return compare_result >= 0;
  throw std::invalid_argument("unsupported compare op: " + op);
}

bool compareValueSafe(const Value& lhs, const Value& rhs, const std::string& op) {
  try {
    return matchesCompareOp(lhs == rhs ? 0 : (lhs < rhs ? -1 : 1), op);
  } catch (...) {
    return false;
  }
}

Table applyFilterAndLimit(const Table& input, const SourcePushdownSpec& pushdown,
                          bool materialize_rows) {
  const std::size_t row_count =
      input.columnar_cache ? input.columnar_cache->row_count : input.rowCount();
  Table filtered;
  filtered.schema = input.schema;
  filtered.columnar_cache = std::make_shared<ColumnarTable>();
  filtered.columnar_cache->schema = filtered.schema;
  filtered.columnar_cache->columns.resize(filtered.schema.fields.size());
  filtered.columnar_cache->arrow_formats.resize(filtered.schema.fields.size());

  for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
    if (pushdown.filter.enabled) {
      if (pushdown.filter.column_index >= input.schema.fields.size()) {
        throw std::runtime_error("source pushdown filter column out of range");
      }
      const auto& cell = input.columnar_cache->columns[pushdown.filter.column_index].values[row_index];
      if (!compareValueSafe(cell, pushdown.filter.value, pushdown.filter.op)) {
        continue;
      }
    }
    for (std::size_t col = 0; col < filtered.schema.fields.size(); ++col) {
      filtered.columnar_cache->columns[col].values.push_back(input.columnar_cache->columns[col].values[row_index]);
    }
    filtered.columnar_cache->row_count += 1;
    if (pushdown.limit != 0 && filtered.columnar_cache->row_count >= pushdown.limit) {
      break;
    }
  }

  if (filtered.columnar_cache->row_count > 0) {
    filtered.columnar_cache->batch_row_counts.push_back(filtered.columnar_cache->row_count);
  }
  if (materialize_rows) {
    filtered.rows.reserve(filtered.columnar_cache->row_count);
    for (std::size_t row_index = 0; row_index < filtered.columnar_cache->row_count; ++row_index) {
      Row row;
      row.reserve(filtered.schema.fields.size());
      for (std::size_t col = 0; col < filtered.schema.fields.size(); ++col) {
        row.push_back(filtered.columnar_cache->columns[col].values[row_index]);
      }
      filtered.rows.push_back(std::move(row));
    }
  }
  return filtered;
}

std::string encodeGroupKey(const Row& key_row) {
  std::string out;
  for (const auto& value : key_row) {
    const auto encoded = std::to_string(static_cast<int>(value.type())) + ":" + value.toString();
    out.append(std::to_string(encoded.size()));
    out.push_back('#');
    out.append(encoded);
  }
  return out;
}

bool applyAggregatePushdown(const Table& input, const SourcePushdownSpec& pushdown, Table* out) {
  if (out == nullptr) {
    throw std::invalid_argument("aggregate pushdown output cannot be null");
  }
  if (!pushdown.has_aggregate || pushdown.aggregate.keys.empty() || pushdown.aggregate.aggregates.empty()) {
    return false;
  }
  for (const auto key_index : pushdown.aggregate.keys) {
    if (key_index >= input.schema.fields.size()) return false;
  }
  for (const auto& agg : pushdown.aggregate.aggregates) {
    if (agg.function != AggregateFunction::Count && agg.value_index >= input.schema.fields.size()) {
      return false;
    }
  }

  struct AggState {
    bool initialized = false;
    Value value;
    double sum = 0.0;
    std::size_t count = 0;
  };
  struct GroupState {
    Row key_values;
    std::vector<AggState> agg_states;
  };

  std::unordered_map<std::string, GroupState> groups;
  const std::size_t row_count =
      input.columnar_cache ? input.columnar_cache->row_count : input.rowCount();
  for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
    Row keys;
    keys.reserve(pushdown.aggregate.keys.size());
    for (const auto key_idx : pushdown.aggregate.keys) {
      keys.push_back(input.columnar_cache->columns[key_idx].values[row_index]);
    }
    const auto key = encodeGroupKey(keys);
    auto it = groups.find(key);
    if (it == groups.end()) {
      GroupState state;
      state.key_values = keys;
      state.agg_states.resize(pushdown.aggregate.aggregates.size());
      it = groups.emplace(key, std::move(state)).first;
    }
    for (std::size_t agg_index = 0; agg_index < pushdown.aggregate.aggregates.size(); ++agg_index) {
      const auto& agg = pushdown.aggregate.aggregates[agg_index];
      auto& state = it->second.agg_states[agg_index];
      Value cell;
      if (agg.function != AggregateFunction::Count) {
        cell = input.columnar_cache->columns[agg.value_index].values[row_index];
      }
      switch (agg.function) {
        case AggregateFunction::Count:
          state.count += 1;
          break;
        case AggregateFunction::Sum:
          if (cell.isNumber()) {
            state.sum += cell.asDouble();
          }
          break;
        case AggregateFunction::Avg:
          if (cell.isNumber()) {
            state.sum += cell.asDouble();
            state.count += 1;
          }
          break;
        case AggregateFunction::Min:
          if (!state.initialized || cell < state.value) {
            state.value = cell;
            state.initialized = true;
          }
          break;
        case AggregateFunction::Max:
          if (!state.initialized || cell > state.value) {
            state.value = cell;
            state.initialized = true;
          }
          break;
      }
    }
  }

  std::vector<std::string> out_fields;
  out_fields.reserve(pushdown.aggregate.keys.size() + pushdown.aggregate.aggregates.size());
  for (const auto key_idx : pushdown.aggregate.keys) {
    out_fields.push_back(input.schema.fields[key_idx]);
  }
  for (const auto& agg : pushdown.aggregate.aggregates) {
    out_fields.push_back(agg.output_name);
  }
  Table aggregated = makeColumnFirstTable(out_fields);
  for (auto& entry : groups) {
    Row row = entry.second.key_values;
    row.reserve(out_fields.size());
    for (std::size_t agg_index = 0; agg_index < pushdown.aggregate.aggregates.size(); ++agg_index) {
      const auto& agg = pushdown.aggregate.aggregates[agg_index];
      const auto& state = entry.second.agg_states[agg_index];
      switch (agg.function) {
        case AggregateFunction::Count:
          row.emplace_back(static_cast<int64_t>(state.count));
          break;
        case AggregateFunction::Sum:
          row.emplace_back(state.sum);
          break;
        case AggregateFunction::Avg:
          row.emplace_back(state.count == 0 ? Value() : Value(state.sum / static_cast<double>(state.count)));
          break;
        case AggregateFunction::Min:
        case AggregateFunction::Max:
          row.emplace_back(state.initialized ? state.value : Value());
          break;
      }
    }
    appendRow(&aggregated, std::move(row));
  }
  if (aggregated.columnar_cache->row_count > 0) {
    aggregated.columnar_cache->batch_row_counts.push_back(aggregated.columnar_cache->row_count);
  }
  *out = std::move(aggregated);
  return true;
}

}  // namespace

FileSourceProbeResult probe_file_source(const std::string& path) {
  FileSourceProbeResult result;
  result.path = path;
  result.suggested_table_name = defaultTableNameFromPath(path);
  const auto lower_path = toLowerCopy(path);
  if (lower_path.size() >= 4 &&
      (lower_path.ends_with(".csv") || lower_path.ends_with(".tsv"))) {
    result.kind = FileSourceKind::Csv;
    result.csv_delimiter = lower_path.ends_with(".tsv") ? '\t' : ',';
    result.schema = read_csv_schema(path, result.csv_delimiter);
    return result;
  }
  if (lower_path.ends_with(".json") || lower_path.ends_with(".jsonl") ||
      lower_path.ends_with(".ndjson")) {
    result.kind = FileSourceKind::Json;
    result.json_options = probeJsonOptions(path);
    result.schema = infer_json_file_schema(result.json_options);
    return result;
  }

  const auto lines = readSampleLines(path, 2);
  if (lines.empty()) {
    throw std::runtime_error("probe found empty input: " + path);
  }
  const char first = lines.front().empty() ? '\0' : lines.front().front();
  if (first == '{' || first == '[') {
    result.kind = FileSourceKind::Json;
    result.json_options = probeJsonOptions(path);
    result.schema = infer_json_file_schema(result.json_options);
    return result;
  }

  bool csv_header = false;
  const auto delimiter = chooseDelimitedFormat(lines, &csv_header);
  if (delimiter.has_value() && csv_header) {
    result.kind = FileSourceKind::Csv;
    result.csv_delimiter = *delimiter;
    result.schema = read_csv_schema(path, result.csv_delimiter);
    return result;
  }

  result.kind = FileSourceKind::Line;
  result.line_options = probeLineOptions(path);
  result.schema = infer_line_file_schema(result.line_options);
  return result;
}

Schema infer_line_file_schema(const LineFileOptions& options) {
  std::vector<std::string> columns;
  columns.reserve(options.mappings.size());
  for (const auto& mapping : options.mappings) {
    columns.push_back(mapping.column);
  }
  return Schema(std::move(columns));
}

Schema infer_json_file_schema(const JsonFileOptions& options) {
  return Schema(options.columns);
}

Table load_line_file(const std::string& path, const LineFileOptions& options) {
  if (options.mappings.empty()) {
    throw std::invalid_argument("line source mappings cannot be empty");
  }
  if (options.mode == LineParseMode::Regex && options.regex_pattern.empty()) {
    throw std::invalid_argument("regex mode requires regex_pattern");
  }

  std::vector<std::string> columns;
  columns.reserve(options.mappings.size());
  for (const auto& mapping : options.mappings) {
    columns.push_back(mapping.column);
  }
  Table table = makeColumnFirstTable(columns);

  std::ifstream input(path);
  if (!input.is_open()) {
    throw std::runtime_error("cannot open line file: " + path);
  }

  std::regex line_regex;
  if (options.mode == LineParseMode::Regex) {
    line_regex = std::regex(options.regex_pattern);
  }

  std::string line;
  while (std::getline(input, line)) {
    if (options.skip_empty_lines && line.empty()) {
      continue;
    }
    Row row;
    row.reserve(options.mappings.size());
    if (options.mode == LineParseMode::Split) {
      const auto tokens = splitByDelimiter(line, options.split_delimiter);
      for (const auto& mapping : options.mappings) {
        if (mapping.source_index < tokens.size()) {
          row.push_back(parseScalarCell(std::string(tokens[mapping.source_index])));
        } else {
          row.emplace_back(Value());
        }
      }
    } else {
      std::smatch matched;
      if (!std::regex_match(line, matched, line_regex)) {
        continue;
      }
      for (const auto& mapping : options.mappings) {
        if (mapping.source_index < matched.size()) {
          row.push_back(parseScalarCell(matched[mapping.source_index].str()));
        } else {
          row.emplace_back(Value());
        }
      }
    }
    appendRow(&table, std::move(row));
  }

  if (table.columnar_cache->row_count > 0) {
    table.columnar_cache->batch_row_counts.push_back(table.columnar_cache->row_count);
  }
  return table;
}

Table load_json_file(const std::string& path, const JsonFileOptions& options) {
  if (options.columns.empty()) {
    throw std::invalid_argument("json source columns cannot be empty");
  }
  Table table = makeColumnFirstTable(options.columns);

  std::ifstream input(path);
  if (!input.is_open()) {
    throw std::runtime_error("cannot open json file: " + path);
  }

  if (options.format == JsonFileFormat::JsonLines) {
    std::string line;
    while (std::getline(input, line)) {
      if (line.empty()) {
        continue;
      }
      JsonCursor cursor(line);
      appendRow(&table, mapJsonObjectToRow(cursor.parseObject(), options));
    }
  } else {
    std::string payload((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
    JsonCursor cursor(payload);
    if (!cursor.consume('[')) {
      throw std::runtime_error("json array source should start with '['");
    }
    while (true) {
      if (cursor.consume(']')) {
        break;
      }
      appendRow(&table, mapJsonObjectToRow(cursor.parseObject(), options));
      if (cursor.consume(']')) {
        break;
      }
      if (!cursor.consume(',')) {
        throw std::runtime_error("json array source missing ',' between objects");
      }
    }
  }

  if (table.columnar_cache->row_count > 0) {
    table.columnar_cache->batch_row_counts.push_back(table.columnar_cache->row_count);
  }
  return table;
}

bool execute_file_source_pushdown(const FileSourceConnectorSpec& spec, const Schema& schema,
                                  const SourcePushdownSpec& pushdown, bool materialize_rows,
                                  Table* out) {
  if (out == nullptr) {
    throw std::invalid_argument("source pushdown output cannot be null");
  }
  if (spec.kind == FileSourceKind::Csv) {
    return execute_csv_source_pushdown(spec.path, schema, pushdown, spec.csv_delimiter,
                                       materialize_rows, out);
  }
  Table loaded = (spec.kind == FileSourceKind::Line)
                     ? load_line_file(spec.path, spec.line_options)
                     : load_json_file(spec.path, spec.json_options);
  if (pushdown.filter.enabled || pushdown.limit != 0) {
    loaded = applyFilterAndLimit(loaded, pushdown, materialize_rows);
  }
  if (pushdown.has_aggregate) {
    if (!applyAggregatePushdown(loaded, pushdown, &loaded)) {
      return false;
    }
    if (pushdown.limit != 0 && loaded.rowCount() > pushdown.limit) {
      Table limited;
      limited.schema = loaded.schema;
      limited.columnar_cache = std::make_shared<ColumnarTable>();
      limited.columnar_cache->schema = limited.schema;
      limited.columnar_cache->columns.resize(limited.schema.fields.size());
      limited.columnar_cache->arrow_formats.resize(limited.schema.fields.size());
      for (std::size_t row_index = 0; row_index < pushdown.limit; ++row_index) {
        Row row;
        row.reserve(limited.schema.fields.size());
        for (std::size_t col = 0; col < limited.schema.fields.size(); ++col) {
          row.push_back(loaded.columnar_cache->columns[col].values[row_index]);
        }
        appendRow(&limited, std::move(row));
      }
      if (limited.columnar_cache->row_count > 0) {
        limited.columnar_cache->batch_row_counts.push_back(limited.columnar_cache->row_count);
      }
      loaded = std::move(limited);
    }
    *out = std::move(loaded);
    return true;
  }
  loaded = projectTable(loaded, pushdown.projected_columns, materialize_rows);
  *out = std::move(loaded);
  return true;
}

std::string file_source_format_name(const FileSourceConnectorSpec& spec) {
  switch (spec.kind) {
    case FileSourceKind::Csv:
      return "csv";
    case FileSourceKind::Line:
      return "line";
    case FileSourceKind::Json:
      return "json";
  }
  return "unknown";
}

std::string file_source_options_signature(const FileSourceConnectorSpec& spec) {
  std::ostringstream out;
  if (spec.kind == FileSourceKind::Csv) {
    out << "delimiter=" << spec.csv_delimiter;
  } else if (spec.kind == FileSourceKind::Line) {
    out << "mode=" << (spec.line_options.mode == LineParseMode::Split ? "split" : "regex");
    if (spec.line_options.mode == LineParseMode::Split) {
      out << ";split_delimiter=" << spec.line_options.split_delimiter;
    } else {
      out << ";regex=" << spec.line_options.regex_pattern;
    }
    out << ";columns=";
    for (std::size_t i = 0; i < spec.line_options.mappings.size(); ++i) {
      if (i > 0) out << ",";
      out << spec.line_options.mappings[i].column << ":" << spec.line_options.mappings[i].source_index;
    }
  } else {
    out << "format=" << (spec.json_options.format == JsonFileFormat::JsonLines ? "jsonl" : "json_array");
    out << ";columns=";
    for (std::size_t i = 0; i < spec.json_options.columns.size(); ++i) {
      if (i > 0) out << ",";
      out << spec.json_options.columns[i];
    }
  }
  return out.str();
}

}  // namespace dataflow
