#include "src/dataflow/core/execution/file_source.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <cctype>
#include <cstdlib>
#include <filesystem>
#include <functional>
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
#include "src/dataflow/core/execution/runtime/simd_dispatch.h"
#include "src/dataflow/core/logical/planner/plan.h"

namespace dataflow {
namespace {

constexpr std::size_t kTextRowSampleBytes = 1 << 15;

bool matchesCompareOp(int compare_result, const std::string& op);
bool compareValueSafe(const Value& lhs, const Value& rhs, const std::string& op);

struct ParsedScalar {
  std::string_view text;
  std::string owned_text;
  bool is_null = false;
  bool is_bool = false;
  bool bool_value = false;
  bool is_int64 = false;
  int64_t int64_value = 0;
  bool is_double = false;
  double double_value = 0.0;
  bool has_full_value = false;
  Value full_value;
};

struct LineParsedScalar {
  std::string_view text;
  bool is_null = false;
  bool is_bool = false;
  bool bool_value = false;
  bool is_int64 = false;
  int64_t int64_value = 0;
  bool is_double = false;
  double double_value = 0.0;
};

struct JsonCapturedFieldLookup {
  std::vector<std::pair<std::string_view, int>> small_entries;
  std::unordered_map<std::string_view, int> large_entries;
  bool use_large_map = false;

  int find(std::string_view key) const {
    if (use_large_map) {
      const auto it = large_entries.find(key);
      return it == large_entries.end() ? -1 : it->second;
    }
    for (const auto& entry : small_entries) {
      if (entry.first == key) {
        return entry.second;
      }
    }
    return -1;
  }
};

enum class SimpleLineRegexCaptureKind {
  Digits = 0,
  Word = 1,
  Rest = 2,
  UntilQuote = 3,
  Bool = 4,
  PrefixedDigits = 5,
};

struct SimpleLineRegexCapture {
  std::string leading_literal;
  SimpleLineRegexCaptureKind kind = SimpleLineRegexCaptureKind::Rest;
  std::string prefix;
};

struct SimpleLineRegexPlan {
  bool supported = false;
  std::vector<SimpleLineRegexCapture> captures;
  std::string trailing_literal;
};

bool parseInt64Scalar(std::string_view raw_view, int64_t* out) {
  if (raw_view.empty() || out == nullptr) {
    return false;
  }
  const auto result = std::from_chars(raw_view.data(), raw_view.data() + raw_view.size(), *out);
  return result.ec == std::errc() && result.ptr == raw_view.data() + raw_view.size();
}

bool parseDoubleScalar(std::string_view raw_view, double* out) {
  if (raw_view.empty() || out == nullptr) {
    return false;
  }
  const std::string raw(raw_view);
  char* end = nullptr;
  *out = std::strtod(raw.c_str(), &end);
  return end != raw.c_str() && *end == '\0';
}

bool mightBeDoubleScalar(std::string_view raw_view) {
  if (raw_view.empty()) {
    return false;
  }
  bool saw_digit = false;
  bool saw_decimal_or_exp = false;
  for (char ch : raw_view) {
    if (ch >= '0' && ch <= '9') {
      saw_digit = true;
      continue;
    }
    if (ch == '.' || ch == 'e' || ch == 'E') {
      saw_decimal_or_exp = true;
      continue;
    }
    if (ch == '+' || ch == '-') {
      continue;
    }
    return false;
  }
  return saw_digit && saw_decimal_or_exp;
}

bool viewStartsWith(std::string_view value, std::string_view prefix) {
  return value.size() >= prefix.size() &&
         value.substr(0, prefix.size()) == prefix;
}

bool viewEndsWith(std::string_view value, std::string_view suffix) {
  return value.size() >= suffix.size() &&
         value.substr(value.size() - suffix.size()) == suffix;
}

std::string decodeRegexLiteral(std::string_view pattern) {
  std::string out;
  out.reserve(pattern.size());
  for (std::size_t i = 0; i < pattern.size(); ++i) {
    if (pattern[i] == '\\' && i + 1 < pattern.size()) {
      ++i;
    }
    out.push_back(pattern[i]);
  }
  return out;
}

std::optional<SimpleLineRegexCaptureKind> classifySimpleLineRegexCapture(
    std::string_view capture, std::string* prefix) {
  if (capture == R"(\d+)") return SimpleLineRegexCaptureKind::Digits;
  if (capture == R"(\w+)") return SimpleLineRegexCaptureKind::Word;
  if (capture == R"(.+)") return SimpleLineRegexCaptureKind::Rest;
  if (capture == R"([^"]+)" || capture == R"([^\"]+)") {
    return SimpleLineRegexCaptureKind::UntilQuote;
  }
  if (capture == "true|false") return SimpleLineRegexCaptureKind::Bool;
  if (capture.size() > 3 && viewEndsWith(capture, R"(\d+)")) {
    if (prefix != nullptr) {
      *prefix = decodeRegexLiteral(capture.substr(0, capture.size() - 3));
    }
    return SimpleLineRegexCaptureKind::PrefixedDigits;
  }
  return std::nullopt;
}

SimpleLineRegexPlan compileSimpleLineRegexPlan(const std::string& pattern) {
  SimpleLineRegexPlan plan;
  if (pattern.size() < 2 || pattern.front() != '^' || pattern.back() != '$') {
    return plan;
  }
  const std::string_view body(pattern.data() + 1, pattern.size() - 2);
  std::size_t cursor = 0;
  std::size_t literal_begin = 0;
  while (cursor < body.size()) {
    if (body[cursor] != '(') {
      if (body[cursor] == '\\' && cursor + 1 < body.size()) {
        cursor += 2;
      } else {
        ++cursor;
      }
      continue;
    }
    const std::string leading = decodeRegexLiteral(body.substr(literal_begin, cursor - literal_begin));
    const std::size_t capture_begin = cursor + 1;
    const std::size_t capture_end = body.find(')', capture_begin);
    if (capture_end == std::string_view::npos) {
      return SimpleLineRegexPlan{};
    }
    std::string prefix;
    const auto kind = classifySimpleLineRegexCapture(
        body.substr(capture_begin, capture_end - capture_begin), &prefix);
    if (!kind.has_value()) {
      return SimpleLineRegexPlan{};
    }
    SimpleLineRegexCapture compiled;
    compiled.leading_literal = leading;
    compiled.kind = *kind;
    compiled.prefix = std::move(prefix);
    plan.captures.push_back(std::move(compiled));
    cursor = capture_end + 1;
    literal_begin = cursor;
  }
  plan.trailing_literal = decodeRegexLiteral(body.substr(literal_begin));
  plan.supported = !plan.captures.empty();
  return plan;
}

bool matchSimpleLineRegex(const SimpleLineRegexPlan& plan, std::string_view line,
                          std::vector<std::string_view>* captures) {
  if (!plan.supported || captures == nullptr) {
    return false;
  }
  captures->assign(plan.captures.size(), std::string_view());
  std::size_t cursor = 0;
  for (std::size_t i = 0; i < plan.captures.size(); ++i) {
    const auto& capture = plan.captures[i];
    if (!viewStartsWith(line.substr(cursor), capture.leading_literal)) {
      return false;
    }
    cursor += capture.leading_literal.size();
    const std::string_view next_literal =
        i + 1 < plan.captures.size() ? std::string_view(plan.captures[i + 1].leading_literal)
                                     : std::string_view(plan.trailing_literal);
    const std::size_t start = cursor;
    switch (capture.kind) {
      case SimpleLineRegexCaptureKind::Digits:
        while (cursor < line.size() && std::isdigit(static_cast<unsigned char>(line[cursor]))) {
          ++cursor;
        }
        break;
      case SimpleLineRegexCaptureKind::Word:
        while (cursor < line.size()) {
          const unsigned char ch = static_cast<unsigned char>(line[cursor]);
          if (!(std::isalnum(ch) || ch == '_')) break;
          ++cursor;
        }
        break;
      case SimpleLineRegexCaptureKind::Bool:
        if (viewStartsWith(line.substr(cursor), "true")) {
          cursor += 4;
        } else if (viewStartsWith(line.substr(cursor), "false")) {
          cursor += 5;
        }
        break;
      case SimpleLineRegexCaptureKind::UntilQuote: {
        const auto pos = line.find('"', cursor);
        cursor = pos == std::string_view::npos ? line.size() : pos;
        break;
      }
      case SimpleLineRegexCaptureKind::PrefixedDigits:
        if (!viewStartsWith(line.substr(cursor), capture.prefix)) {
          return false;
        }
        cursor += capture.prefix.size();
        while (cursor < line.size() && std::isdigit(static_cast<unsigned char>(line[cursor]))) {
          ++cursor;
        }
        break;
      case SimpleLineRegexCaptureKind::Rest:
        if (next_literal.empty()) {
          cursor = line.size();
        } else {
          const auto pos = line.find(next_literal, cursor);
          if (pos == std::string_view::npos) {
            return false;
          }
          cursor = pos;
        }
        break;
    }
    if (cursor == start) {
      return false;
    }
    (*captures)[i] = line.substr(start, cursor - start);
  }
  if (!viewStartsWith(line.substr(cursor), plan.trailing_literal)) {
    return false;
  }
  cursor += plan.trailing_literal.size();
  return cursor == line.size();
}

LineParsedScalar parseLineParsedScalar(std::string_view raw_view) {
  LineParsedScalar parsed;
  parsed.text = raw_view;
  if (raw_view.empty() || raw_view == "null" || raw_view == "NULL") {
    parsed.is_null = true;
    return parsed;
  }
  if (raw_view == "true" || raw_view == "TRUE") {
    parsed.is_bool = true;
    parsed.bool_value = true;
    return parsed;
  }
  if (raw_view == "false" || raw_view == "FALSE") {
    parsed.is_bool = true;
    parsed.bool_value = false;
    return parsed;
  }
  if (parseInt64Scalar(raw_view, &parsed.int64_value)) {
    parsed.is_int64 = true;
    return parsed;
  }
  if (mightBeDoubleScalar(raw_view) && parseDoubleScalar(raw_view, &parsed.double_value)) {
    parsed.is_double = true;
    return parsed;
  }
  return parsed;
}

std::string_view parsedScalarText(const ParsedScalar& parsed) {
  return parsed.owned_text.empty() ? parsed.text : std::string_view(parsed.owned_text);
}

void resetParsedScalarLight(ParsedScalar* parsed) {
  if (parsed == nullptr) {
    return;
  }
  parsed->text = std::string_view();
  parsed->owned_text.clear();
  parsed->is_null = false;
  parsed->is_bool = false;
  parsed->bool_value = false;
  parsed->is_int64 = false;
  parsed->int64_value = 0;
  parsed->is_double = false;
  parsed->double_value = 0.0;
  parsed->has_full_value = false;
  parsed->full_value = Value();
}

void resetTouchedParsedScalars(std::vector<ParsedScalar>* values,
                               std::vector<uint8_t>* touched_flags,
                               std::vector<std::size_t>* touched_indices) {
  if (values == nullptr || touched_flags == nullptr || touched_indices == nullptr) {
    return;
  }
  for (const auto index : *touched_indices) {
    resetParsedScalarLight(&(*values)[index]);
    (*touched_flags)[index] = 0;
  }
  touched_indices->clear();
}

void resetLineParsedScalarVector(std::vector<LineParsedScalar>* values) {
  if (values == nullptr) {
    return;
  }
  for (auto& value : *values) {
    value.text = std::string_view();
    value.is_null = false;
    value.is_bool = false;
    value.bool_value = false;
    value.is_int64 = false;
    value.int64_value = 0;
    value.is_double = false;
    value.double_value = 0.0;
  }
}

Value parseScalarCell(std::string_view raw_view) {
  if (raw_view.empty() || raw_view == "null" || raw_view == "NULL") return Value();
  if (raw_view == "true" || raw_view == "TRUE") return Value(true);
  if (raw_view == "false" || raw_view == "FALSE") return Value(false);

  int64_t int_value = 0;
  if (parseInt64Scalar(raw_view, &int_value)) {
    return Value(int_value);
  }

  double double_value = 0.0;
  if (mightBeDoubleScalar(raw_view) && parseDoubleScalar(raw_view, &double_value)) {
    return Value(double_value);
  }
  const std::string raw(raw_view);
  if (raw.size() >= 2 && raw.front() == '[' && raw.back() == ']') {
    const auto vec = Value::parseFixedVector(raw);
    if (!vec.empty()) {
      return Value(vec);
    }
  }
  return Value(std::move(raw));
}

Value parsedScalarToValue(const ParsedScalar& parsed) {
  if (parsed.has_full_value) return parsed.full_value;
  if (parsed.is_null) return Value();
  if (parsed.is_bool) return Value(parsed.bool_value);
  if (parsed.is_int64) return Value(parsed.int64_value);
  if (parsed.is_double) return Value(parsed.double_value);
  return parseScalarCell(parsedScalarText(parsed));
}

Value lineParsedScalarToValue(const LineParsedScalar& parsed) {
  if (parsed.is_null) return Value();
  if (parsed.is_bool) return Value(parsed.bool_value);
  if (parsed.is_int64) return Value(parsed.int64_value);
  if (parsed.is_double) return Value(parsed.double_value);
  return parseScalarCell(parsed.text);
}

bool tryMatchesParsedScalarFilter(const ParsedScalar& lhs, const Value& rhs, const std::string& op) {
  if (lhs.has_full_value) {
    return compareValueSafe(lhs.full_value, rhs, op);
  }
  if (lhs.is_null) {
    return compareValueSafe(Value(), rhs, op);
  }
  if (lhs.is_bool) {
    return compareValueSafe(Value(lhs.bool_value), rhs, op);
  }
  if (lhs.is_int64) {
    return compareValueSafe(Value(lhs.int64_value), rhs, op);
  }
  if (lhs.is_double) {
    return compareValueSafe(Value(lhs.double_value), rhs, op);
  }
  if (rhs.type() == DataType::String) {
    const auto rhs_text = std::string_view(rhs.asString());
    const int compare_result =
        parsedScalarText(lhs) == rhs_text ? 0 : (parsedScalarText(lhs) < rhs_text ? -1 : 1);
    return matchesCompareOp(compare_result, op);
  }
  return compareValueSafe(Value(std::string(parsedScalarText(lhs))), rhs, op);
}

bool tryMatchesLineParsedScalarFilter(const LineParsedScalar& lhs, const Value& rhs,
                                      const std::string& op) {
  if (lhs.is_null) {
    return compareValueSafe(Value(), rhs, op);
  }
  if (lhs.is_bool) {
    return compareValueSafe(Value(lhs.bool_value), rhs, op);
  }
  if (lhs.is_int64) {
    return compareValueSafe(Value(lhs.int64_value), rhs, op);
  }
  if (lhs.is_double) {
    return compareValueSafe(Value(lhs.double_value), rhs, op);
  }
  if (rhs.type() == DataType::String) {
    const auto rhs_text = std::string_view(rhs.asString());
    const int compare_result = lhs.text == rhs_text ? 0 : (lhs.text < rhs_text ? -1 : 1);
    return matchesCompareOp(compare_result, op);
  }
  return compareValueSafe(Value(std::string(lhs.text)), rhs, op);
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

Table makeColumnFirstTable(const Schema& schema) {
  Table table;
  table.schema = schema;
  table.columnar_cache = std::make_shared<ColumnarTable>();
  table.columnar_cache->schema = table.schema;
  table.columnar_cache->columns.resize(schema.fields.size());
  table.columnar_cache->arrow_formats.resize(schema.fields.size());
  return table;
}

void appendCapturedValues(Table* table, const std::vector<std::size_t>& schema_indices,
                          const std::vector<Value>& values) {
  if (table == nullptr || table->columnar_cache == nullptr) {
    throw std::invalid_argument("table cache is not initialized");
  }
  if (schema_indices.size() != values.size()) {
    throw std::runtime_error("captured row width does not match schema selection");
  }
  for (std::size_t local_index = 0; local_index < schema_indices.size(); ++local_index) {
    table->columnar_cache->columns[schema_indices[local_index]].values.push_back(values[local_index]);
  }
  table->columnar_cache->row_count += 1;
}

std::vector<std::string_view> splitByDelimiter(std::string_view line, char delimiter) {
  std::vector<std::string_view> tokens;
  std::size_t start = 0;
  while (start <= line.size()) {
    const char* begin = line.data() + start;
    const char* end = line.data() + line.size();
    const char* pos = simdDispatch().find_byte(begin, end, delimiter);
    if (pos == nullptr) {
      tokens.push_back(std::string_view(begin, static_cast<std::size_t>(end - begin)));
      break;
    }
    tokens.push_back(std::string_view(begin, static_cast<std::size_t>(pos - begin)));
    start = static_cast<std::size_t>(pos - line.data()) + 1;
  }
  return tokens;
}

struct LineCapturePlan {
  std::size_t max_source_index = 0;
  std::vector<std::vector<std::size_t>> targets_by_source;
};

LineCapturePlan buildLineCapturePlan(const LineFileOptions& options) {
  LineCapturePlan plan;
  for (std::size_t column_index = 0; column_index < options.mappings.size(); ++column_index) {
    plan.max_source_index = std::max(plan.max_source_index, options.mappings[column_index].source_index);
  }
  plan.targets_by_source.resize(plan.max_source_index + 1);
  for (std::size_t column_index = 0; column_index < options.mappings.size(); ++column_index) {
    plan.targets_by_source[options.mappings[column_index].source_index].push_back(column_index);
  }
  return plan;
}

LineCapturePlan buildLineCapturePlan(const LineFileOptions& options,
                                     const std::vector<std::size_t>& schema_indices) {
  LineCapturePlan plan;
  for (const auto schema_index : schema_indices) {
    const auto& mapping = options.mappings[schema_index];
    plan.max_source_index = std::max(plan.max_source_index, mapping.source_index);
  }
  plan.targets_by_source.resize(plan.max_source_index + 1);
  for (std::size_t local_index = 0; local_index < schema_indices.size(); ++local_index) {
    const auto schema_index = schema_indices[local_index];
    plan.targets_by_source[options.mappings[schema_index].source_index].push_back(local_index);
  }
  return plan;
}

void fillLineSplitValues(std::string_view line, const LineCapturePlan& plan,
                         std::vector<Value>* values, char delimiter) {
  if (values == nullptr) {
    throw std::invalid_argument("line split values output cannot be null");
  }
  std::size_t source_index = 0;
  std::size_t start = 0;
  while (source_index <= plan.max_source_index && start <= line.size()) {
    const char* begin = line.data() + start;
    const char* end = line.data() + line.size();
    const char* pos = simdDispatch().find_byte(begin, end, delimiter);
    const auto token = pos == nullptr
                           ? std::string_view(begin, static_cast<std::size_t>(end - begin))
                           : std::string_view(begin, static_cast<std::size_t>(pos - begin));
    if (source_index < plan.targets_by_source.size()) {
      for (const auto target_index : plan.targets_by_source[source_index]) {
        (*values)[target_index] = parseScalarCell(token);
      }
    }
    if (pos == nullptr) {
      break;
    }
    start = static_cast<std::size_t>(pos - line.data()) + 1;
    ++source_index;
  }
}

void fillLineSplitParsedValues(std::string_view line, const LineCapturePlan& plan,
                               std::vector<LineParsedScalar>* values, char delimiter) {
  if (values == nullptr) {
    throw std::invalid_argument("line split parsed values output cannot be null");
  }
  std::size_t source_index = 0;
  std::size_t start = 0;
  while (source_index <= plan.max_source_index && start <= line.size()) {
    const char* begin = line.data() + start;
    const char* end = line.data() + line.size();
    const char* pos = simdDispatch().find_byte(begin, end, delimiter);
    const auto token = pos == nullptr
                           ? std::string_view(begin, static_cast<std::size_t>(end - begin))
                           : std::string_view(begin, static_cast<std::size_t>(pos - begin));
    if (source_index < plan.targets_by_source.size()) {
      for (const auto target_index : plan.targets_by_source[source_index]) {
        (*values)[target_index] = parseLineParsedScalar(token);
      }
    }
    if (pos == nullptr) {
      break;
    }
    start = static_cast<std::size_t>(pos - line.data()) + 1;
    ++source_index;
  }
}

void fillLineRegexParsedValues(const std::vector<std::string_view>& captures,
                               const std::vector<std::size_t>& schema_indices,
                               const LineFileOptions& options,
                               std::vector<LineParsedScalar>* values) {
  if (values == nullptr) {
    throw std::invalid_argument("line regex parsed values output cannot be null");
  }
  for (std::size_t local_index = 0; local_index < schema_indices.size(); ++local_index) {
    const auto schema_index = schema_indices[local_index];
    const auto& mapping = options.mappings[schema_index];
    if (mapping.source_index > 0 &&
        static_cast<std::size_t>(mapping.source_index) <= captures.size()) {
      (*values)[local_index] = parseLineParsedScalar(captures[mapping.source_index - 1]);
    }
  }
}

class JsonCursor {
 public:
  explicit JsonCursor(std::string_view payload) : payload_(payload) {}

  struct StringRef {
    std::string_view view;
    std::string owned;
  };

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

  StringRef parseStringRef() {
    skipWhitespace();
    if (pos_ >= payload_.size() || payload_[pos_] != '"') {
      throw std::runtime_error("json string expected");
    }
    ++pos_;
    const std::size_t start = pos_;
    bool has_escape = false;
    while (pos_ < payload_.size()) {
      const char ch = payload_[pos_];
      if (ch == '"') {
        if (!has_escape) {
          StringRef out;
          out.view = payload_.substr(start, pos_ - start);
          ++pos_;
          return out;
        }
        return StringRef{std::string_view(), parseStringFromEscaped(start)};
      }
      if (ch == '\\') {
        has_escape = true;
        ++pos_;
        if (pos_ >= payload_.size()) {
          break;
        }
      }
      ++pos_;
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
    if (ch == '{') {
      return Value(captureValueLiteral());
    }
    if (ch == '[') {
      return parseNumericArray();
    }
    throw std::runtime_error("unsupported json value kind");
  }

  ParsedScalar parseParsedScalarValue() {
    skipWhitespace();
    const char ch = peek();
    ParsedScalar parsed;
    if (ch == '"') {
      auto text = parseStringRef();
      parsed.text = text.owned.empty() ? text.view : std::string_view();
      parsed.owned_text = std::move(text.owned);
      return parsed;
    }
    if (ch == '-' || std::isdigit(static_cast<unsigned char>(ch))) {
      const std::size_t start = pos_;
      if (payload_[pos_] == '-') ++pos_;
      while (pos_ < payload_.size() && std::isdigit(static_cast<unsigned char>(payload_[pos_]))) {
        ++pos_;
      }
      bool is_double = false;
      if (pos_ < payload_.size() && payload_[pos_] == '.') {
        is_double = true;
        ++pos_;
        while (pos_ < payload_.size() &&
               std::isdigit(static_cast<unsigned char>(payload_[pos_]))) {
          ++pos_;
        }
      }
      if (pos_ < payload_.size() && (payload_[pos_] == 'e' || payload_[pos_] == 'E')) {
        is_double = true;
        ++pos_;
        if (pos_ < payload_.size() && (payload_[pos_] == '+' || payload_[pos_] == '-')) ++pos_;
        while (pos_ < payload_.size() &&
               std::isdigit(static_cast<unsigned char>(payload_[pos_]))) {
          ++pos_;
        }
      }
      const auto token = payload_.substr(start, pos_ - start);
      parsed.text = token;
      if (!is_double && parseInt64Scalar(token, &parsed.int64_value)) {
        parsed.is_int64 = true;
      } else {
        if (!parseDoubleScalar(token, &parsed.double_value)) {
          throw std::runtime_error("invalid json number");
        }
        parsed.is_double = true;
      }
      return parsed;
    }
    if (ch == 't') {
      expectKeyword("true");
      parsed.is_bool = true;
      parsed.bool_value = true;
      return parsed;
    }
    if (ch == 'f') {
      expectKeyword("false");
      parsed.is_bool = true;
      parsed.bool_value = false;
      return parsed;
    }
    if (ch == 'n') {
      expectKeyword("null");
      parsed.is_null = true;
      return parsed;
    }
    if (ch == '{') {
      parsed.owned_text = captureValueLiteral();
      return parsed;
    }
    if (ch == '[') {
      parsed.has_full_value = true;
      parsed.full_value = parseNumericArray();
      return parsed;
    }
    throw std::runtime_error("unsupported json value kind");
  }

  void skipValue() {
    skipWhitespace();
    const char ch = peek();
    if (ch == '"') {
      (void)parseStringRef();
      return;
    }
    if (ch == '-' || std::isdigit(static_cast<unsigned char>(ch))) {
      if (payload_[pos_] == '-') ++pos_;
      while (pos_ < payload_.size() && std::isdigit(static_cast<unsigned char>(payload_[pos_]))) {
        ++pos_;
      }
      if (pos_ < payload_.size() && payload_[pos_] == '.') {
        ++pos_;
        while (pos_ < payload_.size() &&
               std::isdigit(static_cast<unsigned char>(payload_[pos_]))) {
          ++pos_;
        }
      }
      if (pos_ < payload_.size() && (payload_[pos_] == 'e' || payload_[pos_] == 'E')) {
        ++pos_;
        if (pos_ < payload_.size() && (payload_[pos_] == '+' || payload_[pos_] == '-')) ++pos_;
        while (pos_ < payload_.size() &&
               std::isdigit(static_cast<unsigned char>(payload_[pos_]))) {
          ++pos_;
        }
      }
      return;
    }
    if (ch == 't') {
      expectKeyword("true");
      return;
    }
    if (ch == 'f') {
      expectKeyword("false");
      return;
    }
    if (ch == 'n') {
      expectKeyword("null");
      return;
    }
    if (ch == '{') {
      skipObject();
      return;
    }
    if (ch == '[') {
      skipArray();
      return;
    }
    throw std::runtime_error("unsupported json value kind");
  }

  std::string captureValueLiteral() {
    skipWhitespace();
    const std::size_t start = pos_;
    skipValue();
    return std::string(payload_.substr(start, pos_ - start));
  }

  void skipObject() {
    if (!consume('{')) {
      throw std::runtime_error("json object expected");
    }
    skipWhitespace();
    if (consume('}')) {
      return;
    }
    while (true) {
      (void)parseStringRef();
      if (!consume(':')) {
        throw std::runtime_error("json object missing ':'");
      }
      skipValue();
      skipWhitespace();
      if (consume('}')) {
        break;
      }
      if (!consume(',')) {
        throw std::runtime_error("json object missing ','");
      }
    }
  }

  void skipArray() {
    if (!consume('[')) {
      throw std::runtime_error("json array expected");
    }
    skipWhitespace();
    if (consume(']')) {
      return;
    }
    while (true) {
      skipValue();
      skipWhitespace();
      if (consume(']')) {
        break;
      }
      if (!consume(',')) {
        throw std::runtime_error("json array missing ','");
      }
    }
  }

  template <typename Fn>
  void forEachObjectEntry(Fn&& on_entry) {
    if (!consume('{')) {
      throw std::runtime_error("json object expected");
    }
    skipWhitespace();
    if (consume('}')) {
      return;
    }
    while (true) {
      const std::string key = parseString();
      if (!consume(':')) {
        throw std::runtime_error("json object missing ':'");
      }
      on_entry(key, parseValue());
      skipWhitespace();
      if (consume('}')) {
        break;
      }
      if (!consume(',')) {
        throw std::runtime_error("json object missing ','");
      }
    }
  }

  template <typename Fn>
  void forEachObjectParsedEntry(Fn&& on_entry) {
    if (!consume('{')) {
      throw std::runtime_error("json object expected");
    }
    skipWhitespace();
    if (consume('}')) {
      return;
    }
    while (true) {
      StringRef key = parseStringRef();
      if (!consume(':')) {
        throw std::runtime_error("json object missing ':'");
      }
      on_entry(key, this);
      skipWhitespace();
      if (consume('}')) {
        break;
      }
      if (!consume(',')) {
        throw std::runtime_error("json object missing ','");
      }
    }
  }

  std::vector<std::pair<std::string, Value>> parseObjectEntries() {
    std::vector<std::pair<std::string, Value>> out;
    forEachObjectEntry([&](std::string key, Value value) {
      out.push_back({std::move(key), std::move(value)});
    });
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
  std::string parseStringFromEscaped(std::size_t start) {
    std::string out;
    out.reserve(pos_ - start);
    std::size_t cursor = start;
    while (cursor < payload_.size()) {
      const char ch = payload_[cursor++];
      if (ch == '"') {
        pos_ = cursor;
        return out;
      }
      if (ch == '\\') {
        if (cursor >= payload_.size()) break;
        const char esc = payload_[cursor++];
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

std::string readFilePrefix(const std::string& path, std::size_t limit = 4096) {
  std::ifstream input(path, std::ios::binary);
  if (!input.is_open()) {
    throw std::runtime_error("cannot open file: " + path);
  }
  std::string prefix(limit, '\0');
  input.read(prefix.data(), static_cast<std::streamsize>(limit));
  prefix.resize(static_cast<std::size_t>(input.gcount()));
  return prefix;
}

std::size_t estimateTextRows(const std::string& path) {
  std::error_code ec;
  const auto file_size = std::filesystem::file_size(path, ec);
  if (ec || file_size == 0) {
    return 0;
  }
  std::ifstream input(path, std::ios::binary);
  if (!input.is_open()) {
    return 0;
  }
  std::string sample;
  sample.resize(std::min<std::size_t>(file_size, kTextRowSampleBytes));
  input.read(sample.data(), static_cast<std::streamsize>(sample.size()));
  sample.resize(static_cast<std::size_t>(input.gcount()));
  if (sample.empty()) {
    return 0;
  }
  std::size_t line_count = 0;
  for (const char ch : sample) {
    if (ch == '\n') {
      ++line_count;
    }
  }
  if (line_count == 0) {
    return 0;
  }
  const std::size_t avg_line_bytes =
      std::max<std::size_t>(1, sample.size() / line_count);
  return std::max<std::size_t>(1, file_size / avg_line_bytes);
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

bool looksLikeJsonObjectLines(const std::vector<std::string>& lines) {
  if (lines.empty()) {
    return false;
  }
  for (const auto& line : lines) {
    if (line.empty() || line.front() != '{' || line.back() != '}') {
      return false;
    }
  }
  return true;
}

bool looksLikeJsonArrayPayload(const std::vector<std::string>& lines) {
  if (lines.empty()) {
    return false;
  }
  return lines.front().front() == '[';
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

std::string delimiterFormatName(char delimiter, bool csv_header) {
  if (csv_header) {
    if (delimiter == '\t') return "tsv";
    return "csv";
  }
  return "line_split";
}

std::string delimiterReason(char delimiter, std::size_t columns, bool csv_header) {
  std::ostringstream out;
  out << "delimiter=";
  if (delimiter == '\t') {
    out << "\\t";
  } else if (delimiter == ' ') {
    out << "space";
  } else {
    out << delimiter;
  }
  out << ";columns=" << columns;
  out << ";header=" << (csv_header ? "true" : "false");
  return out.str();
}

std::string joinStrings(const std::vector<std::string>& values, const std::string& delimiter) {
  std::ostringstream out;
  for (std::size_t i = 0; i < values.size(); ++i) {
    if (i > 0) {
      out << delimiter;
    }
    out << values[i];
  }
  return out.str();
}

FileSourceProbeCandidate makeCandidate(std::string format_name, FileSourceKind kind, int score,
                                       std::vector<std::string> evidence) {
  FileSourceProbeCandidate candidate;
  candidate.format_name = std::move(format_name);
  candidate.kind = kind;
  candidate.score = score;
  candidate.evidence = std::move(evidence);
  candidate.reason = joinStrings(candidate.evidence, "; ");
  return candidate;
}

std::vector<FileSourceProbeCandidate> buildProbeCandidates(
    const std::string& path, const std::vector<std::string>& lines) {
  std::vector<FileSourceProbeCandidate> candidates;
  const auto lower_path = toLowerCopy(path);

  const bool csv_ext = lower_path.ends_with(".csv");
  const bool tsv_ext = lower_path.ends_with(".tsv");
  const bool json_ext = lower_path.ends_with(".json");
  const bool jsonl_ext = lower_path.ends_with(".jsonl") || lower_path.ends_with(".ndjson");
  const bool line_ext = lower_path.ends_with(".log") || lower_path.ends_with(".txt") ||
                        lower_path.ends_with(".lines");

  if (!lines.empty()) {
    if (looksLikeJsonObjectLines(lines)) {
      candidates.push_back(makeCandidate(
          "json_lines", FileSourceKind::Json, 92 + (json_ext || jsonl_ext ? 8 : 0),
          jsonl_ext
              ? std::vector<std::string>{"content starts with object", "newline-delimited objects",
                                         "jsonl/ndjson extension"}
              : std::vector<std::string>{"content starts with object",
                                         "newline-delimited objects"}));
    } else if (looksLikeJsonArrayPayload(lines)) {
      candidates.push_back(makeCandidate(
          "json_array", FileSourceKind::Json, 94 + (json_ext ? 8 : 0),
          json_ext ? std::vector<std::string>{"content starts with array", "json extension"}
                   : std::vector<std::string>{"content starts with array"}));
    }

    bool csv_header = false;
    const auto delimiter = chooseDelimitedFormat(lines, &csv_header);
    if (delimiter.has_value()) {
      const auto columns = splitByDelimiter(lines.front(), *delimiter).size();
      const int base_score = static_cast<int>(40 + std::min<std::size_t>(columns, 12) * 3);
      const int header_bonus = csv_header ? 25 : 0;
      const bool matching_tabular_ext =
          ((*delimiter == ',' && csv_ext) || (*delimiter == '\t' && tsv_ext));
      const bool delimiter_prefers_line = *delimiter != ',' && *delimiter != '\t';
      const int ext_bonus = matching_tabular_ext ? 20 : 0;
      const int line_bonus = csv_header ? -10 : (delimiter_prefers_line ? 24 : 8);
      if (csv_header) {
        candidates.push_back(makeCandidate(delimiterFormatName(*delimiter, true),
                                           FileSourceKind::Csv,
                                           base_score + header_bonus + ext_bonus,
                                           {delimiterReason(*delimiter, columns, true),
                                            ext_bonus > 0 ? "matching delimited extension"
                                                          : "header-shaped first row"}));
      } else if (matching_tabular_ext) {
        candidates.push_back(makeCandidate(delimiterFormatName(*delimiter, true),
                                           FileSourceKind::Csv,
                                           base_score + 28 + ext_bonus,
                                           {delimiterReason(*delimiter, columns, false),
                                            "stable delimited columns with matching extension"}));
      }
      std::vector<std::string> line_evidence = {
          delimiterReason(*delimiter, columns, false),
          csv_header ? "header not trusted for line fallback"
                     : "stable split columns without header",
      };
      if (line_ext && delimiter_prefers_line) {
        line_evidence.push_back("line-oriented extension");
      }
      candidates.push_back(makeCandidate(delimiterFormatName(*delimiter, false),
                                         FileSourceKind::Line,
                                         base_score + line_bonus + (csv_header ? 0 : ext_bonus / 2) +
                                             (line_ext && delimiter_prefers_line ? 8 : 0),
                                         std::move(line_evidence)));
    } else {
      candidates.push_back(makeCandidate("line_split", FileSourceKind::Line, 15,
                                         {"no stable delimiter match",
                                          "fallback line split candidate"}));
    }
  }

  if (csv_ext) {
    candidates.push_back(
        makeCandidate("csv", FileSourceKind::Csv, 55, {"csv extension fallback"}));
  }
  if (tsv_ext) {
    candidates.push_back(
        makeCandidate("tsv", FileSourceKind::Csv, 55, {"tsv extension fallback"}));
  }
  if (json_ext && std::none_of(candidates.begin(), candidates.end(), [](const auto& candidate) {
        return candidate.kind == FileSourceKind::Json;
      })) {
    candidates.push_back(
        makeCandidate("json_lines", FileSourceKind::Json, 58, {"json extension fallback"}));
  }
  if (jsonl_ext && std::none_of(candidates.begin(), candidates.end(), [](const auto& candidate) {
        return candidate.format_name == "json_lines";
      })) {
    candidates.push_back(makeCandidate("json_lines", FileSourceKind::Json, 60,
                                       {"jsonl/ndjson extension fallback"}));
  }

  std::sort(candidates.begin(), candidates.end(),
            [](const auto& lhs, const auto& rhs) { return lhs.score > rhs.score; });
  return candidates;
}

std::string probeConfidence(const std::vector<FileSourceProbeCandidate>& candidates) {
  if (candidates.empty()) {
    return "low";
  }
  const int top = candidates.front().score;
  const int second = candidates.size() >= 2 ? candidates[1].score : 0;
  const int gap = top - second;
  if (top >= 90 && gap >= 15) {
    return "high";
  }
  if (top >= 65 && gap >= 8) {
    return "medium";
  }
  return "low";
}

std::vector<std::string> probeWarnings(const std::vector<FileSourceProbeCandidate>& candidates,
                                       const std::string& confidence) {
  std::vector<std::string> warnings;
  if (candidates.empty()) {
    warnings.push_back("probe found no candidates");
    return warnings;
  }
  if (confidence == "low") {
    warnings.push_back("probe confidence is low; consider explicit USING ... OPTIONS(...)");
  }
  if (candidates.size() >= 2 && (candidates[0].score - candidates[1].score) < 8) {
    warnings.push_back("top probe candidates are close in score");
  }
  if (candidates.front().score < 40) {
    warnings.push_back("probe selected a weak fallback candidate");
  }
  return warnings;
}

JsonFileOptions probeJsonOptions(const std::string& path) {
  JsonFileOptions options;
  const auto prefix = readFilePrefix(path);
  std::size_t pos = 0;
  while (pos < prefix.size() && std::isspace(static_cast<unsigned char>(prefix[pos]))) {
    ++pos;
  }
  if (pos >= prefix.size()) {
    throw std::runtime_error("json probe found empty input: " + path);
  }
  if (prefix[pos] == '[') {
    options.format = JsonFileFormat::JsonArray;
    const auto payload = readFilePayload(path);
    JsonCursor cursor(payload);
    if (!cursor.consume('[')) {
      throw std::runtime_error("json array source should start with '['");
    }
    if (cursor.consume(']')) {
      return options;
    }
    cursor.forEachObjectEntry([&](std::string key, Value) { options.columns.push_back(std::move(key)); });
    return options;
  }
  options.format = JsonFileFormat::JsonLines;
  const auto lines = readSampleLines(path, 1);
  if (lines.empty()) {
    throw std::runtime_error("json probe found no objects: " + path);
  }
  JsonCursor cursor(lines.front());
  cursor.forEachObjectEntry([&](std::string key, Value) { options.columns.push_back(std::move(key)); });
  return options;
}

struct AggState {
  bool initialized = false;
  Value value;
  double sum = 0.0;
  std::size_t count = 0;
};

struct GroupState {
  std::vector<AggState> agg_states;
};

struct GroupKey {
  Row values;
};

enum class CompiledPredicateStepKind {
  Comparison = 0,
  And = 1,
  Or = 2,
};

struct CompiledPredicateStep {
  CompiledPredicateStepKind kind = CompiledPredicateStepKind::Comparison;
  std::size_t schema_index = 0;
  int local_index = -1;
  Value value;
  std::string op;
};

struct AggregateAccessPlan {
  std::vector<std::size_t> schema_indices;
  std::vector<int> local_by_schema;
  std::vector<std::size_t> local_key_indices;
  std::vector<int> local_value_by_aggregate;
  std::vector<CompiledPredicateStep> predicate_steps;
};

struct SelectionAccessPlan {
  std::vector<std::size_t> schema_indices;
  std::vector<int> local_by_schema;
  std::vector<std::size_t> output_schema_indices;
  std::vector<CompiledPredicateStep> predicate_steps;
};

struct GroupKeyHash {
  std::size_t operator()(const GroupKey& key) const;
};

struct GroupKeyEq {
  bool operator()(const GroupKey& lhs, const GroupKey& rhs) const;
};

using AggregateGroupMap = std::unordered_map<GroupKey, GroupState, GroupKeyHash, GroupKeyEq>;

struct SingleGroupKeyHash {
  std::size_t operator()(const Value& value) const;
};

struct SingleGroupKeyEq {
  bool operator()(const Value& lhs, const Value& rhs) const;
};

using SingleAggregateGroupMap =
    std::unordered_map<Value, GroupState, SingleGroupKeyHash, SingleGroupKeyEq>;
using SingleCountGroupMap =
    std::unordered_map<Value, std::size_t, SingleGroupKeyHash, SingleGroupKeyEq>;

struct SingleNumericAggregateState {
  double sum = 0.0;
  std::size_t count = 0;
  bool has_value = false;
  Value value;
};

using SingleNumericAggregateGroupMap =
    std::unordered_map<Value, SingleNumericAggregateState, SingleGroupKeyHash, SingleGroupKeyEq>;

bool validateAggregatePushdown(const Schema& schema, const SourcePushdownSpec& pushdown);
AggregateAccessPlan buildAggregateAccessPlan(const Schema& schema, const SourcePushdownSpec& pushdown);
SelectionAccessPlan buildSelectionAccessPlan(const Schema& schema,
                                             const SourcePushdownSpec& pushdown);
void collectPredicateSchemaIndices(const std::shared_ptr<PlanPredicateExpr>& expr,
                                   const std::function<void(std::size_t)>& visit);
void appendCompiledPredicateSteps(const std::shared_ptr<PlanPredicateExpr>& expr,
                                  const std::vector<int>& local_by_schema,
                                  std::vector<CompiledPredicateStep>* out);
bool evaluateCompiledPredicateValues(const std::vector<Value>& values,
                                     const std::vector<CompiledPredicateStep>& steps,
                                     std::vector<uint8_t>* scratch);
bool evaluateCompiledPredicateParsed(const std::vector<ParsedScalar>& values,
                                     const std::vector<CompiledPredicateStep>& steps,
                                     std::vector<uint8_t>* scratch);
bool evaluateCompiledPredicateLineParsed(const std::vector<LineParsedScalar>& values,
                                         const std::vector<CompiledPredicateStep>& steps,
                                         std::vector<uint8_t>* scratch);
bool evaluateCompiledPredicateRow(const Table& input, std::size_t row_index,
                                  const std::vector<CompiledPredicateStep>& steps,
                                  std::vector<uint8_t>* scratch);
bool includeRowForSelection(const std::vector<Value>& values,
                            const SelectionAccessPlan& access_plan,
                            const SourcePushdownSpec& pushdown,
                            std::vector<uint8_t>* predicate_scratch);
bool includeParsedRowForSelection(const std::vector<ParsedScalar>& values,
                                  const SelectionAccessPlan& access_plan,
                                  const SourcePushdownSpec& pushdown,
                                  std::vector<uint8_t>* predicate_scratch);
bool includeLineParsedRowForSelection(const std::vector<LineParsedScalar>& values,
                                      const SelectionAccessPlan& access_plan,
                                      const SourcePushdownSpec& pushdown,
                                      std::vector<uint8_t>* predicate_scratch);
void appendSelectionRow(Table* out, const SelectionAccessPlan& access_plan,
                        const std::vector<Value>& values, bool materialize_rows);
bool includeRowForAggregate(const std::vector<Value>& values, const AggregateAccessPlan& access_plan,
                            const SourcePushdownSpec& pushdown,
                            std::vector<uint8_t>* predicate_scratch);
bool includeParsedRowForAggregate(const std::vector<ParsedScalar>& values,
                                  const AggregateAccessPlan& access_plan,
                                  const SourcePushdownSpec& pushdown,
                                  std::vector<uint8_t>* predicate_scratch);
bool includeLineParsedRowForAggregate(const std::vector<LineParsedScalar>& values,
                                      const AggregateAccessPlan& access_plan,
                                      const SourcePushdownSpec& pushdown,
                                      std::vector<uint8_t>* predicate_scratch);
void accumulateAggregateRow(const std::vector<Value>& values, const AggregateAccessPlan& access_plan,
                            const SourcePushdownSpec& pushdown,
                            AggregateGroupMap* groups);
void accumulateSingleKeyAggregateRow(const std::vector<Value>& values,
                                     const AggregateAccessPlan& access_plan,
                                     const SourcePushdownSpec& pushdown,
                                     SingleAggregateGroupMap* groups);
void updateAggregateStatesParsed(const std::vector<ParsedScalar>& values,
                                 const AggregateAccessPlan& access_plan,
                                 const SourcePushdownSpec& pushdown, GroupState* group);
void updateAggregateStatesLineParsed(const std::vector<LineParsedScalar>& values,
                                     const AggregateAccessPlan& access_plan,
                                     const SourcePushdownSpec& pushdown, GroupState* group);
Table finalizeAggregatedGroups(const Schema& schema, const SourcePushdownSpec& pushdown,
                               AggregateGroupMap& groups, bool materialize_rows);
Table finalizeSingleKeyAggregatedGroups(const Schema& schema, const SourcePushdownSpec& pushdown,
                                        SingleAggregateGroupMap& groups, bool materialize_rows);
Table finalizeSingleCountGroups(const Schema& schema, const SourcePushdownSpec& pushdown,
                                SingleCountGroupMap& groups, bool materialize_rows);
Table finalizeSingleNumericAggregateGroups(const Schema& schema,
                                           const SourcePushdownSpec& pushdown,
                                           SingleNumericAggregateGroupMap& groups,
                                           bool materialize_rows);

void resetValues(std::vector<Value>* values) {
  if (values == nullptr) {
    throw std::invalid_argument("values buffer cannot be null");
  }
  std::fill(values->begin(), values->end(), Value());
}

std::vector<std::size_t> buildCapturedSchemaIndices(const Schema& schema,
                                                    const SourcePushdownSpec& pushdown) {
  std::vector<uint8_t> capture_mask(schema.fields.size(), static_cast<uint8_t>(0));
  auto capture_schema_index = [&](std::size_t schema_index) {
    if (schema_index >= capture_mask.size()) {
      throw std::runtime_error("source pushdown capture column out of range");
    }
    capture_mask[schema_index] = 1;
  };

  if (pushdown.has_aggregate) {
    for (const auto key_index : pushdown.aggregate.keys) {
      capture_schema_index(key_index);
    }
    for (const auto& agg : pushdown.aggregate.aggregates) {
      if (agg.function != AggregateFunction::Count) {
        capture_schema_index(agg.value_index);
      }
    }
  } else {
    const bool require_all_columns =
        pushdown.projected_columns.empty() || pushdown.projected_columns.size() >= schema.fields.size();
    if (require_all_columns) {
      std::fill(capture_mask.begin(), capture_mask.end(), static_cast<uint8_t>(1));
    } else {
      for (const auto column_index : pushdown.projected_columns) {
        capture_schema_index(column_index);
      }
    }
  }

  for (const auto& filter : pushdown.filters) {
    capture_schema_index(filter.column_index);
  }
  collectPredicateSchemaIndices(pushdown.predicate_expr, capture_schema_index);

  std::vector<std::size_t> schema_indices;
  schema_indices.reserve(schema.fields.size());
  for (std::size_t schema_index = 0; schema_index < capture_mask.size(); ++schema_index) {
    if (capture_mask[schema_index] != 0) {
      schema_indices.push_back(schema_index);
    }
  }
  return schema_indices;
}

SelectionAccessPlan buildSelectionAccessPlan(const Schema& schema,
                                             const SourcePushdownSpec& pushdown) {
  SelectionAccessPlan plan;
  plan.schema_indices = buildCapturedSchemaIndices(schema, pushdown);
  plan.local_by_schema.assign(schema.fields.size(), -1);
  for (std::size_t local_index = 0; local_index < plan.schema_indices.size(); ++local_index) {
    plan.local_by_schema[plan.schema_indices[local_index]] = static_cast<int>(local_index);
  }
  const bool require_all_columns =
      pushdown.projected_columns.empty() || pushdown.projected_columns.size() >= schema.fields.size();
  if (require_all_columns) {
    plan.output_schema_indices.resize(schema.fields.size());
    for (std::size_t schema_index = 0; schema_index < schema.fields.size(); ++schema_index) {
      plan.output_schema_indices[schema_index] = schema_index;
    }
  } else {
    plan.output_schema_indices = pushdown.projected_columns;
  }
  plan.predicate_steps.reserve(8);
  appendCompiledPredicateSteps(pushdown.predicate_expr, plan.local_by_schema,
                               &plan.predicate_steps);
  return plan;
}

std::unordered_map<std::string, std::size_t> buildJsonColumnIndex(const JsonFileOptions& options);
std::unordered_map<std::string, std::size_t> buildJsonColumnIndex(
    const JsonFileOptions& options, const std::vector<std::size_t>& schema_indices);
JsonCapturedFieldLookup buildJsonCapturedFieldIndex(
    const JsonFileOptions& options, const std::vector<std::size_t>& schema_indices);

Table loadLineFileSelected(const std::string& path, const Schema& schema,
                           const LineFileOptions& options,
                           const std::vector<std::size_t>& schema_indices) {
  if (options.mappings.empty()) {
    throw std::invalid_argument("line source mappings cannot be empty");
  }
  if (options.mode == LineParseMode::Regex && options.regex_pattern.empty()) {
    throw std::invalid_argument("regex mode requires regex_pattern");
  }

  Table table = makeColumnFirstTable(schema);
  std::ifstream input(path);
  if (!input.is_open()) {
    throw std::runtime_error("cannot open line file: " + path);
  }

  std::regex line_regex;
  SimpleLineRegexPlan simple_plan;
  if (options.mode == LineParseMode::Regex) {
    simple_plan = compileSimpleLineRegexPlan(options.regex_pattern);
    if (!simple_plan.supported) {
      line_regex = std::regex(options.regex_pattern, std::regex::optimize);
    }
  }
  const auto capture_plan =
      options.mode == LineParseMode::Split ? buildLineCapturePlan(options, schema_indices)
                                           : LineCapturePlan{};

  std::string line;
  std::vector<Value> values(schema_indices.size());
  std::vector<std::string_view> regex_captures;
  while (std::getline(input, line)) {
    if (options.skip_empty_lines && line.empty()) {
      continue;
    }
    resetValues(&values);
    if (options.mode == LineParseMode::Split) {
      fillLineSplitValues(line, capture_plan, &values, options.split_delimiter);
    } else {
      bool matched_ok = false;
      regex_captures.clear();
      std::cmatch matched;
      if (simple_plan.supported) {
        matched_ok = matchSimpleLineRegex(simple_plan, line, &regex_captures);
      } else {
        const char* begin = line.data();
        const char* end = begin + line.size();
        matched_ok = std::regex_match(begin, end, matched, line_regex);
      }
      if (!matched_ok) continue;
      for (std::size_t local_index = 0; local_index < schema_indices.size(); ++local_index) {
        const auto schema_index = schema_indices[local_index];
        const auto& mapping = options.mappings[schema_index];
        if (simple_plan.supported) {
          if (mapping.source_index > 0 &&
              static_cast<std::size_t>(mapping.source_index) <= regex_captures.size()) {
            values[local_index] = parseScalarCell(regex_captures[mapping.source_index - 1]);
          }
        } else if (mapping.source_index < matched.size()) {
          const auto& capture = matched[mapping.source_index];
          values[local_index] = parseScalarCell(
              std::string_view(capture.first,
                               static_cast<std::size_t>(capture.second - capture.first)));
        }
      }
    }
    appendCapturedValues(&table, schema_indices, values);
  }

  if (table.columnar_cache->row_count > 0) {
    table.columnar_cache->batch_row_counts.push_back(table.columnar_cache->row_count);
  }
  return table;
}

Table loadJsonFileSelected(const std::string& path, const Schema& schema,
                           const JsonFileOptions& options,
                           const std::vector<std::size_t>& schema_indices) {
  if (options.columns.empty()) {
    throw std::invalid_argument("json source columns cannot be empty");
  }

  Table table = makeColumnFirstTable(schema);
  std::ifstream input(path);
  if (!input.is_open()) {
    throw std::runtime_error("cannot open json file: " + path);
  }
  const auto json_index = buildJsonColumnIndex(options, schema_indices);
  std::vector<Value> values(schema_indices.size());

  if (options.format == JsonFileFormat::JsonLines) {
    std::string line;
    while (std::getline(input, line)) {
      if (line.empty()) {
        continue;
      }
      JsonCursor cursor(line);
      resetValues(&values);
      cursor.forEachObjectEntry([&](std::string key, Value value) {
        const auto it = json_index.find(key);
        if (it != json_index.end()) {
          values[it->second] = std::move(value);
        }
      });
      appendCapturedValues(&table, schema_indices, values);
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
      resetValues(&values);
      cursor.forEachObjectEntry([&](std::string key, Value value) {
        const auto it = json_index.find(key);
        if (it != json_index.end()) {
          values[it->second] = std::move(value);
        }
      });
      appendCapturedValues(&table, schema_indices, values);
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

bool executeLineSourcePushdown(const std::string& path, const Schema& schema,
                               const LineFileOptions& options,
                               const SourcePushdownSpec& pushdown,
                               bool materialize_rows, Table* out) {
  if (out == nullptr) {
    throw std::invalid_argument("line source pushdown output cannot be null");
  }
  if (options.mappings.empty()) {
    throw std::invalid_argument("line source mappings cannot be empty");
  }
  if (options.mode == LineParseMode::Regex && options.regex_pattern.empty()) {
    throw std::invalid_argument("regex mode requires regex_pattern");
  }

  const auto access_plan = buildSelectionAccessPlan(schema, pushdown);
  Table table = makeColumnFirstTable(schema);
  const auto estimated_rows =
      pushdown.limit == 0 ? estimateTextRows(path) : pushdown.limit;
  const auto reserve_rows = estimated_rows == 0 ? std::size_t(1024) : estimated_rows;
  if (materialize_rows) {
    table.rows.reserve(reserve_rows);
  }
  for (const auto schema_index : access_plan.output_schema_indices) {
    table.columnar_cache->columns[schema_index].values.reserve(reserve_rows);
  }

  std::ifstream input(path);
  if (!input.is_open()) {
    throw std::runtime_error("cannot open line file: " + path);
  }
  std::regex line_regex;
  SimpleLineRegexPlan simple_plan;
  if (options.mode == LineParseMode::Regex) {
    simple_plan = compileSimpleLineRegexPlan(options.regex_pattern);
    if (!simple_plan.supported) {
      line_regex = std::regex(options.regex_pattern, std::regex::optimize);
    }
  }
  const auto capture_plan =
      options.mode == LineParseMode::Split
          ? buildLineCapturePlan(options, access_plan.schema_indices)
          : LineCapturePlan{};

  std::string line;
  std::vector<Value> values(access_plan.schema_indices.size());
  std::vector<std::string_view> regex_captures;
  std::vector<LineParsedScalar> parsed_values;
  if (options.mode == LineParseMode::Split) {
    parsed_values.resize(access_plan.schema_indices.size());
  }
  std::vector<uint8_t> predicate_scratch(access_plan.predicate_steps.size());
  while (std::getline(input, line)) {
    if (options.skip_empty_lines && line.empty()) {
      continue;
    }
    if (options.mode == LineParseMode::Split) {
      resetLineParsedScalarVector(&parsed_values);
      fillLineSplitParsedValues(line, capture_plan, &parsed_values, options.split_delimiter);
    } else {
      resetValues(&values);
      bool matched_ok = false;
      regex_captures.clear();
      std::cmatch matched;
      if (simple_plan.supported) {
        matched_ok = matchSimpleLineRegex(simple_plan, line, &regex_captures);
      } else {
        const char* begin = line.data();
        const char* end = begin + line.size();
        matched_ok = std::regex_match(begin, end, matched, line_regex);
      }
      if (!matched_ok) continue;
      for (std::size_t local_index = 0; local_index < access_plan.schema_indices.size(); ++local_index) {
        const auto schema_index = access_plan.schema_indices[local_index];
        const auto& mapping = options.mappings[schema_index];
        if (simple_plan.supported) {
          if (mapping.source_index > 0 &&
              static_cast<std::size_t>(mapping.source_index) <= regex_captures.size()) {
            values[local_index] = parseScalarCell(regex_captures[mapping.source_index - 1]);
          }
        } else if (mapping.source_index < matched.size()) {
          const auto& capture = matched[mapping.source_index];
          values[local_index] = parseScalarCell(
              std::string_view(capture.first,
                               static_cast<std::size_t>(capture.second - capture.first)));
        }
      }
    }
    if (options.mode == LineParseMode::Split) {
      if (!includeLineParsedRowForSelection(parsed_values, access_plan, pushdown,
                                            &predicate_scratch)) {
        continue;
      }
      Row row;
      if (materialize_rows) {
        row.reserve(access_plan.output_schema_indices.size());
      }
      for (const auto schema_index : access_plan.output_schema_indices) {
        const auto local_index = access_plan.local_by_schema[schema_index];
        if (local_index < 0) {
          continue;
        }
        Value value =
            lineParsedScalarToValue(parsed_values[static_cast<std::size_t>(local_index)]);
        table.columnar_cache->columns[schema_index].values.push_back(value);
        if (materialize_rows) {
          row.push_back(std::move(value));
        }
      }
      if (materialize_rows) {
        table.rows.push_back(std::move(row));
      }
      table.columnar_cache->row_count += 1;
    } else {
      if (!includeRowForSelection(values, access_plan, pushdown, &predicate_scratch)) {
        continue;
      }
      appendSelectionRow(&table, access_plan, values, materialize_rows);
    }
    if (pushdown.limit != 0 && table.columnar_cache->row_count >= pushdown.limit) {
      break;
    }
  }

  if (table.columnar_cache->row_count > 0) {
    table.columnar_cache->batch_row_counts.push_back(table.columnar_cache->row_count);
  }
  *out = std::move(table);
  return true;
}

bool executeJsonSourcePushdown(const std::string& path, const Schema& schema,
                               const JsonFileOptions& options,
                               const SourcePushdownSpec& pushdown,
                               bool materialize_rows, Table* out) {
  if (out == nullptr) {
    throw std::invalid_argument("json source pushdown output cannot be null");
  }
  if (options.columns.empty()) {
    throw std::invalid_argument("json source columns cannot be empty");
  }

  const auto access_plan = buildSelectionAccessPlan(schema, pushdown);
  Table table = makeColumnFirstTable(schema);
  const auto estimated_rows =
      pushdown.limit == 0 ? estimateTextRows(path) : pushdown.limit;
  const auto reserve_rows = estimated_rows == 0 ? std::size_t(1024) : estimated_rows;
  if (materialize_rows) {
    table.rows.reserve(reserve_rows);
  }
  for (const auto schema_index : access_plan.output_schema_indices) {
    table.columnar_cache->columns[schema_index].values.reserve(reserve_rows);
  }

  std::ifstream input(path);
  if (!input.is_open()) {
    throw std::runtime_error("cannot open json file: " + path);
  }
  const auto captured_fields = buildJsonCapturedFieldIndex(options, access_plan.schema_indices);
  std::vector<ParsedScalar> parsed_values(access_plan.schema_indices.size());
  std::vector<uint8_t> parsed_touched(parsed_values.size(), 0);
  std::vector<std::size_t> parsed_touched_indices;
  parsed_touched_indices.reserve(parsed_values.size());
  std::vector<uint8_t> predicate_scratch(access_plan.predicate_steps.size());

  if (options.format == JsonFileFormat::JsonLines) {
    std::string line;
    while (std::getline(input, line)) {
      if (line.empty()) {
        continue;
      }
      JsonCursor cursor(line);
      resetTouchedParsedScalars(&parsed_values, &parsed_touched, &parsed_touched_indices);
      cursor.forEachObjectParsedEntry([&](const JsonCursor::StringRef& key, JsonCursor* json_cursor) {
        const auto key_text = key.owned.empty() ? key.view : std::string_view(key.owned);
        const int local_index = captured_fields.find(key_text);
        if (local_index >= 0) {
          const auto index = static_cast<std::size_t>(local_index);
          if (parsed_touched[index] == 0) {
            parsed_touched[index] = 1;
            parsed_touched_indices.push_back(index);
          }
          parsed_values[index] = json_cursor->parseParsedScalarValue();
        } else {
          json_cursor->skipValue();
        }
      });
      if (!includeParsedRowForSelection(parsed_values, access_plan, pushdown,
                                        &predicate_scratch)) {
        continue;
      }
      Row row;
      if (materialize_rows) {
        row.reserve(access_plan.output_schema_indices.size());
      }
      for (const auto schema_index : access_plan.output_schema_indices) {
        const auto local_index = access_plan.local_by_schema[schema_index];
        if (local_index < 0) {
          continue;
        }
        Value value =
            parsedScalarToValue(parsed_values[static_cast<std::size_t>(local_index)]);
        table.columnar_cache->columns[schema_index].values.push_back(value);
        if (materialize_rows) {
          row.push_back(std::move(value));
        }
      }
      if (materialize_rows) {
        table.rows.push_back(std::move(row));
      }
      table.columnar_cache->row_count += 1;
      if (pushdown.limit != 0 && table.columnar_cache->row_count >= pushdown.limit) {
        break;
      }
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
      resetTouchedParsedScalars(&parsed_values, &parsed_touched, &parsed_touched_indices);
      cursor.forEachObjectParsedEntry([&](const JsonCursor::StringRef& key, JsonCursor* json_cursor) {
        const auto key_text = key.owned.empty() ? key.view : std::string_view(key.owned);
        const int local_index = captured_fields.find(key_text);
        if (local_index >= 0) {
          const auto index = static_cast<std::size_t>(local_index);
          if (parsed_touched[index] == 0) {
            parsed_touched[index] = 1;
            parsed_touched_indices.push_back(index);
          }
          parsed_values[index] = json_cursor->parseParsedScalarValue();
        } else {
          json_cursor->skipValue();
        }
      });
      if (includeParsedRowForSelection(parsed_values, access_plan, pushdown,
                                       &predicate_scratch)) {
        Row row;
        if (materialize_rows) {
          row.reserve(access_plan.output_schema_indices.size());
        }
        for (const auto schema_index : access_plan.output_schema_indices) {
          const auto local_index = access_plan.local_by_schema[schema_index];
          if (local_index < 0) {
            continue;
          }
          Value value =
              parsedScalarToValue(parsed_values[static_cast<std::size_t>(local_index)]);
          table.columnar_cache->columns[schema_index].values.push_back(value);
          if (materialize_rows) {
            row.push_back(std::move(value));
          }
        }
        if (materialize_rows) {
          table.rows.push_back(std::move(row));
        }
        table.columnar_cache->row_count += 1;
        if (pushdown.limit != 0 && table.columnar_cache->row_count >= pushdown.limit) {
          break;
        }
      }
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
  *out = std::move(table);
  return true;
}

bool try_execute_line_aggregate(const std::string& path, const Schema& schema,
                                const LineFileOptions& options,
                                const SourcePushdownSpec& pushdown, Table* out) {
  if (out == nullptr) {
    throw std::invalid_argument("line aggregate output cannot be null");
  }
  if (!validateAggregatePushdown(schema, pushdown)) {
    return false;
  }
  const auto access_plan = buildAggregateAccessPlan(schema, pushdown);
  std::ifstream input(path);
  if (!input.is_open()) {
    throw std::runtime_error("cannot open line file: " + path);
  }
  const bool single_key_count_shape =
      pushdown.shape == SourcePushdownShape::SingleKeyCount &&
      pushdown.aggregate.keys.size() == 1 &&
      pushdown.aggregate.aggregates.size() == 1 &&
      pushdown.aggregate.aggregates.front().function == AggregateFunction::Count &&
      !pushdown.predicate_expr;
  const bool single_key_numeric_shape =
      pushdown.shape == SourcePushdownShape::SingleKeyNumericAggregate &&
      pushdown.aggregate.keys.size() == 1 &&
      pushdown.aggregate.aggregates.size() == 1 &&
      pushdown.aggregate.aggregates.front().function != AggregateFunction::Count &&
      !pushdown.predicate_expr;

  std::regex line_regex;
  SimpleLineRegexPlan simple_plan;
  if (options.mode == LineParseMode::Regex) {
    if (options.regex_pattern.empty()) {
      return false;
    }
    simple_plan = compileSimpleLineRegexPlan(options.regex_pattern);
    if (!simple_plan.supported) {
      line_regex = std::regex(options.regex_pattern, std::regex::optimize);
    }
  }
  const auto capture_plan = options.mode == LineParseMode::Split
                                ? buildLineCapturePlan(options, access_plan.schema_indices)
                                : LineCapturePlan{};

  const bool single_key_aggregate = pushdown.aggregate.keys.size() == 1;
  AggregateGroupMap groups;
  SingleAggregateGroupMap single_groups;
  SingleCountGroupMap single_count_groups;
  SingleNumericAggregateGroupMap single_numeric_groups;
  if (single_key_aggregate) {
    single_groups.reserve(64);
    single_count_groups.reserve(64);
    single_numeric_groups.reserve(64);
  } else {
    groups.reserve(64);
  }
  std::string line;
  std::vector<Value> values(access_plan.schema_indices.size());
  std::vector<std::string_view> regex_captures;
  std::vector<LineParsedScalar> parsed_values;
  if (options.mode == LineParseMode::Split) {
    parsed_values.resize(access_plan.schema_indices.size());
  }
  std::vector<uint8_t> predicate_scratch(access_plan.predicate_steps.size());
  while (std::getline(input, line)) {
    if (options.skip_empty_lines && line.empty()) {
      continue;
    }
    if (options.mode == LineParseMode::Split) {
      resetLineParsedScalarVector(&parsed_values);
      fillLineSplitParsedValues(line, capture_plan, &parsed_values, options.split_delimiter);
    } else {
      resetValues(&values);
      bool matched_ok = false;
      regex_captures.clear();
      std::cmatch matched;
      if (simple_plan.supported) {
        matched_ok = matchSimpleLineRegex(simple_plan, line, &regex_captures);
      } else {
        const char* begin = line.data();
        const char* end = begin + line.size();
        matched_ok = std::regex_match(begin, end, matched, line_regex);
      }
      if (!matched_ok) continue;
      for (std::size_t local_index = 0; local_index < access_plan.schema_indices.size(); ++local_index) {
        const auto schema_index = access_plan.schema_indices[local_index];
        const auto& mapping = options.mappings[schema_index];
        if (simple_plan.supported) {
          if (mapping.source_index > 0 &&
              static_cast<std::size_t>(mapping.source_index) <= regex_captures.size()) {
            values[local_index] = parseScalarCell(regex_captures[mapping.source_index - 1]);
          }
        } else if (mapping.source_index < matched.size()) {
          const auto& capture = matched[mapping.source_index];
          values[local_index] = parseScalarCell(
              std::string_view(capture.first,
                               static_cast<std::size_t>(capture.second - capture.first)));
        }
      }
    }
    if (options.mode == LineParseMode::Split) {
      if (!includeLineParsedRowForAggregate(parsed_values, access_plan, pushdown,
                                            &predicate_scratch)) {
        continue;
      }
      if (single_key_count_shape) {
        const Value key =
            lineParsedScalarToValue(parsed_values[access_plan.local_key_indices.front()]);
        single_count_groups[key] += 1;
      } else if (single_key_numeric_shape) {
        const Value key =
            lineParsedScalarToValue(parsed_values[access_plan.local_key_indices.front()]);
        auto& state = single_numeric_groups[key];
        const auto& agg = pushdown.aggregate.aggregates.front();
        const auto& cell =
            parsed_values[static_cast<std::size_t>(access_plan.local_value_by_aggregate.front())];
        switch (agg.function) {
          case AggregateFunction::Sum:
            if (cell.is_int64) state.sum += static_cast<double>(cell.int64_value);
            else if (cell.is_double) state.sum += cell.double_value;
            break;
          case AggregateFunction::Avg:
            if (cell.is_int64) {
              state.sum += static_cast<double>(cell.int64_value);
              state.count += 1;
            } else if (cell.is_double) {
              state.sum += cell.double_value;
              state.count += 1;
            }
            break;
          case AggregateFunction::Min: {
            const Value value = lineParsedScalarToValue(cell);
            if (!state.has_value || value < state.value) {
              state.has_value = true;
              state.value = value;
            }
            break;
          }
          case AggregateFunction::Max: {
            const Value value = lineParsedScalarToValue(cell);
            if (!state.has_value || value > state.value) {
              state.has_value = true;
              state.value = value;
            }
            break;
          }
          case AggregateFunction::Count:
            break;
        }
      } else if (single_key_aggregate) {
        const Value key =
            lineParsedScalarToValue(parsed_values[access_plan.local_key_indices.front()]);
        auto it = single_groups.find(key);
        if (it == single_groups.end()) {
          GroupState state;
          state.agg_states.resize(pushdown.aggregate.aggregates.size());
          it = single_groups.emplace(key, std::move(state)).first;
        }
        updateAggregateStatesLineParsed(parsed_values, access_plan, pushdown, &it->second);
      } else {
        GroupKey key;
        key.values.reserve(access_plan.local_key_indices.size());
        for (const auto local_key_index : access_plan.local_key_indices) {
          key.values.push_back(lineParsedScalarToValue(parsed_values[local_key_index]));
        }
        auto it = groups.find(key);
        if (it == groups.end()) {
          GroupState state;
          state.agg_states.resize(pushdown.aggregate.aggregates.size());
          it = groups.emplace(std::move(key), std::move(state)).first;
        }
        updateAggregateStatesLineParsed(parsed_values, access_plan, pushdown, &it->second);
      }
    } else {
      if (!includeRowForAggregate(values, access_plan, pushdown, &predicate_scratch)) {
        continue;
      }
      if (single_key_count_shape) {
        const Value& key = values[access_plan.local_key_indices.front()];
        single_count_groups[key] += 1;
      } else if (single_key_numeric_shape) {
        const Value& key = values[access_plan.local_key_indices.front()];
        auto& state = single_numeric_groups[key];
        const auto& agg = pushdown.aggregate.aggregates.front();
        const Value& cell =
            values[static_cast<std::size_t>(access_plan.local_value_by_aggregate.front())];
        switch (agg.function) {
          case AggregateFunction::Sum:
            if (cell.isNumber()) state.sum += cell.asDouble();
            break;
          case AggregateFunction::Avg:
            if (cell.isNumber()) {
              state.sum += cell.asDouble();
              state.count += 1;
            }
            break;
          case AggregateFunction::Min:
            if (!state.has_value || cell < state.value) {
              state.has_value = true;
              state.value = cell;
            }
            break;
          case AggregateFunction::Max:
            if (!state.has_value || cell > state.value) {
              state.has_value = true;
              state.value = cell;
            }
            break;
          case AggregateFunction::Count:
            break;
        }
      } else if (single_key_aggregate) {
        accumulateSingleKeyAggregateRow(values, access_plan, pushdown, &single_groups);
      } else {
        accumulateAggregateRow(values, access_plan, pushdown, &groups);
      }
    }
  }
  *out = single_key_count_shape
             ? finalizeSingleCountGroups(schema, pushdown, single_count_groups, false)
             : (single_key_numeric_shape
                    ? finalizeSingleNumericAggregateGroups(schema, pushdown,
                                                          single_numeric_groups, false)
                    : (single_key_aggregate
                           ? finalizeSingleKeyAggregatedGroups(schema, pushdown, single_groups, false)
                           : finalizeAggregatedGroups(schema, pushdown, groups, false)));
  if (pushdown.limit != 0 && out->rowCount() > pushdown.limit) {
    *out = limitTable(*out, pushdown.limit, false);
  }
  return true;
}

bool try_execute_json_aggregate(const std::string& path, const Schema& schema,
                                const JsonFileOptions& options,
                                const SourcePushdownSpec& pushdown, Table* out) {
  if (out == nullptr) {
    throw std::invalid_argument("json aggregate output cannot be null");
  }
  if (!validateAggregatePushdown(schema, pushdown)) {
    return false;
  }
  const auto access_plan = buildAggregateAccessPlan(schema, pushdown);
  std::ifstream input(path);
  if (!input.is_open()) {
    throw std::runtime_error("cannot open json file: " + path);
  }
  const auto captured_fields = buildJsonCapturedFieldIndex(options, access_plan.schema_indices);
  const bool single_key_count_shape =
      pushdown.shape == SourcePushdownShape::SingleKeyCount &&
      pushdown.aggregate.keys.size() == 1 &&
      pushdown.aggregate.aggregates.size() == 1 &&
      pushdown.aggregate.aggregates.front().function == AggregateFunction::Count &&
      !pushdown.predicate_expr;
  const bool single_key_numeric_shape =
      pushdown.shape == SourcePushdownShape::SingleKeyNumericAggregate &&
      pushdown.aggregate.keys.size() == 1 &&
      pushdown.aggregate.aggregates.size() == 1 &&
      pushdown.aggregate.aggregates.front().function != AggregateFunction::Count &&
      !pushdown.predicate_expr;

  const bool single_key_aggregate = pushdown.aggregate.keys.size() == 1;
  AggregateGroupMap groups;
  SingleAggregateGroupMap single_groups;
  SingleCountGroupMap single_count_groups;
  SingleNumericAggregateGroupMap single_numeric_groups;
  if (single_key_aggregate) {
    single_groups.reserve(64);
    single_count_groups.reserve(64);
    single_numeric_groups.reserve(64);
  } else {
    groups.reserve(64);
  }
  std::vector<ParsedScalar> parsed_values(access_plan.schema_indices.size());
  std::vector<uint8_t> parsed_touched(parsed_values.size(), 0);
  std::vector<std::size_t> parsed_touched_indices;
  parsed_touched_indices.reserve(parsed_values.size());
  std::vector<uint8_t> predicate_scratch(access_plan.predicate_steps.size());

  if (options.format == JsonFileFormat::JsonLines) {
    std::string line;
    while (std::getline(input, line)) {
      if (line.empty()) {
        continue;
      }
      JsonCursor cursor(line);
      resetTouchedParsedScalars(&parsed_values, &parsed_touched, &parsed_touched_indices);
      cursor.forEachObjectParsedEntry([&](const JsonCursor::StringRef& key, JsonCursor* json_cursor) {
        const auto key_text = key.owned.empty() ? key.view : std::string_view(key.owned);
        const int local_index = captured_fields.find(key_text);
        if (local_index >= 0) {
          const auto index = static_cast<std::size_t>(local_index);
          if (parsed_touched[index] == 0) {
            parsed_touched[index] = 1;
            parsed_touched_indices.push_back(index);
          }
          parsed_values[index] =
              json_cursor->parseParsedScalarValue();
        } else {
          json_cursor->skipValue();
        }
      });
      if (!includeParsedRowForAggregate(parsed_values, access_plan, pushdown,
                                        &predicate_scratch)) {
        continue;
      }
      if (single_key_count_shape) {
        const Value key =
            parsedScalarToValue(parsed_values[access_plan.local_key_indices.front()]);
        single_count_groups[key] += 1;
      } else if (single_key_numeric_shape) {
        const Value key =
            parsedScalarToValue(parsed_values[access_plan.local_key_indices.front()]);
        auto& state = single_numeric_groups[key];
        const auto& agg = pushdown.aggregate.aggregates.front();
        const auto& cell =
            parsed_values[static_cast<std::size_t>(access_plan.local_value_by_aggregate.front())];
        switch (agg.function) {
          case AggregateFunction::Sum:
            if (cell.is_int64) state.sum += static_cast<double>(cell.int64_value);
            else if (cell.is_double) state.sum += cell.double_value;
            break;
          case AggregateFunction::Avg:
            if (cell.is_int64) {
              state.sum += static_cast<double>(cell.int64_value);
              state.count += 1;
            } else if (cell.is_double) {
              state.sum += cell.double_value;
              state.count += 1;
            }
            break;
          case AggregateFunction::Min: {
            const Value value = parsedScalarToValue(cell);
            if (!state.has_value || value < state.value) {
              state.has_value = true;
              state.value = value;
            }
            break;
          }
          case AggregateFunction::Max: {
            const Value value = parsedScalarToValue(cell);
            if (!state.has_value || value > state.value) {
              state.has_value = true;
              state.value = value;
            }
            break;
          }
          case AggregateFunction::Count:
            break;
        }
      } else if (single_key_aggregate) {
        const Value key = parsedScalarToValue(parsed_values[access_plan.local_key_indices.front()]);
        auto it = single_groups.find(key);
        if (it == single_groups.end()) {
          GroupState state;
          state.agg_states.resize(pushdown.aggregate.aggregates.size());
          it = single_groups.emplace(key, std::move(state)).first;
        }
        updateAggregateStatesParsed(parsed_values, access_plan, pushdown, &it->second);
      } else {
        GroupKey key;
        key.values.reserve(access_plan.local_key_indices.size());
        for (const auto local_key_index : access_plan.local_key_indices) {
          key.values.push_back(parsedScalarToValue(parsed_values[local_key_index]));
        }
        auto it = groups.find(key);
        if (it == groups.end()) {
          GroupState state;
          state.agg_states.resize(pushdown.aggregate.aggregates.size());
          it = groups.emplace(std::move(key), std::move(state)).first;
        }
        updateAggregateStatesParsed(parsed_values, access_plan, pushdown, &it->second);
      }
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
      resetTouchedParsedScalars(&parsed_values, &parsed_touched, &parsed_touched_indices);
      cursor.forEachObjectParsedEntry([&](const JsonCursor::StringRef& key, JsonCursor* json_cursor) {
        const auto key_text = key.owned.empty() ? key.view : std::string_view(key.owned);
        const int local_index = captured_fields.find(key_text);
        if (local_index >= 0) {
          const auto index = static_cast<std::size_t>(local_index);
          if (parsed_touched[index] == 0) {
            parsed_touched[index] = 1;
            parsed_touched_indices.push_back(index);
          }
          parsed_values[index] =
              json_cursor->parseParsedScalarValue();
        } else {
          json_cursor->skipValue();
        }
      });
      if (includeParsedRowForAggregate(parsed_values, access_plan, pushdown,
                                       &predicate_scratch)) {
        if (single_key_count_shape) {
          const Value key =
              parsedScalarToValue(parsed_values[access_plan.local_key_indices.front()]);
          single_count_groups[key] += 1;
        } else if (single_key_numeric_shape) {
          const Value key =
              parsedScalarToValue(parsed_values[access_plan.local_key_indices.front()]);
          auto& state = single_numeric_groups[key];
          const auto& agg = pushdown.aggregate.aggregates.front();
          const auto& cell =
              parsed_values[static_cast<std::size_t>(access_plan.local_value_by_aggregate.front())];
          switch (agg.function) {
            case AggregateFunction::Sum:
              if (cell.is_int64) state.sum += static_cast<double>(cell.int64_value);
              else if (cell.is_double) state.sum += cell.double_value;
              break;
            case AggregateFunction::Avg:
              if (cell.is_int64) {
                state.sum += static_cast<double>(cell.int64_value);
                state.count += 1;
              } else if (cell.is_double) {
                state.sum += cell.double_value;
                state.count += 1;
              }
              break;
            case AggregateFunction::Min: {
              const Value value = parsedScalarToValue(cell);
              if (!state.has_value || value < state.value) {
                state.has_value = true;
                state.value = value;
              }
              break;
            }
            case AggregateFunction::Max: {
              const Value value = parsedScalarToValue(cell);
              if (!state.has_value || value > state.value) {
                state.has_value = true;
                state.value = value;
              }
              break;
            }
            case AggregateFunction::Count:
              break;
          }
        } else if (single_key_aggregate) {
          const Value key =
              parsedScalarToValue(parsed_values[access_plan.local_key_indices.front()]);
          auto it = single_groups.find(key);
          if (it == single_groups.end()) {
            GroupState state;
            state.agg_states.resize(pushdown.aggregate.aggregates.size());
            it = single_groups.emplace(key, std::move(state)).first;
          }
          updateAggregateStatesParsed(parsed_values, access_plan, pushdown, &it->second);
        } else {
          GroupKey key;
          key.values.reserve(access_plan.local_key_indices.size());
          for (const auto local_key_index : access_plan.local_key_indices) {
            key.values.push_back(parsedScalarToValue(parsed_values[local_key_index]));
          }
          auto it = groups.find(key);
          if (it == groups.end()) {
            GroupState state;
            state.agg_states.resize(pushdown.aggregate.aggregates.size());
            it = groups.emplace(std::move(key), std::move(state)).first;
          }
          updateAggregateStatesParsed(parsed_values, access_plan, pushdown, &it->second);
        }
      }
      if (cursor.consume(']')) {
        break;
      }
      if (!cursor.consume(',')) {
        throw std::runtime_error("json array source missing ',' between objects");
      }
    }
  }

  *out = single_key_count_shape
             ? finalizeSingleCountGroups(schema, pushdown, single_count_groups, false)
             : (single_key_numeric_shape
                    ? finalizeSingleNumericAggregateGroups(schema, pushdown,
                                                          single_numeric_groups, false)
                    : (single_key_aggregate
                           ? finalizeSingleKeyAggregatedGroups(schema, pushdown, single_groups, false)
                           : finalizeAggregatedGroups(schema, pushdown, groups, false)));
  if (pushdown.limit != 0 && out->rowCount() > pushdown.limit) {
    *out = limitTable(*out, pushdown.limit, false);
  }
  return true;
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

std::unordered_map<std::string, std::size_t> buildJsonColumnIndex(const JsonFileOptions& options) {
  std::unordered_map<std::string, std::size_t> index;
  index.reserve(options.columns.size());
  for (std::size_t i = 0; i < options.columns.size(); ++i) {
    index.emplace(options.columns[i], i);
  }
  return index;
}

std::unordered_map<std::string, std::size_t> buildJsonColumnIndex(
    const JsonFileOptions& options, const std::vector<std::size_t>& schema_indices) {
  std::unordered_map<std::string, std::size_t> index;
  index.reserve(schema_indices.size());
  for (std::size_t local_index = 0; local_index < schema_indices.size(); ++local_index) {
    index.emplace(options.columns[schema_indices[local_index]], local_index);
  }
  return index;
}

JsonCapturedFieldLookup buildJsonCapturedFieldIndex(
    const JsonFileOptions& options, const std::vector<std::size_t>& schema_indices) {
  JsonCapturedFieldLookup lookup;
  constexpr std::size_t kLargeLookupThreshold = 8;
  lookup.use_large_map = schema_indices.size() > kLargeLookupThreshold;
  if (lookup.use_large_map) {
    lookup.large_entries.reserve(schema_indices.size());
  } else {
    lookup.small_entries.reserve(schema_indices.size());
  }
  for (std::size_t local_index = 0; local_index < schema_indices.size(); ++local_index) {
    const auto key = std::string_view(options.columns[schema_indices[local_index]]);
    if (lookup.use_large_map) {
      lookup.large_entries.emplace(key, static_cast<int>(local_index));
    } else {
      lookup.small_entries.emplace_back(key, static_cast<int>(local_index));
    }
  }
  return lookup;
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

void collectPredicateSchemaIndices(const std::shared_ptr<PlanPredicateExpr>& expr,
                                   const std::function<void(std::size_t)>& visit) {
  if (!expr || !visit) {
    return;
  }
  if (expr->kind == PlanPredicateExprKind::Comparison) {
    visit(expr->comparison.column_index);
    return;
  }
  collectPredicateSchemaIndices(expr->left, visit);
  collectPredicateSchemaIndices(expr->right, visit);
}

void appendCompiledPredicateSteps(const std::shared_ptr<PlanPredicateExpr>& expr,
                                  const std::vector<int>& local_by_schema,
                                  std::vector<CompiledPredicateStep>* out) {
  if (!expr || out == nullptr) {
    return;
  }
  if (expr->kind == PlanPredicateExprKind::Comparison) {
    CompiledPredicateStep step;
    step.kind = CompiledPredicateStepKind::Comparison;
    step.schema_index = expr->comparison.column_index;
    step.local_index = expr->comparison.column_index < local_by_schema.size()
                           ? local_by_schema[expr->comparison.column_index]
                           : -1;
    step.value = expr->comparison.value;
    step.op = expr->comparison.op;
    out->push_back(std::move(step));
    return;
  }
  appendCompiledPredicateSteps(expr->left, local_by_schema, out);
  appendCompiledPredicateSteps(expr->right, local_by_schema, out);
  CompiledPredicateStep step;
  step.kind = expr->kind == PlanPredicateExprKind::And ? CompiledPredicateStepKind::And
                                                       : CompiledPredicateStepKind::Or;
  out->push_back(std::move(step));
}

bool evaluateCompiledPredicateValues(const std::vector<Value>& values,
                                     const std::vector<CompiledPredicateStep>& steps,
                                     std::vector<uint8_t>* scratch) {
  if (steps.empty()) {
    return true;
  }
  if (scratch == nullptr) {
    throw std::invalid_argument("predicate scratch cannot be null");
  }
  scratch->resize(steps.size());
  std::size_t stack_size = 0;
  for (const auto& step : steps) {
    if (step.kind == CompiledPredicateStepKind::Comparison) {
      const bool match = step.local_index >= 0 &&
                         static_cast<std::size_t>(step.local_index) < values.size() &&
                         compareValueSafe(values[static_cast<std::size_t>(step.local_index)],
                                          step.value, step.op);
      (*scratch)[stack_size++] = match ? 1U : 0U;
      continue;
    }
    if (stack_size < 2) {
      return false;
    }
    const bool right = (*scratch)[--stack_size] != 0;
    const bool left = (*scratch)[stack_size - 1] != 0;
    (*scratch)[stack_size - 1] =
        (step.kind == CompiledPredicateStepKind::And ? (left && right) : (left || right))
            ? 1U
            : 0U;
  }
  return stack_size == 1 && (*scratch)[0] != 0;
}

bool evaluateCompiledPredicateParsed(const std::vector<ParsedScalar>& values,
                                     const std::vector<CompiledPredicateStep>& steps,
                                     std::vector<uint8_t>* scratch) {
  if (steps.empty()) {
    return true;
  }
  if (scratch == nullptr) {
    throw std::invalid_argument("predicate scratch cannot be null");
  }
  scratch->resize(steps.size());
  std::size_t stack_size = 0;
  for (const auto& step : steps) {
    if (step.kind == CompiledPredicateStepKind::Comparison) {
      const bool match = step.local_index >= 0 &&
                         static_cast<std::size_t>(step.local_index) < values.size() &&
                         tryMatchesParsedScalarFilter(values[static_cast<std::size_t>(step.local_index)],
                                                      step.value, step.op);
      (*scratch)[stack_size++] = match ? 1U : 0U;
      continue;
    }
    if (stack_size < 2) {
      return false;
    }
    const bool right = (*scratch)[--stack_size] != 0;
    const bool left = (*scratch)[stack_size - 1] != 0;
    (*scratch)[stack_size - 1] =
        (step.kind == CompiledPredicateStepKind::And ? (left && right) : (left || right))
            ? 1U
            : 0U;
  }
  return stack_size == 1 && (*scratch)[0] != 0;
}

bool evaluateCompiledPredicateLineParsed(const std::vector<LineParsedScalar>& values,
                                         const std::vector<CompiledPredicateStep>& steps,
                                         std::vector<uint8_t>* scratch) {
  if (steps.empty()) {
    return true;
  }
  if (scratch == nullptr) {
    throw std::invalid_argument("predicate scratch cannot be null");
  }
  scratch->resize(steps.size());
  std::size_t stack_size = 0;
  for (const auto& step : steps) {
    if (step.kind == CompiledPredicateStepKind::Comparison) {
      const bool match = step.local_index >= 0 &&
                         static_cast<std::size_t>(step.local_index) < values.size() &&
                         tryMatchesLineParsedScalarFilter(
                             values[static_cast<std::size_t>(step.local_index)], step.value,
                             step.op);
      (*scratch)[stack_size++] = match ? 1U : 0U;
      continue;
    }
    if (stack_size < 2) {
      return false;
    }
    const bool right = (*scratch)[--stack_size] != 0;
    const bool left = (*scratch)[stack_size - 1] != 0;
    (*scratch)[stack_size - 1] =
        (step.kind == CompiledPredicateStepKind::And ? (left && right) : (left || right))
            ? 1U
            : 0U;
  }
  return stack_size == 1 && (*scratch)[0] != 0;
}

bool evaluateCompiledPredicateRow(const Table& input, std::size_t row_index,
                                  const std::vector<CompiledPredicateStep>& steps,
                                  std::vector<uint8_t>* scratch) {
  if (steps.empty()) {
    return true;
  }
  if (input.columnar_cache == nullptr) {
    throw std::invalid_argument("predicate input requires columnar cache");
  }
  if (scratch == nullptr) {
    throw std::invalid_argument("predicate scratch cannot be null");
  }
  scratch->resize(steps.size());
  std::size_t stack_size = 0;
  for (const auto& step : steps) {
    if (step.kind == CompiledPredicateStepKind::Comparison) {
      if (step.schema_index >= input.columnar_cache->columns.size()) {
        (*scratch)[stack_size++] = 0;
        continue;
      }
      const auto& column = input.columnar_cache->columns[step.schema_index].values;
      const bool match = row_index < column.size() &&
                         compareValueSafe(column[row_index], step.value, step.op);
      (*scratch)[stack_size++] = match ? 1U : 0U;
      continue;
    }
    if (stack_size < 2) {
      return false;
    }
    const bool right = (*scratch)[--stack_size] != 0;
    const bool left = (*scratch)[stack_size - 1] != 0;
    (*scratch)[stack_size - 1] =
        (step.kind == CompiledPredicateStepKind::And ? (left && right) : (left || right))
            ? 1U
            : 0U;
  }
  return stack_size == 1 && (*scratch)[0] != 0;
}

bool includeRowForSelection(const std::vector<Value>& values,
                            const SelectionAccessPlan& access_plan,
                            const SourcePushdownSpec& pushdown,
                            std::vector<uint8_t>* predicate_scratch) {
  for (const auto& filter : pushdown.filters) {
    const auto local_index = access_plan.local_by_schema[filter.column_index];
    if (local_index < 0 ||
        !compareValueSafe(values[static_cast<std::size_t>(local_index)], filter.value, filter.op)) {
      return false;
    }
  }
  return evaluateCompiledPredicateValues(values, access_plan.predicate_steps,
                                         predicate_scratch);
}

bool includeParsedRowForSelection(const std::vector<ParsedScalar>& values,
                                  const SelectionAccessPlan& access_plan,
                                  const SourcePushdownSpec& pushdown,
                                  std::vector<uint8_t>* predicate_scratch) {
  for (const auto& filter : pushdown.filters) {
    const auto local_index = access_plan.local_by_schema[filter.column_index];
    if (local_index < 0 ||
        !tryMatchesParsedScalarFilter(values[static_cast<std::size_t>(local_index)],
                                      filter.value, filter.op)) {
      return false;
    }
  }
  return evaluateCompiledPredicateParsed(values, access_plan.predicate_steps,
                                         predicate_scratch);
}

bool includeLineParsedRowForSelection(const std::vector<LineParsedScalar>& values,
                                      const SelectionAccessPlan& access_plan,
                                      const SourcePushdownSpec& pushdown,
                                      std::vector<uint8_t>* predicate_scratch) {
  for (const auto& filter : pushdown.filters) {
    const auto local_index = access_plan.local_by_schema[filter.column_index];
    if (local_index < 0 ||
        !tryMatchesLineParsedScalarFilter(values[static_cast<std::size_t>(local_index)],
                                          filter.value, filter.op)) {
      return false;
    }
  }
  return evaluateCompiledPredicateLineParsed(values, access_plan.predicate_steps,
                                             predicate_scratch);
}

void appendSelectionRow(Table* out, const SelectionAccessPlan& access_plan,
                        const std::vector<Value>& values, bool materialize_rows) {
  if (out == nullptr || out->columnar_cache == nullptr) {
    throw std::invalid_argument("selection output table is not initialized");
  }
  Row row;
  if (materialize_rows) {
    row.reserve(access_plan.output_schema_indices.size());
  }
  for (const auto schema_index : access_plan.output_schema_indices) {
    const auto local_index = access_plan.local_by_schema[schema_index];
    if (local_index < 0) {
      continue;
    }
    const auto& value = values[static_cast<std::size_t>(local_index)];
    out->columnar_cache->columns[schema_index].values.push_back(value);
    if (materialize_rows) {
      row.push_back(value);
    }
  }
  if (materialize_rows) {
    out->rows.push_back(std::move(row));
  }
  out->columnar_cache->row_count += 1;
}

Table applyFilterAndLimit(const Table& input, const SourcePushdownSpec& pushdown) {
  const std::size_t row_count =
      input.columnar_cache ? input.columnar_cache->row_count : input.rowCount();
  Table filtered = makeColumnFirstTable(input.schema);
  const auto reserve_rows =
      pushdown.limit == 0 ? row_count : std::min<std::size_t>(row_count, pushdown.limit);
  std::vector<int> local_by_schema(filtered.schema.fields.size(), -1);
  for (std::size_t schema_index = 0; schema_index < local_by_schema.size(); ++schema_index) {
    local_by_schema[schema_index] = static_cast<int>(schema_index);
  }
  std::vector<CompiledPredicateStep> predicate_steps;
  predicate_steps.reserve(8);
  appendCompiledPredicateSteps(pushdown.predicate_expr, local_by_schema, &predicate_steps);
  std::vector<uint8_t> predicate_scratch(predicate_steps.size());
  std::vector<uint8_t> materialized_columns(filtered.schema.fields.size(), static_cast<uint8_t>(0));
  for (std::size_t col = 0; col < filtered.schema.fields.size(); ++col) {
    if (input.columnar_cache->columns[col].values.size() == row_count) {
      materialized_columns[col] = 1;
      filtered.columnar_cache->columns[col].values.reserve(reserve_rows);
    }
  }

  for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
    bool matches_filters = true;
    for (const auto& filter : pushdown.filters) {
      if (filter.column_index >= input.schema.fields.size()) {
        throw std::runtime_error("source pushdown filter column out of range");
      }
      const auto& cell = input.columnar_cache->columns[filter.column_index].values[row_index];
      if (!compareValueSafe(cell, filter.value, filter.op)) {
        matches_filters = false;
        break;
      }
    }
    if (!matches_filters) {
      continue;
    }
    if (!evaluateCompiledPredicateRow(input, row_index, predicate_steps,
                                      &predicate_scratch)) {
      continue;
    }
    for (std::size_t col = 0; col < filtered.schema.fields.size(); ++col) {
      if (materialized_columns[col] != 0) {
        filtered.columnar_cache->columns[col].values.push_back(
            input.columnar_cache->columns[col].values[row_index]);
      }
    }
    filtered.columnar_cache->row_count += 1;
    if (pushdown.limit != 0 && filtered.columnar_cache->row_count >= pushdown.limit) {
      break;
    }
  }

  if (filtered.columnar_cache->row_count > 0) {
    filtered.columnar_cache->batch_row_counts.push_back(filtered.columnar_cache->row_count);
  }
  return filtered;
}

std::size_t hashCombine(std::size_t seed, std::size_t value) {
  return seed ^ (value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2));
}

bool valuesEqualForKey(const Value& lhs, const Value& rhs) {
  if (lhs.type() == rhs.type()) {
    return lhs == rhs;
  }
  if (lhs.isNumber() && rhs.isNumber()) {
    return lhs.asDouble() == rhs.asDouble();
  }
  return false;
}

std::size_t hashValueForKey(const Value& value) {
  std::size_t seed = static_cast<std::size_t>(value.type());
  switch (value.type()) {
    case DataType::Nil:
      return hashCombine(seed, 0);
    case DataType::Bool:
      return hashCombine(seed, std::hash<bool>{}(value.asBool()));
    case DataType::Int64:
    case DataType::Double:
      return hashCombine(seed, std::hash<double>{}(value.asDouble()));
    case DataType::String:
      return hashCombine(seed, std::hash<std::string>{}(value.asString()));
    case DataType::FixedVector: {
      std::size_t vec_seed = seed;
      for (const auto item : value.asFixedVector()) {
        vec_seed = hashCombine(vec_seed, std::hash<float>{}(item));
      }
      return vec_seed;
    }
  }
  return seed;
}

std::size_t GroupKeyHash::operator()(const GroupKey& key) const {
  std::size_t seed = 0;
  for (const auto& value : key.values) {
    seed = hashCombine(seed, hashValueForKey(value));
  }
  return seed;
}

bool GroupKeyEq::operator()(const GroupKey& lhs, const GroupKey& rhs) const {
  if (lhs.values.size() != rhs.values.size()) {
    return false;
  }
  for (std::size_t i = 0; i < lhs.values.size(); ++i) {
    if (!valuesEqualForKey(lhs.values[i], rhs.values[i])) {
      return false;
    }
  }
  return true;
}

std::size_t SingleGroupKeyHash::operator()(const Value& value) const {
  return hashValueForKey(value);
}

bool SingleGroupKeyEq::operator()(const Value& lhs, const Value& rhs) const {
  return valuesEqualForKey(lhs, rhs);
}

GroupKey makeGroupKey(const std::vector<Value>& values, const std::vector<std::size_t>& key_indices) {
  GroupKey key;
  key.values.reserve(key_indices.size());
  for (const auto key_idx : key_indices) {
    key.values.push_back(values[key_idx]);
  }
  return key;
}

bool validateAggregatePushdown(const Schema& schema, const SourcePushdownSpec& pushdown) {
  if (!pushdown.has_aggregate || pushdown.aggregate.keys.empty() || pushdown.aggregate.aggregates.empty()) {
    return false;
  }
  for (const auto key_index : pushdown.aggregate.keys) {
    if (key_index >= schema.fields.size()) return false;
  }
  for (const auto& filter : pushdown.filters) {
    if (filter.column_index >= schema.fields.size()) return false;
  }
  bool predicate_valid = true;
  collectPredicateSchemaIndices(pushdown.predicate_expr, [&](std::size_t column_index) {
    if (column_index >= schema.fields.size()) {
      predicate_valid = false;
    }
  });
  if (!predicate_valid) return false;
  for (const auto& agg : pushdown.aggregate.aggregates) {
    if (agg.function != AggregateFunction::Count && agg.value_index >= schema.fields.size()) {
      return false;
    }
  }
  return true;
}

AggregateAccessPlan buildAggregateAccessPlan(const Schema& schema, const SourcePushdownSpec& pushdown) {
  AggregateAccessPlan plan;
  plan.local_by_schema.assign(schema.fields.size(), -1);
  auto add_schema_index = [&](std::size_t schema_index) {
    if (plan.local_by_schema[schema_index] >= 0) {
      return;
    }
    plan.local_by_schema[schema_index] = static_cast<int>(plan.schema_indices.size());
    plan.schema_indices.push_back(schema_index);
  };
  for (const auto& filter : pushdown.filters) {
    add_schema_index(filter.column_index);
  }
  collectPredicateSchemaIndices(pushdown.predicate_expr, add_schema_index);
  for (const auto key_index : pushdown.aggregate.keys) {
    add_schema_index(key_index);
  }
  for (const auto& agg : pushdown.aggregate.aggregates) {
    if (agg.function != AggregateFunction::Count) {
      add_schema_index(agg.value_index);
    }
  }
  plan.local_key_indices.reserve(pushdown.aggregate.keys.size());
  for (const auto key_index : pushdown.aggregate.keys) {
    plan.local_key_indices.push_back(static_cast<std::size_t>(plan.local_by_schema[key_index]));
  }
  plan.local_value_by_aggregate.reserve(pushdown.aggregate.aggregates.size());
  for (const auto& agg : pushdown.aggregate.aggregates) {
    plan.local_value_by_aggregate.push_back(
        agg.function == AggregateFunction::Count ? -1 : plan.local_by_schema[agg.value_index]);
  }
  plan.predicate_steps.reserve(8);
  appendCompiledPredicateSteps(pushdown.predicate_expr, plan.local_by_schema,
                               &plan.predicate_steps);
  return plan;
}

bool includeRowForAggregate(const std::vector<Value>& values, const AggregateAccessPlan& access_plan,
                            const SourcePushdownSpec& pushdown,
                            std::vector<uint8_t>* predicate_scratch) {
  for (const auto& filter : pushdown.filters) {
    const auto local_index = access_plan.local_by_schema[filter.column_index];
    if (!compareValueSafe(values[static_cast<std::size_t>(local_index)], filter.value, filter.op)) {
      return false;
    }
  }
  return evaluateCompiledPredicateValues(values, access_plan.predicate_steps,
                                         predicate_scratch);
}

bool includeParsedRowForAggregate(const std::vector<ParsedScalar>& values,
                                  const AggregateAccessPlan& access_plan,
                                  const SourcePushdownSpec& pushdown,
                                  std::vector<uint8_t>* predicate_scratch) {
  for (const auto& filter : pushdown.filters) {
    const auto local_index = access_plan.local_by_schema[filter.column_index];
    if (!tryMatchesParsedScalarFilter(values[static_cast<std::size_t>(local_index)],
                                      filter.value, filter.op)) {
      return false;
    }
  }
  return evaluateCompiledPredicateParsed(values, access_plan.predicate_steps,
                                         predicate_scratch);
}

bool includeLineParsedRowForAggregate(const std::vector<LineParsedScalar>& values,
                                      const AggregateAccessPlan& access_plan,
                                      const SourcePushdownSpec& pushdown,
                                      std::vector<uint8_t>* predicate_scratch) {
  for (const auto& filter : pushdown.filters) {
    const auto local_index = access_plan.local_by_schema[filter.column_index];
    if (!tryMatchesLineParsedScalarFilter(values[static_cast<std::size_t>(local_index)],
                                          filter.value, filter.op)) {
      return false;
    }
  }
  return evaluateCompiledPredicateLineParsed(values, access_plan.predicate_steps,
                                             predicate_scratch);
}

void updateAggregateStates(const std::vector<Value>& values, const AggregateAccessPlan& access_plan,
                           const SourcePushdownSpec& pushdown, GroupState* group) {
  if (group == nullptr) {
    throw std::invalid_argument("aggregate group cannot be null");
  }
  for (std::size_t agg_index = 0; agg_index < pushdown.aggregate.aggregates.size(); ++agg_index) {
    const auto& agg = pushdown.aggregate.aggregates[agg_index];
    auto& state = group->agg_states[agg_index];
    const Value* cell = nullptr;
    if (agg.function != AggregateFunction::Count) {
      cell = &values[static_cast<std::size_t>(access_plan.local_value_by_aggregate[agg_index])];
    }
    switch (agg.function) {
      case AggregateFunction::Count:
        state.count += 1;
        break;
      case AggregateFunction::Sum:
        if (cell->isNumber()) {
          state.sum += cell->asDouble();
        }
        break;
      case AggregateFunction::Avg:
        if (cell->isNumber()) {
          state.sum += cell->asDouble();
          state.count += 1;
        }
        break;
      case AggregateFunction::Min:
        if (!state.initialized || *cell < state.value) {
          state.value = *cell;
          state.initialized = true;
        }
        break;
      case AggregateFunction::Max:
        if (!state.initialized || *cell > state.value) {
          state.value = *cell;
          state.initialized = true;
        }
        break;
    }
  }
}

void updateAggregateStatesParsed(const std::vector<ParsedScalar>& values,
                                 const AggregateAccessPlan& access_plan,
                                 const SourcePushdownSpec& pushdown, GroupState* group) {
  if (group == nullptr) {
    throw std::invalid_argument("aggregate group cannot be null");
  }
  for (std::size_t agg_index = 0; agg_index < pushdown.aggregate.aggregates.size(); ++agg_index) {
    const auto& agg = pushdown.aggregate.aggregates[agg_index];
    auto& state = group->agg_states[agg_index];
    const ParsedScalar* cell = nullptr;
    if (agg.function != AggregateFunction::Count) {
      cell = &values[static_cast<std::size_t>(access_plan.local_value_by_aggregate[agg_index])];
    }
    switch (agg.function) {
      case AggregateFunction::Count:
        state.count += 1;
        break;
      case AggregateFunction::Sum:
        if (cell->is_int64) {
          state.sum += static_cast<double>(cell->int64_value);
        } else if (cell->is_double) {
          state.sum += cell->double_value;
        }
        break;
      case AggregateFunction::Avg:
        if (cell->is_int64) {
          state.sum += static_cast<double>(cell->int64_value);
          state.count += 1;
        } else if (cell->is_double) {
          state.sum += cell->double_value;
          state.count += 1;
        }
        break;
      case AggregateFunction::Min: {
        const Value parsed_value = parsedScalarToValue(*cell);
        if (!state.initialized || parsed_value < state.value) {
          state.value = parsed_value;
          state.initialized = true;
        }
        break;
      }
      case AggregateFunction::Max: {
        const Value parsed_value = parsedScalarToValue(*cell);
        if (!state.initialized || parsed_value > state.value) {
          state.value = parsed_value;
          state.initialized = true;
        }
        break;
      }
    }
  }
}

void updateAggregateStatesLineParsed(const std::vector<LineParsedScalar>& values,
                                     const AggregateAccessPlan& access_plan,
                                     const SourcePushdownSpec& pushdown, GroupState* group) {
  if (group == nullptr) {
    throw std::invalid_argument("aggregate group cannot be null");
  }
  for (std::size_t agg_index = 0; agg_index < pushdown.aggregate.aggregates.size(); ++agg_index) {
    const auto& agg = pushdown.aggregate.aggregates[agg_index];
    auto& state = group->agg_states[agg_index];
    const LineParsedScalar* cell = nullptr;
    if (agg.function != AggregateFunction::Count) {
      cell = &values[static_cast<std::size_t>(access_plan.local_value_by_aggregate[agg_index])];
    }
    switch (agg.function) {
      case AggregateFunction::Count:
        state.count += 1;
        break;
      case AggregateFunction::Sum:
        if (cell->is_int64) {
          state.sum += static_cast<double>(cell->int64_value);
        } else if (cell->is_double) {
          state.sum += cell->double_value;
        }
        break;
      case AggregateFunction::Avg:
        if (cell->is_int64) {
          state.sum += static_cast<double>(cell->int64_value);
          state.count += 1;
        } else if (cell->is_double) {
          state.sum += cell->double_value;
          state.count += 1;
        }
        break;
      case AggregateFunction::Min: {
        const Value parsed_value = lineParsedScalarToValue(*cell);
        if (!state.initialized || parsed_value < state.value) {
          state.value = parsed_value;
          state.initialized = true;
        }
        break;
      }
      case AggregateFunction::Max: {
        const Value parsed_value = lineParsedScalarToValue(*cell);
        if (!state.initialized || parsed_value > state.value) {
          state.value = parsed_value;
          state.initialized = true;
        }
        break;
      }
    }
  }
}

void accumulateAggregateRow(const std::vector<Value>& values, const AggregateAccessPlan& access_plan,
                            const SourcePushdownSpec& pushdown, AggregateGroupMap* groups) {
  if (groups == nullptr) {
    throw std::invalid_argument("aggregate groups cannot be null");
  }
  auto key = makeGroupKey(values, access_plan.local_key_indices);
  auto it = groups->find(key);
  if (it == groups->end()) {
    GroupState state;
    state.agg_states.resize(pushdown.aggregate.aggregates.size());
    it = groups->emplace(std::move(key), std::move(state)).first;
  }
  updateAggregateStates(values, access_plan, pushdown, &it->second);
}

void accumulateSingleKeyAggregateRow(const std::vector<Value>& values,
                                     const AggregateAccessPlan& access_plan,
                                     const SourcePushdownSpec& pushdown,
                                     SingleAggregateGroupMap* groups) {
  if (groups == nullptr) {
    throw std::invalid_argument("single aggregate groups cannot be null");
  }
  const Value& key = values[access_plan.local_key_indices.front()];
  auto it = groups->find(key);
  if (it == groups->end()) {
    GroupState state;
    state.agg_states.resize(pushdown.aggregate.aggregates.size());
    it = groups->emplace(key, std::move(state)).first;
  }
  updateAggregateStates(values, access_plan, pushdown, &it->second);
}

Table finalizeAggregatedGroups(const Schema& schema, const SourcePushdownSpec& pushdown,
                               AggregateGroupMap& groups, bool materialize_rows) {
  std::vector<std::string> out_fields;
  out_fields.reserve(pushdown.aggregate.keys.size() + pushdown.aggregate.aggregates.size());
  for (const auto key_idx : pushdown.aggregate.keys) {
    out_fields.push_back(schema.fields[key_idx]);
  }
  for (const auto& agg : pushdown.aggregate.aggregates) {
    out_fields.push_back(agg.output_name);
  }
  Table aggregated = makeColumnFirstTable(out_fields);
  for (auto& entry : groups) {
    Row row = entry.first.values;
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
          row.emplace_back(state.count == 0 ? Value()
                                            : Value(state.sum / static_cast<double>(state.count)));
          break;
        case AggregateFunction::Min:
        case AggregateFunction::Max:
          row.emplace_back(state.initialized ? state.value : Value());
          break;
      }
    }
    if (materialize_rows) {
      aggregated.rows.push_back(row);
    }
    for (std::size_t i = 0; i < row.size(); ++i) {
      aggregated.columnar_cache->columns[i].values.push_back(row[i]);
    }
    aggregated.columnar_cache->row_count += 1;
  }
  if (aggregated.columnar_cache->row_count > 0) {
    aggregated.columnar_cache->batch_row_counts.push_back(aggregated.columnar_cache->row_count);
  }
  return aggregated;
}

Table finalizeSingleKeyAggregatedGroups(const Schema& schema, const SourcePushdownSpec& pushdown,
                                        SingleAggregateGroupMap& groups, bool materialize_rows) {
  std::vector<std::string> out_fields;
  out_fields.reserve(pushdown.aggregate.keys.size() + pushdown.aggregate.aggregates.size());
  out_fields.push_back(schema.fields[pushdown.aggregate.keys.front()]);
  for (const auto& agg : pushdown.aggregate.aggregates) {
    out_fields.push_back(agg.output_name);
  }
  Table aggregated = makeColumnFirstTable(out_fields);
  for (auto& entry : groups) {
    Row row;
    row.reserve(out_fields.size());
    row.push_back(entry.first);
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
          row.emplace_back(state.count == 0 ? Value()
                                            : Value(state.sum / static_cast<double>(state.count)));
          break;
        case AggregateFunction::Min:
        case AggregateFunction::Max:
          row.emplace_back(state.initialized ? state.value : Value());
          break;
      }
    }
    if (materialize_rows) {
      aggregated.rows.push_back(row);
    }
    for (std::size_t i = 0; i < row.size(); ++i) {
      aggregated.columnar_cache->columns[i].values.push_back(row[i]);
    }
    aggregated.columnar_cache->row_count += 1;
  }
  if (aggregated.columnar_cache->row_count > 0) {
    aggregated.columnar_cache->batch_row_counts.push_back(aggregated.columnar_cache->row_count);
  }
  return aggregated;
}

Table finalizeSingleCountGroups(const Schema& schema, const SourcePushdownSpec& pushdown,
                                SingleCountGroupMap& groups, bool materialize_rows) {
  std::vector<std::string> out_fields;
  out_fields.push_back(schema.fields[pushdown.aggregate.keys.front()]);
  out_fields.push_back(pushdown.aggregate.aggregates.front().output_name);
  Table aggregated = makeColumnFirstTable(out_fields);
  aggregated.columnar_cache->row_count = groups.size();
  for (auto& column : aggregated.columnar_cache->columns) {
    column.values.reserve(groups.size());
  }
  for (const auto& entry : groups) {
    if (materialize_rows) {
      Row row;
      row.reserve(2);
      row.push_back(entry.first);
      row.emplace_back(static_cast<int64_t>(entry.second));
      aggregated.rows.push_back(std::move(row));
    }
    aggregated.columnar_cache->columns[0].values.push_back(entry.first);
    aggregated.columnar_cache->columns[1].values.push_back(
        Value(static_cast<int64_t>(entry.second)));
  }
  if (aggregated.columnar_cache->row_count > 0) {
    aggregated.columnar_cache->batch_row_counts.push_back(aggregated.columnar_cache->row_count);
  }
  return aggregated;
}

Table finalizeSingleNumericAggregateGroups(const Schema& schema,
                                           const SourcePushdownSpec& pushdown,
                                           SingleNumericAggregateGroupMap& groups,
                                           bool materialize_rows) {
  std::vector<std::string> out_fields;
  out_fields.push_back(schema.fields[pushdown.aggregate.keys.front()]);
  out_fields.push_back(pushdown.aggregate.aggregates.front().output_name);
  Table aggregated = makeColumnFirstTable(out_fields);
  aggregated.columnar_cache->row_count = groups.size();
  for (auto& column : aggregated.columnar_cache->columns) {
    column.values.reserve(groups.size());
  }
  const auto fn = pushdown.aggregate.aggregates.front().function;
  for (const auto& entry : groups) {
    Value out_value;
    switch (fn) {
      case AggregateFunction::Sum:
        out_value = Value(entry.second.sum);
        break;
      case AggregateFunction::Avg:
        out_value = entry.second.count == 0
                        ? Value()
                        : Value(entry.second.sum /
                                static_cast<double>(entry.second.count));
        break;
      case AggregateFunction::Min:
      case AggregateFunction::Max:
        out_value = entry.second.has_value ? entry.second.value : Value();
        break;
      case AggregateFunction::Count:
        out_value = Value();
        break;
    }
    if (materialize_rows) {
      Row row;
      row.reserve(2);
      row.push_back(entry.first);
      row.push_back(out_value);
      aggregated.rows.push_back(std::move(row));
    }
    aggregated.columnar_cache->columns[0].values.push_back(entry.first);
    aggregated.columnar_cache->columns[1].values.push_back(out_value);
  }
  if (aggregated.columnar_cache->row_count > 0) {
    aggregated.columnar_cache->batch_row_counts.push_back(aggregated.columnar_cache->row_count);
  }
  return aggregated;
}

bool applyAggregatePushdown(const Table& input, const SourcePushdownSpec& pushdown, Table* out) {
  if (out == nullptr) {
    throw std::invalid_argument("aggregate pushdown output cannot be null");
  }
  if (!validateAggregatePushdown(input.schema, pushdown)) {
    return false;
  }
  const auto access_plan = buildAggregateAccessPlan(input.schema, pushdown);
  const bool single_key_aggregate = pushdown.aggregate.keys.size() == 1;
  AggregateGroupMap groups;
  SingleAggregateGroupMap single_groups;
  if (single_key_aggregate) {
    single_groups.reserve(64);
  } else {
    groups.reserve(64);
  }
  const std::size_t row_count =
      input.columnar_cache ? input.columnar_cache->row_count : input.rowCount();
  std::vector<Value> values(access_plan.schema_indices.size());
  std::vector<uint8_t> predicate_scratch(access_plan.predicate_steps.size());
  for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
    for (std::size_t local_index = 0; local_index < access_plan.schema_indices.size(); ++local_index) {
      values[local_index] =
          input.columnar_cache->columns[access_plan.schema_indices[local_index]].values[row_index];
    }
    if (!includeRowForAggregate(values, access_plan, pushdown, &predicate_scratch)) {
      continue;
    }
    if (single_key_aggregate) {
      accumulateSingleKeyAggregateRow(values, access_plan, pushdown, &single_groups);
    } else {
      accumulateAggregateRow(values, access_plan, pushdown, &groups);
    }
  }
  Table aggregated =
      single_key_aggregate
          ? finalizeSingleKeyAggregatedGroups(input.schema, pushdown, single_groups, false)
          : finalizeAggregatedGroups(input.schema, pushdown, groups, false);
  *out = std::move(aggregated);
  return true;
}

}  // namespace

FileSourceProbeResult probe_file_source(const std::string& path) {
  FileSourceProbeResult result;
  result.path = path;
  result.suggested_table_name = defaultTableNameFromPath(path);
  const auto lines = readSampleLines(path, 4);
  if (lines.empty()) {
    throw std::runtime_error("probe found empty input: " + path);
  }
  result.candidates = buildProbeCandidates(path, lines);
  if (result.candidates.empty()) {
    throw std::runtime_error("probe could not score any candidate formats: " + path);
  }
  const auto& best = result.candidates.front();
  result.kind = best.kind;
  result.format_name = best.format_name;
  result.score = best.score;
  result.confidence = probeConfidence(result.candidates);
  result.warnings = probeWarnings(result.candidates, result.confidence);
  if (best.kind == FileSourceKind::Json) {
    result.json_options = probeJsonOptions(path);
    result.schema = infer_json_file_schema(result.json_options);
  } else if (best.kind == FileSourceKind::Csv) {
    result.csv_delimiter = best.format_name == "tsv" ? '\t' : ',';
    bool csv_header = false;
    const auto delimiter = chooseDelimitedFormat(lines, &csv_header);
    if (delimiter.has_value()) {
      result.csv_delimiter = *delimiter;
    }
    result.schema = read_csv_schema(path, result.csv_delimiter);
  } else {
    result.line_options = probeLineOptions(path);
    result.schema = infer_line_file_schema(result.line_options);
  }
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
  SimpleLineRegexPlan simple_plan;
  if (options.mode == LineParseMode::Regex) {
    simple_plan = compileSimpleLineRegexPlan(options.regex_pattern);
    if (!simple_plan.supported) {
      line_regex = std::regex(options.regex_pattern, std::regex::optimize);
    }
  }
  const auto capture_plan =
      options.mode == LineParseMode::Split ? buildLineCapturePlan(options) : LineCapturePlan{};

  std::string line;
  std::vector<std::string_view> regex_captures;
  std::vector<LineParsedScalar> regex_parsed_values;
  std::vector<std::size_t> regex_schema_indices;
  if (options.mode == LineParseMode::Regex && simple_plan.supported) {
    regex_parsed_values.resize(options.mappings.size());
    regex_schema_indices.resize(options.mappings.size());
    for (std::size_t i = 0; i < regex_schema_indices.size(); ++i) {
      regex_schema_indices[i] = i;
    }
  }
  while (std::getline(input, line)) {
    if (options.skip_empty_lines && line.empty()) {
      continue;
    }
    Row row;
    row.reserve(options.mappings.size());
    if (options.mode == LineParseMode::Split) {
      row.resize(options.mappings.size());
      fillLineSplitValues(line, capture_plan, &row, options.split_delimiter);
    } else {
      bool matched_ok = false;
      regex_captures.clear();
      std::cmatch matched;
      if (simple_plan.supported) {
        matched_ok = matchSimpleLineRegex(simple_plan, line, &regex_captures);
      } else {
        const char* begin = line.data();
        const char* end = begin + line.size();
        matched_ok = std::regex_match(begin, end, matched, line_regex);
      }
      if (!matched_ok) continue;
      if (simple_plan.supported) {
        resetLineParsedScalarVector(&regex_parsed_values);
        fillLineRegexParsedValues(regex_captures, regex_schema_indices, options,
                                  &regex_parsed_values);
      } else {
        for (const auto& mapping : options.mappings) {
          if (mapping.source_index < matched.size()) {
            const auto& capture = matched[mapping.source_index];
            row.push_back(parseScalarCell(
                std::string_view(capture.first,
                                 static_cast<std::size_t>(capture.second - capture.first))));
          } else {
            row.emplace_back(Value());
          }
        }
      }
      if (simple_plan.supported) {
        row.clear();
        row.reserve(options.mappings.size());
        for (const auto& parsed : regex_parsed_values) {
          row.push_back(lineParsedScalarToValue(parsed));
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
  const auto json_index = buildJsonColumnIndex(options);

  if (options.format == JsonFileFormat::JsonLines) {
    std::string line;
    while (std::getline(input, line)) {
      if (line.empty()) {
        continue;
      }
      JsonCursor cursor(line);
      Row row(options.columns.size());
      cursor.forEachObjectEntry([&](std::string key, Value value) {
        const auto it = json_index.find(key);
        if (it != json_index.end()) {
          row[it->second] = std::move(value);
        }
      });
      appendRow(&table, std::move(row));
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
      Row row(options.columns.size());
      cursor.forEachObjectEntry([&](std::string key, Value value) {
        const auto it = json_index.find(key);
        if (it != json_index.end()) {
          row[it->second] = std::move(value);
        }
      });
      appendRow(&table, std::move(row));
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
  if (pushdown.has_aggregate) {
    if (spec.kind == FileSourceKind::Line) {
      if (try_execute_line_aggregate(spec.path, schema, spec.line_options, pushdown, out)) {
        return true;
      }
    } else {
      if (try_execute_json_aggregate(spec.path, schema, spec.json_options, pushdown, out)) {
        return true;
      }
    }
  }
  const bool has_selection_pushdown =
      !pushdown.filters.empty() || static_cast<bool>(pushdown.predicate_expr) || pushdown.limit != 0;
  if (has_selection_pushdown) {
    if (spec.kind == FileSourceKind::Line) {
      return executeLineSourcePushdown(spec.path, schema, spec.line_options, pushdown,
                                       materialize_rows, out);
    }
    return executeJsonSourcePushdown(spec.path, schema, spec.json_options, pushdown,
                                     materialize_rows, out);
  }
  const auto captured_schema_indices = buildCapturedSchemaIndices(schema, pushdown);
  const bool use_columnar_capture =
      captured_schema_indices.size() < schema.fields.size();
  Table loaded =
      use_columnar_capture
          ? (spec.kind == FileSourceKind::Line
                 ? loadLineFileSelected(spec.path, schema, spec.line_options, captured_schema_indices)
                 : loadJsonFileSelected(spec.path, schema, spec.json_options, captured_schema_indices))
          : (spec.kind == FileSourceKind::Line)
                ? load_line_file(spec.path, spec.line_options)
                : load_json_file(spec.path, spec.json_options);
  if (!pushdown.filters.empty() || pushdown.predicate_expr || pushdown.limit != 0) {
    loaded = applyFilterAndLimit(loaded, pushdown);
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
