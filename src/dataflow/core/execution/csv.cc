#include "src/dataflow/core/execution/csv.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <fstream>
#include <limits>
#include <memory_resource>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <unordered_map>

#include "src/dataflow/core/execution/columnar_batch.h"
#include "src/dataflow/core/execution/runtime/simd_dispatch.h"

namespace dataflow {

namespace {

constexpr std::size_t kReadAllColumns = static_cast<std::size_t>(-1);
constexpr std::size_t kCsvReadChunkSize = 1 << 16;
constexpr std::size_t kCsvSampleBytes = 1 << 15;
constexpr std::string_view kTrueLower = "true";
constexpr std::string_view kTrueUpper = "TRUE";
constexpr std::string_view kFalseLower = "false";
constexpr std::string_view kFalseUpper = "FALSE";

bool useSimdCsvTokenizer(const SourceExecutionPattern* execution_pattern) {
  return execution_pattern != nullptr &&
         execution_pattern->tokenizer_mode == SourceTokenizerMode::SimdFastUnquoted;
}

bool hasSignificantLeadingZero(std::string_view s) {
  if (s.empty()) return false;
  std::size_t pos = 0;
  if (s[0] == '-' || s[0] == '+') pos = 1;
  if (pos + 1 >= s.size()) return false;
  return s[pos] == '0';
}

bool mightBeDoubleLexically(std::string_view s) {
  if (s.empty()) return false;
  bool saw_digit = false;
  bool saw_decimal_or_exp = false;
  for (char ch : s) {
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

enum class Int64ParseStatus {
  NotInteger,
  Parsed,
  Overflow,
};

struct ParsedCell {
  std::string_view text;
  std::optional<std::string> owned_text;
  bool is_null = false;
  bool is_bool = false;
  bool bool_value = false;
  bool is_int64 = false;
  int64_t int64_value = 0;
  bool is_double = false;
  double double_value = 0.0;
};

const ParsedCell& emptyParsedCell() {
  static const ParsedCell kEmptyParsedCell{};
  return kEmptyParsedCell;
}

std::string_view parsedCellText(const ParsedCell& parsed) {
  return parsed.owned_text.has_value() ? std::string_view(*parsed.owned_text) : parsed.text;
}

Int64ParseStatus parseInt64(std::string_view s, int64_t* out) {
  if (s.empty()) {
    return Int64ParseStatus::NotInteger;
  }
  const auto result = std::from_chars(s.data(), s.data() + s.size(), *out);
  if (result.ec == std::errc() && result.ptr == s.data() + s.size()) {
    return Int64ParseStatus::Parsed;
  }
  if (result.ec == std::errc::result_out_of_range) {
    return Int64ParseStatus::Overflow;
  }
  return Int64ParseStatus::NotInteger;
}

bool parseDouble(std::string_view s, double* out) {
  if (s.empty()) return false;
  constexpr std::size_t kStackBufferSize = 128;
  if (s.size() < kStackBufferSize) {
    std::array<char, kStackBufferSize> buffer{};
    std::memcpy(buffer.data(), s.data(), s.size());
    buffer[s.size()] = '\0';
    char* end = nullptr;
    *out = std::strtod(buffer.data(), &end);
    return end != buffer.data() && *end == '\0';
  }
  std::string owned(s);
  char* end = nullptr;
  *out = std::strtod(owned.c_str(), &end);
  return end != owned.c_str() && *end == '\0';
}

Value parseCell(std::string_view cell) {
  if (cell.empty()) return Value();
  if (cell == kTrueLower || cell == kTrueUpper) return Value(true);
  if (cell == kFalseLower || cell == kFalseUpper) return Value(false);
  if (hasSignificantLeadingZero(cell)) return Value(std::string(cell));
  if (cell.size() >= 2 && cell.front() == '[' && cell.back() == ']') {
    const auto vec = Value::parseFixedVector(std::string(cell));
    if (!vec.empty()) {
      return Value(vec);
    }
  }
  int64_t int_value = 0;
  switch (parseInt64(cell, &int_value)) {
    case Int64ParseStatus::Parsed:
      return Value(int_value);
    case Int64ParseStatus::Overflow:
      return Value(std::string(cell));
    case Int64ParseStatus::NotInteger:
      break;
  }
  if (mightBeDoubleLexically(cell)) {
    double double_value = 0.0;
    if (parseDouble(cell, &double_value)) {
      return Value(double_value);
    }
  }
  return Value(std::string(cell));
}

ParsedCell parseParsedCell(std::string_view cell) {
  ParsedCell parsed;
  parsed.text = cell;
  if (cell.empty()) {
    parsed.is_null = true;
    return parsed;
  }
  if (cell == kTrueLower || cell == kTrueUpper) {
    parsed.is_bool = true;
    parsed.bool_value = true;
    return parsed;
  }
  if (cell == kFalseLower || cell == kFalseUpper) {
    parsed.is_bool = true;
    parsed.bool_value = false;
    return parsed;
  }
  if (hasSignificantLeadingZero(cell)) {
    return parsed;
  }
  if (cell.size() >= 2 && cell.front() == '[' && cell.back() == ']') {
    return parsed;
  }
  int64_t int_value = 0;
  switch (parseInt64(cell, &int_value)) {
    case Int64ParseStatus::Parsed:
      parsed.is_int64 = true;
      parsed.int64_value = int_value;
      return parsed;
    case Int64ParseStatus::Overflow:
      return parsed;
    case Int64ParseStatus::NotInteger:
      break;
  }
  if (mightBeDoubleLexically(cell)) {
    double double_value = 0.0;
    if (parseDouble(cell, &double_value)) {
      parsed.is_double = true;
      parsed.double_value = double_value;
      return parsed;
    }
  }
  return parsed;
}

ParsedCell persistParsedCell(ParsedCell parsed) {
  if (!parsed.is_null && !parsed.is_bool && !parsed.is_int64 && !parsed.is_double &&
      !parsedCellText(parsed).empty()) {
    parsed.owned_text.emplace(parsedCellText(parsed));
    parsed.text = std::string_view();
  }
  return parsed;
}

void resetParsedCell(ParsedCell* parsed) {
  if (parsed == nullptr) {
    return;
  }
  parsed->text = std::string_view();
  parsed->owned_text.reset();
  parsed->is_null = false;
  parsed->is_bool = false;
  parsed->bool_value = false;
  parsed->is_int64 = false;
  parsed->int64_value = 0;
  parsed->is_double = false;
  parsed->double_value = 0.0;
}

Value parsedCellToValue(const ParsedCell& parsed) {
  if (parsed.is_null) return Value();
  if (parsed.is_bool) return Value(parsed.bool_value);
  if (parsed.is_int64) return Value(parsed.int64_value);
  if (parsed.is_double) return Value(parsed.double_value);
  return parseCell(parsedCellText(parsed));
}

void appendSizeToString(std::string* out, std::size_t value) {
  char buffer[32];
  const auto result = std::to_chars(buffer, buffer + sizeof(buffer), value);
  if (result.ec != std::errc()) {
    throw std::runtime_error("failed to encode key length");
  }
  out->append(buffer, result.ptr);
}

void encodeParsedGroupKeyRow(const std::vector<ParsedCell>& key_cells, std::string* out) {
  if (out == nullptr) {
    throw std::invalid_argument("group key output is null");
  }
  out->clear();
  out->reserve(std::max(out->capacity(), key_cells.size() * std::size_t(8)));
  for (const auto& cell : key_cells) {
    const auto text = parsedCellText(cell);
    appendSizeToString(out, text.size());
    out->push_back(':');
    out->append(text.data(), text.size());
  }
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

bool tryMatchesValueFilter(const Value& lhs, const Value& rhs, const std::string& op) {
  try {
    return matchesCompareOp(lhs == rhs ? 0 : (lhs < rhs ? -1 : 1), op);
  } catch (...) {
    return false;
  }
}

bool tryMatchesParsedCellFilter(const ParsedCell& lhs, const Value& rhs, const std::string& op) {
  if (lhs.is_null) {
    return tryMatchesValueFilter(Value(), rhs, op);
  }
  if (lhs.is_bool) {
    return tryMatchesValueFilter(Value(lhs.bool_value), rhs, op);
  }
  if (lhs.is_int64) {
    return tryMatchesValueFilter(Value(lhs.int64_value), rhs, op);
  }
  if (lhs.is_double) {
    return tryMatchesValueFilter(Value(lhs.double_value), rhs, op);
  }
  if (rhs.type() == DataType::String) {
    const auto rhs_text = std::string_view(rhs.asString());
    const auto lhs_text = parsedCellText(lhs);
    const int compare_result =
        lhs_text == rhs_text ? 0 : (lhs_text < rhs_text ? -1 : 1);
    return matchesCompareOp(compare_result, op);
  }
  return tryMatchesValueFilter(Value(std::string(parsedCellText(lhs))), rhs, op);
}

bool tryMatchesRawCellFilter(std::string_view cell, const Value& rhs, const std::string& op) {
  return tryMatchesParsedCellFilter(parseParsedCell(cell), rhs, op);
}

void collectPredicateColumns(const std::shared_ptr<PlanPredicateExpr>& expr,
                             const std::function<void(std::size_t)>& visit) {
  if (!expr || !visit) {
    return;
  }
  if (expr->kind == PlanPredicateExprKind::Comparison) {
    visit(expr->comparison.column_index);
    return;
  }
  collectPredicateColumns(expr->left, visit);
  collectPredicateColumns(expr->right, visit);
}

bool evaluatePredicateExprParsed(const std::vector<ParsedCell>& values,
                                 const std::vector<std::size_t>& generations,
                                 std::size_t row_generation,
                                 const std::vector<int>& local_by_schema,
                                 const std::shared_ptr<PlanPredicateExpr>& expr) {
  if (!expr) {
    return true;
  }
  if (expr->kind == PlanPredicateExprKind::Comparison) {
    const auto local_index = local_by_schema[expr->comparison.column_index];
    if (local_index < 0) {
      return false;
    }
    const auto local = static_cast<std::size_t>(local_index);
    const auto& value =
        generations[local] == row_generation ? values[local] : emptyParsedCell();
    return tryMatchesParsedCellFilter(value, expr->comparison.value, expr->comparison.op);
  }
  const bool left =
      evaluatePredicateExprParsed(values, generations, row_generation, local_by_schema, expr->left);
  const bool right = evaluatePredicateExprParsed(values, generations, row_generation,
                                                 local_by_schema, expr->right);
  if (expr->kind == PlanPredicateExprKind::And) {
    return left && right;
  }
  return left || right;
}

bool shouldCaptureColumn(std::size_t column_index, std::size_t capture_until_column,
                         const std::vector<uint8_t>* capture_mask) {
  if (capture_until_column != kReadAllColumns && column_index > capture_until_column) {
    return false;
  }
  return capture_mask == nullptr ||
         (column_index < capture_mask->size() && (*capture_mask)[column_index] != 0);
}

std::size_t countCapturedColumns(const std::vector<uint8_t>& capture_mask) {
  return static_cast<std::size_t>(
      std::count(capture_mask.begin(), capture_mask.end(), static_cast<uint8_t>(1)));
}

std::size_t estimateCsvRows(const std::string& path) {
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
  sample.resize(std::min<std::size_t>(file_size, kCsvSampleBytes));
  input.read(sample.data(), static_cast<std::streamsize>(sample.size()));
  sample.resize(static_cast<std::size_t>(input.gcount()));
  if (sample.empty()) {
    return 0;
  }

  bool in_quotes = false;
  bool pending_quote = false;
  bool skip_next_lf = false;
  std::size_t row_bytes = 0;
  std::size_t row_count = 0;
  std::size_t data_row_bytes = 0;

  auto finish_row = [&]() {
    if (row_count > 0) {
      data_row_bytes += row_bytes;
    }
    row_bytes = 0;
    ++row_count;
  };

  for (const char ch : sample) {
    if (skip_next_lf) {
      skip_next_lf = false;
      if (ch == '\n') {
        continue;
      }
    }
    if (pending_quote) {
      if (ch == '"') {
        pending_quote = false;
        row_bytes += 1;
        continue;
      }
      in_quotes = false;
      pending_quote = false;
    }
    if (ch == '"') {
      if (in_quotes) {
        pending_quote = true;
      } else {
        in_quotes = true;
      }
      row_bytes += 1;
      continue;
    }
    if (!in_quotes && (ch == '\n' || ch == '\r')) {
      finish_row();
      if (ch == '\r') {
        skip_next_lf = true;
      }
      continue;
    }
    row_bytes += 1;
  }
  if (row_bytes > 0) {
    finish_row();
  }
  if (row_count <= 1 || data_row_bytes == 0) {
    return 0;
  }
  const std::size_t data_rows = row_count - 1;
  const std::size_t avg_row_bytes = std::max<std::size_t>(1, data_row_bytes / data_rows);
  return std::max<std::size_t>(1, file_size / avg_row_bytes);
}

void reserveTableStorage(Table* table, const std::vector<uint8_t>& output_mask,
                         bool materialize_rows, std::size_t estimated_rows) {
  if (table == nullptr || estimated_rows == 0 || table->columnar_cache == nullptr) {
    return;
  }
  if (materialize_rows) {
    table->rows.reserve(estimated_rows);
  }
  for (std::size_t column_index = 0; column_index < output_mask.size(); ++column_index) {
    if (materialize_rows || output_mask[column_index] != 0) {
      table->columnar_cache->columns[column_index].values.reserve(estimated_rows);
    }
  }
}

void assignNullBackingsForUncapturedColumns(const std::vector<uint8_t>& output_mask,
                                            ColumnarTable* cache) {
  if (cache == nullptr || cache->row_count == 0) {
    return;
  }
  for (std::size_t column_index = 0; column_index < output_mask.size(); ++column_index) {
    if (output_mask[column_index] != 0) {
      continue;
    }
    auto backing = std::make_shared<ArrowColumnBacking>();
    backing->format = "n";
    backing->length = cache->row_count;
    backing->null_count = static_cast<int64_t>(cache->row_count);
    cache->columns[column_index].arrow_backing = std::move(backing);
  }
}

template <typename RowStartFn, typename CellFn, typename RowEndFn>
void scanCsvCells(std::istream* input, char delimiter, std::size_t capture_until_column,
                  const std::vector<uint8_t>* capture_mask, RowStartFn&& on_row_start,
                  CellFn&& on_cell, RowEndFn&& on_row_end,
                  const SourceExecutionPattern* execution_pattern = nullptr) {
  if (input == nullptr) {
    throw std::invalid_argument("csv input is null");
  }
  std::array<char, kCsvReadChunkSize> chunk{};
  std::string scratch;
  scratch.reserve(64);
  bool scratch_active = false;
  bool in_quotes = false;
  bool pending_quote = false;
  bool skip_next_lf = false;
  std::size_t column_index = 0;
  bool keep_cell = shouldCaptureColumn(column_index, capture_until_column, capture_mask);
  const char* segment_start = nullptr;
  const bool enable_simd_fast_tokenizer = useSimdCsvTokenizer(execution_pattern);
  const std::array<char, 4> special_bytes = {delimiter, '"', '\n', '\r'};
  on_row_start();

  auto flush_segment_to_scratch = [&](const char* segment_end) {
    if (!keep_cell || segment_start == nullptr) {
      return;
    }
    scratch.append(segment_start,
                   static_cast<std::size_t>(segment_end - segment_start));
    segment_start = nullptr;
  };
  auto activate_scratch = [&](const char* segment_end) {
    if (!keep_cell) {
      segment_start = nullptr;
      return;
    }
    if (!scratch_active) {
      scratch.clear();
      scratch_active = true;
    }
    flush_segment_to_scratch(segment_end);
  };
  auto emit_cell = [&](const char* segment_end) -> bool {
    if (keep_cell) {
      if (scratch_active) {
        flush_segment_to_scratch(segment_end);
        if (!on_cell(column_index, std::string_view(scratch))) {
          return false;
        }
      } else if (segment_start != nullptr) {
        if (!on_cell(column_index,
                     std::string_view(segment_start,
                                      static_cast<std::size_t>(segment_end - segment_start)))) {
          return false;
        }
      } else if (!on_cell(column_index, std::string_view())) {
        return false;
      }
    }
    scratch.clear();
    scratch_active = false;
    segment_start = nullptr;
    ++column_index;
    keep_cell = shouldCaptureColumn(column_index, capture_until_column, capture_mask);
    return true;
  };
  auto emit_row = [&](const char* segment_end) -> bool {
    if (!emit_cell(segment_end)) {
      return false;
    }
    const std::size_t total_columns = column_index;
    column_index = 0;
    keep_cell = shouldCaptureColumn(column_index, capture_until_column, capture_mask);
    scratch.clear();
    scratch_active = false;
    segment_start = nullptr;
    if (!on_row_end(total_columns)) {
      return false;
    }
    on_row_start();
    return true;
  };

  while (input->good()) {
    input->read(chunk.data(), static_cast<std::streamsize>(chunk.size()));
    const std::streamsize read = input->gcount();
    if (read <= 0) {
      break;
    }
    const char* chunk_begin = chunk.data();
    const char* chunk_end = chunk_begin + read;
    const char* ptr = chunk_begin;
    while (ptr != chunk_end) {
      if (enable_simd_fast_tokenizer && !keep_cell && !in_quotes && !pending_quote &&
          !skip_next_lf && !scratch_active) {
        char matched = '\0';
        const char* special = simdDispatch().find_first_of(
            ptr, chunk_end, special_bytes.data(), special_bytes.size(), &matched);
        if (special == nullptr) {
          ptr = chunk_end;
          break;
        }
        ptr = special;
      }
      const char ch = *ptr;
      if (skip_next_lf) {
        skip_next_lf = false;
        if (ch == '\n') {
          ++ptr;
          continue;
        }
      }
      if (pending_quote) {
        if (ch == '"') {
          pending_quote = false;
          if (keep_cell) {
            activate_scratch(ptr);
            scratch.push_back('"');
          }
          ++ptr;
          continue;
        }
        in_quotes = false;
        pending_quote = false;
      }
      if (ch == '"') {
        if (in_quotes) {
          pending_quote = true;
        } else {
          in_quotes = true;
          activate_scratch(ptr);
        }
        ++ptr;
        continue;
      }
      if (!in_quotes && ch == delimiter) {
        if (!emit_cell(ptr)) {
          return;
        }
        ++ptr;
        continue;
      }
      if (!in_quotes && (ch == '\n' || ch == '\r')) {
        if (!emit_row(ptr)) {
          return;
        }
        if (ch == '\r') {
          skip_next_lf = true;
        }
        ++ptr;
        continue;
      }
      if (!keep_cell) {
        ++ptr;
        continue;
      }
      if (scratch_active) {
        scratch.push_back(ch);
      } else if (segment_start == nullptr) {
        segment_start = ptr;
      }
      ++ptr;
    }
    if (keep_cell && segment_start != nullptr) {
      activate_scratch(chunk_end);
    }
  }
  if (pending_quote) {
    pending_quote = false;
    in_quotes = false;
  }
  if (column_index > 0 || scratch_active || segment_start != nullptr) {
    (void)emit_row(segment_start);
  }
}


Schema readCsvHeader(std::ifstream* input, const std::string& path, char delimiter) {
  if (input == nullptr || !input->is_open()) {
    throw std::runtime_error("cannot open csv file: " + path);
  }
  std::vector<std::string> header;
  scanCsvCells(
      input, delimiter, kReadAllColumns, nullptr, [&]() { header.clear(); },
      [&](std::size_t, std::string_view cell) {
        header.emplace_back(cell);
        return true;
      },
      [&](std::size_t) { return false; });
  return Schema(std::move(header));
}

Table loadCsvInternal(const std::string& path, char delimiter, bool materialize_rows,
                      const Schema* schema_hint,
                      const std::vector<std::size_t>* projected_columns) {
  Schema schema;
  if (schema_hint == nullptr) {
    std::ifstream header_input(path, std::ios::binary);
    schema = readCsvHeader(&header_input, path, delimiter);
  } else {
    schema = *schema_hint;
  }

  std::ifstream input(path, std::ios::binary);
  if (!input.is_open()) {
    throw std::runtime_error("cannot open csv file: " + path);
  }

  std::vector<uint8_t> projected_mask(schema.fields.size(), static_cast<uint8_t>(1));
  std::size_t last_required_column =
      schema.fields.empty() ? 0 : (schema.fields.size() - 1);
  if (projected_columns != nullptr && !projected_columns->empty()) {
    std::fill(projected_mask.begin(), projected_mask.end(), static_cast<uint8_t>(0));
    last_required_column = 0;
    for (const auto column_index : *projected_columns) {
      if (column_index >= projected_mask.size()) {
        throw std::runtime_error("projected csv column index out of range");
      }
      projected_mask[column_index] = 1;
      last_required_column = std::max(last_required_column, column_index);
    }
  }

  Table table;
  table.schema = schema;
  table.columnar_cache = std::make_shared<ColumnarTable>();
  table.columnar_cache->schema = table.schema;
  table.columnar_cache->columns.resize(table.schema.fields.size());
  table.columnar_cache->arrow_formats.resize(table.schema.fields.size());
  const auto estimated_rows = estimateCsvRows(path);
  reserveTableStorage(&table, projected_mask, materialize_rows, estimated_rows);
  const std::size_t scanned_column_count = last_required_column + 1;
  bool skip_header = true;
  std::size_t row_start = 0;
  Row row;
  if (materialize_rows) {
    row.reserve(projected_columns == nullptr ? table.schema.fields.size()
                                             : countCapturedColumns(projected_mask));
  }
  scanCsvCells(
      &input, delimiter, scanned_column_count - 1, &projected_mask,
      [&]() {
        row_start = table.columnar_cache->row_count;
        if (materialize_rows) {
          row.clear();
        }
      },
      [&](std::size_t column_index, std::string_view cell_value) {
        Value parsed = parseCell(cell_value);
        table.columnar_cache->columns[column_index].values.push_back(parsed);
        if (materialize_rows) {
          row.push_back(std::move(parsed));
        }
        return true;
      },
      [&](std::size_t total_columns) {
        if (skip_header) {
          skip_header = false;
          for (std::size_t i = 0; i < table.columnar_cache->columns.size(); ++i) {
            if (materialize_rows || projected_mask[i] != 0) {
              table.columnar_cache->columns[i].values.resize(row_start);
            }
          }
          return true;
        }
        const bool row_valid =
            materialize_rows ? (total_columns == table.schema.fields.size())
                             : (total_columns >= scanned_column_count);
        if (row_valid) {
          if (materialize_rows) {
            table.rows.push_back(row);
          }
          table.columnar_cache->row_count += 1;
        } else {
          for (std::size_t i = 0; i < table.columnar_cache->columns.size(); ++i) {
            if (materialize_rows || projected_mask[i] != 0) {
              table.columnar_cache->columns[i].values.resize(row_start);
            }
          }
        }
        return true;
      });

  if (table.columnar_cache->row_count > 0) {
    assignNullBackingsForUncapturedColumns(projected_mask, table.columnar_cache.get());
    table.columnar_cache->batch_row_counts.push_back(table.columnar_cache->row_count);
  }
  return table;
}

}  // namespace

Schema read_csv_schema(const std::string& path, char delimiter) {
  std::ifstream input(path);
  return readCsvHeader(&input, path, delimiter);
}

Table load_csv(const std::string& path, char delimiter, bool materialize_rows) {
  return loadCsvInternal(path, delimiter, materialize_rows, nullptr, nullptr);
}

Table load_csv_projected(const std::string& path, const Schema& schema,
                         const std::vector<std::size_t>& projected_columns, char delimiter,
                         bool materialize_rows) {
  return loadCsvInternal(path, delimiter, materialize_rows, &schema, &projected_columns);
}

bool execute_csv_source_pushdown(const std::string& path, const Schema& schema,
                                 const SourcePushdownSpec& pushdown,
                                 char delimiter, bool materialize_rows, Table* out,
                                 const SourceExecutionPattern* execution_pattern) {
  if (out == nullptr) {
    throw std::invalid_argument("csv source pushdown output is null");
  }
  const bool has_filters = !pushdown.filters.empty() || static_cast<bool>(pushdown.predicate_expr);
  if (pushdown.has_aggregate) {
    if (!try_execute_csv_aggregate(path, schema, pushdown, delimiter, out, execution_pattern)) {
      return false;
    }
    if (pushdown.limit != 0 && out->rowCount() > pushdown.limit) {
      *out = limitTable(*out, pushdown.limit, false);
    }
    return true;
  }
  if (!has_filters && pushdown.limit == 0) {
    const bool require_all_columns =
        pushdown.projected_columns.empty() || pushdown.projected_columns.size() >= schema.fields.size();
    *out = require_all_columns
               ? load_csv(path, delimiter, materialize_rows)
               : load_csv_projected(path, schema, pushdown.projected_columns, delimiter,
                                    materialize_rows);
    return true;
  }

  for (const auto& filter : pushdown.filters) {
    if (filter.column_index >= schema.fields.size()) {
      return false;
    }
  }
  bool predicate_valid = true;
  collectPredicateColumns(pushdown.predicate_expr, [&](std::size_t column_index) {
    if (column_index >= schema.fields.size()) {
      predicate_valid = false;
    }
  });
  if (!predicate_valid) {
    return false;
  }

  const bool require_all_columns =
      pushdown.projected_columns.empty() || pushdown.projected_columns.size() >= schema.fields.size();
  std::vector<uint8_t> output_mask(schema.fields.size(),
                                   static_cast<uint8_t>(require_all_columns ? 1 : 0));
  std::vector<uint8_t> capture_mask = output_mask;
  std::size_t last_required_column =
      schema.fields.empty() ? 0 : (schema.fields.size() - 1);
  if (!require_all_columns) {
    last_required_column = 0;
    for (const auto column_index : pushdown.projected_columns) {
      if (column_index >= schema.fields.size()) {
        return false;
      }
      output_mask[column_index] = 1;
      capture_mask[column_index] = 1;
      last_required_column = std::max(last_required_column, column_index);
    }
  }
  for (const auto& filter : pushdown.filters) {
    capture_mask[filter.column_index] = 1;
    last_required_column = std::max(last_required_column, filter.column_index);
  }
  std::vector<int> predicate_local_by_schema(schema.fields.size(), -1);
  std::vector<std::size_t> predicate_schema_indices;
  collectPredicateColumns(pushdown.predicate_expr, [&](std::size_t column_index) {
    capture_mask[column_index] = 1;
    last_required_column = std::max(last_required_column, column_index);
    if (predicate_local_by_schema[column_index] < 0) {
      predicate_local_by_schema[column_index] = static_cast<int>(predicate_schema_indices.size());
      predicate_schema_indices.push_back(column_index);
    }
  });

  std::ifstream input(path, std::ios::binary);
  if (!input.is_open()) {
    throw std::runtime_error("cannot open csv file: " + path);
  }

  Table table;
  table.schema = schema;
  table.columnar_cache = std::make_shared<ColumnarTable>();
  table.columnar_cache->schema = table.schema;
  table.columnar_cache->columns.resize(table.schema.fields.size());
  table.columnar_cache->arrow_formats.resize(table.schema.fields.size());
  const auto estimated_rows = estimateCsvRows(path);
  const auto reserve_rows =
      pushdown.limit == 0 ? estimated_rows
                          : std::min<std::size_t>(estimated_rows == 0 ? pushdown.limit
                                                                      : estimated_rows,
                                                  pushdown.limit);
  reserveTableStorage(&table, output_mask, materialize_rows, reserve_rows);

  bool skip_header = true;
  std::size_t row_start = 0;
  Row row;
  bool filter_match = !has_filters;
  std::vector<std::vector<std::size_t>> filter_positions_by_column(last_required_column + 1);
  for (std::size_t filter_index = 0; filter_index < pushdown.filters.size(); ++filter_index) {
    filter_positions_by_column[pushdown.filters[filter_index].column_index].push_back(filter_index);
  }
  std::vector<ParsedCell> predicate_values(predicate_schema_indices.size());
  std::vector<std::size_t> predicate_generations(predicate_schema_indices.size(), 0);
  std::size_t row_generation = 0;
  if (materialize_rows) {
    row.reserve(require_all_columns ? table.schema.fields.size() : pushdown.projected_columns.size());
  }
  scanCsvCells(
      &input, delimiter, last_required_column, &capture_mask,
      [&]() {
        ++row_generation;
        row_start = table.columnar_cache->row_count;
        filter_match = true;
        if (materialize_rows) {
          row.clear();
        }
      },
      [&](std::size_t column_index, std::string_view cell_value) {
        if (skip_header) {
          return true;
        }
        ParsedCell parsed;
        const bool has_filter = column_index < filter_positions_by_column.size() &&
                                !filter_positions_by_column[column_index].empty();
        const bool needed_for_predicate =
            column_index < predicate_local_by_schema.size() &&
            predicate_local_by_schema[column_index] >= 0;
        const bool needed_for_output = output_mask[column_index] != 0;
        if (has_filter || needed_for_predicate || needed_for_output) {
          parsed = parseParsedCell(cell_value);
        }
        if (has_filter) {
          for (const auto filter_index : filter_positions_by_column[column_index]) {
            const auto& filter = pushdown.filters[filter_index];
            filter_match =
                filter_match && tryMatchesParsedCellFilter(parsed, filter.value, filter.op);
          }
        }
        if (needed_for_predicate) {
          const auto local_index = static_cast<std::size_t>(predicate_local_by_schema[column_index]);
          predicate_values[local_index] = persistParsedCell(parsed);
          predicate_generations[local_index] = row_generation;
        }
        if (needed_for_output) {
          Value output_value = parsedCellToValue(parsed);
          table.columnar_cache->columns[column_index].values.push_back(output_value);
          if (materialize_rows) {
            row.push_back(std::move(output_value));
          }
        }
        return true;
      },
      [&](std::size_t total_columns) {
        if (skip_header) {
          skip_header = false;
          for (std::size_t i = 0; i < table.columnar_cache->columns.size(); ++i) {
            if (output_mask[i] != 0) {
              table.columnar_cache->columns[i].values.resize(row_start);
            }
          }
          return true;
        }
        const bool predicate_match =
            evaluatePredicateExprParsed(predicate_values, predicate_generations, row_generation,
                                        predicate_local_by_schema, pushdown.predicate_expr);
        if (total_columns < last_required_column + 1 || !filter_match || !predicate_match) {
          for (std::size_t i = 0; i < table.columnar_cache->columns.size(); ++i) {
            if (output_mask[i] != 0) {
              table.columnar_cache->columns[i].values.resize(row_start);
            }
          }
          return true;
        }
        if (materialize_rows) {
          table.rows.push_back(row);
        }
        table.columnar_cache->row_count += 1;
        if (pushdown.limit != 0 && table.columnar_cache->row_count >= pushdown.limit) {
          return false;
        }
        return true;
      },
      execution_pattern);

  if (table.columnar_cache->row_count > 0) {
    assignNullBackingsForUncapturedColumns(output_mask, table.columnar_cache.get());
    table.columnar_cache->batch_row_counts.push_back(table.columnar_cache->row_count);
  }
  *out = std::move(table);
  return true;
}

Table makeCsvAggregateResultTable(const Schema& schema,
                                  const std::vector<std::size_t>& key_indices,
                                  const std::vector<AggregateSpec>& aggs) {
  Table result;
  for (const auto key_index : key_indices) {
    result.schema.fields.push_back(schema.fields[key_index]);
  }
  for (const auto& agg : aggs) {
    result.schema.fields.push_back(agg.output_name);
  }
  for (std::size_t i = 0; i < result.schema.fields.size(); ++i) {
    result.schema.index[result.schema.fields[i]] = i;
  }
  result.columnar_cache = std::make_shared<ColumnarTable>();
  result.columnar_cache->schema = result.schema;
  result.columnar_cache->columns.resize(result.schema.fields.size());
  result.columnar_cache->arrow_formats.resize(result.schema.fields.size());
  return result;
}

bool try_execute_csv_aggregate(const std::string& path, const Schema& schema,
                               const SourcePushdownSpec& pushdown,
                               char delimiter, Table* out,
                               const SourceExecutionPattern* execution_pattern) {
  if (out == nullptr) {
    throw std::invalid_argument("csv aggregate output is null");
  }
  const auto& key_indices = pushdown.aggregate.keys;
  const auto& aggs = pushdown.aggregate.aggregates;
  const auto& filters = pushdown.filters;
  if (key_indices.empty() || aggs.empty()) {
    return false;
  }
  std::size_t last_required_column = 0;
  for (const auto key_index : key_indices) {
    if (key_index >= schema.fields.size()) {
      return false;
    }
    last_required_column = std::max(last_required_column, key_index);
  }
  for (const auto& filter : filters) {
    if (filter.column_index >= schema.fields.size()) {
      return false;
    }
    last_required_column = std::max(last_required_column, filter.column_index);
  }
  bool predicate_valid = true;
  std::vector<int> predicate_local_by_schema(schema.fields.size(), -1);
  std::vector<std::size_t> predicate_schema_indices;
  collectPredicateColumns(pushdown.predicate_expr, [&](std::size_t column_index) {
    if (column_index >= schema.fields.size()) {
      predicate_valid = false;
      return;
    }
    last_required_column = std::max(last_required_column, column_index);
    if (predicate_local_by_schema[column_index] < 0) {
      predicate_local_by_schema[column_index] = static_cast<int>(predicate_schema_indices.size());
      predicate_schema_indices.push_back(column_index);
    }
  });
  if (!predicate_valid) {
    return false;
  }
  for (const auto& agg : aggs) {
    if (agg.function != AggregateFunction::Count &&
        (agg.value_index >= schema.fields.size())) {
      return false;
    }
    if (agg.function != AggregateFunction::Count) {
      last_required_column = std::max(last_required_column, agg.value_index);
    }
  }

  std::ifstream input(path);
  if (!input.is_open()) {
    throw std::runtime_error("cannot open csv file: " + path);
  }

  std::vector<uint8_t> matched_rows;
  bool use_sparse_filter_bitmap = false;
  if (filters.size() == 1 && !pushdown.predicate_expr) {
    const auto& filter = filters.front();
    constexpr std::size_t kFilterSampleRows = 4096;
    std::size_t sampled_rows = 0;
    std::size_t matched_sample_rows = 0;
    bool skip_header = true;
    std::ifstream sample_input(path, std::ios::binary);
    if (!sample_input.is_open()) {
      throw std::runtime_error("cannot open csv file: " + path);
    }
    std::vector<uint8_t> sample_mask(filter.column_index + 1, static_cast<uint8_t>(0));
    sample_mask[filter.column_index] = 1;
    scanCsvCells(
        &sample_input, delimiter, filter.column_index, &sample_mask,
        []() {},
        [&](std::size_t column_index, std::string_view cell) {
          if (skip_header) return true;
          if (column_index == filter.column_index &&
              tryMatchesRawCellFilter(cell, filter.value, filter.op)) {
            ++matched_sample_rows;
          }
          return true;
        },
        [&](std::size_t) {
          if (skip_header) {
            skip_header = false;
            return true;
          }
          ++sampled_rows;
          return sampled_rows < kFilterSampleRows;
        },
        execution_pattern);
    if (sampled_rows > 0 && matched_sample_rows * 10 <= sampled_rows) {
      use_sparse_filter_bitmap = true;
    }
  }

  if (use_sparse_filter_bitmap) {
    const auto& filter = filters.front();
    bool skip_header = true;
    std::ifstream filter_input(path, std::ios::binary);
    if (!filter_input.is_open()) {
      throw std::runtime_error("cannot open csv file: " + path);
    }
    std::vector<uint8_t> filter_mask(filter.column_index + 1, static_cast<uint8_t>(0));
    filter_mask[filter.column_index] = 1;
    bool current_match = false;
    scanCsvCells(
        &filter_input, delimiter, filter.column_index, &filter_mask,
        [&]() { current_match = false; },
        [&](std::size_t column_index, std::string_view cell) {
          if (skip_header) return true;
          if (column_index == filter.column_index) {
            current_match = tryMatchesRawCellFilter(cell, filter.value, filter.op);
          }
          return true;
        },
        [&](std::size_t) {
          if (skip_header) {
            skip_header = false;
            return true;
          }
          matched_rows.push_back(current_match ? 1 : 0);
          return true;
        },
        execution_pattern);
    const bool has_match = std::any_of(matched_rows.begin(), matched_rows.end(),
                                       [](uint8_t value) { return value != 0; });
    if (!has_match) {
      Table result = makeCsvAggregateResultTable(schema, key_indices, aggs);
      *out = std::move(result);
      return true;
    }
  }

  if (pushdown.shape == SourcePushdownShape::SingleKeyCount && key_indices.size() == 1 &&
      aggs.size() == 1 && aggs.front().function == AggregateFunction::Count &&
      !pushdown.predicate_expr) {
    std::vector<uint8_t> capture_mask(last_required_column + 1, static_cast<uint8_t>(0));
    capture_mask[key_indices.front()] = 1;
    std::vector<std::vector<std::size_t>> filter_positions_by_column(last_required_column + 1);
    for (std::size_t i = 0; i < filters.size(); ++i) {
      capture_mask[filters[i].column_index] = 1;
      filter_positions_by_column[filters[i].column_index].push_back(i);
    }

    bool skip_header = true;
    bool filter_match = filters.empty();
    std::size_t logical_row_index = 0;
    bool current_row_selected = true;
    bool key_seen = false;
    std::string current_key;
    std::pmr::monotonic_buffer_resource key_resource;
    std::unordered_map<std::string_view, std::size_t, std::hash<std::string_view>> key_to_index;
    std::pmr::vector<std::pmr::string> ordered_keys(&key_resource);
    std::vector<std::size_t> counts;
    key_to_index.reserve(64);
    ordered_keys.reserve(64);
    counts.reserve(64);

    scanCsvCells(
        &input, delimiter, last_required_column, &capture_mask,
        [&]() {
          current_row_selected = matched_rows.empty() ||
                                 (logical_row_index < matched_rows.size() &&
                                  matched_rows[logical_row_index] != 0);
          filter_match = current_row_selected;
          key_seen = false;
          current_key.clear();
        },
        [&](std::size_t column_index, std::string_view cell) {
          if (!current_row_selected) {
            return true;
          }
          if (column_index == key_indices.front()) {
            current_key = cell;
            key_seen = true;
          }
          if (column_index < filter_positions_by_column.size() &&
              !filter_positions_by_column[column_index].empty()) {
            const ParsedCell parsed = parseParsedCell(cell);
            for (const auto filter_index : filter_positions_by_column[column_index]) {
              const auto& filter = filters[filter_index];
              filter_match =
                  filter_match && tryMatchesParsedCellFilter(parsed, filter.value, filter.op);
            }
          }
          return true;
        },
        [&](std::size_t total_columns) {
          if (skip_header) {
            skip_header = false;
            return true;
          }
          if (!matched_rows.empty() && !current_row_selected) {
            ++logical_row_index;
            return true;
          }
          if (total_columns < last_required_column + 1 || !filter_match || !key_seen) {
            ++logical_row_index;
            return true;
          }
          auto it = key_to_index.find(current_key);
          if (it == key_to_index.end()) {
            const std::size_t index = ordered_keys.size();
            ordered_keys.emplace_back(current_key.data(), current_key.size());
            counts.push_back(1);
            key_to_index.emplace(std::string_view(ordered_keys.back()), index);
          } else {
            counts[it->second] += 1;
          }
          ++logical_row_index;
          return true;
        },
        execution_pattern);

    Table result = makeCsvAggregateResultTable(schema, key_indices, aggs);
    result.columnar_cache->row_count = ordered_keys.size();
    for (auto& column : result.columnar_cache->columns) {
      column.values.reserve(ordered_keys.size());
    }
    for (std::size_t i = 0; i < ordered_keys.size(); ++i) {
      result.columnar_cache->columns[0].values.push_back(parseCell(ordered_keys[i]));
      result.columnar_cache->columns[1].values.push_back(
          Value(static_cast<int64_t>(counts[i])));
    }
    if (result.columnar_cache->row_count > 0) {
      result.columnar_cache->batch_row_counts.push_back(result.columnar_cache->row_count);
    }
    if (pushdown.limit != 0 && result.rowCount() > pushdown.limit) {
      result = limitTable(result, pushdown.limit, false);
    }
    *out = std::move(result);
    return true;
  }

  if (pushdown.shape == SourcePushdownShape::SingleKeyNumericAggregate && key_indices.size() == 1 &&
      aggs.size() == 1 && aggs.front().function != AggregateFunction::Count &&
      !pushdown.predicate_expr) {
    const auto& agg = aggs.front();
    std::vector<uint8_t> capture_mask(last_required_column + 1, static_cast<uint8_t>(0));
    capture_mask[key_indices.front()] = 1;
    capture_mask[agg.value_index] = 1;
    std::vector<std::vector<std::size_t>> filter_positions_by_column(last_required_column + 1);
    for (std::size_t i = 0; i < filters.size(); ++i) {
      capture_mask[filters[i].column_index] = 1;
      filter_positions_by_column[filters[i].column_index].push_back(i);
    }

    struct NumericAggregateState {
      double sum = 0.0;
      std::size_t count = 0;
      bool has_value = false;
      double value_double = 0.0;
      bool value_is_int = false;
      int64_t value_int = 0;
    };

    bool skip_header = true;
    bool filter_match = filters.empty();
    std::size_t logical_row_index = 0;
    bool current_row_selected = true;
    bool key_seen = false;
    bool numeric_present = false;
    double numeric_value = 0.0;
    bool numeric_is_int = false;
    int64_t int_value = 0;
    std::string_view current_key;
    std::pmr::monotonic_buffer_resource key_resource;
    std::unordered_map<std::string_view, std::size_t, std::hash<std::string_view>> key_to_index;
    std::pmr::vector<std::pmr::string> ordered_keys(&key_resource);
    std::vector<NumericAggregateState> states;
    key_to_index.reserve(64);
    ordered_keys.reserve(64);
    states.reserve(64);

    scanCsvCells(
        &input, delimiter, last_required_column, &capture_mask,
        [&]() {
          current_row_selected = matched_rows.empty() ||
                                 (logical_row_index < matched_rows.size() &&
                                  matched_rows[logical_row_index] != 0);
          filter_match = current_row_selected;
          key_seen = false;
          numeric_present = false;
          numeric_value = 0.0;
          numeric_is_int = false;
          int_value = 0;
          current_key = std::string_view();
        },
        [&](std::size_t column_index, std::string_view cell) {
          if (!current_row_selected) {
            return true;
          }
          if (column_index == key_indices.front()) {
            current_key = cell;
            key_seen = true;
          }
          const bool has_filter = column_index < filter_positions_by_column.size() &&
                                  !filter_positions_by_column[column_index].empty();
          const bool needed_for_numeric = column_index == agg.value_index;
          if (has_filter || needed_for_numeric) {
            const ParsedCell parsed = parseParsedCell(cell);
            if (has_filter) {
              for (const auto filter_index : filter_positions_by_column[column_index]) {
                const auto& filter = filters[filter_index];
                filter_match =
                    filter_match && tryMatchesParsedCellFilter(parsed, filter.value, filter.op);
              }
            }
            if (needed_for_numeric) {
              if (parsed.is_int64) {
                numeric_present = true;
                numeric_is_int = true;
                int_value = parsed.int64_value;
                numeric_value = static_cast<double>(parsed.int64_value);
              } else if (parsed.is_double) {
                numeric_present = true;
                numeric_is_int = false;
                numeric_value = parsed.double_value;
              }
            }
          }
          return true;
        },
        [&](std::size_t total_columns) {
          if (skip_header) {
            skip_header = false;
            return true;
          }
          if (!matched_rows.empty() && !current_row_selected) {
            ++logical_row_index;
            return true;
          }
          if (total_columns < last_required_column + 1 || !filter_match || !key_seen) {
            ++logical_row_index;
            return true;
          }
          auto it = key_to_index.find(current_key);
          if (it == key_to_index.end()) {
            const std::size_t index = ordered_keys.size();
            ordered_keys.emplace_back(current_key.data(), current_key.size());
            states.push_back(NumericAggregateState{});
            it = key_to_index.emplace(std::string_view(ordered_keys.back()), index).first;
          }
          auto& state = states[it->second];
          switch (agg.function) {
            case AggregateFunction::Sum:
              if (numeric_present) {
                state.sum += numeric_value;
              }
              break;
            case AggregateFunction::Avg:
              if (numeric_present) {
                state.sum += numeric_value;
                state.count += 1;
              }
              break;
            case AggregateFunction::Min:
              if (numeric_present && (!state.has_value || numeric_value < state.value_double)) {
                state.has_value = true;
                state.value_double = numeric_value;
                state.value_is_int = numeric_is_int;
                state.value_int = int_value;
              }
              break;
            case AggregateFunction::Max:
              if (numeric_present && (!state.has_value || numeric_value > state.value_double)) {
                state.has_value = true;
                state.value_double = numeric_value;
                state.value_is_int = numeric_is_int;
                state.value_int = int_value;
              }
              break;
            case AggregateFunction::Count:
              break;
          }
          ++logical_row_index;
          return true;
        },
        execution_pattern);

    Table result = makeCsvAggregateResultTable(schema, key_indices, aggs);
    result.columnar_cache->row_count = ordered_keys.size();
    for (auto& column : result.columnar_cache->columns) {
      column.values.reserve(ordered_keys.size());
    }
    for (std::size_t i = 0; i < ordered_keys.size(); ++i) {
      result.columnar_cache->columns[0].values.push_back(parseCell(ordered_keys[i]));
      const auto& state = states[i];
      switch (agg.function) {
        case AggregateFunction::Sum:
          result.columnar_cache->columns[1].values.push_back(Value(state.sum));
          break;
        case AggregateFunction::Avg:
          result.columnar_cache->columns[1].values.push_back(
              state.count == 0 ? Value()
                               : Value(state.sum / static_cast<double>(state.count)));
          break;
        case AggregateFunction::Min:
        case AggregateFunction::Max:
          if (!state.has_value) {
            result.columnar_cache->columns[1].values.push_back(Value());
          } else if (state.value_is_int) {
            result.columnar_cache->columns[1].values.push_back(Value(state.value_int));
          } else {
            result.columnar_cache->columns[1].values.push_back(Value(state.value_double));
          }
          break;
        case AggregateFunction::Count:
          break;
      }
    }
    if (result.columnar_cache->row_count > 0) {
      result.columnar_cache->batch_row_counts.push_back(result.columnar_cache->row_count);
    }
    if (pushdown.limit != 0 && result.rowCount() > pushdown.limit) {
      result = limitTable(result, pushdown.limit, false);
    }
    *out = std::move(result);
    return true;
  }

  struct AggregateState {
    std::vector<double> sums;
    std::vector<std::size_t> counts;
    std::vector<double> mins;
    std::vector<bool> has_min;
    std::vector<bool> min_is_int;
    std::vector<int64_t> min_ints;
    std::vector<double> maxs;
    std::vector<bool> has_max;
    std::vector<bool> max_is_int;
    std::vector<int64_t> max_ints;
  };

  auto init_state = [&]() {
    AggregateState state;
    state.sums.assign(aggs.size(), 0.0);
    state.counts.assign(aggs.size(), 0);
    state.mins.assign(aggs.size(), 0.0);
    state.has_min.assign(aggs.size(), false);
    state.min_is_int.assign(aggs.size(), false);
    state.min_ints.assign(aggs.size(), 0);
    state.maxs.assign(aggs.size(), 0.0);
    state.has_max.assign(aggs.size(), false);
    state.max_is_int.assign(aggs.size(), false);
    state.max_ints.assign(aggs.size(), 0);
    return state;
  };

  if (key_indices.size() == 1) {
    const std::size_t key_column_index = key_indices.front();
    std::vector<uint8_t> capture_mask(last_required_column + 1, static_cast<uint8_t>(0));
    capture_mask[key_column_index] = 1;
    std::vector<std::vector<std::size_t>> filter_positions_by_column(last_required_column + 1);
    for (std::size_t i = 0; i < filters.size(); ++i) {
      capture_mask[filters[i].column_index] = 1;
      filter_positions_by_column[filters[i].column_index].push_back(i);
    }
    for (const auto column_index : predicate_schema_indices) {
      capture_mask[column_index] = 1;
    }
    std::vector<std::vector<std::size_t>> agg_positions_by_column(last_required_column + 1);
    for (std::size_t i = 0; i < aggs.size(); ++i) {
      if (aggs[i].function == AggregateFunction::Count) {
        continue;
      }
      capture_mask[aggs[i].value_index] = 1;
      agg_positions_by_column[aggs[i].value_index].push_back(i);
    }

    bool skip_header = true;
    bool filter_match = filters.empty();
    std::size_t logical_row_index = 0;
    bool current_row_selected = true;
    bool key_seen = false;
    std::string current_key;
    std::vector<double> numeric_values(aggs.size(), 0.0);
    std::vector<bool> numeric_is_int(aggs.size(), false);
    std::vector<int64_t> int_values(aggs.size(), 0);
    std::vector<ParsedCell> predicate_values(predicate_schema_indices.size());
    std::vector<std::size_t> numeric_generations(aggs.size(), 0);
    std::vector<std::size_t> predicate_generations(predicate_schema_indices.size(), 0);
    std::size_t row_generation = 0;

    std::pmr::monotonic_buffer_resource key_resource;
    std::pmr::vector<std::pmr::string> ordered_keys(&key_resource);
    std::unordered_map<std::string_view, std::size_t, std::hash<std::string_view>> key_to_index;
    std::vector<AggregateState> ordered_states;
    ordered_keys.reserve(64);
    key_to_index.reserve(64);
    ordered_states.reserve(64);

    scanCsvCells(
        &input, delimiter, last_required_column, &capture_mask,
        [&]() {
          ++row_generation;
          current_row_selected = matched_rows.empty() ||
                                 (logical_row_index < matched_rows.size() &&
                                  matched_rows[logical_row_index] != 0);
          filter_match = current_row_selected;
          key_seen = false;
          current_key.clear();
        },
        [&](std::size_t column_index, std::string_view cell) {
          if (!current_row_selected) {
            return true;
          }
          if (column_index == key_column_index) {
            current_key.assign(cell.data(), cell.size());
            key_seen = true;
          }
          ParsedCell parsed;
          bool parsed_ready = false;
          const bool has_filter = column_index < filter_positions_by_column.size() &&
                                  !filter_positions_by_column[column_index].empty();
          const bool needed_for_predicate =
              column_index < predicate_local_by_schema.size() &&
              predicate_local_by_schema[column_index] >= 0;
          const bool needed_for_numeric =
              column_index < agg_positions_by_column.size() &&
              !agg_positions_by_column[column_index].empty();
          if (has_filter || needed_for_predicate || needed_for_numeric) {
            parsed = parseParsedCell(cell);
            parsed_ready = true;
          }
          if (has_filter) {
            for (const auto filter_index : filter_positions_by_column[column_index]) {
              const auto& filter = filters[filter_index];
              filter_match =
                  filter_match && tryMatchesParsedCellFilter(parsed, filter.value, filter.op);
            }
          }
          if (needed_for_numeric) {
            for (const auto agg_index : agg_positions_by_column[column_index]) {
              if (parsed_ready && parsed.is_int64) {
                numeric_values[agg_index] = static_cast<double>(parsed.int64_value);
                numeric_generations[agg_index] = row_generation;
                numeric_is_int[agg_index] = true;
                int_values[agg_index] = parsed.int64_value;
              } else if (parsed_ready && parsed.is_double) {
                numeric_values[agg_index] = parsed.double_value;
                numeric_generations[agg_index] = row_generation;
                numeric_is_int[agg_index] = false;
              }
            }
          }
          if (needed_for_predicate) {
            const auto predicate_local =
                static_cast<std::size_t>(predicate_local_by_schema[column_index]);
            predicate_values[predicate_local] = persistParsedCell(std::move(parsed));
            predicate_generations[predicate_local] = row_generation;
          }
          return true;
        },
        [&](std::size_t total_columns) {
          if (skip_header) {
            skip_header = false;
            return true;
          }
          if (!matched_rows.empty() && !current_row_selected) {
            ++logical_row_index;
            return true;
          }
          if (total_columns < last_required_column + 1 || !filter_match || !key_seen ||
              !evaluatePredicateExprParsed(predicate_values, predicate_generations, row_generation,
                                           predicate_local_by_schema, pushdown.predicate_expr)) {
            ++logical_row_index;
            return true;
          }
          const std::string_view current_key_view(current_key);
          auto it = key_to_index.find(current_key_view);
          if (it == key_to_index.end()) {
            const std::size_t index = ordered_keys.size();
            ordered_keys.emplace_back(current_key.data(), current_key.size());
            ordered_states.push_back(init_state());
            it = key_to_index.emplace(std::string_view(ordered_keys.back()), index).first;
          }
          auto& state = ordered_states[it->second];
          for (std::size_t i = 0; i < aggs.size(); ++i) {
            const auto& agg = aggs[i];
            const bool has_numeric = numeric_generations[i] == row_generation;
            switch (agg.function) {
              case AggregateFunction::Count:
                state.counts[i] += 1;
                break;
              case AggregateFunction::Sum:
              case AggregateFunction::Avg:
                if (has_numeric) {
                  state.sums[i] += numeric_values[i];
                  state.counts[i] += 1;
                }
                break;
              case AggregateFunction::Min:
                if (has_numeric &&
                    (!state.has_min[i] || numeric_values[i] < state.mins[i])) {
                  state.mins[i] = numeric_values[i];
                  state.has_min[i] = true;
                  state.min_is_int[i] = numeric_is_int[i];
                  state.min_ints[i] = int_values[i];
                }
                break;
              case AggregateFunction::Max:
                if (has_numeric &&
                    (!state.has_max[i] || numeric_values[i] > state.maxs[i])) {
                  state.maxs[i] = numeric_values[i];
                  state.has_max[i] = true;
                  state.max_is_int[i] = numeric_is_int[i];
                  state.max_ints[i] = int_values[i];
                }
                break;
            }
          }
          ++logical_row_index;
          return true;
        },
        execution_pattern);

    Table result;
    result.schema.fields.push_back(schema.fields[key_column_index]);
    for (const auto& agg : aggs) {
      result.schema.fields.push_back(agg.output_name);
    }
    for (std::size_t i = 0; i < result.schema.fields.size(); ++i) {
      result.schema.index[result.schema.fields[i]] = i;
    }
    auto cache = std::make_shared<ColumnarTable>();
    cache->schema = result.schema;
    cache->columns.resize(result.schema.fields.size());
    cache->arrow_formats.resize(result.schema.fields.size());
    cache->row_count = ordered_keys.size();
    for (auto& column : cache->columns) {
      column.values.reserve(ordered_keys.size());
    }
    for (std::size_t index = 0; index < ordered_keys.size(); ++index) {
      cache->columns[0].values.push_back(parseCell(std::string_view(ordered_keys[index])));
      const auto& state = ordered_states[index];
      for (std::size_t i = 0; i < aggs.size(); ++i) {
        Value value;
        switch (aggs[i].function) {
          case AggregateFunction::Count:
            value = Value(static_cast<int64_t>(state.counts[i]));
            break;
          case AggregateFunction::Sum:
            value = Value(state.sums[i]);
            break;
          case AggregateFunction::Avg:
            value = Value(state.counts[i] == 0 ? 0.0
                                               : state.sums[i] / static_cast<double>(state.counts[i]));
            break;
          case AggregateFunction::Min:
            value = !state.has_min[i]
                        ? Value()
                        : (state.min_is_int[i] ? Value(state.min_ints[i]) : Value(state.mins[i]));
            break;
          case AggregateFunction::Max:
            value = !state.has_max[i]
                        ? Value()
                        : (state.max_is_int[i] ? Value(state.max_ints[i]) : Value(state.maxs[i]));
            break;
        }
        cache->columns[1 + i].values.push_back(std::move(value));
      }
    }
    result.columnar_cache = std::move(cache);
    *out = std::move(result);
    return true;
  }

  struct AggregateEntry {
    std::vector<Value> key_values;
    AggregateState state;
  };

  std::pmr::monotonic_buffer_resource key_resource;
  std::unordered_map<std::string_view, std::size_t, std::hash<std::string_view>>
      single_key_entry_index;
  std::unordered_map<std::string, std::size_t> multi_key_entry_index;
  std::pmr::vector<std::pmr::string> single_key_storage(&key_resource);
  std::vector<AggregateEntry> ordered_entries;
  single_key_entry_index.reserve(64);
  multi_key_entry_index.reserve(64);
  single_key_storage.reserve(64);
  ordered_entries.reserve(64);
  std::vector<uint8_t> capture_mask(last_required_column + 1, static_cast<uint8_t>(0));
  std::vector<std::size_t> key_position_by_column(last_required_column + 1,
                                                  static_cast<std::size_t>(-1));
  for (std::size_t i = 0; i < key_indices.size(); ++i) {
    capture_mask[key_indices[i]] = 1;
    key_position_by_column[key_indices[i]] = i;
  }
  std::vector<std::vector<std::size_t>> filter_positions_by_column(last_required_column + 1);
  for (std::size_t i = 0; i < filters.size(); ++i) {
    capture_mask[filters[i].column_index] = 1;
    filter_positions_by_column[filters[i].column_index].push_back(i);
  }
  for (const auto column_index : predicate_schema_indices) {
    capture_mask[column_index] = 1;
  }
  std::vector<std::vector<std::size_t>> agg_positions_by_column(last_required_column + 1);
  for (std::size_t i = 0; i < aggs.size(); ++i) {
    if (aggs[i].function == AggregateFunction::Count) {
      continue;
    }
    capture_mask[aggs[i].value_index] = 1;
    agg_positions_by_column[aggs[i].value_index].push_back(i);
  }

  bool skip_header = true;
  bool filter_match = filters.empty();
  std::size_t logical_row_index = 0;
  bool current_row_selected = true;
  std::vector<ParsedCell> key_cells(key_indices.size());
  std::vector<double> numeric_values(aggs.size(), 0.0);
  std::vector<bool> numeric_is_int(aggs.size(), false);
  std::vector<int64_t> int_values(aggs.size(), 0);
  std::vector<ParsedCell> predicate_values(predicate_schema_indices.size());
  std::vector<std::size_t> key_generations(key_indices.size(), 0);
  std::vector<std::size_t> numeric_generations(aggs.size(), 0);
  std::vector<std::size_t> predicate_generations(predicate_schema_indices.size(), 0);
  std::string encoded_key_scratch;
  std::size_t row_generation = 0;

  scanCsvCells(
      &input, delimiter, last_required_column, &capture_mask,
      [&]() {
        ++row_generation;
        current_row_selected = matched_rows.empty() ||
                               (logical_row_index < matched_rows.size() &&
                                matched_rows[logical_row_index] != 0);
        filter_match = current_row_selected;
        for (std::size_t i = 0; i < key_cells.size(); ++i) {
          if (key_generations[i] == row_generation) {
            resetParsedCell(&key_cells[i]);
          }
        }
      },
      [&](std::size_t column_index, std::string_view cell) {
        if (!current_row_selected) {
          return true;
        }
        ParsedCell parsed;
        bool parsed_ready = false;
        const bool has_filter = column_index < filter_positions_by_column.size() &&
                                !filter_positions_by_column[column_index].empty();
        const bool needed_for_predicate =
            column_index < predicate_local_by_schema.size() &&
            predicate_local_by_schema[column_index] >= 0;
        const bool needed_for_numeric =
            column_index < agg_positions_by_column.size() &&
            !agg_positions_by_column[column_index].empty();
        const bool needed_for_key =
            column_index < key_position_by_column.size() &&
            key_position_by_column[column_index] != static_cast<std::size_t>(-1);
        if (has_filter || needed_for_predicate || needed_for_numeric || needed_for_key) {
          parsed = parseParsedCell(cell);
          parsed_ready = true;
        }
        if (has_filter) {
          for (const auto filter_index : filter_positions_by_column[column_index]) {
            const auto& filter = filters[filter_index];
            filter_match =
                filter_match && tryMatchesParsedCellFilter(parsed, filter.value, filter.op);
          }
        }
        if (needed_for_key) {
          const auto key_position = key_position_by_column[column_index];
          key_cells[key_position] = persistParsedCell(parsed);
          key_generations[key_position] = row_generation;
        }
        if (needed_for_numeric) {
          for (const auto agg_index : agg_positions_by_column[column_index]) {
            if (parsed_ready && parsed.is_int64) {
              numeric_values[agg_index] = static_cast<double>(parsed.int64_value);
              numeric_generations[agg_index] = row_generation;
              numeric_is_int[agg_index] = true;
              int_values[agg_index] = parsed.int64_value;
            } else if (parsed_ready && parsed.is_double) {
              numeric_values[agg_index] = parsed.double_value;
              numeric_generations[agg_index] = row_generation;
            }
          }
        }
        if (needed_for_predicate) {
          const auto predicate_local =
              static_cast<std::size_t>(predicate_local_by_schema[column_index]);
          predicate_values[predicate_local] = persistParsedCell(parsed);
          predicate_generations[predicate_local] = row_generation;
        }
        return true;
      },
      [&](std::size_t total_columns) {
        if (skip_header) {
          skip_header = false;
          return true;
        }
        if (!matched_rows.empty()) {
          if (!current_row_selected) {
            ++logical_row_index;
            return true;
          }
        }
        const bool keys_ready =
            std::all_of(key_generations.begin(), key_generations.end(),
                        [&](std::size_t generation) { return generation == row_generation; });
        if (total_columns < last_required_column + 1 || !filter_match || !keys_ready ||
            !evaluatePredicateExprParsed(predicate_values, predicate_generations, row_generation,
                                         predicate_local_by_schema, pushdown.predicate_expr)) {
          ++logical_row_index;
          return true;
        }

        std::size_t entry_pos = 0;
        if (key_cells.size() == 1) {
          const auto key_text = parsedCellText(key_cells.front());
          auto it = single_key_entry_index.find(key_text);
          if (it == single_key_entry_index.end()) {
            AggregateEntry entry;
            entry.key_values.reserve(1);
            entry.key_values.push_back(parsedCellToValue(key_cells.front()));
            entry.state = init_state();
            ordered_entries.push_back(std::move(entry));
            entry_pos = ordered_entries.size() - 1;
            single_key_storage.emplace_back(key_text.data(), key_text.size());
            single_key_entry_index.emplace(std::string_view(single_key_storage.back()), entry_pos);
          } else {
            entry_pos = it->second;
          }
        } else {
          encodeParsedGroupKeyRow(key_cells, &encoded_key_scratch);
          auto it = multi_key_entry_index.find(encoded_key_scratch);
          if (it == multi_key_entry_index.end()) {
            AggregateEntry entry;
            entry.key_values.reserve(key_cells.size());
            for (const auto& key_cell : key_cells) {
              entry.key_values.push_back(parsedCellToValue(key_cell));
            }
            entry.state = init_state();
            ordered_entries.push_back(std::move(entry));
            entry_pos = ordered_entries.size() - 1;
            multi_key_entry_index.emplace(encoded_key_scratch, entry_pos);
          } else {
            entry_pos = it->second;
          }
        }
        auto& state = ordered_entries[entry_pos].state;
        for (std::size_t i = 0; i < aggs.size(); ++i) {
          const auto& agg = aggs[i];
          const bool has_numeric = numeric_generations[i] == row_generation;
          switch (agg.function) {
            case AggregateFunction::Count:
              state.counts[i] += 1;
              break;
            case AggregateFunction::Sum:
            case AggregateFunction::Avg:
              if (has_numeric) {
                state.sums[i] += numeric_values[i];
                state.counts[i] += 1;
              }
              break;
            case AggregateFunction::Min:
              if (has_numeric &&
                  (!state.has_min[i] || numeric_values[i] < state.mins[i])) {
                state.mins[i] = numeric_values[i];
                state.has_min[i] = true;
                state.min_is_int[i] = numeric_is_int[i];
                state.min_ints[i] = int_values[i];
              }
              break;
            case AggregateFunction::Max:
              if (has_numeric &&
                  (!state.has_max[i] || numeric_values[i] > state.maxs[i])) {
                state.maxs[i] = numeric_values[i];
                state.has_max[i] = true;
                state.max_is_int[i] = numeric_is_int[i];
                state.max_ints[i] = int_values[i];
              }
              break;
          }
        }
        ++logical_row_index;
        return true;
      },
      execution_pattern);

  Table result;
  for (const auto key_index : key_indices) {
    result.schema.fields.push_back(schema.fields[key_index]);
  }
  for (const auto& agg : aggs) {
    result.schema.fields.push_back(agg.output_name);
  }
  for (std::size_t i = 0; i < result.schema.fields.size(); ++i) {
    result.schema.index[result.schema.fields[i]] = i;
  }
  auto cache = std::make_shared<ColumnarTable>();
  cache->schema = result.schema;
  cache->columns.resize(result.schema.fields.size());
  cache->arrow_formats.resize(result.schema.fields.size());
  cache->row_count = ordered_entries.size();
  for (auto& column : cache->columns) {
    column.values.reserve(ordered_entries.size());
  }
  for (const auto& entry : ordered_entries) {
    const auto& state = entry.state;
    for (std::size_t key_pos = 0; key_pos < entry.key_values.size(); ++key_pos) {
      cache->columns[key_pos].values.push_back(entry.key_values[key_pos]);
    }
    for (std::size_t i = 0; i < aggs.size(); ++i) {
      Value value;
      switch (aggs[i].function) {
        case AggregateFunction::Count:
          value = Value(static_cast<int64_t>(state.counts[i]));
          break;
        case AggregateFunction::Sum:
          value = Value(state.sums[i]);
          break;
        case AggregateFunction::Avg:
          value = Value(state.counts[i] == 0 ? 0.0
                                             : state.sums[i] /
                                                   static_cast<double>(state.counts[i]));
          break;
        case AggregateFunction::Min:
          value = !state.has_min[i]
                      ? Value()
                      : (state.min_is_int[i] ? Value(state.min_ints[i]) : Value(state.mins[i]));
          break;
        case AggregateFunction::Max:
          value = !state.has_max[i]
                      ? Value()
                      : (state.max_is_int[i] ? Value(state.max_ints[i]) : Value(state.maxs[i]));
          break;
      }
      cache->columns[key_indices.size() + i].values.push_back(std::move(value));
    }
  }
  result.columnar_cache = std::move(cache);
  *out = std::move(result);
  return true;
}

void save_csv(const Table& table, const std::string& path) {
  std::ofstream output(path);
  if (!output.is_open()) {
    throw std::runtime_error("cannot write csv file: " + path);
  }

  for (size_t i = 0; i < table.schema.fields.size(); ++i) {
    if (i > 0) output << ',';
    output << table.schema.fields[i];
  }
  output << '\n';

  for (const auto& row : table.rows) {
    for (size_t i = 0; i < row.size(); ++i) {
      if (i > 0) output << ',';
      output << row[i].toString();
    }
    output << '\n';
  }
}

}  // namespace dataflow
