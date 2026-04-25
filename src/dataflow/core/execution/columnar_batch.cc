#include "src/dataflow/core/execution/columnar_batch.h"

#include <algorithm>
#include <bit>
#include <cctype>
#include <cmath>
#include <cstring>
#include <span>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <stdexcept>

#include "src/dataflow/core/execution/runtime/simd_dispatch.h"
#include "src/dataflow/core/execution/arrow_format.h"

namespace dataflow {

namespace {

constexpr char kGroupDelim = '\x1f';

enum class CompareOp { Eq, Ne, Lt, Gt, Le, Ge };

int64_t parseEpochMillis(const std::string& raw);
std::tm toUtcTm(std::time_t seconds);

std::string formatTimestampMillis(int64_t millis) {
  const auto seconds = static_cast<std::time_t>(millis / 1000);
  const int64_t remainder = millis % 1000;
  auto tm = toUtcTm(seconds);
  std::ostringstream out;
  out << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S");
  if (remainder != 0) {
    out << "." << std::setfill('0') << std::setw(3) << std::llabs(remainder);
  }
  return out.str();
}

std::tm toUtcTm(std::time_t seconds) {
  std::tm tm = {};
#if defined(_WIN32)
  if (gmtime_s(&tm, &seconds) != 0) {
    throw std::runtime_error("failed to convert epoch to UTC");
  }
#else
  if (gmtime_r(&seconds, &tm) == nullptr) {
    throw std::runtime_error("failed to convert epoch to UTC");
  }
#endif
  return tm;
}

[[maybe_unused]] std::string valueAsString(const Value& value) {
  if (value.type() == DataType::Nil) {
    return std::string();
  }
  return value.toString();
}

bool arrowBitmapHasValue(const uint8_t* bitmap, std::size_t index) {
  if (bitmap == nullptr) {
    return true;
  }
  return ((bitmap[index >> 3] >> (index & 7)) & 0x01u) != 0;
}

template <typename T>
std::shared_ptr<void> allocateArrowArray(std::size_t size) {
  if (size == 0) {
    return nullptr;
  }
  return std::shared_ptr<void>(new T[size], [](void* ptr) {
    delete[] static_cast<T*>(ptr);
  });
}

void arrowBitmapSetValue(uint8_t* bitmap, std::size_t index, bool valid) {
  if (bitmap == nullptr || !valid) {
    return;
  }
  bitmap[index >> 3] |= static_cast<uint8_t>(1u << (index & 7));
}

std::size_t countBitmapValidBits(std::span<const uint8_t> bitmap, std::size_t bit_count) {
  if (bitmap.empty() || bit_count == 0) {
    return 0;
  }
  const std::size_t full_bytes = bit_count / 8;
  const std::size_t remaining_bits = bit_count % 8;
  std::size_t set_bits = 0;
  for (std::size_t index = 0; index < full_bytes; ++index) {
    set_bits += static_cast<std::size_t>(std::popcount(static_cast<unsigned int>(bitmap[index])));
  }
  if (remaining_bits != 0) {
    const auto mask = static_cast<uint8_t>((1u << remaining_bits) - 1u);
    set_bits += static_cast<std::size_t>(
        std::popcount(static_cast<unsigned int>(bitmap[full_bytes] & mask)));
  }
  return set_bits;
}

int64_t countBitmapNulls(const std::shared_ptr<void>& bitmap, std::size_t bit_count) {
  if (bitmap == nullptr || bit_count == 0) {
    return 0;
  }
  const auto byte_count = (bit_count + 7) / 8;
  const auto* bits = static_cast<const uint8_t*>(bitmap.get());
  const auto valid_bits = countBitmapValidBits(std::span<const uint8_t>(bits, byte_count), bit_count);
  return static_cast<int64_t>(bit_count - valid_bits);
}

std::shared_ptr<void> copySelectedArrowBitmap(const uint8_t* input_bitmap,
                                              const RowSelection& selection) {
  if (selection.selected_count == 0 || input_bitmap == nullptr) {
    return nullptr;
  }
  const auto output_bytes = (selection.selected_count + 7) / 8;
  auto output_bitmap = allocateArrowArray<uint8_t>(output_bytes);
  auto* output = static_cast<uint8_t*>(output_bitmap.get());
  std::memset(output, 0, output_bytes);
  std::size_t out_index = 0;
  for (const auto row_index : selection.indices) {
    arrowBitmapSetValue(output, out_index, arrowBitmapHasValue(input_bitmap, row_index));
    ++out_index;
  }
  return output_bitmap;
}

template <typename T>
std::shared_ptr<void> copySelectedArrowValues(const T* input_values,
                                              const RowSelection& selection) {
  if (selection.selected_count == 0) {
    return nullptr;
  }
  auto output_values = allocateArrowArray<T>(selection.selected_count);
  auto* output = static_cast<T*>(output_values.get());
  std::size_t out_index = 0;
  for (const auto row_index : selection.indices) {
    output[out_index++] = input_values[row_index];
  }
  return output_values;
}

template <typename Offset>
ValueColumnBuffer copySelectedArrowStringColumn(const ArrowColumnBacking& backing,
                                                const RowSelection& selection) {
  ValueColumnBuffer output_column;
  auto output_backing = std::make_shared<ArrowColumnBacking>();
  output_backing->format = backing.format;
  output_backing->length = selection.selected_count;
  output_backing->null_bitmap = copySelectedArrowBitmap(
      static_cast<const uint8_t*>(backing.null_bitmap.get()), selection);

  auto offsets = allocateArrowArray<Offset>(selection.selected_count + 1);
  auto* output_offsets = static_cast<Offset*>(offsets.get());
  const auto* input_offsets = static_cast<const Offset*>(backing.value_buffer.get());
  const auto* input_data = static_cast<const char*>(backing.extra_buffer.get());
  output_offsets[0] = 0;

  std::size_t total_bytes = 0;
  std::size_t out_index = 0;
  for (const auto row_index : selection.indices) {
    const auto begin = static_cast<std::size_t>(input_offsets[row_index]);
    const auto end = static_cast<std::size_t>(input_offsets[row_index + 1]);
    total_bytes += (end - begin);
    output_offsets[++out_index] = static_cast<Offset>(total_bytes);
  }

  auto values = allocateArrowArray<char>(total_bytes);
  auto* output_data = static_cast<char*>(values.get());
  std::size_t data_offset = 0;
  for (const auto row_index : selection.indices) {
    const auto begin = static_cast<std::size_t>(input_offsets[row_index]);
    const auto end = static_cast<std::size_t>(input_offsets[row_index + 1]);
    const auto size = end - begin;
    if (size > 0) {
      std::memcpy(output_data + data_offset, input_data + begin, size);
      data_offset += size;
    }
  }

  output_backing->value_buffer = std::move(offsets);
  output_backing->extra_buffer = std::move(values);
  output_backing->null_count =
      countBitmapNulls(output_backing->null_bitmap, selection.selected_count);
  output_column.arrow_backing = std::move(output_backing);
  return output_column;
}

ValueColumnBuffer copySelectedArrowFixedVectorColumn(const ArrowColumnBacking& backing,
                                                     const RowSelection& selection) {
  ValueColumnBuffer output_column;
  auto output_backing = std::make_shared<ArrowColumnBacking>();
  output_backing->format = backing.format;
  output_backing->child_format = backing.child_format;
  output_backing->fixed_list_size = backing.fixed_list_size;
  output_backing->length = selection.selected_count;
  output_backing->child_length =
      selection.selected_count * static_cast<std::size_t>(std::max(backing.fixed_list_size, 0));
  output_backing->null_bitmap = copySelectedArrowBitmap(
      static_cast<const uint8_t*>(backing.null_bitmap.get()), selection);

  const auto child_length = output_backing->child_length;
  auto child_values = allocateArrowArray<float>(child_length);
  auto* output = static_cast<float*>(child_values.get());
  const auto* input = static_cast<const float*>(backing.child_value_buffer.get());
  const auto width = static_cast<std::size_t>(std::max(backing.fixed_list_size, 0));
  std::size_t out_index = 0;
  for (const auto row_index : selection.indices) {
    if (width > 0) {
      std::memcpy(output + out_index * width, input + row_index * width, sizeof(float) * width);
    }
    ++out_index;
  }
  output_backing->child_value_buffer = std::move(child_values);
  output_backing->null_count =
      countBitmapNulls(output_backing->null_bitmap, selection.selected_count);
  output_column.arrow_backing = std::move(output_backing);
  return output_column;
}

ValueColumnBuffer copySelectedArrowColumn(const ValueColumnBuffer& input_column,
                                          const RowSelection& selection) {
  if (input_column.arrow_backing == nullptr || !input_column.values.empty()) {
    return {};
  }
  const auto& backing = *input_column.arrow_backing;
  switch (backing.format.empty() ? '?' : backing.format[0]) {
    case 'n': {
      ValueColumnBuffer output_column;
      auto output_backing = std::make_shared<ArrowColumnBacking>();
      output_backing->format = backing.format;
      output_backing->length = selection.selected_count;
      output_backing->null_count = static_cast<int64_t>(selection.selected_count);
      output_column.arrow_backing = std::move(output_backing);
      return output_column;
    }
    case 'b': {
      auto output_column = ValueColumnBuffer{};
      auto output_backing = std::make_shared<ArrowColumnBacking>();
      output_backing->format = backing.format;
      output_backing->length = selection.selected_count;
      output_backing->null_bitmap = copySelectedArrowBitmap(
          static_cast<const uint8_t*>(backing.null_bitmap.get()), selection);
      const auto output_bytes = (selection.selected_count + 7) / 8;
      auto output_values = allocateArrowArray<uint8_t>(output_bytes);
      auto* output = static_cast<uint8_t*>(output_values.get());
      std::memset(output, 0, output_bytes);
      const auto* input = static_cast<const uint8_t*>(backing.value_buffer.get());
      std::size_t out_index = 0;
      for (const auto row_index : selection.indices) {
        arrowBitmapSetValue(
            output, out_index,
            ((input[row_index >> 3] >> (row_index & 7)) & 0x01u) != 0);
        ++out_index;
      }
      output_backing->value_buffer = std::move(output_values);
      output_backing->null_count =
          countBitmapNulls(output_backing->null_bitmap, selection.selected_count);
      output_column.arrow_backing = std::move(output_backing);
      return output_column;
    }
    case 'i':
      return ValueColumnBuffer{
          {},
          [&]() {
            auto output_backing = std::make_shared<ArrowColumnBacking>();
            output_backing->format = backing.format;
            output_backing->length = selection.selected_count;
            output_backing->null_bitmap = copySelectedArrowBitmap(
                static_cast<const uint8_t*>(backing.null_bitmap.get()), selection);
            output_backing->value_buffer = copySelectedArrowValues(
                static_cast<const int32_t*>(backing.value_buffer.get()), selection);
            output_backing->null_count =
                countBitmapNulls(output_backing->null_bitmap, selection.selected_count);
            return std::shared_ptr<const ArrowColumnBacking>(std::move(output_backing));
          }()};
    case 'I':
      return ValueColumnBuffer{
          {},
          [&]() {
            auto output_backing = std::make_shared<ArrowColumnBacking>();
            output_backing->format = backing.format;
            output_backing->length = selection.selected_count;
            output_backing->null_bitmap = copySelectedArrowBitmap(
                static_cast<const uint8_t*>(backing.null_bitmap.get()), selection);
            output_backing->value_buffer = copySelectedArrowValues(
                static_cast<const uint32_t*>(backing.value_buffer.get()), selection);
            output_backing->null_count =
                countBitmapNulls(output_backing->null_bitmap, selection.selected_count);
            return std::shared_ptr<const ArrowColumnBacking>(std::move(output_backing));
          }()};
    case 'l':
      return ValueColumnBuffer{
          {},
          [&]() {
            auto output_backing = std::make_shared<ArrowColumnBacking>();
            output_backing->format = backing.format;
            output_backing->length = selection.selected_count;
            output_backing->null_bitmap = copySelectedArrowBitmap(
                static_cast<const uint8_t*>(backing.null_bitmap.get()), selection);
            output_backing->value_buffer = copySelectedArrowValues(
                static_cast<const int64_t*>(backing.value_buffer.get()), selection);
            output_backing->null_count =
                countBitmapNulls(output_backing->null_bitmap, selection.selected_count);
            return std::shared_ptr<const ArrowColumnBacking>(std::move(output_backing));
          }()};
    case 'L':
      return ValueColumnBuffer{
          {},
          [&]() {
            auto output_backing = std::make_shared<ArrowColumnBacking>();
            output_backing->format = backing.format;
            output_backing->length = selection.selected_count;
            output_backing->null_bitmap = copySelectedArrowBitmap(
                static_cast<const uint8_t*>(backing.null_bitmap.get()), selection);
            output_backing->value_buffer = copySelectedArrowValues(
                static_cast<const uint64_t*>(backing.value_buffer.get()), selection);
            output_backing->null_count =
                countBitmapNulls(output_backing->null_bitmap, selection.selected_count);
            return std::shared_ptr<const ArrowColumnBacking>(std::move(output_backing));
          }()};
    case 'f':
      return ValueColumnBuffer{
          {},
          [&]() {
            auto output_backing = std::make_shared<ArrowColumnBacking>();
            output_backing->format = backing.format;
            output_backing->length = selection.selected_count;
            output_backing->null_bitmap = copySelectedArrowBitmap(
                static_cast<const uint8_t*>(backing.null_bitmap.get()), selection);
            output_backing->value_buffer = copySelectedArrowValues(
                static_cast<const float*>(backing.value_buffer.get()), selection);
            output_backing->null_count =
                countBitmapNulls(output_backing->null_bitmap, selection.selected_count);
            return std::shared_ptr<const ArrowColumnBacking>(std::move(output_backing));
          }()};
    case 'g':
      return ValueColumnBuffer{
          {},
          [&]() {
            auto output_backing = std::make_shared<ArrowColumnBacking>();
            output_backing->format = backing.format;
            output_backing->length = selection.selected_count;
            output_backing->null_bitmap = copySelectedArrowBitmap(
                static_cast<const uint8_t*>(backing.null_bitmap.get()), selection);
            output_backing->value_buffer = copySelectedArrowValues(
                static_cast<const double*>(backing.value_buffer.get()), selection);
            output_backing->null_count =
                countBitmapNulls(output_backing->null_bitmap, selection.selected_count);
            return std::shared_ptr<const ArrowColumnBacking>(std::move(output_backing));
          }()};
    case 'u':
      return copySelectedArrowStringColumn<int32_t>(backing, selection);
    case 'U':
      return copySelectedArrowStringColumn<int64_t>(backing, selection);
    case '+':
      if (isArrowFixedSizeListFormat(backing.format)) {
        return copySelectedArrowFixedVectorColumn(backing, selection);
      }
      break;
    default:
      break;
  }
  return {};
}

template <typename T>
std::shared_ptr<void> copyArrowPrefixValues(const T* input_values, std::size_t row_count) {
  if (row_count == 0) {
    return nullptr;
  }
  auto output_values = allocateArrowArray<T>(row_count);
  std::memcpy(output_values.get(), input_values, sizeof(T) * row_count);
  return output_values;
}

std::size_t countArrowNullsPrefix(const ArrowColumnBacking& backing, std::size_t row_count) {
  return static_cast<std::size_t>(countBitmapNulls(backing.null_bitmap, row_count));
}

ValueColumnBuffer shareArrowPrefixColumn(const ValueColumnBuffer& input_column,
                                         std::size_t row_count) {
  if (input_column.arrow_backing == nullptr || !input_column.values.empty()) {
    return {};
  }
  const auto& backing = *input_column.arrow_backing;
  const auto shared_length = std::min<std::size_t>(row_count, backing.length);
  if (shared_length == 0) {
    return ValueColumnBuffer{{}, std::make_shared<ArrowColumnBacking>(ArrowColumnBacking{
                                         backing.format,
                                         backing.child_format,
                                         backing.null_bitmap,
                                         backing.value_buffer,
                                         backing.extra_buffer,
                                         backing.child_value_buffer,
                                         backing.fixed_list_size,
                                         0,
                                         0,
                                         0})};
  }
  if (shared_length == backing.length) {
    return input_column;
  }
  auto output_backing = std::make_shared<ArrowColumnBacking>(backing);
  output_backing->length = shared_length;
  output_backing->null_count = static_cast<int64_t>(countArrowNullsPrefix(backing, shared_length));
  if (isArrowFixedSizeListFormat(backing.format)) {
    output_backing->child_length =
        shared_length * static_cast<std::size_t>(std::max(backing.fixed_list_size, 0));
  }
  return ValueColumnBuffer{{}, std::move(output_backing)};
}

[[maybe_unused]] int64_t valueAsInt64(const Value& value) {
  if (value.type() == DataType::Nil) {
    throw std::runtime_error("computed function argument cannot be null");
  }
  if (!value.isNumber()) {
    throw std::runtime_error("computed function argument must be numeric");
  }
  return value.asInt64();
}

int64_t valueAsEpochMillis(const Value& value) {
  if (value.type() == DataType::Nil) {
    throw std::runtime_error("window assign source column cannot be null");
  }
  if (value.isNumber()) {
    return value.asInt64();
  }
  return parseEpochMillis(value.toString());
}

CompareOp parseCompareOp(const std::string& op) {
  if (op == "=" || op == "==") return CompareOp::Eq;
  if (op == "!=") return CompareOp::Ne;
  if (op == "<") return CompareOp::Lt;
  if (op == ">") return CompareOp::Gt;
  if (op == "<=") return CompareOp::Le;
  if (op == ">=") return CompareOp::Ge;
  throw std::runtime_error("unsupported filter op: " + op);
}

bool matchesCompareOp(int compare_result, CompareOp op) {
  switch (op) {
    case CompareOp::Eq:
      return compare_result == 0;
    case CompareOp::Ne:
      return compare_result != 0;
    case CompareOp::Lt:
      return compare_result < 0;
    case CompareOp::Gt:
      return compare_result > 0;
    case CompareOp::Le:
      return compare_result <= 0;
    case CompareOp::Ge:
      return compare_result >= 0;
  }
  return false;
}

NumericCompareOp toNumericCompareOp(const std::string& op) {
  switch (parseCompareOp(op)) {
    case CompareOp::Eq:
      return NumericCompareOp::Eq;
    case CompareOp::Ne:
      return NumericCompareOp::Ne;
    case CompareOp::Lt:
      return NumericCompareOp::Lt;
    case CompareOp::Gt:
      return NumericCompareOp::Gt;
    case CompareOp::Le:
      return NumericCompareOp::Le;
    case CompareOp::Ge:
      return NumericCompareOp::Ge;
  }
  throw std::runtime_error("unsupported filter op: " + op);
}

bool tryMatchesValueCompare(const Value& lhs, const Value& rhs, CompareOp op) {
  try {
    return matchesCompareOp(lhs == rhs ? 0 : (lhs < rhs ? -1 : 1), op);
  } catch (...) {
    return false;
  }
}

template <typename T>
int compareScalar(const T& lhs, const T& rhs) {
  return lhs < rhs ? -1 : (lhs > rhs ? 1 : 0);
}

bool valueIsNull(const Value& value) {
  return value.type() == DataType::Nil;
}

bool matchesTypedValueCompare(const Value& lhs, const Value& rhs, CompareOp op) {
  if (lhs.type() == DataType::Nil || rhs.type() == DataType::Nil) {
    if (op == CompareOp::Eq) {
      return valueIsNull(lhs) && valueIsNull(rhs);
    }
    if (op == CompareOp::Ne) {
      return valueIsNull(lhs) != valueIsNull(rhs);
    }
    return false;
  }
  if (rhs.isNumber() && lhs.isNumber()) {
    return matchesCompareOp(compareScalar(lhs.asDouble(), rhs.asDouble()), op);
  }
  if (rhs.type() == DataType::String && lhs.type() == DataType::String) {
    return matchesCompareOp(compareScalar(lhs.asString(), rhs.asString()), op);
  }
  return tryMatchesValueCompare(lhs, rhs, op);
}

int64_t parseEpochMillis(const std::string& raw) {
  if (raw.empty()) {
    return 0;
  }
  const bool numeric =
      std::all_of(raw.begin(), raw.end(), [](char ch) { return ch >= '0' && ch <= '9'; });
  if (numeric) {
    return static_cast<int64_t>(std::stoll(raw));
  }

  std::tm tm = {};
  std::istringstream in(raw);
  in >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
  if (in.fail()) {
    in.clear();
    in.str(raw);
    in >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
  }
  if (in.fail()) {
    throw std::runtime_error("unsupported timestamp format: " + raw);
  }
  return static_cast<int64_t>(timegm(&tm)) * 1000;
}

int64_t valueColumnEpochMillisAt(const ValueColumnBuffer& buffer, std::size_t row_index) {
  if (valueColumnIsNullAt(buffer, row_index)) {
    throw std::runtime_error("window assign source column cannot be null");
  }
  if (!buffer.values.empty()) {
    const auto& value = buffer.values[row_index];
    if (value.isNumber()) {
      return value.asInt64();
    }
    return parseEpochMillis(value.toString());
  }
  if (buffer.arrow_backing == nullptr) {
    throw std::runtime_error("window assign source column cannot be null");
  }
  switch (buffer.arrow_backing->format.empty() ? '?' : buffer.arrow_backing->format[0]) {
    case 'b':
    case 'i':
    case 'I':
    case 'l':
    case 'L':
    case 'f':
    case 'g':
      return valueColumnInt64At(buffer, row_index);
    case 'u':
    case 'U':
      return parseEpochMillis(valueColumnStringAt(buffer, row_index));
    default:
      return parseEpochMillis(valueColumnValueAt(buffer, row_index).toString());
  }
}

std::string trimString(std::string input) {
  auto is_space = [](char ch) { return std::isspace(static_cast<unsigned char>(ch)) != 0; };
  auto start = input.begin();
  while (start != input.end() && is_space(*start)) ++start;
  auto end = input.end();
  while (end != start && is_space(*(end - 1))) --end;
  return std::string(start, end);
}

std::string ltrimString(std::string input) {
  auto is_space = [](char ch) { return std::isspace(static_cast<unsigned char>(ch)) != 0; };
  auto start = input.begin();
  while (start != input.end() && is_space(*start)) ++start;
  return std::string(start, input.end());
}

std::string rtrimString(std::string input) {
  auto is_space = [](char ch) { return std::isspace(static_cast<unsigned char>(ch)) != 0; };
  auto end = input.end();
  while (end != input.begin() && is_space(*(end - 1))) --end;
  return std::string(input.begin(), end);
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

std::string replaceString(std::string source, const std::string& from, const std::string& to) {
  if (from.empty()) {
    return source;
  }
  std::size_t pos = 0;
  while ((pos = source.find(from, pos)) != std::string::npos) {
    source.replace(pos, from.size(), to);
    pos += to.size();
  }
  return source;
}

template <typename Fn>
std::vector<Value> unaryStringKernel(const StringColumnBuffer& input, Fn&& fn) {
  std::vector<Value> out(input.values.size());
  for (std::size_t i = 0; i < input.values.size(); ++i) {
    if (input.is_null[i] != 0) {
      continue;
    }
    out[i] = fn(input.values[i]);
  }
  return out;
}

}  // namespace

std::size_t valueColumnRowCount(const ValueColumnBuffer& buffer) {
  if (!buffer.values.empty()) {
    return buffer.values.size();
  }
  if (buffer.arrow_backing != nullptr) {
    return buffer.arrow_backing->length;
  }
  return 0;
}

bool valueColumnIsNullAt(const ValueColumnBuffer& buffer, std::size_t row_index) {
  if (!buffer.values.empty()) {
    return buffer.values[row_index].isNull();
  }
  if (buffer.arrow_backing == nullptr) {
    return true;
  }
  if (buffer.arrow_backing->format == kArrowFormatNull) {
    return true;
  }
  return !arrowBitmapHasValue(static_cast<const uint8_t*>(buffer.arrow_backing->null_bitmap.get()),
                              row_index);
}

std::string_view valueColumnStringViewAt(const ValueColumnBuffer& buffer, std::size_t row_index) {
  if (!buffer.values.empty()) {
    if (buffer.values[row_index].isNull()) {
      return std::string_view();
    }
    return buffer.values[row_index].asString();
  }
  if (buffer.arrow_backing == nullptr || valueColumnIsNullAt(buffer, row_index)) {
    return std::string_view();
  }
  const auto& backing = *buffer.arrow_backing;
  if (backing.format == kArrowFormatUtf8) {
    const auto* offsets = static_cast<const int32_t*>(backing.value_buffer.get());
    const auto* data = static_cast<const char*>(backing.extra_buffer.get());
    const auto begin = offsets[row_index];
    const auto end = offsets[row_index + 1];
    return std::string_view(data + begin, static_cast<std::size_t>(end - begin));
  }
  if (backing.format == kArrowFormatLargeUtf8) {
    const auto* offsets = static_cast<const int64_t*>(backing.value_buffer.get());
    const auto* data = static_cast<const char*>(backing.extra_buffer.get());
    const auto begin = offsets[row_index];
    const auto end = offsets[row_index + 1];
    return std::string_view(data + begin, static_cast<std::size_t>(end - begin));
  }
  return std::string_view();
}

std::string valueColumnStringAt(const ValueColumnBuffer& buffer, std::size_t row_index) {
  if (!buffer.values.empty()) {
    return buffer.values[row_index].toString();
  }
  if (buffer.arrow_backing == nullptr) {
    return std::string();
  }
  const auto& backing = *buffer.arrow_backing;
  if (valueColumnIsNullAt(buffer, row_index)) {
    return std::string("null");
  }
  if (backing.format == kArrowFormatUtf8) {
    const auto* offsets = static_cast<const int32_t*>(backing.value_buffer.get());
    const auto* data = static_cast<const char*>(backing.extra_buffer.get());
    return std::string(data + offsets[row_index], data + offsets[row_index + 1]);
  }
  if (backing.format == kArrowFormatLargeUtf8) {
    const auto* offsets = static_cast<const int64_t*>(backing.value_buffer.get());
    const auto* data = static_cast<const char*>(backing.extra_buffer.get());
    return std::string(data + offsets[row_index], data + offsets[row_index + 1]);
  }
  if (backing.format == kArrowFormatBool) {
    const auto* bits = static_cast<const uint8_t*>(backing.value_buffer.get());
    return ((bits[row_index >> 3] >> (row_index & 7)) & 0x01u) != 0 ? "true" : "false";
  }
  if (backing.format == kArrowFormatInt32) {
    return std::to_string(static_cast<const int32_t*>(backing.value_buffer.get())[row_index]);
  }
  if (backing.format == kArrowFormatUInt32) {
    return std::to_string(static_cast<const uint32_t*>(backing.value_buffer.get())[row_index]);
  }
  if (backing.format == kArrowFormatInt64) {
    return std::to_string(static_cast<const int64_t*>(backing.value_buffer.get())[row_index]);
  }
  if (backing.format == kArrowFormatUInt64) {
    return std::to_string(static_cast<const uint64_t*>(backing.value_buffer.get())[row_index]);
  }
  if (backing.format == kArrowFormatFloat32) {
    return Value(static_cast<double>(static_cast<const float*>(backing.value_buffer.get())[row_index]))
        .toString();
  }
  if (backing.format == kArrowFormatFloat64) {
    return Value(static_cast<const double*>(backing.value_buffer.get())[row_index]).toString();
  }
  return valueColumnValueAt(buffer, row_index).toString();
}

int64_t valueColumnInt64At(const ValueColumnBuffer& buffer, std::size_t row_index) {
  if (!buffer.values.empty()) {
    return buffer.values[row_index].asInt64();
  }
  if (buffer.arrow_backing == nullptr) {
    throw std::runtime_error("value column is not numeric");
  }
  const auto& backing = *buffer.arrow_backing;
  switch (backing.format.empty() ? '?' : backing.format[0]) {
    case 'b': {
      const auto* bits = static_cast<const uint8_t*>(backing.value_buffer.get());
      return static_cast<int64_t>(((bits[row_index >> 3] >> (row_index & 7)) & 0x01u) != 0);
    }
    case 'i':
      return static_cast<const int32_t*>(backing.value_buffer.get())[row_index];
    case 'I':
      return static_cast<int64_t>(static_cast<const uint32_t*>(backing.value_buffer.get())[row_index]);
    case 'l':
      return static_cast<const int64_t*>(backing.value_buffer.get())[row_index];
    case 'L':
      return static_cast<int64_t>(static_cast<const uint64_t*>(backing.value_buffer.get())[row_index]);
    case 'f':
      return static_cast<int64_t>(static_cast<const float*>(backing.value_buffer.get())[row_index]);
    case 'g':
      return static_cast<int64_t>(static_cast<const double*>(backing.value_buffer.get())[row_index]);
    default:
      return valueColumnValueAt(buffer, row_index).asInt64();
  }
}

double valueColumnDoubleAt(const ValueColumnBuffer& buffer, std::size_t row_index) {
  if (!buffer.values.empty()) {
    return buffer.values[row_index].asDouble();
  }
  if (buffer.arrow_backing == nullptr) {
    throw std::runtime_error("value column is not numeric");
  }
  const auto& backing = *buffer.arrow_backing;
  switch (backing.format.empty() ? '?' : backing.format[0]) {
    case 'b': {
      const auto* bits = static_cast<const uint8_t*>(backing.value_buffer.get());
      return static_cast<double>(((bits[row_index >> 3] >> (row_index & 7)) & 0x01u) != 0);
    }
    case 'i':
      return static_cast<const int32_t*>(backing.value_buffer.get())[row_index];
    case 'I':
      return static_cast<const uint32_t*>(backing.value_buffer.get())[row_index];
    case 'l':
      return static_cast<double>(static_cast<const int64_t*>(backing.value_buffer.get())[row_index]);
    case 'L':
      return static_cast<double>(static_cast<const uint64_t*>(backing.value_buffer.get())[row_index]);
    case 'f':
      return static_cast<const float*>(backing.value_buffer.get())[row_index];
    case 'g':
      return static_cast<const double*>(backing.value_buffer.get())[row_index];
    default:
      return valueColumnValueAt(buffer, row_index).asDouble();
  }
}

Value valueColumnValueAt(const ValueColumnBuffer& buffer, std::size_t row_index) {
  if (!buffer.values.empty()) {
    return buffer.values[row_index];
  }
  if (buffer.arrow_backing == nullptr) {
    return Value();
  }

  const auto& backing = *buffer.arrow_backing;
  if (valueColumnIsNullAt(buffer, row_index)) {
    return Value();
  }

  if (backing.format == kArrowFormatBool) {
    const auto* bits = static_cast<const uint8_t*>(backing.value_buffer.get());
    return Value(((bits[row_index >> 3] >> (row_index & 7)) & 0x01u) != 0);
  }
  if (backing.format == kArrowFormatNull) {
    return Value();
  }
  if (backing.format == kArrowFormatInt32) {
    return Value(static_cast<int64_t>(
        static_cast<const int32_t*>(backing.value_buffer.get())[row_index]));
  }
  if (backing.format == kArrowFormatUInt32) {
    return Value(static_cast<int64_t>(
        static_cast<const uint32_t*>(backing.value_buffer.get())[row_index]));
  }
  if (backing.format == kArrowFormatInt64) {
    return Value(static_cast<const int64_t*>(backing.value_buffer.get())[row_index]);
  }
  if (backing.format == kArrowFormatUInt64) {
    return Value(static_cast<int64_t>(
        static_cast<const uint64_t*>(backing.value_buffer.get())[row_index]));
  }
  if (backing.format == kArrowFormatFloat32) {
    return Value(static_cast<double>(
        static_cast<const float*>(backing.value_buffer.get())[row_index]));
  }
  if (backing.format == kArrowFormatFloat64) {
    return Value(static_cast<const double*>(backing.value_buffer.get())[row_index]);
  }
  if (backing.format == kArrowFormatUtf8) {
    const auto* offsets = static_cast<const int32_t*>(backing.value_buffer.get());
    const auto* data = static_cast<const char*>(backing.extra_buffer.get());
    return Value(std::string(data + offsets[row_index], data + offsets[row_index + 1]));
  }
  if (backing.format == kArrowFormatLargeUtf8) {
    const auto* offsets = static_cast<const int64_t*>(backing.value_buffer.get());
    const auto* data = static_cast<const char*>(backing.extra_buffer.get());
    return Value(std::string(data + offsets[row_index], data + offsets[row_index + 1]));
  }
  if (isArrowFixedSizeListFormat(backing.format)) {
    const auto* values = static_cast<const float*>(backing.child_value_buffer.get());
    std::vector<float> vec(static_cast<std::size_t>(backing.fixed_list_size));
    std::memcpy(vec.data(), values + row_index * static_cast<std::size_t>(backing.fixed_list_size),
                sizeof(float) * static_cast<std::size_t>(backing.fixed_list_size));
    return Value(std::move(vec));
  }

  throw std::runtime_error("unsupported Arrow backing format: " + backing.format);
}

std::shared_ptr<const ColumnarTable> snapshotColumnarCache(const Table* table) {
  if (table == nullptr) {
    throw std::invalid_argument("snapshotColumnarCache table is null");
  }
  std::lock_guard<std::mutex> lock(table->columnar_cache_mu);
  return table->columnar_cache;
}

std::shared_ptr<ColumnarTable> makeColumnarCache(const Table& table) {
  auto cache = std::make_shared<ColumnarTable>();
  cache->schema = table.schema;
  cache->columns.reserve(table.schema.fields.size());
  cache->arrow_formats.resize(table.schema.fields.size());
  cache->row_count = table.rows.size();
  cache->batch_row_counts.push_back(table.rows.size());
  for (std::size_t column_index = 0; column_index < table.schema.fields.size(); ++column_index) {
    ValueColumnBuffer column;
    column.values.reserve(table.rows.size());
    for (const auto& row : table.rows) {
      column.values.push_back(column_index < row.size() ? row[column_index] : Value());
    }
    cache->columns.push_back(std::move(column));
  }
  return cache;
}

std::shared_ptr<const ColumnarTable> ensureColumnarCache(const Table* table) {
  if (table == nullptr) {
    throw std::invalid_argument("ensureColumnarCache table is null");
  }
  std::lock_guard<std::mutex> lock(table->columnar_cache_mu);
  if (!table->columnar_cache) {
    table->columnar_cache = makeColumnarCache(*table);
  }
  return table->columnar_cache;
}

std::size_t valueColumnViewRowCount(const ValueColumnView& view) {
  if (view.buffer == nullptr) {
    return 0;
  }
  return valueColumnRowCount(*view.buffer);
}

ValueColumnView viewValueColumn(const Table& table, std::size_t column_index) {
  const auto cache = ensureColumnarCache(&table);
  if (column_index >= cache->columns.size()) {
    throw std::runtime_error("column index out of range");
  }
  return ValueColumnView{cache, &cache->columns[column_index]};
}

std::vector<ValueColumnView> viewValueColumns(const Table& table,
                                              const std::vector<std::size_t>& indices) {
  const auto cache = ensureColumnarCache(&table);
  std::vector<ValueColumnView> out;
  out.reserve(indices.size());
  for (const auto index : indices) {
    if (index >= cache->columns.size()) {
      throw std::runtime_error("column index out of range");
    }
    out.push_back(ValueColumnView{cache, &cache->columns[index]});
  }
  return out;
}

StringColumnBuffer makeNullStringColumn(std::size_t row_count) {
  StringColumnBuffer out;
  out.values.resize(row_count);
  out.is_null.assign(row_count, static_cast<uint8_t>(1));
  return out;
}

StringColumnBuffer makeConstantStringColumn(std::size_t row_count, std::string value) {
  StringColumnBuffer out;
  out.values.assign(row_count, std::move(value));
  out.is_null.assign(row_count, 0);
  return out;
}

StringColumnBuffer materializeStringColumn(const Table& table, std::size_t column_index) {
  const auto value_column = viewValueColumn(table, column_index);
  StringColumnBuffer out;
  const auto row_count = valueColumnRowCount(*value_column.buffer);
  out.values.resize(row_count);
  out.is_null.assign(row_count, 0);
  for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
    if (valueColumnIsNullAt(*value_column.buffer, row_index)) {
      out.is_null[row_index] = 1;
      continue;
    }
    if (value_column.buffer->arrow_backing != nullptr &&
        isArrowUtf8Format(value_column.buffer->arrow_backing->format)) {
      const auto view = valueColumnStringViewAt(*value_column.buffer, row_index);
      out.values[row_index].assign(view.data(), view.size());
    } else {
      out.values[row_index] = valueColumnStringAt(*value_column.buffer, row_index);
    }
  }
  return out;
}

Int64ColumnBuffer makeNullInt64Column(std::size_t row_count) {
  Int64ColumnBuffer out;
  out.values.resize(row_count);
  out.is_null.assign(row_count, static_cast<uint8_t>(1));
  return out;
}

Int64ColumnBuffer makeConstantInt64Column(std::size_t row_count, int64_t value) {
  Int64ColumnBuffer out;
  out.values.assign(row_count, value);
  out.is_null.assign(row_count, 0);
  return out;
}

Int64ColumnBuffer materializeInt64Column(const Table& table, std::size_t column_index) {
  const auto value_column = viewValueColumn(table, column_index);
  Int64ColumnBuffer out;
  const auto row_count = valueColumnRowCount(*value_column.buffer);
  out.values.resize(row_count);
  out.is_null.assign(row_count, 0);
  for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
    if (valueColumnIsNullAt(*value_column.buffer, row_index)) {
      out.is_null[row_index] = 1;
      continue;
    }
    out.values[row_index] = valueColumnInt64At(*value_column.buffer, row_index);
  }
  return out;
}

ValueColumnBuffer materializeValueColumn(const Table& table, std::size_t column_index) {
  return *viewValueColumn(table, column_index).buffer;
}

DoubleColumnBuffer makeNullDoubleColumn(std::size_t row_count) {
  DoubleColumnBuffer out;
  out.values.resize(row_count);
  out.is_null.assign(row_count, static_cast<uint8_t>(1));
  return out;
}

DoubleColumnBuffer materializeDoubleColumn(const Table& table, std::size_t column_index) {
  const auto value_column = viewValueColumn(table, column_index);
  DoubleColumnBuffer out;
  const auto row_count = valueColumnRowCount(*value_column.buffer);
  out.values.resize(row_count);
  out.is_null.assign(row_count, 0);
  for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
    if (valueColumnIsNullAt(*value_column.buffer, row_index)) {
      out.is_null[row_index] = 1;
      continue;
    }
    try {
      out.values[row_index] = valueColumnDoubleAt(*value_column.buffer, row_index);
    } catch (...) {
      out.is_null[row_index] = 1;
      throw;
    }
  }
  return out;
}

std::vector<ValueColumnBuffer> materializeValueColumns(const Table& table,
                                                       const std::vector<std::size_t>& indices) {
  const auto columns = viewValueColumns(table, indices);
  std::vector<ValueColumnBuffer> out;
  out.reserve(columns.size());
  for (const auto& column : columns) {
    out.push_back(*column.buffer);
  }
  return out;
}

std::vector<std::string> materializeSerializedKeys(const Table& table,
                                                   const std::vector<std::size_t>& indices) {
  const auto columns = viewValueColumns(table, indices);
  std::vector<std::string> out;
  const auto row_count = columns.empty() ? table.rowCount() : valueColumnRowCount(*columns.front().buffer);
  out.reserve(row_count);
  for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
    std::string key;
    for (std::size_t i = 0; i < columns.size(); ++i) {
      if (i > 0) {
        key.push_back(kGroupDelim);
      }
      key += valueColumnStringAt(*columns[i].buffer, row_index);
    }
    out.push_back(std::move(key));
  }
  return out;
}

std::unordered_map<std::string, std::vector<std::size_t>> buildHashBuckets(
    const std::vector<std::string>& keys) {
  std::unordered_map<std::string, std::vector<std::size_t>> buckets;
  buckets.reserve(keys.size());
  for (std::size_t row_index = 0; row_index < keys.size(); ++row_index) {
    buckets[keys[row_index]].push_back(row_index);
  }
  return buckets;
}

RowSelection vectorizedFilterSelection(const ValueColumnBuffer& input, const Value& rhs,
                                       bool (*pred)(const Value& lhs, const Value& rhs)) {
  RowSelection out;
  const auto row_count = valueColumnRowCount(input);
  out.input_row_count = row_count;
  out.selected.assign(row_count, 0);
  out.indices.reserve(row_count);
  for (std::size_t i = 0; i < row_count; ++i) {
    if (!pred(valueColumnValueAt(input, i), rhs)) {
      continue;
    }
    out.selected[i] = 1;
    out.indices.push_back(i);
    ++out.selected_count;
  }
  return out;
}

RowSelection vectorizedFilterSelection(const ValueColumnView& input, const Value& rhs,
                                       bool (*pred)(const Value& lhs, const Value& rhs)) {
  return vectorizedFilterSelection(*input.buffer, rhs, pred);
}

RowSelection vectorizedFilterSelection(const ValueColumnBuffer& input, const Value& rhs,
                                       const std::string& op) {
  return vectorizedFilterSelection(input, rhs, op, 0);
}

RowSelection vectorizedFilterSelection(const ValueColumnView& input, const Value& rhs,
                                       const std::string& op) {
  return vectorizedFilterSelection(*input.buffer, rhs, op, 0);
}

RowSelection vectorizedFilterSelection(const ValueColumnBuffer& input, const Value& rhs,
                                       const std::string& op, std::size_t max_selected) {
  const auto parsed_op = parseCompareOp(op);
  RowSelection out;
  const auto row_count = valueColumnRowCount(input);
  out.input_row_count = row_count;
  out.selected.assign(row_count, 0);
  out.indices.reserve(row_count);
  const bool bounded = max_selected != 0;

  if (!input.values.empty()) {
    if (rhs.isNumber()) {
      const auto rhs_value = rhs.asDouble();
      for (std::size_t i = 0; i < row_count; ++i) {
        const auto& lhs = input.values[i];
        if (lhs.type() == DataType::Nil || !lhs.isNumber()) {
          continue;
        }
        if (!matchesCompareOp(compareScalar(lhs.asDouble(), rhs_value), parsed_op)) {
          continue;
        }
        out.selected[i] = 1;
        out.indices.push_back(i);
        ++out.selected_count;
        if (bounded && out.selected_count >= max_selected) {
          break;
        }
      }
      return out;
    }
    if (rhs.type() == DataType::String) {
      const auto& rhs_value = rhs.asString();
      for (std::size_t i = 0; i < row_count; ++i) {
        const auto& lhs = input.values[i];
        if (lhs.type() != DataType::String) {
          continue;
        }
        if (!matchesCompareOp(compareScalar(lhs.asString(), rhs_value), parsed_op)) {
          continue;
        }
        out.selected[i] = 1;
        out.indices.push_back(i);
        ++out.selected_count;
        if (bounded && out.selected_count >= max_selected) {
          break;
        }
      }
      return out;
    }
  }

  if (rhs.isNull() || input.arrow_backing == nullptr) {
    for (std::size_t i = 0; i < row_count; ++i) {
      const Value& lhs = input.values.empty() ? valueColumnValueAt(input, i) : input.values[i];
      if (!matchesTypedValueCompare(lhs, rhs, parsed_op)) {
        continue;
      }
      out.selected[i] = 1;
      out.indices.push_back(i);
      ++out.selected_count;
      if (bounded && out.selected_count >= max_selected) {
        break;
      }
    }
    return out;
  }

  if (rhs.type() == DataType::String &&
      isArrowUtf8Format(input.arrow_backing->format)) {
    const auto rhs_view = std::string_view(rhs.asString());
    for (std::size_t i = 0; i < row_count; ++i) {
      if (valueColumnIsNullAt(input, i)) {
        continue;
      }
      if (!matchesCompareOp(compareScalar(valueColumnStringViewAt(input, i), rhs_view), parsed_op)) {
        continue;
      }
      out.selected[i] = 1;
      out.indices.push_back(i);
      ++out.selected_count;
      if (bounded && out.selected_count >= max_selected) {
        break;
      }
    }
    return out;
  }

  if (rhs.isNumber()) {
    switch (input.arrow_backing->format.empty() ? '?' : input.arrow_backing->format[0]) {
      case 'b':
      case 'i':
      case 'I':
      case 'l':
      case 'L':
      case 'f':
      case 'g': {
        DoubleColumnBuffer numeric;
        numeric.values.resize(row_count);
        numeric.is_null.assign(row_count, 0);
        for (std::size_t i = 0; i < row_count; ++i) {
          if (valueColumnIsNullAt(input, i)) {
            numeric.is_null[i] = 1;
            continue;
          }
          numeric.values[i] = valueColumnDoubleAt(input, i);
        }
        auto selected = simdDispatch().select_double(
            numeric.values.data(), numeric.is_null.data(), row_count, rhs.asDouble(),
            toNumericCompareOp(op), max_selected);
        out.selected = std::move(selected.selected);
        out.indices = std::move(selected.indices);
        out.selected_count = selected.selected_count;
        return out;
      }
      default:
        break;
    }
  }

  for (std::size_t i = 0; i < row_count; ++i) {
    const auto lhs = valueColumnValueAt(input, i);
    if (!matchesTypedValueCompare(lhs, rhs, parsed_op)) {
      continue;
    }
    out.selected[i] = 1;
    out.indices.push_back(i);
    ++out.selected_count;
    if (bounded && out.selected_count >= max_selected) {
      break;
    }
  }
  return out;
}

RowSelection vectorizedFilterSelection(const ValueColumnView& input, const Value& rhs,
                                       const std::string& op, std::size_t max_selected) {
  return vectorizedFilterSelection(*input.buffer, rhs, op, max_selected);
}

RowSelection vectorizedColumnCompareSelection(const ValueColumnView& lhs,
                                             const ValueColumnView& rhs,
                                             const std::string& op) {
  const auto parsed_op = parseCompareOp(op);
  const auto row_count = valueColumnViewRowCount(lhs);
  if (valueColumnViewRowCount(rhs) != row_count) {
    throw std::runtime_error("column comparison row count mismatch");
  }
  RowSelection out;
  out.input_row_count = row_count;
  out.selected.assign(row_count, 0);
  out.indices.reserve(row_count);
  for (std::size_t i = 0; i < row_count; ++i) {
    if (valueColumnIsNullAt(*lhs.buffer, i) || valueColumnIsNullAt(*rhs.buffer, i)) {
      continue;
    }
    const auto left_value = valueColumnValueAt(*lhs.buffer, i);
    const auto right_value = valueColumnValueAt(*rhs.buffer, i);
    if (!matchesTypedValueCompare(left_value, right_value, parsed_op)) {
      continue;
    }
    out.selected[i] = 1;
    out.indices.push_back(i);
    ++out.selected_count;
  }
  return out;
}

Table projectTable(const Table& table, const std::vector<std::size_t>& indices,
                   const std::vector<std::string>& aliases, bool materialize_rows) {
  Table out;
  out.schema.fields.reserve(indices.size());
  for (std::size_t i = 0; i < indices.size(); ++i) {
    const auto index = indices[i];
    if (index >= table.schema.fields.size()) {
      throw std::runtime_error("select index out of range");
    }
    out.schema.fields.push_back(i < aliases.size() && !aliases[i].empty()
                                    ? aliases[i]
                                    : table.schema.fields[index]);
    out.schema.index[out.schema.fields.back()] = i;
  }
  if (const auto cache_in = snapshotColumnarCache(&table)) {
    auto cache = std::make_shared<ColumnarTable>();
    cache->schema = out.schema;
    cache->columns.reserve(indices.size());
    cache->arrow_formats.reserve(indices.size());
    cache->batch_row_counts = cache_in->batch_row_counts;
    cache->row_count = table.rowCount();
    for (const auto index : indices) {
      cache->columns.push_back(cache_in->columns[index]);
      if (index < cache_in->arrow_formats.size()) {
        cache->arrow_formats.push_back(cache_in->arrow_formats[index]);
      } else {
        cache->arrow_formats.emplace_back();
      }
    }
    out.columnar_cache = std::move(cache);
    if (materialize_rows) {
      const auto row_count =
          cache_in->row_count > 0 ? cache_in->row_count : table.rowCount();
      out.rows.reserve(row_count);
      for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
        Row projected;
        projected.reserve(indices.size());
        for (const auto index : indices) {
          projected.push_back(valueColumnValueAt(cache_in->columns[index], row_index));
        }
        out.rows.push_back(std::move(projected));
      }
    }
  } else {
    out.rows.reserve(table.rows.size());
    for (const auto& row : table.rows) {
      Row projected;
      projected.reserve(indices.size());
      for (const auto index : indices) {
        if (index >= row.size()) {
          throw std::runtime_error("select index out of range");
        }
        projected.push_back(row[index]);
      }
      out.rows.push_back(std::move(projected));
    }
  }
  return out;
}

Table filterTable(const Table& table, const RowSelection& selection, bool materialize_rows) {
  const auto input_row_count =
      selection.input_row_count != 0 ? selection.input_row_count : selection.selected.size();
  if (input_row_count != table.rowCount()) {
    throw std::runtime_error("selection size mismatch");
  }
  Table out(table.schema, {});
  if (const auto cache_in = snapshotColumnarCache(&table)) {
    auto cache = std::make_shared<ColumnarTable>();
    cache->schema = out.schema;
    cache->columns.reserve(cache_in->columns.size());
    cache->arrow_formats = cache_in->arrow_formats;
    cache->row_count = selection.selected_count;
    if (selection.selected_count > 0) {
      cache->batch_row_counts.push_back(selection.selected_count);
    }
    for (const auto& input_column : cache_in->columns) {
      ValueColumnBuffer output_column = copySelectedArrowColumn(input_column, selection);
      if (output_column.arrow_backing == nullptr) {
        output_column.values.reserve(selection.selected_count);
        for (const auto row_index : selection.indices) {
          output_column.values.push_back(valueColumnValueAt(input_column, row_index));
        }
      }
      cache->columns.push_back(std::move(output_column));
    }
    out.columnar_cache = std::move(cache);
    if (materialize_rows) {
      out.rows.reserve(selection.selected_count);
      for (const auto row_index : selection.indices) {
        Row row;
        row.reserve(cache_in->columns.size());
        for (const auto& input_column : cache_in->columns) {
          row.push_back(valueColumnValueAt(input_column, row_index));
        }
        out.rows.push_back(std::move(row));
      }
    }
  } else {
    out.rows.reserve(selection.selected_count);
    for (const auto row_index : selection.indices) {
      out.rows.push_back(table.rows[row_index]);
    }
  }
  return out;
}

Table limitTable(const Table& table, std::size_t limit, bool materialize_rows) {
  Table out(table.schema, {});
  const auto row_count = std::min(table.rowCount(), limit);
  if (const auto cache_in = snapshotColumnarCache(&table)) {
    auto cache = std::make_shared<ColumnarTable>();
    cache->schema = out.schema;
    cache->columns.reserve(cache_in->columns.size());
    cache->arrow_formats = cache_in->arrow_formats;
    cache->row_count = row_count;
    if (row_count > 0) {
      cache->batch_row_counts.push_back(row_count);
    }
    for (const auto& input_column : cache_in->columns) {
      ValueColumnBuffer output_column = shareArrowPrefixColumn(input_column, row_count);
      if (output_column.arrow_backing == nullptr) {
        output_column.values.reserve(row_count);
        for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
          output_column.values.push_back(valueColumnValueAt(input_column, row_index));
        }
      }
      cache->columns.push_back(std::move(output_column));
    }
    out.columnar_cache = std::move(cache);
    if (materialize_rows) {
      out.rows.reserve(row_count);
      for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
        Row row;
        row.reserve(cache_in->columns.size());
        for (const auto& input_column : cache_in->columns) {
          row.push_back(valueColumnValueAt(input_column, row_index));
        }
        out.rows.push_back(std::move(row));
      }
    }
  } else {
    out.rows.reserve(row_count);
    for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
      out.rows.push_back(table.rows[row_index]);
    }
  }
  return out;
}

Table sortTable(const Table& table, const std::vector<std::size_t>& indices,
                const std::vector<bool>& ascending) {
  if (!ascending.empty() && ascending.size() != indices.size()) {
    throw std::runtime_error("ORDER BY direction count mismatch");
  }
  const auto row_count = table.rowCount();
  if (indices.empty() || row_count < 2) {
    return table;
  }

  const auto columns = viewValueColumns(table, indices);
  std::vector<bool> directions = ascending;
  if (directions.empty()) {
    directions.assign(indices.size(), true);
  }

  std::vector<std::size_t> row_order(row_count);
  for (std::size_t i = 0; i < row_order.size(); ++i) {
    row_order[i] = i;
  }

  std::stable_sort(
      row_order.begin(), row_order.end(),
      [&](std::size_t lhs_index, std::size_t rhs_index) {
        for (std::size_t column_index = 0; column_index < columns.size(); ++column_index) {
          const auto lhs = valueColumnValueAt(*columns[column_index].buffer, lhs_index);
          const auto rhs = valueColumnValueAt(*columns[column_index].buffer, rhs_index);
          if (lhs.isNull() && rhs.isNull()) {
            continue;
          }
          if (lhs.isNull()) {
            return false;
          }
          if (rhs.isNull()) {
            return true;
          }
          if (lhs == rhs) {
            continue;
          }
          return directions[column_index] ? (lhs < rhs) : (lhs > rhs);
        }
        return false;
      });

  Table out(table.schema, {});
  if (!table.rows.empty()) {
    out.rows.reserve(row_count);
    for (auto row_index : row_order) {
      out.rows.push_back(table.rows[row_index]);
    }
  }
  if (const auto cache_in = snapshotColumnarCache(&table)) {
    auto cache = std::make_shared<ColumnarTable>();
    cache->schema = out.schema;
    cache->arrow_formats = cache_in->arrow_formats;
    cache->batch_row_counts = {row_count};
    cache->row_count = row_count;
    cache->columns.reserve(cache_in->columns.size());
    for (std::size_t column_index = 0; column_index < cache_in->columns.size(); ++column_index) {
      const auto& input_column = cache_in->columns[column_index];
      ValueColumnBuffer output_column;
      output_column.values.reserve(row_order.size());
      for (auto row_index : row_order) {
        output_column.values.push_back(valueColumnValueAt(input_column, row_index));
      }
      cache->columns.push_back(std::move(output_column));
    }
    out.columnar_cache = std::move(cache);
  }
  return out;
}

Table topNTable(const Table& table, const std::vector<std::size_t>& indices,
                const std::vector<bool>& ascending, std::size_t limit,
                bool materialize_rows) {
  if (!ascending.empty() && ascending.size() != indices.size()) {
    throw std::runtime_error("ORDER BY direction count mismatch");
  }
  const auto row_count = table.rowCount();
  if (indices.empty() || row_count < 2 || limit >= row_count) {
    auto sorted = sortTable(table, indices, ascending);
    if (materialize_rows && sorted.rows.empty()) {
      materializeRows(&sorted);
    }
    return sorted;
  }

  const auto columns = viewValueColumns(table, indices);
  std::vector<bool> directions = ascending;
  if (directions.empty()) {
    directions.assign(indices.size(), true);
  }

  auto less = [&](std::size_t lhs_index, std::size_t rhs_index) {
    for (std::size_t column_index = 0; column_index < columns.size(); ++column_index) {
      const auto lhs = valueColumnValueAt(*columns[column_index].buffer, lhs_index);
      const auto rhs = valueColumnValueAt(*columns[column_index].buffer, rhs_index);
      if (lhs.isNull() && rhs.isNull()) {
        continue;
      }
      if (lhs.isNull()) {
        return false;
      }
      if (rhs.isNull()) {
        return true;
      }
      if (lhs == rhs) {
        continue;
      }
      return directions[column_index] ? (lhs < rhs) : (lhs > rhs);
    }
    return lhs_index < rhs_index;
  };

  std::vector<std::size_t> row_order(row_count);
  for (std::size_t i = 0; i < row_order.size(); ++i) {
    row_order[i] = i;
  }

  std::partial_sort(row_order.begin(), row_order.begin() + limit, row_order.end(), less);
  row_order.resize(limit);

  Table out(table.schema, {});
  if (materialize_rows && !table.rows.empty()) {
    out.rows.reserve(limit);
    for (auto row_index : row_order) {
      out.rows.push_back(table.rows[row_index]);
    }
  }
  if (const auto cache_in = snapshotColumnarCache(&table)) {
    auto cache = std::make_shared<ColumnarTable>();
    cache->schema = out.schema;
    cache->arrow_formats = cache_in->arrow_formats;
    cache->batch_row_counts = {limit};
    cache->row_count = limit;
    cache->columns.reserve(cache_in->columns.size());
    for (std::size_t column_index = 0; column_index < cache_in->columns.size(); ++column_index) {
      const auto& input_column = cache_in->columns[column_index];
      ValueColumnBuffer output_column;
      output_column.values.reserve(limit);
      for (auto row_index : row_order) {
        output_column.values.push_back(valueColumnValueAt(input_column, row_index));
      }
      cache->columns.push_back(std::move(output_column));
    }
    out.columnar_cache = std::move(cache);
  }
  if (materialize_rows && out.rows.empty()) {
    materializeRows(&out);
  }
  return out;
}

Table concatenateTables(const std::vector<Table>& tables, bool materialize_rows) {
  Table out;
  std::size_t total_rows = 0;
  for (const auto& table : tables) {
    if (table.schema.fields.empty()) {
      continue;
    }
    if (out.schema.fields.empty()) {
      out.schema = table.schema;
    } else if (out.schema.fields != table.schema.fields) {
      throw std::runtime_error("concatenateTables schema mismatch");
    }
    total_rows += table.rowCount();
  }
  if (out.schema.fields.empty()) {
    return out;
  }

  auto cache = std::make_shared<ColumnarTable>();
  cache->schema = out.schema;
  cache->columns.resize(out.schema.fields.size());
  cache->arrow_formats.resize(out.schema.fields.size());
  cache->row_count = total_rows;

  for (const auto& table : tables) {
    if (table.schema.fields.empty() || table.rowCount() == 0) {
      continue;
    }
    const auto source_cache = ensureColumnarCache(&table);
    cache->batch_row_counts.push_back(table.rowCount());
    for (std::size_t column = 0; column < source_cache->columns.size(); ++column) {
      auto& out_values = cache->columns[column].values;
      const auto row_count = valueColumnRowCount(source_cache->columns[column]);
      out_values.reserve(out_values.size() + row_count);
      for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
        out_values.push_back(valueColumnValueAt(source_cache->columns[column], row_index));
      }
    }
  }
  out.columnar_cache = std::move(cache);
  if (materialize_rows) {
    materializeRows(&out);
  }
  return out;
}

std::vector<Value> vectorizedWindowStart(const ValueColumnBuffer& input, uint64_t window_ms) {
  if (window_ms == 0) {
    throw std::runtime_error("window size cannot be zero");
  }
  const auto window = static_cast<int64_t>(window_ms);
  const auto row_count = valueColumnRowCount(input);
  std::vector<Value> out(row_count);
  for (std::size_t i = 0; i < row_count; ++i) {
    if (valueColumnIsNullAt(input, i)) {
      continue;
    }
    const auto ts_ms = valueColumnEpochMillisAt(input, i);
    const auto window_start = (ts_ms / window) * window;
    out[i] = Value(window_start);
  }
  return out;
}

std::vector<Value> vectorizedWindowStart(const ValueColumnView& input, uint64_t window_ms) {
  return vectorizedWindowStart(*input.buffer, window_ms);
}

Table assignSlidingWindow(const Table& table, std::size_t time_column_index, uint64_t window_ms,
                          uint64_t slide_ms, const std::string& output_column,
                          bool materialize_rows, bool format_window_as_timestamp) {
  if (window_ms == 0) {
    throw std::runtime_error("window size cannot be zero");
  }
  if (slide_ms == 0) {
    slide_ms = window_ms;
  }
  if (slide_ms > window_ms) {
    throw std::runtime_error("window slide must be <= window size");
  }
  if (slide_ms == window_ms) {
    Table out = table;
    if (out.schema.has(output_column)) {
      throw std::runtime_error("window output column already exists: " + output_column);
    }
    const auto input = viewValueColumn(out, time_column_index);
    auto bucket_values = vectorizedWindowStart(input, window_ms);
    if (format_window_as_timestamp) {
      for (auto& value : bucket_values) {
        if (!value.isNull()) {
          value = Value(formatTimestampMillis(value.asInt64()));
        }
      }
    }
    appendNamedColumn(&out, output_column, std::move(bucket_values), false);
    if (materialize_rows && out.rows.empty()) {
      materializeRows(&out);
    }
    return out;
  }

  Table materialized = table;
  materializeRows(&materialized);
  Table out;
  out.schema = materialized.schema;
  if (out.schema.has(output_column)) {
    throw std::runtime_error("window output column already exists: " + output_column);
  }
  out.schema.fields.push_back(output_column);
  out.schema.index.emplace(output_column, out.schema.fields.size() - 1);

  const auto windows_per_row = std::max<uint64_t>(1, (window_ms + slide_ms - 1) / slide_ms);
  out.rows.reserve(materialized.rows.size() * static_cast<std::size_t>(windows_per_row));
  for (const auto& row : materialized.rows) {
    if (time_column_index >= row.size()) {
      throw std::runtime_error("window column index out of range");
    }
    const auto& value = row[time_column_index];
    if (value.isNull()) {
      auto new_row = row;
      new_row.push_back(Value());
      out.rows.push_back(std::move(new_row));
      continue;
    }
    const int64_t ts_ms = value.isNumber() ? value.asInt64() : parseEpochMillis(value.toString());
    const int64_t slide = static_cast<int64_t>(slide_ms);
    const int64_t window = static_cast<int64_t>(window_ms);
    const int64_t last_start = (ts_ms / slide) * slide;
    bool emitted = false;
    for (uint64_t i = 0; i < windows_per_row; ++i) {
      const int64_t start = last_start - static_cast<int64_t>(i) * slide;
      if (ts_ms >= start && ts_ms < start + window) {
        auto new_row = row;
        new_row.push_back(format_window_as_timestamp ? Value(formatTimestampMillis(start))
                                                    : Value(start));
        out.rows.push_back(std::move(new_row));
        emitted = true;
      }
    }
    if (!emitted) {
      auto new_row = row;
      new_row.push_back(Value());
      out.rows.push_back(std::move(new_row));
    }
  }

  if (!materialize_rows) {
    // Keep row representation only; callers that need columnar cache can build it lazily.
    out.columnar_cache.reset();
  }
  return out;
}

std::vector<Value> vectorizedStringLength(const StringColumnBuffer& input) {
  return unaryStringKernel(input, [](const std::string& value) {
    return Value(static_cast<int64_t>(value.size()));
  });
}

std::vector<Value> vectorizedStringLower(const StringColumnBuffer& input) {
  return unaryStringKernel(input, [](const std::string& value) { return Value(lowerString(value)); });
}

std::vector<Value> vectorizedStringUpper(const StringColumnBuffer& input) {
  return unaryStringKernel(input, [](const std::string& value) { return Value(upperString(value)); });
}

std::vector<Value> vectorizedStringTrim(const StringColumnBuffer& input) {
  return unaryStringKernel(input, [](const std::string& value) { return Value(trimString(value)); });
}

std::vector<Value> vectorizedStringLtrim(const StringColumnBuffer& input) {
  return unaryStringKernel(input, [](const std::string& value) { return Value(ltrimString(value)); });
}

std::vector<Value> vectorizedStringRtrim(const StringColumnBuffer& input) {
  return unaryStringKernel(input, [](const std::string& value) { return Value(rtrimString(value)); });
}

std::vector<Value> vectorizedStringReverse(const StringColumnBuffer& input) {
  return unaryStringKernel(input, [](const std::string& value) { return Value(reverseString(value)); });
}

std::vector<Value> vectorizedStringConcat(const std::vector<StringColumnBuffer>& inputs) {
  if (inputs.empty()) {
    throw std::runtime_error("CONCAT expects at least 1 argument");
  }
  const auto row_count = inputs.front().values.size();
  std::vector<Value> out(row_count);
  for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
    bool has_null = false;
    std::string result;
    for (const auto& input : inputs) {
      if (input.values.size() != row_count || input.is_null.size() != row_count) {
        throw std::runtime_error("columnar kernel input row count mismatch");
      }
      if (input.is_null[row_index] != 0) {
        has_null = true;
        break;
      }
      result += input.values[row_index];
    }
    if (!has_null) {
      out[row_index] = Value(std::move(result));
    }
  }
  return out;
}

std::vector<Value> vectorizedStringConcatWs(const std::vector<StringColumnBuffer>& inputs) {
  if (inputs.size() < 2) {
    throw std::runtime_error("CONCAT_WS expects at least 2 arguments");
  }
  const auto row_count = inputs.front().values.size();
  std::vector<Value> out(row_count);
  for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
    bool has_null = false;
    for (const auto& input : inputs) {
      if (input.values.size() != row_count || input.is_null.size() != row_count) {
        throw std::runtime_error("columnar kernel input row count mismatch");
      }
      if (input.is_null[row_index] != 0) {
        has_null = true;
        break;
      }
    }
    if (has_null) {
      continue;
    }
    const auto& delim = inputs[0].values[row_index];
    std::string result;
    for (std::size_t i = 1; i < inputs.size(); ++i) {
      if (i > 1) {
        result += delim;
      }
      result += inputs[i].values[row_index];
    }
    out[row_index] = Value(std::move(result));
  }
  return out;
}

std::vector<Value> vectorizedStringLeft(const StringColumnBuffer& input,
                                        const Int64ColumnBuffer& length) {
  if (input.values.size() != length.values.size() || input.is_null.size() != length.is_null.size() ||
      input.values.size() != input.is_null.size()) {
    throw std::runtime_error("columnar kernel input row count mismatch");
  }
  std::vector<Value> out(input.values.size());
  for (std::size_t i = 0; i < input.values.size(); ++i) {
    if (input.is_null[i] != 0 || length.is_null[i] != 0) {
      continue;
    }
    if (length.values[i] <= 0) {
      out[i] = Value(std::string());
      continue;
    }
    const auto byte_length = static_cast<std::size_t>(length.values[i]);
    out[i] = byte_length >= input.values[i].size()
                 ? Value(input.values[i])
                 : Value(input.values[i].substr(0, byte_length));
  }
  return out;
}

std::vector<Value> vectorizedStringRight(const StringColumnBuffer& input,
                                         const Int64ColumnBuffer& length) {
  if (input.values.size() != length.values.size() || input.is_null.size() != length.is_null.size() ||
      input.values.size() != input.is_null.size()) {
    throw std::runtime_error("columnar kernel input row count mismatch");
  }
  std::vector<Value> out(input.values.size());
  for (std::size_t i = 0; i < input.values.size(); ++i) {
    if (input.is_null[i] != 0 || length.is_null[i] != 0) {
      continue;
    }
    if (length.values[i] <= 0) {
      out[i] = Value(std::string());
      continue;
    }
    const auto byte_length = static_cast<std::size_t>(length.values[i]);
    out[i] = byte_length >= input.values[i].size()
                 ? Value(input.values[i])
                 : Value(input.values[i].substr(input.values[i].size() - byte_length));
  }
  return out;
}

std::vector<Value> vectorizedStringSubstr(const StringColumnBuffer& input,
                                          const Int64ColumnBuffer& start_one_based,
                                          const Int64ColumnBuffer* length) {
  if (input.values.size() != start_one_based.values.size() ||
      input.is_null.size() != start_one_based.is_null.size() ||
      input.values.size() != input.is_null.size()) {
    throw std::runtime_error("columnar kernel input row count mismatch");
  }
  if (length != nullptr &&
      (input.values.size() != length->values.size() || input.is_null.size() != length->is_null.size())) {
    throw std::runtime_error("columnar kernel input row count mismatch");
  }
  std::vector<Value> out(input.values.size());
  for (std::size_t i = 0; i < input.values.size(); ++i) {
    if (input.is_null[i] != 0 || start_one_based.is_null[i] != 0 ||
        (length != nullptr && length->is_null[i] != 0)) {
      continue;
    }
    const auto start = std::max<int64_t>(1, start_one_based.values[i]);
    if (start > static_cast<int64_t>(input.values[i].size())) {
      out[i] = Value(std::string());
      continue;
    }
    const auto byte_start = static_cast<std::size_t>(start - 1);
    if (length == nullptr) {
      out[i] = Value(input.values[i].substr(byte_start));
      continue;
    }
    if (length->values[i] <= 0) {
      out[i] = Value(std::string());
      continue;
    }
    const auto byte_length = static_cast<std::size_t>(length->values[i]);
    const auto end = std::min<std::size_t>(input.values[i].size(), byte_start + byte_length);
    out[i] = Value(input.values[i].substr(byte_start, end - byte_start));
  }
  return out;
}

std::vector<Value> vectorizedStringReplace(const StringColumnBuffer& input,
                                           const StringColumnBuffer& from,
                                           const StringColumnBuffer& to) {
  if (input.values.size() != from.values.size() || input.values.size() != to.values.size() ||
      input.is_null.size() != from.is_null.size() || input.is_null.size() != to.is_null.size() ||
      input.values.size() != input.is_null.size()) {
    throw std::runtime_error("columnar kernel input row count mismatch");
  }
  std::vector<Value> out(input.values.size());
  for (std::size_t i = 0; i < input.values.size(); ++i) {
    if (input.is_null[i] != 0 || from.is_null[i] != 0 || to.is_null[i] != 0) {
      continue;
    }
    out[i] = Value(replaceString(input.values[i], from.values[i], to.values[i]));
  }
  return out;
}

std::vector<Value> vectorizedStringPosition(const StringColumnBuffer& needle,
                                            const StringColumnBuffer& input) {
  if (needle.values.size() != input.values.size() || needle.is_null.size() != input.is_null.size() ||
      input.values.size() != input.is_null.size()) {
    throw std::runtime_error("columnar kernel input row count mismatch");
  }
  std::vector<Value> out(input.values.size());
  for (std::size_t i = 0; i < input.values.size(); ++i) {
    if (needle.is_null[i] != 0 || input.is_null[i] != 0) {
      continue;
    }
    const auto position = input.values[i].find(needle.values[i]);
    out[i] = position == std::string::npos ? Value(static_cast<int64_t>(0))
                                           : Value(static_cast<int64_t>(position + 1));
  }
  return out;
}

std::vector<Value> vectorizedAbs(const DoubleColumnBuffer& input) {
  std::vector<Value> out(input.values.size());
  for (std::size_t i = 0; i < input.values.size(); ++i) {
    if (input.is_null[i] != 0) {
      continue;
    }
    const auto abs_value = std::fabs(input.values[i]);
    out[i] = Value(abs_value);
  }
  return out;
}

std::vector<Value> vectorizedCeil(const DoubleColumnBuffer& input) {
  std::vector<Value> out(input.values.size());
  for (std::size_t i = 0; i < input.values.size(); ++i) {
    if (input.is_null[i] != 0) {
      continue;
    }
    out[i] = Value(std::ceil(input.values[i]));
  }
  return out;
}

std::vector<Value> vectorizedFloor(const DoubleColumnBuffer& input) {
  std::vector<Value> out(input.values.size());
  for (std::size_t i = 0; i < input.values.size(); ++i) {
    if (input.is_null[i] != 0) {
      continue;
    }
    out[i] = Value(std::floor(input.values[i]));
  }
  return out;
}

std::vector<Value> vectorizedRound(const DoubleColumnBuffer& input) {
  std::vector<Value> out(input.values.size());
  for (std::size_t i = 0; i < input.values.size(); ++i) {
    if (input.is_null[i] != 0) {
      continue;
    }
    out[i] = Value(std::round(input.values[i]));
  }
  return out;
}

std::vector<Value> vectorizedDateYear(const ValueColumnView& input) {
  const auto row_count = valueColumnViewRowCount(input);
  std::vector<Value> out(row_count);
  for (std::size_t i = 0; i < row_count; ++i) {
    if (valueColumnIsNullAt(*input.buffer, i)) {
      continue;
    }
    const int64_t millis = valueColumnEpochMillisAt(*input.buffer, i);
    std::time_t sec = static_cast<std::time_t>(millis / 1000);
    const auto tm_utc = toUtcTm(sec);
    out[i] = Value(static_cast<int64_t>(tm_utc.tm_year + 1900));
  }
  return out;
}

std::vector<Value> vectorizedDateMonth(const ValueColumnView& input) {
  const auto row_count = valueColumnViewRowCount(input);
  std::vector<Value> out(row_count);
  for (std::size_t i = 0; i < row_count; ++i) {
    if (valueColumnIsNullAt(*input.buffer, i)) {
      continue;
    }
    const int64_t millis = valueColumnEpochMillisAt(*input.buffer, i);
    std::time_t sec = static_cast<std::time_t>(millis / 1000);
    const auto tm_utc = toUtcTm(sec);
    out[i] = Value(static_cast<int64_t>(tm_utc.tm_mon + 1));
  }
  return out;
}

std::vector<Value> vectorizedDateDay(const ValueColumnView& input) {
  const auto row_count = valueColumnViewRowCount(input);
  std::vector<Value> out(row_count);
  for (std::size_t i = 0; i < row_count; ++i) {
    if (valueColumnIsNullAt(*input.buffer, i)) {
      continue;
    }
    const int64_t millis = valueColumnEpochMillisAt(*input.buffer, i);
    std::time_t sec = static_cast<std::time_t>(millis / 1000);
    const auto tm_utc = toUtcTm(sec);
    out[i] = Value(static_cast<int64_t>(tm_utc.tm_mday));
  }
  return out;
}

std::vector<Value> computeComputedColumnValues(Table* table, ComputedColumnKind function,
                                              const std::vector<ComputedColumnArg>& args) {
  if (table == nullptr) {
    throw std::invalid_argument("computed function table is null");
  }
  struct BoundComputedArg {
    bool is_literal = false;
    bool is_function = false;
    size_t source_column_index = 0;
    bool literal_is_null = false;
    bool literal_is_numeric = false;
    int64_t literal_int64 = 0;
    double literal_double = 0.0;
    std::string literal_string;
    Value literal;
    std::string source_column_name;
    ComputedColumnKind function = ComputedColumnKind::Copy;
    std::vector<ComputedColumnArg> args;
  };

  std::vector<BoundComputedArg> bound;
  bound.reserve(args.size());
  for (const auto& arg : args) {
    BoundComputedArg item;
    item.is_literal = arg.is_literal;
    item.is_function = arg.is_function;
    item.source_column_index = arg.source_column_index;
    item.source_column_name = arg.source_column_name;
    item.function = arg.function;
    item.args = arg.args;
    if (arg.is_literal) {
      item.literal = arg.literal;
      item.literal_is_null = arg.literal.isNull();
      if (!item.literal_is_null) {
        item.literal_string = arg.literal.type() == DataType::String ? arg.literal.asString()
                                                                    : arg.literal.toString();
        if (arg.literal.isNumber()) {
          item.literal_is_numeric = true;
          item.literal_int64 = arg.literal.asInt64();
          item.literal_double = arg.literal.asDouble();
        }
      }
    }
    bound.push_back(std::move(item));
  }

  const auto row_count = table->rowCount();

  auto resolve_source_index = [&](const BoundComputedArg& arg) {
    if (arg.source_column_index != static_cast<size_t>(-1)) {
      return arg.source_column_index;
    }
    if (arg.source_column_name.empty()) {
      throw std::runtime_error("computed function argument has no source column");
    }
    return table->schema.indexOf(arg.source_column_name);
  };

  auto materialize_value_arg = [&](const BoundComputedArg& arg) -> std::vector<Value> {
    if (arg.is_function) {
      return computeComputedColumnValues(table, arg.function, arg.args);
    }
    if (arg.is_literal) {
      return std::vector<Value>(row_count, arg.literal);
    }
    const auto input = viewValueColumn(*table, resolve_source_index(arg));
    std::vector<Value> values(row_count);
    for (std::size_t i = 0; i < row_count; ++i) {
      values[i] = valueColumnValueAt(*input.buffer, i);
    }
    return values;
  };

  auto string_buffer_from_values = [&](const std::vector<Value>& values) {
    StringColumnBuffer out;
    out.values.resize(values.size());
    out.is_null.assign(values.size(), 0);
    for (std::size_t i = 0; i < values.size(); ++i) {
      if (values[i].isNull()) {
        out.is_null[i] = 1;
        continue;
      }
      out.values[i] = values[i].toString();
    }
    return out;
  };

  auto int64_buffer_from_values = [&](const std::vector<Value>& values) {
    Int64ColumnBuffer out;
    out.values.resize(values.size());
    out.is_null.assign(values.size(), 0);
    for (std::size_t i = 0; i < values.size(); ++i) {
      if (values[i].isNull()) {
        out.is_null[i] = 1;
        continue;
      }
      out.values[i] = values[i].isNumber() ? values[i].asInt64()
                                           : static_cast<int64_t>(std::stoll(values[i].toString()));
    }
    return out;
  };

  auto double_buffer_from_values = [&](const std::vector<Value>& values) {
    DoubleColumnBuffer out;
    out.values.resize(values.size());
    out.is_null.assign(values.size(), 0);
    for (std::size_t i = 0; i < values.size(); ++i) {
      if (values[i].isNull()) {
        out.is_null[i] = 1;
        continue;
      }
      out.values[i] = values[i].isNumber() ? values[i].asDouble()
                                           : std::stod(values[i].toString());
    }
    return out;
  };

  auto materialize_string_args = [&]() {
    std::vector<StringColumnBuffer> columns;
    columns.reserve(bound.size());
    for (const auto& arg : bound) {
      if (arg.is_function) {
        columns.push_back(string_buffer_from_values(materialize_value_arg(arg)));
        continue;
      }
      if (arg.is_literal) {
        if (arg.literal_is_null) {
          columns.push_back(makeNullStringColumn(row_count));
          continue;
        }
        columns.push_back(makeConstantStringColumn(row_count, arg.literal_string));
        continue;
      }
      columns.push_back(materializeStringColumn(*table, resolve_source_index(arg)));
    }
    return columns;
  };

  auto materialize_int64_arg = [&](const BoundComputedArg& arg) -> Int64ColumnBuffer {
    if (arg.is_function) {
      return int64_buffer_from_values(materialize_value_arg(arg));
    }
    if (arg.is_literal) {
      if (arg.literal_is_null) {
        return makeNullInt64Column(row_count);
      }
      if (!arg.literal_is_numeric) {
        throw std::runtime_error("computed function argument must be numeric");
      }
      return makeConstantInt64Column(row_count, arg.literal_int64);
    }
    return materializeInt64Column(*table, resolve_source_index(arg));
  };

  auto materialize_double_arg = [&](const BoundComputedArg& arg) -> DoubleColumnBuffer {
    if (arg.is_function) {
      return double_buffer_from_values(materialize_value_arg(arg));
    }
    if (arg.is_literal) {
      if (arg.literal_is_null) {
        return makeNullDoubleColumn(row_count);
      }
      if (!arg.literal_is_numeric) {
        throw std::runtime_error("computed function argument must be numeric");
      }
      DoubleColumnBuffer out;
      out.values.assign(row_count, arg.literal_double);
      out.is_null.assign(row_count, 0);
      return out;
    }
    return materializeDoubleColumn(*table, resolve_source_index(arg));
  };

  auto materialize_date_field = [&](const BoundComputedArg& arg, int which) {
    std::vector<Value> output(row_count);
    for (std::size_t i = 0; i < row_count; ++i) {
      Value value;
      if (arg.is_literal || arg.is_function) {
        auto values = materialize_value_arg(arg);
        value = values[i];
      } else {
        const auto input = viewValueColumn(*table, resolve_source_index(arg));
        if (i >= valueColumnViewRowCount(input)) {
          throw std::runtime_error("computed function argument index out of range");
        }
        value = valueColumnValueAt(*input.buffer, i);
      }
      if (value.isNull()) {
        continue;
      }
      const int64_t millis = valueAsEpochMillis(value);
      std::time_t sec = static_cast<std::time_t>(millis / 1000);
      const auto tm_utc = toUtcTm(sec);
      int64_t field = 0;
      if (which == 0) {
        field = static_cast<int64_t>(tm_utc.tm_year + 1900);
      } else if (which == 1) {
        field = static_cast<int64_t>(tm_utc.tm_mon + 1);
      } else {
        field = static_cast<int64_t>(tm_utc.tm_mday);
      }
      output[i] = Value(field);
    }
    return output;
  };

  switch (function) {
    case ComputedColumnKind::Copy:
      throw std::runtime_error("computed function must not be COPY");
    case ComputedColumnKind::Cast: {
      if (bound.size() != 2 || !bound[1].is_literal || bound[1].literal_is_null) {
        throw std::runtime_error("CAST expects value and target type");
      }
      const auto values = materialize_value_arg(bound[0]);
      const auto& target = bound[1].literal_string;
      std::vector<Value> out(values.size());
      for (std::size_t i = 0; i < values.size(); ++i) {
        const auto& value = values[i];
        if (value.isNull()) {
          continue;
        }
        if (target == "STRING" || target == "TEXT") {
          out[i] = Value(value.toString());
        } else if (target == "INT" || target == "INTEGER" || target == "INT64" ||
                   target == "BIGINT") {
          out[i] = Value(value.isNumber() ? value.asInt64()
                                          : static_cast<int64_t>(std::stoll(value.toString())));
        } else if (target == "DOUBLE" || target == "FLOAT" || target == "REAL") {
          out[i] = Value(value.isNumber() ? value.asDouble() : std::stod(value.toString()));
        } else if (target == "BOOL" || target == "BOOLEAN") {
          if (value.isBool()) {
            out[i] = Value(value.asBool());
          } else if (value.isNumber()) {
            out[i] = Value(value.asDouble() != 0.0);
          } else {
            auto text = value.toString();
            std::transform(text.begin(), text.end(), text.begin(), [](unsigned char ch) {
              return static_cast<char>(std::tolower(ch));
            });
            out[i] = Value(text == "true" || text == "1");
          }
        } else {
          throw std::runtime_error("unsupported CAST target type: " + target);
        }
      }
      return out;
    }
    case ComputedColumnKind::StringLength:
      if (bound.size() != 1) throw std::runtime_error("LENGTH expects 1 argument");
      return vectorizedStringLength(materialize_string_args()[0]);
    case ComputedColumnKind::StringLower:
      if (bound.size() != 1) throw std::runtime_error("LOWER expects 1 argument");
      return vectorizedStringLower(materialize_string_args()[0]);
    case ComputedColumnKind::StringUpper:
      if (bound.size() != 1) throw std::runtime_error("UPPER expects 1 argument");
      return vectorizedStringUpper(materialize_string_args()[0]);
    case ComputedColumnKind::StringTrim:
      if (bound.size() != 1) throw std::runtime_error("TRIM expects 1 argument");
      return vectorizedStringTrim(materialize_string_args()[0]);
    case ComputedColumnKind::StringConcat:
      return vectorizedStringConcat(materialize_string_args());
    case ComputedColumnKind::StringReverse:
      if (bound.size() != 1) throw std::runtime_error("REVERSE expects 1 argument");
      return vectorizedStringReverse(materialize_string_args()[0]);
    case ComputedColumnKind::StringConcatWs:
      return vectorizedStringConcatWs(materialize_string_args());
    case ComputedColumnKind::StringLeft:
      if (bound.size() != 2) throw std::runtime_error("LEFT requires 2 arguments");
      return vectorizedStringLeft(materialize_string_args()[0], materialize_int64_arg(bound[1]));
    case ComputedColumnKind::StringRight:
      if (bound.size() != 2) throw std::runtime_error("RIGHT requires 2 arguments");
      return vectorizedStringRight(materialize_string_args()[0], materialize_int64_arg(bound[1]));
    case ComputedColumnKind::StringPosition:
      if (bound.size() != 2) throw std::runtime_error("POSITION requires 2 arguments");
      {
        auto string_args = materialize_string_args();
        return vectorizedStringPosition(string_args[0], string_args[1]);
      }
    case ComputedColumnKind::StringSubstr:
      if (bound.size() < 2 || bound.size() > 3) throw std::runtime_error("SUBSTR expects 2 or 3 arguments");
      if (bound.size() == 2) {
        auto input = materialize_string_args();
        return vectorizedStringSubstr(input[0], materialize_int64_arg(bound[1]), nullptr);
      }
      {
        auto input = materialize_string_args();
        auto length = materialize_int64_arg(bound[2]);
        return vectorizedStringSubstr(input[0], materialize_int64_arg(bound[1]), &length);
      }
    case ComputedColumnKind::StringLtrim:
      if (bound.size() != 1) throw std::runtime_error("LTRIM/RTRIM requires 1 argument");
      return vectorizedStringLtrim(materialize_string_args()[0]);
    case ComputedColumnKind::StringRtrim:
      if (bound.size() != 1) throw std::runtime_error("LTRIM/RTRIM requires 1 argument");
      return vectorizedStringRtrim(materialize_string_args()[0]);
    case ComputedColumnKind::StringReplace:
      if (bound.size() != 3) throw std::runtime_error("REPLACE requires 3 arguments");
      {
        auto input = materialize_string_args();
        return vectorizedStringReplace(input[0], input[1], input[2]);
      }
    case ComputedColumnKind::NumericAbs:
      if (bound.size() != 1) throw std::runtime_error("ABS expects 1 argument");
      return vectorizedAbs(materialize_double_arg(bound[0]));
    case ComputedColumnKind::NumericCeil:
      if (bound.size() != 1) throw std::runtime_error("CEIL expects 1 argument");
      return vectorizedCeil(materialize_double_arg(bound[0]));
    case ComputedColumnKind::NumericFloor:
      if (bound.size() != 1) throw std::runtime_error("FLOOR expects 1 argument");
      return vectorizedFloor(materialize_double_arg(bound[0]));
    case ComputedColumnKind::NumericRound:
      if (bound.size() != 1) throw std::runtime_error("ROUND expects 1 argument");
      return vectorizedRound(materialize_double_arg(bound[0]));
    case ComputedColumnKind::DateYear:
      if (bound.size() != 1) throw std::runtime_error("YEAR expects 1 argument");
      if (!bound[0].is_literal) {
        return vectorizedDateYear(viewValueColumn(*table, resolve_source_index(bound[0])));
      }
      return materialize_date_field(bound[0], 0);
    case ComputedColumnKind::DateMonth:
      if (bound.size() != 1) throw std::runtime_error("MONTH expects 1 argument");
      if (!bound[0].is_literal) {
        return vectorizedDateMonth(viewValueColumn(*table, resolve_source_index(bound[0])));
      }
      return materialize_date_field(bound[0], 1);
    case ComputedColumnKind::DateDay:
      if (bound.size() != 1) throw std::runtime_error("DAY expects 1 argument");
      if (!bound[0].is_literal) {
        return vectorizedDateDay(viewValueColumn(*table, resolve_source_index(bound[0])));
      }
      return materialize_date_field(bound[0], 2);
  }
  throw std::runtime_error("unsupported computed function");
}

void appendColumn(Table* table, std::vector<Value>&& values, bool materialize_rows) {
  if (table == nullptr) {
    throw std::invalid_argument("appendColumn table is null");
  }
  const auto cache_snapshot = snapshotColumnarCache(table);
  if (materialize_rows && table->rows.empty() && cache_snapshot != nullptr) {
    materializeRows(table);
  }
  std::size_t expected_row_count = table->rowCount();
  if (expected_row_count == 0 && cache_snapshot != nullptr && cache_snapshot->columns.empty()) {
    expected_row_count = values.size();
  }
  if (values.size() != expected_row_count) {
    throw std::runtime_error("computed column row count mismatch");
  }
  const bool maintain_rows = !table->rows.empty();
  if (maintain_rows) {
    for (auto& row : table->rows) {
      row.reserve(row.size() + 1);
    }
  }
  const bool maintain_cache = cache_snapshot != nullptr;
  ValueColumnBuffer appended;
  if (maintain_rows) {
    if (maintain_cache) {
      appended.values = values;
    }
    for (std::size_t row_index = 0; row_index < table->rows.size(); ++row_index) {
      table->rows[row_index].push_back(std::move(values[row_index]));
    }
  } else if (maintain_cache) {
    appended.values = std::move(values);
  }
  std::lock_guard<std::mutex> lock(table->columnar_cache_mu);
  if (table->columnar_cache) {
    auto cache = table->columnar_cache;
    if (!cache.unique()) {
      cache = std::make_shared<ColumnarTable>(*cache);
    }
    cache->schema = table->schema;
    cache->row_count = expected_row_count;
    cache->columns.push_back(std::move(appended));
    if (cache->arrow_formats.size() < cache->columns.size()) {
      cache->arrow_formats.push_back("");
    }
    table->columnar_cache = std::move(cache);
  }
}

void appendColumn(Table* table, ValueColumnBuffer&& column, bool materialize_rows) {
  if (table == nullptr) {
    throw std::invalid_argument("appendColumn table is null");
  }
  const auto cache_snapshot = snapshotColumnarCache(table);
  if (materialize_rows && table->rows.empty() && cache_snapshot != nullptr) {
    materializeRows(table);
  }
  std::size_t expected_row_count = table->rowCount();
  if (expected_row_count == 0 && cache_snapshot != nullptr && cache_snapshot->columns.empty()) {
    expected_row_count = valueColumnRowCount(column);
  }
  if (valueColumnRowCount(column) != expected_row_count) {
    throw std::runtime_error("computed column row count mismatch");
  }
  const bool maintain_rows = !table->rows.empty();
  const bool maintain_cache = cache_snapshot != nullptr;
  if (maintain_rows) {
    for (std::size_t row_index = 0; row_index < table->rows.size(); ++row_index) {
      table->rows[row_index].push_back(valueColumnValueAt(column, row_index));
    }
  }
  ValueColumnBuffer appended = maintain_cache ? std::move(column) : ValueColumnBuffer{};
  std::lock_guard<std::mutex> lock(table->columnar_cache_mu);
  if (table->columnar_cache) {
    auto cache = table->columnar_cache;
    if (!cache.unique()) {
      cache = std::make_shared<ColumnarTable>(*cache);
    }
    cache->schema = table->schema;
    cache->row_count = expected_row_count;
    cache->columns.push_back(std::move(appended));
    if (cache->arrow_formats.size() < cache->columns.size()) {
      if (cache->columns.back().arrow_backing != nullptr) {
        cache->arrow_formats.push_back(cache->columns.back().arrow_backing->format);
      } else {
        cache->arrow_formats.push_back("");
      }
    }
    table->columnar_cache = std::move(cache);
  }
}

void appendNamedColumn(Table* table, const std::string& column_name, std::vector<Value>&& values,
                       bool materialize_rows) {
  if (table == nullptr) {
    throw std::invalid_argument("appendNamedColumn table is null");
  }
  if (!table->columnar_cache) {
    table->columnar_cache = makeColumnarCache(*table);
  }
  table->schema.fields.push_back(column_name);
  table->schema.index[column_name] = table->schema.fields.size() - 1;
  appendColumn(table, std::move(values), materialize_rows);
}

void appendNamedColumn(Table* table, const std::string& column_name, ValueColumnBuffer&& column,
                       bool materialize_rows) {
  if (table == nullptr) {
    throw std::invalid_argument("appendNamedColumn table is null");
  }
  if (!table->columnar_cache) {
    table->columnar_cache = makeColumnarCache(*table);
  }
  table->schema.fields.push_back(column_name);
  table->schema.index[column_name] = table->schema.fields.size() - 1;
  appendColumn(table, std::move(column), materialize_rows);
}

}  // namespace dataflow
