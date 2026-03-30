#include "src/dataflow/core/execution/stream/binary_row_batch.h"

#include <cstring>
#include <stdexcept>
#include <unordered_map>

namespace dataflow {

void StringBlobStorage::clear() {
  offsets.clear();
  blob.clear();
}

void StringBlobStorage::reserve(size_t count, size_t bytes) {
  offsets.reserve(count + 1);
  blob.reserve(bytes);
}

uint32_t StringBlobStorage::append(const std::string& value) {
  if (offsets.empty()) {
    offsets.push_back(0);
  }
  blob.insert(blob.end(), value.begin(), value.end());
  offsets.push_back(static_cast<uint32_t>(blob.size()));
  return static_cast<uint32_t>(offsets.size() - 2);
}

uint32_t StringBlobStorage::append(std::string_view value) {
  if (offsets.empty()) {
    offsets.push_back(0);
  }
  blob.insert(blob.end(), value.begin(), value.end());
  offsets.push_back(static_cast<uint32_t>(blob.size()));
  return static_cast<uint32_t>(offsets.size() - 2);
}

std::string_view StringBlobStorage::view(size_t index) const {
  if (offsets.size() < 2 || index + 1 >= offsets.size()) {
    return {};
  }
  const uint32_t begin = offsets[index];
  const uint32_t end = offsets[index + 1];
  if (begin > end || end > blob.size()) {
    return {};
  }
  return std::string_view(blob.data() + begin, end - begin);
}

size_t StringBlobStorage::size() const {
  return offsets.empty() ? 0 : offsets.size() - 1;
}

namespace {

constexpr uint8_t kMagic[4] = {'B', 'R', 'B', '1'};
constexpr uint8_t kEncodingPlain = 0;
constexpr uint8_t kEncodingDictionary = 1;

size_t varintSize(uint64_t value) {
  size_t size = 1;
  while (value >= 0x80) {
    value >>= 7;
    ++size;
  }
  return size;
}

struct RawWriter {
  uint8_t* data = nullptr;
  size_t capacity = 0;
  size_t offset = 0;

  void ensure(size_t bytes) {
    if (offset + bytes > capacity) {
      throw std::out_of_range("binary row batch writer overflow");
    }
  }

  void appendByte(uint8_t value) {
    ensure(1);
    data[offset++] = value;
  }

  void appendBytes(const void* src, size_t size) {
    ensure(size);
    std::memcpy(data + offset, src, size);
    offset += size;
  }
};

struct BufferCursor {
  const uint8_t* data = nullptr;
  size_t size = 0;
  size_t offset = 0;
};

bool readU8(const BufferCursor& src, size_t* offset, uint8_t* out) {
  if (offset == nullptr || out == nullptr || *offset >= src.size) return false;
  *out = src.data[*offset];
  ++(*offset);
  return true;
}

bool readU32(const BufferCursor& src, size_t* offset, uint32_t* out) {
  if (offset == nullptr || out == nullptr || *offset + 4 > src.size) return false;
  *out = static_cast<uint32_t>(src.data[*offset]) |
         (static_cast<uint32_t>(src.data[*offset + 1]) << 8) |
         (static_cast<uint32_t>(src.data[*offset + 2]) << 16) |
         (static_cast<uint32_t>(src.data[*offset + 3]) << 24);
  *offset += 4;
  return true;
}

bool readU64(const BufferCursor& src, size_t* offset, uint64_t* out) {
  if (offset == nullptr || out == nullptr || *offset + 8 > src.size) return false;
  *out = static_cast<uint64_t>(src.data[*offset]) |
         (static_cast<uint64_t>(src.data[*offset + 1]) << 8) |
         (static_cast<uint64_t>(src.data[*offset + 2]) << 16) |
         (static_cast<uint64_t>(src.data[*offset + 3]) << 24) |
         (static_cast<uint64_t>(src.data[*offset + 4]) << 32) |
         (static_cast<uint64_t>(src.data[*offset + 5]) << 40) |
         (static_cast<uint64_t>(src.data[*offset + 6]) << 48) |
         (static_cast<uint64_t>(src.data[*offset + 7]) << 56);
  *offset += 8;
  return true;
}

bool readVarint(const BufferCursor& src, size_t* offset, uint64_t* out) {
  if (offset == nullptr || out == nullptr) return false;
  uint64_t value = 0;
  uint32_t shift = 0;
  while (*offset < src.size && shift <= 63) {
    const uint8_t byte = src.data[*offset];
    ++(*offset);
    value |= static_cast<uint64_t>(byte & 0x7F) << shift;
    if ((byte & 0x80) == 0) {
      *out = value;
      return true;
    }
    shift += 7;
  }
  return false;
}

uint64_t zigZagEncode(int64_t value) {
  return (static_cast<uint64_t>(value) << 1) ^ static_cast<uint64_t>(value >> 63);
}

int64_t zigZagDecode(uint64_t value) {
  return static_cast<int64_t>((value >> 1) ^ (~(value & 1) + 1));
}

void writeU32(RawWriter* out, uint32_t value) {
  out->appendByte(static_cast<uint8_t>(value & 0xFF));
  out->appendByte(static_cast<uint8_t>((value >> 8) & 0xFF));
  out->appendByte(static_cast<uint8_t>((value >> 16) & 0xFF));
  out->appendByte(static_cast<uint8_t>((value >> 24) & 0xFF));
}

void writeU64(RawWriter* out, uint64_t value) {
  out->appendByte(static_cast<uint8_t>(value & 0xFF));
  out->appendByte(static_cast<uint8_t>((value >> 8) & 0xFF));
  out->appendByte(static_cast<uint8_t>((value >> 16) & 0xFF));
  out->appendByte(static_cast<uint8_t>((value >> 24) & 0xFF));
  out->appendByte(static_cast<uint8_t>((value >> 32) & 0xFF));
  out->appendByte(static_cast<uint8_t>((value >> 40) & 0xFF));
  out->appendByte(static_cast<uint8_t>((value >> 48) & 0xFF));
  out->appendByte(static_cast<uint8_t>((value >> 56) & 0xFF));
}

void writeVarint(RawWriter* out, uint64_t value) {
  while (value >= 0x80) {
    out->appendByte(static_cast<uint8_t>(value) | 0x80);
    value >>= 7;
  }
  out->appendByte(static_cast<uint8_t>(value));
}

void writeString(RawWriter* out, const std::string& value) {
  writeVarint(out, value.size());
  out->appendBytes(value.data(), value.size());
}

bool readString(const BufferCursor& src, size_t* offset, std::string* out) {
  if (offset == nullptr || out == nullptr) return false;
  uint64_t size = 0;
  if (!readVarint(src, offset, &size)) return false;
  if (*offset + size > src.size) return false;
  out->assign(reinterpret_cast<const char*>(src.data + *offset), static_cast<size_t>(size));
  *offset += static_cast<size_t>(size);
  return true;
}

bool readStringView(const BufferCursor& src, size_t* offset, std::string_view* out) {
  if (offset == nullptr || out == nullptr) return false;
  uint64_t size = 0;
  if (!readVarint(src, offset, &size)) return false;
  if (*offset + size > src.size) return false;
  *out = std::string_view(reinterpret_cast<const char*>(src.data + *offset),
                          static_cast<size_t>(size));
  *offset += static_cast<size_t>(size);
  return true;
}

std::vector<size_t> resolveProjectedColumns(const Table& table,
                                            const BinaryRowBatchOptions& options) {
  if (options.projected_columns.empty()) {
    std::vector<size_t> all;
    all.reserve(table.schema.fields.size());
    for (size_t i = 0; i < table.schema.fields.size(); ++i) all.push_back(i);
    return all;
  }
  std::vector<size_t> indices;
  indices.reserve(options.projected_columns.size());
  for (const auto& name : options.projected_columns) {
    indices.push_back(table.schema.indexOf(name));
  }
  return indices;
}

DataType inferColumnType(const Table& table, size_t column_index, size_t row_begin,
                         size_t row_end) {
  DataType inferred = DataType::Nil;
  row_end = std::min(row_end, table.rows.size());
  for (size_t row_idx = row_begin; row_idx < row_end; ++row_idx) {
    const auto& row = table.rows[row_idx];
    if (column_index >= row.size() || row[column_index].isNull()) continue;
    const DataType current = row[column_index].type();
    if (inferred == DataType::Nil) {
      inferred = current;
      continue;
    }
    if (inferred == current) continue;
    if ((inferred == DataType::Int64 && current == DataType::Double) ||
        (inferred == DataType::Double && current == DataType::Int64)) {
      inferred = DataType::Double;
      continue;
    }
    return DataType::String;
  }
  return inferred == DataType::Nil ? DataType::String : inferred;
}

PreparedBinaryRowColumn buildEncodedColumn(const Table& table, size_t column_index, size_t row_begin,
                                           size_t row_end) {
  PreparedBinaryRowColumn encoded;
  encoded.column_index = column_index;
  encoded.type = inferColumnType(table, column_index, row_begin, row_end);
  if (encoded.type != DataType::String) {
    return encoded;
  }

  std::unordered_map<std::string, uint32_t> counts;
  size_t non_null_rows = 0;
  row_end = std::min(row_end, table.rows.size());
  for (size_t row_idx = row_begin; row_idx < row_end; ++row_idx) {
    const auto& row = table.rows[row_idx];
    if (column_index >= row.size() || row[column_index].isNull()) continue;
    ++non_null_rows;
    counts[row[column_index].toString()] += 1;
  }
  if (non_null_rows == 0 || counts.empty()) {
    return encoded;
  }
  if (counts.size() > (non_null_rows / 2) && counts.size() > 256) {
    return encoded;
  }

  encoded.encoding = kEncodingDictionary;
  encoded.dictionary.reserve(counts.size());
  for (const auto& entry : counts) {
    encoded.dictionary_index[entry.first] = static_cast<uint32_t>(encoded.dictionary.size());
    encoded.dictionary.push_back(entry.first);
  }
  return encoded;
}

std::vector<PreparedBinaryRowColumn> buildEncodedColumns(const Table& table,
                                                         const std::vector<size_t>& projected,
                                                         size_t row_begin,
                                                         size_t row_end) {
  std::vector<PreparedBinaryRowColumn> columns;
  columns.reserve(projected.size());
  for (size_t column_index : projected) {
    columns.push_back(buildEncodedColumn(table, column_index, row_begin, row_end));
  }
  return columns;
}

size_t stringEncodedSize(const std::string& value) {
  return varintSize(value.size()) + value.size();
}

size_t estimateValueSize(const Value& value, const PreparedBinaryRowColumn& column) {
  switch (column.type) {
    case DataType::Int64:
      return varintSize(zigZagEncode(value.asInt64()));
    case DataType::Double:
      return sizeof(uint64_t);
    case DataType::String:
      if (column.encoding == kEncodingDictionary) {
        return varintSize(column.dictionary_index.at(value.toString()));
      }
      return stringEncodedSize(value.toString());
    case DataType::FixedVector: {
      const auto& vec = value.asFixedVector();
      return varintSize(vec.size()) + vec.size() * sizeof(uint32_t);
    }
    case DataType::Nil:
    default:
      return stringEncodedSize(value.toString());
  }
}

size_t estimateSerializedSizeInternal(const Table& table, size_t row_begin, size_t row_end,
                                      const std::vector<size_t>& projected,
                                      const std::vector<PreparedBinaryRowColumn>& columns) {
  const size_t row_count = row_end - row_begin;
  size_t size = 0;
  size += 4;  // magic
  size += 4;  // column count
  size += 4;  // row count
  for (size_t i = 0; i < projected.size(); ++i) {
    size += stringEncodedSize(table.schema.fields[projected[i]]);
    size += 1;  // type
    size += 1;  // encoding
    if (columns[i].type == DataType::String && columns[i].encoding == kEncodingDictionary) {
      size += varintSize(columns[i].dictionary.size());
      for (const auto& item : columns[i].dictionary) {
        size += stringEncodedSize(item);
      }
    }
  }
  const size_t null_bytes = (projected.size() + 7) / 8;
  size += row_count * null_bytes;
  for (size_t row_idx = row_begin; row_idx < row_end; ++row_idx) {
    const auto& row = table.rows[row_idx];
    for (size_t i = 0; i < projected.size(); ++i) {
      const size_t column_index = projected[i];
      if (column_index >= row.size() || row[column_index].isNull()) continue;
      size += estimateValueSize(row[column_index], columns[i]);
    }
  }
  return size;
}

size_t serializeRangeInternal(const Table& table, size_t row_begin, size_t row_end,
                              const std::vector<size_t>& projected,
                              const std::vector<PreparedBinaryRowColumn>& columns,
                              RawWriter* writer) {
  writer->appendBytes(kMagic, 4);
  writeU32(writer, static_cast<uint32_t>(projected.size()));
  writeU32(writer, static_cast<uint32_t>(row_end - row_begin));

  for (size_t i = 0; i < projected.size(); ++i) {
    writeString(writer, table.schema.fields[projected[i]]);
    writer->appendByte(static_cast<uint8_t>(columns[i].type));
    writer->appendByte(columns[i].encoding);
    if (columns[i].type == DataType::String && columns[i].encoding == kEncodingDictionary) {
      writeVarint(writer, columns[i].dictionary.size());
      for (const auto& item : columns[i].dictionary) {
        writeString(writer, item);
      }
    }
  }

  const size_t null_bytes = (projected.size() + 7) / 8;
  std::vector<uint8_t> null_bitmap(null_bytes, 0);
  for (size_t row_idx = row_begin; row_idx < row_end; ++row_idx) {
    const auto& row = table.rows[row_idx];
    std::fill(null_bitmap.begin(), null_bitmap.end(), 0);
    for (size_t i = 0; i < projected.size(); ++i) {
      const size_t column_index = projected[i];
      if (column_index >= row.size() || row[column_index].isNull()) {
        null_bitmap[i / 8] |= static_cast<uint8_t>(1u << (i % 8));
      }
    }
    writer->appendBytes(null_bitmap.data(), null_bitmap.size());
    for (size_t i = 0; i < projected.size(); ++i) {
      const size_t column_index = projected[i];
      if (column_index >= row.size() || row[column_index].isNull()) continue;
      const auto& value = row[column_index];
      if (columns[i].type == DataType::String && columns[i].encoding == kEncodingDictionary) {
        writeVarint(writer, columns[i].dictionary_index.at(value.toString()));
      } else if (columns[i].type == DataType::Int64) {
        writeVarint(writer, zigZagEncode(value.asInt64()));
      } else if (columns[i].type == DataType::Double) {
        const double d = value.asDouble();
        uint64_t raw = 0;
        std::memcpy(&raw, &d, sizeof(raw));
        writeU64(writer, raw);
      } else if (columns[i].type == DataType::FixedVector) {
        const auto& vec = value.asFixedVector();
        writeVarint(writer, vec.size());
        for (float item : vec) {
          uint32_t bits = 0;
          std::memcpy(&bits, &item, sizeof(bits));
          writeU32(writer, bits);
        }
      } else {
        writeString(writer, value.toString());
      }
    }
  }
  return writer->offset;
}

bool readValue(const BufferCursor& src, size_t* offset, DataType type, Value* out) {
  if (offset == nullptr || out == nullptr) return false;
  switch (type) {
    case DataType::Int64: {
      uint64_t raw = 0;
      if (!readVarint(src, offset, &raw)) return false;
      *out = Value(zigZagDecode(raw));
      return true;
    }
    case DataType::Double: {
      uint64_t raw = 0;
      if (!readU64(src, offset, &raw)) return false;
      double d = 0.0;
      std::memcpy(&d, &raw, sizeof(d));
      *out = Value(d);
      return true;
    }
    case DataType::String:
    case DataType::Nil:
    default: {
      std::string text;
      if (!readString(src, offset, &text)) return false;
      *out = Value(std::move(text));
      return true;
    }
    case DataType::FixedVector: {
      uint64_t dim = 0;
      if (!readVarint(src, offset, &dim)) return false;
      std::vector<float> vec;
      vec.reserve(static_cast<size_t>(dim));
      for (uint64_t i = 0; i < dim; ++i) {
        uint32_t bits = 0;
        if (!readU32(src, offset, &bits)) return false;
        float v = 0.0f;
        std::memcpy(&v, &bits, sizeof(v));
        vec.push_back(v);
      }
      *out = Value(std::move(vec));
      return true;
    }
  }
}

}  // namespace

PreparedBinaryRowBatch BinaryRowBatchCodec::prepare(const Table& table,
                                                    const BinaryRowBatchOptions& options) const {
  return prepareRange(table, 0, table.rowCount(), options);
}

PreparedBinaryRowBatch BinaryRowBatchCodec::prepareRange(
    const Table& table, size_t row_begin, size_t row_end,
    const BinaryRowBatchOptions& options) const {
  if (row_begin > row_end || row_end > table.rowCount()) {
    throw std::out_of_range("binary row batch row range out of bounds");
  }
  PreparedBinaryRowBatch prepared;
  prepared.projected_columns = resolveProjectedColumns(table, options);
  prepared.encoded_columns =
      buildEncodedColumns(table, prepared.projected_columns, row_begin, row_end);
  prepared.estimated_size = estimateSerializedSizeInternal(table, row_begin, row_end,
                                                           prepared.projected_columns,
                                                           prepared.encoded_columns);
  return prepared;
}

size_t BinaryRowBatchCodec::estimateSerializedSize(const Table& table,
                                                   const BinaryRowBatchOptions& options) const {
  return prepare(table, options).estimated_size;
}

size_t BinaryRowBatchCodec::estimateSerializedSizeRange(const Table& table, size_t row_begin,
                                                        size_t row_end,
                                                        const BinaryRowBatchOptions& options) const {
  return prepareRange(table, row_begin, row_end, options).estimated_size;
}

void BinaryRowBatchCodec::serialize(const Table& table, std::vector<uint8_t>* out,
                                    const BinaryRowBatchOptions& options) const {
  serializeRange(table, 0, table.rowCount(), out, options);
}

void BinaryRowBatchCodec::serializeRange(const Table& table, size_t row_begin, size_t row_end,
                                         std::vector<uint8_t>* out,
                                         const BinaryRowBatchOptions& options) const {
  if (out == nullptr) {
    throw std::invalid_argument("binary row batch serialize output is null");
  }
  if (row_begin > row_end || row_end > table.rowCount()) {
    throw std::out_of_range("binary row batch row range out of bounds");
  }
  const PreparedBinaryRowBatch prepared = prepareRange(table, row_begin, row_end, options);
  serializePreparedRange(table, row_begin, row_end, prepared, out);
}

void BinaryRowBatchCodec::serializePreparedRange(const Table& table, size_t row_begin,
                                                 size_t row_end,
                                                 const PreparedBinaryRowBatch& prepared,
                                                 std::vector<uint8_t>* out) const {
  if (out == nullptr) {
    throw std::invalid_argument("binary row batch serialize output is null");
  }
  if (row_begin > row_end || row_end > table.rowCount()) {
    throw std::out_of_range("binary row batch row range out of bounds");
  }
  out->clear();
  const size_t row_count = row_end - row_begin;
  const size_t estimated = prepared.estimated_size;
  out->reserve(std::max(estimated, row_count * 32 + table.schema.fields.size() * 16));
  out->resize(estimated);
  RawWriter writer{out->data(), out->size(), 0};
  const size_t actual = serializeRangeInternal(table, row_begin, row_end,
                                               prepared.projected_columns,
                                               prepared.encoded_columns, &writer);
  out->resize(actual);
}

size_t BinaryRowBatchCodec::serializeRangeToBuffer(const Table& table, size_t row_begin,
                                                   size_t row_end, uint8_t* out,
                                                   size_t capacity,
                                                   const BinaryRowBatchOptions& options) const {
  const PreparedBinaryRowBatch prepared = prepareRange(table, row_begin, row_end, options);
  return serializePreparedRangeToBuffer(table, row_begin, row_end, prepared, out, capacity);
}

size_t BinaryRowBatchCodec::serializePreparedRangeToBuffer(
    const Table& table, size_t row_begin, size_t row_end,
    const PreparedBinaryRowBatch& prepared, uint8_t* out, size_t capacity) const {
  if (out == nullptr && capacity != 0) {
    throw std::invalid_argument("binary row batch buffer is null");
  }
  if (row_begin > row_end || row_end > table.rowCount()) {
    throw std::out_of_range("binary row batch row range out of bounds");
  }
  const size_t estimated = prepared.estimated_size;
  if (estimated > capacity) {
    throw std::out_of_range("binary row batch buffer capacity too small");
  }
  RawWriter writer{out, capacity, 0};
  return serializeRangeInternal(table, row_begin, row_end, prepared.projected_columns,
                                prepared.encoded_columns, &writer);
}

Table BinaryRowBatchCodec::deserialize(const std::vector<uint8_t>& payload) const {
  return deserializeFromBuffer(payload.data(), payload.size());
}

Table BinaryRowBatchCodec::deserializeFromBuffer(const uint8_t* payload, size_t size) const {
  BufferCursor cursor{payload, size, 0};
  size_t offset = 0;
  if (size < 4 || payload == nullptr || std::memcmp(payload, kMagic, 4) != 0) {
    throw std::runtime_error("binary row batch magic mismatch");
  }
  offset += 4;

  uint32_t column_count = 0;
  uint32_t row_count = 0;
  if (!readU32(cursor, &offset, &column_count) || !readU32(cursor, &offset, &row_count)) {
    throw std::runtime_error("binary row batch header truncated");
  }

  Table table;
  std::vector<PreparedBinaryRowColumn> columns;
  columns.reserve(column_count);
  table.schema.fields.reserve(column_count);
  for (uint32_t i = 0; i < column_count; ++i) {
    std::string name;
    uint8_t type_raw = 0;
    uint8_t encoding = 0;
    if (!readString(cursor, &offset, &name) || !readU8(cursor, &offset, &type_raw) ||
        !readU8(cursor, &offset, &encoding)) {
      throw std::runtime_error("binary row batch schema truncated");
    }
    table.schema.fields.push_back(name);
    table.schema.index[name] = i;
    PreparedBinaryRowColumn column;
    column.column_index = i;
    column.type = static_cast<DataType>(type_raw);
    column.encoding = encoding;
    if (column.type == DataType::String && column.encoding == kEncodingDictionary) {
      uint64_t dict_size = 0;
      if (!readVarint(cursor, &offset, &dict_size)) {
        throw std::runtime_error("binary row batch dictionary truncated");
      }
      column.dictionary.reserve(static_cast<size_t>(dict_size));
      for (uint64_t item = 0; item < dict_size; ++item) {
        std::string value;
        if (!readString(cursor, &offset, &value)) {
          throw std::runtime_error("binary row batch dictionary entry truncated");
        }
        column.dictionary.push_back(std::move(value));
      }
    }
    columns.push_back(std::move(column));
  }

  table.rows.reserve(row_count);
  const size_t null_bytes = (static_cast<size_t>(column_count) + 7) / 8;
  for (uint32_t row_idx = 0; row_idx < row_count; ++row_idx) {
    if (offset + null_bytes > size) {
      throw std::runtime_error("binary row batch null bitmap truncated");
    }
    Row row;
    row.reserve(column_count);
    const uint8_t* null_bitmap = payload + offset;
    offset += null_bytes;
    for (uint32_t col = 0; col < column_count; ++col) {
      const bool is_null = (null_bitmap[col / 8] & static_cast<uint8_t>(1u << (col % 8))) != 0;
      if (is_null) {
        row.emplace_back();
        continue;
      }
      if (columns[col].type == DataType::String && columns[col].encoding == kEncodingDictionary) {
        uint64_t dict_index = 0;
        if (!readVarint(cursor, &offset, &dict_index) ||
            dict_index >= columns[col].dictionary.size()) {
          throw std::runtime_error("binary row batch dictionary index invalid");
        }
        row.emplace_back(columns[col].dictionary[static_cast<size_t>(dict_index)]);
        continue;
      }
      Value value;
      if (!readValue(cursor, &offset, columns[col].type, &value)) {
        throw std::runtime_error("binary row batch value truncated");
      }
      row.push_back(std::move(value));
    }
    table.rows.push_back(std::move(row));
  }
  return table;
}

bool BinaryRowBatchCodec::deserializeWindowKeyValue(const std::vector<uint8_t>& payload,
                                                    WindowKeyValueColumnarBatch* out) const {
  return deserializeWindowKeyValueFromBuffer(payload.data(), payload.size(), out);
}

bool BinaryRowBatchCodec::deserializeWindowKeyValueFromBuffer(const uint8_t* payload, size_t size,
                                                              WindowKeyValueColumnarBatch* out) const {
  if (out == nullptr) return false;
  if (payload == nullptr && size != 0) return false;
  BufferCursor cursor{payload, size, 0};
  size_t offset = 0;
  if (size < 4 || std::memcmp(payload, kMagic, 4) != 0) {
    return false;
  }
  offset += 4;

  uint32_t column_count = 0;
  uint32_t row_count = 0;
  if (!readU32(cursor, &offset, &column_count) || !readU32(cursor, &offset, &row_count)) {
    return false;
  }

  struct ParsedColumn {
    std::string name;
    DataType type = DataType::String;
    uint8_t encoding = kEncodingPlain;
    std::vector<std::string> dictionary;
  };

  std::vector<ParsedColumn> columns;
  columns.reserve(column_count);
  int window_col = -1;
  int key_col = -1;
  int value_col = -1;

  for (uint32_t i = 0; i < column_count; ++i) {
    ParsedColumn column;
    uint8_t type_raw = 0;
    if (!readString(cursor, &offset, &column.name) ||
        !readU8(cursor, &offset, &type_raw) ||
        !readU8(cursor, &offset, &column.encoding)) {
      return false;
    }
    column.type = static_cast<DataType>(type_raw);
    if (column.type == DataType::String && column.encoding == kEncodingDictionary) {
      uint64_t dict_size = 0;
      if (!readVarint(cursor, &offset, &dict_size)) return false;
      column.dictionary.reserve(static_cast<size_t>(dict_size));
      for (uint64_t item = 0; item < dict_size; ++item) {
        std::string_view value;
        if (!readStringView(cursor, &offset, &value)) return false;
        column.dictionary.emplace_back(value);
      }
    }
    if (column.name == "window_start") window_col = static_cast<int>(i);
    if (column.name == "key") key_col = static_cast<int>(i);
    if (column.name == "value") value_col = static_cast<int>(i);
    columns.push_back(std::move(column));
  }

  if (window_col < 0 || key_col < 0 || value_col < 0) {
    return false;
  }

  out->row_count = row_count;
  out->window_start = BinaryStringColumn();
  out->key = BinaryStringColumn();
  out->value = BinaryDoubleColumn();
  out->window_start.is_null.assign(row_count, 0);
  out->key.is_null.assign(row_count, 0);
  out->value.is_null.assign(row_count, 0);
  out->value.values.assign(row_count, 0.0);

  const auto& window_meta = columns[static_cast<size_t>(window_col)];
  const auto& key_meta = columns[static_cast<size_t>(key_col)];
  out->window_start.dictionary_encoded =
      window_meta.type == DataType::String && window_meta.encoding == kEncodingDictionary;
  out->key.dictionary_encoded =
      key_meta.type == DataType::String && key_meta.encoding == kEncodingDictionary;
  if (out->window_start.dictionary_encoded) {
    out->window_start.dictionary.reserve(window_meta.dictionary.size(),
                                         window_meta.dictionary.size() * 16);
    for (const auto& entry : window_meta.dictionary) {
      out->window_start.dictionary.append(entry);
    }
    out->window_start.indices.assign(row_count, 0);
  } else {
    out->window_start.values.reserve(row_count, row_count * 16);
  }
  if (out->key.dictionary_encoded) {
    out->key.dictionary.reserve(key_meta.dictionary.size(), key_meta.dictionary.size() * 16);
    for (const auto& entry : key_meta.dictionary) {
      out->key.dictionary.append(entry);
    }
    out->key.indices.assign(row_count, 0);
  } else {
    out->key.values.reserve(row_count, row_count * 16);
    out->key.values.offsets.assign(row_count + 1, 0);
  }

  const size_t null_bytes = (static_cast<size_t>(column_count) + 7) / 8;
  for (uint32_t row_idx = 0; row_idx < row_count; ++row_idx) {
    if (offset + null_bytes > size) return false;
    const uint8_t* null_bitmap = payload + offset;
    offset += null_bytes;
    for (uint32_t col = 0; col < column_count; ++col) {
      const bool is_null = (null_bitmap[col / 8] & static_cast<uint8_t>(1u << (col % 8))) != 0;
      if (static_cast<int>(col) == window_col) {
        out->window_start.is_null[row_idx] = is_null ? 1 : 0;
        if (is_null && !out->window_start.dictionary_encoded) {
          out->window_start.values.append(std::string());
        }
      }
      if (static_cast<int>(col) == key_col) {
        out->key.is_null[row_idx] = is_null ? 1 : 0;
        if (is_null && !out->key.dictionary_encoded) {
          out->key.values.append(std::string());
        }
      }
      if (static_cast<int>(col) == value_col) {
        out->value.is_null[row_idx] = is_null ? 1 : 0;
      }
      if (is_null) continue;

      const auto& meta = columns[col];
      if (static_cast<int>(col) == window_col) {
        if (out->window_start.dictionary_encoded) {
          uint64_t dict_index = 0;
          if (!readVarint(cursor, &offset, &dict_index) ||
              dict_index >= out->window_start.dictionary.size()) {
            return false;
          }
          out->window_start.indices[row_idx] = static_cast<uint32_t>(dict_index);
        } else {
          std::string_view value;
          if (!readStringView(cursor, &offset, &value)) return false;
          const uint32_t stored = out->window_start.values.append(value);
          if (stored != row_idx) return false;
        }
        continue;
      }

      if (static_cast<int>(col) == key_col) {
        if (out->key.dictionary_encoded) {
          uint64_t dict_index = 0;
          if (!readVarint(cursor, &offset, &dict_index) ||
              dict_index >= out->key.dictionary.size()) {
            return false;
          }
          out->key.indices[row_idx] = static_cast<uint32_t>(dict_index);
        } else {
          std::string_view value;
          if (!readStringView(cursor, &offset, &value)) return false;
          const uint32_t stored = out->key.values.append(value);
          if (stored != row_idx) return false;
        }
        continue;
      }

      if (static_cast<int>(col) == value_col) {
        if (meta.type == DataType::Int64) {
          uint64_t raw = 0;
          if (!readVarint(cursor, &offset, &raw)) return false;
          out->value.values[row_idx] = static_cast<double>(zigZagDecode(raw));
        } else if (meta.type == DataType::Double) {
          uint64_t raw = 0;
          if (!readU64(cursor, &offset, &raw)) return false;
          double d = 0.0;
          std::memcpy(&d, &raw, sizeof(d));
          out->value.values[row_idx] = d;
        } else {
          std::string_view text;
          if (!readStringView(cursor, &offset, &text)) return false;
          out->value.values[row_idx] = std::stod(std::string(text));
        }
        continue;
      }

      Value skipped;
      if (!readValue(cursor, &offset, meta.type, &skipped)) return false;
    }
  }
  return true;
}

std::vector<uint8_t> ByteBufferPool::acquire(size_t min_capacity) {
  if (!cached_.empty()) {
    std::vector<uint8_t> buffer = std::move(cached_.back());
    cached_.pop_back();
    buffer.clear();
    if (buffer.capacity() < min_capacity) {
      buffer.reserve(min_capacity);
    }
    return buffer;
  }
  std::vector<uint8_t> buffer;
  buffer.reserve(min_capacity);
  return buffer;
}

void ByteBufferPool::release(std::vector<uint8_t>&& buffer) {
  buffer.clear();
  if (cached_.size() >= max_cached_) return;
  cached_.push_back(std::move(buffer));
}

}  // namespace dataflow
