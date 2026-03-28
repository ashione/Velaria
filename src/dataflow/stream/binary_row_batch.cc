#include "src/dataflow/stream/binary_row_batch.h"

#include <cstring>
#include <stdexcept>
#include <unordered_map>

namespace dataflow {

namespace {

constexpr uint8_t kMagic[4] = {'B', 'R', 'B', '1'};
constexpr uint8_t kEncodingPlain = 0;
constexpr uint8_t kEncodingDictionary = 1;

void appendU8(std::vector<uint8_t>* out, uint8_t value) { out->push_back(value); }

void appendU32(std::vector<uint8_t>* out, uint32_t value) {
  out->push_back(static_cast<uint8_t>(value & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 16) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 24) & 0xFF));
}

void appendU64(std::vector<uint8_t>* out, uint64_t value) {
  out->push_back(static_cast<uint8_t>(value & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 16) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 24) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 32) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 40) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 48) & 0xFF));
  out->push_back(static_cast<uint8_t>((value >> 56) & 0xFF));
}

bool readU8(const std::vector<uint8_t>& src, size_t* offset, uint8_t* out) {
  if (offset == nullptr || out == nullptr || *offset >= src.size()) return false;
  *out = src[*offset];
  ++(*offset);
  return true;
}

bool readU32(const std::vector<uint8_t>& src, size_t* offset, uint32_t* out) {
  if (offset == nullptr || out == nullptr || *offset + 4 > src.size()) return false;
  *out = static_cast<uint32_t>(src[*offset]) |
         (static_cast<uint32_t>(src[*offset + 1]) << 8) |
         (static_cast<uint32_t>(src[*offset + 2]) << 16) |
         (static_cast<uint32_t>(src[*offset + 3]) << 24);
  *offset += 4;
  return true;
}

bool readU64(const std::vector<uint8_t>& src, size_t* offset, uint64_t* out) {
  if (offset == nullptr || out == nullptr || *offset + 8 > src.size()) return false;
  *out = static_cast<uint64_t>(src[*offset]) |
         (static_cast<uint64_t>(src[*offset + 1]) << 8) |
         (static_cast<uint64_t>(src[*offset + 2]) << 16) |
         (static_cast<uint64_t>(src[*offset + 3]) << 24) |
         (static_cast<uint64_t>(src[*offset + 4]) << 32) |
         (static_cast<uint64_t>(src[*offset + 5]) << 40) |
         (static_cast<uint64_t>(src[*offset + 6]) << 48) |
         (static_cast<uint64_t>(src[*offset + 7]) << 56);
  *offset += 8;
  return true;
}

void appendVarint(std::vector<uint8_t>* out, uint64_t value) {
  while (value >= 0x80) {
    out->push_back(static_cast<uint8_t>(value) | 0x80);
    value >>= 7;
  }
  out->push_back(static_cast<uint8_t>(value));
}

bool readVarint(const std::vector<uint8_t>& src, size_t* offset, uint64_t* out) {
  if (offset == nullptr || out == nullptr) return false;
  uint64_t value = 0;
  uint32_t shift = 0;
  while (*offset < src.size() && shift <= 63) {
    const uint8_t byte = src[*offset];
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

void appendString(std::vector<uint8_t>* out, const std::string& value) {
  appendVarint(out, value.size());
  out->insert(out->end(), value.begin(), value.end());
}

bool readString(const std::vector<uint8_t>& src, size_t* offset, std::string* out) {
  if (offset == nullptr || out == nullptr) return false;
  uint64_t size = 0;
  if (!readVarint(src, offset, &size)) return false;
  if (*offset + size > src.size()) return false;
  out->assign(reinterpret_cast<const char*>(src.data() + *offset), static_cast<size_t>(size));
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

DataType inferColumnType(const Table& table, size_t column_index) {
  DataType inferred = DataType::Nil;
  for (const auto& row : table.rows) {
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

struct EncodedColumn {
  size_t column_index = 0;
  DataType type = DataType::String;
  uint8_t encoding = kEncodingPlain;
  std::vector<std::string> dictionary;
  std::unordered_map<std::string, uint32_t> dictionary_index;
};

EncodedColumn buildEncodedColumn(const Table& table, size_t column_index) {
  EncodedColumn encoded;
  encoded.column_index = column_index;
  encoded.type = inferColumnType(table, column_index);
  if (encoded.type != DataType::String) {
    return encoded;
  }

  std::unordered_map<std::string, uint32_t> counts;
  size_t non_null_rows = 0;
  for (const auto& row : table.rows) {
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

void appendValue(std::vector<uint8_t>* out, const Value& value, DataType type) {
  switch (type) {
    case DataType::Int64:
      appendVarint(out, zigZagEncode(value.asInt64()));
      break;
    case DataType::Double: {
      const double d = value.asDouble();
      uint64_t raw = 0;
      std::memcpy(&raw, &d, sizeof(raw));
      appendU64(out, raw);
      break;
    }
    case DataType::String:
    case DataType::Nil:
    default:
      appendString(out, value.toString());
      break;
  }
}

bool readValue(const std::vector<uint8_t>& src, size_t* offset, DataType type, Value* out) {
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
  }
}

}  // namespace

void BinaryRowBatchCodec::serialize(const Table& table, std::vector<uint8_t>* out,
                                    const BinaryRowBatchOptions& options) const {
  if (out == nullptr) {
    throw std::invalid_argument("binary row batch serialize output is null");
  }
  out->clear();
  out->reserve(table.rowCount() * 32 + table.schema.fields.size() * 16);
  out->insert(out->end(), kMagic, kMagic + 4);

  const std::vector<size_t> projected = resolveProjectedColumns(table, options);
  appendU32(out, static_cast<uint32_t>(projected.size()));
  appendU32(out, static_cast<uint32_t>(table.rowCount()));

  std::vector<EncodedColumn> columns;
  columns.reserve(projected.size());
  for (size_t column_index : projected) {
    EncodedColumn column = buildEncodedColumn(table, column_index);
    columns.push_back(column);
    appendString(out, table.schema.fields[column_index]);
    appendU8(out, static_cast<uint8_t>(column.type));
    appendU8(out, column.encoding);
    if (column.type == DataType::String && column.encoding == kEncodingDictionary) {
      appendVarint(out, column.dictionary.size());
      for (const auto& item : column.dictionary) {
        appendString(out, item);
      }
    }
  }

  const size_t null_bytes = (projected.size() + 7) / 8;
  std::vector<uint8_t> null_bitmap(null_bytes, 0);
  for (const auto& row : table.rows) {
    std::fill(null_bitmap.begin(), null_bitmap.end(), 0);
    for (size_t i = 0; i < projected.size(); ++i) {
      const size_t column_index = projected[i];
      if (column_index >= row.size() || row[column_index].isNull()) {
        null_bitmap[i / 8] |= static_cast<uint8_t>(1u << (i % 8));
      }
    }
    out->insert(out->end(), null_bitmap.begin(), null_bitmap.end());
    for (size_t i = 0; i < projected.size(); ++i) {
      const size_t column_index = projected[i];
      if (column_index >= row.size() || row[column_index].isNull()) continue;
      if (columns[i].type == DataType::String && columns[i].encoding == kEncodingDictionary) {
        appendVarint(out, columns[i].dictionary_index.at(row[column_index].toString()));
        continue;
      }
      appendValue(out, row[column_index], columns[i].type);
    }
  }
}

Table BinaryRowBatchCodec::deserialize(const std::vector<uint8_t>& payload) const {
  size_t offset = 0;
  if (payload.size() < 4 || std::memcmp(payload.data(), kMagic, 4) != 0) {
    throw std::runtime_error("binary row batch magic mismatch");
  }
  offset += 4;

  uint32_t column_count = 0;
  uint32_t row_count = 0;
  if (!readU32(payload, &offset, &column_count) || !readU32(payload, &offset, &row_count)) {
    throw std::runtime_error("binary row batch header truncated");
  }

  Table table;
  std::vector<EncodedColumn> columns;
  columns.reserve(column_count);
  table.schema.fields.reserve(column_count);
  for (uint32_t i = 0; i < column_count; ++i) {
    std::string name;
    uint8_t type_raw = 0;
    uint8_t encoding = 0;
    if (!readString(payload, &offset, &name) || !readU8(payload, &offset, &type_raw) ||
        !readU8(payload, &offset, &encoding)) {
      throw std::runtime_error("binary row batch schema truncated");
    }
    table.schema.fields.push_back(name);
    table.schema.index[name] = i;
    EncodedColumn column;
    column.column_index = i;
    column.type = static_cast<DataType>(type_raw);
    column.encoding = encoding;
    if (column.type == DataType::String && column.encoding == kEncodingDictionary) {
      uint64_t dict_size = 0;
      if (!readVarint(payload, &offset, &dict_size)) {
        throw std::runtime_error("binary row batch dictionary truncated");
      }
      column.dictionary.reserve(static_cast<size_t>(dict_size));
      for (uint64_t item = 0; item < dict_size; ++item) {
        std::string value;
        if (!readString(payload, &offset, &value)) {
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
    if (offset + null_bytes > payload.size()) {
      throw std::runtime_error("binary row batch null bitmap truncated");
    }
    Row row;
    row.reserve(column_count);
    const uint8_t* null_bitmap = payload.data() + offset;
    offset += null_bytes;
    for (uint32_t col = 0; col < column_count; ++col) {
      const bool is_null = (null_bitmap[col / 8] & static_cast<uint8_t>(1u << (col % 8))) != 0;
      if (is_null) {
        row.emplace_back();
        continue;
      }
      if (columns[col].type == DataType::String && columns[col].encoding == kEncodingDictionary) {
        uint64_t dict_index = 0;
        if (!readVarint(payload, &offset, &dict_index) ||
            dict_index >= columns[col].dictionary.size()) {
          throw std::runtime_error("binary row batch dictionary index invalid");
        }
        row.emplace_back(columns[col].dictionary[static_cast<size_t>(dict_index)]);
        continue;
      }
      Value value;
      if (!readValue(payload, &offset, columns[col].type, &value)) {
        throw std::runtime_error("binary row batch value truncated");
      }
      row.push_back(std::move(value));
    }
    table.rows.push_back(std::move(row));
  }
  return table;
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
