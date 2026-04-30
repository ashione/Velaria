#include "src/dataflow/core/execution/serial/serializer.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <sstream>

namespace dataflow {

namespace {

const Table& tableWithRows(const Table& table, std::unique_ptr<Table>* materialized) {
  if (!table.rows.empty() || table.rowCount() == 0) {
    return table;
  }
  *materialized = std::make_unique<Table>(table);
  materializeRows(materialized->get());
  return **materialized;
}

std::string encodeValuePayload(const Value& value) {
  std::ostringstream out;
  switch (value.type()) {
    case DataType::Nil:
      return "";
    case DataType::Bool:
      return value.asBool() ? "1" : "0";
    case DataType::Int64:
      return std::to_string(value.asInt64());
    case DataType::Double:
      out.precision(std::numeric_limits<double>::max_digits10);
      out << value.asDouble();
      return out.str();
    case DataType::String:
      return value.asString();
    case DataType::FixedVector: {
      const auto& vec = value.asFixedVector();
      out << vec.size();
      for (float v : vec) {
        uint32_t bits = 0;
        std::memcpy(&bits, &v, sizeof(bits));
        out << ";" << bits;
      }
      return out.str();
    }
  }
  return "";
}

Value decodeValuePayload(DataType type, const std::string& payload) {
  switch (type) {
    case DataType::Nil:
      return Value();
    case DataType::Bool:
      return Value(payload == "1");
    case DataType::Int64:
      return Value(static_cast<int64_t>(std::stoll(payload)));
    case DataType::Double:
      return Value(std::stod(payload));
    case DataType::String:
      return Value(payload);
    case DataType::FixedVector: {
      std::vector<float> vec;
      std::stringstream ss(payload);
      std::string token;
      if (!std::getline(ss, token, ';')) return Value(vec);
      const std::size_t n = static_cast<std::size_t>(std::stoull(token));
      vec.reserve(n);
      for (std::size_t i = 0; i < n; ++i) {
        if (!std::getline(ss, token, ';')) {
          throw std::runtime_error("invalid fixed vector payload");
        }
        const uint32_t bits = static_cast<uint32_t>(std::stoul(token));
        float v = 0.0f;
        std::memcpy(&v, &bits, sizeof(v));
        vec.push_back(v);
      }
      return Value(std::move(vec));
    }
  }
  return Value();
}

}  // namespace

std::string ProtoLikeSerializer::name() const { return "proto-like"; }

std::string ProtoLikeSerializer::serialize(const Table& table) const {
  std::unique_ptr<Table> materialized;
  const Table& input = tableWithRows(table, &materialized);
  std::ostringstream out;
  out << input.schema.fields.size() << '\n';
  for (const auto& field : input.schema.fields) {
    out << field << '\n';
  }
  out << input.rows.size() << '\n';
  for (const auto& row : input.rows) {
    out << row.size();
    for (size_t i = 0; i < row.size(); ++i) {
      const auto& v = row[i];
      const std::string payload = encodeValuePayload(v);
      out << "|" << static_cast<int>(v.type()) << ":" << payload.size() << ":" << payload;
    }
    out << '\n';
  }
  return out.str();
}

Table ProtoLikeSerializer::deserialize(const std::string& payload) const {
  std::istringstream in(payload);
  std::string token;
  Table table;

  if (!std::getline(in, token)) return table;
  size_t fieldCount = static_cast<size_t>(std::stoull(token));
  table.schema.fields.reserve(fieldCount);
  for (size_t i = 0; i < fieldCount; ++i) {
    std::string field;
    if (!std::getline(in, field)) return Table();
    table.schema.fields.push_back(field);
  }

  if (!std::getline(in, token)) return Table();
  size_t rowCount = static_cast<size_t>(std::stoull(token));
  table.rows.reserve(rowCount);

  for (size_t r = 0; r < rowCount; ++r) {
    if (!std::getline(in, token)) return Table();
    const auto first_sep = token.find('|');
    const size_t itemCount = static_cast<size_t>(std::stoull(first_sep == std::string::npos
                                                                 ? token
                                                                 : token.substr(0, first_sep)));

    Row row;
    row.reserve(itemCount);
    size_t offset = first_sep == std::string::npos ? token.size() : first_sep + 1;
    for (size_t i = 0; i < itemCount; ++i) {
      const size_t type_sep = token.find(':', offset);
      if (type_sep == std::string::npos) return Table();
      const auto typeCode = static_cast<DataType>(std::stoi(token.substr(offset, type_sep - offset)));
      const size_t len_sep = token.find(':', type_sep + 1);
      if (len_sep == std::string::npos) return Table();
      const size_t payload_len =
          static_cast<size_t>(std::stoull(token.substr(type_sep + 1, len_sep - type_sep - 1)));
      const size_t payload_begin = len_sep + 1;
      const size_t payload_end = payload_begin + payload_len;
      if (payload_end > token.size()) return Table();
      const std::string payload = token.substr(payload_begin, payload_len);
      row.emplace_back(decodeValuePayload(typeCode, payload));
      offset = payload_end;
      if (offset < token.size() && token[offset] == '|') ++offset;
    }
    table.rows.push_back(std::move(row));
  }
  for (size_t i = 0; i < table.schema.fields.size(); ++i) {
    table.schema.index[table.schema.fields[i]] = i;
  }
  return table;
}

std::string ArrowLikeSerializer::name() const { return "arrow-like"; }

std::string ArrowLikeSerializer::serialize(const Table& table) const {
  std::unique_ptr<Table> materialized;
  const Table& input = tableWithRows(table, &materialized);
  // Placeholder for future Arrow IPC. Keep output shape stable and self-describing.
  std::ostringstream out;
  out << "ARROW-LIKE-V0\n";
  out << input.schema.fields.size() << '\n';
  for (const auto& field : input.schema.fields) {
    out << field << '\n';
  }
  out << input.rows.size() << '\n';
  for (const auto& row : input.rows) {
    for (size_t i = 0; i < row.size(); ++i) {
      if (i > 0) out << ',';
      out << row[i].toString();
    }
    out << '\n';
  }
  return out.str();
}

Table ArrowLikeSerializer::deserialize(const std::string& payload) const {
  return ProtoLikeSerializer().deserialize(payload);
}

std::unique_ptr<ISerializer> makeSerializer(SerializationKind kind) {
  switch (kind) {
    case SerializationKind::ProtoLike:
      return std::make_unique<ProtoLikeSerializer>();
    case SerializationKind::ArrowLike:
    default:
      return std::make_unique<ArrowLikeSerializer>();
  }
}

}  // namespace dataflow
