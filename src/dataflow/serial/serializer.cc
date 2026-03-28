#include "src/dataflow/serial/serializer.h"

#include <cstddef>
#include <cstdint>
#include <sstream>

namespace dataflow {

std::string ProtoLikeSerializer::name() const { return "proto-like"; }

std::string ProtoLikeSerializer::serialize(const Table& table) const {
  std::ostringstream out;
  out << table.schema.fields.size() << '\n';
  for (const auto& field : table.schema.fields) {
    out << field << '\n';
  }
  out << table.rows.size() << '\n';
  for (const auto& row : table.rows) {
    out << row.size() << ':';
    for (size_t i = 0; i < row.size(); ++i) {
      const auto& v = row[i];
      out << static_cast<int>(v.type()) << '|' << v.toString();
      if (i + 1 < row.size()) out << ',';
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
    const auto colon = token.find(':');
    if (colon == std::string::npos) return Table();
    const size_t itemCount = static_cast<size_t>(std::stoull(token.substr(0, colon)));
    std::string payloadRest = token.substr(colon + 1);

    Row row;
    row.reserve(itemCount);
    size_t offset = 0;
    for (size_t i = 0; i < itemCount; ++i) {
      size_t sep = payloadRest.find(',', offset);
      std::string item = (sep == std::string::npos) ? payloadRest.substr(offset)
                                                   : payloadRest.substr(offset, sep - offset);
      offset = (sep == std::string::npos) ? payloadRest.size() : (sep + 1);

      const size_t bar = item.find('|');
      if (bar == std::string::npos) return Table();
      const auto typeCode = static_cast<DataType>(std::stoi(item.substr(0, bar)));
      const std::string value = item.substr(bar + 1);

      if (typeCode == DataType::Nil) {
        row.emplace_back();
      } else if (typeCode == DataType::Int64) {
        row.emplace_back(static_cast<int64_t>(std::stoll(value)));
      } else if (typeCode == DataType::Double) {
        row.emplace_back(std::stod(value));
      } else {
        row.emplace_back(Value(value));
      }
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
  // Placeholder for future Arrow IPC. Keep output shape stable and self-describing.
  std::ostringstream out;
  out << "ARROW-LIKE-V0\n";
  out << table.schema.fields.size() << '\n';
  for (const auto& field : table.schema.fields) {
    out << field << '\n';
  }
  out << table.rows.size() << '\n';
  for (const auto& row : table.rows) {
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
      return std::unique_ptr<ISerializer>(new ProtoLikeSerializer());
    case SerializationKind::ArrowLike:
    default:
      return std::unique_ptr<ISerializer>(new ArrowLikeSerializer());
  }
}

}  // namespace dataflow
