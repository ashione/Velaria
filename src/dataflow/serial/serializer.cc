#include "src/dataflow/serial/serializer.h"

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
  (void)payload;
  return Table();
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
  (void)payload;
  return Table();
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
