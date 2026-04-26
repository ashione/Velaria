#pragma once

#include <string>
#include <string_view>

#include "src/dataflow/core/logical/sql/frontend/sql_diagnostic.h"

namespace dataflow {
namespace sql {

// Opaque RAII holder for libpg_query protobuf parse result.
// pg_query.h is included ONLY in pg_query_raii.cc — never in a public header.
// Uses pg_query_parse_protobuf() to get length info needed for protobuf-c.
class PgQueryParseResultHolder {
 public:
  explicit PgQueryParseResultHolder(const char* sql);
  ~PgQueryParseResultHolder();

  PgQueryParseResultHolder(const PgQueryParseResultHolder&) = delete;
  PgQueryParseResultHolder& operator=(const PgQueryParseResultHolder&) = delete;

  PgQueryParseResultHolder(PgQueryParseResultHolder&& other) noexcept;
  PgQueryParseResultHolder& operator=(PgQueryParseResultHolder&& other) noexcept;

  bool ok() const;
  SqlDiagnostic diagnostic(std::string_view sql) const;

  // Internal: access protobuf parse tree data and length for protobuf-c unpacking.
  // Returns nullptr if parse failed.
  const char* parseTreeData() const;
  size_t parseTreeLen() const;

 private:
  void* raw_ = nullptr;  // actually PgQueryProtobufParseResult*, cast in .cc
};

}  // namespace sql
}  // namespace dataflow
