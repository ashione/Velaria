#include "src/dataflow/core/logical/sql/frontend/pg_query_raii.h"

extern "C" {
#include "pg_query.h"
}

#include <cstdlib>
#include <cstring>

namespace dataflow {
namespace sql {

PgQueryParseResultHolder::PgQueryParseResultHolder(const char* sql) {
  auto* r = static_cast<::PgQueryProtobufParseResult*>(
      std::malloc(sizeof(::PgQueryProtobufParseResult)));
  if (r == nullptr) { raw_ = nullptr; return; }
  *r = pg_query_parse_protobuf(sql);
  raw_ = static_cast<void*>(r);
}

PgQueryParseResultHolder::~PgQueryParseResultHolder() {
  if (raw_ != nullptr) {
    auto* r = static_cast<::PgQueryProtobufParseResult*>(raw_);
    pg_query_free_protobuf_parse_result(*r);
    std::free(r);
    raw_ = nullptr;
  }
}

PgQueryParseResultHolder::PgQueryParseResultHolder(PgQueryParseResultHolder&& other) noexcept
    : raw_(other.raw_) { other.raw_ = nullptr; }

PgQueryParseResultHolder& PgQueryParseResultHolder::operator=(
    PgQueryParseResultHolder&& other) noexcept {
  if (this != &other) {
    if (raw_ != nullptr) {
      auto* r = static_cast<::PgQueryProtobufParseResult*>(raw_);
      pg_query_free_protobuf_parse_result(*r);
      std::free(r);
    }
    raw_ = other.raw_;
    other.raw_ = nullptr;
  }
  return *this;
}

bool PgQueryParseResultHolder::ok() const {
  if (raw_ == nullptr) return false;
  return static_cast<const ::PgQueryProtobufParseResult*>(raw_)->error == nullptr;
}

SqlDiagnostic PgQueryParseResultHolder::diagnostic(std::string_view) const {
  SqlDiagnostic d;
  d.phase = SqlDiagnostic::Phase::Parse;
  d.error_type = "parse_error";
  if (raw_ != nullptr) {
    auto* r = static_cast<const ::PgQueryProtobufParseResult*>(raw_);
    if (r->error != nullptr) {
      d.message = r->error->message ? r->error->message : "unknown parse error";
      d.byte_offset = r->error->cursorpos;
      return d;
    }
  }
  d.message = "unknown parse error";
  return d;
}

const char* PgQueryParseResultHolder::parseTreeData() const {
  if (raw_ == nullptr) return nullptr;
  return static_cast<const ::PgQueryProtobufParseResult*>(raw_)->parse_tree.data;
}

size_t PgQueryParseResultHolder::parseTreeLen() const {
  if (raw_ == nullptr) return 0;
  return static_cast<const ::PgQueryProtobufParseResult*>(raw_)->parse_tree.len;
}

}  // namespace sql
}  // namespace dataflow
