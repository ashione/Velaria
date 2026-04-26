#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "src/dataflow/core/logical/sql/frontend/sql_diagnostic.h"
#include "src/dataflow/core/logical/sql/frontend/sql_feature_validator.h"
#include "src/dataflow/core/logical/sql/frontend/sql_frontend.h"

namespace dataflow {
namespace sql {

class PgQueryFrontend {
 public:
  PgQueryFrontend();
  ~PgQueryFrontend();

  PgQueryFrontend(const PgQueryFrontend&) = delete;
  PgQueryFrontend& operator=(const PgQueryFrontend&) = delete;

  SqlFrontendResult process(std::string_view sql, const SqlFeaturePolicy& policy);
  std::string version() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace sql
}  // namespace dataflow
