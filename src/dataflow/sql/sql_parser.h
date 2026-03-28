#pragma once

#include <string>

#include "src/dataflow/sql/sql_ast.h"
#include "src/dataflow/sql/sql_errors.h"

namespace dataflow {
namespace sql {

class SqlParser {
 public:
  static SqlQuery parse(const std::string& sql);
};

}  // namespace sql
}  // namespace dataflow
