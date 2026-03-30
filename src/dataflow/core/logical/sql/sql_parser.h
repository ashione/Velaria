#pragma once

#include <string>
#include <vector>

#include "src/dataflow/core/logical/sql/sql_ast.h"
#include "src/dataflow/core/logical/sql/sql_errors.h"

namespace dataflow {
namespace sql {

class SqlParser {
 public:
  static SqlStatement parse(const std::string& sql);
};

}  // namespace sql
}  // namespace dataflow
