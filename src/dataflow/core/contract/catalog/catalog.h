#pragma once

#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "src/dataflow/core/contract/api/dataframe.h"
#include "src/dataflow/core/logical/sql/sql_errors.h"
#include "src/dataflow/core/execution/table.h"
#include "src/dataflow/core/logical/sql/sql_ast.h"

namespace dataflow {

class ViewCatalog {
 public:
  void createView(const std::string& name, const DataFrame& df,
                  sql::TableKind kind = sql::TableKind::Regular);
  void createTable(const std::string& name, const std::vector<std::string>& columns,
                  sql::TableKind kind = sql::TableKind::Regular);
  void createTable(const std::string& name, const std::vector<std::string>& columns,
                  const std::vector<std::string>& col_types, sql::TableKind kind);
  sql::TableKind tableKind(const std::string& name) const;
  std::vector<std::string> getColumnTypes(const std::string& name) const;
  bool isSourceTable(const std::string& name) const;
  bool isSinkTable(const std::string& name) const;
  bool hasView(const std::string& name) const;
  bool hasTable(const std::string& name) const;
  const DataFrame& getView(const std::string& name) const;
  DataFrame& getViewMutable(const std::string& name);
  void appendToView(const std::string& name, const Table& appendTable);

 private:
  std::unordered_map<std::string, DataFrame> views_;
  std::unordered_map<std::string, sql::TableKind> table_kinds_;
  std::unordered_map<std::string, std::vector<std::string>> column_types_;
};

}  // namespace dataflow
