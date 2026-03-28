#pragma once

#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "src/dataflow/api/dataframe.h"
#include "src/dataflow/sql/sql_errors.h"
#include "src/dataflow/core/table.h"

namespace dataflow {

class ViewCatalog {
 public:
  void createView(const std::string& name, const DataFrame& df);
  void createTable(const std::string& name, const std::vector<std::string>& columns);
  bool hasView(const std::string& name) const;
  bool hasTable(const std::string& name) const;
  const DataFrame& getView(const std::string& name) const;
  DataFrame& getViewMutable(const std::string& name);
  void appendToView(const std::string& name, const Table& appendTable);

 private:
  std::unordered_map<std::string, DataFrame> views_;
};

}  // namespace dataflow
