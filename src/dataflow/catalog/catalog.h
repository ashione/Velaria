#pragma once

#include <string>
#include <unordered_map>

#include "src/dataflow/api/dataframe.h"
#include "src/dataflow/sql/sql_errors.h"

namespace dataflow {

class ViewCatalog {
 public:
  void createView(const std::string& name, const DataFrame& df);
  bool hasView(const std::string& name) const;
  const DataFrame& getView(const std::string& name) const;

 private:
  std::unordered_map<std::string, DataFrame> views_;
};

}  // namespace dataflow
