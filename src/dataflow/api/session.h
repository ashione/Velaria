#pragma once

#include "src/dataflow/api/dataframe.h"
#include "src/dataflow/catalog/catalog.h"
#include "src/dataflow/sql/sql_parser.h"
#include "src/dataflow/sql/sql_planner.h"

namespace dataflow {

class SparkSession {
 public:
  static SparkSession& builder();

  DataFrame read_csv(const std::string& path, char delimiter = ',');
  DataFrame createDataFrame(const Table& table);

  void createTempView(const std::string& name, const DataFrame& df);
  DataFrame sql(const std::string& sql);

 private:
  ViewCatalog catalog_;
};

}  // namespace dataflow
