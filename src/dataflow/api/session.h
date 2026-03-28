#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "src/dataflow/api/dataframe.h"

namespace dataflow {

class SparkSession {
 public:
  static SparkSession& builder();

  DataFrame read_csv(const std::string& path, char delimiter = ',');
  DataFrame createDataFrame(const Table& table);

  void createTempView(const std::string& name, const DataFrame& df);
  DataFrame sql(const std::string& sql);

 private:
  std::unordered_map<std::string, DataFrame> tempViews_;
};

}  // namespace dataflow
