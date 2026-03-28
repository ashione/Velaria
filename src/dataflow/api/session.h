#pragma once

#include "src/dataflow/api/dataframe.h"
#include "src/dataflow/catalog/catalog.h"
#include "src/dataflow/stream/stream.h"
#include "src/dataflow/sql/sql_parser.h"
#include "src/dataflow/sql/sql_planner.h"

namespace dataflow {

class DataflowSession {
 public:
  static DataflowSession& builder();

  DataFrame read_csv(const std::string& path, char delimiter = ',');
  StreamingDataFrame readStream(std::shared_ptr<StreamSource> source);
  StreamingDataFrame readStreamCsvDir(const std::string& directory, char delimiter = ',');
  DataFrame createDataFrame(const Table& table);
  DataflowJobHandle submitAsync(const DataFrame& df, const ExecutionOptions& options = {});
  DataflowJobHandle submitAsync(const std::string& sql, const ExecutionOptions& options = {});
  Table submit(const DataFrame& df, const ExecutionOptions& options = {});
  Table submit(const std::string& sql, const ExecutionOptions& options = {});

  void createTempView(const std::string& name, const DataFrame& df);
  DataFrame sql(const std::string& sql);

 private:
  ViewCatalog catalog_;
};

}  // namespace dataflow
