#pragma once

#include "src/dataflow/core/contract/api/dataframe.h"
#include "src/dataflow/core/contract/catalog/catalog.h"
#include "src/dataflow/core/execution/source_materialization.h"
#include "src/dataflow/core/execution/stream/stream.h"
#include "src/dataflow/core/logical/sql/sql_parser.h"
#include "src/dataflow/core/logical/sql/sql_planner.h"
#include <unordered_map>

namespace dataflow {

class DataflowSession {
 public:
  static DataflowSession& builder();

  DataFrame read_csv(const std::string& path, const SourceOptions& options);
  DataFrame read_csv(const std::string& path, char delimiter, const SourceOptions& options);
  DataFrame read_csv(const std::string& path, char delimiter = ',');
  StreamingDataFrame readStream(std::shared_ptr<StreamSource> source);
  StreamingDataFrame readStreamCsvDir(const std::string& directory, char delimiter = ',');
  DataFrame createDataFrame(Table table);
  DataflowJobHandle submitAsync(const DataFrame& df, const ExecutionOptions& options = {});
  DataflowJobHandle submitAsync(const std::string& sql, const ExecutionOptions& options = {});
  Table submit(const DataFrame& df, const ExecutionOptions& options = {});
  Table submit(const std::string& sql, const ExecutionOptions& options = {});

  void createTempView(const std::string& name, const DataFrame& df);
  void createTempView(const std::string& name, const StreamingDataFrame& df);
  void registerStreamSink(const std::string& name, std::shared_ptr<StreamSink> sink);
  DataFrame sql(const std::string& sql);
  std::string explainSql(const std::string& sql);
  DataFrame vectorQuery(const std::string& table, const std::string& vector_column,
                        const std::vector<float>& query_vector, size_t top_k,
                        VectorDistanceMetric metric = VectorDistanceMetric::Cosine);
  std::string explainVectorQuery(const std::string& table, const std::string& vector_column,
                                 const std::vector<float>& query_vector, size_t top_k,
                                 VectorDistanceMetric metric = VectorDistanceMetric::Cosine);
  StreamingDataFrame streamSql(const std::string& sql);
  std::string explainStreamSql(const std::string& sql,
                               const StreamingQueryOptions& options = {});
  StreamingQuery startStreamSql(const std::string& sql,
                                const StreamingQueryOptions& options = {});

 private:
  ViewCatalog catalog_;
  std::unordered_map<std::string, StreamingDataFrame> stream_views_;
  std::unordered_map<std::string, std::shared_ptr<StreamSink>> stream_sinks_;
};

}  // namespace dataflow
