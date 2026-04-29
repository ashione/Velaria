#pragma once

#include "src/dataflow/core/contract/api/dataframe.h"
#include "src/dataflow/core/contract/catalog/catalog.h"
#include "src/dataflow/core/execution/file_source.h"
#include "src/dataflow/core/execution/source_materialization.h"
#include "src/dataflow/core/execution/stream/stream.h"
#include "src/dataflow/core/logical/sql/frontend/sql_frontend.h"
#include "src/dataflow/core/logical/sql/sql_parser.h"
#include "src/dataflow/core/logical/sql/sql_planner.h"
#include <deque>
#include <mutex>
#include <unordered_map>

namespace dataflow {

class DataflowSession {
 public:
  static DataflowSession& builder();

  DataFrame read_csv(const std::string& path, const SourceOptions& options);
  DataFrame read_csv(const std::string& path, char delimiter, const SourceOptions& options);
  DataFrame read_csv(const std::string& path, char delimiter = ',');
  FileSourceProbeResult probe(const std::string& path) const;
  DataFrame read(const std::string& path, const SourceOptions& options);
  DataFrame read(const std::string& path);
  DataFrame read_line_file(const std::string& path, const LineFileOptions& options,
                           const SourceOptions& source_options);
  DataFrame read_line_file(const std::string& path, const LineFileOptions& options);
  DataFrame read_json(const std::string& path, const JsonFileOptions& options,
                      const SourceOptions& source_options);
  DataFrame read_json(const std::string& path, const JsonFileOptions& options);
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
  DataFrame hybridSearch(const std::string& table, const std::string& vector_column,
                         const std::vector<float>& query_vector,
                         const HybridSearchOptions& options = {});
  std::string explainVectorQuery(const std::string& table, const std::string& vector_column,
                                 const std::vector<float>& query_vector, size_t top_k,
                                 VectorDistanceMetric metric = VectorDistanceMetric::Cosine);
  std::string explainHybridSearch(const std::string& table, const std::string& vector_column,
                                  const std::vector<float>& query_vector,
                                  const HybridSearchOptions& options = {});
  StreamingDataFrame streamSql(const std::string& sql);
  std::string explainStreamSql(const std::string& sql,
                               const StreamingQueryOptions& options = {});
  StreamingQuery startStreamSql(const std::string& sql,
                                const StreamingQueryOptions& options = {});

  // Returns the active SQL frontend name (e.g. "legacy", "pg_query", "dual")
  std::string sqlFrontendName() const;

 private:
  ViewCatalog catalog_;
  std::unordered_map<std::string, StreamingDataFrame> stream_views_;
  std::unordered_map<std::string, std::shared_ptr<StreamSink>> stream_sinks_;
  mutable std::mutex legacy_parse_cache_mu_;
  std::deque<std::string> legacy_parse_cache_order_;
  std::unordered_map<std::string, sql::SqlStatement> legacy_parse_cache_;
};

}  // namespace dataflow
