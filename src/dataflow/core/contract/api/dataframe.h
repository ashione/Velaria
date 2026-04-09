#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "src/dataflow/core/execution/table.h"
#include "src/dataflow/core/logical/planner/plan.h"
#include "src/dataflow/core/execution/runtime/executor.h"
#include "src/dataflow/experimental/runtime/job_master.h"

namespace dataflow {

class DataFrame;
class VectorIndex;
enum class VectorDistanceMetric { Cosine, Dot, L2 };

struct HybridSearchOptions {
  VectorDistanceMetric metric = VectorDistanceMetric::Cosine;
  std::size_t top_k = 10;
  std::optional<double> score_threshold;
};

struct CachedVectorColumn {
  std::shared_ptr<VectorIndex> index;
  std::vector<size_t> row_ids;
};

class GroupedDataFrame {
 public:
  GroupedDataFrame(PlanNodePtr plan, std::vector<size_t> keys, std::shared_ptr<Executor> exec,
                   std::shared_ptr<const Schema> schema_hint = nullptr)
      : plan_(std::move(plan)),
        keys_(std::move(keys)),
        executor_(std::move(exec)),
        schema_hint_(std::move(schema_hint)) {}

  DataFrame sum(const std::string& valueColumn, const std::string& outColumn = "sum") const;

 private:
  PlanNodePtr plan_;
  std::vector<size_t> keys_;
  std::shared_ptr<Executor> executor_;
  std::shared_ptr<const Schema> schema_hint_;
};

class DataFrame {
 public:
  DataFrame() = default;
  DataFrame(Table table);
  explicit DataFrame(PlanNodePtr plan, std::shared_ptr<Executor> exec = nullptr,
                     std::shared_ptr<const Schema> schema_hint = nullptr);

  DataFrame select(const std::vector<std::string>& columns) const;
  DataFrame selectByIndices(const std::vector<size_t>& columns,
                           const std::vector<std::string>& aliases = {}) const;
  DataFrame filter(const std::string& column, const std::string& op, const Value& value) const;
  DataFrame filterByIndex(size_t columnIndex, const std::string& op, const Value& value) const;
  DataFrame withColumn(const std::string& name, const std::string& sourceColumn) const;
  DataFrame withColumn(const std::string& name, ComputedColumnKind function,
                      const std::vector<ComputedColumnArg>& args) const;
  DataFrame drop(const std::string& column) const;
  DataFrame orderBy(const std::vector<std::string>& columns,
                    const std::vector<bool>& ascending = {}) const;
  DataFrame limit(size_t n) const;
  DataFrame repartition(size_t parts) const;
  DataFrame cache() const;
  DataFrame aggregate(const std::vector<size_t>& keys,
                     const std::vector<AggregateSpec>& aggs) const;
  DataFrame aggregate(const std::vector<size_t>& keys,
                     const std::vector<AggregateSpec>& aggs,
                     const AggregateExecSpec& exec_spec) const;
  DataFrame vectorQuery(const std::string& vectorColumn,
                        const std::vector<float>& queryVector,
                        size_t top_k,
                        VectorDistanceMetric metric = VectorDistanceMetric::Cosine) const;
  DataFrame hybridSearch(const std::string& vectorColumn,
                         const std::vector<float>& queryVector,
                         const HybridSearchOptions& options = {}) const;
  std::string explainVectorQuery(const std::string& vectorColumn,
                                 const std::vector<float>& queryVector,
                                 size_t top_k,
                                 VectorDistanceMetric metric = VectorDistanceMetric::Cosine) const;
  std::string explainHybridSearch(const std::string& vectorColumn,
                                  const std::vector<float>& queryVector,
                                  const HybridSearchOptions& options = {}) const;

  GroupedDataFrame groupBy(const std::vector<std::string>& keys) const;
  DataFrame join(const DataFrame& right, const std::string& leftOn, const std::string& rightOn,
                JoinKind kind = JoinKind::Inner) const;

  size_t count() const;
  void show(size_t max_rows = 20) const;
  std::vector<Row> collect() const;
  std::string serializePlan() const;

  DataflowJobHandle submitAsync(const ExecutionOptions& options = {}) const;
  std::string explain() const;

  Table toTable() const;
  const Table& materializedTable() const;
  const Schema& schema() const;

 private:
  const Table& materialize() const;
  const CachedVectorColumn& vectorColumnCache(const std::string& vectorColumn) const;

  PlanNodePtr plan_;
  std::shared_ptr<Executor> executor_;
  mutable std::shared_ptr<const Schema> schema_hint_;
  mutable bool cached_ = false;
  mutable Table cached_table_;
  mutable std::unordered_map<size_t, CachedVectorColumn> vector_cache_;
};

}  // namespace dataflow
