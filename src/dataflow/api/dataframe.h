#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "src/dataflow/core/table.h"
#include "src/dataflow/planner/plan.h"
#include "src/dataflow/runtime/executor.h"

namespace dataflow {

class DataFrame;

class GroupedDataFrame {
 public:
  GroupedDataFrame(PlanNodePtr plan, std::vector<size_t> keys, std::shared_ptr<Executor> exec)
      : plan_(std::move(plan)), keys_(std::move(keys)), executor_(std::move(exec)) {}

  DataFrame sum(const std::string& valueColumn, const std::string& outColumn = "sum") const;

 private:
  PlanNodePtr plan_;
  std::vector<size_t> keys_;
  std::shared_ptr<Executor> executor_;
};

class DataFrame {
 public:
  DataFrame() = default;
  DataFrame(Table table);
  explicit DataFrame(PlanNodePtr plan, std::shared_ptr<Executor> exec = nullptr);

  DataFrame select(const std::vector<std::string>& columns) const;
  DataFrame filter(const std::string& column, const std::string& op, const Value& value) const;
  DataFrame withColumn(const std::string& name, const std::string& sourceColumn) const;
  DataFrame drop(const std::string& column) const;
  DataFrame limit(size_t n) const;
  DataFrame repartition(size_t parts) const;
  DataFrame cache() const;

  GroupedDataFrame groupBy(const std::vector<std::string>& keys) const;
  DataFrame join(const DataFrame& right, const std::string& leftOn, const std::string& rightOn,
                JoinKind kind = JoinKind::Inner) const;

  size_t count() const;
  void show(size_t max_rows = 20) const;
  std::vector<Row> collect() const;

  Table toTable() const;
  const Schema& schema() const;

 private:
  const Table& materialize() const;

  PlanNodePtr plan_;
  std::shared_ptr<Executor> executor_;
  mutable bool cached_ = false;
  mutable Table cached_table_;
};

}  // namespace dataflow
