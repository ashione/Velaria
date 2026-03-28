#include "src/dataflow/api/dataframe.h"

#include <algorithm>
#include <iostream>
#include <stdexcept>

namespace dataflow {

namespace {

bool eqPred(const Value& lhs, const Value& rhs) { return lhs == rhs; }
bool nePred(const Value& lhs, const Value& rhs) { return lhs != rhs; }
bool ltPred(const Value& lhs, const Value& rhs) { return lhs < rhs; }
bool gtPred(const Value& lhs, const Value& rhs) { return lhs > rhs; }
bool ltePred(const Value& lhs, const Value& rhs) { return lhs < rhs || lhs == rhs; }
bool gtePred(const Value& lhs, const Value& rhs) { return lhs > rhs || lhs == rhs; }

std::shared_ptr<Executor> defaultExecutor() { return std::make_shared<LocalExecutor>(); }

}  // namespace

DataFrame::DataFrame(Table table)
    : plan_(std::make_shared<SourcePlan>("memory", std::move(table))),
      executor_(defaultExecutor()) {}

DataFrame::DataFrame(PlanNodePtr plan, std::shared_ptr<Executor> exec)
    : plan_(std::move(plan)), executor_(std::move(exec)) {
  if (!executor_) executor_ = defaultExecutor();
}

const Table& DataFrame::materialize() const {
  if (!cached_) {
    cached_table_ = executor_->execute(plan_);
    cached_ = true;
  }
  return cached_table_;
}

DataFrame DataFrame::select(const std::vector<std::string>& columns) const {
  const auto source = materialize();
  std::vector<size_t> idxs;
  idxs.reserve(columns.size());
  for (const auto& c : columns) idxs.push_back(source.schema.indexOf(c));

  auto node = std::make_shared<SelectPlan>(plan_, idxs);
  return DataFrame(node, executor_);
}

DataFrame DataFrame::filter(const std::string& column, const std::string& op, const Value& value) const {
  const auto source = materialize();
  bool (*pred)(const Value&, const Value&) = &eqPred;
  if (op == "==" || op == "=") {
    pred = &eqPred;
  } else if (op == "!=") {
    pred = &nePred;
  } else if (op == "<") {
    pred = &ltPred;
  } else if (op == ">") {
    pred = &gtPred;
  } else if (op == "<=") {
    pred = &ltePred;
  } else if (op == ">=") {
    pred = &gtePred;
  } else {
    throw std::invalid_argument("unsupported filter op: " + op);
  }

  auto node = std::make_shared<FilterPlan>(plan_, source.schema.indexOf(column), value, pred);
  return DataFrame(node, executor_);
}

DataFrame DataFrame::withColumn(const std::string& name, const std::string& sourceColumn) const {
  const auto source = materialize();
  auto node = std::make_shared<WithColumnPlan>(plan_, name, source.schema.indexOf(sourceColumn));
  return DataFrame(node, executor_);
}

DataFrame DataFrame::drop(const std::string& column) const {
  const auto source = materialize();
  std::vector<size_t> keep;
  for (size_t i = 0; i < source.schema.fields.size(); ++i) {
    if (source.schema.fields[i] != column) keep.push_back(i);
  }
  auto node = std::make_shared<DropPlan>(plan_, keep);
  return DataFrame(node, executor_);
}

DataFrame DataFrame::limit(size_t n) const {
  auto node = std::make_shared<LimitPlan>(plan_, n);
  return DataFrame(node, executor_);
}

DataFrame DataFrame::repartition(size_t parts) const {
  (void)parts;  // no-op in v0.1
  return *this;
}

DataFrame DataFrame::cache() const {
  // materialize eagerly now to simulate cache for v0.1
  materialize();
  return *this;
}

GroupedDataFrame DataFrame::groupBy(const std::vector<std::string>& keys) const {
  const auto source = materialize();
  std::vector<size_t> idxs;
  idxs.reserve(keys.size());
  for (const auto& k : keys) idxs.push_back(source.schema.indexOf(k));
  return GroupedDataFrame(plan_, idxs, executor_);
}

DataFrame DataFrame::join(const DataFrame& right, const std::string& leftOn, const std::string& rightOn,
                         JoinKind kind) const {
  const auto leftSchema = materialize().schema;
  const auto rightSchema = right.materialize().schema;
  auto node = std::make_shared<JoinPlan>(plan_, right.plan_, leftSchema.indexOf(leftOn),
                                         rightSchema.indexOf(rightOn), kind);
  return DataFrame(node, executor_);
}

size_t DataFrame::count() const {
  return materialize().rowCount();
}

std::vector<Row> DataFrame::collect() const {
  return materialize().rows;
}

void DataFrame::show(size_t max_rows) const {
  const auto t = materialize();
  for (size_t i = 0; i < t.schema.fields.size(); ++i) {
    if (i) std::cout << "\t";
    std::cout << t.schema.fields[i];
  }
  std::cout << "\n";
  for (size_t i = 0; i < t.rows.size() && i < max_rows; ++i) {
    for (size_t j = 0; j < t.rows[i].size(); ++j) {
      if (j) std::cout << "\t";
      std::cout << t.rows[i][j].toString();
    }
    std::cout << "\n";
  }
}

Table DataFrame::toTable() const {
  return materialize();
}

const Schema& DataFrame::schema() const {
  return materialize().schema;
}

DataFrame GroupedDataFrame::sum(const std::string& valueColumn, const std::string&) const {
  const auto source = executor_->execute(plan_);
  auto node = std::make_shared<GroupBySumPlan>(plan_, keys_, source.schema.indexOf(valueColumn));
  return DataFrame(node, executor_);
}

}  // namespace dataflow
