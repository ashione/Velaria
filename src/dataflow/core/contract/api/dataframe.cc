#include "src/dataflow/core/contract/api/dataframe.h"

#include <algorithm>
#include <iostream>
#include <stdexcept>
#include <cstdint>
#include <sstream>

#include "src/dataflow/ai/plugin_runtime.h"
#include "src/dataflow/core/execution/runtime/vector_index.h"

namespace dataflow {

namespace {

bool eqPred(const Value& lhs, const Value& rhs) { return lhs == rhs; }
bool nePred(const Value& lhs, const Value& rhs) { return lhs != rhs; }
bool ltPred(const Value& lhs, const Value& rhs) { return lhs < rhs; }
bool gtPred(const Value& lhs, const Value& rhs) { return lhs > rhs; }
bool ltePred(const Value& lhs, const Value& rhs) { return lhs < rhs || lhs == rhs; }
bool gtePred(const Value& lhs, const Value& rhs) { return lhs > rhs || lhs == rhs; }

std::shared_ptr<Executor> defaultExecutor() { return std::make_shared<LocalExecutor>(); }

VectorSearchMetric toRuntimeMetric(VectorDistanceMetric metric) {
  return metric == VectorDistanceMetric::L2
             ? VectorSearchMetric::L2
             : (metric == VectorDistanceMetric::Dot ? VectorSearchMetric::Dot
                                                    : VectorSearchMetric::Cosine);
}

std::string planKindName(PlanKind kind) {
  switch (kind) {
    case PlanKind::Source:
      return "Source";
    case PlanKind::Select:
      return "Select";
    case PlanKind::Filter:
      return "Filter";
    case PlanKind::WithColumn:
      return "WithColumn";
    case PlanKind::Drop:
      return "Drop";
    case PlanKind::Limit:
      return "Limit";
    case PlanKind::WindowAssign:
      return "WindowAssign";
    case PlanKind::GroupBySum:
      return "GroupBySum";
    case PlanKind::Aggregate:
      return "Aggregate";
    case PlanKind::Join:
      return "Join";
    case PlanKind::Sink:
      return "Sink";
    default:
      return "Unknown";
  }
}

void explainPlan(const PlanNodePtr& node, std::ostringstream& out, int depth = 0) {
  if (!node) {
    out << std::string(depth * 2, ' ') << "null";
    return;
  }
  out << std::string(depth * 2, ' ') << planKindName(node->kind) << "\n";
  if (node->kind == PlanKind::Select) {
    const auto* n = static_cast<SelectPlan*>(node.get());
    for (const auto& child : {n->child}) {
      explainPlan(child, out, depth + 1);
    }
    return;
  }
  if (node->kind == PlanKind::Filter) {
    const auto* n = static_cast<FilterPlan*>(node.get());
    out << std::string((depth + 1) * 2, ' ') << "idx=" << n->column_index << "\n";
    explainPlan(n->child, out, depth + 1);
    return;
  }
  if (node->kind == PlanKind::WithColumn) {
    const auto* n = static_cast<WithColumnPlan*>(node.get());
    out << std::string((depth + 1) * 2, ' ') << "added=" << n->added_column << "\n";
    if (n->function != ComputedColumnKind::Copy) {
      out << std::string((depth + 1) * 2, ' ') << "function="
          << static_cast<int>(n->function) << "\n";
    }
    explainPlan(n->child, out, depth + 1);
    return;
  }
  if (node->kind == PlanKind::Drop) {
    const auto* n = static_cast<DropPlan*>(node.get());
    out << std::string((depth + 1) * 2, ' ') << "keep=" << n->keep_indices.size() << "\n";
    explainPlan(n->child, out, depth + 1);
    return;
  }
  if (node->kind == PlanKind::Limit) {
    const auto* n = static_cast<LimitPlan*>(node.get());
    out << std::string((depth + 1) * 2, ' ') << "n=" << n->n << "\n";
    explainPlan(n->child, out, depth + 1);
    return;
  }
  if (node->kind == PlanKind::Aggregate || node->kind == PlanKind::GroupBySum) {
    if (node->kind == PlanKind::Aggregate) {
      const auto* n = static_cast<AggregatePlan*>(node.get());
      out << std::string((depth + 1) * 2, ' ') << "keys=" << n->keys.size() << ", aggs="
          << n->aggregates.size() << "\n";
      explainPlan(n->child, out, depth + 1);
      return;
    }
    const auto* n = static_cast<GroupBySumPlan*>(node.get());
    out << std::string((depth + 1) * 2, ' ') << "keys=" << n->keys.size() << ", value="
        << n->value_index << "\n";
    explainPlan(n->child, out, depth + 1);
    return;
  }
  if (node->kind == PlanKind::WindowAssign) {
    const auto* n = static_cast<WindowAssignPlan*>(node.get());
    out << std::string((depth + 1) * 2, ' ') << "idx=" << n->time_column_index
        << ", window_ms=" << n->window_ms << ", output=" << n->output_column << "\n";
    explainPlan(n->child, out, depth + 1);
    return;
  }
  if (node->kind == PlanKind::Join) {
    const auto* n = static_cast<JoinPlan*>(node.get());
    out << std::string((depth + 1) * 2, ' ') << "left_key=" << n->left_key
        << ", right_key=" << n->right_key << "\n";
    explainPlan(n->left, out, depth + 1);
    explainPlan(n->right, out, depth + 1);
    return;
  }
  if (node->kind == PlanKind::Source) {
    const auto* n = static_cast<SourcePlan*>(node.get());
    out << std::string((depth + 1) * 2, ' ') << "source=" << n->source_name << "\n";
    return;
  }
  if (node->kind == PlanKind::Sink) {
    const auto* n = static_cast<SinkPlan*>(node.get());
    out << std::string((depth + 1) * 2, ' ') << "sink=" << n->sink_name << "\n";
    explainPlan(n->child, out, depth + 1);
    return;
  }
}

}  // namespace

DataFrame::DataFrame(Table table)
    : plan_(std::make_shared<SourcePlan>("memory", std::move(table))),
      executor_(defaultExecutor()) {}

DataFrame::DataFrame(PlanNodePtr plan, std::shared_ptr<Executor> exec)
    : plan_(std::move(plan)), executor_(std::move(exec)) {
  if (!executor_) executor_ = defaultExecutor();
}

DataflowJobHandle DataFrame::submitAsync(const ExecutionOptions& options) const {
  return JobMaster::instance().submit(plan_, executor_, options);
}

const Table& DataFrame::materialize() const {
  if (!cached_) {
    ai::PluginContext ctx;
    ctx.trace_id = "df-" + std::to_string(reinterpret_cast<uintptr_t>(this));
    ctx.session_id = "default";
    ctx.labels["api"] = "DataFrame::materialize";

    ai::PluginPayload payload;
    payload.plan = explain();
    payload.summary = "DataFrame execute begin";

    auto before_exec =
        ai::PluginManager::instance().runHook(ai::HookPoint::kPlanBeforeExecute, ctx, &payload);
    if (!before_exec.continue_execution()) {
      throw std::runtime_error(before_exec.reason.empty() ? "plan execution was blocked by ai plugin"
                                                         : before_exec.reason);
    }

    cached_table_ = executor_->execute(plan_);

    payload.row_count = cached_table_.rowCount();
    payload.summary = "DataFrame execute end";
    auto after_exec =
        ai::PluginManager::instance().runHook(ai::HookPoint::kPlanAfterExecute, ctx, &payload);
    if (!after_exec.continue_execution()) {
      throw std::runtime_error(after_exec.reason.empty() ? "plan execution was blocked after execution"
                                                        : after_exec.reason);
    }

    cached_ = true;
  }
  return cached_table_;
}

const CachedVectorColumn& DataFrame::vectorColumnCache(const std::string& vectorColumn) const {
  const auto& source = materialize();
  const size_t vector_index = source.schema.indexOf(vectorColumn);
  const auto it = vector_cache_.find(vector_index);
  if (it != vector_cache_.end()) {
    return it->second;
  }

  CachedVectorColumn cache;
  cache.row_ids.reserve(source.rows.size());
  std::vector<std::vector<float>> vectors;
  vectors.reserve(source.rows.size());

  std::size_t expected_dim = 0;
  bool has_dimension = false;
  for (size_t row_id = 0; row_id < source.rows.size(); ++row_id) {
    if (vector_index >= source.rows[row_id].size()) continue;
    const auto& cell = source.rows[row_id][vector_index];
    if (cell.isNull()) continue;

    std::vector<float> vec = cell.type() == DataType::FixedVector
                                 ? cell.asFixedVector()
                                 : Value::parseFixedVector(cell.toString());
    if (!has_dimension) {
      expected_dim = vec.size();
      has_dimension = true;
    } else if (vec.size() != expected_dim) {
      throw std::invalid_argument("fixed vector length mismatch in vector column cache");
    }
    cache.row_ids.push_back(row_id);
    vectors.push_back(std::move(vec));
  }

  cache.index = std::shared_ptr<VectorIndex>(makeExactScanVectorIndex(std::move(vectors)).release());
  const auto inserted = vector_cache_.emplace(vector_index, std::move(cache));
  return inserted.first->second;
}

std::string DataFrame::explain() const {
  std::ostringstream out;
  explainPlan(plan_, out, 0);
  return out.str();
}

DataFrame DataFrame::select(const std::vector<std::string>& columns) const {
  const auto source = materialize();
  std::vector<size_t> idxs;
  idxs.reserve(columns.size());
  for (const auto& c : columns) idxs.push_back(source.schema.indexOf(c));
  return selectByIndices(idxs);
}

DataFrame DataFrame::selectByIndices(const std::vector<size_t>& columns,
                                    const std::vector<std::string>& aliases) const {
  if (!columns.empty()) {
    const auto source = materialize();
    for (size_t idx : columns) {
      if (idx >= source.schema.fields.size()) {
        throw std::out_of_range("select index out of range: " + std::to_string(idx));
      }
    }
  }
  auto node = std::make_shared<SelectPlan>(plan_, columns, aliases);
  return DataFrame(node, executor_);
}

DataFrame DataFrame::filterByIndex(size_t columnIndex, const std::string& op, const Value& value) const {
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
  auto node = std::make_shared<FilterPlan>(plan_, columnIndex, value, op, pred);
  return DataFrame(node, executor_);
}

DataFrame DataFrame::filter(const std::string& column, const std::string& op, const Value& value) const {
  const auto source = materialize();
  return filterByIndex(source.schema.indexOf(column), op, value);
}

DataFrame DataFrame::withColumn(const std::string& name, const std::string& sourceColumn) const {
  const auto source = materialize();
  auto node = std::make_shared<WithColumnPlan>(plan_, name, source.schema.indexOf(sourceColumn));
  return DataFrame(node, executor_);
}

DataFrame DataFrame::withColumn(const std::string& name, ComputedColumnKind function,
                               const std::vector<ComputedColumnArg>& args) const {
  for (const auto& arg : args) {
    if (!arg.is_literal && arg.source_column_index == static_cast<size_t>(-1)) {
      throw std::invalid_argument("withColumn expression argument column index cannot be -1");
    }
  }
  auto node = std::make_shared<WithColumnPlan>(plan_, name, function, args);
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

DataFrame DataFrame::aggregate(const std::vector<size_t>& keys,
                              const std::vector<AggregateSpec>& aggs) const {
  auto node = std::make_shared<AggregatePlan>(plan_, keys, aggs);
  return DataFrame(node, executor_);
}

DataFrame DataFrame::vectorQuery(const std::string& vectorColumn,
                                 const std::vector<float>& queryVector,
                                 size_t top_k,
                                 VectorDistanceMetric metric) const {
  if (queryVector.empty()) {
    throw std::invalid_argument("query vector cannot be empty");
  }
  const auto& cache = vectorColumnCache(vectorColumn);
  if (cache.index->dimension() != 0 && cache.index->dimension() != queryVector.size()) {
    throw std::invalid_argument("fixed vector length mismatch in vectorQuery");
  }
  VectorSearchOptions options;
  options.top_k = top_k;
  options.metric = toRuntimeMetric(metric);
  const auto scored = cache.index->search(queryVector, options);
  const size_t take = scored.size();
  Table out;
  out.schema = Schema({"row_id", "score"});
  out.rows.reserve(take);
  for (size_t i = 0; i < take; ++i) {
    Row row;
    row.emplace_back(static_cast<int64_t>(cache.row_ids.at(scored[i].row_id)));
    row.emplace_back(scored[i].score);
    out.rows.push_back(std::move(row));
  }
  return DataFrame(std::move(out));
}

std::string DataFrame::explainVectorQuery(const std::string& vectorColumn,
                                          const std::vector<float>& queryVector, size_t top_k,
                                          VectorDistanceMetric metric) const {
  const auto& cache = vectorColumnCache(vectorColumn);
  VectorSearchOptions options;
  options.top_k = top_k;
  options.metric = toRuntimeMetric(metric);
  if (!queryVector.empty() && cache.index->dimension() != 0 &&
      queryVector.size() != cache.index->dimension()) {
    throw std::invalid_argument("query vector dimension mismatch in explainVectorQuery");
  }
  return cache.index->explain(options);
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

std::string DataFrame::serializePlan() const {
  return dataflow::serializePlan(plan_);
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

DataFrame GroupedDataFrame::sum(const std::string& valueColumn, const std::string& outColumn) const {
  const auto source = executor_->execute(plan_);
  AggregateSpec spec{AggregateFunction::Sum, source.schema.indexOf(valueColumn), outColumn};
  auto node = std::make_shared<AggregatePlan>(plan_, keys_, std::vector<AggregateSpec>{spec});
  return DataFrame(node, executor_);
}

}  // namespace dataflow
