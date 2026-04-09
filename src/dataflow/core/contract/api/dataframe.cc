#include "src/dataflow/core/contract/api/dataframe.h"

#include <algorithm>
#include <stdexcept>
#include <cstdint>
#include <iostream>
#include <limits>
#include <sstream>

#include "src/dataflow/ai/plugin_runtime.h"
#include "src/dataflow/core/execution/columnar_batch.h"
#include "src/dataflow/core/execution/runtime/simd_dispatch.h"
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

std::shared_ptr<const Schema> makeSchemaHint(const Schema& schema) {
  return std::make_shared<Schema>(schema);
}

VectorSearchMetric toRuntimeMetric(VectorDistanceMetric metric) {
  return metric == VectorDistanceMetric::L2
             ? VectorSearchMetric::L2
             : (metric == VectorDistanceMetric::Dot ? VectorSearchMetric::Dot
                                                    : VectorSearchMetric::Cosine);
}

const char* metricName(VectorDistanceMetric metric) {
  switch (metric) {
    case VectorDistanceMetric::Cosine:
      return "cosine";
    case VectorDistanceMetric::Dot:
      return "dot";
    case VectorDistanceMetric::L2:
      return "l2";
  }
  return metricName(VectorDistanceMetric::Cosine);
}

bool metricPrefersHigherScores(VectorDistanceMetric metric) {
  return metric == VectorDistanceMetric::Dot;
}

const char* thresholdComparison(VectorDistanceMetric metric) {
  return metricPrefersHigherScores(metric) ? ">=" : "<=";
}

std::string formatThreshold(const std::optional<double>& threshold) {
  if (!threshold.has_value()) {
    return "none";
  }
  std::ostringstream out;
  out.precision(std::numeric_limits<double>::max_digits10);
  out << *threshold;
  return out.str();
}

struct HybridQueryPlanAnalysis {
  bool valid = true;
  std::size_t source_count = 0;
  bool has_filter = false;
  bool filter_pushdown = false;
  std::string error;
};

bool subtreeSupportsSourceFilterPushdown(const PlanNodePtr& plan) {
  if (!plan) {
    return false;
  }
  switch (plan->kind) {
    case PlanKind::Source: {
      const auto* node = static_cast<const SourcePlan*>(plan.get());
      return node->storage_kind == SourceStorageKind::CsvFile;
    }
    case PlanKind::Filter:
      return subtreeSupportsSourceFilterPushdown(static_cast<const FilterPlan*>(plan.get())->child);
    case PlanKind::Limit:
      return subtreeSupportsSourceFilterPushdown(static_cast<const LimitPlan*>(plan.get())->child);
    default:
      return false;
  }
}

void analyzeHybridQueryPlanRecursive(const PlanNodePtr& plan, HybridQueryPlanAnalysis* analysis) {
  if (!plan || analysis == nullptr || !analysis->valid) {
    return;
  }
  switch (plan->kind) {
    case PlanKind::Source:
      ++analysis->source_count;
      return;
    case PlanKind::Select:
      analyzeHybridQueryPlanRecursive(static_cast<const SelectPlan*>(plan.get())->child, analysis);
      return;
    case PlanKind::Filter: {
      analysis->has_filter = true;
      analysis->filter_pushdown =
          analysis->filter_pushdown || subtreeSupportsSourceFilterPushdown(plan);
      analyzeHybridQueryPlanRecursive(static_cast<const FilterPlan*>(plan.get())->child, analysis);
      return;
    }
    case PlanKind::WithColumn:
      analyzeHybridQueryPlanRecursive(static_cast<const WithColumnPlan*>(plan.get())->child,
                                      analysis);
      return;
    case PlanKind::Drop:
      analyzeHybridQueryPlanRecursive(static_cast<const DropPlan*>(plan.get())->child, analysis);
      return;
    case PlanKind::Limit:
      analyzeHybridQueryPlanRecursive(static_cast<const LimitPlan*>(plan.get())->child, analysis);
      return;
    case PlanKind::OrderBy:
      analyzeHybridQueryPlanRecursive(static_cast<const OrderByPlan*>(plan.get())->child, analysis);
      return;
    case PlanKind::WindowAssign:
      analyzeHybridQueryPlanRecursive(
          static_cast<const WindowAssignPlan*>(plan.get())->child, analysis);
      return;
    case PlanKind::Join:
    case PlanKind::Aggregate:
    case PlanKind::Sink:
      analysis->valid = false;
      analysis->error =
          "only supports single-source DataFrame plans without JOIN, AGGREGATE, or SINK";
      return;
  }
}

HybridQueryPlanAnalysis analyzeHybridQueryPlan(const PlanNodePtr& plan) {
  HybridQueryPlanAnalysis analysis;
  analyzeHybridQueryPlanRecursive(plan, &analysis);
  if (analysis.valid && analysis.source_count != 1) {
    analysis.valid = false;
    analysis.error = "only supports single-source DataFrame plans";
  }
  return analysis;
}

void validateHybridQueryPlan(const HybridQueryPlanAnalysis& analysis, const char* api_name) {
  if (!analysis.valid) {
    throw std::invalid_argument(std::string(api_name) + " " + analysis.error);
  }
}

bool passesScoreThreshold(double score, const HybridSearchOptions& options) {
  if (!options.score_threshold.has_value()) {
    return true;
  }
  return metricPrefersHigherScores(options.metric) ? score >= *options.score_threshold
                                                   : score <= *options.score_threshold;
}

std::vector<VectorSearchResult> applyScoreThreshold(const std::vector<VectorSearchResult>& scored,
                                                    const HybridSearchOptions& options) {
  if (!options.score_threshold.has_value()) {
    return scored;
  }
  std::vector<VectorSearchResult> filtered;
  filtered.reserve(scored.size());
  for (const auto& item : scored) {
    if (passesScoreThreshold(item.score, options)) {
      filtered.push_back(item);
    }
  }
  return filtered;
}

RowSelection makeRowSelection(std::size_t row_count, const std::vector<std::size_t>& indices) {
  RowSelection selection;
  selection.input_row_count = row_count;
  selection.selected.assign(row_count, 0);
    selection.indices = indices;
  selection.selected_count = indices.size();
  for (const auto row_index : indices) {
    if (row_index >= row_count) {
      throw std::out_of_range("hybridSearch selected row index out of range");
    }
    selection.selected[row_index] = 1;
  }
  return selection;
}

Table gatherHybridSearchRows(const Table& input, const std::vector<VectorSearchResult>& scored,
                             const CachedVectorColumn& cache) {
  if (input.schema.index.count("vector_score") != 0) {
    throw std::invalid_argument("hybridSearch output column already exists: vector_score");
  }
  std::vector<std::size_t> row_indices;
  row_indices.reserve(scored.size());
  std::vector<Value> scores;
  scores.reserve(scored.size());
  for (const auto& item : scored) {
    row_indices.push_back(cache.row_ids.at(item.row_id));
    scores.emplace_back(item.score);
  }
  Table out = filterTable(input, makeRowSelection(input.rowCount(), row_indices), false);
  appendNamedColumn(&out, "vector_score", std::move(scores), false);
  return out;
}

std::string formatHybridSearchExplain(const HybridSearchOptions& options,
                                      const HybridQueryPlanAnalysis& analysis,
                                      const CachedVectorColumn& cache, std::size_t input_rows,
                                      std::size_t returned_rows) {
  std::ostringstream out;
  out << "mode=exact-scan-hybrid-search\n";
  out << "metric=" << metricName(options.metric) << "\n";
  out << "top_k=" << options.top_k << "\n";
  out << "score_threshold=" << formatThreshold(options.score_threshold) << "\n";
  out << "score_threshold_compare=" << thresholdComparison(options.metric) << "\n";
  out << "input_rows=" << input_rows << "\n";
  out << "candidate_rows=" << cache.row_ids.size() << "\n";
  out << "returned_rows=" << returned_rows << "\n";
  out << "column_filter_stage=before-vector\n";
  out << "column_filter_execution="
      << (analysis.has_filter ? (analysis.filter_pushdown ? "source-pushdown" : "post-load-filter")
                              : "none")
      << "\n";
  out << "acceleration=flat-buffer+simd-topk\n";
  out << "backend=" << activeSimdBackendName() << "\n";
  return out.str();
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
    case PlanKind::OrderBy:
      return "OrderBy";
    case PlanKind::WindowAssign:
      return "WindowAssign";
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

PlanNodePtr pushLimitThroughPassthrough(const PlanNodePtr& node, size_t limit) {
  if (!node) {
    return std::make_shared<LimitPlan>(node, limit);
  }
  switch (node->kind) {
    case PlanKind::Limit: {
      const auto* n = static_cast<const LimitPlan*>(node.get());
      return pushLimitThroughPassthrough(n->child, std::min(limit, n->n));
    }
    case PlanKind::Select: {
      const auto* n = static_cast<const SelectPlan*>(node.get());
      return std::make_shared<SelectPlan>(pushLimitThroughPassthrough(n->child, limit), n->indices,
                                          n->aliases);
    }
    case PlanKind::WithColumn: {
      const auto* n = static_cast<const WithColumnPlan*>(node.get());
      if (n->function == ComputedColumnKind::Copy) {
        auto rewritten = std::make_shared<WithColumnPlan>(
            pushLimitThroughPassthrough(n->child, limit), n->added_column, n->source_column_index);
        rewritten->function = n->function;
        rewritten->args = n->args;
        return rewritten;
      }
      return std::make_shared<WithColumnPlan>(pushLimitThroughPassthrough(n->child, limit),
                                              n->added_column, n->function, n->args);
    }
    case PlanKind::Drop: {
      const auto* n = static_cast<const DropPlan*>(node.get());
      return std::make_shared<DropPlan>(pushLimitThroughPassthrough(n->child, limit),
                                        n->keep_indices);
    }
    case PlanKind::WindowAssign: {
      const auto* n = static_cast<const WindowAssignPlan*>(node.get());
      return std::make_shared<WindowAssignPlan>(pushLimitThroughPassthrough(n->child, limit),
                                                n->time_column_index, n->window_ms,
                                                n->output_column);
    }
    default:
      return std::make_shared<LimitPlan>(node, limit);
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
  if (node->kind == PlanKind::OrderBy) {
    const auto* n = static_cast<OrderByPlan*>(node.get());
    out << std::string((depth + 1) * 2, ' ') << "keys=" << n->indices.size() << "\n";
    explainPlan(n->child, out, depth + 1);
    return;
  }
  if (node->kind == PlanKind::Aggregate) {
    const auto* n = static_cast<AggregatePlan*>(node.get());
    out << std::string((depth + 1) * 2, ' ') << "keys=" << n->keys.size() << ", aggs="
        << n->aggregates.size() << "\n";
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
    : plan_([&]() {
        if (!table.schema.fields.empty()) {
          ensureColumnarCache(&table);
          table.rows.clear();
        }
        return std::make_shared<SourcePlan>("memory", std::move(table));
      }()),
      executor_(defaultExecutor()) {
  const auto* source = static_cast<const SourcePlan*>(plan_.get());
  schema_hint_ = makeSchemaHint(source->schema);
}

DataFrame::DataFrame(PlanNodePtr plan, std::shared_ptr<Executor> exec,
                     std::shared_ptr<const Schema> schema_hint)
    : plan_(std::move(plan)), executor_(std::move(exec)), schema_hint_(std::move(schema_hint)) {
  if (!executor_) executor_ = defaultExecutor();
  if (!schema_hint_ && plan_ && plan_->kind == PlanKind::Source) {
    const auto* source = static_cast<const SourcePlan*>(plan_.get());
    schema_hint_ = makeSchemaHint(source->schema);
  }
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
    if (!schema_hint_) {
      schema_hint_ = makeSchemaHint(cached_table_.schema);
    }

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
  std::vector<std::vector<float>> vectors;

  std::size_t expected_dim = 0;
  bool has_dimension = false;
  const auto vector_column = viewValueColumn(source, vector_index);
  const auto row_count = valueColumnRowCount(*vector_column.buffer);
  cache.row_ids.reserve(row_count);
  vectors.reserve(row_count);
  for (size_t row_id = 0; row_id < row_count; ++row_id) {
    const auto cell = valueColumnValueAt(*vector_column.buffer, row_id);
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
  const auto& source = schema();
  std::vector<size_t> idxs;
  idxs.reserve(columns.size());
  for (const auto& c : columns) idxs.push_back(source.indexOf(c));
  return selectByIndices(idxs);
}

DataFrame DataFrame::selectByIndices(const std::vector<size_t>& columns,
                                    const std::vector<std::string>& aliases) const {
  const auto& source = schema();
  if (!columns.empty()) {
    for (size_t idx : columns) {
      if (idx >= source.fields.size()) {
        throw std::out_of_range("select index out of range: " + std::to_string(idx));
      }
    }
  }
  Schema output_schema;
  output_schema.fields.reserve(columns.size());
  for (std::size_t i = 0; i < columns.size(); ++i) {
    output_schema.fields.push_back(i < aliases.size() && !aliases[i].empty()
                                       ? aliases[i]
                                       : source.fields[columns[i]]);
    output_schema.index[output_schema.fields.back()] = i;
  }
  auto node = std::make_shared<SelectPlan>(plan_, columns, aliases);
  return DataFrame(node, executor_, makeSchemaHint(output_schema));
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
  return DataFrame(node, executor_, schema_hint_);
}

DataFrame DataFrame::filter(const std::string& column, const std::string& op, const Value& value) const {
  return filterByIndex(schema().indexOf(column), op, value);
}

DataFrame DataFrame::withColumn(const std::string& name, const std::string& sourceColumn) const {
  const auto& source = schema();
  Schema output_schema = source;
  output_schema.fields.push_back(name);
  output_schema.index[name] = output_schema.fields.size() - 1;
  auto node = std::make_shared<WithColumnPlan>(plan_, name, source.indexOf(sourceColumn));
  return DataFrame(node, executor_, makeSchemaHint(output_schema));
}

DataFrame DataFrame::withColumn(const std::string& name, ComputedColumnKind function,
                               const std::vector<ComputedColumnArg>& args) const {
  for (const auto& arg : args) {
    if (!arg.is_literal && arg.source_column_index == static_cast<size_t>(-1)) {
      throw std::invalid_argument("withColumn expression argument column index cannot be -1");
    }
  }
  Schema output_schema = schema();
  output_schema.fields.push_back(name);
  output_schema.index[name] = output_schema.fields.size() - 1;
  auto node = std::make_shared<WithColumnPlan>(plan_, name, function, args);
  return DataFrame(node, executor_, makeSchemaHint(output_schema));
}

DataFrame DataFrame::drop(const std::string& column) const {
  const auto& source = schema();
  std::vector<size_t> keep;
  Schema output_schema;
  for (size_t i = 0; i < source.fields.size(); ++i) {
    if (source.fields[i] != column) {
      keep.push_back(i);
      output_schema.index[source.fields[i]] = output_schema.fields.size();
      output_schema.fields.push_back(source.fields[i]);
    }
  }
  auto node = std::make_shared<DropPlan>(plan_, keep);
  return DataFrame(node, executor_, makeSchemaHint(output_schema));
}

DataFrame DataFrame::orderBy(const std::vector<std::string>& columns,
                             const std::vector<bool>& ascending) const {
  if (!ascending.empty() && ascending.size() != columns.size()) {
    throw std::invalid_argument("ORDER BY direction count mismatch");
  }
  const auto& source = schema();
  std::vector<size_t> indices;
  indices.reserve(columns.size());
  for (const auto& column : columns) {
    indices.push_back(source.indexOf(column));
  }
  std::vector<bool> directions = ascending;
  if (directions.empty()) {
    directions.assign(columns.size(), true);
  }
  auto node = std::make_shared<OrderByPlan>(plan_, std::move(indices), std::move(directions));
  return DataFrame(node, executor_, schema_hint_);
}

DataFrame DataFrame::limit(size_t n) const {
  auto node = pushLimitThroughPassthrough(plan_, n);
  return DataFrame(node, executor_, schema_hint_);
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
  return aggregate(keys, aggs, AggregateExecSpec{});
}

DataFrame DataFrame::aggregate(const std::vector<size_t>& keys,
                              const std::vector<AggregateSpec>& aggs,
                              const AggregateExecSpec& exec_spec) const {
  const auto& source = schema();
  Schema output_schema;
  output_schema.fields.reserve(keys.size() + aggs.size());
  for (std::size_t i = 0; i < keys.size(); ++i) {
    if (keys[i] >= source.fields.size()) {
      throw std::out_of_range("aggregate key index out of range: " + std::to_string(keys[i]));
    }
    output_schema.index[source.fields[keys[i]]] = output_schema.fields.size();
    output_schema.fields.push_back(source.fields[keys[i]]);
  }
  for (const auto& agg : aggs) {
    output_schema.index[agg.output_name] = output_schema.fields.size();
    output_schema.fields.push_back(agg.output_name);
  }
  auto node = std::make_shared<AggregatePlan>(plan_, keys, aggs, exec_spec);
  node->has_exec_spec = !exec_spec.reason.empty() || !exec_spec.rejected_candidates.empty() ||
                        !exec_spec.key_layout.transforms.empty() || exec_spec.expected_groups != 0 ||
                        exec_spec.reserved_buckets != 0 || exec_spec.input_requires_sort ||
                        exec_spec.use_local_global;
  return DataFrame(node, executor_, makeSchemaHint(output_schema));
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
  auto cache_out = std::make_shared<ColumnarTable>();
  cache_out->schema = out.schema;
  cache_out->columns.resize(out.schema.fields.size());
  cache_out->arrow_formats.resize(out.schema.fields.size());
  cache_out->row_count = take;
  if (take > 0) {
    cache_out->batch_row_counts.push_back(take);
  }
  cache_out->columns[0].values.reserve(take);
  cache_out->columns[1].values.reserve(take);
  for (size_t i = 0; i < take; ++i) {
    cache_out->columns[0].values.emplace_back(static_cast<int64_t>(cache.row_ids.at(scored[i].row_id)));
    cache_out->columns[1].values.emplace_back(scored[i].score);
  }
  out.columnar_cache = std::move(cache_out);
  return DataFrame(std::move(out));
}

DataFrame DataFrame::hybridSearch(const std::string& vectorColumn,
                                  const std::vector<float>& queryVector,
                                  const HybridSearchOptions& options) const {
  if (queryVector.empty()) {
    throw std::invalid_argument("query vector cannot be empty");
  }
  const auto analysis = analyzeHybridQueryPlan(plan_);
  validateHybridQueryPlan(analysis, "hybridSearch");

  const auto& input = materialize();
  const auto& cache = vectorColumnCache(vectorColumn);
  if (cache.index->dimension() != 0 && cache.index->dimension() != queryVector.size()) {
    throw std::invalid_argument("fixed vector length mismatch in hybridSearch");
  }

  VectorSearchOptions runtime_options;
  runtime_options.top_k = options.top_k;
  runtime_options.metric = toRuntimeMetric(options.metric);
  const auto filtered = applyScoreThreshold(cache.index->search(queryVector, runtime_options),
                                            options);
  return DataFrame(gatherHybridSearchRows(input, filtered, cache));
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

std::string DataFrame::explainHybridSearch(const std::string& vectorColumn,
                                           const std::vector<float>& queryVector,
                                           const HybridSearchOptions& options) const {
  if (queryVector.empty()) {
    throw std::invalid_argument("query vector cannot be empty");
  }
  const auto analysis = analyzeHybridQueryPlan(plan_);
  validateHybridQueryPlan(analysis, "explainHybridSearch");

  const auto& input = materialize();
  const auto& cache = vectorColumnCache(vectorColumn);
  if (cache.index->dimension() != 0 && queryVector.size() != cache.index->dimension()) {
    throw std::invalid_argument("query vector dimension mismatch in explainHybridSearch");
  }

  VectorSearchOptions runtime_options;
  runtime_options.top_k = options.top_k;
  runtime_options.metric = toRuntimeMetric(options.metric);
  const auto filtered = applyScoreThreshold(cache.index->search(queryVector, runtime_options),
                                            options);
  return formatHybridSearchExplain(options, analysis, cache, input.rowCount(), filtered.size());
}

GroupedDataFrame DataFrame::groupBy(const std::vector<std::string>& keys) const {
  const auto& source = schema();
  std::vector<size_t> idxs;
  idxs.reserve(keys.size());
  for (const auto& k : keys) idxs.push_back(source.indexOf(k));
  return GroupedDataFrame(plan_, idxs, executor_, schema_hint_);
}

DataFrame DataFrame::join(const DataFrame& right, const std::string& leftOn, const std::string& rightOn,
                         JoinKind kind) const {
  const auto& leftSchema = schema();
  const auto& rightSchema = right.schema();
  Schema output_schema;
  output_schema.fields = leftSchema.fields;
  output_schema.fields.insert(output_schema.fields.end(), rightSchema.fields.begin(),
                              rightSchema.fields.end());
  for (std::size_t i = 0; i < output_schema.fields.size(); ++i) {
    output_schema.index[output_schema.fields[i]] = i;
  }
  auto node = std::make_shared<JoinPlan>(plan_, right.plan_, leftSchema.indexOf(leftOn),
                                         rightSchema.indexOf(rightOn), kind);
  return DataFrame(node, executor_, makeSchemaHint(output_schema));
}

size_t DataFrame::count() const {
  return materialize().rowCount();
}

std::vector<Row> DataFrame::collect() const {
  auto table = toTable();
  materializeRows(&table);
  return std::move(table.rows);
}

std::string DataFrame::serializePlan() const {
  return dataflow::serializePlan(plan_);
}

void DataFrame::show(size_t max_rows) const {
  auto t = toTable();
  materializeRows(&t);
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
  auto table = materialize();
  materializeRows(&table);
  return table;
}

const Table& DataFrame::materializedTable() const {
  return materialize();
}

const Schema& DataFrame::schema() const {
  if (schema_hint_) {
    return *schema_hint_;
  }
  return materialize().schema;
}

DataFrame GroupedDataFrame::sum(const std::string& valueColumn, const std::string& outColumn) const {
  const Schema* source_schema = schema_hint_.get();
  Schema materialized_schema;
  if (source_schema == nullptr) {
    materialized_schema = executor_->execute(plan_).schema;
    source_schema = &materialized_schema;
  }
  AggregateSpec spec{AggregateFunction::Sum, source_schema->indexOf(valueColumn), outColumn};
  auto node = std::make_shared<AggregatePlan>(plan_, keys_, std::vector<AggregateSpec>{spec});
  Schema output_schema;
  output_schema.fields.reserve(keys_.size() + 1);
  for (std::size_t key_index : keys_) {
    output_schema.index[source_schema->fields[key_index]] = output_schema.fields.size();
    output_schema.fields.push_back(source_schema->fields[key_index]);
  }
  output_schema.index[outColumn] = output_schema.fields.size();
  output_schema.fields.push_back(outColumn);
  return DataFrame(node, executor_, makeSchemaHint(output_schema));
}

}  // namespace dataflow
