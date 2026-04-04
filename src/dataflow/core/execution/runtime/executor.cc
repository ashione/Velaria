#include "src/dataflow/core/execution/runtime/executor.h"

#include <memory>
#include <algorithm>
#include <cstdint>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <unordered_map>
#include <vector>

#include "src/dataflow/core/execution/columnar_batch.h"
#include "src/dataflow/core/execution/csv.h"
#include "src/dataflow/core/execution/source_materialization.h"
#include "src/dataflow/experimental/runtime/rpc_runner.h"

namespace dataflow {

namespace {

constexpr char kGroupDelim = '\x1f';

std::vector<std::string> splitKey(const std::string& key) {
  std::vector<std::string> out;
  std::string cur;
  for (char c : key) {
    if (c == kGroupDelim) {
      out.push_back(cur);
      cur.clear();
    } else {
      cur.push_back(c);
    }
  }
  out.push_back(cur);
  return out;
}

bool canUseSingleStringKeyFastPath(const ValueColumnBuffer& column) {
  if (column.arrow_backing != nullptr) {
    return column.arrow_backing->format == "u" || column.arrow_backing->format == "U";
  }
  for (const auto& value : column.values) {
    if (!value.isNull()) {
      return value.type() == DataType::String;
    }
  }
  return true;
}

std::string_view singleStringKeyAt(const ValueColumnBuffer& column, std::size_t row_index) {
  if (valueColumnIsNullAt(column, row_index)) {
    return std::string_view("null");
  }
  const auto view = valueColumnStringViewAt(column, row_index);
  if (!view.empty()) {
    return view;
  }
  return std::string_view();
}

struct AggregateAccumulator {
  std::vector<double> sum;
  std::vector<std::size_t> count;
  std::vector<Value> minVal;
  std::vector<bool> hasMin;
  std::vector<Value> maxVal;
  std::vector<bool> hasMax;
};

std::vector<std::size_t> allColumnIndices(const Schema& schema) {
  std::vector<std::size_t> indices;
  indices.reserve(schema.fields.size());
  for (std::size_t i = 0; i < schema.fields.size(); ++i) {
    indices.push_back(i);
  }
  return indices;
}

std::vector<std::size_t> allColumnIndices(std::size_t width) {
  std::vector<std::size_t> indices;
  indices.reserve(width);
  for (std::size_t i = 0; i < width; ++i) {
    indices.push_back(i);
  }
  return indices;
}

struct BorrowedOrOwnedTable {
  const Table* table = nullptr;
  std::shared_ptr<Table> retained;
};

using SourceRequirementMap = std::unordered_map<const SourcePlan*, std::vector<std::size_t>>;

std::vector<std::size_t> normalizeRequiredColumns(std::vector<std::size_t> indices) {
  std::sort(indices.begin(), indices.end());
  indices.erase(std::unique(indices.begin(), indices.end()), indices.end());
  return indices;
}

void appendRequiredColumn(std::vector<std::size_t>* target, std::size_t column_index) {
  if (target == nullptr) {
    throw std::invalid_argument("required column target is null");
  }
  target->push_back(column_index);
}

void appendRequiredColumns(std::vector<std::size_t>* target,
                           const std::vector<std::size_t>& columns) {
  if (target == nullptr) {
    throw std::invalid_argument("required column target is null");
  }
  target->insert(target->end(), columns.begin(), columns.end());
}

std::size_t planOutputWidth(const PlanNodePtr& plan) {
  if (!plan) {
    return 0;
  }
  switch (plan->kind) {
    case PlanKind::Source:
      return static_cast<const SourcePlan*>(plan.get())->schema.fields.size();
    case PlanKind::Select:
      return static_cast<const SelectPlan*>(plan.get())->indices.size();
    case PlanKind::Filter:
      return planOutputWidth(static_cast<const FilterPlan*>(plan.get())->child);
    case PlanKind::WithColumn:
      return planOutputWidth(static_cast<const WithColumnPlan*>(plan.get())->child) + 1;
    case PlanKind::Drop:
      return static_cast<const DropPlan*>(plan.get())->keep_indices.size();
    case PlanKind::Limit:
      return planOutputWidth(static_cast<const LimitPlan*>(plan.get())->child);
    case PlanKind::OrderBy:
      return planOutputWidth(static_cast<const OrderByPlan*>(plan.get())->child);
    case PlanKind::WindowAssign:
      return planOutputWidth(static_cast<const WindowAssignPlan*>(plan.get())->child) + 1;
    case PlanKind::GroupBySum: {
      const auto* node = static_cast<const GroupBySumPlan*>(plan.get());
      return node->keys.size() + 1;
    }
    case PlanKind::Aggregate: {
      const auto* node = static_cast<const AggregatePlan*>(plan.get());
      return node->keys.size() + node->aggregates.size();
    }
    case PlanKind::Join: {
      const auto* node = static_cast<const JoinPlan*>(plan.get());
      return planOutputWidth(node->left) + planOutputWidth(node->right);
    }
    case PlanKind::Sink:
      return planOutputWidth(static_cast<const SinkPlan*>(plan.get())->child);
  }
  return 0;
}

void collectSourceRequirements(const PlanNodePtr& plan, const std::vector<std::size_t>& required_output,
                               SourceRequirementMap* requirements) {
  if (!plan || requirements == nullptr) {
    return;
  }
  switch (plan->kind) {
    case PlanKind::Source: {
      auto* node = static_cast<const SourcePlan*>(plan.get());
      auto& required = (*requirements)[node];
      appendRequiredColumns(&required, required_output);
      required = normalizeRequiredColumns(std::move(required));
      return;
    }
    case PlanKind::Select: {
      const auto* node = static_cast<const SelectPlan*>(plan.get());
      std::vector<std::size_t> child_required;
      child_required.reserve(required_output.size());
      for (const auto index : required_output) {
        if (index < node->indices.size()) {
          child_required.push_back(node->indices[index]);
        }
      }
      collectSourceRequirements(node->child, normalizeRequiredColumns(std::move(child_required)),
                                requirements);
      return;
    }
    case PlanKind::Filter: {
      const auto* node = static_cast<const FilterPlan*>(plan.get());
      auto child_required = required_output;
      appendRequiredColumn(&child_required, node->column_index);
      collectSourceRequirements(node->child, normalizeRequiredColumns(std::move(child_required)),
                                requirements);
      return;
    }
    case PlanKind::WithColumn: {
      const auto* node = static_cast<const WithColumnPlan*>(plan.get());
      const auto child_width = planOutputWidth(node->child);
      std::vector<std::size_t> child_required;
      child_required.reserve(required_output.size() + node->args.size() + 1);
      bool needs_added_column = false;
      for (const auto index : required_output) {
        if (index < child_width) {
          child_required.push_back(index);
        } else if (index == child_width) {
          needs_added_column = true;
        }
      }
      if (needs_added_column) {
        if (node->function == ComputedColumnKind::Copy) {
          appendRequiredColumn(&child_required, node->source_column_index);
        } else {
          for (const auto& arg : node->args) {
            if (!arg.is_literal) {
              appendRequiredColumn(&child_required, arg.source_column_index);
            }
          }
        }
      }
      collectSourceRequirements(node->child, normalizeRequiredColumns(std::move(child_required)),
                                requirements);
      return;
    }
    case PlanKind::Drop: {
      const auto* node = static_cast<const DropPlan*>(plan.get());
      std::vector<std::size_t> child_required;
      child_required.reserve(required_output.size());
      for (const auto index : required_output) {
        if (index < node->keep_indices.size()) {
          child_required.push_back(node->keep_indices[index]);
        }
      }
      collectSourceRequirements(node->child, normalizeRequiredColumns(std::move(child_required)),
                                requirements);
      return;
    }
    case PlanKind::Limit: {
      const auto* node = static_cast<const LimitPlan*>(plan.get());
      collectSourceRequirements(node->child, required_output, requirements);
      return;
    }
    case PlanKind::OrderBy: {
      const auto* node = static_cast<const OrderByPlan*>(plan.get());
      auto child_required = required_output;
      appendRequiredColumns(&child_required, node->indices);
      collectSourceRequirements(node->child, normalizeRequiredColumns(std::move(child_required)),
                                requirements);
      return;
    }
    case PlanKind::WindowAssign: {
      const auto* node = static_cast<const WindowAssignPlan*>(plan.get());
      const auto child_width = planOutputWidth(node->child);
      std::vector<std::size_t> child_required;
      child_required.reserve(required_output.size() + 1);
      bool needs_window_output = false;
      for (const auto index : required_output) {
        if (index < child_width) {
          child_required.push_back(index);
        } else if (index == child_width) {
          needs_window_output = true;
        }
      }
      if (needs_window_output) {
        appendRequiredColumn(&child_required, node->time_column_index);
      }
      collectSourceRequirements(node->child, normalizeRequiredColumns(std::move(child_required)),
                                requirements);
      return;
    }
    case PlanKind::GroupBySum: {
      const auto* node = static_cast<const GroupBySumPlan*>(plan.get());
      std::vector<std::size_t> child_required = node->keys;
      appendRequiredColumn(&child_required, node->value_index);
      collectSourceRequirements(node->child, normalizeRequiredColumns(std::move(child_required)),
                                requirements);
      return;
    }
    case PlanKind::Aggregate: {
      const auto* node = static_cast<const AggregatePlan*>(plan.get());
      std::vector<std::size_t> child_required = node->keys;
      for (const auto& aggregate : node->aggregates) {
        if (aggregate.function != AggregateFunction::Count) {
          appendRequiredColumn(&child_required, aggregate.value_index);
        }
      }
      collectSourceRequirements(node->child, normalizeRequiredColumns(std::move(child_required)),
                                requirements);
      return;
    }
    case PlanKind::Join: {
      const auto* node = static_cast<const JoinPlan*>(plan.get());
      const auto left_width = planOutputWidth(node->left);
      std::vector<std::size_t> left_required;
      std::vector<std::size_t> right_required;
      left_required.reserve(required_output.size() + 1);
      right_required.reserve(required_output.size() + 1);
      for (const auto index : required_output) {
        if (index < left_width) {
          left_required.push_back(index);
        } else {
          right_required.push_back(index - left_width);
        }
      }
      appendRequiredColumn(&left_required, node->left_key);
      appendRequiredColumn(&right_required, node->right_key);
      collectSourceRequirements(node->left, normalizeRequiredColumns(std::move(left_required)),
                                requirements);
      collectSourceRequirements(node->right, normalizeRequiredColumns(std::move(right_required)),
                                requirements);
      return;
    }
    case PlanKind::Sink: {
      const auto* node = static_cast<const SinkPlan*>(plan.get());
      collectSourceRequirements(node->child, required_output, requirements);
      return;
    }
  }
}

bool cacheCoversProjectedColumns(const std::vector<std::size_t>& cached_columns,
                                 const std::vector<std::size_t>& required_columns) {
  if (required_columns.empty()) {
    return true;
  }
  for (const auto index : required_columns) {
    if (!std::binary_search(cached_columns.begin(), cached_columns.end(), index)) {
      return false;
    }
  }
  return true;
}

std::vector<std::size_t> requestedSourceColumns(const SourcePlan& node,
                                                const SourceRequirementMap& requirements) {
  auto it = requirements.find(&node);
  if (it == requirements.end() || it->second.empty()) {
    return allColumnIndices(node.schema);
  }
  return it->second;
}

bool tryExecuteSourcePushdown(const SourcePlan& node, const SourcePushdownSpec& pushdown,
                              bool materialize_rows, Table* out) {
  switch (node.storage_kind) {
    case SourceStorageKind::InMemory:
      return false;
    case SourceStorageKind::CsvFile:
      return execute_csv_source_pushdown(node.csv_path, node.schema, pushdown,
                                         node.csv_delimiter, materialize_rows, out);
  }
  return false;
}

bool buildSourcePushdownSpec(const PlanNodePtr& plan, const SourceRequirementMap& requirements,
                             const SourcePlan** source_out, SourcePushdownSpec* pushdown_out) {
  if (!plan || source_out == nullptr || pushdown_out == nullptr) {
    return false;
  }
  switch (plan->kind) {
    case PlanKind::Source: {
      const auto* source = static_cast<const SourcePlan*>(plan.get());
      if (source->storage_kind != SourceStorageKind::CsvFile) {
        return false;
      }
      SourcePushdownSpec spec;
      spec.projected_columns = requestedSourceColumns(*source, requirements);
      *source_out = source;
      *pushdown_out = std::move(spec);
      return true;
    }
    case PlanKind::Filter: {
      const auto* node = static_cast<const FilterPlan*>(plan.get());
      SourcePushdownSpec spec;
      const SourcePlan* source = nullptr;
      if (!buildSourcePushdownSpec(node->child, requirements, &source, &spec)) {
        return false;
      }
      spec.filter.enabled = true;
      spec.filter.column_index = node->column_index;
      spec.filter.value = node->value;
      spec.filter.op = node->op;
      *source_out = source;
      *pushdown_out = std::move(spec);
      return true;
    }
    case PlanKind::Limit: {
      const auto* node = static_cast<const LimitPlan*>(plan.get());
      SourcePushdownSpec spec;
      const SourcePlan* source = nullptr;
      if (!buildSourcePushdownSpec(node->child, requirements, &source, &spec)) {
        return false;
      }
      spec.limit = spec.limit == 0 ? node->n : std::min(spec.limit, node->n);
      *source_out = source;
      *pushdown_out = std::move(spec);
      return true;
    }
    default:
      return false;
  }
}

std::shared_ptr<Table> loadCsvSource(const SourcePlan& node,
                                     const SourceRequirementMap& requirements) {
  const auto required_columns = requestedSourceColumns(node, requirements);
  {
    std::lock_guard<std::mutex> lock(node.cached_table_mu);
    if (node.cached_table &&
        cacheCoversProjectedColumns(node.cached_projected_indices, required_columns)) {
      return node.cached_table;
    }
  }

Table loaded;
  const bool require_all_columns = required_columns.size() >= node.schema.fields.size();
  SourcePushdownSpec pushdown;
  pushdown.projected_columns = required_columns;
  if (node.options.materialization.enabled) {
    std::ostringstream options;
    options << "delimiter=" << node.csv_delimiter;
    const auto fingerprint =
        capture_file_source_fingerprint(node.csv_path, "csv", options.str());
    const std::string materialization_root =
        node.options.materialization.root.empty() ? default_source_materialization_root()
                                                  : node.options.materialization.root;
    const SourceMaterializationStore store(materialization_root,
                                           node.options.materialization.data_format);
    if (require_all_columns) {
      if (const auto entry = store.lookup(fingerprint); entry.has_value()) {
        try {
          loaded = store.load(*entry, false);
        } catch (...) {
          loaded = load_csv(fingerprint.abs_path, node.csv_delimiter, false);
        }
      } else {
        loaded = load_csv(fingerprint.abs_path, node.csv_delimiter, false);
        try {
          store.save(fingerprint, loaded);
        } catch (...) {
        }
      }
    } else {
      if (!tryExecuteSourcePushdown(node, pushdown, false, &loaded)) {
        loaded = load_csv_projected(node.csv_path, node.schema, required_columns,
                                    node.csv_delimiter, false);
      }
      if (!store.lookup(fingerprint).has_value()) {
        try {
          auto full = load_csv(fingerprint.abs_path, node.csv_delimiter, false);
          store.save(fingerprint, full);
        } catch (...) {
        }
      }
    }
  } else {
    if (!tryExecuteSourcePushdown(node, pushdown, false, &loaded)) {
      loaded = require_all_columns
                   ? load_csv(node.csv_path, node.csv_delimiter, false)
                   : load_csv_projected(node.csv_path, node.schema, required_columns,
                                        node.csv_delimiter, false);
    }
  }

  auto retained = std::make_shared<Table>(std::move(loaded));
  {
    std::lock_guard<std::mutex> lock(node.cached_table_mu);
    node.cached_table = retained;
    node.cached_projected_indices = required_columns;
  }
  return retained;
}

Table executePlanWithRequirements(const LocalExecutor& executor, const PlanNodePtr& plan,
                                  const SourceRequirementMap& requirements);

BorrowedOrOwnedTable borrowOrExecute(const LocalExecutor& executor, const PlanNodePtr& plan,
                                     const SourceRequirementMap& requirements) {
  if (!plan) {
    auto owned = std::make_shared<Table>();
    return BorrowedOrOwnedTable{owned.get(), std::move(owned)};
  }
  if (plan->kind == PlanKind::Source) {
    const auto* node = static_cast<const SourcePlan*>(plan.get());
    if (node->storage_kind == SourceStorageKind::InMemory) {
      return BorrowedOrOwnedTable{&node->table, nullptr};
    }
    auto retained = loadCsvSource(*node, requirements);
    return BorrowedOrOwnedTable{retained.get(), std::move(retained)};
  }
  const SourcePlan* pushed_source = nullptr;
  SourcePushdownSpec pushed_spec;
  if (buildSourcePushdownSpec(plan, requirements, &pushed_source, &pushed_spec) &&
      !pushed_source->options.materialization.enabled) {
    Table loaded;
    if (tryExecuteSourcePushdown(*pushed_source, pushed_spec, false, &loaded)) {
      auto owned = std::make_shared<Table>(std::move(loaded));
      return BorrowedOrOwnedTable{owned.get(), std::move(owned)};
    }
  }
  auto owned = std::make_shared<Table>(executePlanWithRequirements(executor, plan, requirements));
  return BorrowedOrOwnedTable{owned.get(), std::move(owned)};
}

}  // namespace

Table executeAggregateTable(const Table& input, const std::vector<size_t>& key_indices,
                            const std::vector<AggregateSpec>& aggs) {
  std::vector<std::size_t> aggregate_value_indices;
  aggregate_value_indices.reserve(aggs.size());
  for (const auto& agg : aggs) {
    if (agg.function != AggregateFunction::Count) {
      aggregate_value_indices.push_back(agg.value_index);
    }
  }
  const auto aggregate_inputs = viewValueColumns(input, aggregate_value_indices);
  std::vector<const ValueColumnView*> aggregate_columns;
  aggregate_columns.reserve(aggs.size());
  std::size_t aggregate_input_index = 0;
  for (const auto& agg : aggs) {
    if (agg.function == AggregateFunction::Count) {
      aggregate_columns.push_back(nullptr);
    } else {
      aggregate_columns.push_back(&aggregate_inputs[aggregate_input_index++]);
    }
  }

  auto initAccumulator = [&]() {
    AggregateAccumulator acc;
    acc.sum.assign(aggs.size(), 0.0);
    acc.count.assign(aggs.size(), 0);
    acc.minVal.resize(aggs.size());
    acc.hasMin.assign(aggs.size(), false);
    acc.maxVal.resize(aggs.size());
    acc.hasMax.assign(aggs.size(), false);
    return acc;
  };

  std::vector<std::string> ordered_keys;
  if (key_indices.size() == 1) {
    const auto key_column = viewValueColumn(input, key_indices[0]);
    if (canUseSingleStringKeyFastPath(*key_column.buffer)) {
      std::unordered_map<std::string_view, AggregateAccumulator,
                         std::hash<std::string_view>> states;
      std::vector<std::string_view> ordered_key_views;
      ordered_key_views.reserve(input.rowCount());
      for (std::size_t row_index = 0; row_index < input.rowCount(); ++row_index) {
        const auto key = singleStringKeyAt(*key_column.buffer, row_index);
        auto it = states.find(key);
        if (it == states.end()) {
          ordered_key_views.push_back(key);
          it = states.emplace(key, initAccumulator()).first;
        }
        auto& acc = it->second;
        for (std::size_t i = 0; i < aggs.size(); ++i) {
          const auto& agg = aggs[i];
          if (agg.function == AggregateFunction::Count) {
            acc.count[i] += 1;
            continue;
          }
          const auto& column = *aggregate_columns[i]->buffer;
          if (valueColumnIsNullAt(column, row_index)) {
            continue;
          }
          if (agg.function == AggregateFunction::Sum || agg.function == AggregateFunction::Avg) {
            acc.sum[i] += valueColumnDoubleAt(column, row_index);
            acc.count[i] += 1;
          } else if (agg.function == AggregateFunction::Min) {
            const auto v = valueColumnValueAt(column, row_index);
            if (!acc.hasMin[i] || v < acc.minVal[i]) {
              acc.minVal[i] = v;
              acc.hasMin[i] = true;
            }
          } else if (agg.function == AggregateFunction::Max) {
            const auto v = valueColumnValueAt(column, row_index);
            if (!acc.hasMax[i] || v > acc.maxVal[i]) {
              acc.maxVal[i] = v;
              acc.hasMax[i] = true;
            }
          }
        }
      }

      Table out;
      for (const auto idx : key_indices) {
        out.schema.fields.push_back(input.schema.fields[idx]);
      }
      for (const auto& agg : aggs) {
        out.schema.fields.push_back(agg.output_name);
      }
      for (std::size_t i = 0; i < out.schema.fields.size(); ++i) {
        out.schema.index[out.schema.fields[i]] = i;
      }
      auto cache = std::make_shared<ColumnarTable>();
      cache->schema = out.schema;
      cache->columns.resize(out.schema.fields.size());
      cache->arrow_formats.resize(out.schema.fields.size());
      cache->row_count = ordered_key_views.size();
      for (auto& column : cache->columns) {
        column.values.reserve(ordered_key_views.size());
      }
      for (const auto key_view : ordered_key_views) {
        const auto& acc = states.at(key_view);
        std::size_t out_column_index = 0;
        cache->columns[out_column_index++].values.push_back(Value(std::string(key_view)));
        for (std::size_t i = 0; i < aggs.size(); ++i) {
          const auto& agg = aggs[i];
          Value value;
          switch (agg.function) {
            case AggregateFunction::Sum:
              value = Value(acc.sum[i]);
              break;
            case AggregateFunction::Count:
              value = Value(static_cast<int64_t>(acc.count[i]));
              break;
            case AggregateFunction::Avg:
              value = Value(acc.count[i] == 0 ? 0.0
                                              : acc.sum[i] / static_cast<double>(acc.count[i]));
              break;
            case AggregateFunction::Min:
              value = acc.minVal[i];
              break;
            case AggregateFunction::Max:
              value = acc.maxVal[i];
              break;
          }
          cache->columns[out_column_index++].values.push_back(std::move(value));
        }
      }
      out.columnar_cache = std::move(cache);
      return out;
    }
  }

  std::unordered_map<std::string, AggregateAccumulator> states;
  const auto serialized_keys = materializeSerializedKeys(input, key_indices);
  for (std::size_t row_index = 0; row_index < serialized_keys.size(); ++row_index) {
    const std::string& key = serialized_keys[row_index];
    auto it = states.find(key);
    if (it == states.end()) {
      ordered_keys.push_back(key);
      it = states.emplace(key, initAccumulator()).first;
    }
    auto& acc = it->second;

    for (std::size_t i = 0; i < aggs.size(); ++i) {
      const auto& agg = aggs[i];
      if (agg.function == AggregateFunction::Count) {
        acc.count[i] += 1;
        continue;
      }
      const auto& column = *aggregate_columns[i]->buffer;
      if (valueColumnIsNullAt(column, row_index)) {
        continue;
      }
      if (agg.function == AggregateFunction::Sum || agg.function == AggregateFunction::Avg) {
        acc.sum[i] += valueColumnDoubleAt(column, row_index);
        acc.count[i] += 1;
      } else if (agg.function == AggregateFunction::Min) {
        const auto v = valueColumnValueAt(column, row_index);
        if (!acc.hasMin[i] || v < acc.minVal[i]) {
          acc.minVal[i] = v;
          acc.hasMin[i] = true;
        }
      } else if (agg.function == AggregateFunction::Max) {
        const auto v = valueColumnValueAt(column, row_index);
        if (!acc.hasMax[i] || v > acc.maxVal[i]) {
          acc.maxVal[i] = v;
          acc.hasMax[i] = true;
        }
      }
    }
  }

  Table out;
  for (const auto idx : key_indices) {
    out.schema.fields.push_back(input.schema.fields[idx]);
  }
  for (const auto& agg : aggs) {
    out.schema.fields.push_back(agg.output_name);
  }
  for (std::size_t i = 0; i < out.schema.fields.size(); ++i) {
    out.schema.index[out.schema.fields[i]] = i;
  }
  auto cache = std::make_shared<ColumnarTable>();
  cache->schema = out.schema;
  cache->columns.resize(out.schema.fields.size());
  cache->arrow_formats.resize(out.schema.fields.size());
  cache->row_count = ordered_keys.size();
  for (auto& column : cache->columns) {
    column.values.reserve(ordered_keys.size());
  }

  for (const auto& key : ordered_keys) {
    const auto& acc = states.at(key);
    std::size_t out_column_index = 0;
    if (!key_indices.empty()) {
      for (const auto& k : splitKey(key)) {
        Value value(k);
        cache->columns[out_column_index++].values.push_back(std::move(value));
      }
    }
    for (std::size_t i = 0; i < aggs.size(); ++i) {
      const auto& agg = aggs[i];
      Value value;
      switch (agg.function) {
        case AggregateFunction::Sum:
          value = Value(acc.sum[i]);
          break;
        case AggregateFunction::Count:
          value = Value(static_cast<int64_t>(acc.count[i]));
          break;
        case AggregateFunction::Avg:
          value = Value(acc.count[i] == 0 ? 0.0
                                          : acc.sum[i] / static_cast<double>(acc.count[i]));
          break;
        case AggregateFunction::Min:
          value = acc.minVal[i];
          break;
        case AggregateFunction::Max:
          value = acc.maxVal[i];
          break;
      }
      cache->columns[out_column_index++].values.push_back(std::move(value));
    }
  }
  out.columnar_cache = std::move(cache);
  return out;
}

RunnerExecutor::RunnerExecutor(std::shared_ptr<RpcRunner> runner,
                             std::shared_ptr<Executor> fallback,
                             bool dual_path)
    : runner_(std::move(runner)),
      fallback_(std::move(fallback)),
      dual_path_(dual_path) {
  if (!fallback_) {
    fallback_ = std::make_shared<LocalExecutor>();
  }
}

Table RunnerExecutor::execute(const PlanNodePtr& plan) const {
  Table local_result = fallback_->execute(plan);
  if (!runner_ || !dual_path_) {
    return local_result;
  }
  const uint64_t message_id = next_message_id_.fetch_add(1, std::memory_order_relaxed);
  const uint64_t correlation_id = message_id;
  if (!runner_->sendControl(message_id, correlation_id, "local", "remote", RpcMessageType::Control,
                           "RunRequest", "local_execute")) {
    return local_result;
  }
  if (!runner_->sendDataBatch(message_id + 1, correlation_id, "local", "remote", local_result)) {
    return local_result;
  }
  return local_result;
}

namespace {

Table executePlanWithRequirements(const LocalExecutor& executor, const PlanNodePtr& plan,
                                  const SourceRequirementMap& requirements) {
  if (!plan) return Table();

  switch (plan->kind) {
    case PlanKind::Source: {
      const auto* node = static_cast<const SourcePlan*>(plan.get());
      if (node->storage_kind == SourceStorageKind::InMemory) {
        return node->table;
      }
      return *loadCsvSource(*node, requirements);
    }
    case PlanKind::Select: {
      const auto* node = static_cast<const SelectPlan*>(plan.get());
      const auto input = borrowOrExecute(executor, node->child, requirements);
      return projectTable(*input.table, node->indices, node->aliases, false);
    }
    case PlanKind::Filter: {
      const auto* node = static_cast<const FilterPlan*>(plan.get());
      const SourcePlan* pushed_source = nullptr;
      SourcePushdownSpec pushed_spec;
      if (buildSourcePushdownSpec(plan, requirements, &pushed_source, &pushed_spec) &&
          !pushed_source->options.materialization.enabled) {
        Table pushed;
        if (tryExecuteSourcePushdown(*pushed_source, pushed_spec, false, &pushed)) {
          return pushed;
        }
      }
      const auto child_input = borrowOrExecute(executor, node->child, requirements);
      const auto input = viewValueColumn(*child_input.table, node->column_index);
      const auto selection = vectorizedFilterSelection(input, node->value, node->op);
      return filterTable(*child_input.table, selection, false);
    }
    case PlanKind::WithColumn: {
      const auto* node = static_cast<const WithColumnPlan*>(plan.get());
      const auto table_input = borrowOrExecute(executor, node->child, requirements);
      Table out = *table_input.table;
      if (node->function == ComputedColumnKind::Copy) {
        appendNamedColumn(&out, node->added_column,
                          materializeValueColumn(out, node->source_column_index), false);
      } else {
        appendNamedColumn(&out, node->added_column,
                          computeComputedColumnValues(&out, node->function, node->args), false);
      }
      return out;
    }
    case PlanKind::Drop: {
      const auto* node = static_cast<const DropPlan*>(plan.get());
      const auto input = borrowOrExecute(executor, node->child, requirements);
      return projectTable(*input.table, node->keep_indices, {}, false);
    }
    case PlanKind::Limit: {
      const auto* node = static_cast<const LimitPlan*>(plan.get());
      const SourcePlan* pushed_source = nullptr;
      SourcePushdownSpec pushed_spec;
      if (buildSourcePushdownSpec(plan, requirements, &pushed_source, &pushed_spec) &&
          !pushed_source->options.materialization.enabled) {
        Table pushed;
        if (tryExecuteSourcePushdown(*pushed_source, pushed_spec, false, &pushed)) {
          return pushed;
        }
      }
      if (node->child && node->child->kind == PlanKind::Filter) {
        const auto* filter = static_cast<const FilterPlan*>(node->child.get());
        const auto table_input = borrowOrExecute(executor, filter->child, requirements);
        const auto input = viewValueColumn(*table_input.table, filter->column_index);
        const auto selection =
            vectorizedFilterSelection(input, filter->value, filter->op, node->n);
        return filterTable(*table_input.table, selection, false);
      }
      const auto input = borrowOrExecute(executor, node->child, requirements);
      return limitTable(*input.table, node->n, false);
    }
    case PlanKind::OrderBy: {
      const auto* node = static_cast<const OrderByPlan*>(plan.get());
      const auto input = borrowOrExecute(executor, node->child, requirements);
      Table in = *input.table;
      return sortTable(in, node->indices, node->ascending);
    }
    case PlanKind::WindowAssign: {
      const auto* node = static_cast<const WindowAssignPlan*>(plan.get());
      const auto table_input = borrowOrExecute(executor, node->child, requirements);
      Table out = *table_input.table;
      const auto input = viewValueColumn(out, node->time_column_index);
      appendNamedColumn(&out, node->output_column,
                        vectorizedWindowStart(input, node->window_ms), false);
      return out;
    }
    case PlanKind::Aggregate:
    case PlanKind::GroupBySum: {
      std::vector<size_t> keyIdx;
      std::vector<AggregateSpec> aggs;

      if (plan->kind == PlanKind::Aggregate) {
        const auto* node = static_cast<const AggregatePlan*>(plan.get());
        keyIdx = node->keys;
        aggs = node->aggregates;
        if (node->child && node->child->kind == PlanKind::Source) {
          const auto* source = static_cast<const SourcePlan*>(node->child.get());
          if (!source->options.materialization.enabled) {
            Table fast_out;
            SourcePushdownSpec pushdown;
            pushdown.has_aggregate = true;
            pushdown.aggregate.keys = keyIdx;
            pushdown.aggregate.aggregates = aggs;
            if (tryExecuteSourcePushdown(*source, pushdown, false, &fast_out)) {
              return fast_out;
            }
          }
        }
        const auto input = borrowOrExecute(executor, node->child, requirements);
        return executeAggregateTable(*input.table, keyIdx, aggs);
      } else {
        const auto* node = static_cast<const GroupBySumPlan*>(plan.get());
        keyIdx = node->keys;
        AggregateSpec sumSpec;
        sumSpec.function = AggregateFunction::Sum;
        sumSpec.value_index = node->value_index;
        sumSpec.output_name = "sum";
        aggs.push_back(sumSpec);
        const auto input = borrowOrExecute(executor, node->child, requirements);
        return executeAggregateTable(*input.table, keyIdx, aggs);
      }
    }
    case PlanKind::Join: {
      const auto* node = static_cast<const JoinPlan*>(plan.get());
      const auto left_input = borrowOrExecute(executor, node->left, requirements);
      const auto right_input = borrowOrExecute(executor, node->right, requirements);
      const auto left_keys = materializeSerializedKeys(*left_input.table, {node->left_key});
      const auto right_keys = materializeSerializedKeys(*right_input.table, {node->right_key});
      const auto left_columns = viewValueColumns(*left_input.table, allColumnIndices(left_input.table->schema));
      const auto right_columns =
          viewValueColumns(*right_input.table, allColumnIndices(right_input.table->schema));
      Table out;
      out.schema.fields = left_input.table->schema.fields;
      out.schema.fields.insert(out.schema.fields.end(), right_input.table->schema.fields.begin(),
                               right_input.table->schema.fields.end());
      for (size_t i = 0; i < out.schema.fields.size(); ++i) out.schema.index[out.schema.fields[i]] = i;
      auto cache = std::make_shared<ColumnarTable>();
      cache->schema = out.schema;
      cache->columns.resize(out.schema.fields.size());
      cache->arrow_formats.resize(out.schema.fields.size());

      const auto rightBuckets = buildHashBuckets(right_keys);

      for (std::size_t left_index = 0; left_index < left_input.table->rowCount(); ++left_index) {
        auto hit = rightBuckets.find(left_keys[left_index]);
        if (hit != rightBuckets.end()) {
          for (const auto right_index : hit->second) {
            Row merged;
            merged.reserve(out.schema.fields.size());
            std::size_t out_column_index = 0;
            for (const auto& column : left_columns) {
              const auto value = valueColumnValueAt(*column.buffer, left_index);
              merged.push_back(value);
              cache->columns[out_column_index++].values.push_back(value);
            }
            for (const auto& column : right_columns) {
              const auto value = valueColumnValueAt(*column.buffer, right_index);
              merged.push_back(value);
              cache->columns[out_column_index++].values.push_back(value);
            }
            out.rows.push_back(std::move(merged));
          }
        } else if (node->kind == JoinKind::Left) {
          Row merged;
          merged.reserve(out.schema.fields.size());
          std::size_t out_column_index = 0;
          for (const auto& column : left_columns) {
            const auto value = valueColumnValueAt(*column.buffer, left_index);
            merged.push_back(value);
            cache->columns[out_column_index++].values.push_back(value);
          }
          for (size_t i = 0; i < right_input.table->schema.fields.size(); ++i) {
            Value value;
            merged.push_back(value);
            cache->columns[out_column_index++].values.push_back(std::move(value));
          }
          out.rows.push_back(std::move(merged));
        }
      }
      cache->row_count = out.rows.size();
      out.columnar_cache = std::move(cache);
      return out;
    }
    case PlanKind::Sink: {
      const auto* node = static_cast<const SinkPlan*>(plan.get());
      return executePlanWithRequirements(executor, node->child, requirements);
    }
    default:
      return Table();
  }
}

}  // namespace

Table LocalExecutor::execute(const PlanNodePtr& plan) const {
  SourceRequirementMap requirements;
  collectSourceRequirements(plan, allColumnIndices(planOutputWidth(plan)), &requirements);
  return executePlanWithRequirements(*this, plan, requirements);
}

}  // namespace dataflow
