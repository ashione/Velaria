#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "src/dataflow/core/execution/file_source.h"
#include "src/dataflow/core/execution/source_materialization.h"
#include "src/dataflow/core/execution/table.h"

namespace dataflow {

struct PlanNode;
using PlanNodePtr = std::shared_ptr<PlanNode>;

enum class PlanKind {
  Source,
  Select,
  Filter,
  WithColumn,
  Drop,
  Limit,
  WindowAssign,
  Aggregate,
  Join,
  Sink,
  OrderBy
};
enum class SourceStorageKind { InMemory, CsvFile };
enum class JoinKind { Inner, Left, Right, Full };
enum class AggregateFunction { Sum, Count, Avg, Min, Max };
enum class ComputedColumnKind {
  Copy,
  StringLength,
  StringLower,
  StringUpper,
  StringTrim,
  StringConcat,
  StringReverse,
  StringConcatWs,
  StringLeft,
  StringRight,
  StringPosition,
  StringSubstr,
  StringLtrim,
  StringRtrim,
  StringReplace,
  NumericAbs,
  NumericCeil,
  NumericFloor,
  NumericRound,
  DateYear,
  DateMonth,
  DateDay
};

enum class AggImplKind {
  Dense,
  HashPacked,
  HashFixed,
  HashRef,
  SortStreaming,
  LocalGlobal,
};

enum class AggregatePartialLayoutKind {
  GenericTable,
  KeyColumnar,
};

struct KeyLayoutSpec {
  std::vector<std::size_t> normalized_key_indices;
  bool nullable_encoded = false;
  bool fixed_width = false;
  bool packed = false;
  bool ordered_input = false;
  bool low_cardinality = false;
  std::vector<std::string> transforms;
};

struct AggregatePropertySet {
  std::size_t key_count = 0;
  bool has_nullable_keys = false;
  bool all_fixed_width = false;
  bool all_int64_like = false;
  bool all_string_like = false;
  bool packable = false;
  bool ordered_input = false;
  bool partition_local = false;
  bool low_cardinality = false;
};

struct AggregateRuntimeFeedback {
  std::size_t input_rows = 0;
  std::size_t output_groups = 0;
  std::size_t rehash_count = 0;
  std::size_t collision_count = 0;
  double dense_fill_ratio = 0.0;
  double local_compression_ratio = 0.0;
  bool fallback_used = false;
  bool observed_ordered_input = false;
  std::string fallback_reason;
};

struct AggregateExecSpec {
  AggImplKind impl_kind = AggImplKind::HashRef;
  AggregatePartialLayoutKind partial_layout = AggregatePartialLayoutKind::GenericTable;
  KeyLayoutSpec key_layout;
  AggregatePropertySet properties;
  bool use_local_global = false;
  bool input_requires_sort = false;
  std::size_t expected_groups = 0;
  std::size_t reserved_buckets = 0;
  std::string reason;
  std::vector<std::string> rejected_candidates;
};

struct AggregateSpec {
  AggregateFunction function;
  size_t value_index;
  std::string output_name;
};

struct SourceAggregatePushdownSpec {
  std::vector<size_t> keys;
  std::vector<AggregateSpec> aggregates;
};

struct SourceFilterPushdownSpec {
  bool enabled = false;
  size_t column_index = 0;
  Value value;
  std::string op;
};

struct SourcePushdownSpec {
  std::vector<std::size_t> projected_columns;
  SourceFilterPushdownSpec filter;
  std::size_t limit = 0;
  bool has_aggregate = false;
  SourceAggregatePushdownSpec aggregate;
};

struct ComputedColumnArg {
  bool is_literal = false;
  Value literal;
  size_t source_column_index = 0;
  std::string source_column_name;
};

struct PlanNode {
  explicit PlanNode(PlanKind k) : kind(k) {}
  virtual ~PlanNode() = default;
  PlanKind kind;
};

struct SourcePlan : PlanNode {
  std::string source_name;
  SourceStorageKind storage_kind = SourceStorageKind::InMemory;
  Schema schema;
  Table table;
  std::string csv_path;
  char csv_delimiter = ',';
  FileSourceKind file_source_kind = FileSourceKind::Csv;
  LineFileOptions line_options;
  JsonFileOptions json_options;
  SourceOptions options;
  mutable std::mutex cached_table_mu;
  mutable std::shared_ptr<Table> cached_table;
  mutable std::vector<std::size_t> cached_projected_indices;

  explicit SourcePlan(std::string name, Table t)
      : PlanNode(PlanKind::Source),
        source_name(std::move(name)),
        storage_kind(SourceStorageKind::InMemory),
        schema(t.schema),
        table(std::move(t)) {}

  SourcePlan(std::string name, std::string path, char delimiter, Schema source_schema,
             SourceOptions source_options = {})
      : PlanNode(PlanKind::Source),
        source_name(std::move(name)),
        storage_kind(SourceStorageKind::CsvFile),
        schema(std::move(source_schema)),
        csv_path(std::move(path)),
        csv_delimiter(delimiter),
        options(std::move(source_options)) {}

  SourcePlan(std::string name, std::string path, Schema source_schema,
             LineFileOptions source_line_options, SourceOptions source_options = {})
      : PlanNode(PlanKind::Source),
        source_name(std::move(name)),
        storage_kind(SourceStorageKind::CsvFile),
        schema(std::move(source_schema)),
        csv_path(std::move(path)),
        csv_delimiter(','),
        file_source_kind(FileSourceKind::Line),
        line_options(std::move(source_line_options)),
        options(std::move(source_options)) {}

  SourcePlan(std::string name, std::string path, Schema source_schema,
             JsonFileOptions source_json_options, SourceOptions source_options = {})
      : PlanNode(PlanKind::Source),
        source_name(std::move(name)),
        storage_kind(SourceStorageKind::CsvFile),
        schema(std::move(source_schema)),
        csv_path(std::move(path)),
        csv_delimiter(','),
        file_source_kind(FileSourceKind::Json),
        json_options(std::move(source_json_options)),
        options(std::move(source_options)) {}
};

struct SelectPlan : PlanNode {
  PlanNodePtr child;
  std::vector<size_t> indices;
  std::vector<std::string> aliases;
  explicit SelectPlan(PlanNodePtr p, std::vector<size_t> idx, std::vector<std::string> aliases = {})
      : PlanNode(PlanKind::Select), child(std::move(p)), indices(std::move(idx)),
        aliases(std::move(aliases)) {}
};

struct FilterPlan : PlanNode {
  PlanNodePtr child;
  size_t column_index;
  Value value;
  std::string op;
  bool (*pred)(const Value& lhs, const Value& rhs);
  FilterPlan(PlanNodePtr p, size_t cidx, Value v, std::string op_name,
             bool (*pfn)(const Value&, const Value&))
      : PlanNode(PlanKind::Filter),
        child(std::move(p)),
        column_index(cidx),
        value(std::move(v)),
        op(std::move(op_name)),
        pred(pfn) {}
};

struct WithColumnPlan : PlanNode {
  PlanNodePtr child;
  std::string added_column;
  size_t source_column_index;
  ComputedColumnKind function;
  std::vector<ComputedColumnArg> args;
  WithColumnPlan(PlanNodePtr p, std::string col, size_t idx)
      : PlanNode(PlanKind::WithColumn),
        child(std::move(p)),
        added_column(std::move(col)),
        source_column_index(idx),
        function(ComputedColumnKind::Copy) {}
  WithColumnPlan(PlanNodePtr p, std::string col, ComputedColumnKind fn,
                std::vector<ComputedColumnArg> args)
      : PlanNode(PlanKind::WithColumn),
        child(std::move(p)),
        added_column(std::move(col)),
        source_column_index(static_cast<size_t>(-1)),
        function(fn),
        args(std::move(args)) {}
};

struct DropPlan : PlanNode {
  PlanNodePtr child;
  std::vector<size_t> keep_indices;
  DropPlan(PlanNodePtr p, std::vector<size_t> keep)
      : PlanNode(PlanKind::Drop), child(std::move(p)), keep_indices(std::move(keep)) {}
};

struct LimitPlan : PlanNode {
  PlanNodePtr child;
  size_t n;
  LimitPlan(PlanNodePtr p, size_t limit) : PlanNode(PlanKind::Limit), child(std::move(p)), n(limit) {}
};

struct OrderByPlan : PlanNode {
  PlanNodePtr child;
  std::vector<size_t> indices;
  std::vector<bool> ascending;
  OrderByPlan(PlanNodePtr p, std::vector<size_t> idx, std::vector<bool> asc)
      : PlanNode(PlanKind::OrderBy),
        child(std::move(p)),
        indices(std::move(idx)),
        ascending(std::move(asc)) {}
};

struct WindowAssignPlan : PlanNode {
  PlanNodePtr child;
  size_t time_column_index;
  uint64_t window_ms;
  std::string output_column;
  WindowAssignPlan(PlanNodePtr p, size_t cidx, uint64_t w, std::string output)
      : PlanNode(PlanKind::WindowAssign),
        child(std::move(p)),
        time_column_index(cidx),
        window_ms(w),
        output_column(std::move(output)) {}
};

struct AggregatePlan : PlanNode {
  PlanNodePtr child;
  std::vector<size_t> keys;
  std::vector<AggregateSpec> aggregates;
  AggregateExecSpec exec_spec;
  AggregateRuntimeFeedback runtime_feedback;
  bool has_exec_spec = false;
  bool has_runtime_feedback = false;
  AggregatePlan(PlanNodePtr p, std::vector<size_t> ks, std::vector<AggregateSpec> aggs,
                AggregateExecSpec spec = {})
      : PlanNode(PlanKind::Aggregate),
        child(std::move(p)),
        keys(std::move(ks)),
        aggregates(std::move(aggs)),
        exec_spec(std::move(spec)) {}
};

struct JoinPlan : PlanNode {
  PlanNodePtr left;
  PlanNodePtr right;
  size_t left_key;
  size_t right_key;
  JoinKind kind;
  JoinPlan(PlanNodePtr l, PlanNodePtr r, size_t lk, size_t rk, JoinKind k)
      : PlanNode(PlanKind::Join), left(std::move(l)), right(std::move(r)), left_key(lk), right_key(rk), kind(k) {}
};

struct SinkPlan : PlanNode {
  PlanNodePtr child;
  std::string sink_name;
  explicit SinkPlan(PlanNodePtr p, std::string sink)
      : PlanNode(PlanKind::Sink), child(std::move(p)), sink_name(std::move(sink)) {}
};

std::string serializePlan(const PlanNodePtr& plan);
PlanNodePtr deserializePlan(const std::string& payload);

}  // namespace dataflow
