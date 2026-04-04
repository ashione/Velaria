#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "src/dataflow/core/execution/table.h"
#include "src/dataflow/core/execution/source_materialization.h"

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
  GroupBySum,
  Aggregate,
  Join,
  Sink,
  OrderBy
};
enum class SourceStorageKind { InMemory, CsvFile };
enum class JoinKind { Inner, Left, Right, Full };
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

struct GroupBySumPlan : PlanNode {
  PlanNodePtr child;
  std::vector<size_t> keys;
  size_t value_index;
  GroupBySumPlan(PlanNodePtr p, std::vector<size_t> ks, size_t vid)
      : PlanNode(PlanKind::GroupBySum), child(std::move(p)), keys(std::move(ks)), value_index(vid) {}
};

enum class AggregateFunction { Sum, Count, Avg, Min, Max };

struct AggregateSpec {
  AggregateFunction function;
  size_t value_index;
  std::string output_name;
};

struct AggregatePlan : PlanNode {
  PlanNodePtr child;
  std::vector<size_t> keys;
  std::vector<AggregateSpec> aggregates;
  AggregatePlan(PlanNodePtr p, std::vector<size_t> ks, std::vector<AggregateSpec> aggs)
      : PlanNode(PlanKind::Aggregate),
        child(std::move(p)),
        keys(std::move(ks)),
        aggregates(std::move(aggs)) {}
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
