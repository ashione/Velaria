#pragma once

#include <memory>
#include <string>
#include <vector>

#include "src/dataflow/core/table.h"

namespace dataflow {

struct PlanNode;
using PlanNodePtr = std::shared_ptr<PlanNode>;

enum class PlanKind { Source, Select, Filter, WithColumn, Drop, Limit, GroupBySum, Aggregate, Join };
enum class JoinKind { Inner, Left, Right, Full };

struct PlanNode {
  explicit PlanNode(PlanKind k) : kind(k) {}
  virtual ~PlanNode() = default;
  PlanKind kind;
};

struct SourcePlan : PlanNode {
  std::string source_name;
  Table table;
  explicit SourcePlan(std::string name, Table t)
      : PlanNode(PlanKind::Source), source_name(std::move(name)), table(std::move(t)) {}
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
  WithColumnPlan(PlanNodePtr p, std::string col, size_t idx)
      : PlanNode(PlanKind::WithColumn),
        child(std::move(p)),
        added_column(std::move(col)),
        source_column_index(idx) {}
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

std::string serializePlan(const PlanNodePtr& plan);
PlanNodePtr deserializePlan(const std::string& payload);

}  // namespace dataflow
