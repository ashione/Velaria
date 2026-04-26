#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "src/dataflow/core/execution/value.h"

namespace dataflow {
namespace sql {

struct UnboundColumnRef {
  std::optional<std::string> qualifier;
  std::string name;
};

struct UnboundAggregateExpr {
  enum Function { Sum, Count, Avg, Min, Max };
  Function function;
  std::optional<UnboundColumnRef> argument;
  bool count_all = false;
  std::optional<std::string> alias;
};

struct UnboundFunctionExpr {
  std::string name;
  std::vector<Value> args;
  std::optional<std::string> alias;
};

struct UnboundLiteralExpr {
  Value value;
  std::optional<std::string> alias;
};

struct UnboundStarExpr {
  bool table_star = false;
  std::optional<std::string> qualifier;
};

using UnboundSelectItem = std::variant<
    UnboundColumnRef, UnboundAggregateExpr, UnboundFunctionExpr,
    UnboundLiteralExpr, UnboundStarExpr>;

struct UnboundPredicate {
  enum Op { Eq, Ne, Lt, Lte, Gt, Gte, And, Or, Not };
  Op op = Eq;
  std::optional<UnboundColumnRef> lhs;
  std::optional<Value> rhs_literal;
  std::optional<UnboundColumnRef> rhs_column;
  std::shared_ptr<struct UnboundPredicateNode> left;
  std::shared_ptr<struct UnboundPredicateNode> right;
};

struct UnboundPredicateNode {
  UnboundPredicate pred;
};

struct UnboundPlanNode {
  enum Kind { Scan, Filter, Project, Aggregate, Order, Limit };
  Kind kind = Scan;
  std::string source_name;
  std::optional<std::string> source_alias;
  std::shared_ptr<UnboundPredicateNode> filter_expr;
  std::vector<UnboundSelectItem> select_items;
  std::vector<UnboundColumnRef> group_keys;
  std::vector<UnboundAggregateExpr> aggregates;
  std::vector<std::pair<UnboundColumnRef, bool>> order_items;
  std::optional<std::size_t> limit_count;
  std::optional<std::size_t> limit_offset;
};

struct UnboundPlan {
  std::vector<UnboundPlanNode> nodes;
};

}  // namespace sql
}  // namespace dataflow
