#pragma once

#include <optional>
#include <string>
#include <vector>

#include "src/dataflow/core/value.h"

namespace dataflow {
namespace sql {

enum class AggregateFunctionKind { Sum, Count, Avg, Min, Max };
enum class BinaryOperatorKind { Eq, Ne, Lt, Lte, Gt, Gte };

struct ColumnRef {
  std::string qualifier;
  std::string name;
};

struct AggregateExpr {
  AggregateFunctionKind function = AggregateFunctionKind::Sum;
  ColumnRef argument;
  bool count_all = false;
  std::string alias;
};

struct SelectItem {
  bool is_all = false;
  bool is_table_all = false;
  std::string table_name_or_alias;
  bool is_aggregate = false;
  bool is_literal = false;
  ColumnRef column;
  AggregateExpr aggregate;
  Value literal;
  std::string alias;
};

struct FromItem {
  std::string name;
  std::string alias;
};

struct JoinItem {
  bool is_left = false;
  FromItem right;
  ColumnRef left_key;
  ColumnRef right_key;
};

struct Predicate {
  ColumnRef lhs;
  Value rhs;
  BinaryOperatorKind op = BinaryOperatorKind::Eq;
};

struct SqlQuery {
  std::vector<SelectItem> select_items;
  bool has_from = false;
  FromItem from;
  std::optional<JoinItem> join;
  std::optional<Predicate> where;
  std::vector<ColumnRef> group_by;
  std::optional<Predicate> having;
  std::optional<std::size_t> limit;
};

}  // namespace sql
}  // namespace dataflow
