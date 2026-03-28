#pragma once

#include <optional>
#include <string>
#include <vector>

#include "src/dataflow/core/table.h"
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
  bool lhs_is_aggregate = false;
  ColumnRef lhs;
  AggregateExpr lhs_aggregate;
  Value rhs;
  BinaryOperatorKind op = BinaryOperatorKind::Eq;
};

struct SqlColumnDef {
  std::string name;
  std::string type;
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

enum class SqlStatementKind { Select, CreateTable, InsertValues, InsertSelect };
enum class TableKind { Regular, Source, Sink };

struct CreateTableStmt {
  std::string table;
  std::vector<SqlColumnDef> columns;
  TableKind kind = TableKind::Regular;
};

struct InsertStmt {
  std::string table;
  std::vector<std::string> columns;
  SqlQuery query;
  std::vector<Row> values;
  bool select_from = false;
};

struct SqlStatement {
  SqlStatementKind kind = SqlStatementKind::Select;
  SqlQuery query;
  CreateTableStmt create;
  InsertStmt insert;
};

}  // namespace sql
}  // namespace dataflow
