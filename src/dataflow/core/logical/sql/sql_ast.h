#pragma once

#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "src/dataflow/core/execution/table.h"
#include "src/dataflow/core/execution/value.h"

namespace dataflow {
namespace sql {

enum class AggregateFunctionKind { Sum, Count, Avg, Min, Max, StdDev };
enum class BinaryOperatorKind { Eq, Ne, Lt, Lte, Gt, Gte };
enum class StringFunctionKind {
  Length,
  Lower,
  Upper,
  Trim,
  Concat,
  Reverse,
  ConcatWs,
  Left,
  Right,
  Substr,
  Ltrim,
  Rtrim,
  Position,
  Replace,
  Abs,
  Ceil,
  Floor,
  Round,
  Year,
  Month,
  Day
};

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

struct StringFunctionArg {
  bool is_column = false;
  ColumnRef column;
  Value literal;
};

struct StringFunctionExpr {
  StringFunctionKind function = StringFunctionKind::Lower;
  std::vector<StringFunctionArg> args;
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
  bool is_string_function = false;
  StringFunctionExpr string_function;
  std::string alias;
};

struct OrderByItem {
  ColumnRef column;
  bool ascending = true;
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

struct WindowSpec {
  ColumnRef time_column;
  uint64_t every_ms = 0;
  std::string output_column = "window_start";
};

struct HybridSearchSpec {
  ColumnRef vector_column;
  std::string query_vector;
  std::string metric = "cosine";
  std::size_t top_k = 10;
  std::optional<double> score_threshold;
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
  std::optional<HybridSearchSpec> hybrid_search;
  std::optional<WindowSpec> window;
  std::vector<ColumnRef> group_by;
  std::optional<Predicate> having;
  std::vector<OrderByItem> order_by;
  std::optional<std::size_t> limit;
};

enum class SqlStatementKind { Select, CreateTable, InsertValues, InsertSelect };
enum class TableKind { Regular, Source, Sink };

struct CreateTableStmt {
  std::string table;
  std::vector<SqlColumnDef> columns;
  TableKind kind = TableKind::Regular;
  std::string provider;
  std::unordered_map<std::string, std::string> options;
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
