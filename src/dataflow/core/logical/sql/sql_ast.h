#pragma once

#include <memory>
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
  Day,
  IsoYear,
  IsoWeek,
  Week,
  YearWeek,
  Now,
  Today,
  CurrentTimestamp,
  UnixTimestamp,
  Cast
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

struct StringFunctionExpr;

struct StringFunctionArg {
  bool is_column = false;
  bool is_function = false;
  ColumnRef column;
  Value literal;
  std::shared_ptr<StringFunctionExpr> function;
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

struct GroupByExpr {
  bool is_string_function = false;
  ColumnRef column;
  StringFunctionExpr string_function;
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
  bool rhs_is_column_candidate = false;
  ColumnRef lhs;
  ColumnRef rhs_column;
  AggregateExpr lhs_aggregate;
  Value rhs;
  BinaryOperatorKind op = BinaryOperatorKind::Eq;
};

enum class PredicateExprKind { Comparison, And, Or };

struct PredicateExpr {
  PredicateExprKind kind = PredicateExprKind::Comparison;
  Predicate predicate;
  std::shared_ptr<PredicateExpr> left;
  std::shared_ptr<PredicateExpr> right;
};

struct WindowSpec {
  ColumnRef time_column;
  uint64_t every_ms = 0;
  uint64_t slide_ms = 0;
  std::string output_column = "window_start";
};

struct HybridSearchSpec {
  ColumnRef vector_column;
  std::string query_vector;
  std::string metric = "cosine";
  std::size_t top_k = 10;
  std::optional<double> score_threshold;
};

struct KeywordSearchSpec {
  std::vector<ColumnRef> columns;
  std::string query_text;
  std::size_t top_k = 10;
};

struct SqlColumnDef {
  std::string name;
  std::string type;
};

struct SqlQuery;

struct SqlUnionTerm {
  bool all = false;
  std::shared_ptr<SqlQuery> query;
};

struct SqlQuery {
  std::vector<SelectItem> select_items;
  bool has_from = false;
  FromItem from;
  std::optional<JoinItem> join;
  std::shared_ptr<PredicateExpr> where;
  std::optional<KeywordSearchSpec> keyword_search;
  std::optional<HybridSearchSpec> hybrid_search;
  std::optional<WindowSpec> window;
  std::vector<GroupByExpr> group_by;
  std::shared_ptr<PredicateExpr> having;
  std::vector<OrderByItem> order_by;
  std::optional<std::size_t> limit;
  std::vector<SqlUnionTerm> union_terms;
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
