#include "src/dataflow/core/contract/api/session.h"
#include "src/dataflow/core/logical/sql/frontend/pg_query_frontend.h"

#include <cstdlib>
#include <iostream>
#include <string>

static int run=0,pass=0,fail=0;
#define T(n) do{run++;std::cout<<"  "<<n<<"... ";}while(0)
#define P() do{pass++;std::cout<<"PASS"<<std::endl;}while(0)
#define F(m) do{fail++;std::cout<<"FAIL: "<<m<<std::endl;return;}while(0)
#define C(c,m) do{if(!(c))F(m);}while(0)

static void printDiagnostics(const dataflow::sql::SqlFrontendResult& result) {
  for (const auto& d : result.diagnostics) {
    std::cout << " diagnostic[" << d.error_type << "] " << d.message << std::endl;
  }
}

static void t_pg_only_ignores_env() {
  T("pg_only_ignores_env");
  setenv("VELARIA_SQL_FRONTEND","dual",1);
  auto& s=dataflow::DataflowSession::builder();
  auto df=s.sql("SELECT 1 AS n");
  C(df.toTable().rowCount()==1,"rows");
  C(s.sqlFrontendName()=="pg_query","name");
  unsetenv("VELARIA_SQL_FRONTEND"); P();
}

static void t_pg_lower_create_extensions() {
  T("pg_lower_create_extensions");
  dataflow::sql::PgQueryFrontend frontend;
  auto result=frontend.process(
      "CREATE SOURCE TABLE stream_events (key STRING, value INT) "
      "USING csv OPTIONS(path: '/tmp/stream-input', delimiter: ',')",
      dataflow::sql::SqlFeaturePolicy::cliDefault());
  C(result.diagnostics.empty(),"diagnostics");
  C(result.statement.kind==dataflow::sql::SqlStatementKind::CreateTable,"kind");
  C(result.statement.create.kind==dataflow::sql::TableKind::Source,"source");
  C(result.statement.create.provider=="csv","provider");
  C(result.statement.create.options.at("path")=="/tmp/stream-input","path");
  P();
}

static void t_pg_lower_search_and_window_extensions() {
  T("pg_lower_search_and_window_extensions");
  dataflow::sql::PgQueryFrontend frontend;
  auto keyword=frontend.process(
      "SELECT id, title FROM docs WHERE bucket = 1 "
      "KEYWORD SEARCH(title, body) QUERY 'payment timeout' TOP_K 20",
      dataflow::sql::SqlFeaturePolicy::cliDefault());
  if (!keyword.diagnostics.empty()) printDiagnostics(keyword);
  C(keyword.diagnostics.empty(),"keyword_diagnostics");
  C(keyword.statement.query.keyword_search.has_value(),"keyword");
  C(keyword.statement.query.keyword_search->columns.size()==2,"keyword_columns");
  C(keyword.statement.query.keyword_search->query_text=="payment timeout","keyword_query");

  auto window=frontend.process(
      "SELECT window_start, key, COUNT(*) AS n FROM stream_events "
      "WINDOW BY ts EVERY 60000 SLIDE 30000 AS window_start GROUP BY window_start, key",
      dataflow::sql::SqlFeaturePolicy::cliDefault());
  C(window.diagnostics.empty(),"window_diagnostics");
  C(window.statement.query.window.has_value(),"window");
  C(window.statement.query.window->every_ms==60000,"every");
  C(window.statement.query.window->slide_ms==30000,"slide");
  P();
}

static void t_pg_lower_ast_parity() {
  T("pg_lower_ast_parity");
  dataflow::sql::PgQueryFrontend frontend;
  auto query=frontend.process(
      "SELECT *, u.*, 1 AS one, 'x' AS sx, u.region, SUM(a.score) AS total, "
      "LOWER(u.region) AS lower_region, CAST(a.score AS STRING) AS score_text "
      "FROM users u LEFT JOIN actions a ON u.user_id = a.user_id "
      "WHERE a.score > 0 GROUP BY u.region, LOWER(u.region) "
      "HAVING SUM(a.score) > 15 ORDER BY total DESC LIMIT 5",
      dataflow::sql::SqlFeaturePolicy::cliDefault());
  if (!query.diagnostics.empty()) printDiagnostics(query);
  C(query.diagnostics.empty(),"query_diagnostics");
  C(query.statement.kind==dataflow::sql::SqlStatementKind::Select,"select_kind");
  C(query.statement.query.join.has_value(),"join_present");
  C(query.statement.query.join->is_left,"left_join");
  C(query.statement.query.join->left_key.qualifier=="u","left_key_qualifier");
  C(query.statement.query.join->right_key.qualifier=="a","right_key_qualifier");
  C(query.statement.query.select_items.size()==8,"select_item_count");
  C(query.statement.query.select_items[0].is_all,"star");
  C(query.statement.query.select_items[1].is_table_all,"table_star");
  C(query.statement.query.select_items[1].table_name_or_alias=="u","table_star_name");
  C(query.statement.query.select_items[2].is_literal,"literal_int");
  C(query.statement.query.select_items[3].is_literal,"literal_string");
  C(query.statement.query.select_items[5].is_aggregate,"aggregate");
  C(query.statement.query.select_items[6].is_string_function,"scalar_function");
  C(query.statement.query.select_items[7].is_string_function,"cast_function");
  C(query.statement.query.group_by.size()==2,"group_by_count");
  C(query.statement.query.group_by[1].is_string_function,"group_by_scalar_function");
  C(query.statement.query.having!=nullptr,"having");
  C(query.statement.query.having->predicate.lhs_is_aggregate,"having_aggregate");
  C(query.statement.query.order_by.size()==1,"order_by_count");
  C(query.statement.query.order_by[0].column.name=="total","order_by_alias");
  C(query.statement.query.limit.value_or(0)==5,"limit");

  auto insert=frontend.process(
      "INSERT INTO t VALUES (1, 'a', TRUE, 3.5, NULL), (2, 'b', FALSE, 4.25, NULL)",
      dataflow::sql::SqlFeaturePolicy::cliDefault());
  if (!insert.diagnostics.empty()) printDiagnostics(insert);
  C(insert.diagnostics.empty(),"insert_diagnostics");
  C(insert.statement.kind==dataflow::sql::SqlStatementKind::InsertValues,"insert_values_kind");
  C(insert.statement.insert.values.size()==2,"insert_row_count");
  C(insert.statement.insert.values[0].size()==5,"insert_column_count");
  C(insert.statement.insert.values[0][0].asInt64()==1,"insert_int");
  C(insert.statement.insert.values[0][1].asString()=="a","insert_string");
  C(insert.statement.insert.values[0][2].asBool(),"insert_bool");
  C(insert.statement.insert.values[0][3].asDouble()==3.5,"insert_double");
  C(insert.statement.insert.values[0][4].isNull(),"insert_null");

  auto union_all=frontend.process(
      "SELECT id FROM left_t UNION ALL SELECT id FROM right_t",
      dataflow::sql::SqlFeaturePolicy::cliDefault());
  C(union_all.diagnostics.empty(),"union_all_diagnostics");
  C(union_all.statement.query.union_terms.size()==1,"union_all_term");
  C(union_all.statement.query.union_terms[0].all,"union_all_flag");

  auto intersect=frontend.process(
      "SELECT id FROM left_t INTERSECT SELECT id FROM right_t",
      dataflow::sql::SqlFeaturePolicy::cliDefault());
  C(!intersect.diagnostics.empty(),"intersect_rejected");

  auto except=frontend.process(
      "SELECT id FROM left_t EXCEPT SELECT id FROM right_t",
      dataflow::sql::SqlFeaturePolicy::cliDefault());
  C(!except.diagnostics.empty(),"except_rejected");

  auto non_eq_join=frontend.process(
      "SELECT u.id FROM users u INNER JOIN actions a ON u.id > a.user_id",
      dataflow::sql::SqlFeaturePolicy::cliDefault());
  C(!non_eq_join.diagnostics.empty(),"non_eq_join_rejected");
  P();
}

static void t_pg_e2e_select_from_where_group_limit() {
  T("pg_e2e_select_from_where_group_limit");
  auto& s=dataflow::DataflowSession::builder();
  dataflow::Table t;
  t.schema.fields = {"name", "score"};
  t.schema.index["name"] = 0;
  t.schema.index["score"] = 1;
  t.rows.push_back({dataflow::Value(std::string("alice")), dataflow::Value(static_cast<int64_t>(10))});
  t.rows.push_back({dataflow::Value(std::string("bob")), dataflow::Value(static_cast<int64_t>(20))});
  t.rows.push_back({dataflow::Value(std::string("alice")), dataflow::Value(static_cast<int64_t>(30))});
  auto df = s.createDataFrame(std::move(t));
  s.createTempView("people_pg_only", df);
  auto result = s.sql(
    "SELECT name, SUM(score) AS total FROM people_pg_only WHERE score > 0 GROUP BY name ORDER BY total DESC LIMIT 10");
  auto table = result.toTable();
  C(table.rowCount()==2,"row_count");
  bool found_alice = false, found_bob = false;
  for (size_t i = 0; i < table.rowCount(); i++) {
    std::string name = table.rows[i][0].asString();
    int64_t total = table.rows[i][1].asInt64();
    if (name == "alice") { C(total==40,"alice_total"); found_alice = true; }
    if (name == "bob")   { C(total==20,"bob_total"); found_bob = true; }
  }
  C(found_alice && found_bob,"both_groups_found");
  P();
}

int main() {
  std::cout<<"=== Pg-only Frontend Regression Tests ==="<<std::endl;
  t_pg_only_ignores_env();
  t_pg_lower_create_extensions();
  t_pg_lower_search_and_window_extensions();
  t_pg_lower_ast_parity();
  t_pg_e2e_select_from_where_group_limit();
  std::cout<<std::endl<<"Run: "<<run<<" Pass: "<<pass<<" Fail: "<<fail<<std::endl;
  return fail>0?1:0;
}
