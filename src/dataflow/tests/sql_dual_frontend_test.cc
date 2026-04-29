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
  t_pg_e2e_select_from_where_group_limit();
  std::cout<<std::endl<<"Run: "<<run<<" Pass: "<<pass<<" Fail: "<<fail<<std::endl;
  return fail>0?1:0;
}
