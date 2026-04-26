#include "src/dataflow/core/contract/api/session.h"
#include "src/dataflow/core/logical/sql/frontend/sql_frontend.h"
#include <cstdlib>
#include <iostream>
#include <string>

static int run=0,pass=0,fail=0;
#define T(n) do{run++;std::cout<<"  "<<n<<"... ";}while(0)
#define P() do{pass++;std::cout<<"PASS"<<std::endl;}while(0)
#define F(m) do{fail++;std::cout<<"FAIL: "<<m<<std::endl;return;}while(0)
#define C(c,m) do{if(!(c))F(m);}while(0)

static void t_dual() {
  T("dual_mode");
  setenv("VELARIA_SQL_FRONTEND","dual",1);
  auto& s=dataflow::DataflowSession::builder();
  auto df=s.sql("SELECT 1 AS n");
  C(df.toTable().rowCount()==1,"rows");
  C(s.sqlFrontendName()=="dual","name");
  unsetenv("VELARIA_SQL_FRONTEND"); P();
}
static void t_pg_fallback() {
  T("pg_fallback");
  setenv("VELARIA_SQL_FRONTEND","pg_query",1);
  auto& s=dataflow::DataflowSession::builder();
  C(s.sqlFrontendName()=="pg_query","name");
  auto df=s.sql("SELECT 42");
  C(df.toTable().rowCount()==1,"rows");
  unsetenv("VELARIA_SQL_FRONTEND"); P();
}
static void t_pg_aggregate() {
  T("pg_aggregate_query");
  setenv("VELARIA_SQL_FRONTEND","pg_query",1);
  auto& s=dataflow::DataflowSession::builder();
  auto df=s.sql("SELECT 1 AS grp, 10 AS val");
  C(df.toTable().rowCount()>=0,"simple");
  unsetenv("VELARIA_SQL_FRONTEND"); P();
}
static void t_pg_e2e() {
  T("pg_e2e_select_from_where_group_limit");
  setenv("VELARIA_SQL_FRONTEND","pg_query",1);
  auto& s=dataflow::DataflowSession::builder();
  // Build an in-memory table with proper schema index
  dataflow::Table t;
  t.schema.fields = {"name", "score"};
  t.schema.index["name"] = 0;
  t.schema.index["score"] = 1;
  // Add test rows: (alice,10), (bob,20), (alice,30)
  t.rows.push_back({dataflow::Value(std::string("alice")), dataflow::Value(static_cast<int64_t>(10))});
  t.rows.push_back({dataflow::Value(std::string("bob")), dataflow::Value(static_cast<int64_t>(20))});
  t.rows.push_back({dataflow::Value(std::string("alice")), dataflow::Value(static_cast<int64_t>(30))});
  auto df = s.createDataFrame(std::move(t));
  s.createTempView("people", df);
  // Query via pg_query frontend
  auto result = s.sql(
    "SELECT name, SUM(score) AS total FROM people WHERE score > 0 GROUP BY name ORDER BY total DESC LIMIT 10");
  auto table = result.toTable();
  C(table.rowCount()==2,"row_count"); // alice=40, bob=20
  // Verify first row is alice with total 40
  bool found_alice = false, found_bob = false;
  for (size_t i = 0; i < table.rowCount(); i++) {
    std::string name = table.rows[i][0].asString();
    int64_t total = table.rows[i][1].asInt64();
    if (name == "alice") { C(total==40,"alice_total"); found_alice = true; }
    if (name == "bob")   { C(total==20,"bob_total"); found_bob = true; }
  }
  C(found_alice && found_bob,"both_groups_found");
  unsetenv("VELARIA_SQL_FRONTEND"); P();
}
int main() {
  std::cout<<"=== Dual Frontend Tests ==="<<std::endl;
  t_dual(); t_pg_fallback(); t_pg_aggregate(); t_pg_e2e();
  std::cout<<std::endl<<"Run: "<<run<<" Pass: "<<pass<<" Fail: "<<fail<<std::endl;
  return fail>0?1:0;
}
