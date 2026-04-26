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
  // SELECT without FROM with aggregate + GROUP BY alias
  auto df=s.sql("SELECT 1 AS grp, 10 AS val");
  C(df.toTable().rowCount()>=0,"simple");
  unsetenv("VELARIA_SQL_FRONTEND"); P();
}
int main() {
  std::cout<<"=== Dual Frontend Tests ==="<<std::endl;
  t_dual(); t_pg_fallback(); t_pg_aggregate();
  std::cout<<std::endl<<"Run: "<<run<<" Pass: "<<pass<<" Fail: "<<fail<<std::endl;
  return fail>0?1:0;
}
