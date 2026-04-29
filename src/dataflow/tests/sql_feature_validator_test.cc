#include "src/dataflow/core/logical/sql/frontend/sql_feature_validator.h"
#include <iostream>

static int run=0,pass=0,fail=0;
#define T(n) do{run++;std::cout<<"  "<<n<<"... ";}while(0)
#define P() do{pass++;std::cout<<"PASS"<<std::endl;}while(0)
#define F(m) do{fail++;std::cout<<"FAIL: "<<m<<std::endl;return;}while(0)
#define C(c,m) do{if(!(c))F(m);}while(0)

static void t_agent() {
  T("agent_policy");
  auto p=dataflow::sql::SqlFeaturePolicy::agentDefault();
  C(!p.allow_cte,"no cte"); C(!p.allow_subquery,"no sub");
  C(p.allow_dml,"allow dml"); C(p.allow_ddl,"allow ddl");
  C(p.allow_join,"allow join");
  P();
}
static void t_default() {
  T("default_policy");
  dataflow::sql::SqlFeaturePolicy p;
  C(!p.allow_multi_statement,"no multi");
  C(!p.allow_cte,"no cte"); C(p.allow_join,"join");
  P();
}
int main() {
  std::cout<<"=== Feature Validator Tests ==="<<std::endl;
  t_agent(); t_default();
  std::cout<<std::endl<<"Run: "<<run<<" Pass: "<<pass<<" Fail: "<<fail<<std::endl;
  return fail>0?1:0;
}
