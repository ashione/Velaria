#include "src/dataflow/core/contract/api/session.h"
#include "src/dataflow/core/logical/sql/frontend/source_offset_map.h"
#include "src/dataflow/core/logical/sql/frontend/sql_diagnostic.h"
#include "src/dataflow/core/logical/sql/frontend/sql_feature_validator.h"
#include "src/dataflow/core/logical/sql/frontend/sql_frontend.h"
#include "src/dataflow/core/logical/sql/sql_parser.h"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>

static int run = 0, pass = 0, fail = 0;
#define T(n) do { run++; std::cout << "  " << n << "... "; } while(0)
#define P() do { pass++; std::cout << "PASS" << std::endl; } while(0)
#define F(m) do { fail++; std::cout << "FAIL: " << m << std::endl; return; } while(0)
#define C(c,m) do { if(!(c)) F(m); } while(0)

static void test_ascii() {
  T("offset_ascii");
  dataflow::sql::SourceOffsetMap m("SELECT a\nFROM t\nWHERE a > 1");
  C(m.lineCount()==3, "lines");
  auto p=m.positionAt(0);
  C(p && p->line==1 && p->column_utf8==1, "pos0");
  auto q=m.positionAt(9);
  C(q && q->line==2 && q->column_utf8==1, "line2");
  P();
}
static void test_unicode() {
  T("offset_unicode");
  dataflow::sql::SourceOffsetMap m("SELECT 金额 FROM t");
  C(m.lineCount()==1, "lines");
  auto p=m.positionAt(7);
  C(p && p->column_utf8==8 && p->column_utf16==8, "utf16");
  P();
}
static void test_diag() {
  T("diagnostic");
  dataflow::sql::SqlDiagnostic d;
  C(d.ok(), "default ok");
  d.error_type="test"; C(!d.ok(), "not ok");
  P();
}
static void test_config() {
  T("config_default");
  unsetenv("VELARIA_SQL_FRONTEND");
  auto c=dataflow::sql::sqlFrontendConfigFromEnv();
  C(c.kind==dataflow::sql::SqlFrontendKind::Legacy, "legacy default");
  setenv("VELARIA_SQL_FRONTEND","pg_query",1);
  auto c2=dataflow::sql::sqlFrontendConfigFromEnv();
  C(c2.kind==dataflow::sql::SqlFrontendKind::PgQuery, "pg_query");
  unsetenv("VELARIA_SQL_FRONTEND");
  P();
}
static void test_legacy() {
  T("legacy_parser");
  auto st=dataflow::sql::SqlParser::parse("SELECT a, b FROM t WHERE a > 1");
  C(st.kind==dataflow::sql::SqlStatementKind::Select, "select");
  C(st.query.has_from,"from");
  C(st.query.where!=nullptr,"where");
  P();
}
static void test_session() {
  T("session_legacy");
  auto& s=dataflow::DataflowSession::builder();
  auto df=s.sql("SELECT 1 AS one");
  C(df.toTable().rowCount()==1,"rows");
  C(s.sqlFrontendName()=="legacy","name");
  P();
}
static void test_explain() {
  T("explain_frontend");
  auto& s=dataflow::DataflowSession::builder();
  auto e=s.explainSql("SELECT 1");
  C(e.find("frontend")!=std::string::npos,"frontend section");
  C(e.find("dialect=velaria_sql_v1")!=std::string::npos,"dialect");
  P();
}
int main() {
  std::cout << "=== SQL Frontend Tests ===" << std::endl;
  test_ascii(); test_unicode(); test_diag(); test_config();
  test_legacy(); test_session(); test_explain();
  std::cout << std::endl << "Run: "<<run<<" Pass: "<<pass<<" Fail: "<<fail<<std::endl;
  return fail>0?1:0;
}
