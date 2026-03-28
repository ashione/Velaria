#include "src/dataflow/api/session.h"

#include "src/dataflow/core/csv.h"

namespace dataflow {

DataflowSession& DataflowSession::builder() {
  static DataflowSession session;
  return session;
}

DataFrame DataflowSession::read_csv(const std::string& path, char delimiter) {
  return DataFrame(load_csv(path, delimiter));
}

DataFrame DataflowSession::createDataFrame(const Table& table) {
  return DataFrame(table);
}

void DataflowSession::createTempView(const std::string& name, const DataFrame& df) {
  catalog_.createView(name, df);
}

DataFrame DataflowSession::sql(const std::string& sql) {
  auto query = sql::SqlParser::parse(sql);
  sql::SqlPlanner planner;
  return planner.plan(query, catalog_);
}

}  // namespace dataflow
