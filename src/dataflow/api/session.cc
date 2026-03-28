#include "src/dataflow/api/session.h"

#include "src/dataflow/core/csv.h"

namespace dataflow {

SparkSession& SparkSession::builder() {
  static SparkSession session;
  return session;
}

DataFrame SparkSession::read_csv(const std::string& path, char delimiter) {
  return DataFrame(load_csv(path, delimiter));
}

DataFrame SparkSession::createDataFrame(const Table& table) {
  return DataFrame(table);
}

void SparkSession::createTempView(const std::string& name, const DataFrame& df) {
  catalog_.createView(name, df);
}

DataFrame SparkSession::sql(const std::string& sql) {
  auto query = sql::SqlParser::parse(sql);
  sql::SqlPlanner planner;
  return planner.plan(query, catalog_);
}

}  // namespace dataflow
