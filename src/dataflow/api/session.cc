#include "src/dataflow/api/session.h"

#include <algorithm>
#include <regex>
#include <sstream>

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
  tempViews_[name] = df;
}

DataFrame SparkSession::sql(const std::string& sql) {
  // v0.1 supports minimal SQL: SELECT * FROM <view>[ LIMIT N]
  std::regex re(R"(^\s*SELECT\s+\*\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*(LIMIT\s+(\d+))?\s*$)",
               std::regex::icase);
  std::smatch match;
  if (!std::regex_match(sql, match, re)) {
    throw std::invalid_argument("unsupported SQL (v0.1 only supports SELECT * FROM view [LIMIT N])");
  }
  auto it = tempViews_.find(match[1].str());
  if (it == tempViews_.end()) {
    throw std::invalid_argument("view not found: " + match[1].str());
  }
  if (match[3].matched) {
    size_t n = static_cast<size_t>(std::stoul(match[3].str()));
    return it->second.limit(n);
  }
  return it->second;
}

}  // namespace dataflow
