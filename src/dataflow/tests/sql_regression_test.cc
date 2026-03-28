#include <cstdint>
#include <functional>
#include <iostream>
#include <string>
#include <utility>

#include "src/dataflow/api/session.h"
#include "src/dataflow/sql/sql_parser.h"

namespace {

using dataflow::DataflowSession;
using dataflow::Table;
using dataflow::Value;

int g_failed = 0;

void expect(bool cond, const std::string& name) {
  if (cond) {
    std::cout << "[PASS] " << name << "\n";
  } else {
    std::cout << "[FAIL] " << name << "\n";
    ++g_failed;
  }
}

void expectNoThrow(const std::string& name, const std::function<void()>& fn) {
  try {
    fn();
    std::cout << "[PASS] " << name << "\n";
  } catch (...) {
    std::cout << "[FAIL] " << name << " got: unexpected throw\n";
    ++g_failed;
  }
}

void expectThrows(const std::string& name, const std::function<void()>& fn,
                 const std::string& expected_substr = "") {
  try {
    fn();
    std::cout << "[FAIL] " << name << "\n";
    ++g_failed;
  } catch (const std::exception& e) {
    if (expected_substr.empty()) {
      std::cout << "[PASS] " << name << "\n";
      return;
    }
    if (std::string(e.what()).find(expected_substr) != std::string::npos) {
      std::cout << "[PASS] " << name << "\n";
      return;
    }
    std::cout << "[FAIL] " << name << " got:" << e.what() << "\n";
    ++g_failed;
  }
}

bool hasSummaryRow(const Table& t, const std::string& region, int64_t total_score, int64_t user_count) {
  auto toInt = [](const Value& v) { return static_cast<int64_t>(v.asDouble()); };
  for (const auto& row : t.rows) {
    if (row[0].asString() == region && toInt(row[1]) == total_score && toInt(row[2]) == user_count) {
      return true;
    }
  }
  return false;
}

void runParserRegression() {
  expectNoThrow(
      "parser_having_aggregate_parenthesized_regression",
      []() {
        const auto st = dataflow::sql::SqlParser::parse(
            "SELECT token, SUM(score) AS total_score FROM rpc_input GROUP BY token "
            "HAVING SUM(score) > 15");
        expect(st.query.having.has_value(), "parser_having_aggregate_present");
      });

  expectNoThrow(
      "parser_projection_where_group_join_limit",
      []() {
        const auto st = dataflow::sql::SqlParser::parse(
            "SELECT u.region, SUM(a.score) AS total_score, COUNT(*) AS user_count "
            "FROM users u INNER JOIN actions a ON u.user_id = a.user_id "
            "WHERE a.score > 6 GROUP BY u.region HAVING SUM(a.score) > 15 LIMIT 10");
        expect(st.query.select_items.size() == 3, "parser_projection_size");
        expect(st.query.where.has_value(), "parser_where_present");
        expect(st.query.join.has_value(), "parser_join_present");
        expect(st.query.group_by.size() == 1, "parser_groupby_size");
        expect(st.query.having.has_value(), "parser_having_present");
        expect(st.query.limit.value_or(0) == 10, "parser_limit_present");
      });

  expectNoThrow(
      "parser_alias_and_limit_projection",
      []() {
        const auto st = dataflow::sql::SqlParser::parse(
            "SELECT u.user_id AS uid, token FROM users u INNER JOIN actions a "
            "ON u.user_id = a.user_id WHERE u.score >= 0 LIMIT 2");
        expect(st.query.select_items.size() == 2, "parser_alias_projection_size");
        expect(st.query.join.has_value(), "parser_alias_join_present");
        expect(st.query.limit.value_or(0) == 2, "parser_alias_limit_present");
      });
}

void runSemanticRegression() {
  DataflowSession& s = DataflowSession::builder();

  s.submit("CREATE TABLE t_users_v1 (user_id INT, region STRING, score INT)");
  s.submit("INSERT INTO t_users_v1 VALUES (1, 'apac', 25), (2, 'emea', 18), (3, 'na', 34)");

  expectThrows(
      "planner_where_aggregate_rejected",
      [&]() {
        s.submit("SELECT region, SUM(score) AS total_score FROM t_users_v1 WHERE SUM(score) > 10 "
                 "GROUP BY region");
      },
      "WHERE does not support aggregate expressions");

  expectThrows(
      "planner_non_aggregate_field_enforced",
      [&]() {
        s.submit("SELECT user_id, SUM(score) AS total_score FROM t_users_v1 GROUP BY region");
      },
      "non-aggregate field must appear in GROUP BY");

  expectThrows(
      "planner_column_not_found",
      [&]() {
        s.submit("SELECT not_exists FROM t_users_v1");
      },
      "column not found");
}

void runComplexDmlRegression() {
  DataflowSession& s = DataflowSession::builder();

  s.submit("CREATE TABLE t_region_users (user_id INT, token STRING, score INT, region STRING)");
  s.submit("INSERT INTO t_region_users VALUES "
           "(1, 'alice', 25, 'apac'), (2, 'bob', 18, 'emea'), (3, 'claire', 34, 'na'), "
           "(4, 'david', 11, 'apac'), (5, 'ella', 7, 'na')");

  s.submit("CREATE TABLE t_region_actions (user_id INT, action STRING, score INT)");
  s.submit("INSERT INTO t_region_actions VALUES "
           "(1, 'view', 5), (1, 'purchase', 20), (2, 'view', 12), (2, 'click', 6), "
           "(3, 'purchase', 30), (4, 'view', 4), (5, 'click', 11)");

  s.submit("CREATE TABLE t_region_summary (region STRING, total_score INT, user_count INT)");
  s.submit(
      "INSERT INTO t_region_summary "
      "SELECT u.region AS region, SUM(a.score) AS total_score, COUNT(*) AS user_count "
      "FROM t_region_users AS u INNER JOIN t_region_actions AS a "
      "ON u.user_id = a.user_id WHERE a.score > 6 GROUP BY u.region HAVING SUM(a.score) > 15");

  Table out = s.submit(
      "SELECT region, total_score, user_count FROM t_region_summary WHERE total_score > 10 LIMIT 5");
  expect(out.rows.size() == 2, "e2e_complex_dml_rows");
  expect(hasSummaryRow(out, "apac", 20, 1), "e2e_complex_dml_row_apac");
  expect(hasSummaryRow(out, "na", 41, 2), "e2e_complex_dml_row_na");
  expect(!hasSummaryRow(out, "emea", 12, 1), "e2e_complex_dml_emea_filtered");
}

}  // namespace

int main() {
  runParserRegression();
  runSemanticRegression();
  runComplexDmlRegression();

  if (g_failed == 0) {
    std::cout << "All regression checks passed.\n";
    return 0;
  }

  std::cout << g_failed << " regression checks failed.\n";
  return 1;
}
