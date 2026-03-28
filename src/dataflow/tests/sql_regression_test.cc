#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "src/dataflow/catalog/catalog.h"
#include "src/dataflow/api/session.h"
#include "src/dataflow/sql/sql_planner.h"
#include "src/dataflow/sql/sql_parser.h"

namespace {

using dataflow::DataflowSession;
using dataflow::DataFrame;
using dataflow::Row;
using dataflow::Schema;
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

  expectNoThrow(
      "parser_join_with_parenthesized_on",
      []() {
        const auto st = dataflow::sql::SqlParser::parse(
            "SELECT u.user_id FROM users u INNER JOIN actions a ON (u.user_id = a.user_id) "
            "WHERE u.user_id > (1)");
        expect(st.query.join.has_value(), "parser_parenthesized_join_present");
      });

  expectNoThrow(
      "parser_predicate_with_parenthesized_rhs",
      []() {
        const auto st = dataflow::sql::SqlParser::parse(
            "SELECT u.user_id FROM users u WHERE u.user_id > (1) LIMIT 1");
        expect(st.query.where.has_value(), "parser_predicate_where_present");
        expect(st.query.limit.value_or(0) == 1, "parser_predicate_limit_present");
      });

  expectNoThrow(
      "parser_create_source_table_csv_options",
      []() {
        const auto st = dataflow::sql::SqlParser::parse(
            "CREATE SOURCE TABLE stream_events (key STRING, value INT) "
            "USING csv OPTIONS(path '/tmp/stream-input', delimiter ',')");
        expect(st.kind == dataflow::sql::SqlStatementKind::CreateTable, "parser_create_csv_kind");
        expect(st.create.kind == dataflow::sql::TableKind::Source, "parser_create_csv_source_kind");
        expect(st.create.provider == "csv", "parser_create_csv_provider");
        expect(st.create.options.at("path") == "/tmp/stream-input", "parser_create_csv_path");
        expect(st.create.options.at("delimiter") == ",", "parser_create_csv_delimiter");
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

void runPlannerPlanRegression() {
  dataflow::ViewCatalog catalog;
  Table t_users =
      Table(Schema({"user_id", "region", "score"}),
            {Row({Value(static_cast<int64_t>(1)), Value(std::string("apac")),
                  Value(static_cast<int64_t>(25))}),
             Row({Value(static_cast<int64_t>(2)), Value(std::string("emea")),
                  Value(static_cast<int64_t>(18))}),
             Row({Value(static_cast<int64_t>(3)), Value(std::string("na")),
                  Value(static_cast<int64_t>(34))})});
  Table t_actions = Table(Schema({"user_id", "event", "score"}),
                         {Row({Value(static_cast<int64_t>(1)), Value(std::string("view")),
                               Value(static_cast<int64_t>(8))}),
                          Row({Value(static_cast<int64_t>(2)), Value(std::string("buy")),
                               Value(static_cast<int64_t>(16))}),
                          Row({Value(static_cast<int64_t>(3)), Value(std::string("view")),
                               Value(static_cast<int64_t>(4))})});
  catalog.createView("users", DataFrame(t_users));
  catalog.createView("actions", DataFrame(t_actions));

  const auto st = dataflow::sql::SqlParser::parse(
      "SELECT u.region, SUM(a.score) AS total_score "
      "FROM users u INNER JOIN actions a ON (u.user_id = a.user_id) "
      "WHERE a.score > 5 "
      "GROUP BY u.region "
      "HAVING total_score > 10 "
      "LIMIT 5");
  dataflow::sql::SqlPlanner planner;
  const auto logical = planner.buildLogicalPlan(st.query, catalog);
  const auto physical = planner.buildPhysicalPlan(logical);
  const auto df = planner.materializeFromPhysical(physical);

  expect(logical.steps.size() >= 5, "planner_logical_has_operators");
  expect(physical.steps.size() == logical.steps.size(), "planner_physical_step_count_match");
  expect(df.toTable().rowCount() > 0, "planner_execution_has_output");
  expect(df.schema().fields.size() == 2, "planner_output_schema_two_columns");
  expect(df.schema().fields[0] == "region", "planner_output_col_region");
  expect(df.schema().fields[1] == "total_score", "planner_output_col_total_score");
}

void runStreamSqlRegression() {
  namespace fs = std::filesystem;

  DataflowSession& s = DataflowSession::builder();
  const std::string input_dir = "/tmp/velaria-stream-sql-regression-input";
  const std::string sink_path = "/tmp/velaria-stream-sql-regression-output.csv";
  const std::string checkpoint_path = "/tmp/velaria-stream-sql-regression.checkpoint";

  fs::remove_all(input_dir);
  fs::create_directories(input_dir);
  fs::remove(sink_path);
  fs::remove(checkpoint_path);

  {
    std::ofstream out(input_dir + "/batch-0001.csv");
    out << "key,value\n";
    out << "userA,10\n";
    out << "userA,5\n";
  }
  {
    std::ofstream out(input_dir + "/batch-0002.csv");
    out << "key,value\n";
    out << "userB,20\n";
    out << "userA,7\n";
  }

  expectNoThrow(
      "stream_sql_create_source_csv",
      [&]() {
        s.sql(
            "CREATE SOURCE TABLE stream_events_csv_v1 (key STRING, value INT) "
            "USING csv OPTIONS(path '" + input_dir + "', delimiter ',')");
      });

  expectNoThrow(
      "stream_sql_create_sink_csv",
      [&]() {
        s.sql(
            "CREATE SINK TABLE stream_summary_csv_v1 (key STRING, value_sum INT) "
            "USING csv OPTIONS(path '" + sink_path + "', delimiter ',')");
      });

  dataflow::StreamingQueryOptions options;
  options.trigger_interval_ms = 0;
  options.checkpoint_path = checkpoint_path;

  auto query = s.startStreamSql(
      "INSERT INTO stream_summary_csv_v1 "
      "SELECT key, SUM(value) AS value_sum "
      "FROM stream_events_csv_v1 "
      "WHERE value > 6 "
      "GROUP BY key "
      "HAVING value_sum > 15 "
      "LIMIT 10",
      options);
  const auto processed = query.awaitTermination(2);
  expect(processed == 2, "stream_sql_csv_insert_processed_batches");

  const auto sink_table = s.read_csv(sink_path).toTable();
  expect(sink_table.rows.size() == 2, "stream_sql_csv_sink_rows");

  bool has_user_a = false;
  bool has_user_b = false;
  const auto key_idx = sink_table.schema.indexOf("key");
  const auto sum_idx = sink_table.schema.indexOf("value_sum");
  for (const auto& row : sink_table.rows) {
    if (row[key_idx].toString() == "userA" && row[sum_idx].asInt64() == 17) {
      has_user_a = true;
    }
    if (row[key_idx].toString() == "userB" && row[sum_idx].asInt64() == 20) {
      has_user_b = true;
    }
  }
  expect(has_user_a, "stream_sql_csv_sink_has_user_a");
  expect(has_user_b, "stream_sql_csv_sink_has_user_b");

  expectThrows(
      "stream_sql_join_rejected",
      [&]() {
        s.streamSql(
            "SELECT a.key, SUM(a.value) AS value_sum "
            "FROM stream_events_csv_v1 a INNER JOIN stream_events_csv_v1 b "
            "ON a.key = b.key GROUP BY a.key");
      },
      "does not support JOIN");

  expectThrows(
      "stream_sql_avg_rejected",
      [&]() {
        s.streamSql(
            "SELECT key, AVG(value) AS avg_value "
            "FROM stream_events_csv_v1 GROUP BY key");
      },
      "only supports SUM and COUNT(*)");

  expectThrows(
      "stream_sql_regular_csv_rejected",
      [&]() {
        s.sql(
            "CREATE TABLE regular_csv_v1 (key STRING, value INT) "
            "USING csv OPTIONS(path '" + input_dir + "')");
      },
      "requires CREATE SOURCE TABLE or CREATE SINK TABLE");
}

}  // namespace

int main() {
  runParserRegression();
  runSemanticRegression();
  runComplexDmlRegression();
  runPlannerPlanRegression();
  runStreamSqlRegression();

  if (g_failed == 0) {
    std::cout << "All regression checks passed.\n";
    return 0;
  }

  std::cout << g_failed << " regression checks failed.\n";
  return 1;
}
