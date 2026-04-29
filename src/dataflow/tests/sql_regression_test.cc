#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "src/dataflow/ai/plugin_runtime.h"
#include "src/dataflow/core/contract/catalog/catalog.h"
#include "src/dataflow/core/contract/api/session.h"
#include "src/dataflow/core/execution/columnar_batch.h"
#include "src/dataflow/core/logical/sql/frontend/pg_query_frontend.h"
#include "src/dataflow/core/logical/sql/sql_errors.h"
#include "src/dataflow/core/logical/sql/sql_planner.h"

namespace {

using dataflow::DataflowSession;
using dataflow::DataFrame;
using dataflow::Row;
using dataflow::Schema;
using dataflow::Table;
using dataflow::Value;

int g_failed = 0;

dataflow::sql::SqlStatement parseSqlForRegression(const std::string& sql) {
  dataflow::sql::PgQueryFrontend frontend;
  auto result = frontend.process(sql, dataflow::sql::SqlFeaturePolicy::cliDefault());
  if (result.diagnostics.empty()) {
    return std::move(result.statement);
  }
  const auto& d = result.diagnostics.front();
  std::string message = d.message;
  if (!d.error_type.empty()) message = "error_type=" + d.error_type + "; message=" + message;
  if (!d.hint.empty()) message += "; hint=" + d.hint;
  if (d.error_type == "unsupported_sql_feature" || d.error_type == "unsupported_statement") {
    throw dataflow::SQLUnsupportedError(message);
  }
  throw dataflow::SQLSyntaxError(message);
}

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

template <typename ErrorT>
void expectThrowsType(const std::string& name, const std::function<void()>& fn,
                      const std::string& expected_substr = "") {
  try {
    fn();
    std::cout << "[FAIL] " << name << "\n";
    ++g_failed;
  } catch (const ErrorT& e) {
    if (expected_substr.empty() ||
        std::string(e.what()).find(expected_substr) != std::string::npos) {
      std::cout << "[PASS] " << name << "\n";
      return;
    }
    std::cout << "[FAIL] " << name << " got:" << e.what() << "\n";
    ++g_failed;
  } catch (const std::exception& e) {
    std::cout << "[FAIL] " << name << " got wrong type:" << e.what() << "\n";
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

class AfterParseRewritePlugin final : public dataflow::ai::IExecutionPlugin {
 public:
  const char* name() const override { return "sql-regression-after-parse-rewrite"; }

  bool supports(dataflow::ai::HookPoint point) const override {
    return point == dataflow::ai::HookPoint::kAfterSqlParse;
  }

  dataflow::ai::PluginResult onAfterSqlParse(const dataflow::ai::PluginContext&,
                                             dataflow::ai::PluginPayload&) override {
    dataflow::ai::PluginResult result;
    result.rewritten_sql = "SELECT score FROM after_parse_rewrite_input";
    return result;
  }
};

struct ScopedPluginRegistration {
  explicit ScopedPluginRegistration(std::shared_ptr<dataflow::ai::IExecutionPlugin> plugin)
      : name(plugin->name()) {
    dataflow::ai::PluginManager::instance().registerPlugin(std::move(plugin));
  }

  ~ScopedPluginRegistration() {
    dataflow::ai::PluginManager::instance().unregisterPlugin(name);
  }

  std::string name;
};

void runParserRegression() {
  expectNoThrow(
      "parser_having_aggregate_parenthesized_regression",
      []() {
        const auto st = parseSqlForRegression(
            "SELECT token, SUM(score) AS total_score FROM rpc_input GROUP BY token "
            "HAVING SUM(score) > 15");
        expect(static_cast<bool>(st.query.having), "parser_having_aggregate_present");
      });

  expectNoThrow(
      "parser_projection_where_group_join_limit",
      []() {
        const auto st = parseSqlForRegression(
            "SELECT u.region, SUM(a.score) AS total_score, COUNT(*) AS user_count "
            "FROM users u INNER JOIN actions a ON u.user_id = a.user_id "
            "WHERE a.score > 6 GROUP BY u.region HAVING SUM(a.score) > 15 LIMIT 10");
        expect(st.query.select_items.size() == 3, "parser_projection_size");
        expect(static_cast<bool>(st.query.where), "parser_where_present");
        expect(st.query.join.has_value(), "parser_join_present");
        expect(st.query.group_by.size() == 1, "parser_groupby_size");
        expect(static_cast<bool>(st.query.having), "parser_having_present");
        expect(st.query.limit.value_or(0) == 10, "parser_limit_present");
      });

  expectNoThrow(
      "parser_create_table_using_json_without_columns",
      []() {
        const auto st = parseSqlForRegression(
            "CREATE TABLE rpc_input USING json OPTIONS(path: '/tmp/input.jsonl', "
            "columns: 'user_id,name', format: 'json_lines')");
        expect(st.kind == dataflow::sql::SqlStatementKind::CreateTable, "parser_create_kind");
        expect(st.create.columns.empty(), "parser_create_columns_optional");
        expect(st.create.provider == "json", "parser_create_provider");
      });

  expectNoThrow(
      "parser_create_table_probe_only_path",
      []() {
        const auto st =
            parseSqlForRegression("CREATE TABLE rpc_auto OPTIONS(path: '/tmp/input.jsonl')");
        expect(st.create.table == "rpc_auto", "parser_create_probe_table_name");
        expect(st.create.options.at("path") == "/tmp/input.jsonl", "parser_create_probe_path");
      });

  expectNoThrow(
      "parser_string_function_projection_parsed",
      []() {
        const auto st = parseSqlForRegression(
            "SELECT LOWER(region) AS region_lower, SUBSTR(region, 1, 3) AS pref, "
            "CONCAT(region, '-', user_id) AS combined "
            "FROM users u");
        expect(st.query.select_items.size() == 3, "parser_string_function_projection_size");
        expect(st.query.select_items[0].is_string_function, "parser_string_function_first");
        expect(st.query.select_items[1].is_string_function, "parser_string_function_second");
        expect(st.query.select_items[2].is_string_function, "parser_string_function_third");
        expect(st.query.select_items[1].alias == "pref", "parser_string_function_alias");
      });

  expectNoThrow(
      "parser_more_string_function_projection_parsed",
      []() {
        const auto st = parseSqlForRegression(
            "SELECT LTRIM(name) AS left_trimmed, RTRIM(name) AS right_trimmed, "
            "REPLACE(region, 'ap', 'AP') AS replaced "
            "FROM users u");
        expect(st.query.select_items.size() == 3, "parser_more_string_function_projection_size");
        expect(st.query.select_items[0].is_string_function, "parser_ltrim_function");
        expect(st.query.select_items[1].is_string_function, "parser_rtrim_function");
        expect(st.query.select_items[2].is_string_function, "parser_replace_function");
      });

  expectNoThrow(
      "parser_extended_string_function_projection_parsed",
      []() {
        const auto st = parseSqlForRegression(
            "SELECT CONCAT_WS('-', region, user_id) AS region_id, LEFT(region, 2) AS head, "
            "RIGHT(region, 2) AS tail, POSITION('a', region) AS first_a "
            "FROM users u");
        expect(st.query.select_items.size() == 4, "parser_extended_string_function_projection_size");
        expect(st.query.select_items[0].is_string_function, "parser_concat_ws_function");
        expect(st.query.select_items[1].is_string_function, "parser_left_function");
        expect(st.query.select_items[2].is_string_function, "parser_right_function");
        expect(st.query.select_items[3].is_string_function, "parser_position_function");
      });

  expectNoThrow(
      "parser_substring_alias_string_function_projection_parsed",
      []() {
        const auto st = parseSqlForRegression(
            "SELECT SUBSTRING(region, 2, 2) AS head FROM users u");
        expect(st.query.select_items.size() == 1, "parser_substring_alias_string_function_projection_size");
        expect(st.query.select_items[0].is_string_function, "parser_substring_alias_function");
      });

  expectNoThrow(
      "parser_string_function_boolean_argument_is_literal",
      []() {
        const auto st = parseSqlForRegression(
            "SELECT LEFT(region, TRUE) AS one FROM users u");
        expect(st.query.select_items.size() == 1, "parser_string_function_boolean_argument_size");
        expect(st.query.select_items[0].is_string_function, "parser_string_function_boolean_argument_flag");
      });

  expectNoThrow(
      "parser_length_alias_string_function_projection_parsed",
      []() {
        const auto st = parseSqlForRegression(
            "SELECT LENGTH(region) AS region_len, LEN(region) AS region_len_alias, "
            "CHAR_LENGTH(region) AS char_len, CHARACTER_LENGTH(region) AS char_len2, "
            "REVERSE(region) AS region_rev "
            "FROM users u");
        expect(st.query.select_items.size() == 5, "parser_length_alias_string_function_projection_size");
        expect(st.query.select_items[0].is_string_function, "parser_length_function");
        expect(st.query.select_items[1].is_string_function, "parser_len_alias_function");
        expect(st.query.select_items[2].is_string_function, "parser_char_length_function");
        expect(st.query.select_items[3].is_string_function, "parser_character_length_function");
        expect(st.query.select_items[4].is_string_function, "parser_reverse_function");
      });

  expectNoThrow(
      "parser_hybrid_search_clause_parsed",
      []() {
        const auto st = parseSqlForRegression(
            "SELECT id, bucket FROM users "
            "WHERE bucket = 1 "
            "HYBRID SEARCH embedding QUERY '[1 0 0]' METRIC cosine TOP_K 7 SCORE_THRESHOLD 0.02");
        expect(st.query.hybrid_search.has_value(), "parser_hybrid_search_present");
        expect(st.query.hybrid_search->vector_column.name == "embedding",
               "parser_hybrid_search_column");
        expect(st.query.hybrid_search->query_vector == "[1 0 0]",
               "parser_hybrid_search_query_vector");
        expect(st.query.hybrid_search->metric == "cosine", "parser_hybrid_search_metric");
        expect(st.query.hybrid_search->top_k == 7, "parser_hybrid_search_top_k");
        expect(st.query.hybrid_search->score_threshold.has_value(),
               "parser_hybrid_search_threshold_present");
      });

  expectNoThrow(
      "parser_where_and_or_clause_parsed",
      []() {
        const auto st = parseSqlForRegression(
            "SELECT user_id FROM users WHERE region = 'apac' OR (region = 'emea' AND score > 10)");
        expect(static_cast<bool>(st.query.where), "parser_where_and_or_present");
      });

  expectNoThrow(
      "parser_alias_and_limit_projection",
      []() {
        const auto st = parseSqlForRegression(
            "SELECT u.user_id AS uid, token FROM users u INNER JOIN actions a "
            "ON u.user_id = a.user_id WHERE u.score >= 0 LIMIT 2");
        expect(st.query.select_items.size() == 2, "parser_alias_projection_size");
        expect(st.query.join.has_value(), "parser_alias_join_present");
        expect(st.query.limit.value_or(0) == 2, "parser_alias_limit_present");
      });

  expectNoThrow(
      "parser_order_by_projection",
      []() {
        const auto st = parseSqlForRegression(
            "SELECT user_id, score FROM users ORDER BY score DESC, user_id ASC LIMIT 2");
        expect(st.query.order_by.size() == 2, "parser_order_by_size");
        expect(!st.query.order_by[0].ascending, "parser_order_by_first_desc");
        expect(st.query.order_by[1].ascending, "parser_order_by_second_asc");
        expect(st.query.limit.value_or(0) == 2, "parser_order_by_limit_present");
      });

  expectNoThrow(
      "parser_union_all_projection",
      []() {
        const auto st = parseSqlForRegression(
            "SELECT user_id FROM users UNION ALL SELECT user_id FROM archived_users");
        expect(st.query.union_terms.size() == 1, "parser_union_all_term_size");
        expect(st.query.union_terms[0].all, "parser_union_all_flag");
      });

  expectNoThrow(
      "parser_union_distinct_projection",
      []() {
        const auto st = parseSqlForRegression(
            "SELECT region FROM users UNION SELECT region FROM archived_users");
        expect(st.query.union_terms.size() == 1, "parser_union_distinct_term_size");
        expect(!st.query.union_terms[0].all, "parser_union_distinct_flag");
      });

  expectThrowsType<dataflow::SQLUnsupportedError>(
      "parser_intersect_rejected",
      []() {
        parseSqlForRegression(
            "SELECT region FROM users INTERSECT SELECT region FROM archived_users");
      },
      "INTERSECT");

  expectThrowsType<dataflow::SQLUnsupportedError>(
      "parser_except_rejected",
      []() {
        parseSqlForRegression(
            "SELECT region FROM users EXCEPT SELECT region FROM archived_users");
      },
      "EXCEPT");

  expectThrowsType<dataflow::SQLUnsupportedError>(
      "parser_select_distinct_rejected",
      []() {
        parseSqlForRegression("SELECT DISTINCT region FROM users");
      },
      "SELECT DISTINCT");

  expectThrowsType<dataflow::SQLUnsupportedError>(
      "parser_aggregate_distinct_rejected",
      []() {
        parseSqlForRegression("SELECT COUNT(DISTINCT region) AS n FROM users");
      },
      "Aggregate DISTINCT");

  expectThrowsType<dataflow::SQLUnsupportedError>(
      "parser_predicate_rhs_expression_rejected",
      []() {
        parseSqlForRegression("SELECT user_id FROM users WHERE score > 1 + 1");
      },
      "predicate right-hand");

  expectThrowsType<dataflow::SQLUnsupportedError>(
      "parser_limit_expression_rejected",
      []() {
        parseSqlForRegression("SELECT user_id FROM users LIMIT 1 + 1");
      },
      "LIMIT");

  expectThrowsType<dataflow::SQLUnsupportedError>(
      "parser_insert_values_expression_rejected",
      []() {
        parseSqlForRegression("INSERT INTO users VALUES (1 + 1, 'apac', 25)");
      },
      "VALUES");

  expectNoThrow(
      "parser_keyword_search_clause_parsed",
      []() {
        const auto st = parseSqlForRegression(
            "SELECT id, title, keyword_score FROM docs "
            "WHERE bucket = 1 "
            "KEYWORD SEARCH(title, body) QUERY 'payment timeout' TOP_K 20");
        expect(st.query.keyword_search.has_value(), "parser_keyword_search_present");
        expect(st.query.keyword_search->columns.size() == 2, "parser_keyword_search_column_count");
        expect(st.query.keyword_search->query_text == "payment timeout",
               "parser_keyword_search_query_text");
        expect(st.query.keyword_search->top_k == 20, "parser_keyword_search_top_k");
      });

  expectNoThrow(
      "parser_join_with_parenthesized_on",
      []() {
        const auto st = parseSqlForRegression(
            "SELECT u.user_id FROM users u INNER JOIN actions a ON (u.user_id = a.user_id) "
            "WHERE u.user_id > (1)");
        expect(st.query.join.has_value(), "parser_parenthesized_join_present");
      });

  expectNoThrow(
      "parser_predicate_with_parenthesized_rhs",
      []() {
        const auto st = parseSqlForRegression(
            "SELECT u.user_id FROM users u WHERE u.user_id > (1) LIMIT 1");
        expect(static_cast<bool>(st.query.where), "parser_predicate_where_present");
        expect(st.query.limit.value_or(0) == 1, "parser_predicate_limit_present");
      });

  expectNoThrow(
      "parser_create_source_table_csv_options",
      []() {
        const auto st = parseSqlForRegression(
            "CREATE SOURCE TABLE stream_events (key STRING, value INT) "
            "USING csv OPTIONS(path: '/tmp/stream-input', delimiter: ',')");
        expect(st.kind == dataflow::sql::SqlStatementKind::CreateTable, "parser_create_csv_kind");
        expect(st.create.kind == dataflow::sql::TableKind::Source, "parser_create_csv_source_kind");
        expect(st.create.provider == "csv", "parser_create_csv_provider");
        expect(st.create.options.at("path") == "/tmp/stream-input", "parser_create_csv_path");
        expect(st.create.options.at("delimiter") == ",", "parser_create_csv_delimiter");
      });

  expectNoThrow(
      "parser_stream_window_clause",
      []() {
        const auto st = parseSqlForRegression(
            "SELECT window_start, key, SUM(value) AS value_sum FROM stream_events "
            "WINDOW BY ts EVERY 60000 AS window_start GROUP BY window_start, key");
        expect(st.query.window.has_value(), "parser_window_present");
        expect(st.query.window->every_ms == 60000, "parser_window_ms");
        expect(st.query.window->slide_ms == 60000, "parser_window_default_slide_ms");
        expect(st.query.window->output_column == "window_start", "parser_window_output");
      });

  expectNoThrow(
      "parser_stream_sliding_window_clause",
      []() {
        const auto st = parseSqlForRegression(
            "SELECT window_start, key, COUNT(*) AS event_count FROM stream_events "
            "WINDOW BY ts EVERY 60000 SLIDE 30000 AS window_start GROUP BY window_start, key");
        expect(st.query.window.has_value(), "parser_sliding_window_present");
        expect(st.query.window->every_ms == 60000, "parser_sliding_window_ms");
        expect(st.query.window->slide_ms == 30000, "parser_sliding_slide_ms");
        expect(st.query.window->output_column == "window_start", "parser_sliding_window_output");
      });

  expectNoThrow(
      "parser_group_by_scalar_expression",
      []() {
        const auto st = parseSqlForRegression(
            "SELECT ISO_YEAR(ts) AS iso_year, WEEK(ts) AS iso_week, COUNT(*) AS n "
            "FROM input_table GROUP BY ISO_YEAR(ts), WEEK(ts)");
        expect(st.query.group_by.size() == 2, "parser_group_by_scalar_expression_size");
        expect(st.query.group_by[0].is_string_function,
               "parser_group_by_scalar_expression_first_function");
        expect(st.query.group_by[1].is_string_function,
               "parser_group_by_scalar_expression_second_function");
      });

  expectNoThrow(
      "parser_time_function_projection_parsed",
      []() {
        const auto st = parseSqlForRegression(
            "SELECT NOW() AS now_value, today() AS day_value, currentTimestamp() AS current_ts, "
            "CURRENT_TIMESTAMP() AS current_ts_2, UNIX_TIMESTAMP('2026-01-01') AS epoch_s "
            "FROM users u");
        expect(st.query.select_items.size() == 5, "parser_time_function_projection_size");
        expect(st.query.select_items[0].is_string_function, "parser_now_function");
        expect(st.query.select_items[1].is_string_function, "parser_today_function");
        expect(st.query.select_items[2].is_string_function, "parser_current_timestamp_function");
        expect(st.query.select_items[3].is_string_function, "parser_current_timestamp_keyword_function");
        expect(st.query.select_items[4].is_string_function, "parser_unix_timestamp_function");
      });

  expectNoThrow(
      "parser_nested_cast_string_function",
      []() {
        const auto st = parseSqlForRegression(
            "SELECT SUBSTR(CAST(open AS STRING), 1, 4) AS open_prefix FROM input_table");
        expect(st.query.select_items.size() == 1, "parser_nested_cast_projection_size");
        expect(st.query.select_items[0].is_string_function,
               "parser_nested_cast_projection_function");
        expect(st.query.select_items[0].string_function.args[0].is_function,
               "parser_nested_cast_argument_function");
      });

  expectNoThrow(
      "parser_insert_select_with_target_columns",
      []() {
        const auto st = parseSqlForRegression(
            "INSERT INTO sink_table (score, region) "
            "SELECT user_score AS points, user_region AS area FROM source_table LIMIT 1");
        expect(st.kind == dataflow::sql::SqlStatementKind::InsertSelect,
               "parser_insert_select_kind");
        expect(st.insert.columns.size() == 2, "parser_insert_select_column_count");
        expect(st.insert.columns[0] == "score", "parser_insert_select_first_target_column");
        expect(st.insert.query.limit.value_or(0) == 1, "parser_insert_select_limit");
      });

  expectThrowsType<dataflow::SQLSyntaxError>(
      "parser_syntax_error_category",
      []() {
        parseSqlForRegression("SELECT 'unterminated");
      },
      "unterminated quoted string");

  expectNoThrow(
      "parser_quoted_identifier_with_unicode_columns",
      []() {
        const auto st = parseSqlForRegression(
            "SELECT \"目的市\" FROM input_table WHERE \"始发市\" = '杭州' LIMIT 50");
        expect(st.query.select_items.size() == 1, "parser_quoted_identifier_projection_size");
        expect(st.query.select_items[0].column.name == "目的市",
               "parser_quoted_identifier_projection_name");
        expect(static_cast<bool>(st.query.where), "parser_quoted_identifier_where_present");
        expect(st.query.limit.value_or(0) == 50, "parser_quoted_identifier_limit");
      });

  expectThrowsType<dataflow::SQLSyntaxError>(
      "parser_unquoted_unicode_identifier_reports_readable_error",
      []() {
        parseSqlForRegression("SELECT 目的市 FROM input_table");
      },
      "invalid token byte 0x");

  expectThrowsType<dataflow::SQLUnsupportedError>(
      "parser_unsupported_statement_category",
      []() {
        parseSqlForRegression("UPDATE users SET score = 1");
      },
      "not supported in SQL v1");
}

void runSemanticRegression() {
  DataflowSession& s = DataflowSession::builder();

  s.submit("CREATE TABLE t_users_v1 (user_id INT, region STRING, score INT)");
  s.submit("INSERT INTO t_users_v1 VALUES (1, 'apac', 25), (2, 'emea', 18), (3, 'na', 34)");

  expectThrows(
      "insert_values_duplicate_target_column_rejected",
      [&]() {
        s.submit("INSERT INTO t_users_v1 (user_id, user_id) VALUES (11, 12)");
      },
      "duplicate insert column");

  expectThrows(
      "insert_values_unknown_target_column_rejected",
      [&]() {
        s.submit("INSERT INTO t_users_v1 (user_id, unknown_col) VALUES (11, 12)");
      },
      "column not found");

  expectThrows(
      "insert_values_column_count_mismatch_rejected",
      [&]() {
        s.submit("INSERT INTO t_users_v1 (user_id, region) VALUES (11)");
      },
      "INSERT VALUES column count mismatch");

  s.submit("CREATE TABLE t_values_mixed_v1 (id INT, label STRING, flag BOOL, amount DOUBLE, missing STRING)");
  s.submit("INSERT INTO t_values_mixed_v1 VALUES "
           "(1, 'a', TRUE, 3.5, NULL), "
           "(2, 'b', FALSE, 4.25, NULL)");
  Table mixed_values = s.submit(
      "SELECT id, label, flag, amount, missing FROM t_values_mixed_v1 ORDER BY id");
  expect(mixed_values.rows.size() == 2, "insert_values_mixed_rows");
  expect(mixed_values.rows[0][0].asInt64() == 1, "insert_values_mixed_int");
  expect(mixed_values.rows[0][1].asString() == "a", "insert_values_mixed_string");
  expect(mixed_values.rows[0][2].asBool(), "insert_values_mixed_true");
  expect(mixed_values.rows[0][3].asDouble() == 3.5, "insert_values_mixed_double");
  expect(mixed_values.rows[0][4].isNull(), "insert_values_mixed_null");
  expect(!mixed_values.rows[1][2].asBool(), "insert_values_mixed_false");

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

  expectThrows(
      "planner_batch_window_sql_rejected",
      [&]() {
        s.submit(
            "SELECT region, SUM(score) AS total_score FROM t_users_v1 "
            "WINDOW BY score EVERY 60000 AS window_start GROUP BY region");
      },
      "only supported in stream SQL");
  expectThrowsType<dataflow::SQLUnsupportedError>(
      "planner_batch_window_unsupported_category",
      [&]() {
        s.submit(
            "SELECT region, SUM(score) AS total_score FROM t_users_v1 "
            "WINDOW BY score EVERY 60000 AS window_start GROUP BY region");
      },
      "not supported in SQL v1");
  expectThrowsType<dataflow::SQLUnsupportedError>(
      "planner_stream_window_slide_gt_window_rejected",
      [&]() {
        dataflow::Table stream_events;
        stream_events.schema = dataflow::Schema({"ts", "region", "score"});
        stream_events.rows = {
            {dataflow::Value("2026-03-28T09:00:00"), dataflow::Value("apac"),
             dataflow::Value(int64_t(1))},
        };
        s.createTempView(
            "invalid_window_slide_events",
            s.readStream(std::make_shared<dataflow::MemoryStreamSource>(
                std::vector<dataflow::Table>{stream_events})));
        s.explainStreamSql(
            "SELECT window_start, region, COUNT(*) AS cnt "
            "FROM invalid_window_slide_events "
            "WINDOW BY ts EVERY 60000 SLIDE 70000 AS window_start GROUP BY window_start, region");
      },
      "WINDOW SLIDE");

  expectThrows(
      "planner_select_star_mixed_projection_rejected",
      [&]() {
        s.submit("SELECT *, region FROM t_users_v1");
      },
      "SELECT * cannot be mixed with other projections");

  expectThrows(
      "planner_hybrid_search_join_rejected",
      [&]() {
        s.submit("SELECT * FROM t_users_v1 u JOIN t_users_v1 v ON u.user_id = v.user_id "
                 "HYBRID SEARCH score QUERY '[1 0 0]'");
      },
      "HYBRID SEARCH does not support JOIN");

  expectThrows(
      "planner_hybrid_search_aggregate_rejected",
      [&]() {
        s.submit("SELECT region, SUM(score) AS total_score FROM t_users_v1 "
                 "HYBRID SEARCH score QUERY '[1 0 0]' GROUP BY region");
      },
      "HYBRID SEARCH does not support aggregate");

  expectThrows(
      "planner_keyword_search_aggregate_rejected",
      [&]() {
        s.submit("SELECT region, SUM(score) AS total_score FROM t_users_v1 "
                 "KEYWORD SEARCH(region) QUERY 'apac' TOP_K 2 GROUP BY region");
      },
      "KEYWORD SEARCH does not support aggregate");

  Table and_rows = s.submit(
      "SELECT user_id FROM t_users_v1 WHERE region = 'apac' AND score > 20 ORDER BY user_id LIMIT 5");
  expect(and_rows.rows.size() == 1, "planner_where_and_rows");
  expect(and_rows.rows[0][0].asInt64() == 1, "planner_where_and_first_user");

  Table or_rows = s.submit(
      "SELECT user_id FROM t_users_v1 WHERE region = 'apac' OR region = 'emea' ORDER BY user_id LIMIT 5");
  expect(or_rows.rows.size() == 2, "planner_where_or_rows");
  expect(or_rows.rows[0][0].asInt64() == 1, "planner_where_or_first_user");
  expect(or_rows.rows[1][0].asInt64() == 2, "planner_where_or_second_user");

  Table table_star_rows = s.submit(
      "SELECT u.* FROM t_users_v1 u ORDER BY u.user_id");
  expect(table_star_rows.rows.size() == 3, "planner_table_star_rows");
  expect(table_star_rows.schema.fields.size() == 3, "planner_table_star_schema_size");
  expect(table_star_rows.schema.fields[0].find("user_id") != std::string::npos,
         "planner_table_star_first_field");
  expect(table_star_rows.rows[0][0].asInt64() == 1, "planner_table_star_first_user");

  s.submit("CREATE TABLE t_left_actions_v1 (user_id INT, action_name STRING)");
  s.submit("INSERT INTO t_left_actions_v1 VALUES (1, 'view'), (1, 'buy'), (3, 'view')");
  Table left_join_rows = s.submit(
      "SELECT u.user_id, a.action_name FROM t_users_v1 u "
      "LEFT JOIN t_left_actions_v1 a ON u.user_id = a.user_id "
      "ORDER BY u.user_id");
  expect(left_join_rows.rows.size() == 4, "planner_left_join_rows");
  bool saw_user2_null = false;
  for (const auto& row : left_join_rows.rows) {
    if (row[0].asInt64() == 2 && row[1].isNull()) saw_user2_null = true;
  }
  expect(saw_user2_null, "planner_left_join_null_row");

  expectThrowsType<dataflow::SQLUnsupportedError>(
      "planner_non_equality_join_rejected",
      [&]() {
        s.submit("SELECT u.user_id FROM t_users_v1 u INNER JOIN t_left_actions_v1 a "
                 "ON u.user_id > a.user_id");
      },
      "equality JOIN");

  s.submit("CREATE TABLE t_prices_v1 (trade_date STRING, open DOUBLE, close DOUBLE, open_text STRING)");
  s.submit("INSERT INTO t_prices_v1 VALUES "
           "('2026-01-01', 10.25, 9.75, '10.25'), "
           "('2026-01-02', 8.50, 8.75, '8.50'), "
           "('2026-01-03', 7.10, 6.05, '7.10')");
  Table down_days = s.submit(
      "SELECT COUNT(*) AS down_days FROM t_prices_v1 WHERE open > close");
  expect(down_days.rows.size() == 1, "planner_column_compare_count_rows");
  expect(down_days.rows[0][0].asInt64() == 2, "planner_column_compare_count_value");
  Table cast_substr = s.submit(
      "SELECT trade_date, SUBSTR(CAST(open AS STRING), 1, 4) AS open_prefix, "
      "CAST(open_text AS DOUBLE) AS open_number "
      "FROM t_prices_v1 ORDER BY trade_date LIMIT 1");
  expect(cast_substr.rows.size() == 1, "planner_cast_substr_rows");
  expect(cast_substr.rows[0][1].toString() == "10.2", "planner_cast_substr_prefix");
  expect(cast_substr.rows[0][2].asDouble() == 10.25, "planner_cast_string_to_double");
  s.submit("CREATE TABLE t_date_expr_v1 (ts_ms INT)");
  s.submit("INSERT INTO t_date_expr_v1 VALUES (1767225600000)");
  Table nested_date_expr = s.submit(
      "SELECT YEAR(CAST(ts_ms AS STRING)) AS y, YEARWEEK(CAST(ts_ms AS STRING)) AS yw "
      "FROM t_date_expr_v1");
  expect(nested_date_expr.rows.size() == 1, "planner_nested_date_expr_rows");
  expect(nested_date_expr.rows[0][0].asInt64() == 2026, "planner_nested_date_expr_year");
  expect(nested_date_expr.rows[0][1].asInt64() == 202601, "planner_nested_date_expr_yearweek");

  s.submit("CREATE TABLE t_users_archive_v1 (user_id INT, region STRING, score INT)");
  s.submit("INSERT INTO t_users_archive_v1 VALUES (2, 'emea', 18), (4, 'latam', 21)");

  Table union_all_rows = s.submit(
      "SELECT user_id FROM t_users_v1 UNION ALL SELECT user_id FROM t_users_archive_v1");
  expect(union_all_rows.rows.size() == 5, "planner_union_all_rows");

  Table union_rows = s.submit(
      "SELECT user_id FROM t_users_v1 UNION SELECT user_id FROM t_users_archive_v1");
  expect(union_rows.rows.size() == 4, "planner_union_distinct_rows");

  Table delimiter_left;
  delimiter_left.schema = Schema({"left_text", "right_text"});
  delimiter_left.rows = {
      {Value(std::string("alpha") + std::string(1, '\x1f') + "beta"), Value("gamma")},
  };
  Table delimiter_right;
  delimiter_right.schema = Schema({"left_text", "right_text"});
  delimiter_right.rows = {
      {Value("alpha"), Value(std::string("beta") + std::string(1, '\x1f') + "gamma")},
  };
  s.createTempView("t_union_delim_left_v1", s.createDataFrame(delimiter_left));
  s.createTempView("t_union_delim_right_v1", s.createDataFrame(delimiter_right));
  Table delimiter_union_rows = s.submit(
      "SELECT left_text, right_text FROM t_union_delim_left_v1 "
      "UNION SELECT left_text, right_text FROM t_union_delim_right_v1");
  expect(delimiter_union_rows.rows.size() == 2, "planner_union_distinct_embedded_delimiter_rows");

  expectThrows(
      "planner_union_column_count_mismatch_rejected",
      [&]() {
        s.submit(
            "SELECT user_id, region FROM t_users_v1 UNION SELECT user_id FROM t_users_archive_v1");
      },
      "same number of projected columns");

  expectThrowsType<dataflow::SQLUnsupportedError>(
      "planner_union_order_by_rejected",
      [&]() {
        s.submit(
            "SELECT user_id FROM t_users_v1 UNION SELECT user_id FROM t_users_archive_v1 ORDER BY user_id");
      },
      "UNION does not support ORDER BY");

  expectThrowsType<dataflow::SQLUnsupportedError>(
      "stream_sql_union_rejected",
      [&]() {
        const auto parsed =
            parseSqlForRegression(
                "SELECT user_id FROM stream_left_v1 UNION SELECT user_id FROM stream_right_v1");
        dataflow::sql::SqlPlanner planner;
        planner.buildStreamLogicalPlan(parsed.query);
      },
      "stream SQL does not support UNION");

  expectNoThrow(
      "unicode_quoted_identifier_select_regression",
      [&]() {
        s.submit("CREATE TABLE t_cn_v1 (\"始发市\" STRING, \"目的市\" STRING)");
        s.submit("INSERT INTO t_cn_v1 VALUES ('杭州', '北京'), ('杭州', '上海'), ('深圳', '广州')");
        const auto out = s.submit(
            "SELECT \"目的市\" FROM t_cn_v1 WHERE \"始发市\" = '杭州' LIMIT 50");
        expect(out.rows.size() == 2, "unicode_quoted_identifier_rows");
        expect(out.schema.fields.size() == 1, "unicode_quoted_identifier_schema_size");
        expect(out.schema.fields[0] == "目的市", "unicode_quoted_identifier_schema_name");
        expect(out.rows[0][0].asString() == "北京", "unicode_quoted_identifier_first_value");
        expect(out.rows[1][0].asString() == "上海", "unicode_quoted_identifier_second_value");
      });

  s.submit("CREATE SOURCE TABLE t_source_guard_v1 (user_id INT, region STRING)");
  expectThrows(
      "source_table_insert_rejected",
      [&]() {
        s.submit("INSERT INTO t_source_guard_v1 VALUES (1, 'apac')");
      },
      "INSERT INTO is not allowed on SOURCE TABLE");
  expectThrowsType<dataflow::SQLTableKindError>(
      "source_table_insert_table_kind_category",
      [&]() {
        s.submit("INSERT INTO t_source_guard_v1 VALUES (1, 'apac')");
      },
      "table-kind constraint violation");

  expectThrows(
      "source_table_insert_select_rejected",
      [&]() {
        s.submit(
            "INSERT INTO t_source_guard_v1 SELECT user_id, region FROM t_users_v1 LIMIT 1");
      },
      "INSERT INTO is not allowed on SOURCE TABLE");

  s.submit("CREATE SINK TABLE t_sink_guard_v1 (user_id INT, region STRING)");
  s.submit("INSERT INTO t_sink_guard_v1 VALUES (7, 'latam')");
  expectNoThrow(
      "sink_table_insert_select_allowed",
      [&]() {
        s.submit(
            "INSERT INTO t_sink_guard_v1 SELECT user_id, region FROM t_users_v1 LIMIT 1");
      });
  expectThrows(
      "sink_table_select_rejected",
      [&]() {
        s.submit("SELECT user_id, region FROM t_sink_guard_v1");
      },
      "SELECT from SINK table is not allowed");
  expectThrowsType<dataflow::SQLTableKindError>(
      "sink_table_select_table_kind_category",
      [&]() {
        s.submit("SELECT user_id, region FROM t_sink_guard_v1");
      },
      "table-kind constraint violation");

  Table hybrid_users;
  hybrid_users.schema = Schema({"id", "bucket", "embedding"});
  hybrid_users.rows = {
      {Value(int64_t(10)), Value(int64_t(0)), Value(std::vector<float>{1.0f, 0.0f, 0.0f})},
      {Value(int64_t(20)), Value(int64_t(1)), Value(std::vector<float>{0.9f, 0.1f, 0.0f})},
      {Value(int64_t(30)), Value(int64_t(1)), Value(std::vector<float>{0.0f, 1.0f, 0.0f})},
  };
  s.createTempView("t_hybrid_sql_v1", s.createDataFrame(hybrid_users));
  Table hybrid_result = s.submit(
      "SELECT id, bucket, vector_score FROM t_hybrid_sql_v1 "
      "WHERE bucket = 1 "
      "HYBRID SEARCH embedding QUERY '[1 0 0]' METRIC cosine TOP_K 2");
  expect(hybrid_result.rows.size() == 2, "sql_hybrid_search_row_count");
  expect(hybrid_result.schema.fields.size() == 3, "sql_hybrid_search_schema_width");
  expect(hybrid_result.schema.fields[2] == "vector_score", "sql_hybrid_search_score_column");
  expect(hybrid_result.rows[0][0].asInt64() == 20, "sql_hybrid_search_first_id");

  const std::string hybrid_explain = s.explainSql(
      "SELECT id, bucket, vector_score FROM t_hybrid_sql_v1 "
      "WHERE bucket = 1 "
      "HYBRID SEARCH embedding QUERY '[1 0 0]' METRIC cosine TOP_K 2");
  expect(hybrid_explain.find("HybridSearch column=embedding top_k=2") != std::string::npos,
         "sql_hybrid_search_explain_logical");

  Table keyword_users;
  keyword_users.schema = Schema({"id", "bucket", "status", "title", "body", "embedding"});
  keyword_users.rows = {
      {Value(int64_t(10)), Value(int64_t(1)), Value("open"), Value("Payment timeout"),
       Value("checkout payment timeout issue"), Value(std::vector<float>{0.8f, 0.2f, 0.0f})},
      {Value(int64_t(20)), Value(int64_t(1)), Value("open"), Value("Payment retry"),
       Value("payment timeout recovered after retry"), Value(std::vector<float>{1.0f, 0.0f, 0.0f})},
      {Value(int64_t(30)), Value(int64_t(1)), Value("closed"), Value("Refund delay"),
       Value("refund queue backlog"), Value(std::vector<float>{0.0f, 1.0f, 0.0f})},
  };
  s.createTempView("t_keyword_sql_v1", s.createDataFrame(keyword_users));

  Table keyword_result = s.submit(
      "SELECT id, title, keyword_score FROM t_keyword_sql_v1 "
      "WHERE bucket = 1 AND status = 'open' "
      "KEYWORD SEARCH(title, body) QUERY 'payment timeout' TOP_K 2");
  expect(keyword_result.rows.size() == 2, "sql_keyword_search_row_count");
  expect(keyword_result.schema.fields[2] == "keyword_score", "sql_keyword_search_score_column");
  expect(keyword_result.rows[0][0].asInt64() == 10, "sql_keyword_search_first_id");

  Table keyword_hybrid_result = s.submit(
      "SELECT id, keyword_score, vector_score FROM t_keyword_sql_v1 "
      "WHERE bucket = 1 AND status = 'open' "
      "KEYWORD SEARCH(title, body) QUERY 'payment timeout' TOP_K 2 "
      "HYBRID SEARCH embedding QUERY '[1 0 0]' METRIC cosine TOP_K 1");
  expect(keyword_hybrid_result.rows.size() == 1, "sql_keyword_hybrid_row_count");
  expect(keyword_hybrid_result.schema.fields[1] == "keyword_score",
         "sql_keyword_hybrid_keyword_score_column");
  expect(keyword_hybrid_result.schema.fields[2] == "vector_score",
         "sql_keyword_hybrid_vector_score_column");
  expect(keyword_hybrid_result.rows[0][0].asInt64() == 20, "sql_keyword_hybrid_first_id");

  const std::string keyword_explain = s.explainSql(
      "SELECT id, title, keyword_score FROM t_keyword_sql_v1 "
      "WHERE bucket = 1 AND status = 'open' "
      "KEYWORD SEARCH(title, body) QUERY 'payment timeout' TOP_K 2");
  expect(keyword_explain.find("KeywordSearch columns=2 top_k=2") != std::string::npos,
         "sql_keyword_search_explain_logical");

  Table keyword_cn_users;
  keyword_cn_users.schema = Schema({"id", "title", "body"});
  keyword_cn_users.rows = {
      {Value(int64_t(1)), Value("支付超时"), Value("订单支付超时，需要尽快处理")},
      {Value(int64_t(2)), Value("退款延迟"), Value("退款流程排队较慢")},
      {Value(int64_t(3)), Value("支付重试"), Value("支付超时后重试成功")},
      {Value(int64_t(4)), Value("支付成功"), Value("支付处理超时告警")},
  };
  s.createTempView("t_keyword_cn_v1", s.createDataFrame(keyword_cn_users));
  Table keyword_cn_result = s.submit(
      "SELECT id, title, keyword_score FROM t_keyword_cn_v1 "
      "KEYWORD SEARCH(title, body) QUERY '支付超时' TOP_K 2");
  expect(keyword_cn_result.rows.size() == 2, "sql_keyword_search_cn_row_count");
  expect(keyword_cn_result.rows[0][0].asInt64() == 1, "sql_keyword_search_cn_first_id");
  const auto second_cn_id = keyword_cn_result.rows[1][0].asInt64();
  expect(second_cn_id == 3 || second_cn_id == 4, "sql_keyword_search_cn_second_id");

  s.submit("CREATE TABLE t_insert_select_positional_v1 (region STRING, total_score INT)");
  expectNoThrow(
      "insert_select_without_target_columns_uses_positional_mapping",
      [&]() {
        s.submit(
            "INSERT INTO t_insert_select_positional_v1 "
            "SELECT region AS area, score AS points FROM t_users_v1 LIMIT 2");
      });
  Table positional = s.submit(
      "SELECT region, total_score FROM t_insert_select_positional_v1 WHERE total_score > 0 LIMIT 5");
  expect(positional.rows.size() == 2, "insert_select_positional_rows");
  expect(positional.rows[0][0].toString() == "apac", "insert_select_positional_first_region");
  expect(positional.rows[0][1].asInt64() == 25, "insert_select_positional_first_score");

  s.submit("CREATE TABLE t_null_users (user_id INT, region STRING)");
  s.submit("INSERT INTO t_null_users VALUES (1, NULL)");
  Table null_string_function_batch = s.submit(
      "SELECT LOWER(region) AS region_lower FROM t_null_users LIMIT 1");
  expect(null_string_function_batch.rows.size() == 1, "null_string_function_batch_rows");
  expect(null_string_function_batch.rows[0][0].isNull(), "null_string_function_result_is_null");

  s.submit(
      "CREATE TABLE t_insert_select_explicit_v1 (region STRING, total_score INT, note STRING)");
  expectNoThrow(
      "insert_select_with_target_columns_uses_positional_mapping",
      [&]() {
        s.submit(
            "INSERT INTO t_insert_select_explicit_v1 (total_score, region) "
            "SELECT score AS points, region AS area FROM t_users_v1 LIMIT 1");
      });
  Table explicit_target = s.submit(
      "SELECT region, total_score, note FROM t_insert_select_explicit_v1 LIMIT 1");
  expect(explicit_target.rows.size() == 1, "insert_select_explicit_rows");
  expect(explicit_target.rows[0][0].toString() == "apac", "insert_select_explicit_region");
  expect(explicit_target.rows[0][1].asInt64() == 25, "insert_select_explicit_score");
  expect(explicit_target.rows[0][2].isNull(), "insert_select_explicit_unspecified_column_is_null");

  Table string_function_batch = s.submit(
      "SELECT LOWER(region) AS region_lower, UPPER(region) AS region_upper, "
      "TRIM('  spaced  ') AS trimmed, "
      "CONCAT(region, '-', user_id) AS region_user, SUBSTR(region, 2, 2) AS region_prefix "
      "FROM t_users_v1 LIMIT 1");
  expect(string_function_batch.rows.size() == 1, "string_function_query_returns_row");
  expect(string_function_batch.rows[0][0].toString() == "apac", "string_function_lower_ok");
  expect(string_function_batch.rows[0][1].toString() == "APAC", "string_function_upper_ok");
  expect(string_function_batch.rows[0][2].toString() == "spaced", "string_function_trim_ok");
  expect(string_function_batch.rows[0][3].toString() == "apac-1", "string_function_concat_ok");
  expect(string_function_batch.rows[0][4].toString() == "pa", "string_function_substr_ok");

  Table more_string_function_batch = s.submit(
      "SELECT LTRIM('  abc  ') AS left_trimmed, RTRIM('  abc  ') AS right_trimmed, "
      "REPLACE(region, 'a', 'A') AS replaced "
      "FROM t_users_v1 LIMIT 1");
  expect(more_string_function_batch.rows.size() == 1, "more_string_function_batch_rows");
  expect(more_string_function_batch.rows[0][0].toString() == "abc  ", "string_function_ltrim_ok");
  expect(more_string_function_batch.rows[0][1].toString() == "  abc", "string_function_rtrim_ok");
  expect(more_string_function_batch.rows[0][2].toString() == "ApAc", "string_function_replace_ok");

  Table extended_string_function_batch = s.submit(
      "SELECT CONCAT_WS('-', region, user_id) AS region_id, "
      "LEFT(region, 2) AS left_region, RIGHT(region, 2) AS right_region, "
      "POSITION('a', region) AS a_pos "
      "FROM t_users_v1 LIMIT 1");
  expect(extended_string_function_batch.rows.size() == 1, "extended_string_function_batch_rows");
  expect(extended_string_function_batch.rows[0][0].toString() == "apac-1",
         "string_function_concat_ws_ok");
  expect(extended_string_function_batch.rows[0][1].toString() == "ap",
         "string_function_left_ok");
  expect(extended_string_function_batch.rows[0][2].toString() == "ac",
         "string_function_right_ok");
  expect(extended_string_function_batch.rows[0][3].asInt64() == 1, "string_function_position_ok");

  Table alias_and_extra_string_function_batch = s.submit(
      "SELECT LEN(region) AS len_region, CHAR_LENGTH(region) AS char_len, "
      "CHARACTER_LENGTH(region) AS char_len2, REVERSE(region) AS region_rev "
      "FROM t_users_v1 LIMIT 1");
  expect(alias_and_extra_string_function_batch.rows.size() == 1,
         "alias_and_extra_string_function_batch_rows");
  expect(alias_and_extra_string_function_batch.rows[0][0].asInt64() == 4, "string_function_len_region");
  expect(alias_and_extra_string_function_batch.rows[0][1].asInt64() == 4,
         "string_function_char_length_region");
  expect(alias_and_extra_string_function_batch.rows[0][2].asInt64() == 4,
         "string_function_character_length_region");
  expect(alias_and_extra_string_function_batch.rows[0][3].toString() == "capa",
         "string_function_reverse_region");

  Table no_from_time_batch = s.submit(
      "SELECT NOW() AS now_value, TODAY() AS today_value, currentTimestamp() AS current_ts, "
      "UNIX_TIMESTAMP() AS epoch_s");
  expect(no_from_time_batch.rows.size() == 1, "time_function_no_from_rows");
  expect(no_from_time_batch.schema.fields.size() == 4, "time_function_no_from_columns");
  expect(no_from_time_batch.rows[0][0].toString().size() >= 19, "time_function_now_shape");
  expect(no_from_time_batch.rows[0][1].toString().size() == 10, "time_function_today_shape");
  expect(no_from_time_batch.rows[0][2].toString().size() >= 19,
         "time_function_current_timestamp_shape");
  expect(no_from_time_batch.rows[0][3].asInt64() > 1700000000,
         "time_function_unix_timestamp_current_value");

  Table no_from_mixed_time_batch = s.submit(
      "SELECT NOW() AS now_value, 7 AS marker, TODAY() AS today_value");
  expect(no_from_mixed_time_batch.rows.size() == 1, "time_function_no_from_mixed_rows");
  expect(no_from_mixed_time_batch.rows[0][1].asInt64() == 7,
         "time_function_no_from_mixed_literal_position");
  expect(no_from_mixed_time_batch.rows[0][2].toString().size() == 10,
         "time_function_no_from_mixed_scalar_position");

  Table stable_time_batch = s.submit(
      "SELECT UNIX_TIMESTAMP('2026-01-01') AS epoch_s, "
      "YEAR(TODAY()) AS current_year, LENGTH(TODAY()) AS today_len "
      "FROM t_users_v1 LIMIT 1");
  expect(stable_time_batch.rows.size() == 1, "time_function_stable_rows");
  expect(stable_time_batch.rows[0][0].asInt64() == 1767225600,
         "time_function_unix_timestamp_literal");
  expect(stable_time_batch.rows[0][1].asInt64() >= 2020, "time_function_today_year");
  expect(stable_time_batch.rows[0][2].asInt64() == 10, "time_function_today_length");

  Table grouped_time_batch = s.submit(
      "SELECT TODAY() AS today_value, COUNT(*) AS n FROM t_users_v1 GROUP BY TODAY()");
  expect(grouped_time_batch.rows.size() == 1, "time_function_grouped_rows");
  expect(grouped_time_batch.rows[0][1].asInt64() == 3, "time_function_grouped_count");

  Table ordered_batch = s.submit(
      "SELECT user_id, region, score FROM t_users_v1 ORDER BY score DESC, user_id ASC LIMIT 3");
  expect(ordered_batch.rows.size() == 3, "order_by_batch_rows");
  expect(ordered_batch.rows[0][0].asInt64() == 3, "order_by_batch_first_user");
  expect(ordered_batch.rows[1][0].asInt64() == 1, "order_by_batch_second_user");
  expect(ordered_batch.rows[2][0].asInt64() == 2, "order_by_batch_third_user");

  expectThrows(
      "order_by_hidden_batch_column_rejected",
      [&]() {
        s.submit("SELECT region FROM t_users_v1 ORDER BY score DESC");
      },
      "ORDER BY column must appear in SELECT output");

  Table substring_alias_string_function_batch = s.submit(
      "SELECT SUBSTRING(region, 2, 2) AS head FROM t_users_v1 LIMIT 1");
  expect(substring_alias_string_function_batch.rows.size() == 1, "substring_alias_batch_rows");
  expect(substring_alias_string_function_batch.rows[0][0].toString() == "pa", "substring_alias_substr_ok");

  expectThrows(
      "planner_string_function_group_expression_required",
      [&]() {
        s.submit("SELECT LOWER(region) AS region_lower, SUM(score) AS total_score FROM t_users_v1 "
                 "GROUP BY region");
      },
      "non-aggregate expression must appear in GROUP BY");

  Table lower_grouped_batch = s.submit(
      "SELECT LOWER(region) AS region_lower, SUM(score) AS total_score FROM t_users_v1 "
      "GROUP BY LOWER(region) ORDER BY region_lower");
  expect(lower_grouped_batch.rows.size() == 3, "planner_string_function_grouped_rows");
  expect(lower_grouped_batch.rows[0][0].toString() == "apac",
         "planner_string_function_grouped_first_key");

  expectThrows(
      "planner_concat_arity_zero_rejected",
      [&]() {
        s.submit("SELECT CONCAT() FROM t_users_v1");
      },
      "CONCAT expects at least 1 argument");

  expectThrows(
      "planner_length_arity_error",
      [&]() {
        s.submit("SELECT LENGTH() FROM t_users_v1");
      },
      "LENGTH expects 1 argument");

  expectThrows(
      "planner_reverse_arity_error",
      [&]() {
        s.submit("SELECT REVERSE(region, '-') FROM t_users_v1");
      },
      "REVERSE expects 1 argument");

  expectThrows(
      "planner_now_arity_error",
      [&]() {
        s.submit("SELECT NOW(1) FROM t_users_v1");
      },
      "current time scalar function expects 0 arguments");

  expectThrows(
      "planner_unix_timestamp_arity_error",
      [&]() {
        s.submit("SELECT UNIX_TIMESTAMP('2026-01-01', '2026-01-02') FROM t_users_v1");
      },
      "UNIX_TIMESTAMP expects 0 or 1 argument");

  expectThrows(
      "planner_string_function_arity_error",
      [&]() {
        s.submit("SELECT SUBSTR(region, 2, 3, 1) FROM t_users_v1");
      },
      "SUBSTR expects 2 or 3 arguments");

  expectThrows(
      "planner_ltrim_takes_one_argument",
      [&]() {
        s.submit("SELECT LTRIM(region, 1) FROM t_users_v1");
      },
      "LTRIM/RTRIM requires 1 argument");

  expectThrows(
      "planner_replace_requires_three_arguments",
      [&]() {
        s.submit("SELECT REPLACE(region, 'a') FROM t_users_v1");
      },
      "REPLACE requires exactly 3 arguments");

  expectThrows(
      "planner_left_requires_two_arguments",
      [&]() {
        s.submit("SELECT LEFT(region) FROM t_users_v1");
      },
      "LEFT requires 2 arguments");

  expectThrows(
      "planner_right_requires_two_arguments",
      [&]() {
        s.submit("SELECT RIGHT(region, 1, 2) FROM t_users_v1");
      },
      "RIGHT requires 2 arguments");

  expectThrows(
      "planner_concat_ws_requires_at_least_two_arguments",
      [&]() {
        s.submit("SELECT CONCAT_WS(',') FROM t_users_v1");
      },
      "CONCAT_WS requires at least 2 arguments");

  expectThrows(
      "planner_position_requires_two_arguments",
      [&]() {
        s.submit("SELECT POSITION(region) FROM t_users_v1");
      },
      "POSITION requires 2 arguments");
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

  const auto st = parseSqlForRegression(
      "SELECT u.region, SUM(a.score) AS total_score "
      "FROM users u INNER JOIN actions a ON (u.user_id = a.user_id) "
      "WHERE a.score > 5 "
      "GROUP BY u.region "
      "HAVING total_score > 10 "
      "ORDER BY total_score DESC "
      "LIMIT 5");
  dataflow::sql::SqlPlanner planner;
  const auto logical = planner.buildLogicalPlan(st.query, catalog);
  const auto physical = planner.buildPhysicalPlan(logical);
  const auto df = planner.materializeFromPhysical(physical);

  expect(logical.steps.size() >= 5, "planner_logical_has_operators");
  expect(std::any_of(logical.steps.begin(), logical.steps.end(),
                     [](const dataflow::sql::LogicalPlanStep& step) {
                       return step.kind == dataflow::sql::LogicalStepKind::OrderBy;
                     }),
         "planner_logical_has_order_by");
  expect(physical.steps.size() == logical.steps.size(), "planner_physical_step_count_match");
  expect(df.toTable().rowCount() > 0, "planner_execution_has_output");
  expect(df.schema().fields.size() == 2, "planner_output_schema_two_columns");
  expect(df.schema().fields[0] == "region", "planner_output_col_region");
  expect(df.schema().fields[1] == "total_score", "planner_output_col_total_score");
}

std::string explainFieldValue(const std::string& explain, const std::string& key) {
  const std::string prefix = key + "=";
  const auto pos = explain.find(prefix);
  if (pos == std::string::npos) {
    return "";
  }
  const auto start = pos + prefix.size();
  const auto end = explain.find('\n', start);
  return explain.substr(start, end == std::string::npos ? std::string::npos : end - start);
}

void runBatchExplainRegression() {
  DataflowSession& session = DataflowSession::builder();
  Table left(Schema({"alpha", "beta", "gamma"}),
             {Row({Value(int64_t(1)), Value(int64_t(10)), Value(int64_t(3))}),
              Row({Value(int64_t(1)), Value(int64_t(10)), Value(int64_t(7))}),
              Row({Value(int64_t(2)), Value(int64_t(20)), Value(int64_t(11))})});
  Table right(Schema({"window_start", "key", "value"}),
              {Row({Value(int64_t(1)), Value(int64_t(10)), Value(int64_t(3))}),
               Row({Value(int64_t(1)), Value(int64_t(10)), Value(int64_t(7))}),
               Row({Value(int64_t(2)), Value(int64_t(20)), Value(int64_t(11))})});
  session.createTempView("batch_explain_generic_a", DataFrame(left));
  session.createTempView("batch_explain_generic_b", DataFrame(right));

  const std::string explain_a = session.explainSql(
      "SELECT alpha, beta, SUM(gamma) AS total_value "
      "FROM batch_explain_generic_a GROUP BY alpha, beta");
  const std::string explain_b = session.explainSql(
      "SELECT window_start, key, SUM(value) AS total_value "
      "FROM batch_explain_generic_b GROUP BY window_start, key");

  expect(explain_a.find("logical\n") != std::string::npos,
         "batch_explain_contains_logical_section");
  expect(explain_a.find("physical\n") != std::string::npos,
         "batch_explain_contains_physical_section");
  expect(explain_a.find("strategy\n") != std::string::npos,
         "batch_explain_contains_strategy_section");
  expect(!explainFieldValue(explain_a, "selected_impl").empty(),
         "batch_explain_reports_selected_impl");
  expect(explainFieldValue(explain_a, "selected_impl") == explainFieldValue(explain_b, "selected_impl"),
         "batch_explain_strategy_is_name_agnostic");

  Table mixed_nullable(
      Schema({"region_code", "segment_name", "score_value"}),
      {Row({Value(int64_t(1)), Value("alpha"), Value(int64_t(3))}),
       Row({Value(), Value("alpha"), Value(int64_t(7))}),
       Row({Value(int64_t(1)), Value(), Value(int64_t(11))}),
       Row({Value(int64_t(2)), Value("beta"), Value(int64_t(13))})});
  session.createTempView("batch_explain_mixed_nullable", DataFrame(mixed_nullable));
  const std::string mixed_explain = session.explainSql(
      "SELECT region_code, segment_name, SUM(score_value) AS total_value "
      "FROM batch_explain_mixed_nullable GROUP BY region_code, segment_name");
  expect(explainFieldValue(mixed_explain, "selected_impl") == "hash-packed",
         "batch_explain_mixed_nullable_prefers_hash_packed");

  Table mixed_three_keys(
      Schema({"tenant_id", "region_name", "segment_name", "score_value"}),
      {Row({Value(int64_t(1)), Value("apac"), Value("alpha"), Value(int64_t(3))}),
       Row({Value(int64_t(1)), Value("apac"), Value("alpha"), Value(int64_t(7))}),
       Row({Value(int64_t(1)), Value("emea"), Value("beta"), Value(int64_t(11))}),
       Row({Value(int64_t(2)), Value("emea"), Value("beta"), Value(int64_t(13))})});
  session.createTempView("batch_explain_three_keys", DataFrame(mixed_three_keys));
  const std::string three_key_explain = session.explainSql(
      "SELECT tenant_id, region_name, segment_name, SUM(score_value) AS total_value "
      "FROM batch_explain_three_keys GROUP BY tenant_id, region_name, segment_name");
  expect(explainFieldValue(three_key_explain, "selected_impl") == "hash-packed",
         "batch_explain_three_key_mixed_prefers_hash_packed");

  Table mixed_bool_keys(
      Schema({"tenant_id", "region_name", "success", "score_value"}),
      {Row({Value(int64_t(1)), Value("apac"), Value(true), Value(int64_t(3))}),
       Row({Value(int64_t(1)), Value("apac"), Value(false), Value(int64_t(7))}),
       Row({Value(int64_t(1)), Value("emea"), Value(false), Value(int64_t(11))}),
       Row({Value(int64_t(2)), Value("emea"), Value(true), Value(int64_t(13))})});
  session.createTempView("batch_explain_bool_keys", DataFrame(mixed_bool_keys));
  const std::string bool_key_explain = session.explainSql(
      "SELECT tenant_id, region_name, success, SUM(score_value) AS total_value "
      "FROM batch_explain_bool_keys GROUP BY tenant_id, region_name, success");
  expect(explainFieldValue(bool_key_explain, "selected_impl") == "hash-packed",
         "batch_explain_three_key_bool_prefers_hash_packed");

  Table leading_zero_keys(
      Schema({"date", "hour", "caller_psm", "score_value"}),
      {Row({Value("2026-04-04"), Value("01"), Value("svcA"), Value(int64_t(3))}),
       Row({Value("2026-04-04"), Value("01"), Value("svcA"), Value(int64_t(7))}),
       Row({Value("2026-04-04"), Value("10"), Value("svcA"), Value(int64_t(11))})});
  session.createTempView("batch_explain_leading_zero", DataFrame(leading_zero_keys));
  auto leading_zero_rows =
      session.sql("SELECT date, hour, caller_psm, COUNT(*) AS cnt "
                  "FROM batch_explain_leading_zero GROUP BY date, hour, caller_psm")
          .toTable();
  dataflow::materializeRows(&leading_zero_rows);
  expect(leading_zero_rows.rows.size() == 2,
         "batch_group_by_preserves_leading_zero_hour_groups");
}

void runColumnarFallbackExplainRegression() {
  DataflowSession session;
  Table invalid_cache_table(
      Schema({"region", "score"}),
      {Row({Value("apac"), Value(int64_t(3))}),
       Row({Value("emea"), Value(int64_t(5))})});
  invalid_cache_table.columnar_cache = std::make_shared<dataflow::ColumnarTable>();
  invalid_cache_table.columnar_cache->schema = invalid_cache_table.schema;
  invalid_cache_table.columnar_cache->columns.resize(invalid_cache_table.schema.fields.size());
  invalid_cache_table.columnar_cache->arrow_formats.resize(invalid_cache_table.schema.fields.size());
  invalid_cache_table.columnar_cache->row_count = invalid_cache_table.rows.size();
  invalid_cache_table.columnar_cache->batch_row_counts.push_back(invalid_cache_table.rows.size());
  invalid_cache_table.columnar_cache->columns[0].values.push_back(Value("apac"));
  invalid_cache_table.columnar_cache->columns[1].values.push_back(Value(int64_t(3)));
  invalid_cache_table.columnar_cache->columns[1].values.push_back(Value(int64_t(5)));

  session.createTempView("invalid_columnar_explain_input", DataFrame(invalid_cache_table));
  const std::string explain = session.explainSql(
      "SELECT region, SUM(score) AS total_score "
      "FROM invalid_columnar_explain_input GROUP BY region");
  expect(explain.find("columnar_fallback_type=invalid-columnar-cache") != std::string::npos,
         "batch_explain_reports_invalid_columnar_cache");
  expect(explain.find("columnar_reason=input table has an invalid retained columnar cache") !=
             std::string::npos,
         "batch_explain_reports_invalid_columnar_reason");
  expect(explain.find("input row count mismatch") != std::string::npos,
         "batch_explain_preserves_invalid_columnar_validation_detail");
}

void runAfterParseRewriteRegression() {
  expectNoThrow(
      "after_parse_rewrite_reparses_statement",
      []() {
        ScopedPluginRegistration registration(std::make_shared<AfterParseRewritePlugin>());
        DataflowSession session;
        Table input(
            Schema({"region", "score"}),
            {Row({Value("apac"), Value(int64_t(7))})});
        session.createTempView("after_parse_rewrite_input", DataFrame(input));

        auto out = session.sql("SELECT region FROM after_parse_rewrite_input").toTable();
        expect(out.schema.fields.size() == 1, "after_parse_rewrite_schema_width");
        expect(out.schema.fields[0] == "score", "after_parse_rewrite_schema_uses_new_sql");
        expect(out.rows.size() == 1, "after_parse_rewrite_row_count");
        expect(out.rows[0][0].asInt64() == 7, "after_parse_rewrite_result_value");
      });
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
            "USING csv OPTIONS(path: '" + input_dir + "', delimiter: ',')");
      });

  expectNoThrow(
      "stream_sql_create_sink_csv",
      [&]() {
        s.sql(
            "CREATE SINK TABLE stream_summary_csv_v1 (key STRING, value_sum INT) "
            "USING csv OPTIONS(path: '" + sink_path + "', delimiter: ',')");
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

  auto sink_table = s.read_csv(sink_path).toTable();
  dataflow::materializeRows(&sink_table);
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
      "stream_sql_star_mixed_projection_rejected",
      [&]() {
        s.streamSql("SELECT *, key FROM stream_events_csv_v1");
      },
      "SELECT * cannot be mixed with other projections");

  expectThrows(
      "stream_sql_order_by_unbounded_source_rejected",
      [&]() {
        auto rejected_query = s.startStreamSql(
            "INSERT INTO stream_summary_csv_v1 "
            "SELECT key, value FROM stream_events_csv_v1 ORDER BY value DESC",
            options);
        rejected_query.awaitTermination(1);
      },
      "bounded source");

  expectThrows(
      "stream_sql_explain_order_by_unbounded_source_rejected",
      [&]() {
        s.explainStreamSql(
            "INSERT INTO stream_summary_csv_v1 "
            "SELECT key, value FROM stream_events_csv_v1 ORDER BY value DESC",
            options);
      },
      "bounded source");

  const std::string function_sink_path = "/tmp/velaria-stream-sql-function-output.csv";
  const std::string function_checkpoint_path = "/tmp/velaria-stream-sql-function-checkpoint";
  fs::remove(function_sink_path);
  fs::remove(function_checkpoint_path);
  dataflow::StreamingQueryOptions function_options;
  function_options.trigger_interval_ms = 0;
  function_options.local_workers = 2;
  function_options.max_inflight_partitions = 2;
  function_options.checkpoint_path = function_checkpoint_path;

  dataflow::Table function_batch;
  function_batch.schema = dataflow::Schema({"ts", "key", "value"});
  function_batch.rows = {
      {Value("2026-03-29T10:00:00"), Value("Akey"), Value(int64_t(-10))},
      {Value("2026-03-29T10:01:00"), Value("Bkey"), Value(int64_t(5))},
  };
  s.createTempView("stream_function_events_v1",
                   s.readStream(std::make_shared<dataflow::MemoryStreamSource>(
                       std::vector<dataflow::Table>{function_batch})));

  s.sql(
      "CREATE SINK TABLE stream_function_summary_v1 "
      "(key_lower STRING, key_upper STRING, key_head STRING, value_abs INT, year INT, month INT, day INT) "
      "USING csv OPTIONS(path: '" +
      function_sink_path + "', delimiter: ',')");

  const auto function_explain = s.explainStreamSql(
      "INSERT INTO stream_function_summary_v1 "
      "SELECT LOWER(key) AS key_lower, UPPER(key) AS key_upper, LEFT(key, 2) AS key_head, "
      "ABS(value) AS value_abs, YEAR(ts) AS year, MONTH(ts) AS month, DAY(ts) AS day "
      "FROM stream_function_events_v1",
      function_options);
  expect(function_explain.find("WithColumn") != std::string::npos,
         "stream_sql_explain_string_function_enabled");
  expect(function_explain.find("function=lower") != std::string::npos,
         "stream_sql_explain_lower_function");
  expect(function_explain.find("function=abs") != std::string::npos,
         "stream_sql_explain_abs_function");

  expectNoThrow("stream_sql_string_functions_supported", [&]() {
    auto function_query = s.startStreamSql(
        "INSERT INTO stream_function_summary_v1 "
        "SELECT LOWER(key) AS key_lower, UPPER(key) AS key_upper, LEFT(key, 2) AS key_head, "
        "ABS(value) AS value_abs, YEAR(ts) AS year, MONTH(ts) AS month, DAY(ts) AS day "
        "FROM stream_function_events_v1",
        function_options);
    expect(function_query.awaitTermination(1) == 1, "stream_sql_function_query_processed_batches");
  });

  auto function_sink_table = s.read_csv(function_sink_path).toTable();
  dataflow::materializeRows(&function_sink_table);
  expect(function_sink_table.rows.size() == 2, "stream_sql_function_output_rows");
  const auto function_k_lower_idx = function_sink_table.schema.indexOf("key_lower");
  const auto function_k_upper_idx = function_sink_table.schema.indexOf("key_upper");
  const auto function_k_head_idx = function_sink_table.schema.indexOf("key_head");
  const auto function_abs_idx = function_sink_table.schema.indexOf("value_abs");
  const auto function_year_idx = function_sink_table.schema.indexOf("year");
  const auto function_month_idx = function_sink_table.schema.indexOf("month");
  const auto function_day_idx = function_sink_table.schema.indexOf("day");

  bool has_function_a = false;
  bool has_function_b = false;
  for (const auto& row : function_sink_table.rows) {
    if (row[function_k_lower_idx].toString() == "akey" && row[function_k_upper_idx].toString() == "AKEY" &&
        row[function_k_head_idx].toString() == "Ak" && row[function_abs_idx].asInt64() == 10 &&
        row[function_year_idx].asInt64() == 2026 && row[function_month_idx].asInt64() == 3 &&
        row[function_day_idx].asInt64() == 29) {
      has_function_a = true;
    }
    if (row[function_k_lower_idx].toString() == "bkey" && row[function_k_upper_idx].toString() == "BKEY" &&
        row[function_k_head_idx].toString() == "Bk" && row[function_abs_idx].asInt64() == 5 &&
        row[function_year_idx].asInt64() == 2026 && row[function_month_idx].asInt64() == 3 &&
        row[function_day_idx].asInt64() == 29) {
      has_function_b = true;
    }
  }
  expect(has_function_a, "stream_sql_function_output_has_function_row_a");
  expect(has_function_b, "stream_sql_function_output_has_function_row_b");

  const std::string order_sink_path = "/tmp/velaria-stream-sql-order-output.csv";
  fs::remove(order_sink_path);
  dataflow::Table order_batch;
  order_batch.schema = dataflow::Schema({"id", "key", "value"});
  order_batch.rows = {
      {Value(int64_t(1)), Value("c"), Value(int64_t(5))},
      {Value(int64_t(2)), Value("a"), Value(int64_t(5))},
      {Value(int64_t(3)), Value("b"), Value(int64_t(9))},
  };
  s.createTempView(
      "stream_order_events_v1",
      s.readStream(std::make_shared<dataflow::MemoryStreamSource>(std::vector<Table>{order_batch})));
  s.sql(
      "CREATE SINK TABLE stream_order_summary_v1 (id INT, key STRING, value INT) "
      "USING csv OPTIONS(path: '" +
      order_sink_path + "', delimiter: ',')");
  auto order_query = s.startStreamSql(
      "INSERT INTO stream_order_summary_v1 "
      "SELECT id, key, value FROM stream_order_events_v1 ORDER BY value DESC, key ASC",
      function_options);
  expect(order_query.awaitTermination(1) == 1, "stream_sql_order_query_processed_batches");
  expectThrows(
      "stream_sql_order_by_hidden_column_rejected",
      [&]() {
        s.streamSql("SELECT key FROM stream_order_events_v1 ORDER BY value DESC");
      },
      "ORDER BY column must appear in SELECT output");
  auto order_sink_table = s.read_csv(order_sink_path).toTable();
  dataflow::materializeRows(&order_sink_table);
  expect(order_sink_table.rows.size() == 3, "stream_sql_order_output_rows");
  expect(order_sink_table.rows[0][0].asInt64() == 3, "stream_sql_order_first_row");
  expect(order_sink_table.rows[1][0].asInt64() == 2, "stream_sql_order_second_row");
  expect(order_sink_table.rows[2][0].asInt64() == 1, "stream_sql_order_third_row");

  expectThrows(
      "stream_sql_explain_insert_column_list_rejected",
      [&]() {
        s.explainStreamSql(
            "INSERT INTO stream_summary_csv_v1 (key, value_sum) "
            "SELECT key, SUM(value) AS value_sum "
            "FROM stream_events_csv_v1 GROUP BY key");
      },
      "does not support INSERT column list");

  expectNoThrow(
      "stream_sql_explain_string_function_supported",
      [&]() {
        const auto explain = s.explainStreamSql("SELECT LOWER(key) AS key_lower FROM stream_events_csv_v1",
                                              options);
        expect(explain.find("function=lower") != std::string::npos,
               "stream_sql_explain_string_function_lower");
      });

  const std::string multi_sink_path = "/tmp/velaria-stream-sql-multi-aggregate-output.csv";
  fs::remove(multi_sink_path);
  dataflow::Table multi_batch;
  multi_batch.schema = dataflow::Schema({"key", "value"});
  multi_batch.rows = {
      {Value("userA"), Value(int64_t(10))},
      {Value("userA"), Value(int64_t(5))},
      {Value("userA"), Value(int64_t(7))},
      {Value("userB"), Value(int64_t(20))},
  };
  s.createTempView(
      "stream_multi_events_v1",
      s.readStream(std::make_shared<dataflow::MemoryStreamSource>(std::vector<Table>{multi_batch})));
  s.sql(
      "CREATE SINK TABLE stream_multi_summary_v1 "
      "(key STRING, value_sum INT, event_count INT, min_value INT, max_value INT, avg_value DOUBLE) "
      "USING csv OPTIONS(path: '" +
      multi_sink_path + "', delimiter: ',')");
  const std::string multi_hot_sink_path = "/tmp/velaria-stream-sql-multi-aggregate-hot-output.csv";
  fs::remove(multi_hot_sink_path);
  s.sql(
      "CREATE SINK TABLE stream_multi_hot_summary_v1 "
      "(key STRING, value_sum INT, event_count INT, min_value INT, max_value INT, avg_value DOUBLE) "
      "USING csv OPTIONS(path: '" +
      multi_hot_sink_path + "', delimiter: ',')");

  dataflow::StreamingQueryOptions multi_options;
  multi_options.trigger_interval_ms = 0;
  multi_options.execution_mode = dataflow::StreamingExecutionMode::LocalWorkers;
  multi_options.local_workers = 2;
  multi_options.max_inflight_partitions = 2;

  auto multi_hot_query = s.startStreamSql(
      "INSERT INTO stream_multi_hot_summary_v1 "
      "SELECT key, SUM(value) AS value_sum, COUNT(*) AS event_count, "
      "MIN(value) AS min_value, MAX(value) AS max_value, AVG(value) AS avg_value "
      "FROM stream_multi_events_v1 "
      "GROUP BY key",
      multi_options);
  expect(multi_hot_query.awaitTermination(1) == 1,
         "stream_sql_multi_aggregate_hot_processed_batches");
  expect(multi_hot_query.progress().execution_mode == "local-workers",
         "stream_sql_multi_aggregate_hot_local_workers_mode");
  expect(multi_hot_query.progress().used_actor_runtime,
         "stream_sql_multi_aggregate_hot_credit_accelerator_used");

  auto multi_hot_sink_table = s.read_csv(multi_hot_sink_path).toTable();
  dataflow::materializeRows(&multi_hot_sink_table);
  expect(multi_hot_sink_table.rows.size() == 2, "stream_sql_multi_aggregate_hot_rows");

  auto multi_query = s.startStreamSql(
      "INSERT INTO stream_multi_summary_v1 "
      "SELECT key, SUM(value) AS value_sum, COUNT(*) AS event_count, "
      "MIN(value) AS min_value, MAX(value) AS max_value, AVG(value) AS avg_value "
      "FROM stream_multi_events_v1 "
      "GROUP BY key "
      "HAVING avg_value > 7",
      multi_options);
  expect(multi_query.awaitTermination(1) == 1, "stream_sql_multi_aggregate_processed_batches");

  auto multi_sink_table = s.read_csv(multi_sink_path).toTable();
  dataflow::materializeRows(&multi_sink_table);
  expect(multi_sink_table.rows.size() == 2, "stream_sql_multi_aggregate_rows");
  bool has_multi_user_a = false;
  bool has_multi_user_b = false;
  const auto multi_key_idx = multi_sink_table.schema.indexOf("key");
  const auto multi_sum_idx = multi_sink_table.schema.indexOf("value_sum");
  const auto multi_count_idx = multi_sink_table.schema.indexOf("event_count");
  const auto multi_min_idx = multi_sink_table.schema.indexOf("min_value");
  const auto multi_max_idx = multi_sink_table.schema.indexOf("max_value");
  const auto multi_avg_idx = multi_sink_table.schema.indexOf("avg_value");
  for (const auto& row : multi_sink_table.rows) {
    if (row[multi_key_idx].toString() == "userA" && row[multi_sum_idx].asInt64() == 22 &&
        row[multi_count_idx].asInt64() == 3 && row[multi_min_idx].asInt64() == 5 &&
        row[multi_max_idx].asInt64() == 10 && row[multi_avg_idx].asDouble() > 7.3 &&
        row[multi_avg_idx].asDouble() < 7.4) {
      has_multi_user_a = true;
    }
    if (row[multi_key_idx].toString() == "userB" && row[multi_sum_idx].asInt64() == 20 &&
        row[multi_count_idx].asInt64() == 1 && row[multi_min_idx].asInt64() == 20 &&
        row[multi_max_idx].asInt64() == 20 && row[multi_avg_idx].asDouble() == 20.0) {
      has_multi_user_b = true;
    }
  }
  expect(has_multi_user_a, "stream_sql_multi_aggregate_user_a");
  expect(has_multi_user_b, "stream_sql_multi_aggregate_user_b");

  const std::string batch_csv_path = "/tmp/velaria-sql-create-table-batch.csv";
  {
    std::ofstream out(batch_csv_path);
    out << "key,value\nuserA,10\nuserB,20\n";
  }
  expectNoThrow(
      "sql_create_table_regular_csv_using_provider",
      [&]() {
        s.sql("CREATE TABLE regular_csv_v1 USING csv OPTIONS(path: '" + batch_csv_path + "')");
      });
  auto regular_csv_table =
      s.sql("SELECT key, value FROM regular_csv_v1 ORDER BY key").toTable();
  expect(regular_csv_table.rowCount() == 2, "sql_create_table_regular_csv_row_count");
  expect(regular_csv_table.rows[0][0].asString() == "userA",
         "sql_create_table_regular_csv_first_key");

  const std::string batch_line_path = "/tmp/velaria-sql-create-table-batch.log";
  {
    std::ofstream out(batch_line_path);
    out << "1001|ok|12.5\n1002|fail|9.5\n";
  }
  expectNoThrow(
      "sql_create_table_regular_line_using_provider",
      [&]() {
        s.sql("CREATE TABLE regular_line_v1 USING line OPTIONS(path: '" + batch_line_path +
              "', mode: 'split', delimiter: '|', "
              "columns: 'user_id,state,score')");
      });
  auto regular_line_table =
      s.sql("SELECT user_id, state FROM regular_line_v1 ORDER BY user_id").toTable();
  expect(regular_line_table.rowCount() == 2, "sql_create_table_regular_line_row_count");
  expect(regular_line_table.rows[1][1].asString() == "fail",
         "sql_create_table_regular_line_second_state");

  const std::string batch_json_path = "/tmp/velaria-sql-create-table-batch.jsonl";
  {
    std::ofstream out(batch_json_path);
    out << "{\"user_id\":1,\"name\":\"alice\"}\n";
    out << "{\"user_id\":2,\"name\":\"bob\"}\n";
  }
  expectNoThrow(
      "sql_create_table_regular_json_using_provider",
      [&]() {
        s.sql("CREATE TABLE regular_json_v1 USING json OPTIONS(path: '" + batch_json_path +
              "', columns: 'user_id,name', format: 'json_lines')");
      });
  auto regular_json_table =
      s.sql("SELECT name FROM regular_json_v1 WHERE user_id > 1").toTable();
  expect(regular_json_table.rowCount() == 1, "sql_create_table_regular_json_row_count");
  expect(regular_json_table.rows[0][0].asString() == "bob",
         "sql_create_table_regular_json_filter");

  expectNoThrow(
      "sql_create_table_probe_only_path",
      [&]() {
        s.sql("CREATE TABLE regular_json_auto_v1 OPTIONS(path: '" + batch_json_path + "')");
      });
  auto regular_json_auto_table =
      s.sql("SELECT user_id FROM regular_json_auto_v1 ORDER BY user_id").toTable();
  expect(regular_json_auto_table.rowCount() == 2, "sql_create_table_probe_only_path_row_count");

  const std::string window_sink_path = "/tmp/velaria-stream-window-sql-regression-output.csv";
  fs::remove(window_sink_path);

  dataflow::Table window_batch;
  window_batch.schema = dataflow::Schema({"ts", "key", "value"});
  window_batch.rows = {
      {Value("2026-03-29T10:00:00"), Value("userA"), Value(int64_t(1))},
      {Value("2026-03-29T10:00:10"), Value("userA"), Value(int64_t(2))},
      {Value("2026-03-29T10:01:05"), Value("userB"), Value(int64_t(3))},
  };
  s.createTempView(
      "stream_window_events_v1",
      s.readStream(std::make_shared<dataflow::MemoryStreamSource>(std::vector<Table>{window_batch})));
  s.sql(
      "CREATE SINK TABLE stream_window_summary_v1 (window_start STRING, key STRING, value_sum INT) "
      "USING csv OPTIONS(path: '" +
      window_sink_path + "', delimiter: ',')");

  dataflow::StreamingQueryOptions window_options;
  window_options.trigger_interval_ms = 0;
  window_options.execution_mode = dataflow::StreamingExecutionMode::LocalWorkers;
  window_options.local_workers = 2;
  window_options.max_inflight_partitions = 2;

  auto window_query = s.startStreamSql(
      "INSERT INTO stream_window_summary_v1 "
      "SELECT window_start, key, SUM(value) AS value_sum "
      "FROM stream_window_events_v1 "
      "WINDOW BY ts EVERY 60000 AS window_start "
      "GROUP BY window_start, key",
      window_options);
  expect(window_query.awaitTermination() == 1, "stream_sql_window_processed_batches");
  expect(window_query.progress().execution_mode == "local-workers",
         "stream_sql_window_local_workers_mode");
  expect(window_query.progress().used_actor_runtime,
         "stream_sql_window_credit_accelerator_used");

  auto window_sink_table = s.read_csv(window_sink_path).toTable();
  dataflow::materializeRows(&window_sink_table);
  expect(window_sink_table.rows.size() == 2, "stream_sql_window_sink_rows");
  bool has_window_user_a = false;
  bool has_window_user_b = false;
  const auto window_idx = window_sink_table.schema.indexOf("window_start");
  const auto window_key_idx = window_sink_table.schema.indexOf("key");
  const auto window_sum_idx = window_sink_table.schema.indexOf("value_sum");
  for (const auto& row : window_sink_table.rows) {
    if (row[window_idx].toString() == "2026-03-29T10:00:00" &&
        row[window_key_idx].toString() == "userA" &&
        row[window_sum_idx].asInt64() == 3) {
      has_window_user_a = true;
    }
    if (row[window_idx].toString() == "2026-03-29T10:01:00" &&
        row[window_key_idx].toString() == "userB" &&
        row[window_sum_idx].asInt64() == 3) {
      has_window_user_b = true;
    }
  }
  expect(has_window_user_a, "stream_sql_window_sink_has_user_a");
  expect(has_window_user_b, "stream_sql_window_sink_has_user_b");

  const std::string window_count_sink_path = "/tmp/velaria-stream-window-count-sql-regression-output.csv";
  fs::remove(window_count_sink_path);
  s.sql(
      "CREATE SINK TABLE stream_window_count_summary_v1 "
      "(window_start STRING, key STRING, event_count INT) "
      "USING csv OPTIONS(path: '" +
      window_count_sink_path + "', delimiter: ',')");

  auto window_count_query = s.startStreamSql(
      "INSERT INTO stream_window_count_summary_v1 "
      "SELECT window_start, key, COUNT(*) AS event_count "
      "FROM stream_window_events_v1 "
      "WINDOW BY ts EVERY 60000 AS window_start "
      "GROUP BY window_start, key",
      window_options);
  expect(window_count_query.awaitTermination() == 1, "stream_sql_window_count_processed_batches");
  expect(window_count_query.progress().execution_mode == "local-workers",
         "stream_sql_window_count_local_workers_mode");
  expect(window_count_query.progress().used_actor_runtime,
         "stream_sql_window_count_credit_accelerator_used");

  auto window_count_sink_table = s.read_csv(window_count_sink_path).toTable();
  dataflow::materializeRows(&window_count_sink_table);
  expect(window_count_sink_table.rows.size() == 2, "stream_sql_window_count_sink_rows");
  bool has_count_user_a = false;
  bool has_count_user_b = false;
  const auto count_window_idx = window_count_sink_table.schema.indexOf("window_start");
  const auto count_key_idx = window_count_sink_table.schema.indexOf("key");
  const auto count_value_idx = window_count_sink_table.schema.indexOf("event_count");
  for (const auto& row : window_count_sink_table.rows) {
    if (row[count_window_idx].toString() == "2026-03-29T10:00:00" &&
        row[count_key_idx].toString() == "userA" &&
        row[count_value_idx].asInt64() == 2) {
      has_count_user_a = true;
    }
    if (row[count_window_idx].toString() == "2026-03-29T10:01:00" &&
        row[count_key_idx].toString() == "userB" &&
        row[count_value_idx].asInt64() == 1) {
      has_count_user_b = true;
    }
  }
  expect(has_count_user_a, "stream_sql_window_count_sink_has_user_a");
  expect(has_count_user_b, "stream_sql_window_count_sink_has_user_b");

  const std::string sliding_sink_path = "/tmp/velaria-stream-sliding-sql-regression-output.csv";
  fs::remove(sliding_sink_path);
  s.sql(
      "CREATE SINK TABLE stream_sliding_summary_v1 "
      "(window_start STRING, key STRING, event_count INT) "
      "USING csv OPTIONS(path: '" +
      sliding_sink_path + "', delimiter: ',')");

  auto sliding_query = s.startStreamSql(
      "INSERT INTO stream_sliding_summary_v1 "
      "SELECT window_start, key, COUNT(*) AS event_count "
      "FROM stream_window_events_v1 "
      "WINDOW BY ts EVERY 60000 SLIDE 30000 AS window_start "
      "GROUP BY window_start, key",
      window_options);
  expect(sliding_query.awaitTermination() == 1, "stream_sql_sliding_processed_batches");

  auto sliding_sink_table = s.read_csv(sliding_sink_path).toTable();
  dataflow::materializeRows(&sliding_sink_table);
  expect(sliding_sink_table.rows.size() == 4, "stream_sql_sliding_sink_rows");
  bool has_user_a_prev = false;
  bool has_user_a_curr = false;
  bool has_user_b_prev = false;
  bool has_user_b_curr = false;
  const auto sliding_window_idx = sliding_sink_table.schema.indexOf("window_start");
  const auto sliding_key_idx = sliding_sink_table.schema.indexOf("key");
  const auto sliding_count_idx = sliding_sink_table.schema.indexOf("event_count");
  for (const auto& row : sliding_sink_table.rows) {
    if (row[sliding_window_idx].toString() == "2026-03-29T09:59:30" &&
        row[sliding_key_idx].toString() == "userA" &&
        row[sliding_count_idx].asInt64() == 2) {
      has_user_a_prev = true;
    }
    if (row[sliding_window_idx].toString() == "2026-03-29T10:00:00" &&
        row[sliding_key_idx].toString() == "userA" &&
        row[sliding_count_idx].asInt64() == 2) {
      has_user_a_curr = true;
    }
    if (row[sliding_window_idx].toString() == "2026-03-29T10:00:30" &&
        row[sliding_key_idx].toString() == "userB" &&
        row[sliding_count_idx].asInt64() == 1) {
      has_user_b_prev = true;
    }
    if (row[sliding_window_idx].toString() == "2026-03-29T10:01:00" &&
        row[sliding_key_idx].toString() == "userB" &&
        row[sliding_count_idx].asInt64() == 1) {
      has_user_b_curr = true;
    }
  }
  expect(has_user_a_prev, "stream_sql_sliding_sink_has_user_a_prev");
  expect(has_user_a_curr, "stream_sql_sliding_sink_has_user_a_curr");
  expect(has_user_b_prev, "stream_sql_sliding_sink_has_user_b_prev");
  expect(has_user_b_curr, "stream_sql_sliding_sink_has_user_b_curr");

  const std::string alias_sink_path = "/tmp/velaria-stream-window-alias-sql-regression-output.csv";
  fs::remove(alias_sink_path);
  s.sql(
      "CREATE SINK TABLE stream_window_alias_summary_v1 "
      "(bucket STRING, entity_key STRING, event_count INT) "
      "USING csv OPTIONS(path: '" +
      alias_sink_path + "', delimiter: ',')");

  auto alias_query = s.startStreamSql(
      "INSERT INTO stream_window_alias_summary_v1 "
      "SELECT window_start AS bucket, key AS entity_key, COUNT(*) AS event_count "
      "FROM stream_window_events_v1 "
      "WINDOW BY ts EVERY 60000 AS window_start "
      "GROUP BY window_start, key",
      window_options);
  expect(alias_query.awaitTermination() == 1, "stream_sql_window_alias_processed_batches");

  auto alias_sink_table = s.read_csv(alias_sink_path).toTable();
  dataflow::materializeRows(&alias_sink_table);
  expect(alias_sink_table.schema.has("bucket"), "stream_sql_window_alias_bucket_column");
  expect(alias_sink_table.schema.has("entity_key"), "stream_sql_window_alias_entity_key_column");
  expect(alias_sink_table.rows.size() == 2, "stream_sql_window_alias_rows");
  bool has_alias_user_a = false;
  bool has_alias_user_b = false;
  const auto alias_bucket_idx = alias_sink_table.schema.indexOf("bucket");
  const auto alias_key_idx = alias_sink_table.schema.indexOf("entity_key");
  const auto alias_count_idx = alias_sink_table.schema.indexOf("event_count");
  for (const auto& row : alias_sink_table.rows) {
    if (row[alias_bucket_idx].toString() == "2026-03-29T10:00:00" &&
        row[alias_key_idx].toString() == "userA" &&
        row[alias_count_idx].asInt64() == 2) {
      has_alias_user_a = true;
    }
    if (row[alias_bucket_idx].toString() == "2026-03-29T10:01:00" &&
        row[alias_key_idx].toString() == "userB" &&
        row[alias_count_idx].asInt64() == 1) {
      has_alias_user_b = true;
    }
  }
  expect(has_alias_user_a, "stream_sql_window_alias_has_user_a");
  expect(has_alias_user_b, "stream_sql_window_alias_has_user_b");

  expectThrows(
      "stream_sql_start_insert_column_list_rejected",
      [&]() {
        s.startStreamSql(
            "INSERT INTO stream_window_count_summary_v1 (window_start, key, event_count) "
            "SELECT window_start, key, COUNT(*) AS event_count "
            "FROM stream_window_events_v1 "
            "WINDOW BY ts EVERY 60000 AS window_start "
            "GROUP BY window_start, key",
            window_options);
      },
      "does not support INSERT column list");

  expectThrowsType<dataflow::SQLTableKindError>(
      "stream_sql_select_non_stream_source_rejected",
      [&]() {
        s.streamSql("SELECT user_id, region FROM t_region_users");
      },
      "table-kind constraint violation");

  s.submit("CREATE TABLE non_stream_batch_sink_v1 (key STRING, value INT)");
  expectThrowsType<dataflow::SQLTableKindError>(
      "stream_sql_insert_to_non_sink_table_rejected",
      [&]() {
        s.startStreamSql(
            "INSERT INTO non_stream_batch_sink_v1 SELECT key, value FROM stream_events_csv_v1",
            options);
      },
      "table-kind constraint violation");

  expectThrowsType<dataflow::SQLTableKindError>(
      "stream_sql_explain_to_non_sink_table_rejected",
      [&]() {
        s.explainStreamSql(
            "INSERT INTO non_stream_batch_sink_v1 "
            "SELECT key, SUM(value) AS value_sum "
            "FROM stream_events_csv_v1 "
            "GROUP BY key",
            options);
      },
      "table-kind constraint violation");

  expectThrowsType<dataflow::CatalogNotFoundError>(
      "stream_sql_source_not_found_rejected",
      [&]() {
        s.streamSql("SELECT key FROM stream_source_missing_v1");
      },
      "stream view not found");
}

}  // namespace

int main() {
  runParserRegression();
  runSemanticRegression();
  runComplexDmlRegression();
  runPlannerPlanRegression();
  runBatchExplainRegression();
  runColumnarFallbackExplainRegression();
  runAfterParseRewriteRegression();
  runStreamSqlRegression();

  if (g_failed == 0) {
    std::cout << "All regression checks passed.\n";
    return 0;
  }

  std::cout << g_failed << " regression checks failed.\n";
  return 1;
}
