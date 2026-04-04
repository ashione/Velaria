#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "src/dataflow/core/contract/catalog/catalog.h"
#include "src/dataflow/core/contract/api/session.h"
#include "src/dataflow/core/logical/sql/sql_errors.h"
#include "src/dataflow/core/logical/sql/sql_planner.h"
#include "src/dataflow/core/logical/sql/sql_parser.h"

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
      "parser_string_function_projection_parsed",
      []() {
        const auto st = dataflow::sql::SqlParser::parse(
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
        const auto st = dataflow::sql::SqlParser::parse(
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
        const auto st = dataflow::sql::SqlParser::parse(
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
        const auto st = dataflow::sql::SqlParser::parse(
            "SELECT SUBSTRING(region, 2, 2) AS head FROM users u");
        expect(st.query.select_items.size() == 1, "parser_substring_alias_string_function_projection_size");
        expect(st.query.select_items[0].is_string_function, "parser_substring_alias_function");
      });

  expectNoThrow(
      "parser_string_function_boolean_argument_is_literal",
      []() {
        const auto st = dataflow::sql::SqlParser::parse(
            "SELECT LEFT(region, TRUE) AS one FROM users u");
        expect(st.query.select_items.size() == 1, "parser_string_function_boolean_argument_size");
        expect(st.query.select_items[0].is_string_function, "parser_string_function_boolean_argument_flag");
      });

  expectNoThrow(
      "parser_length_alias_string_function_projection_parsed",
      []() {
        const auto st = dataflow::sql::SqlParser::parse(
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
      "parser_order_by_projection",
      []() {
        const auto st = dataflow::sql::SqlParser::parse(
            "SELECT user_id, score FROM users ORDER BY score DESC, user_id ASC LIMIT 2");
        expect(st.query.order_by.size() == 2, "parser_order_by_size");
        expect(!st.query.order_by[0].ascending, "parser_order_by_first_desc");
        expect(st.query.order_by[1].ascending, "parser_order_by_second_asc");
        expect(st.query.limit.value_or(0) == 2, "parser_order_by_limit_present");
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

  expectNoThrow(
      "parser_stream_window_clause",
      []() {
        const auto st = dataflow::sql::SqlParser::parse(
            "SELECT window_start, key, SUM(value) AS value_sum FROM stream_events "
            "WINDOW BY ts EVERY 60000 AS window_start GROUP BY window_start, key");
        expect(st.query.window.has_value(), "parser_window_present");
        expect(st.query.window->every_ms == 60000, "parser_window_ms");
        expect(st.query.window->output_column == "window_start", "parser_window_output");
      });

  expectNoThrow(
      "parser_insert_select_with_target_columns",
      []() {
        const auto st = dataflow::sql::SqlParser::parse(
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
        dataflow::sql::SqlParser::parse("SELECT 'unterminated");
      },
      "unterminated string literal");

  expectThrowsType<dataflow::SQLUnsupportedError>(
      "parser_unsupported_statement_category",
      []() {
        dataflow::sql::SqlParser::parse("UPDATE users SET score = 1");
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

  expectThrows(
      "planner_select_star_mixed_projection_rejected",
      [&]() {
        s.submit("SELECT *, region FROM t_users_v1");
      },
      "SELECT * cannot be mixed with other projections");

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

  expectThrowsType<dataflow::SQLUnsupportedError>(
      "planner_string_function_unsupported_in_aggregate",
      [&]() {
        s.submit("SELECT LOWER(region) AS region_lower, SUM(score) AS total_score FROM t_users_v1 "
                 "GROUP BY region");
      },
      "not supported in SQL v1");

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

  const auto st = dataflow::sql::SqlParser::parse(
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
      "USING csv OPTIONS(path '" +
      function_sink_path + "', delimiter ',')");

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

  const auto function_sink_table = s.read_csv(function_sink_path).toTable();
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
      "USING csv OPTIONS(path '" +
      order_sink_path + "', delimiter ',')");
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
  const auto order_sink_table = s.read_csv(order_sink_path).toTable();
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
      "USING csv OPTIONS(path '" +
      multi_sink_path + "', delimiter ',')");
  const std::string multi_hot_sink_path = "/tmp/velaria-stream-sql-multi-aggregate-hot-output.csv";
  fs::remove(multi_hot_sink_path);
  s.sql(
      "CREATE SINK TABLE stream_multi_hot_summary_v1 "
      "(key STRING, value_sum INT, event_count INT, min_value INT, max_value INT, avg_value DOUBLE) "
      "USING csv OPTIONS(path '" +
      multi_hot_sink_path + "', delimiter ',')");

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

  const auto multi_hot_sink_table = s.read_csv(multi_hot_sink_path).toTable();
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

  const auto multi_sink_table = s.read_csv(multi_sink_path).toTable();
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

  expectThrows(
      "stream_sql_regular_csv_rejected",
      [&]() {
        s.sql(
            "CREATE TABLE regular_csv_v1 (key STRING, value INT) "
            "USING csv OPTIONS(path '" + input_dir + "')");
      },
      "requires CREATE SOURCE TABLE or CREATE SINK TABLE");

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
      "USING csv OPTIONS(path '" +
      window_sink_path + "', delimiter ',')");

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

  const auto window_sink_table = s.read_csv(window_sink_path).toTable();
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
      "USING csv OPTIONS(path '" +
      window_count_sink_path + "', delimiter ',')");

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

  const auto window_count_sink_table = s.read_csv(window_count_sink_path).toTable();
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

  const std::string alias_sink_path = "/tmp/velaria-stream-window-alias-sql-regression-output.csv";
  fs::remove(alias_sink_path);
  s.sql(
      "CREATE SINK TABLE stream_window_alias_summary_v1 "
      "(bucket STRING, entity_key STRING, event_count INT) "
      "USING csv OPTIONS(path '" +
      alias_sink_path + "', delimiter ',')");

  auto alias_query = s.startStreamSql(
      "INSERT INTO stream_window_alias_summary_v1 "
      "SELECT window_start AS bucket, key AS entity_key, COUNT(*) AS event_count "
      "FROM stream_window_events_v1 "
      "WINDOW BY ts EVERY 60000 AS window_start "
      "GROUP BY window_start, key",
      window_options);
  expect(alias_query.awaitTermination() == 1, "stream_sql_window_alias_processed_batches");

  const auto alias_sink_table = s.read_csv(alias_sink_path).toTable();
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
  runStreamSqlRegression();

  if (g_failed == 0) {
    std::cout << "All regression checks passed.\n";
    return 0;
  }

  std::cout << g_failed << " regression checks failed.\n";
  return 1;
}
