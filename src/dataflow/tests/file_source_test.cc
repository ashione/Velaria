#include <fstream>
#include <unordered_map>
#include <iostream>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

#include "src/dataflow/core/contract/api/session.h"
#include "src/dataflow/core/execution/columnar_batch.h"
#include "src/dataflow/core/execution/csv.h"
#include "src/dataflow/core/execution/runtime/execution_optimizer.h"

namespace {

void expect(bool cond, const std::string& msg) {
  if (!cond) {
    throw std::runtime_error(msg);
  }
}

std::string make_temp_file(const std::string& pattern) {
  std::string path = "/tmp/" + pattern + "-XXXXXX";
  std::vector<char> buffer(path.begin(), path.end());
  buffer.push_back('\0');
  const int fd = mkstemp(buffer.data());
  if (fd == -1) {
    throw std::runtime_error("mkstemp failed");
  }
  close(fd);
  return std::string(buffer.data());
}

void write_file(const std::string& path, const std::string& body) {
  std::ofstream out(path);
  if (!out.is_open()) {
    throw std::runtime_error("cannot write file");
  }
  out << body;
}

double sum_column_as_double(const dataflow::Table& table, std::size_t column) {
  double sum = 0.0;
  for (const auto& row : table.rows) {
    if (column < row.size() && row[column].isNumber()) {
      sum += row[column].asDouble();
    }
  }
  return sum;
}

void expect_same_value_columns(const dataflow::Table& lhs, const dataflow::Table& rhs,
                               std::size_t column_index) {
  expect(lhs.columnar_cache != nullptr, "lhs columnar cache missing");
  expect(rhs.columnar_cache != nullptr, "rhs columnar cache missing");
  const auto& lhs_values = lhs.columnar_cache->columns[column_index].values;
  const auto& rhs_values = rhs.columnar_cache->columns[column_index].values;
  expect(lhs_values.size() == rhs_values.size(), "column size mismatch");
  for (std::size_t i = 0; i < lhs_values.size(); ++i) {
    expect(lhs_values[i] == rhs_values[i], "column value mismatch");
  }
}

std::unordered_map<std::string, int64_t> count_by_group(const dataflow::Table& table,
                                                        std::size_t key_column,
                                                        std::size_t value_column) {
  expect(table.columnar_cache != nullptr, "columnar cache missing");
  std::unordered_map<std::string, int64_t> out;
  const auto& keys = table.columnar_cache->columns[key_column].values;
  const auto& values = table.columnar_cache->columns[value_column].values;
  expect(keys.size() == values.size(), "group count column size mismatch");
  for (std::size_t i = 0; i < keys.size(); ++i) {
    out[keys[i].asString()] = values[i].asInt64();
  }
  return out;
}

std::unordered_map<std::string, double> double_by_group(const dataflow::Table& table,
                                                        std::size_t key_column,
                                                        std::size_t value_column) {
  expect(table.columnar_cache != nullptr, "columnar cache missing");
  std::unordered_map<std::string, double> out;
  const auto& keys = table.columnar_cache->columns[key_column].values;
  const auto& values = table.columnar_cache->columns[value_column].values;
  expect(keys.size() == values.size(), "group double column size mismatch");
  for (std::size_t i = 0; i < keys.size(); ++i) {
    out[keys[i].asString()] = values[i].asDouble();
  }
  return out;
}

std::string two_key_id(const dataflow::Value& first, const dataflow::Value& second) {
  return first.toString() + "|" + second.toString();
}

std::unordered_map<std::string, int64_t> count_by_two_groups(const dataflow::Table& table,
                                                             std::size_t first_key_column,
                                                             std::size_t second_key_column,
                                                             std::size_t value_column) {
  expect(table.columnar_cache != nullptr, "columnar cache missing");
  std::unordered_map<std::string, int64_t> out;
  const auto& first_keys = table.columnar_cache->columns[first_key_column].values;
  const auto& second_keys = table.columnar_cache->columns[second_key_column].values;
  const auto& values = table.columnar_cache->columns[value_column].values;
  expect(first_keys.size() == second_keys.size(), "first and second key size mismatch");
  expect(first_keys.size() == values.size(), "two-key count column size mismatch");
  for (std::size_t i = 0; i < first_keys.size(); ++i) {
    out[two_key_id(first_keys[i], second_keys[i])] = values[i].asInt64();
  }
  return out;
}

std::unordered_map<std::string, double> double_by_two_groups(const dataflow::Table& table,
                                                             std::size_t first_key_column,
                                                             std::size_t second_key_column,
                                                             std::size_t value_column) {
  expect(table.columnar_cache != nullptr, "columnar cache missing");
  std::unordered_map<std::string, double> out;
  const auto& first_keys = table.columnar_cache->columns[first_key_column].values;
  const auto& second_keys = table.columnar_cache->columns[second_key_column].values;
  const auto& values = table.columnar_cache->columns[value_column].values;
  expect(first_keys.size() == second_keys.size(), "first and second key size mismatch");
  expect(first_keys.size() == values.size(), "two-key double column size mismatch");
  for (std::size_t i = 0; i < first_keys.size(); ++i) {
    out[two_key_id(first_keys[i], second_keys[i])] = values[i].asDouble();
  }
  return out;
}

}  // namespace

int main() {
  try {
    auto& session = dataflow::DataflowSession::builder();

    const auto split_path = make_temp_file("velaria-line-split");
    write_file(split_path, "1001|ok|12.5\n1002|fail|9.5\n");
    dataflow::LineFileOptions split_options;
    split_options.mode = dataflow::LineParseMode::Split;
    split_options.split_delimiter = '|';
    split_options.mappings = {
        {"user_id", 0},
        {"state", 1},
        {"score", 2},
    };
    auto split_table = session.read_line_file(split_path, split_options).toTable();
    expect(split_table.schema.fields.size() == 3, "split schema width mismatch");
    expect(split_table.rowCount() == 2, "split row count mismatch");
    expect(split_table.rows[0][0].asInt64() == 1001, "split user_id mismatch");
    expect(split_table.rows[0][1].asString() == "ok", "split state mismatch");
    expect(split_table.rows[0][2].asDouble() == 12.5, "split score mismatch");

    const auto split_complex_path = make_temp_file("velaria-line-split-complex");
    write_file(split_complex_path, "1003|true|null|C:\\\\logs\\\\app\n1004|false|7|hello\\tworld\n");
    dataflow::LineFileOptions split_complex_options;
    split_complex_options.mode = dataflow::LineParseMode::Split;
    split_complex_options.split_delimiter = '|';
    split_complex_options.mappings = {
        {"user_id", 0},
        {"ok", 1},
        {"score", 2},
        {"note", 3},
    };
    auto split_complex_table = session.read_line_file(split_complex_path, split_complex_options).toTable();
    expect(split_complex_table.rowCount() == 2, "split complex row count mismatch");
    expect(split_complex_table.rows[0][1].asBool(), "split complex bool mismatch");
    expect(split_complex_table.rows[0][2].isNull(), "split complex null mismatch");
    expect(split_complex_table.rows[0][3].asString() == "C:\\\\logs\\\\app",
           "split complex escaped path mismatch");

    const auto regex_path = make_temp_file("velaria-line-regex");
    write_file(regex_path, "uid=7 action=click latency=11\nuid=8 action=view latency=13\n");
    dataflow::LineFileOptions regex_options;
    regex_options.mode = dataflow::LineParseMode::Regex;
    regex_options.regex_pattern = R"(^uid=(\d+) action=(\w+) latency=(\d+)$)";
    regex_options.mappings = {
        {"uid", 1},
        {"action", 2},
        {"latency", 3},
    };
    auto regex_table = session.read_line_file(regex_path, regex_options).toTable();
    expect(regex_table.rowCount() == 2, "regex row count mismatch");
    expect(regex_table.rows[1][0].asInt64() == 8, "regex uid mismatch");
    expect(regex_table.rows[1][1].asString() == "view", "regex action mismatch");
    auto regex_pushdown = session.read_line_file(regex_path, regex_options)
                            .filter("uid", ">", dataflow::Value(int64_t(7)))
                            .limit(1)
                            .toTable();
    expect(regex_pushdown.rowCount() == 1, "regex pushdown row count mismatch");
    expect(regex_pushdown.rows[0][1].asString() == "view", "regex pushdown row mismatch");

    const auto regex_complex_path = make_temp_file("velaria-line-regex-complex");
    write_file(regex_complex_path,
               "uid=9 action=\"open file\" latency=17 ok=true note=C:\\\\logs\\\\app\n"
               "uid=10 action=\"sync job\" latency=21 ok=false note=hello\\\\nworld\n");
    dataflow::LineFileOptions regex_complex_options;
    regex_complex_options.mode = dataflow::LineParseMode::Regex;
    regex_complex_options.regex_pattern =
        R"(^uid=(\d+) action=\"([^\"]+)\" latency=(\d+) ok=(true|false) note=(.+)$)";
    regex_complex_options.mappings = {
        {"uid", 1},
        {"action", 2},
        {"latency", 3},
        {"ok", 4},
        {"note", 5},
    };
    auto regex_complex_table = session.read_line_file(regex_complex_path, regex_complex_options).toTable();
    expect(regex_complex_table.rowCount() == 2, "regex complex row count mismatch");
    expect(regex_complex_table.rows[0][1].asString() == "open file",
           "regex complex quoted action mismatch");
    expect(regex_complex_table.rows[0][3].asBool(), "regex complex bool mismatch");
    expect(regex_complex_table.rows[1][4].asString() == "hello\\\\nworld",
           "regex complex escaped note mismatch");

    const auto jsonl_path = make_temp_file("velaria-jsonl");
    write_file(jsonl_path,
               "{\"user_id\":1,\"name\":\"alice\",\"vec\":[1,2]}\n"
               "{\"user_id\":2,\"name\":\"bob\",\"vec\":[0.5,4]}\n");
    dataflow::JsonFileOptions jsonl_options;
    jsonl_options.format = dataflow::JsonFileFormat::JsonLines;
    jsonl_options.columns = {"user_id", "name", "vec"};
    const auto json_probe = session.probe(jsonl_path);
    expect(json_probe.kind == dataflow::FileSourceKind::Json, "json probe kind mismatch");
    expect(json_probe.schema.fields.size() == 3, "json probe schema width mismatch");
    auto json_auto_table = session.read(jsonl_path).toTable();
    expect(json_auto_table.rowCount() == 2, "json auto row count mismatch");
    auto jsonl_table = session.read_json(jsonl_path, jsonl_options).toTable();
    expect(jsonl_table.rowCount() == 2, "jsonl row count mismatch");
    expect(jsonl_table.rows[0][1].asString() == "alice", "jsonl name mismatch");
    expect(jsonl_table.rows[0][2].asFixedVector().size() == 2, "jsonl vec width mismatch");

    const auto jsonl_complex_path = make_temp_file("velaria-jsonl-complex");
    write_file(jsonl_complex_path,
               "{\"user_id\":3,\"name\":\"al\\\"ice\",\"ok\":true,\"note\":\"line\\nnext\",\"score\":null}\n"
               "{\"user_id\":4,\"name\":\"tab\\tuser\",\"ok\":false,\"note\":\"C:\\\\logs\",\"score\":7}\n");
    dataflow::JsonFileOptions jsonl_complex_options;
    jsonl_complex_options.format = dataflow::JsonFileFormat::JsonLines;
    jsonl_complex_options.columns = {"user_id", "name", "ok", "note", "score"};
    auto jsonl_complex_table = session.read_json(jsonl_complex_path, jsonl_complex_options).toTable();
    expect(jsonl_complex_table.rowCount() == 2, "jsonl complex row count mismatch");
    expect(jsonl_complex_table.rows[0][1].asString() == "al\"ice",
           "jsonl complex escaped quote mismatch");
    expect(jsonl_complex_table.rows[0][2].asBool(), "jsonl complex bool mismatch");
    expect(jsonl_complex_table.rows[0][3].asString() == "line\nnext",
           "jsonl complex escaped newline mismatch");
    expect(jsonl_complex_table.rows[0][4].isNull(), "jsonl complex null mismatch");

    const auto json_array_path = make_temp_file("velaria-json-array");
    write_file(json_array_path,
               "[{\"event\":\"open\",\"cost\":1.5},{\"event\":\"close\",\"cost\":2}]\n");
    dataflow::JsonFileOptions json_array_options;
    json_array_options.format = dataflow::JsonFileFormat::JsonArray;
    json_array_options.columns = {"event", "cost"};
    auto json_array_table = session.read_json(json_array_path, json_array_options).toTable();
    expect(json_array_table.rowCount() == 2, "json array row count mismatch");
    expect(json_array_table.rows[1][0].asString() == "close", "json array event mismatch");
    expect(json_array_table.rows[1][1].asInt64() == 2, "json array cost mismatch");
    auto json_pushdown = session.read_json(json_array_path, json_array_options)
                           .filter("cost", ">", dataflow::Value(1.5))
                           .limit(1)
                           .toTable();
    expect(json_pushdown.rowCount() == 1, "json pushdown row count mismatch");
    expect(json_pushdown.rows[0][0].asString() == "close", "json pushdown row mismatch");

    const auto csv_path = make_temp_file("velaria-source-opt-csv");
    write_file(csv_path, "grp,val\nA,10\nA,20\nB,5\n");
    const auto line_agg_path = make_temp_file("velaria-source-opt-line");
    write_file(line_agg_path, "A|10\nA|20\nB|5\n");
    const auto json_agg_path = make_temp_file("velaria-source-opt-json");
    write_file(json_agg_path, "{\"grp\":\"A\",\"val\":10}\n{\"grp\":\"A\",\"val\":20}\n{\"grp\":\"B\",\"val\":5}\n");

    dataflow::LineFileOptions line_agg_options;
    line_agg_options.mode = dataflow::LineParseMode::Split;
    line_agg_options.split_delimiter = '|';
    line_agg_options.mappings = {{"grp", 0}, {"val", 1}};
    dataflow::JsonFileOptions json_agg_options;
    json_agg_options.format = dataflow::JsonFileFormat::JsonLines;
    json_agg_options.columns = {"grp", "val"};

    auto csv_agg = session.read_csv(csv_path)
                     .filter("val", ">", dataflow::Value(int64_t(0)))
                     .groupBy({"grp"})
                     .sum("val", "sum_val")
                     .toTable();
    auto line_agg = session.read_line_file(line_agg_path, line_agg_options)
                      .filter("val", ">", dataflow::Value(int64_t(0)))
                      .groupBy({"grp"})
                      .sum("val", "sum_val")
                      .toTable();
    auto json_agg = session.read_json(json_agg_path, json_agg_options)
                      .filter("val", ">", dataflow::Value(int64_t(0)))
                      .groupBy({"grp"})
                      .sum("val", "sum_val")
                      .toTable();
    expect(csv_agg.rowCount() == line_agg.rowCount(), "line aggregate pushdown row count mismatch");
    expect(csv_agg.rowCount() == json_agg.rowCount(), "json aggregate pushdown row count mismatch");
    expect(sum_column_as_double(csv_agg, 1) == sum_column_as_double(line_agg, 1),
           "line aggregate pushdown sum mismatch");
    expect(sum_column_as_double(csv_agg, 1) == sum_column_as_double(json_agg, 1),
           "json aggregate pushdown sum mismatch");

    const auto csv_quote_path = make_temp_file("velaria-source-quoted-csv");
    write_file(csv_quote_path,
               "id,name,note,code,extra\n"
               "1,\"a,b\",\"line1\r\nline2\",001,\n"
               "2,\"plain\",\"say \"\"hi\"\"\",42,\n");
    auto quoted_table = session.read_csv(csv_quote_path).toTable();
    expect(quoted_table.rowCount() == 2, "quoted csv row count mismatch");
    expect(quoted_table.rows[0][1].asString() == "a,b", "quoted csv delimiter mismatch");
    expect(quoted_table.rows[0][2].asString() == "line1\r\nline2",
           "quoted csv crlf mismatch");
    expect(quoted_table.rows[0][3].asString() == "001", "quoted csv leading zero mismatch");
    expect(quoted_table.rows[0][4].isNull(), "quoted csv trailing empty mismatch");
    expect(quoted_table.rows[1][2].asString() == "say \"hi\"",
           "quoted csv escaped quote mismatch");

    const auto projected_full = dataflow::load_csv(csv_quote_path, ',', false);
    const auto projected_only =
        dataflow::load_csv_projected(csv_quote_path, projected_full.schema, {1, 3}, ',', false);
    expect(projected_only.rowCount() == projected_full.rowCount(),
           "projected csv row count mismatch");
    expect_same_value_columns(projected_full, projected_only, 1);
    expect_same_value_columns(projected_full, projected_only, 3);
    expect(projected_only.columnar_cache->columns[0].values.empty(),
           "projected csv unexpected id materialization");

    const auto columnar_only = dataflow::load_csv(csv_quote_path, ',', false);
    const auto row_materialized = dataflow::load_csv(csv_quote_path, ',', true);
    expect(columnar_only.rowCount() == row_materialized.rowCount(),
           "materialize rows row count mismatch");
    expect_same_value_columns(columnar_only, row_materialized, 0);
    expect_same_value_columns(columnar_only, row_materialized, 1);
    expect_same_value_columns(columnar_only, row_materialized, 3);

    const auto csv_logic_path = make_temp_file("velaria-source-predicate-csv");
    write_file(csv_logic_path, "grp,val\nA,10\nA,20\nB,5\nC,30\n");
    const auto csv_logic_schema = dataflow::read_csv_schema(csv_logic_path);
    auto grp_a = std::make_shared<dataflow::PlanPredicateExpr>();
    grp_a->kind = dataflow::PlanPredicateExprKind::Comparison;
    grp_a->comparison = {0, dataflow::Value("A"), "="};
    auto grp_c = std::make_shared<dataflow::PlanPredicateExpr>();
    grp_c->kind = dataflow::PlanPredicateExprKind::Comparison;
    grp_c->comparison = {0, dataflow::Value("C"), "="};
    auto val_gt = std::make_shared<dataflow::PlanPredicateExpr>();
    val_gt->kind = dataflow::PlanPredicateExprKind::Comparison;
    val_gt->comparison = {1, dataflow::Value(int64_t(10)), ">"};
    auto grp_or = std::make_shared<dataflow::PlanPredicateExpr>();
    grp_or->kind = dataflow::PlanPredicateExprKind::Or;
    grp_or->left = grp_a;
    grp_or->right = grp_c;
    auto predicate = std::make_shared<dataflow::PlanPredicateExpr>();
    predicate->kind = dataflow::PlanPredicateExprKind::And;
    predicate->left = grp_or;
    predicate->right = val_gt;
    dataflow::SourcePushdownSpec predicate_pushdown;
    predicate_pushdown.projected_columns = {0, 1};
    predicate_pushdown.predicate_expr = predicate;
    dataflow::Table predicate_result;
    expect(dataflow::execute_csv_source_pushdown(csv_logic_path, csv_logic_schema,
                                                 predicate_pushdown, ',', true, &predicate_result),
           "csv predicate pushdown execution failed");
    expect(predicate_result.rowCount() == 2, "csv predicate pushdown row count mismatch");
    expect(predicate_result.rows[0][0].asString() == "A",
           "csv predicate pushdown first group mismatch");
    expect(predicate_result.rows[1][0].asString() == "C",
           "csv predicate pushdown second group mismatch");
    session.sql("CREATE TABLE predicate_csv_v1 USING csv OPTIONS(path: '" + csv_logic_path + "')");
    auto predicate_sql = session
                             .sql("SELECT grp, val FROM predicate_csv_v1 "
                                  "WHERE (grp = 'A' OR grp = 'C') AND val > 10 ORDER BY grp, val")
                             .toTable();
    expect(predicate_sql.rowCount() == predicate_result.rowCount(),
           "csv predicate sql row count mismatch");
    expect(predicate_sql.rows[0][0].asString() == predicate_result.rows[0][0].asString(),
           "csv predicate sql first row mismatch");
    expect(predicate_sql.rows[1][0].asString() == predicate_result.rows[1][0].asString(),
           "csv predicate sql second row mismatch");

    dataflow::SourcePushdownSpec csv_predicate_count_pushdown;
    csv_predicate_count_pushdown.predicate_expr = predicate;
    csv_predicate_count_pushdown.has_aggregate = true;
    csv_predicate_count_pushdown.aggregate.keys = {0};
    csv_predicate_count_pushdown.aggregate.aggregates = {
        {dataflow::AggregateFunction::Count, 0, "cnt"},
    };
    csv_predicate_count_pushdown.shape =
        dataflow::classifySourcePushdownShape(csv_predicate_count_pushdown);
    expect(csv_predicate_count_pushdown.shape == dataflow::SourcePushdownShape::SingleKeyCount,
           "csv predicate count should select typed source shape");
    dataflow::Table csv_predicate_count_result;
    expect(dataflow::execute_csv_source_pushdown(csv_logic_path, csv_logic_schema,
                                                 csv_predicate_count_pushdown, ',',
                                                 false, &csv_predicate_count_result),
           "csv predicate count aggregate pushdown failed");
    const auto csv_predicate_counts = count_by_group(csv_predicate_count_result, 0, 1);
    expect(csv_predicate_counts.size() == 2,
           "csv predicate count aggregate group count mismatch");
    expect(csv_predicate_counts.at("A") == 1, "csv predicate count aggregate A mismatch");
    expect(csv_predicate_counts.at("C") == 1, "csv predicate count aggregate C mismatch");

    dataflow::SourcePushdownSpec csv_predicate_sum_pushdown;
    csv_predicate_sum_pushdown.predicate_expr = predicate;
    csv_predicate_sum_pushdown.has_aggregate = true;
    csv_predicate_sum_pushdown.aggregate.keys = {0};
    csv_predicate_sum_pushdown.aggregate.aggregates = {
        {dataflow::AggregateFunction::Sum, 1, "sum_val"},
    };
    csv_predicate_sum_pushdown.shape =
        dataflow::classifySourcePushdownShape(csv_predicate_sum_pushdown);
    expect(csv_predicate_sum_pushdown.shape ==
               dataflow::SourcePushdownShape::SingleKeyNumericAggregate,
           "csv predicate sum should select typed source shape");
    dataflow::Table csv_predicate_sum_result;
    expect(dataflow::execute_csv_source_pushdown(csv_logic_path, csv_logic_schema,
                                                 csv_predicate_sum_pushdown, ',',
                                                 false, &csv_predicate_sum_result),
           "csv predicate sum aggregate pushdown failed");
    const auto csv_predicate_sums = double_by_group(csv_predicate_sum_result, 0, 1);
    expect(csv_predicate_sums.size() == 2,
           "csv predicate sum aggregate group count mismatch");
    expect(csv_predicate_sums.at("A") == 20.0, "csv predicate sum aggregate A mismatch");
    expect(csv_predicate_sums.at("C") == 30.0, "csv predicate sum aggregate C mismatch");

    const auto csv_predicate_avg_path = make_temp_file("velaria-source-predicate-csv-avg");
    write_file(csv_predicate_avg_path,
               "grp,val,flag\nA,text,1\nB,5,0\nC,bad,1\n");
    const auto csv_predicate_avg_schema = dataflow::read_csv_schema(csv_predicate_avg_path);
    auto avg_grp_a = std::make_shared<dataflow::PlanPredicateExpr>();
    avg_grp_a->kind = dataflow::PlanPredicateExprKind::Comparison;
    avg_grp_a->comparison = {0, dataflow::Value("A"), "="};
    auto avg_grp_c = std::make_shared<dataflow::PlanPredicateExpr>();
    avg_grp_c->kind = dataflow::PlanPredicateExprKind::Comparison;
    avg_grp_c->comparison = {0, dataflow::Value("C"), "="};
    auto avg_grp_or = std::make_shared<dataflow::PlanPredicateExpr>();
    avg_grp_or->kind = dataflow::PlanPredicateExprKind::Or;
    avg_grp_or->left = avg_grp_a;
    avg_grp_or->right = avg_grp_c;
    auto avg_flag = std::make_shared<dataflow::PlanPredicateExpr>();
    avg_flag->kind = dataflow::PlanPredicateExprKind::Comparison;
    avg_flag->comparison = {2, dataflow::Value(int64_t(1)), "="};
    auto avg_predicate = std::make_shared<dataflow::PlanPredicateExpr>();
    avg_predicate->kind = dataflow::PlanPredicateExprKind::And;
    avg_predicate->left = avg_grp_or;
    avg_predicate->right = avg_flag;

    dataflow::SourcePushdownSpec csv_predicate_avg_pushdown;
    csv_predicate_avg_pushdown.predicate_expr = avg_predicate;
    csv_predicate_avg_pushdown.has_aggregate = true;
    csv_predicate_avg_pushdown.aggregate.keys = {0};
    csv_predicate_avg_pushdown.aggregate.aggregates = {
        {dataflow::AggregateFunction::Avg, 1, "avg_val"},
    };
    csv_predicate_avg_pushdown.shape =
        dataflow::classifySourcePushdownShape(csv_predicate_avg_pushdown);
    expect(csv_predicate_avg_pushdown.shape ==
               dataflow::SourcePushdownShape::SingleKeyNumericAggregate,
           "csv predicate avg should select typed source shape");
    dataflow::Table csv_predicate_avg_typed_result;
    expect(dataflow::execute_csv_source_pushdown(csv_predicate_avg_path,
                                                 csv_predicate_avg_schema,
                                                 csv_predicate_avg_pushdown, ',',
                                                 false,
                                                 &csv_predicate_avg_typed_result),
           "csv predicate typed avg aggregate pushdown failed");

    dataflow::SourcePushdownSpec csv_predicate_avg_generic_pushdown =
        csv_predicate_avg_pushdown;
    csv_predicate_avg_generic_pushdown.shape = dataflow::SourcePushdownShape::Generic;
    dataflow::Table csv_predicate_avg_generic_result;
    expect(dataflow::execute_csv_source_pushdown(csv_predicate_avg_path,
                                                 csv_predicate_avg_schema,
                                                 csv_predicate_avg_generic_pushdown, ',',
                                                 false,
                                                 &csv_predicate_avg_generic_result),
           "csv predicate generic avg aggregate pushdown failed");
    const auto csv_predicate_avg_typed =
        double_by_group(csv_predicate_avg_typed_result, 0, 1);
    const auto csv_predicate_avg_generic =
        double_by_group(csv_predicate_avg_generic_result, 0, 1);
    expect(csv_predicate_avg_typed == csv_predicate_avg_generic,
           "csv predicate typed avg should match generic empty-numeric semantics");
    expect(csv_predicate_avg_typed.at("A") == 0.0,
           "csv predicate avg empty numeric A mismatch");
    expect(csv_predicate_avg_typed.at("C") == 0.0,
           "csv predicate avg empty numeric C mismatch");

    const auto csv_multi_key_path = make_temp_file("velaria-source-multi-key-csv");
    write_file(csv_multi_key_path,
               "region,svc,val,flag\n"
               "us,api,10,1\n"
               "us,api,20,1\n"
               "us,batch,5,0\n"
               "eu,api,7,1\n"
               "eu,batch,8,1\n");
    const auto csv_multi_key_schema = dataflow::read_csv_schema(csv_multi_key_path);
    auto multi_flag = std::make_shared<dataflow::PlanPredicateExpr>();
    multi_flag->kind = dataflow::PlanPredicateExprKind::Comparison;
    multi_flag->comparison = {3, dataflow::Value(int64_t(1)), "="};
    dataflow::SourcePushdownSpec csv_multi_count_pushdown;
    csv_multi_count_pushdown.predicate_expr = multi_flag;
    csv_multi_count_pushdown.has_aggregate = true;
    csv_multi_count_pushdown.aggregate.keys = {0, 1};
    csv_multi_count_pushdown.aggregate.aggregates = {
        {dataflow::AggregateFunction::Count, 0, "cnt"},
    };
    csv_multi_count_pushdown.shape =
        dataflow::classifySourcePushdownShape(csv_multi_count_pushdown);
    expect(csv_multi_count_pushdown.shape == dataflow::SourcePushdownShape::MultiKeyCount,
           "csv multi-key count should select typed source shape");
    dataflow::Table csv_multi_count_selected_result;
    expect(dataflow::execute_csv_source_pushdown(csv_multi_key_path, csv_multi_key_schema,
                                                 csv_multi_count_pushdown, ',',
                                                 false, &csv_multi_count_selected_result),
           "csv multi-key selected count aggregate pushdown failed");
    dataflow::SourcePushdownSpec csv_multi_count_generic_pushdown = csv_multi_count_pushdown;
    csv_multi_count_generic_pushdown.shape = dataflow::SourcePushdownShape::Generic;
    dataflow::Table csv_multi_count_generic_result;
    expect(dataflow::execute_csv_source_pushdown(csv_multi_key_path, csv_multi_key_schema,
                                                 csv_multi_count_generic_pushdown, ',',
                                                 false, &csv_multi_count_generic_result),
           "csv multi-key generic count aggregate pushdown failed");
    const auto csv_multi_counts =
        count_by_two_groups(csv_multi_count_selected_result, 0, 1, 2);
    expect(csv_multi_counts == count_by_two_groups(csv_multi_count_generic_result, 0, 1, 2),
           "csv multi-key selected count should match generic result");
    expect(csv_multi_counts.at("us|api") == 2, "csv multi-key count us api mismatch");
    expect(csv_multi_counts.at("eu|api") == 1, "csv multi-key count eu api mismatch");
    expect(csv_multi_counts.at("eu|batch") == 1, "csv multi-key count eu batch mismatch");

    dataflow::SourcePushdownSpec csv_multi_sum_pushdown;
    csv_multi_sum_pushdown.predicate_expr = multi_flag;
    csv_multi_sum_pushdown.has_aggregate = true;
    csv_multi_sum_pushdown.aggregate.keys = {0, 1};
    csv_multi_sum_pushdown.aggregate.aggregates = {
        {dataflow::AggregateFunction::Sum, 2, "sum_val"},
    };
    csv_multi_sum_pushdown.shape =
        dataflow::classifySourcePushdownShape(csv_multi_sum_pushdown);
    expect(csv_multi_sum_pushdown.shape ==
               dataflow::SourcePushdownShape::MultiKeyNumericAggregate,
           "csv multi-key sum should select typed source shape");
    dataflow::Table csv_multi_sum_selected_result;
    expect(dataflow::execute_csv_source_pushdown(csv_multi_key_path, csv_multi_key_schema,
                                                 csv_multi_sum_pushdown, ',',
                                                 false, &csv_multi_sum_selected_result),
           "csv multi-key selected sum aggregate pushdown failed");
    dataflow::SourcePushdownSpec csv_multi_sum_generic_pushdown = csv_multi_sum_pushdown;
    csv_multi_sum_generic_pushdown.shape = dataflow::SourcePushdownShape::Generic;
    dataflow::Table csv_multi_sum_generic_result;
    expect(dataflow::execute_csv_source_pushdown(csv_multi_key_path, csv_multi_key_schema,
                                                 csv_multi_sum_generic_pushdown, ',',
                                                 false, &csv_multi_sum_generic_result),
           "csv multi-key generic sum aggregate pushdown failed");
    const auto csv_multi_sums =
        double_by_two_groups(csv_multi_sum_selected_result, 0, 1, 2);
    expect(csv_multi_sums == double_by_two_groups(csv_multi_sum_generic_result, 0, 1, 2),
           "csv multi-key selected sum should match generic result");
    expect(csv_multi_sums.at("us|api") == 30.0, "csv multi-key sum us api mismatch");
    expect(csv_multi_sums.at("eu|api") == 7.0, "csv multi-key sum eu api mismatch");
    expect(csv_multi_sums.at("eu|batch") == 8.0, "csv multi-key sum eu batch mismatch");

    dataflow::SourcePushdownSpec csv_multi_avg_pushdown;
    csv_multi_avg_pushdown.predicate_expr = multi_flag;
    csv_multi_avg_pushdown.has_aggregate = true;
    csv_multi_avg_pushdown.aggregate.keys = {0, 1};
    csv_multi_avg_pushdown.aggregate.aggregates = {
        {dataflow::AggregateFunction::Avg, 2, "avg_val"},
    };
    csv_multi_avg_pushdown.shape =
        dataflow::classifySourcePushdownShape(csv_multi_avg_pushdown);
    expect(csv_multi_avg_pushdown.shape ==
               dataflow::SourcePushdownShape::MultiKeyNumericAggregate,
           "csv multi-key avg should select typed source shape");
    dataflow::Table csv_multi_avg_selected_result;
    expect(dataflow::execute_csv_source_pushdown(csv_multi_key_path, csv_multi_key_schema,
                                                 csv_multi_avg_pushdown, ',',
                                                 false, &csv_multi_avg_selected_result),
           "csv multi-key selected avg aggregate pushdown failed");
    dataflow::SourcePushdownSpec csv_multi_avg_generic_pushdown = csv_multi_avg_pushdown;
    csv_multi_avg_generic_pushdown.shape = dataflow::SourcePushdownShape::Generic;
    dataflow::Table csv_multi_avg_generic_result;
    expect(dataflow::execute_csv_source_pushdown(csv_multi_key_path, csv_multi_key_schema,
                                                 csv_multi_avg_generic_pushdown, ',',
                                                 false, &csv_multi_avg_generic_result),
           "csv multi-key generic avg aggregate pushdown failed");
    const auto csv_multi_avgs =
        double_by_two_groups(csv_multi_avg_selected_result, 0, 1, 2);
    expect(csv_multi_avgs == double_by_two_groups(csv_multi_avg_generic_result, 0, 1, 2),
           "csv multi-key selected avg should match generic result");
    expect(csv_multi_avgs.at("us|api") == 15.0, "csv multi-key avg us api mismatch");
    expect(csv_multi_avgs.at("eu|api") == 7.0, "csv multi-key avg eu api mismatch");
    expect(csv_multi_avgs.at("eu|batch") == 8.0, "csv multi-key avg eu batch mismatch");

    const auto line_multi_key_path = make_temp_file("velaria-source-multi-key-line");
    write_file(line_multi_key_path,
               "us|api|10|1\n"
               "us|api|20|1\n"
               "us|batch|5|0\n"
               "eu|api|7|1\n"
               "eu|batch|8|1\n");
    dataflow::LineFileOptions line_multi_key_options;
    line_multi_key_options.mode = dataflow::LineParseMode::Split;
    line_multi_key_options.split_delimiter = '|';
    line_multi_key_options.mappings = {
        {"region", 0},
        {"svc", 1},
        {"val", 2},
        {"flag", 3},
    };
    dataflow::FileSourceConnectorSpec line_multi_key_spec;
    line_multi_key_spec.kind = dataflow::FileSourceKind::Line;
    line_multi_key_spec.path = line_multi_key_path;
    line_multi_key_spec.line_options = line_multi_key_options;
    const auto line_multi_key_schema =
        dataflow::infer_line_file_schema(line_multi_key_options);
    dataflow::SourcePushdownSpec line_multi_sum_pushdown = csv_multi_sum_pushdown;
    line_multi_sum_pushdown.shape =
        dataflow::classifySourcePushdownShape(line_multi_sum_pushdown);
    dataflow::Table line_multi_sum_selected_result;
    expect(dataflow::execute_file_source_pushdown(line_multi_key_spec,
                                                  line_multi_key_schema,
                                                  line_multi_sum_pushdown, false,
                                                  &line_multi_sum_selected_result),
           "line multi-key selected sum aggregate pushdown failed");
    dataflow::SourcePushdownSpec line_multi_sum_generic_pushdown = line_multi_sum_pushdown;
    line_multi_sum_generic_pushdown.shape = dataflow::SourcePushdownShape::Generic;
    dataflow::Table line_multi_sum_generic_result;
    expect(dataflow::execute_file_source_pushdown(line_multi_key_spec,
                                                  line_multi_key_schema,
                                                  line_multi_sum_generic_pushdown, false,
                                                  &line_multi_sum_generic_result),
           "line multi-key generic sum aggregate pushdown failed");
    expect(double_by_two_groups(line_multi_sum_selected_result, 0, 1, 2) ==
               double_by_two_groups(line_multi_sum_generic_result, 0, 1, 2),
           "line multi-key selected sum should match generic result");

    const auto json_multi_key_path = make_temp_file("velaria-source-multi-key-json");
    write_file(json_multi_key_path,
               "{\"region\":\"us\",\"svc\":\"api\",\"val\":10,\"flag\":1}\n"
               "{\"region\":\"us\",\"svc\":\"api\",\"val\":20,\"flag\":1}\n"
               "{\"region\":\"us\",\"svc\":\"batch\",\"val\":5,\"flag\":0}\n"
               "{\"region\":\"eu\",\"svc\":\"api\",\"val\":7,\"flag\":1}\n"
               "{\"region\":\"eu\",\"svc\":\"batch\",\"val\":8,\"flag\":1}\n");
    dataflow::JsonFileOptions json_multi_key_options;
    json_multi_key_options.format = dataflow::JsonFileFormat::JsonLines;
    json_multi_key_options.columns = {"region", "svc", "val", "flag"};
    dataflow::FileSourceConnectorSpec json_multi_key_spec;
    json_multi_key_spec.kind = dataflow::FileSourceKind::Json;
    json_multi_key_spec.path = json_multi_key_path;
    json_multi_key_spec.json_options = json_multi_key_options;
    const auto json_multi_key_schema =
        dataflow::infer_json_file_schema(json_multi_key_options);
    dataflow::SourcePushdownSpec json_multi_count_pushdown = csv_multi_count_pushdown;
    json_multi_count_pushdown.shape =
        dataflow::classifySourcePushdownShape(json_multi_count_pushdown);
    dataflow::Table json_multi_count_typed_result;
    expect(dataflow::execute_file_source_pushdown(json_multi_key_spec,
                                                  json_multi_key_schema,
                                                  json_multi_count_pushdown, false,
                                                  &json_multi_count_typed_result),
           "json multi-key typed count aggregate pushdown failed");
    dataflow::SourcePushdownSpec json_multi_count_generic_pushdown = json_multi_count_pushdown;
    json_multi_count_generic_pushdown.shape = dataflow::SourcePushdownShape::Generic;
    dataflow::Table json_multi_count_generic_result;
    expect(dataflow::execute_file_source_pushdown(json_multi_key_spec,
                                                  json_multi_key_schema,
                                                  json_multi_count_generic_pushdown, false,
                                                  &json_multi_count_generic_result),
           "json multi-key generic count aggregate pushdown failed");
    expect(count_by_two_groups(json_multi_count_typed_result, 0, 1, 2) ==
               count_by_two_groups(json_multi_count_generic_result, 0, 1, 2),
           "json multi-key typed count should match generic result");

    const auto json_numeric_key_path =
        make_temp_file("velaria-source-multi-key-json-numeric");
    write_file(json_numeric_key_path,
               "{\"region\":\"us\",\"bucket\":1,\"flag\":1}\n"
               "{\"region\":\"us\",\"bucket\":1.0,\"flag\":1}\n"
               "{\"region\":\"eu\",\"bucket\":2,\"flag\":1}\n");
    dataflow::JsonFileOptions json_numeric_key_options;
    json_numeric_key_options.format = dataflow::JsonFileFormat::JsonLines;
    json_numeric_key_options.columns = {"region", "bucket", "flag"};
    dataflow::FileSourceConnectorSpec json_numeric_key_spec;
    json_numeric_key_spec.kind = dataflow::FileSourceKind::Json;
    json_numeric_key_spec.path = json_numeric_key_path;
    json_numeric_key_spec.json_options = json_numeric_key_options;
    const auto json_numeric_key_schema =
        dataflow::infer_json_file_schema(json_numeric_key_options);
    auto numeric_flag = std::make_shared<dataflow::PlanPredicateExpr>();
    numeric_flag->kind = dataflow::PlanPredicateExprKind::Comparison;
    numeric_flag->comparison = {2, dataflow::Value(int64_t(1)), "="};
    dataflow::SourcePushdownSpec json_numeric_count_pushdown;
    json_numeric_count_pushdown.predicate_expr = numeric_flag;
    json_numeric_count_pushdown.has_aggregate = true;
    json_numeric_count_pushdown.aggregate.keys = {0, 1};
    json_numeric_count_pushdown.aggregate.aggregates = {
        {dataflow::AggregateFunction::Count, 0, "cnt"},
    };
    json_numeric_count_pushdown.shape =
        dataflow::classifySourcePushdownShape(json_numeric_count_pushdown);
    dataflow::Table json_numeric_count_typed_result;
    expect(dataflow::execute_file_source_pushdown(json_numeric_key_spec,
                                                  json_numeric_key_schema,
                                                  json_numeric_count_pushdown, false,
                                                  &json_numeric_count_typed_result),
           "json numeric multi-key typed count aggregate pushdown failed");
    dataflow::SourcePushdownSpec json_numeric_count_generic_pushdown =
        json_numeric_count_pushdown;
    json_numeric_count_generic_pushdown.shape = dataflow::SourcePushdownShape::Generic;
    dataflow::Table json_numeric_count_generic_result;
    expect(dataflow::execute_file_source_pushdown(json_numeric_key_spec,
                                                  json_numeric_key_schema,
                                                  json_numeric_count_generic_pushdown, false,
                                                  &json_numeric_count_generic_result),
           "json numeric multi-key generic count aggregate pushdown failed");
    const auto json_numeric_counts =
        count_by_two_groups(json_numeric_count_typed_result, 0, 1, 2);
    expect(json_numeric_counts == count_by_two_groups(json_numeric_count_generic_result, 0, 1, 2),
           "json numeric multi-key typed count should match generic result");
    expect(json_numeric_counts.size() == 2,
           "json numeric multi-key should coalesce int and double numeric keys");
    expect(json_numeric_counts.at("us|1") == 2,
           "json numeric multi-key us bucket count mismatch");

    const auto json_array_multi_key_path =
        make_temp_file("velaria-source-multi-key-json-array");
    write_file(json_array_multi_key_path,
               "["
               "{\"region\":\"us\",\"svc\":\"api\",\"flag\":1},"
               "{\"region\":\"us\",\"svc\":\"api\",\"flag\":1},"
               "{\"region\":\"eu\",\"svc\":\"batch\",\"flag\":1}"
               "]");
    dataflow::JsonFileOptions json_array_multi_key_options = json_multi_key_options;
    json_array_multi_key_options.format = dataflow::JsonFileFormat::JsonArray;
    dataflow::FileSourceConnectorSpec json_array_multi_key_spec;
    json_array_multi_key_spec.kind = dataflow::FileSourceKind::Json;
    json_array_multi_key_spec.path = json_array_multi_key_path;
    json_array_multi_key_spec.json_options = json_array_multi_key_options;
    dataflow::Table json_array_multi_count_typed_result;
    expect(dataflow::execute_file_source_pushdown(json_array_multi_key_spec,
                                                  json_multi_key_schema,
                                                  json_multi_count_pushdown, false,
                                                  &json_array_multi_count_typed_result),
           "json array multi-key typed count aggregate pushdown failed");
    dataflow::SourcePushdownSpec json_array_multi_count_generic_pushdown =
        json_multi_count_pushdown;
    json_array_multi_count_generic_pushdown.shape = dataflow::SourcePushdownShape::Generic;
    dataflow::Table json_array_multi_count_generic_result;
    expect(dataflow::execute_file_source_pushdown(json_array_multi_key_spec,
                                                  json_multi_key_schema,
                                                  json_array_multi_count_generic_pushdown,
                                                  false,
                                                  &json_array_multi_count_generic_result),
           "json array multi-key generic count aggregate pushdown failed");
    expect(count_by_two_groups(json_array_multi_count_typed_result, 0, 1, 2) ==
               count_by_two_groups(json_array_multi_count_generic_result, 0, 1, 2),
           "json array multi-key typed count should match generic result");

    const auto csv_sparse_a_path = make_temp_file("velaria-source-sparse-filter-a");
    const auto csv_sparse_b_path = make_temp_file("velaria-source-sparse-filter-b");
    {
      std::ofstream sparse_a(csv_sparse_a_path);
      std::ofstream sparse_b(csv_sparse_b_path);
      if (!sparse_a.is_open() || !sparse_b.is_open()) {
        throw std::runtime_error("cannot write sparse filter csv");
      }
      sparse_a << "grp,code\n";
      sparse_b << "grp,code\n";
      for (int i = 0; i < 100; ++i) sparse_a << "A,1\n";
      for (int i = 0; i < 100; ++i) sparse_a << "B,1\n";
      for (int i = 0; i < 1948; ++i) sparse_a << "A,01\n";
      for (int i = 0; i < 1948; ++i) sparse_a << "B,01\n";
      for (int i = 0; i < 200; ++i) sparse_a << "A,1\n";
      for (int i = 0; i < 200; ++i) sparse_a << "B,1\n";
      for (int i = 0; i < 252; ++i) sparse_a << "A,01\n";
      for (int i = 0; i < 252; ++i) sparse_a << "B,01\n";

      for (int i = 0; i < 300; ++i) sparse_b << "A,1\n";
      for (int i = 0; i < 300; ++i) sparse_b << "B,1\n";
      for (int i = 0; i < 1748; ++i) sparse_b << "A,01\n";
      for (int i = 0; i < 1748; ++i) sparse_b << "B,01\n";
      for (int i = 0; i < 452; ++i) sparse_b << "A,01\n";
      for (int i = 0; i < 452; ++i) sparse_b << "B,01\n";
    }
    const auto sparse_schema = dataflow::read_csv_schema(csv_sparse_a_path);
    dataflow::SourcePushdownSpec sparse_pushdown;
    sparse_pushdown.filters.push_back({1, dataflow::Value(int64_t(1)), "="});
    sparse_pushdown.has_aggregate = true;
    sparse_pushdown.aggregate.keys = {0};
    sparse_pushdown.aggregate.aggregates = {
        {dataflow::AggregateFunction::Count, 0, "cnt"},
    };
    dataflow::Table sparse_result_a;
    dataflow::Table sparse_result_b;
    expect(dataflow::execute_csv_source_pushdown(
               csv_sparse_a_path, sparse_schema, sparse_pushdown, ',', false, &sparse_result_a),
           "csv sparse aggregate pushdown a failed");
    expect(dataflow::execute_csv_source_pushdown(
               csv_sparse_b_path, sparse_schema, sparse_pushdown, ',', false, &sparse_result_b),
           "csv sparse aggregate pushdown b failed");
    const auto counts_a = count_by_group(sparse_result_a, 0, 1);
    const auto counts_b = count_by_group(sparse_result_b, 0, 1);
    expect(counts_a == counts_b, "csv sparse filter ordering changed aggregate result");
    expect(counts_a.at("A") == 300, "csv sparse filter A count mismatch");
    expect(counts_a.at("B") == 300, "csv sparse filter B count mismatch");

    const auto csv_large_numeric_path = make_temp_file("velaria-source-large-numeric-aggregate");
    {
      std::ofstream large(csv_large_numeric_path);
      if (!large.is_open()) {
        throw std::runtime_error("cannot write large numeric aggregate csv");
      }
      large << "id,grp,val\n";
      for (int i = 0; i < 50000; ++i) {
        large << i << ",g" << (i % 16) << "," << ((i * 37) % 1000) << "\n";
      }
    }
    const auto large_schema = dataflow::read_csv_schema(csv_large_numeric_path);
    dataflow::SourcePushdownSpec large_pushdown;
    large_pushdown.filters.push_back({2, dataflow::Value(int64_t(500)), ">"});
    large_pushdown.has_aggregate = true;
    large_pushdown.aggregate.keys = {1};
    large_pushdown.aggregate.aggregates = {
        {dataflow::AggregateFunction::Sum, 2, "sum_val"},
    };
    dataflow::Table large_result;
    expect(dataflow::execute_csv_source_pushdown(
               csv_large_numeric_path, large_schema, large_pushdown, ',', false, &large_result),
           "csv large numeric aggregate pushdown failed");
    expect(large_result.rowCount() == 16, "csv large numeric aggregate key count mismatch");
    for (const auto& key : large_result.columnar_cache->columns[0].values) {
      expect(key.type() == dataflow::DataType::String,
             "csv large numeric aggregate key type mismatch");
    }

    const auto line_logic_path = make_temp_file("velaria-source-predicate-line");
    write_file(line_logic_path, "1|A|10\n2|A|20\n3|B|5\n4|C|30\n");
    dataflow::LineFileOptions line_logic_options;
    line_logic_options.mode = dataflow::LineParseMode::Split;
    line_logic_options.split_delimiter = '|';
    line_logic_options.mappings = {{"id", 0}, {"grp", 1}, {"val", 2}};
    auto line_grp_a = std::make_shared<dataflow::PlanPredicateExpr>();
    line_grp_a->kind = dataflow::PlanPredicateExprKind::Comparison;
    line_grp_a->comparison = {1, dataflow::Value("A"), "="};
    auto line_grp_c = std::make_shared<dataflow::PlanPredicateExpr>();
    line_grp_c->kind = dataflow::PlanPredicateExprKind::Comparison;
    line_grp_c->comparison = {1, dataflow::Value("C"), "="};
    auto line_val_gt = std::make_shared<dataflow::PlanPredicateExpr>();
    line_val_gt->kind = dataflow::PlanPredicateExprKind::Comparison;
    line_val_gt->comparison = {2, dataflow::Value(int64_t(10)), ">"};
    auto line_grp_or = std::make_shared<dataflow::PlanPredicateExpr>();
    line_grp_or->kind = dataflow::PlanPredicateExprKind::Or;
    line_grp_or->left = line_grp_a;
    line_grp_or->right = line_grp_c;
    auto line_predicate = std::make_shared<dataflow::PlanPredicateExpr>();
    line_predicate->kind = dataflow::PlanPredicateExprKind::And;
    line_predicate->left = line_grp_or;
    line_predicate->right = line_val_gt;
    dataflow::SourcePushdownSpec line_pushdown;
    line_pushdown.projected_columns = {1, 2};
    line_pushdown.predicate_expr = line_predicate;
    dataflow::FileSourceConnectorSpec line_spec;
    line_spec.kind = dataflow::FileSourceKind::Line;
    line_spec.path = line_logic_path;
    line_spec.line_options = line_logic_options;
    dataflow::Table line_pushdown_result;
    expect(dataflow::execute_file_source_pushdown(
               line_spec, dataflow::infer_line_file_schema(line_logic_options),
               line_pushdown, false, &line_pushdown_result),
           "line predicate pushdown execution failed");
    auto line_reference = session.read_line_file(line_logic_path, line_logic_options)
                              .filterPredicate(line_predicate)
                              .select({"grp", "val"})
                              .toTable();
    expect(line_pushdown_result.rowCount() == line_reference.rowCount(),
           "line predicate pushdown row count mismatch");
    expect(line_pushdown_result.columnar_cache->columns[1].values.size() ==
               line_reference.columnar_cache->columns[0].values.size(),
           "line predicate pushdown grp size mismatch");
    expect(line_pushdown_result.columnar_cache->columns[2].values.size() ==
               line_reference.columnar_cache->columns[1].values.size(),
           "line predicate pushdown val size mismatch");
    for (std::size_t i = 0; i < line_reference.rowCount(); ++i) {
      expect(line_pushdown_result.columnar_cache->columns[1].values[i] ==
                 line_reference.columnar_cache->columns[0].values[i],
             "line predicate pushdown grp mismatch");
      expect(line_pushdown_result.columnar_cache->columns[2].values[i] ==
                 line_reference.columnar_cache->columns[1].values[i],
             "line predicate pushdown val mismatch");
    }

    dataflow::SourcePushdownSpec line_count_pushdown;
    line_count_pushdown.predicate_expr = line_predicate;
    line_count_pushdown.has_aggregate = true;
    line_count_pushdown.aggregate.keys = {1};
    line_count_pushdown.aggregate.aggregates = {
        {dataflow::AggregateFunction::Count, 1, "cnt"},
    };
    dataflow::Table line_count_result;
    expect(dataflow::execute_file_source_pushdown(
               line_spec, dataflow::infer_line_file_schema(line_logic_options),
               line_count_pushdown, false, &line_count_result),
           "line predicate count aggregate pushdown failed");
    const auto line_counts = count_by_group(line_count_result, 0, 1);
    expect(line_counts.size() == 2, "line predicate count aggregate group count mismatch");
    expect(line_counts.at("A") == 1, "line predicate count aggregate A mismatch");
    expect(line_counts.at("C") == 1, "line predicate count aggregate C mismatch");

    dataflow::SourcePushdownSpec line_sum_pushdown;
    line_sum_pushdown.predicate_expr = line_predicate;
    line_sum_pushdown.has_aggregate = true;
    line_sum_pushdown.aggregate.keys = {1};
    line_sum_pushdown.aggregate.aggregates = {
        {dataflow::AggregateFunction::Sum, 2, "sum_val"},
    };
    dataflow::Table line_sum_result;
    expect(dataflow::execute_file_source_pushdown(
               line_spec, dataflow::infer_line_file_schema(line_logic_options),
               line_sum_pushdown, false, &line_sum_result),
           "line predicate sum aggregate pushdown failed");
    const auto line_sums = double_by_group(line_sum_result, 0, 1);
    expect(line_sums.size() == 2, "line predicate sum aggregate group count mismatch");
    expect(line_sums.at("A") == 20.0, "line predicate sum aggregate A mismatch");
    expect(line_sums.at("C") == 30.0, "line predicate sum aggregate C mismatch");

    const auto json_logic_path = make_temp_file("velaria-source-predicate-json");
    write_file(json_logic_path,
               "{\"id\":1,\"grp\":\"A\",\"val\":10}\n"
               "{\"id\":2,\"grp\":\"A\",\"val\":20}\n"
               "{\"id\":3,\"grp\":\"B\",\"val\":5}\n"
               "{\"id\":4,\"grp\":\"C\",\"val\":30}\n");
    dataflow::JsonFileOptions json_logic_options;
    json_logic_options.format = dataflow::JsonFileFormat::JsonLines;
    json_logic_options.columns = {"id", "grp", "val"};
    dataflow::SourcePushdownSpec json_predicate_pushdown;
    json_predicate_pushdown.projected_columns = {1, 2};
    json_predicate_pushdown.predicate_expr = line_predicate;
    dataflow::FileSourceConnectorSpec json_spec;
    json_spec.kind = dataflow::FileSourceKind::Json;
    json_spec.path = json_logic_path;
    json_spec.json_options = json_logic_options;
    dataflow::Table json_pushdown_result;
    expect(dataflow::execute_file_source_pushdown(
               json_spec, dataflow::infer_json_file_schema(json_logic_options),
               json_predicate_pushdown, false, &json_pushdown_result),
           "json predicate pushdown execution failed");
    auto json_reference = session.read_json(json_logic_path, json_logic_options)
                              .filterPredicate(line_predicate)
                              .select({"grp", "val"})
                              .toTable();
    expect(json_pushdown_result.rowCount() == json_reference.rowCount(),
           "json predicate pushdown row count mismatch");
    expect(json_pushdown_result.columnar_cache->columns[1].values.size() ==
               json_reference.columnar_cache->columns[0].values.size(),
           "json predicate pushdown grp size mismatch");
    expect(json_pushdown_result.columnar_cache->columns[2].values.size() ==
               json_reference.columnar_cache->columns[1].values.size(),
           "json predicate pushdown val size mismatch");
    for (std::size_t i = 0; i < json_reference.rowCount(); ++i) {
      expect(json_pushdown_result.columnar_cache->columns[1].values[i] ==
                 json_reference.columnar_cache->columns[0].values[i],
             "json predicate pushdown grp mismatch");
      expect(json_pushdown_result.columnar_cache->columns[2].values[i] ==
                 json_reference.columnar_cache->columns[1].values[i],
             "json predicate pushdown val mismatch");
    }

    dataflow::SourcePushdownSpec json_count_pushdown;
    json_count_pushdown.predicate_expr = line_predicate;
    json_count_pushdown.has_aggregate = true;
    json_count_pushdown.aggregate.keys = {1};
    json_count_pushdown.aggregate.aggregates = {
        {dataflow::AggregateFunction::Count, 1, "cnt"},
    };
    dataflow::Table json_count_result;
    expect(dataflow::execute_file_source_pushdown(
               json_spec, dataflow::infer_json_file_schema(json_logic_options),
               json_count_pushdown, false, &json_count_result),
           "json predicate count aggregate pushdown failed");
    const auto json_counts = count_by_group(json_count_result, 0, 1);
    expect(json_counts.size() == 2, "json predicate count aggregate group count mismatch");
    expect(json_counts.at("A") == 1, "json predicate count aggregate A mismatch");
    expect(json_counts.at("C") == 1, "json predicate count aggregate C mismatch");

    dataflow::SourcePushdownSpec json_sum_pushdown;
    json_sum_pushdown.predicate_expr = line_predicate;
    json_sum_pushdown.has_aggregate = true;
    json_sum_pushdown.aggregate.keys = {1};
    json_sum_pushdown.aggregate.aggregates = {
        {dataflow::AggregateFunction::Sum, 2, "sum_val"},
    };
    dataflow::Table json_sum_result;
    expect(dataflow::execute_file_source_pushdown(
               json_spec, dataflow::infer_json_file_schema(json_logic_options),
               json_sum_pushdown, false, &json_sum_result),
           "json predicate sum aggregate pushdown failed");
    const auto json_sums = double_by_group(json_sum_result, 0, 1);
    expect(json_sums.size() == 2, "json predicate sum aggregate group count mismatch");
    expect(json_sums.at("A") == 20.0, "json predicate sum aggregate A mismatch");
    expect(json_sums.at("C") == 30.0, "json predicate sum aggregate C mismatch");

    std::cout << "[test] file source split/regex/json array/jsonl ok" << std::endl;
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "[test] file source split/regex/json array/jsonl failed: " << ex.what()
              << std::endl;
    return 1;
  }
}
