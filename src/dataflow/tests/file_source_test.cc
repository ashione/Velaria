#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

#include "src/dataflow/core/contract/api/session.h"

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

    const auto jsonl_path = make_temp_file("velaria-jsonl");
    write_file(jsonl_path,
               "{\"user_id\":1,\"name\":\"alice\",\"vec\":[1,2]}\n"
               "{\"user_id\":2,\"name\":\"bob\",\"vec\":[0.5,4]}\n");
    dataflow::JsonFileOptions jsonl_options;
    jsonl_options.format = dataflow::JsonFileFormat::JsonLines;
    jsonl_options.columns = {"user_id", "name", "vec"};
    auto jsonl_table = session.read_json(jsonl_path, jsonl_options).toTable();
    expect(jsonl_table.rowCount() == 2, "jsonl row count mismatch");
    expect(jsonl_table.rows[0][1].asString() == "alice", "jsonl name mismatch");
    expect(jsonl_table.rows[0][2].asFixedVector().size() == 2, "jsonl vec width mismatch");

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

    std::cout << "[test] file source split/regex/json array/jsonl ok" << std::endl;
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "[test] file source split/regex/json array/jsonl failed: " << ex.what()
              << std::endl;
    return 1;
  }
}
