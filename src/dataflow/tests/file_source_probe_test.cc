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

std::string make_temp_file(const std::string& pattern, const std::string& suffix = "") {
  std::string path = "/tmp/" + pattern + "-XXXXXX" + suffix;
  std::vector<char> buffer(path.begin(), path.end());
  buffer.push_back('\0');
  const int fd = suffix.empty() ? mkstemp(buffer.data()) : mkstemps(buffer.data(), suffix.size());
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

}  // namespace

int main() {
  try {
    auto& session = dataflow::DataflowSession::builder();

    const auto csv_path = make_temp_file("velaria-probe-csv", ".csv");
    write_file(csv_path, "user_id,name\n1,alice\n2,bob\n");
    const auto csv_probe = session.probe(csv_path);
    expect(csv_probe.kind == dataflow::FileSourceKind::Csv, "csv probe kind mismatch");
    expect(csv_probe.format_name == "csv", "csv probe format mismatch");
    expect(csv_probe.confidence == "high", "csv probe confidence mismatch");
    expect(!csv_probe.candidates.empty(), "csv probe candidates missing");
    expect(csv_probe.candidates.front().format_name == "csv", "csv top candidate mismatch");
    expect(!csv_probe.candidates.front().evidence.empty(), "csv probe evidence missing");
    expect(csv_probe.csv_delimiter == ',', "csv probe delimiter mismatch");
    expect(csv_probe.schema.fields.size() == 2, "csv probe schema width mismatch");
    expect(csv_probe.schema.fields[0] == "user_id", "csv probe first column mismatch");
    auto csv_table = session.read(csv_path).toTable();
    expect(csv_table.rowCount() == 2, "csv auto row count mismatch");
    expect(csv_table.rows[1][1].asString() == "bob", "csv auto second row mismatch");

    const auto tsv_path = make_temp_file("velaria-probe-tsv", ".tsv");
    write_file(tsv_path, "user_id\tname\n1\talice\n");
    const auto tsv_probe = session.probe(tsv_path);
    expect(tsv_probe.kind == dataflow::FileSourceKind::Csv, "tsv probe kind mismatch");
    expect(tsv_probe.csv_delimiter == '\t', "tsv probe delimiter mismatch");

    const auto csv_no_header_path = make_temp_file("velaria-probe-csv-no-header", ".csv");
    write_file(csv_no_header_path, "1,alice\n2,bob\n");
    const auto csv_no_header_probe = session.probe(csv_no_header_path);
    expect(csv_no_header_probe.kind == dataflow::FileSourceKind::Csv,
           "csv no-header probe kind mismatch");
    expect(csv_no_header_probe.format_name == "csv", "csv no-header format mismatch");
    expect(csv_no_header_probe.csv_delimiter == ',', "csv no-header delimiter mismatch");

    const auto pseudo_csv_line_path = make_temp_file("velaria-probe-pseudo-csv-line", ".csv");
    write_file(pseudo_csv_line_path, "1001|ok|12.5\n1002|fail|9.5\n");
    const auto pseudo_csv_line_probe = session.probe(pseudo_csv_line_path);
    expect(pseudo_csv_line_probe.kind == dataflow::FileSourceKind::Line,
           "pipe-delimited .csv should prefer line probe");
    expect(pseudo_csv_line_probe.format_name == "line_split",
           "pipe-delimited .csv format mismatch");

    const auto jsonl_path = make_temp_file("velaria-probe-jsonl");
    write_file(jsonl_path, "{\"user_id\":1,\"name\":\"alice\"}\n{\"user_id\":2,\"name\":\"bob\"}\n");
    const auto jsonl_probe = session.probe(jsonl_path);
    expect(jsonl_probe.kind == dataflow::FileSourceKind::Json, "jsonl probe kind mismatch");
    expect(jsonl_probe.format_name == "json_lines", "jsonl probe format name mismatch");
    expect(jsonl_probe.score >= 90, "jsonl probe score too low");
    expect(jsonl_probe.confidence == "high", "jsonl probe confidence mismatch");
    expect(jsonl_probe.warnings.empty(), "jsonl probe warnings mismatch");
    expect(jsonl_probe.json_options.format == dataflow::JsonFileFormat::JsonLines,
           "jsonl probe format mismatch");
    expect(jsonl_probe.schema.fields.size() == 2, "jsonl probe schema width mismatch");
    auto jsonl_table = session.read(jsonl_path).toTable();
    expect(jsonl_table.rowCount() == 2, "jsonl auto row count mismatch");
    expect(jsonl_table.rows[0][1].asString() == "alice", "jsonl auto row mismatch");

    const auto jsonl_no_ext_path = make_temp_file("velaria-probe-jsonl-no-ext");
    write_file(jsonl_no_ext_path, "{\"user_id\":3,\"name\":\"carol\"}\n{\"user_id\":4,\"name\":\"dave\"}\n");
    const auto jsonl_no_ext_probe = session.probe(jsonl_no_ext_path);
    expect(jsonl_no_ext_probe.kind == dataflow::FileSourceKind::Json,
           "jsonl without extension probe kind mismatch");
    expect(jsonl_no_ext_probe.format_name == "json_lines",
           "jsonl without extension format mismatch");

    const auto jsonl_json_ext_path = make_temp_file("velaria-probe-jsonl-json-ext", ".json");
    write_file(jsonl_json_ext_path, "{\"user_id\":5,\"name\":\"eve\"}\n{\"user_id\":6,\"name\":\"frank\"}\n");
    const auto jsonl_json_ext_probe = session.probe(jsonl_json_ext_path);
    expect(jsonl_json_ext_probe.kind == dataflow::FileSourceKind::Json,
           "jsonl with .json extension probe kind mismatch");
    expect(jsonl_json_ext_probe.format_name == "json_lines",
           "jsonl with .json extension format mismatch");

    const auto json_array_path = make_temp_file("velaria-probe-json-array");
    write_file(json_array_path, "[{\"event\":\"open\",\"cost\":1.5},{\"event\":\"close\",\"cost\":2}]\n");
    const auto json_array_probe = session.probe(json_array_path);
    expect(json_array_probe.kind == dataflow::FileSourceKind::Json,
           "json array probe kind mismatch");
    expect(json_array_probe.format_name == "json_array", "json array probe format name mismatch");
    expect(json_array_probe.json_options.format == dataflow::JsonFileFormat::JsonArray,
           "json array probe format mismatch");
    auto json_array_table = session.read(json_array_path).toTable();
    expect(json_array_table.rowCount() == 2, "json array auto row count mismatch");
    expect(json_array_table.rows[1][0].asString() == "close", "json array auto row mismatch");

    const auto json_nested_path = make_temp_file("velaria-probe-json-nested");
    write_file(json_nested_path,
               "[{\"a\":1,\"b\":{\"b1\":1}},{\"a\":2,\"b\":{\"b1\":2,\"b2\":[\"x\",3,null]}}]\n");
    dataflow::JsonFileOptions nested_options;
    nested_options.format = dataflow::JsonFileFormat::JsonArray;
    nested_options.columns = {"a", "b"};
    auto json_nested_table = session.read_json(json_nested_path, nested_options).toTable();
    expect(json_nested_table.rowCount() == 2, "json nested object row count mismatch");
    expect(json_nested_table.rows[0][0].asInt64() == 1, "json nested first scalar mismatch");
    expect(json_nested_table.rows[0][1].asString() == "{\"b1\":1}",
           "json nested first object string mismatch");
    expect(json_nested_table.rows[1][1].asString() == "{\"b1\":2,\"b2\":[\"x\",3,null]}",
           "json nested second object string mismatch");

    const auto line_path = make_temp_file("velaria-probe-line");
    write_file(line_path, "1001|ok|12.5\n1002|fail|9.5\n");
    const auto line_probe = session.probe(line_path);
    expect(line_probe.kind == dataflow::FileSourceKind::Line, "line probe kind mismatch");
    expect(line_probe.format_name == "line_split", "line probe format mismatch");
    expect(line_probe.confidence == "medium", "line probe confidence mismatch");
    expect(line_probe.candidates.size() >= 1, "line probe candidate count mismatch");
    expect(line_probe.line_options.mode == dataflow::LineParseMode::Split,
           "line probe mode mismatch");
    expect(line_probe.line_options.split_delimiter == '|', "line probe delimiter mismatch");
    expect(line_probe.schema.fields.size() == 3, "line probe schema width mismatch");
    expect(line_probe.schema.fields[0] == "c0", "line probe first column mismatch");
    auto line_table = session.read(line_path).toTable();
    expect(line_table.rowCount() == 2, "line auto row count mismatch");
    expect(line_table.rows[0][0].asInt64() == 1001, "line auto first value mismatch");

    std::cout << "[test] file source probe csv/tsv/jsonl/json-array/line ok" << std::endl;
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "[test] file source probe failed: " << ex.what() << std::endl;
    return 1;
  }
}
