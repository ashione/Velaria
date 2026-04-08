#include <chrono>
#include <fstream>
#include <iostream>
#include <limits>
#include <string>
#include <unistd.h>
#include <vector>

#include "src/dataflow/core/contract/api/session.h"

namespace {

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

void write_fixtures(const std::string& csv_path, const std::string& line_path,
                    const std::string& jsonl_path, std::size_t rows) {
  std::ofstream csv(csv_path);
  std::ofstream line(line_path);
  std::ofstream jsonl(jsonl_path);
  if (!csv.is_open() || !line.is_open() || !jsonl.is_open()) {
    throw std::runtime_error("cannot open benchmark fixture file");
  }
  csv << "id,grp,val\n";
  for (std::size_t i = 0; i < rows; ++i) {
    const int grp = static_cast<int>(i % 16);
    const int val = static_cast<int>((i * 37) % 1000);
    csv << i << ",g" << grp << "," << val << "\n";
    line << i << "|g" << grp << "|" << val << "\n";
    jsonl << "{\"id\":" << i << ",\"grp\":\"g" << grp << "\",\"val\":" << val << "}\n";
  }
}

template <typename Fn>
long long run_bench_ms(Fn&& fn, int rounds) {
  using clock = std::chrono::steady_clock;
  long long best = std::numeric_limits<long long>::max();
  for (int i = 0; i < rounds; ++i) {
    const auto begin = clock::now();
    fn();
    const auto end = clock::now();
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
    if (ms < best) best = ms;
  }
  return best;
}

}  // namespace

int main() {
  try {
    auto& session = dataflow::DataflowSession::builder();
    const auto csv_path = make_temp_file("velaria-bench-csv");
    const auto line_path = make_temp_file("velaria-bench-line");
    const auto jsonl_path = make_temp_file("velaria-bench-jsonl");
    write_fixtures(csv_path, line_path, jsonl_path, 200000);

    dataflow::LineFileOptions line_options;
    line_options.mode = dataflow::LineParseMode::Split;
    line_options.split_delimiter = '|';
    line_options.mappings = {{"id", 0}, {"grp", 1}, {"val", 2}};

    dataflow::JsonFileOptions json_options;
    json_options.format = dataflow::JsonFileFormat::JsonLines;
    json_options.columns = {"id", "grp", "val"};

    auto csv_ms = run_bench_ms(
        [&]() {
          auto out = session.read_csv(csv_path)
                         .filter("val", ">", dataflow::Value(int64_t(500)))
                         .groupBy({"grp"})
                         .sum("val", "sum_val")
                         .toTable();
          (void)out;
        },
        3);

    auto line_ms = run_bench_ms(
        [&]() {
          auto out = session.read_line_file(line_path, line_options)
                         .filter("val", ">", dataflow::Value(int64_t(500)))
                         .groupBy({"grp"})
                         .sum("val", "sum_val")
                         .toTable();
          (void)out;
        },
        3);

    auto json_ms = run_bench_ms(
        [&]() {
          auto out = session.read_json(jsonl_path, json_options)
                         .filter("val", ">", dataflow::Value(int64_t(500)))
                         .groupBy({"grp"})
                         .sum("val", "sum_val")
                         .toTable();
          (void)out;
        },
        3);

    std::cout << "[bench] csv_ms=" << csv_ms << " line_ms=" << line_ms << " jsonl_ms=" << json_ms
              << std::endl;
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "[bench] file source benchmark failed: " << ex.what() << std::endl;
    return 1;
  }
}
