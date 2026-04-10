#include <chrono>
#include <algorithm>
#include <array>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <iterator>
#include <sstream>
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
                    const std::string& line_regex_path, const std::string& jsonl_path,
                    std::size_t rows) {
  std::ofstream csv(csv_path);
  std::ofstream line(line_path);
  std::ofstream line_regex(line_regex_path);
  std::ofstream jsonl(jsonl_path);
  if (!csv.is_open() || !line.is_open() || !line_regex.is_open() || !jsonl.is_open()) {
    throw std::runtime_error("cannot open benchmark fixture file");
  }
  csv << "id,grp,val\n";
  for (std::size_t i = 0; i < rows; ++i) {
    const int grp = static_cast<int>(i % 16);
    const int val = static_cast<int>((i * 37) % 1000);
    csv << i << ",g" << grp << "," << val << "\n";
    if (i == 0) {
      line << i << "|g" << grp << "|0.5\n";
    } else {
      line << i << "|g" << grp << "|" << val << "\n";
    }
    line_regex << "uid=" << i << " grp=g" << grp << " val=" << val << " ok="
               << ((i % 2) == 0 ? "true" : "false")
               << " note=path_" << (i % 128) << "\\\\segment" << "\n";
    jsonl << "{\"id\":" << i << ",\"grp\":\"g" << grp << "\",\"val\":" << val << "}\n";
  }
}

template <typename Fn>
long long run_bench_us(Fn&& fn, int rounds) {
  using clock = std::chrono::steady_clock;
  long long best = std::numeric_limits<long long>::max();
  for (int i = 0; i < rounds; ++i) {
    const auto begin = clock::now();
    fn(i);
    const auto end = clock::now();
    const auto us = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
    if (us < best) best = us;
  }
  return best;
}

dataflow::Table run_group_sum(const dataflow::DataFrame& df, const std::string& key_col,
                              const std::string& value_col) {
  return df.filter(value_col, ">", dataflow::Value(int64_t(500)))
      .groupBy({key_col})
      .sum(value_col, "sum_val")
      .toTable();
}

dataflow::Table run_sql_predicate_aggregate(dataflow::DataflowSession& session,
                                            const std::string& table_name,
                                            const std::string& where_clause) {
  return session.sql("SELECT grp, COUNT(*) AS cnt FROM " + table_name + " WHERE " + where_clause +
                     " GROUP BY grp LIMIT 32")
      .toTable();
}

void expect(bool cond, const std::string& msg) {
  if (!cond) {
    throw std::runtime_error(msg);
  }
}

void emit_bench(const std::string& bench_case, std::size_t rows, int rounds, long long best_us,
                std::size_t result_rows, const std::string& note = "") {
  std::cout << "{"
            << "\"bench\":\"file-input\","
            << "\"case\":\"" << bench_case << "\","
            << "\"rows\":" << rows << ","
            << "\"rounds\":" << rounds << ","
            << "\"best_us\":" << best_us << ","
            << "\"result_rows\":" << result_rows;
  if (!note.empty()) {
    std::cout << ",\"note\":\"" << note << "\"";
  }
  std::cout << "}" << std::endl;
}

void emit_compare(const std::string& bench_case, std::size_t rows, int rounds,
                  long long baseline_us, long long candidate_us, const std::string& baseline,
                  const std::string& candidate) {
  const double ratio = baseline_us == 0
                           ? 0.0
                           : static_cast<double>(candidate_us) / static_cast<double>(baseline_us);
  std::cout << "{"
            << "\"bench\":\"file-input-compare\","
            << "\"case\":\"" << bench_case << "\","
            << "\"rows\":" << rows << ","
            << "\"rounds\":" << rounds << ","
            << "\"baseline\":\"" << baseline << "\","
            << "\"candidate\":\"" << candidate << "\","
            << "\"baseline_us\":" << baseline_us << ","
            << "\"candidate_us\":" << candidate_us << ","
            << "\"ratio\":" << ratio
            << "}" << std::endl;
}

std::string table_name_for(const std::string& prefix, int round) {
  std::ostringstream out;
  out << prefix << "_" << ::getpid() << "_" << round;
  return out.str();
}

std::string read_all(const std::string& path) {
  std::ifstream input(path, std::ios::binary);
  if (!input.is_open()) {
    throw std::runtime_error("cannot open benchmark input");
  }
  return std::string(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
}

dataflow::SourceOptions no_pushdown_options(const std::string& tag, int round) {
  dataflow::SourceOptions options;
  options.materialization.enabled = true;
  std::ostringstream root;
  root << "/tmp/velaria_bench_no_pushdown_" << ::getpid() << "_" << tag << "_" << round;
  options.materialization.root = root.str();
  std::filesystem::remove_all(options.materialization.root);
  return options;
}

inline void skip_to_next_line(const char*& ptr, const char* end) {
  while (ptr < end && *ptr != '\n') {
    ++ptr;
  }
  if (ptr < end && *ptr == '\n') {
    ++ptr;
  }
}

inline void skip_digits(const char*& ptr, const char* end) {
  while (ptr < end && *ptr >= '0' && *ptr <= '9') {
    ++ptr;
  }
}

inline int parse_int_fast(const char*& ptr, const char* end) {
  int value = 0;
  while (ptr < end && *ptr >= '0' && *ptr <= '9') {
    value = value * 10 + (*ptr - '0');
    ++ptr;
  }
  return value;
}

inline void expect_char(const char*& ptr, const char* end, char expected,
                        const char* context) {
  if (ptr >= end || *ptr != expected) {
    throw std::runtime_error(context);
  }
  ++ptr;
}

std::size_t hardcode_group_sum_csv_like(const std::string& path, char delimiter,
                                        bool skip_header) {
  const auto payload = read_all(path);
  const char* ptr = payload.data();
  const char* end = ptr + payload.size();
  std::array<long long, 16> sums{};
  std::array<uint8_t, 16> seen{};
  if (skip_header) {
    skip_to_next_line(ptr, end);
  }
  while (ptr < end) {
    skip_digits(ptr, end);
    expect_char(ptr, end, delimiter, "hardcode csv/line missing first delimiter");
    expect_char(ptr, end, 'g', "hardcode csv/line missing group prefix");
    const int bucket = parse_int_fast(ptr, end);
    expect_char(ptr, end, delimiter, "hardcode csv/line missing second delimiter");
    const int val = parse_int_fast(ptr, end);
    skip_to_next_line(ptr, end);
    if (bucket < 0 || bucket >= 16) {
      continue;
    }
    if (val <= 500) {
      continue;
    }
    sums[static_cast<std::size_t>(bucket)] += val;
    seen[static_cast<std::size_t>(bucket)] = 1;
  }
  std::size_t groups = 0;
  for (const auto flag : seen) {
    groups += flag != 0 ? 1U : 0U;
  }
  return groups;
}

std::size_t hardcode_group_sum_jsonl(const std::string& path) {
  const auto payload = read_all(path);
  const char* ptr = payload.data();
  const char* end = ptr + payload.size();
  std::array<long long, 16> sums{};
  std::array<uint8_t, 16> seen{};
  constexpr const char* kGrpKey = "\"grp\":\"g";
  constexpr const char* kValKey = "\"val\":";
  while (ptr < end) {
    const char* grp = std::search(ptr, end, kGrpKey, kGrpKey + 8);
    if (grp == end) {
      break;
    }
    grp += 8;
    const int bucket = parse_int_fast(grp, end);
    const char* val = std::search(grp, end, kValKey, kValKey + 6);
    if (val == end) {
      break;
    }
    val += 6;
    const int parsed_val = parse_int_fast(val, end);
    ptr = val;
    skip_to_next_line(ptr, end);
    if (bucket < 0 || bucket >= 16 || parsed_val <= 500) {
      continue;
    }
    sums[static_cast<std::size_t>(bucket)] += parsed_val;
    seen[static_cast<std::size_t>(bucket)] = 1;
  }
  std::size_t groups = 0;
  for (const auto flag : seen) {
    groups += flag != 0 ? 1U : 0U;
  }
  return groups;
}

std::size_t hardcode_group_sum_regex_like(const std::string& path) {
  const auto payload = read_all(path);
  const char* ptr = payload.data();
  const char* end = ptr + payload.size();
  std::array<long long, 16> sums{};
  std::array<uint8_t, 16> seen{};
  constexpr const char* kGrpKey = " grp=g";
  constexpr const char* kValKey = " val=";
  while (ptr < end) {
    const char* grp = std::search(ptr, end, kGrpKey, kGrpKey + 6);
    if (grp == end) {
      break;
    }
    grp += 6;
    const int bucket = parse_int_fast(grp, end);
    const char* val = std::search(grp, end, kValKey, kValKey + 5);
    if (val == end) {
      break;
    }
    val += 5;
    const int parsed_val = parse_int_fast(val, end);
    ptr = val;
    skip_to_next_line(ptr, end);
    if (bucket < 0 || bucket >= 16 || parsed_val <= 500) {
      continue;
    }
    sums[static_cast<std::size_t>(bucket)] += parsed_val;
    seen[static_cast<std::size_t>(bucket)] = 1;
  }
  std::size_t groups = 0;
  for (const auto flag : seen) {
    groups += flag != 0 ? 1U : 0U;
  }
  return groups;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    auto& session = dataflow::DataflowSession::builder();
    std::size_t rows = 200000;
    int rounds = 3;
    if (argc > 1) {
      rows = static_cast<std::size_t>(std::strtoull(argv[1], nullptr, 10));
    }
    if (argc > 2) {
      rounds = std::max(1, std::atoi(argv[2]));
    }

    const auto csv_path = make_temp_file("velaria-bench-csv");
    const auto line_path = make_temp_file("velaria-bench-line");
    const auto line_regex_path = make_temp_file("velaria-bench-line-regex");
    const auto jsonl_path = make_temp_file("velaria-bench-jsonl");
    write_fixtures(csv_path, line_path, line_regex_path, jsonl_path, rows);

    dataflow::LineFileOptions line_options;
    line_options.mode = dataflow::LineParseMode::Split;
    line_options.split_delimiter = '|';
    line_options.mappings = {{"id", 0}, {"grp", 1}, {"val", 2}};

    dataflow::JsonFileOptions json_options;
    json_options.format = dataflow::JsonFileFormat::JsonLines;
    json_options.columns = {"id", "grp", "val"};
    dataflow::LineFileOptions line_regex_options;
    line_regex_options.mode = dataflow::LineParseMode::Regex;
    line_regex_options.regex_pattern =
        R"(^uid=(\d+) grp=(g\d+) val=(\d+) ok=(true|false) note=(.+)$)";
    line_regex_options.mappings = {{"id", 1}, {"grp", 2}, {"val", 3}, {"ok", 4}, {"note", 5}};

    const auto csv_probe = session.probe(csv_path);
    const auto line_probe = session.probe(line_path);
    const auto json_probe = session.probe(jsonl_path);
    expect(csv_probe.kind == dataflow::FileSourceKind::Csv, "csv probe benchmark kind mismatch");
    expect(line_probe.kind == dataflow::FileSourceKind::Line, "line probe benchmark kind mismatch");
    expect(json_probe.kind == dataflow::FileSourceKind::Json, "json probe benchmark kind mismatch");

    const auto probe_csv_us = run_bench_us([&](int) {
      const auto probe = session.probe(csv_path);
      expect(probe.kind == dataflow::FileSourceKind::Csv, "probe csv benchmark mismatch");
    }, rounds);
    emit_bench("probe_csv", rows, rounds, probe_csv_us, csv_probe.schema.fields.size(),
               csv_probe.format_name);

    const auto probe_line_us = run_bench_us([&](int) {
      const auto probe = session.probe(line_path);
      expect(probe.kind == dataflow::FileSourceKind::Line, "probe line benchmark mismatch");
    }, rounds);
    emit_bench("probe_line", rows, rounds, probe_line_us, line_probe.schema.fields.size(),
               line_probe.format_name);

    const auto probe_json_us = run_bench_us([&](int) {
      const auto probe = session.probe(jsonl_path);
      expect(probe.kind == dataflow::FileSourceKind::Json, "probe json benchmark mismatch");
    }, rounds);
    emit_bench("probe_json_lines", rows, rounds, probe_json_us, json_probe.schema.fields.size(),
               json_probe.format_name);

    const auto csv_explicit_us = run_bench_us([&](int) {
      auto out = run_group_sum(session.read_csv(csv_path), "grp", "val");
      expect(out.rowCount() == 16, "csv explicit benchmark row count mismatch");
    }, rounds);
    const auto csv_hardcode_us = run_bench_us([&](int) {
      expect(hardcode_group_sum_csv_like(csv_path, ',', true) == 16,
             "csv hardcode benchmark row count mismatch");
    }, rounds);
    emit_bench("read_csv_hardcode_group_sum", rows, rounds, csv_hardcode_us, 16, "hardcode");
    emit_bench("read_csv_explicit_group_sum", rows, rounds, csv_explicit_us, 16, "explicit");

    const auto csv_auto_us = run_bench_us([&](int) {
      auto out = run_group_sum(session.read(csv_path), "grp", "val");
      expect(out.rowCount() == 16, "csv auto benchmark row count mismatch");
    }, rounds);
    emit_bench("read_csv_auto_group_sum", rows, rounds, csv_auto_us, 16, "probe+read");

    const auto line_explicit_us = run_bench_us([&](int) {
      auto out = run_group_sum(session.read_line_file(line_path, line_options), "grp", "val");
      expect(out.rowCount() == 16, "line explicit benchmark row count mismatch");
    }, rounds);
    const auto line_hardcode_us = run_bench_us([&](int) {
      expect(hardcode_group_sum_csv_like(line_path, '|', false) == 16,
             "line hardcode benchmark row count mismatch");
    }, rounds);
    emit_bench("read_line_hardcode_group_sum", rows, rounds, line_hardcode_us, 16, "hardcode");
    emit_bench("read_line_explicit_group_sum", rows, rounds, line_explicit_us, 16, "explicit");

    const auto line_auto_us = run_bench_us([&](int) {
      auto out = run_group_sum(session.read(line_path), "c1", "c2");
      expect(out.rowCount() == 16, "line auto benchmark row count mismatch");
    }, rounds);
    emit_bench("read_line_auto_group_sum", rows, rounds, line_auto_us, 16, "probe+read");

    const auto line_regex_parse_us = run_bench_us([&](int) {
      auto out = session.read_line_file(line_regex_path, line_regex_options).limit(1).toTable();
      expect(out.rowCount() == 1, "line regex parse benchmark row count mismatch");
    }, rounds);
    emit_bench("read_line_regex_parse", rows, rounds, line_regex_parse_us, 1,
               "explicit-regex-parse");

    const auto line_regex_explicit_us = run_bench_us([&](int) {
      auto out = run_group_sum(session.read_line_file(line_regex_path, line_regex_options), "grp", "val");
      expect(out.rowCount() == 16, "line regex explicit benchmark row count mismatch");
    }, rounds);
    const auto line_regex_hardcode_us = run_bench_us([&](int) {
      expect(hardcode_group_sum_regex_like(line_regex_path) == 16,
             "line regex hardcode benchmark row count mismatch");
    }, rounds);
    emit_bench("read_line_regex_hardcode_group_sum", rows, rounds, line_regex_hardcode_us, 16,
               "hardcode");
    emit_bench("read_line_regex_explicit_group_sum", rows, rounds, line_regex_explicit_us, 16,
               "explicit");

    const auto json_explicit_us = run_bench_us([&](int) {
      auto out = run_group_sum(session.read_json(jsonl_path, json_options), "grp", "val");
      expect(out.rowCount() == 16, "json explicit benchmark row count mismatch");
    }, rounds);
    const auto json_hardcode_us = run_bench_us([&](int) {
      expect(hardcode_group_sum_jsonl(jsonl_path) == 16,
             "json hardcode benchmark row count mismatch");
    }, rounds);
    emit_bench("read_json_hardcode_group_sum", rows, rounds, json_hardcode_us, 16, "hardcode");
    emit_bench("read_json_explicit_group_sum", rows, rounds, json_explicit_us, 16, "explicit");

    const auto json_auto_us = run_bench_us([&](int) {
      auto out = run_group_sum(session.read(jsonl_path), "grp", "val");
      expect(out.rowCount() == 16, "json auto benchmark row count mismatch");
    }, rounds);
    emit_bench("read_json_auto_group_sum", rows, rounds, json_auto_us, 16, "probe+read");

    const auto sql_probe_create_us = run_bench_us([&](int round) {
      const auto table_name = table_name_for("file_probe_input", round);
      session.sql("CREATE TABLE " + table_name + " OPTIONS(path: '" + jsonl_path + "')");
      auto out = session.sql("SELECT COUNT(*) AS cnt FROM " + table_name).toTable();
      expect(out.rowCount() == 1, "sql probe create benchmark row count mismatch");
    }, rounds);
    emit_bench("sql_create_table_probe_only_json", rows, rounds, sql_probe_create_us, 1,
               "create+count");

    const auto sql_explicit_create_us = run_bench_us([&](int round) {
      const auto table_name = table_name_for("file_explicit_input", round);
      session.sql("CREATE TABLE " + table_name +
                  " USING json OPTIONS(path: '" + jsonl_path +
                  "', columns: 'id,grp,val', format: 'json_lines')");
      auto out = session.sql("SELECT COUNT(*) AS cnt FROM " + table_name).toTable();
      expect(out.rowCount() == 1, "sql explicit create benchmark row count mismatch");
    }, rounds);
    emit_bench("sql_create_table_explicit_json", rows, rounds, sql_explicit_create_us, 1,
               "create+count");

    const auto sql_predicate_and_us = run_bench_us([&](int round) {
      const auto pushdown_view = table_name_for("file_source_bench_csv_predicate_pushdown", round);
      session.createTempView(pushdown_view, session.read_csv(csv_path));
      auto out = run_sql_predicate_aggregate(
          session, pushdown_view, "grp = 'g1' AND val > 500");
      expect(out.rowCount() == 1, "sql predicate and benchmark row count mismatch");
    }, rounds);
    emit_bench("sql_csv_predicate_and_group_count", rows, rounds, sql_predicate_and_us, 1,
               "source-pushdown");
    const auto sql_predicate_and_nopush_us = run_bench_us([&](int round) {
      const auto fallback_view = table_name_for("file_source_bench_csv_predicate_fallback_only", round);
      session.createTempView(fallback_view,
                             session.read_csv(csv_path, no_pushdown_options("csv_and_only", round)));
      auto out = run_sql_predicate_aggregate(
          session, fallback_view, "grp = 'g1' AND val > 500");
      expect(out.rowCount() == 1, "sql predicate and no-pushdown row count mismatch");
    }, rounds);
    emit_compare("sql_csv_predicate_and_group_count", rows, rounds, sql_predicate_and_nopush_us,
                 sql_predicate_and_us, "no-pushdown", "pushdown");

    const auto sql_predicate_or_us = run_bench_us([&](int round) {
      const auto pushdown_view = table_name_for("file_source_bench_csv_or_pushdown", round);
      session.createTempView(pushdown_view, session.read_csv(csv_path));
      auto out = run_sql_predicate_aggregate(
          session, pushdown_view, "grp = 'g1' OR grp = 'g2'");
      expect(out.rowCount() == 2, "sql predicate or benchmark row count mismatch");
    }, rounds);
    emit_bench("sql_csv_predicate_or_group_count", rows, rounds, sql_predicate_or_us, 2,
               "source-pushdown");
    const auto sql_predicate_or_nopush_us = run_bench_us([&](int round) {
      const auto fallback_view = table_name_for("file_source_bench_csv_or_fallback", round);
      session.createTempView(fallback_view,
                             session.read_csv(csv_path, no_pushdown_options("csv_or", round)));
      auto out = run_sql_predicate_aggregate(
          session, fallback_view, "grp = 'g1' OR grp = 'g2'");
      expect(out.rowCount() == 2, "sql predicate or no-pushdown row count mismatch");
    }, rounds);
    emit_compare("sql_csv_predicate_or_group_count", rows, rounds, sql_predicate_or_nopush_us,
                 sql_predicate_or_us, "no-pushdown", "pushdown");

    const auto sql_predicate_mixed_us = run_bench_us([&](int round) {
      const auto pushdown_view = table_name_for("file_source_bench_csv_mixed_pushdown", round);
      session.createTempView(pushdown_view, session.read_csv(csv_path));
      auto out = run_sql_predicate_aggregate(
          session, pushdown_view,
          "(grp = 'g1' OR grp = 'g2') AND val > 500");
      expect(out.rowCount() == 2, "sql predicate mixed benchmark row count mismatch");
    }, rounds);
    emit_bench("sql_csv_predicate_mixed_group_count", rows, rounds, sql_predicate_mixed_us, 2,
               "source-pushdown");
    const auto sql_predicate_mixed_nopush_us = run_bench_us([&](int round) {
      const auto fallback_view = table_name_for("file_source_bench_csv_mixed_fallback", round);
      session.createTempView(fallback_view,
                             session.read_csv(csv_path, no_pushdown_options("csv_mixed", round)));
      auto out = run_sql_predicate_aggregate(
          session, fallback_view, "(grp = 'g1' OR grp = 'g2') AND val > 500");
      expect(out.rowCount() == 2, "sql predicate mixed no-pushdown row count mismatch");
    }, rounds);
    emit_compare("sql_csv_predicate_mixed_group_count", rows, rounds,
                 sql_predicate_mixed_nopush_us, sql_predicate_mixed_us,
                 "no-pushdown", "pushdown");

    const auto sql_predicate_line_or_us = run_bench_us([&](int round) {
      const auto pushdown_view = table_name_for("file_source_bench_line_or_pushdown", round);
      session.createTempView(pushdown_view, session.read_line_file(line_path, line_options));
      auto out = run_sql_predicate_aggregate(
          session, pushdown_view, "grp = 'g1' OR grp = 'g2'");
      expect(out.rowCount() == 2, "sql line predicate or benchmark row count mismatch");
    }, rounds);
    emit_bench("sql_line_predicate_or_group_count", rows, rounds, sql_predicate_line_or_us, 2,
               "source-pushdown");
    const auto sql_predicate_line_or_nopush_us = run_bench_us([&](int round) {
      const auto fallback_view = table_name_for("file_source_bench_line_or_fallback", round);
      session.createTempView(
          fallback_view,
          session.read_line_file(line_path, line_options, no_pushdown_options("line_or", round)));
      auto out = run_sql_predicate_aggregate(
          session, fallback_view, "grp = 'g1' OR grp = 'g2'");
      expect(out.rowCount() == 2, "sql line predicate or no-pushdown row count mismatch");
    }, rounds);
    emit_compare("sql_line_predicate_or_group_count", rows, rounds,
                 sql_predicate_line_or_nopush_us, sql_predicate_line_or_us,
                 "no-pushdown", "pushdown");

    const auto sql_predicate_json_or_us = run_bench_us([&](int round) {
      const auto pushdown_view = table_name_for("file_source_bench_json_or_pushdown", round);
      session.createTempView(pushdown_view, session.read_json(jsonl_path, json_options));
      auto out = run_sql_predicate_aggregate(
          session, pushdown_view, "grp = 'g1' OR grp = 'g2'");
      expect(out.rowCount() == 2, "sql json predicate or benchmark row count mismatch");
    }, rounds);
    emit_bench("sql_json_predicate_or_group_count", rows, rounds, sql_predicate_json_or_us, 2,
               "source-pushdown");
    const auto sql_predicate_json_or_nopush_us = run_bench_us([&](int round) {
      const auto fallback_view = table_name_for("file_source_bench_json_or_fallback", round);
      session.createTempView(
          fallback_view,
          session.read_json(jsonl_path, json_options, no_pushdown_options("json_or", round)));
      auto out = run_sql_predicate_aggregate(
          session, fallback_view, "grp = 'g1' OR grp = 'g2'");
      expect(out.rowCount() == 2, "sql json predicate or no-pushdown row count mismatch");
    }, rounds);
    emit_compare("sql_json_predicate_or_group_count", rows, rounds,
                 sql_predicate_json_or_nopush_us, sql_predicate_json_or_us,
                 "no-pushdown", "pushdown");

    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "[bench] file source benchmark failed: " << ex.what() << std::endl;
    return 1;
  }
}
