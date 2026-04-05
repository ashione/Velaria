#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "src/dataflow/core/contract/api/session.h"
#include "src/dataflow/core/execution/columnar_batch.h"
#include "src/dataflow/core/execution/nanoarrow_ipc_codec.h"
#include "src/dataflow/core/execution/source_materialization.h"
#include "src/dataflow/core/execution/stream/binary_row_batch.h"

namespace {

void expect(bool cond, const std::string& msg) {
  if (!cond) {
    throw std::runtime_error(msg);
  }
}

void expect_table_value(dataflow::Table table, std::size_t row, std::size_t column,
                        const dataflow::Value& expected, const std::string& msg) {
  dataflow::materializeRows(&table);
  expect(row < table.rows.size(), msg + ": row out of range");
  expect(column < table.rows[row].size(), msg + ": column out of range");
  const auto& actual = table.rows[row][column];
  if (expected.isNull()) {
    expect(actual.isNull(), msg + ": expected null but got " + actual.toString());
    return;
  }
  expect(actual == expected,
         msg + ": expected " + expected.toString() + ", got " + actual.toString());
}

std::string make_temp_dir(const std::string& pattern) {
  std::string dir = "/tmp/" + pattern + "-XXXXXX";
  std::vector<char> buffer(dir.begin(), dir.end());
  buffer.push_back('\0');
  char* out = mkdtemp(buffer.data());
  if (out == nullptr) {
    throw std::runtime_error("mkdtemp failed");
  }
  return std::string(out);
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

void write_csv(const std::string& path, const std::string& body) {
  std::ofstream out(path);
  if (!out.is_open()) {
    throw std::runtime_error("cannot write csv file");
  }
  out << body;
}

void expect_roundtrip_sample_table(dataflow::Table table, const std::string& label) {
  dataflow::materializeRows(&table);
  expect(table.schema.fields.size() == 5, label + ": schema width mismatch");
  expect(table.rows.size() == 2, label + ": row count mismatch");
  expect_table_value(table, 0, 0, dataflow::Value(int64_t(1)), label + ": int mismatch");
  expect_table_value(table, 0, 1, dataflow::Value(3.5), label + ": double mismatch");
  expect_table_value(table, 0, 2, dataflow::Value("alice"), label + ": string mismatch");
  expect_table_value(table, 0, 3, dataflow::Value(std::vector<float>{1.0f, 0.5f}),
                     label + ": vector mismatch");
  expect_table_value(table, 0, 4, dataflow::Value(), label + ": null mismatch");
  expect_table_value(table, 1, 0, dataflow::Value(int64_t(2)), label + ": second int mismatch");
  expect_table_value(table, 1, 1, dataflow::Value(), label + ": second null mismatch");
  expect_table_value(table, 1, 2, dataflow::Value("bob"), label + ": second string mismatch");
  expect_table_value(table, 1, 3, dataflow::Value(std::vector<float>{0.0f, 2.0f}),
                     label + ": second vector mismatch");
  expect_table_value(table, 1, 4, dataflow::Value("kept"), label + ": second note mismatch");
}

}  // namespace

int main() {
  try {
    const auto cache_root = make_temp_dir("velaria-source-materialization-cache");
    dataflow::SourceOptions binary_options;
    binary_options.materialization.enabled = true;
    binary_options.materialization.root = cache_root;
    binary_options.materialization.data_format =
        dataflow::MaterializationDataFormat::BinaryRowBatch;

    expect(dataflow::default_source_materialization_data_format() ==
               dataflow::MaterializationDataFormat::BinaryRowBatch,
           "default materialization data format mismatch");
    expect(dataflow::materialization_data_format_name(
               dataflow::MaterializationDataFormat::BinaryRowBatch) == "binary_row_batch",
           "binary data format name mismatch");
    expect(dataflow::materialization_data_format_name(
               dataflow::MaterializationDataFormat::NanoArrowIpc) == "nanoarrow_ipc",
           "nanoarrow data format name mismatch");
    expect(dataflow::materialization_data_format_is_available(
               dataflow::MaterializationDataFormat::BinaryRowBatch),
           "binary data format should be available");
    expect(dataflow::materialization_data_format_is_available(
               dataflow::MaterializationDataFormat::NanoArrowIpc),
           "nanoarrow data format should be available");

    const auto binary_path = make_temp_file("velaria-binary-materialized");
    const auto nanoarrow_path = make_temp_file("velaria-nanoarrow-materialized");
    dataflow::Table roundtrip_source;
    roundtrip_source.schema = dataflow::Schema({"id", "score", "name", "embedding", "note"});
    roundtrip_source.rows = {
        {dataflow::Value(int64_t(1)), dataflow::Value(3.5), dataflow::Value("alice"),
         dataflow::Value(std::vector<float>{1.0f, 0.5f}), dataflow::Value()},
        {dataflow::Value(int64_t(2)), dataflow::Value(), dataflow::Value("bob"),
         dataflow::Value(std::vector<float>{0.0f, 2.0f}), dataflow::Value("kept")},
    };
    dataflow::BinaryRowBatchCodec codec;
    std::vector<uint8_t> payload;
    codec.serialize(roundtrip_source, &payload);
    {
      std::ofstream out(binary_path, std::ios::binary);
      expect(out.is_open(), "cannot open binary roundtrip file");
      out.write(reinterpret_cast<const char*>(payload.data()),
                static_cast<std::streamsize>(payload.size()));
      expect(out.good(), "cannot write binary roundtrip file");
    }
    std::ifstream in(binary_path, std::ios::binary);
    expect(in.is_open(), "cannot reopen binary roundtrip file");
    std::vector<uint8_t> loaded_payload((std::istreambuf_iterator<char>(in)),
                                        std::istreambuf_iterator<char>());
    const auto roundtrip_loaded = codec.deserialize(loaded_payload);
    expect(roundtrip_loaded.schema.fields == roundtrip_source.schema.fields,
           "binary roundtrip schema mismatch");
    expect_roundtrip_sample_table(roundtrip_loaded, "binary roundtrip");

    dataflow::save_nanoarrow_ipc_table(roundtrip_source, nanoarrow_path);
    const auto nanoarrow_loaded = dataflow::load_nanoarrow_ipc_table(nanoarrow_path);
    expect(nanoarrow_loaded.schema.fields == roundtrip_source.schema.fields,
           "nanoarrow roundtrip schema mismatch");
    expect_roundtrip_sample_table(nanoarrow_loaded, "nanoarrow roundtrip");
    const auto nanoarrow_loaded_lazy = dataflow::load_nanoarrow_ipc_table(nanoarrow_path, false);
    expect(nanoarrow_loaded_lazy.rows.empty(), "lazy nanoarrow load should not materialize rows");
    expect(nanoarrow_loaded_lazy.rowCount() == 2, "lazy nanoarrow load row count mismatch");
    expect(dataflow::valueColumnValueAt(*dataflow::viewValueColumn(nanoarrow_loaded_lazy, 2).buffer, 1) ==
               dataflow::Value("bob"),
           "lazy nanoarrow load cache content mismatch");

    const auto csv_path = make_temp_file("velaria-materialized-source");
    write_csv(csv_path, "id,score,name\n1,10,alice\n2,20,bob\n");

    auto& session = dataflow::DataflowSession::builder();
    auto first = session.read_csv(csv_path, binary_options).toTable();
    dataflow::materializeRows(&first);
    expect(first.rows.size() == 2, "first csv read row count mismatch");
    expect_table_value(first, 1, 2, dataflow::Value("bob"), "first csv read content mismatch");

    const auto first_fingerprint =
        dataflow::capture_file_source_fingerprint(csv_path, "csv", "delimiter=,");
    dataflow::SourceMaterializationStore store(
        cache_root, dataflow::MaterializationDataFormat::BinaryRowBatch);
    const auto first_entry = store.lookup(first_fingerprint);
    expect(first_entry.has_value(), "source materialization entry should exist after first read");
    expect(first_entry->data_format == dataflow::MaterializationDataFormat::BinaryRowBatch,
           "source materialization data format mismatch");
    expect(first_entry->fingerprint.source_format == "csv",
           "source materialization source format mismatch");
    expect(first_entry->fingerprint.source_options == "delimiter=,",
           "source materialization source options mismatch");
    expect(first_entry->data_path.size() >= 4 &&
               first_entry->data_path.substr(first_entry->data_path.size() - 4) == ".bin",
           "binary materialization file suffix mismatch");
    const auto first_cache_mtime =
        std::filesystem::last_write_time(first_entry->data_path);

    std::this_thread::sleep_for(std::chrono::milliseconds(25));
    auto second = session.read_csv(csv_path, binary_options).toTable();
    dataflow::materializeRows(&second);
    expect(second.rows.size() == 2, "second csv read row count mismatch");
    expect_table_value(second, 0, 1, dataflow::Value(int64_t(10)),
                       "second csv read content mismatch");
    auto pushed =
        session.read_csv(csv_path)
            .filter("score", ">=", dataflow::Value(int64_t(20)))
            .limit(1)
            .select({"name"})
            .toTable();
    dataflow::materializeRows(&pushed);
    expect(pushed.rows.size() == 1, "source pushdown filter/limit row count mismatch");
    expect_table_value(pushed, 0, 0, dataflow::Value("bob"),
                       "source pushdown filter/limit content mismatch");
    session.createTempView("csv_group_input", session.read_csv(csv_path));
    auto pushed_group_limit =
        session.sql("SELECT name, COUNT(*) AS cnt FROM csv_group_input GROUP BY name LIMIT 1")
            .toTable();
    dataflow::materializeRows(&pushed_group_limit);
    expect(pushed_group_limit.rows.size() == 1,
           "source pushdown aggregate/limit row count mismatch");
    auto async_handle =
        session.submitAsync(session.read_csv(csv_path, binary_options).select({"name"}).limit(1));
    const auto async_wait = async_handle.wait(std::chrono::seconds(5));
    expect(async_wait.has_result, "async wait should expose result");
    auto async_wait_table = async_wait.result;
    dataflow::materializeRows(&async_wait_table);
    expect(async_wait_table.rows.size() == 1, "async wait row count mismatch");
    expect_table_value(async_wait_table, 0, 0, dataflow::Value("alice"),
                       "async wait content mismatch");
    const auto async_query = async_handle.queryJob();
    expect(async_query.has_result, "async query should expose result");
    auto async_query_table = async_query.result;
    dataflow::materializeRows(&async_query_table);
    expect(async_query_table.rows.size() == 1, "async query row count mismatch");
    expect_table_value(async_query_table, 0, 0, dataflow::Value("alice"),
                       "async query content mismatch");
    const auto async_result = async_handle.result(std::chrono::seconds(5));
    auto async_result_table = async_result;
    dataflow::materializeRows(&async_result_table);
    expect(async_result_table.rows.size() == 1, "async result row count mismatch");
    expect_table_value(async_result_table, 0, 0, dataflow::Value("alice"),
                       "async result content mismatch");
    const auto second_entry =
        store.lookup(dataflow::capture_file_source_fingerprint(csv_path, "csv", "delimiter=,"));
    expect(second_entry.has_value(), "source materialization entry should still exist");
    const auto second_cache_mtime =
        std::filesystem::last_write_time(second_entry->data_path);
    expect(first_cache_mtime == second_cache_mtime,
           "cache hit should not rewrite binary materialization file");

    std::this_thread::sleep_for(std::chrono::seconds(1));
    write_csv(csv_path, "id,score,name\n1,10,alice\n2,99,bob\n3,30,charlie\n");
    auto updated = session.read_csv(csv_path, binary_options).toTable();
    dataflow::materializeRows(&updated);
    expect(updated.rows.size() == 3, "updated csv read row count mismatch");
    expect_table_value(updated, 1, 1, dataflow::Value(int64_t(99)),
                       "updated csv read should reflect source changes");
    const auto updated_entry =
        store.lookup(dataflow::capture_file_source_fingerprint(csv_path, "csv", "delimiter=,"));
    expect(updated_entry.has_value(), "updated source materialization entry should exist");
    const auto updated_cache_mtime =
        std::filesystem::last_write_time(updated_entry->data_path);
    expect(updated_cache_mtime != second_cache_mtime,
           "cache invalidation should rewrite binary materialization file");

    const auto nanoarrow_cache_root = make_temp_dir("velaria-source-materialization-nanoarrow");
    dataflow::SourceOptions nanoarrow_options;
    nanoarrow_options.materialization.enabled = true;
    nanoarrow_options.materialization.root = nanoarrow_cache_root;
    nanoarrow_options.materialization.data_format =
        dataflow::MaterializationDataFormat::NanoArrowIpc;

    const auto nanoarrow_csv_path = make_temp_file("velaria-nanoarrow-source");
    write_csv(nanoarrow_csv_path, "id,score,name\n1,10,alice\n2,20,bob\n");
    auto nanoarrow_first = session.read_csv(nanoarrow_csv_path, nanoarrow_options).toTable();
    dataflow::materializeRows(&nanoarrow_first);
    expect(nanoarrow_first.rows.size() == 2, "nanoarrow csv read row count mismatch");
    expect_table_value(nanoarrow_first, 1, 2, dataflow::Value("bob"),
                       "nanoarrow csv read content mismatch");

    dataflow::SourceMaterializationStore nanoarrow_store(
        nanoarrow_cache_root, dataflow::MaterializationDataFormat::NanoArrowIpc);
    const auto nanoarrow_fingerprint =
        dataflow::capture_file_source_fingerprint(nanoarrow_csv_path, "csv", "delimiter=,");
    const auto nanoarrow_entry = nanoarrow_store.lookup(nanoarrow_fingerprint);
    expect(nanoarrow_entry.has_value(), "nanoarrow source materialization entry should exist");
    expect(nanoarrow_entry->data_format == dataflow::MaterializationDataFormat::NanoArrowIpc,
           "nanoarrow source materialization data format mismatch");
    expect(nanoarrow_entry->data_path.size() >= 10 &&
               nanoarrow_entry->data_path.substr(nanoarrow_entry->data_path.size() - 10) ==
                   ".nanoarrow",
           "nanoarrow materialization file suffix mismatch");
    const auto nanoarrow_first_cache_mtime =
        std::filesystem::last_write_time(nanoarrow_entry->data_path);

    std::this_thread::sleep_for(std::chrono::milliseconds(25));
    auto nanoarrow_second =
        session.read_csv(nanoarrow_csv_path, nanoarrow_options).toTable();
    dataflow::materializeRows(&nanoarrow_second);
    expect(nanoarrow_second.rows.size() == 2, "nanoarrow second csv read row count mismatch");
    expect_table_value(nanoarrow_second, 0, 1, dataflow::Value(int64_t(10)),
                       "nanoarrow second csv read content mismatch");
    const auto nanoarrow_second_entry = nanoarrow_store.lookup(
        dataflow::capture_file_source_fingerprint(nanoarrow_csv_path, "csv", "delimiter=,"));
    expect(nanoarrow_second_entry.has_value(),
           "nanoarrow source materialization entry should still exist");
    const auto nanoarrow_lazy = nanoarrow_store.load(*nanoarrow_second_entry, false);
    expect(nanoarrow_lazy.rows.empty(), "nanoarrow store lazy load should keep rows empty");
    expect(nanoarrow_lazy.rowCount() == 2, "nanoarrow store lazy load row count mismatch");
    expect(dataflow::valueColumnValueAt(*dataflow::viewValueColumn(nanoarrow_lazy, 1).buffer, 0) ==
               dataflow::Value(int64_t(10)),
           "nanoarrow store lazy load content mismatch");
    const auto nanoarrow_second_cache_mtime =
        std::filesystem::last_write_time(nanoarrow_second_entry->data_path);
    expect(nanoarrow_first_cache_mtime == nanoarrow_second_cache_mtime,
           "nanoarrow cache hit should not rewrite materialization file");

    const auto quoted_csv_path = make_temp_file("velaria-quoted-largeint-source");
    write_csv(
        quoted_csv_path,
        "record_id,extra,score\n"
        "2026040222134901020610814336213,\"{\"\"cluster\"\":\"\"query\"\",\"\"data_count\"\":200}\",7\n"
        "42,\"{\"\"cluster\"\":\"\"query\"\",\"\"data_count\"\":201}\",8\n");
    auto quoted = session.read_csv(quoted_csv_path).toTable();
    dataflow::materializeRows(&quoted);
    expect(quoted.rows.size() == 2, "quoted csv row count mismatch");
    expect_table_value(quoted, 0, 0, dataflow::Value("2026040222134901020610814336213"),
                       "large integer csv cell should remain string");
    expect_table_value(quoted, 0, 1,
                       dataflow::Value("{\"cluster\":\"query\",\"data_count\":200}"),
                       "quoted json csv cell mismatch");
    expect_table_value(quoted, 1, 2, dataflow::Value(int64_t(8)),
                       "quoted csv numeric tail mismatch");

    const auto multiline_csv_path = make_temp_file("velaria-multiline-quoted-source");
    write_csv(
        multiline_csv_path,
        "id,note,score\n"
        "1,\"hello\nworld\",10\n"
        "2,\"escaped \"\"quote\"\"\",20\n");
    auto multiline = session.read_csv(multiline_csv_path).toTable();
    dataflow::materializeRows(&multiline);
    expect(multiline.rows.size() == 2, "multiline csv row count mismatch");
    expect_table_value(multiline, 0, 0, dataflow::Value(int64_t(1)),
                       "multiline csv first id mismatch");
    expect_table_value(multiline, 0, 1, dataflow::Value("hello\nworld"),
                       "multiline csv embedded newline mismatch");
    expect_table_value(multiline, 1, 1, dataflow::Value("escaped \"quote\""),
                       "multiline csv escaped quote mismatch");
    expect_table_value(multiline, 1, 2, dataflow::Value(int64_t(20)),
                       "multiline csv numeric tail mismatch");

    std::cout << "[test] source materialization binary+nanoarrow cache ok" << std::endl;
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "[test] source materialization cache failed: " << ex.what() << std::endl;
    return 1;
  }
}
