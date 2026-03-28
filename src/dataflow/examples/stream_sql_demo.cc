#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

#include "src/dataflow/api/session.h"
#include "src/dataflow/stream/stream.h"

int main() {
  namespace fs = std::filesystem;

  dataflow::DataflowSession& session = dataflow::DataflowSession::builder();

  const std::string input_dir = "/tmp/velaria-stream-sql-demo-input";
  const std::string output_path = "/tmp/velaria-stream-sql-demo-output.csv";
  const std::string checkpoint_path = "/tmp/velaria-stream-sql-demo.checkpoint";

  fs::remove_all(input_dir);
  fs::create_directories(input_dir);
  fs::remove(output_path);
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

  session.sql(
      "CREATE SOURCE TABLE stream_events_demo (key STRING, value INT) "
      "USING csv OPTIONS(path '" + input_dir + "', delimiter ',')");
  session.sql(
      "CREATE SINK TABLE stream_summary_csv (key STRING, value_sum INT) "
      "USING csv OPTIONS(path '" + output_path + "', delimiter ',')");

  dataflow::StreamingQueryOptions options;
  options.trigger_interval_ms = 50;
  options.max_inflight_batches = 2;
  options.max_queued_partitions = 4;
  options.backpressure_high_watermark = 2;
  options.backpressure_low_watermark = 1;
  options.checkpoint_path = checkpoint_path;
  options.execution_mode = dataflow::StreamingExecutionMode::LocalWorkers;
  options.local_workers = 2;

  auto query = session.startStreamSql(
      "INSERT INTO stream_summary_csv "
      "SELECT key, SUM(value) AS value_sum "
      "FROM stream_events_demo "
      "WHERE value > 6 "
      "GROUP BY key "
      "HAVING value_sum > 15 "
      "LIMIT 10",
      options);

  const size_t batches = query.awaitTermination(2);
  std::cout << "Processed batches: " << batches << std::endl;
  std::cout << "Output path: " << output_path << std::endl;
  std::cout << "Progress: " << query.snapshotJson() << std::endl;
  return 0;
}
