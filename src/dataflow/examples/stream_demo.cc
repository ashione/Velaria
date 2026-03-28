#include <cstdint>
#include <filesystem>
#include <iostream>

#include "src/dataflow/api/session.h"
#include "src/dataflow/stream/stream.h"

int main() {
  dataflow::DataflowSession& session = dataflow::DataflowSession::builder();

  dataflow::Table batch1;
  batch1.schema = dataflow::Schema({"ts", "key", "value"});
  batch1.rows = {
      {dataflow::Value("2026-03-28T09:00:00"), dataflow::Value("userA"),
       dataflow::Value(int64_t(10))},
      {dataflow::Value("2026-03-28T09:00:10"), dataflow::Value("userA"),
       dataflow::Value(int64_t(5))},
  };

  dataflow::Table batch2;
  batch2.schema = dataflow::Schema({"ts", "key", "value"});
  batch2.rows = {
      {dataflow::Value("2026-03-28T09:00:20"), dataflow::Value("userB"),
       dataflow::Value(int64_t(20))},
      {dataflow::Value("2026-03-28T09:00:30"), dataflow::Value("userA"),
       dataflow::Value(int64_t(7))},
  };

  auto source =
      std::make_shared<dataflow::MemoryStreamSource>(std::vector<dataflow::Table>{batch1, batch2});
  auto state = dataflow::makeMemoryStateStore();

  auto stream = session.readStream(source)
                    .withStateStore(state)
                    .filter("value", ">", dataflow::Value(int64_t(6)))
                    .window("ts", 60000, "window_start")
                    .groupBy({"window_start", "key"})
                    .sum("value", true, "value_sum");

  dataflow::StreamingQueryOptions options;
  options.trigger_interval_ms = 50;
  options.max_inflight_batches = 2;
  options.max_queued_partitions = 4;
  options.backpressure_high_watermark = 2;
  options.backpressure_low_watermark = 1;
  options.checkpoint_path = "/tmp/velaria-stream-demo.checkpoint";
  options.execution_mode = dataflow::StreamingExecutionMode::LocalWorkers;
  options.local_workers = 2;
  std::filesystem::remove(options.checkpoint_path);

  auto q = stream.writeStreamToConsole(options);
  q.start();
  size_t batches = q.awaitTermination();

  std::cout << "Processed batches: " << batches << std::endl;
  std::cout << "Progress: " << q.snapshotJson() << std::endl;
  return 0;
}
