#include <iostream>
#include <filesystem>
#include <vector>

#include "src/dataflow/core/contract/api/session.h"
#include "src/dataflow/core/execution/stream/stream.h"

int main() {
  dataflow::DataflowSession& session = dataflow::DataflowSession::builder();

  dataflow::Table batch1;
  batch1.schema = dataflow::Schema({"ts", "key", "value"});
  batch1.rows = {
      {dataflow::Value("2026-03-28T09:00:00"), dataflow::Value("userA"),
       dataflow::Value(int64_t(10))},
      {dataflow::Value("2026-03-28T09:00:05"), dataflow::Value("userB"),
       dataflow::Value(int64_t(5))},
  };

  dataflow::Table batch2;
  batch2.schema = dataflow::Schema({"ts", "key", "value"});
  batch2.rows = {
      {dataflow::Value("2026-03-28T09:00:20"), dataflow::Value("userA"),
       dataflow::Value(int64_t(3))},
      {dataflow::Value("2026-03-28T09:00:35"), dataflow::Value("userB"),
       dataflow::Value(int64_t(7))},
      {dataflow::Value("2026-03-28T09:00:45"), dataflow::Value("userA"),
       dataflow::Value(int64_t(2))},
  };

  auto source =
      std::make_shared<dataflow::MemoryStreamSource>(std::vector<dataflow::Table>{batch1, batch2});
  auto state = dataflow::makeMemoryStateStore();

  auto stream = session.readStream(source)
                    .withStateStore(state)
                    .window("ts", 60000, "window_start")
                    .groupBy({"window_start", "key"})
                    .count(true, "event_count")
                    .filter("event_count", ">", dataflow::Value(int64_t(0)));

  dataflow::StreamingQueryOptions options;
  options.trigger_interval_ms = 20;
  options.checkpoint_path = "/tmp/velaria-stream-stateful.checkpoint";
  std::filesystem::remove(options.checkpoint_path);

  auto q = stream.writeStream(
      std::make_shared<dataflow::FileAppendStreamSink>("/tmp/velaria-stream-stateful.csv"),
      options);
  q.start();
  size_t batches = q.awaitTermination();
  std::cout << "Processed batches: " << batches << std::endl;
  std::cout << "Progress: " << q.snapshotJson() << std::endl;

  return 0;
}
