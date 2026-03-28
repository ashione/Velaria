#include <chrono>
#include <cstdint>
#include <iostream>

#include "src/dataflow/stream/stream.h"

int main() {
  dataflow::Table batch1;
  batch1.schema = dataflow::Schema({"ts", "value"});
  batch1.rows = {
      {dataflow::Value("2026-03-28T09:00:00"), dataflow::Value(int64_t(10))},
      {dataflow::Value("2026-03-28T09:00:10"), dataflow::Value(int64_t(5))},
  };

  dataflow::Table batch2;
  batch2.schema = dataflow::Schema({"ts", "value"});
  batch2.rows = {
      {dataflow::Value("2026-03-28T09:00:20"), dataflow::Value(int64_t(20))},
      {dataflow::Value("2026-03-28T09:00:30"), dataflow::Value(int64_t(7))},
  };

  auto source = std::make_shared<dataflow::MemoryStreamSource>(std::vector<dataflow::Table>{batch1, batch2});

  auto stream = dataflow::StreamingDataFrame(source)
                    .withColumn("dup_value", "value")
                    .select({"value", "dup_value"})
                    .filter("value", ">", dataflow::Value(int64_t(6)));

  auto q = stream.writeStreamToConsole(100);
  size_t batches = q.awaitTermination();

  std::cout << "Processed batches: " << batches << std::endl;
  return 0;
}
