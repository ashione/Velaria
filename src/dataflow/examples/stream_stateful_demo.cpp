#include <iostream>
#include <vector>

#include "src/dataflow/stream/stream.h"

int main() {
  dataflow::Table batch1;
  batch1.schema = dataflow::Schema({"key", "value"});
  batch1.rows = {
      {dataflow::Value("userA"), dataflow::Value(int64_t(10))},
      {dataflow::Value("userB"), dataflow::Value(int64_t(5))},
  };

  dataflow::Table batch2;
  batch2.schema = dataflow::Schema({"key", "value"});
  batch2.rows = {
      {dataflow::Value("userA"), dataflow::Value(int64_t(3))},
      {dataflow::Value("userB"), dataflow::Value(int64_t(7))},
      {dataflow::Value("userA"), dataflow::Value(int64_t(2))},
  };

  auto source = std::make_shared<dataflow::MemoryStreamSource>(std::vector<dataflow::Table>{batch1, batch2});
  auto state = dataflow::makeMemoryStateStore();

  // Stateful groupBy + sum: 第一次输出  userA=10,userB=5；第二次输出 userA=15,userB=12
  auto stream = dataflow::StreamingDataFrame(source)
                    .withStateStore(state)
                    .groupBy({"key"})
                    .sum("value", true);

  auto q = stream.writeStreamToConsole(100);
  size_t batches = q.awaitTermination();
  std::cout << "Processed batches: " << batches << std::endl;

  return 0;
}
