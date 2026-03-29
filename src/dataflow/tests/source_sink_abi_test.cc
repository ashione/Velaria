#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "src/dataflow/api/session.h"
#include "src/dataflow/stream/source_sink_abi.h"
#include "src/dataflow/stream/stream.h"

namespace {

void expect(bool cond, const std::string& msg) {
  if (!cond) throw std::runtime_error(msg);
}

class FakeRuntimeSource : public dataflow::RuntimeSource {
 public:
  explicit FakeRuntimeSource(std::vector<dataflow::Table> batches) : batches_(std::move(batches)) {}

  dataflow::Schema schema() const override {
    if (batches_.empty()) return dataflow::Schema({"value"});
    return batches_.front().schema;
  }

  void open(const dataflow::RuntimeSourceContext& context) override {
    open_context = context;
    opened = true;
    if (opened_from_next_batch) {
      open_backlog_batches.push_back(context.backlog_batches);
      open_inflight_batches.push_back(context.inflight_batches);
    }
  }

  dataflow::SourceStatus nextBatch(dataflow::Table* out, dataflow::SourceBatchToken* token) override {
    if (index_ >= batches_.size()) {
      return dataflow::SourceStatus::EndOfStream;
    }
    *out = batches_[index_];
    token->value = "batch-" + std::to_string(index_);
    ++index_;
    return dataflow::SourceStatus::Ok;
  }

  void ack(const dataflow::SourceBatchToken& token) override { acked_tokens.push_back(token.value); }

  void checkpoint(const dataflow::RuntimeCheckpointMarker& marker) override {
    checkpoints.push_back(marker.source_offset + "#" + std::to_string(marker.batches_processed));
  }

  void close() override { closed = true; }

  dataflow::RuntimeSourceContext open_context;
  bool opened = false;
  bool closed = false;
  bool opened_from_next_batch = false;
  std::vector<std::string> acked_tokens;
  std::vector<std::string> checkpoints;
  std::vector<std::size_t> open_backlog_batches;
  std::vector<std::size_t> open_inflight_batches;

 private:
  std::vector<dataflow::Table> batches_;
  size_t index_ = 0;
};

class FakeRuntimeSink : public dataflow::RuntimeSink {
 public:
  void open(const dataflow::RuntimeSinkContext& context) override {
    open_context = context;
    opened = true;
  }

  dataflow::SinkStatus write(const dataflow::Table& batch) override {
    writes.push_back(batch.rows.size());
    return dataflow::SinkStatus::Ok;
  }

  void checkpoint(const dataflow::RuntimeCheckpointMarker& marker) override {
    checkpoints.push_back(marker.source_offset + "#" + std::to_string(marker.batches_processed));
  }

  void close() override { closed = true; }

  dataflow::RuntimeSinkContext open_context;
  bool opened = false;
  bool closed = false;
  std::vector<size_t> writes;
  std::vector<std::string> checkpoints;
};

}  // namespace

int main() {
  try {
    dataflow::Table batch;
    batch.schema = dataflow::Schema({"value"});
    batch.rows = {{dataflow::Value(int64_t(1))}, {dataflow::Value(int64_t(2))}};

    auto runtime_source = std::make_shared<FakeRuntimeSource>(std::vector<dataflow::Table>{batch, batch});
    auto runtime_sink = std::make_shared<FakeRuntimeSink>();

    dataflow::StreamingQueryOptions options;
    options.trigger_interval_ms = 0;
    options.checkpoint_path = "/tmp/velaria-runtime-abi-checkpoint.txt";
    options.max_inflight_batches = 1;

    auto source = dataflow::makeRuntimeSourceAdapter(runtime_source);
    auto sink = dataflow::makeRuntimeSinkAdapter(runtime_sink);

    auto& session = dataflow::DataflowSession::builder();
    auto query = session.readStream(source).filter("value", ">", dataflow::Value(int64_t(0))).writeStream(sink, options);
    query.start();
    const size_t processed = query.awaitTermination();

    expect(processed == 2, "runtime ABI query should process all batches");
    expect(runtime_source->opened, "runtime source should be opened");
    expect(runtime_sink->opened, "runtime sink should be opened");
    expect(runtime_source->closed, "runtime source should be closed");
    expect(runtime_sink->closed, "runtime sink should be closed");

    expect(runtime_source->open_context.query_id.rfind("stream-query-", 0) == 0,
           "runtime source should receive query_id");
    expect(runtime_source->open_context.checkpoint_path == options.checkpoint_path,
           "runtime source should receive checkpoint path");
    expect(runtime_source->open_context.max_inflight_batches == options.max_inflight_batches,
           "runtime source should receive max inflight batches");
    expect(runtime_sink->open_context.execution_mode == "single-process",
           "runtime sink should receive requested execution mode");
    expect(runtime_sink->open_context.transport_mode == "inproc",
           "runtime sink should receive transport mode");

    expect(runtime_source->acked_tokens.size() == 2,
           "runtime source should ack per checkpointed batch");
    expect(runtime_source->acked_tokens[0] == "batch-0",
           "runtime source should ack the first source token");
    expect(runtime_source->acked_tokens[1] == "batch-1",
           "runtime source should ack the second source token");
    expect(runtime_sink->writes.size() == 2, "runtime sink should receive two writes");
    expect(runtime_sink->checkpoints.size() == 2, "runtime sink should receive checkpoints");

    std::cout << "[test] source sink abi adapter ok" << std::endl;
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "[test] source sink abi adapter failed: " << ex.what() << std::endl;
    return 1;
  }
}
