#include "src/dataflow/runtime/actor_runtime.h"

#include <string>

namespace dataflow {

namespace {

RpcStatus decodeControlFrame(const RpcFrame& frame, RpcControlMessage* out_message) {
  if (!out_message) {
    return makeRpcStatus(RpcStatusCode::ArgumentError, "output control message is null");
  }
  const auto* codec = RpcSerializerRegistry::instance().find(frame.header.codec_id);
  if (!codec) {
    return makeRpcStatus(RpcStatusCode::CodecMissing, "codec missing: " + frame.header.codec_id);
  }
  if (!codec->deserialize(frame.header, frame.payload, out_message)) {
    return makeRpcStatus(RpcStatusCode::DeserializeError, "control deserialize failed");
  }
  return rpcOk();
}

}  // namespace

ActorRuntimeRunner::ActorRuntimeRunner(std::shared_ptr<RpcRunner> runner)
    : runner_(std::move(runner)) {}

RpcStatus ActorRuntimeRunner::sendRunTaskEx(uint64_t msg_id,
                                           uint64_t corr_id,
                                           const std::string& task_id,
                                           const std::string& source,
                                           const std::string& target,
                                           const std::string& op) {
  if (!runner_) {
    return makeRpcStatus(RpcStatusCode::TransportClosed, "runner is null");
  }
  ActorTaskMessage msg;
  msg.task_id = task_id;
  msg.status = op.empty() ? ActorTaskStatus::kIdentity : op;
  return runner_->sendControlEx(msg_id, corr_id, source, target, RpcMessageType::Control,
                               ActorTaskCommand::kRunTask, msg.encode());
}

RpcStatus ActorRuntimeRunner::sendAckEx(uint64_t msg_id,
                                       uint64_t corr_id,
                                       const std::string& task_id,
                                       const std::string& status,
                                       const std::string& source,
                                       const std::string& target) {
  if (!runner_) {
    return makeRpcStatus(RpcStatusCode::TransportClosed, "runner is null");
  }
  ActorTaskMessage msg;
  msg.task_id = task_id;
  msg.status = status.empty() ? ActorTaskStatus::kOk : status;
  return runner_->sendControlEx(msg_id, corr_id, source, target, RpcMessageType::Control,
                               ActorTaskCommand::kTaskAck, msg.encode());
}

RpcStatus ActorRuntimeRunner::sendResultEx(uint64_t msg_id,
                                          uint64_t corr_id,
                                          const std::string& task_id,
                                          const std::string& source,
                                          const std::string& target,
                                          const Table& table) {
  if (!runner_) {
    return makeRpcStatus(RpcStatusCode::TransportClosed, "runner is null");
  }
  ActorTaskMessage msg;
  msg.task_id = task_id;
  msg.status = ActorTaskStatus::kOk;
  msg.rows = table.rowCount();
  const auto control_status =
      runner_->sendControlEx(msg_id, corr_id, source, target, RpcMessageType::Control,
                            ActorTaskCommand::kTaskResult, msg.encode());
  if (!control_status.ok()) return control_status;
  return runner_->sendDataBatchEx(msg_id + 1, corr_id, source, target, table);
}

RpcStatus ActorRuntimeRunner::sendErrorEx(uint64_t msg_id,
                                         uint64_t corr_id,
                                         const std::string& task_id,
                                         const std::string& reason,
                                         const std::string& source,
                                         const std::string& target) {
  if (!runner_) {
    return makeRpcStatus(RpcStatusCode::TransportClosed, "runner is null");
  }
  ActorTaskMessage msg;
  msg.task_id = task_id;
  msg.status = ActorTaskStatus::kError;
  msg.detail = reason;
  return runner_->sendControlEx(msg_id, corr_id, source, target, RpcMessageType::Error,
                               ActorTaskCommand::kTaskError, msg.encode());
}

RpcStatus ActorRuntimeRunner::sendHeartbeatEx(uint64_t msg_id,
                                             uint64_t corr_id,
                                             const std::string& source,
                                             const std::string& target) {
  if (!runner_) {
    return makeRpcStatus(RpcStatusCode::TransportClosed, "runner is null");
  }
  return runner_->sendControlEx(msg_id, corr_id, source, target, RpcMessageType::Heartbeat,
                               ActorTaskCommand::kHeartbeat, "beat=1");
}

RpcStatus ActorRuntimeRunner::receiveTaskControlEx(uint64_t corr_id,
                                                  RpcControlMessage* message,
                                                  int timeout_ms) {
  if (!runner_) {
    return makeRpcStatus(RpcStatusCode::TransportClosed, "runner is null");
  }
  return runner_->receiveControlEx(corr_id, message, timeout_ms);
}

RpcStatus ActorRuntimeRunner::receiveAnyControlEx(RpcControlMessage* message,
                                                  uint64_t* correlation_id,
                                                  int timeout_ms) {
  if (!runner_) {
    return makeRpcStatus(RpcStatusCode::TransportClosed, "runner is null");
  }
  while (true) {
    RpcFrame frame;
    const auto frame_status = runner_->receiveFrameEx(&frame, timeout_ms);
    if (!frame_status.ok()) return frame_status;
    if (frame.header.type != RpcMessageType::Control) continue;
    RpcControlMessage decoded;
    const auto decode_status = decodeControlFrame(frame, &decoded);
    if (!decode_status.ok()) return decode_status;
    if (message) *message = decoded;
    if (correlation_id) {
      *correlation_id = frame.header.correlation_id;
    }
    return rpcOk();
  }
}

RpcStatus ActorRuntimeRunner::receiveTaskDataEx(uint64_t corr_id, Table* table, int timeout_ms) {
  if (!runner_) {
    return makeRpcStatus(RpcStatusCode::TransportClosed, "runner is null");
  }
  return runner_->receiveDataBatchEx(corr_id, table, timeout_ms);
}

bool ActorRuntimeRunner::sendRunTask(uint64_t msg_id,
                                    uint64_t corr_id,
                                    const std::string& task_id,
                                    const std::string& source,
                                    const std::string& target,
                                    const std::string& op) {
  return rpcStatusToBool(sendRunTaskEx(msg_id, corr_id, task_id, source, target, op));
}

bool ActorRuntimeRunner::sendAck(uint64_t msg_id,
                                uint64_t corr_id,
                                const std::string& task_id,
                                const std::string& status,
                                const std::string& source,
                                const std::string& target) {
  return rpcStatusToBool(sendAckEx(msg_id, corr_id, task_id, status, source, target));
}

bool ActorRuntimeRunner::sendResult(uint64_t msg_id,
                                   uint64_t corr_id,
                                   const std::string& task_id,
                                   const std::string& source,
                                   const std::string& target,
                                   const Table& table) {
  return rpcStatusToBool(sendResultEx(msg_id, corr_id, task_id, source, target, table));
}

bool ActorRuntimeRunner::sendError(uint64_t msg_id,
                                  uint64_t corr_id,
                                  const std::string& task_id,
                                  const std::string& reason,
                                  const std::string& source,
                                  const std::string& target) {
  return rpcStatusToBool(sendErrorEx(msg_id, corr_id, task_id, reason, source, target));
}

bool ActorRuntimeRunner::sendHeartbeat(uint64_t msg_id,
                                      uint64_t corr_id,
                                      const std::string& source,
                                      const std::string& target) {
  return rpcStatusToBool(sendHeartbeatEx(msg_id, corr_id, source, target));
}

bool ActorRuntimeRunner::receiveTaskControl(uint64_t corr_id, RpcControlMessage* message, int timeout_ms) {
  return rpcStatusToBool(receiveTaskControlEx(corr_id, message, timeout_ms));
}

bool ActorRuntimeRunner::receiveAnyControl(RpcControlMessage* message,
                                          uint64_t* correlation_id,
                                          int timeout_ms) {
  return rpcStatusToBool(receiveAnyControlEx(message, correlation_id, timeout_ms));
}

bool ActorRuntimeRunner::receiveTaskData(uint64_t corr_id, Table* table, int timeout_ms) {
  return rpcStatusToBool(receiveTaskDataEx(corr_id, table, timeout_ms));
}

ActorWorker::ActorWorker(std::shared_ptr<RpcRunner> runner, std::string node_id, TableTransform transform)
    : runner_(std::move(runner)), node_id_(std::move(node_id)), transform_(std::move(transform)) {}

void ActorWorker::start() {
  if (thread_.joinable()) return;
  stop_requested_ = false;
  thread_ = std::thread([this]() { loop(); });
}

void ActorWorker::stop() {
  stop_requested_ = true;
}

void ActorWorker::join() {
  if (thread_.joinable()) {
    thread_.join();
  }
}

bool ActorWorker::running() const {
  return running_ && !stop_requested_;
}

Table ActorWorker::defaultTransform(const Table& table) const {
  return table;
}

void ActorWorker::loop() {
  running_ = true;
  ActorRuntimeRunner rt(runner_);
  while (!stop_requested_) {
    RpcControlMessage control;
    uint64_t corr_id = 0;
    if (!rt.receiveAnyControl(&control, &corr_id, 100)) {
      continue;
    }
    const auto msg = ActorTaskMessage::decode(control.body);
    if (control.name != ActorTaskCommand::kRunTask || msg.task_id.empty()) {
      continue;
    }

    Table input;
    const auto data_status = rt.receiveTaskDataEx(corr_id, &input, 500);
    if (!data_status.ok()) {
      rt.sendError(runner_->nextMessageId(), corr_id, msg.task_id, data_status.reason,
                   node_id_, "coordinator");
      continue;
    }

    Table out = transform_ ? transform_(input) : defaultTransform(input);
    const auto msg_id = runner_->nextMessageId();
    rt.sendAck(msg_id, corr_id, msg.task_id, ActorTaskStatus::kOk, node_id_, "coordinator");
    rt.sendResult(msg_id + 1, corr_id, msg.task_id, node_id_, "coordinator", out);
  }
  running_ = false;
}

ActorCoordinator::ActorCoordinator(std::shared_ptr<RpcRunner> runner,
                                   std::string source_node,
                                   std::string target_node)
    : runner_(std::move(runner)), source_node_(std::move(source_node)), target_node_(std::move(target_node)) {}

std::string ActorCoordinator::nextTaskId() {
  const std::string task_id = std::to_string(task_seq_++);
  return "task_" + task_id;
}

RpcStatus ActorCoordinator::submitEx(const Table& in, Table* out, int timeout_ms) {
  if (!runner_) {
    return makeRpcStatus(RpcStatusCode::TransportClosed, "runner is null");
  }
  if (!out) {
    return makeRpcStatus(RpcStatusCode::ArgumentError, "output table is null");
  }
  const auto task_id = nextTaskId();
  const auto msg_id = runner_->nextMessageId();
  const auto corr_id = runner_->nextCorrelationId();
  ActorRuntimeRunner rt(runner_);

  const auto task_status =
      rt.sendRunTaskEx(msg_id, corr_id, task_id, source_node_, target_node_);
  if (!task_status.ok()) {
    return task_status;
  }
  const auto input_status =
      runner_->sendDataBatchEx(msg_id + 1, corr_id, source_node_, target_node_, in);
  if (!input_status.ok()) {
    return input_status;
  }

  RpcControlMessage ack;
  const auto ack_status = rt.receiveTaskControlEx(corr_id, &ack, timeout_ms);
  if (!ack_status.ok()) {
    return ack_status;
  }
  const auto ack_msg = ActorTaskMessage::decode(ack.body);
  if (ack.name != ActorTaskCommand::kTaskAck || ack_msg.task_id != task_id ||
      ack_msg.status != ActorTaskStatus::kOk) {
    return makeRpcStatus(RpcStatusCode::TypeMismatch, "unexpected task ack payload");
  }
  const auto result_status = rt.receiveTaskDataEx(corr_id, out, timeout_ms);
  if (!result_status.ok()) {
    return result_status;
  }
  return rpcOk();
}

bool ActorCoordinator::submit(const Table& in, Table* out, int timeout_ms) {
  return rpcStatusToBool(submitEx(in, out, timeout_ms));
}

RpcStatus makeInProcessRunnerPairEx(std::shared_ptr<RpcRunner>* coordinator_runner,
                                    std::shared_ptr<RpcRunner>* worker_runner,
                                    const std::string& coordinator_node,
                                    const std::string& worker_node) {
  if (!coordinator_runner || !worker_runner) {
    return makeRpcStatus(RpcStatusCode::ArgumentError, "runner output is null");
  }
  auto links = InMemoryByteTransport::makePipePair(coordinator_node, worker_node);
  auto c = std::make_shared<RpcRunner>(links.first, std::make_unique<LengthPrefixedFrameCodec>(),
                                       RpcRunnerCodecConfig(), coordinator_node);
  auto w = std::make_shared<RpcRunner>(links.second, std::make_unique<LengthPrefixedFrameCodec>(),
                                       RpcRunnerCodecConfig(), worker_node);
  *coordinator_runner = c;
  *worker_runner = w;
  return rpcOk();
}

bool makeInProcessRunnerPair(std::shared_ptr<RpcRunner>* coordinator_runner,
                             std::shared_ptr<RpcRunner>* worker_runner,
                             const std::string& coordinator_node,
                             const std::string& worker_node) {
  return rpcStatusToBool(makeInProcessRunnerPairEx(coordinator_runner, worker_runner,
                                                   coordinator_node, worker_node));
}

}  // namespace dataflow
