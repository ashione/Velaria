#pragma once

// Runtime module boundary:
// - This header defines the actor runtime primitives used by the dataflow
//   kernel, including message exchange helpers, in-process coordinator/worker
//   wiring, and task execution collaboration on top of RpcRunner.
// - It is responsible for runtime-side execution semantics and transport-level
//   coordination, not process bootstrap, CLI parsing, or executable runner
//   configuration.

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "src/dataflow/core/execution/table.h"
#include "src/dataflow/experimental/runtime/rpc_runner.h"
#include "src/dataflow/experimental/runtime/rpc_contract.h"

namespace dataflow {

// Wire format:
// - Control name=RunTask, body: task_id=...;status=identity;rows=0
// - Control name=TaskAck, body: task_id=...;status=ok;rows=0
// - Control name=TaskResult, body: task_id=...;status=ok;rows=<n>
// - Control name=TaskError, body: task_id=...;status=error;detail=...
// - Control name=Heartbeat, body: beat=1
// - DataBatch: serialized Table payload for RunTask(TaskId) and TaskResult
// Correlation Id is used as task id binding; message id is per-message monotonic id.

class ActorRuntimeRunner {
 public:
  explicit ActorRuntimeRunner(std::shared_ptr<RpcRunner> runner);

  RpcStatus sendRunTaskEx(uint64_t msg_id,
                          uint64_t corr_id,
                          const std::string& task_id,
                          const std::string& source,
                          const std::string& target,
                          const std::string& op = ActorTaskStatus::kIdentity);

  RpcStatus sendAckEx(uint64_t msg_id,
                      uint64_t corr_id,
                      const std::string& task_id,
                      const std::string& status,
                      const std::string& source,
                      const std::string& target);

  RpcStatus sendResultEx(uint64_t msg_id,
                         uint64_t corr_id,
                         const std::string& task_id,
                         const std::string& source,
                         const std::string& target,
                         const Table& table);

  RpcStatus sendErrorEx(uint64_t msg_id,
                        uint64_t corr_id,
                        const std::string& task_id,
                        const std::string& reason,
                        const std::string& source,
                        const std::string& target);

  RpcStatus sendHeartbeatEx(uint64_t msg_id,
                           uint64_t corr_id,
                           const std::string& source,
                           const std::string& target);

  RpcStatus receiveTaskControlEx(uint64_t corr_id,
                                RpcControlMessage* message,
                                int timeout_ms = 1000);

  RpcStatus receiveAnyControlEx(RpcControlMessage* message,
                               uint64_t* correlation_id,
                               int timeout_ms = 1000);

  RpcStatus receiveTaskDataEx(uint64_t corr_id, Table* table, int timeout_ms = 1000);

  bool sendRunTask(uint64_t msg_id,
                   uint64_t corr_id,
                   const std::string& task_id,
                   const std::string& source,
                   const std::string& target,
                   const std::string& op = "identity");
  bool sendAck(uint64_t msg_id,
               uint64_t corr_id,
               const std::string& task_id,
               const std::string& status,
               const std::string& source,
               const std::string& target);
  bool sendResult(uint64_t msg_id,
                  uint64_t corr_id,
                  const std::string& task_id,
                  const std::string& source,
                  const std::string& target,
                  const Table& table);
  bool sendError(uint64_t msg_id,
                 uint64_t corr_id,
                 const std::string& task_id,
                 const std::string& reason,
                 const std::string& source,
                 const std::string& target);
  bool sendHeartbeat(uint64_t msg_id,
                     uint64_t corr_id,
                     const std::string& source,
                     const std::string& target);

  bool receiveTaskControl(uint64_t corr_id, RpcControlMessage* message, int timeout_ms = 1000);
  bool receiveAnyControl(RpcControlMessage* message, uint64_t* correlation_id = nullptr, int timeout_ms = 1000);
  bool receiveTaskData(uint64_t corr_id, Table* table, int timeout_ms = 1000);

 private:
  std::shared_ptr<RpcRunner> runner_;
};

class ActorWorker {
 public:
  using TableTransform = std::function<Table(const Table&)>;

  explicit ActorWorker(std::shared_ptr<RpcRunner> runner,
                       std::string node_id = "worker",
                       TableTransform transform = TableTransform());

  void start();
  void stop();
  void join();
  bool running() const;

 private:
  void loop();
  Table defaultTransform(const Table& table) const;

  std::shared_ptr<RpcRunner> runner_;
  std::string node_id_;
  TableTransform transform_;
  std::thread thread_;
  bool stop_requested_ = false;
  bool running_ = false;
  size_t task_seq_ = 1;
};

class ActorCoordinator {
 public:
  explicit ActorCoordinator(std::shared_ptr<RpcRunner> runner,
                           std::string source_node = "coordinator",
                           std::string target_node = "worker");

  RpcStatus submitEx(const Table& in, Table* out, int timeout_ms = 1000);
  bool submit(const Table& in, Table* out, int timeout_ms = 1000);

 private:
  std::string nextTaskId();

  std::shared_ptr<RpcRunner> runner_;
  std::string source_node_;
  std::string target_node_;
  size_t task_seq_ = 1;
};

RpcStatus makeInProcessRunnerPairEx(std::shared_ptr<RpcRunner>* coordinator_runner,
                                    std::shared_ptr<RpcRunner>* worker_runner,
                                    const std::string& coordinator_node = "coordinator",
                                    const std::string& worker_node = "worker");

bool makeInProcessRunnerPair(std::shared_ptr<RpcRunner>* coordinator_runner,
                            std::shared_ptr<RpcRunner>* worker_runner,
                            const std::string& coordinator_node = "coordinator",
                            const std::string& worker_node = "worker");

}  // namespace dataflow
