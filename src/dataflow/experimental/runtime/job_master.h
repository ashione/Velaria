#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "src/dataflow/core/execution/table.h"
#include "src/dataflow/core/logical/planner/plan.h"
#include "src/dataflow/core/execution/runtime/executor.h"

namespace dataflow {

using JobId = std::string;
using ChainId = std::string;
using TaskId = std::string;
using Attempt = uint32_t;

struct JobRecord;

struct RemoteTaskSpec {
  JobId job_id;
  ChainId chain_id;
  TaskId task_id;
  Attempt attempt = 0;
  std::string payload;
};

struct RemoteTaskCompletion {
  JobId job_id;
  ChainId chain_id;
  TaskId task_id;
  Attempt attempt = 0;
  bool ok = false;
  size_t output_rows = 0;
  bool has_result_table = false;
  Table result_table;
  std::string payload;
  std::string error_message;
  std::string worker_id;
};

enum class TaskState {
  Created,
  Queued,
  Scheduled,
  Running,
  HeartbeatMiss,
  Retrying,
  Succeeded,
  Failed,
  Cancelled,
};

enum class ChainState {
  Created,
  WaitingForDeps,
  Ready,
  Scheduled,
  Running,
  Retrying,
  Succeeded,
  Failed,
  Cancelled,
};

enum class JobState {
  Created,
  Queued,
  Running,
  Retrying,
  Succeeded,
  Failed,
  Cancelled,
  Timeout,
};

struct TaskStateRecord {
  TaskId task_id;
  ChainId chain_id;
  JobId job_id;
  PlanKind plan_kind = PlanKind::Source;
  Attempt attempt = 0;
  Attempt max_attempts = 3;
  TaskState state = TaskState::Created;
  std::string worker_id;
  std::string lease_id;
  std::string operator_id;
  std::chrono::steady_clock::time_point created_at;
  std::chrono::steady_clock::time_point scheduled_at;
  std::chrono::steady_clock::time_point running_at;
  std::chrono::steady_clock::time_point finished_at;
  std::chrono::steady_clock::time_point last_heartbeat_at;
  uint64_t heartbeat_seq = 0;
  uint32_t heartbeat_miss_count = 0;
  size_t input_rows = 0;
  size_t output_rows = 0;
  std::string error_code;
  std::string error_message;
};

struct ChainStateRecord {
  ChainId chain_id;
  JobId job_id;
  ChainState state = ChainState::Created;
  std::vector<TaskId> task_ids;
  std::vector<ChainId> upstream_chain_ids;
  std::vector<ChainId> downstream_chain_ids;
  size_t total_tasks = 0;
  size_t done_tasks = 0;
  size_t failed_tasks = 0;
  size_t running_tasks = 0;
  std::chrono::steady_clock::time_point created_at;
  std::chrono::steady_clock::time_point started_at;
  std::chrono::steady_clock::time_point finished_at;
  Attempt retry_budget = 1;
  Attempt retry_round = 0;
  std::string fail_reason;
};

struct JobStateRecord {
  JobId job_id;
  JobState state = JobState::Created;
  std::vector<ChainId> chain_ids;
  size_t total_chains = 0;
  size_t succeeded_chains = 0;
  size_t failed_chains = 0;
  std::chrono::steady_clock::time_point submitted_at;
  std::chrono::steady_clock::time_point started_at;
  std::chrono::steady_clock::time_point finished_at;
  std::string owner;
  std::string error_summary;
  bool has_result = false;
  Table result;
};

struct HeartbeatConfig {
  uint32_t heartbeat_interval_ms = 1000;
  uint32_t heartbeat_timeout_ms = 5000;
  uint32_t heartbeat_miss_threshold = 3;
};

struct RetryPolicy {
  Attempt max_attempts = 3;
  uint64_t base_backoff_ms = 200;
  uint64_t max_backoff_ms = 3000;
  bool retry_on_timeout = true;
  bool retry_on_transient_error = true;
};

struct ExecutionOptions {
  uint32_t chain_parallelism = 1;
  HeartbeatConfig heartbeat;
  RetryPolicy retry;
  uint32_t max_chain_parallelism() const { return chain_parallelism > 0 ? chain_parallelism : 1; }
};

struct TaskHeartbeat {
  TaskId task_id;
  ChainId chain_id;
  JobId job_id;
  Attempt attempt = 0;
  std::string worker_id;
  uint64_t heartbeat_seq = 0;
  std::chrono::steady_clock::time_point at = std::chrono::steady_clock::now();
  size_t input_rows = 0;
  size_t output_rows = 0;
};

struct JobHandleSnapshot {
  JobId job_id;
  JobState state = JobState::Created;
  std::string state_message;
  std::string status_code;
  bool has_result = false;
  Table result;
};

class DataflowJobHandle {
 public:
  DataflowJobHandle() = default;
  explicit DataflowJobHandle(JobId id) : id_(std::move(id)) {}

  const JobId& id() const { return id_; }
  JobState state() const;
  bool done() const;
  bool cancel() const;
  JobStateRecord queryJob() const;
  ChainStateRecord queryChain(const ChainId& chainId) const;
  TaskStateRecord queryTask(const TaskId& taskId) const;
  std::string snapshotJson() const;
  JobHandleSnapshot wait(std::chrono::milliseconds timeout = std::chrono::milliseconds(0)) const;
  Table result(std::chrono::milliseconds timeout = std::chrono::milliseconds(0)) const;

 private:
  JobId id_;
};

class JobMaster {
 public:
  static JobMaster& instance();

  DataflowJobHandle submit(const PlanNodePtr& root, std::shared_ptr<Executor> executor,
                          const ExecutionOptions& options = {});
  DataflowJobHandle submitRemote(const std::string& payload,
                                const ExecutionOptions& options = {});
  JobStateRecord queryJob(const JobId& id) const;
  ChainStateRecord queryChain(const JobId& id, const ChainId& chain_id) const;
  TaskStateRecord queryTask(const JobId& id, const TaskId& task_id) const;
  std::string snapshotJson(const JobId& id) const;
  void heartbeat(const TaskHeartbeat& heartbeat);
  bool pollRemoteTask(RemoteTaskSpec* task, std::chrono::milliseconds timeout);
  void completeRemoteTask(const RemoteTaskCompletion& completion);
  bool cancel(const JobId& id);
  void shutdown();

 private:
  JobMaster() = default;
  JobMaster(const JobMaster&) = delete;
  JobMaster& operator=(const JobMaster&) = delete;

  ChainId buildChains(const PlanNodePtr& plan, const JobId& job_id,
                     const std::shared_ptr<JobRecord>& job);
  void runJob(const std::shared_ptr<JobRecord>& job);
  void runChain(const std::shared_ptr<JobRecord>& job, const ChainId& chain_id);

  mutable std::mutex mu_;
  std::unordered_map<JobId, std::shared_ptr<JobRecord>> jobs_;
  std::deque<RemoteTaskSpec> pending_remote_tasks_;
  std::condition_variable remote_cv_;
  uint64_t job_seq_ = 0;
};

}  // namespace dataflow
