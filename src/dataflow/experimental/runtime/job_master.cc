#include "src/dataflow/experimental/runtime/job_master.h"

#include <algorithm>
#include <chrono>
#include <deque>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <vector>

#include "src/dataflow/core/execution/runtime/observability.h"

namespace dataflow {

using Clock = std::chrono::steady_clock;

struct ChainRuntime {
  ChainStateRecord state;
  PlanNodePtr execute_root;
};

struct JobRecord {
  JobId job_id;
  ChainId root_chain_id;
  ExecutionOptions options;
  PlanNodePtr root_plan;
  std::shared_ptr<Executor> executor;

  JobStateRecord job_state;
  std::unordered_map<ChainId, ChainRuntime> chains;
  std::unordered_map<TaskId, TaskStateRecord> tasks;

  uint64_t next_chain_seq = 0;
  uint64_t next_task_seq = 0;
  size_t active_chain_count = 0;
  bool cancelled = false;
  bool done = false;
  Table final_result;
  bool remote_mode = false;
  bool remote_task_dispatched = false;
  bool remote_task_completed = false;
  bool remote_has_result_table = false;
  std::string remote_payload;
  Table remote_result_table;

  mutable std::mutex mtx;
  std::condition_variable cv;
};

namespace {

void materializeResultRowsIfPresent(bool has_result, Table* result) {
  if (!has_result || result == nullptr) {
    return;
  }
  materializeRows(result);
}

bool terminal(TaskState state) {
  return state == TaskState::Succeeded || state == TaskState::Failed || state == TaskState::Cancelled;
}

bool terminal(ChainState state) {
  return state == ChainState::Succeeded || state == ChainState::Failed || state == ChainState::Cancelled;
}

bool terminal(JobState state) {
  return state == JobState::Succeeded || state == JobState::Failed || state == JobState::Cancelled ||
         state == JobState::Timeout;
}

const char* toString(TaskState state) {
  switch (state) {
    case TaskState::Created: return "created";
    case TaskState::Queued: return "queued";
    case TaskState::Scheduled: return "scheduled";
    case TaskState::Running: return "running";
    case TaskState::HeartbeatMiss: return "heartbeat_miss";
    case TaskState::Retrying: return "retrying";
    case TaskState::Succeeded: return "succeeded";
    case TaskState::Failed: return "failed";
    case TaskState::Cancelled: return "cancelled";
  }
  return "unknown";
}

const char* toString(ChainState state) {
  switch (state) {
    case ChainState::Created: return "created";
    case ChainState::WaitingForDeps: return "waiting_for_deps";
    case ChainState::Ready: return "ready";
    case ChainState::Scheduled: return "scheduled";
    case ChainState::Running: return "running";
    case ChainState::Retrying: return "retrying";
    case ChainState::Succeeded: return "succeeded";
    case ChainState::Failed: return "failed";
    case ChainState::Cancelled: return "cancelled";
  }
  return "unknown";
}

const char* toString(JobState state) {
  switch (state) {
    case JobState::Created: return "created";
    case JobState::Queued: return "queued";
    case JobState::Running: return "running";
    case JobState::Retrying: return "retrying";
    case JobState::Succeeded: return "succeeded";
    case JobState::Failed: return "failed";
    case JobState::Cancelled: return "cancelled";
    case JobState::Timeout: return "timeout";
  }
  return "unknown";
}

const char* taskStatusCode(TaskState state) {
  switch (state) {
    case TaskState::Created: return "TASK_CREATED";
    case TaskState::Queued: return "TASK_QUEUED";
    case TaskState::Scheduled: return "TASK_SCHEDULED";
    case TaskState::Running: return "TASK_RUNNING";
    case TaskState::HeartbeatMiss: return "TASK_HEARTBEAT_MISS";
    case TaskState::Retrying: return "TASK_RETRYING";
    case TaskState::Succeeded: return "TASK_SUCCEEDED";
    case TaskState::Failed: return "TASK_FAILED";
    case TaskState::Cancelled: return "TASK_CANCELLED";
  }
  return "TASK_UNKNOWN";
}

const char* chainStatusCode(ChainState state) {
  switch (state) {
    case ChainState::Created: return "CHAIN_CREATED";
    case ChainState::WaitingForDeps: return "CHAIN_WAITING_FOR_DEPS";
    case ChainState::Ready: return "CHAIN_READY";
    case ChainState::Scheduled: return "CHAIN_SCHEDULED";
    case ChainState::Running: return "CHAIN_RUNNING";
    case ChainState::Retrying: return "CHAIN_RETRYING";
    case ChainState::Succeeded: return "CHAIN_SUCCEEDED";
    case ChainState::Failed: return "CHAIN_FAILED";
    case ChainState::Cancelled: return "CHAIN_CANCELLED";
  }
  return "CHAIN_UNKNOWN";
}

const char* jobStatusCode(JobState state) {
  switch (state) {
    case JobState::Created: return "JOB_CREATED";
    case JobState::Queued: return "JOB_QUEUED";
    case JobState::Running: return "JOB_RUNNING";
    case JobState::Retrying: return "JOB_RETRYING";
    case JobState::Succeeded: return "JOB_SUCCEEDED";
    case JobState::Failed: return "JOB_FAILED";
    case JobState::Cancelled: return "JOB_CANCELLED";
    case JobState::Timeout: return "JOB_TIMEOUT";
  }
  return "JOB_UNKNOWN";
}

std::string serializeTaskSnapshot(const TaskStateRecord& task) {
  using namespace observability;
  return object({
      field("task_id", task.task_id),
      field("job_id", task.job_id),
      field("chain_id", task.chain_id),
      field("plan_kind", static_cast<int>(task.plan_kind)),
      field("state", toString(task.state)),
      field("status_code", task.error_code.empty() ? taskStatusCode(task.state) : task.error_code),
      field("attempt", task.attempt),
      field("max_attempts", task.max_attempts),
      field("worker_id", task.worker_id),
      field("heartbeat_seq", task.heartbeat_seq),
      field("heartbeat_miss_count", task.heartbeat_miss_count),
      field("input_rows", task.input_rows),
      field("output_rows", task.output_rows),
      field("error_message", task.error_message),
      field("created_at_ms", epochMillis(task.created_at)),
      field("scheduled_at_ms", epochMillis(task.scheduled_at)),
      field("running_at_ms", epochMillis(task.running_at)),
      field("finished_at_ms", epochMillis(task.finished_at)),
      field("last_heartbeat_at_ms", epochMillis(task.last_heartbeat_at)),
  });
}

std::string serializeChainSnapshot(const ChainStateRecord& chain) {
  using namespace observability;
  return object({
      field("chain_id", chain.chain_id),
      field("job_id", chain.job_id),
      field("state", toString(chain.state)),
      field("status_code", chainStatusCode(chain.state)),
      field("task_ids", arrayFromStrings(chain.task_ids), true),
      field("upstream_chain_ids", arrayFromStrings(chain.upstream_chain_ids), true),
      field("downstream_chain_ids", arrayFromStrings(chain.downstream_chain_ids), true),
      field("total_tasks", chain.total_tasks),
      field("done_tasks", chain.done_tasks),
      field("failed_tasks", chain.failed_tasks),
      field("running_tasks", chain.running_tasks),
      field("retry_budget", chain.retry_budget),
      field("retry_round", chain.retry_round),
      field("fail_reason", chain.fail_reason),
      field("created_at_ms", epochMillis(chain.created_at)),
      field("started_at_ms", epochMillis(chain.started_at)),
      field("finished_at_ms", epochMillis(chain.finished_at)),
  });
}

std::string serializeJobSnapshot(const JobStateRecord& job_state,
                                 const std::unordered_map<ChainId, ChainRuntime>& chains,
                                 const std::unordered_map<TaskId, TaskStateRecord>& tasks) {
  using namespace observability;
  std::vector<std::string> chain_json;
  for (const auto& chain_id : job_state.chain_ids) {
    auto it = chains.find(chain_id);
    if (it != chains.end()) chain_json.push_back(serializeChainSnapshot(it->second.state));
  }
  std::vector<std::string> task_json;
  for (const auto& item : tasks) task_json.push_back(serializeTaskSnapshot(item.second));
  return object({
      field("job_id", job_state.job_id),
      field("state", toString(job_state.state)),
      field("status_code", jobStatusCode(job_state.state)),
      field("owner", job_state.owner),
      field("error_summary", job_state.error_summary),
      field("total_chains", job_state.total_chains),
      field("succeeded_chains", job_state.succeeded_chains),
      field("failed_chains", job_state.failed_chains),
      field("has_result", job_state.has_result),
      field("submitted_at_ms", epochMillis(job_state.submitted_at)),
      field("started_at_ms", epochMillis(job_state.started_at)),
      field("finished_at_ms", epochMillis(job_state.finished_at)),
      field("chains", array(chain_json), true),
      field("tasks", array(task_json), true),
  });
}

void emitJobEvent(const std::string& event, const JobRecord& job) {
  using namespace observability;
  std::cout << object({
      field("component", "job_master"),
      field("event", event),
      field("job_id", job.job_id),
      field("state", toString(job.job_state.state)),
      field("status_code", jobStatusCode(job.job_state.state)),
      field("snapshot", serializeJobSnapshot(job.job_state, job.chains, job.tasks), true),
  }) << std::endl;
}

ChainId makeChainId(JobRecord& job) {
  return job.job_id + ":chain_" + std::to_string(++job.next_chain_seq);
}

TaskId makeTaskId(JobRecord& job) {
  return job.job_id + ":task_" + std::to_string(++job.next_task_seq);
}

uint64_t backoffMs(Attempt attempt, const RetryPolicy& policy) {
  if (attempt == 0) return 0;
  uint64_t factor = 1ULL << std::min<Attempt>(attempt, 10);
  return std::min(policy.base_backoff_ms * factor, policy.max_backoff_ms);
}

PlanNodePtr unaryChild(const PlanNodePtr& node) {
  if (!node) return nullptr;
  switch (node->kind) {
    case PlanKind::Select:
      return static_cast<const SelectPlan*>(node.get())->child;
    case PlanKind::Filter:
      return static_cast<const FilterPlan*>(node.get())->child;
    case PlanKind::WithColumn:
      return static_cast<const WithColumnPlan*>(node.get())->child;
    case PlanKind::Drop:
      return static_cast<const DropPlan*>(node.get())->child;
    case PlanKind::Limit:
      return static_cast<const LimitPlan*>(node.get())->child;
    case PlanKind::Aggregate:
      return static_cast<const AggregatePlan*>(node.get())->child;
    case PlanKind::GroupBySum:
      return static_cast<const GroupBySumPlan*>(node.get())->child;
    default:
      return nullptr;
  }
}

TaskStateRecord makeTaskRecord(TaskId task_id, const JobId& job_id, const ChainId& chain_id, PlanKind kind,
                              Attempt max_attempts) {
  TaskStateRecord t;
  t.task_id = task_id;
  t.job_id = job_id;
  t.chain_id = chain_id;
  t.plan_kind = kind;
  t.max_attempts = max_attempts;
  t.state = TaskState::Created;
  t.created_at = Clock::now();
  return t;
}

void updateTaskState(JobRecord& job, const TaskId& task_id, TaskState state, const std::string& msg = "") {
  const auto it = job.tasks.find(task_id);
  if (it == job.tasks.end()) {
    return;
  }
  auto& task = it->second;
  task.state = state;
  task.error_code = taskStatusCode(state);
  if (!msg.empty()) {
    task.error_message = msg;
  }
  if (state == TaskState::Scheduled) {
    task.scheduled_at = Clock::now();
  } else if (state == TaskState::Running) {
    task.running_at = Clock::now();
    task.last_heartbeat_at = Clock::now();
    task.heartbeat_miss_count = 0;
  } else if (terminal(state)) {
    task.finished_at = Clock::now();
  }
}

void updateAllTaskState(ChainStateRecord& chain, TaskState state,
                       JobRecord& job, const std::string& msg = "") {
  for (const auto& task_id : chain.task_ids) {
    updateTaskState(job, task_id, state, msg);
  }
}

void setChainState(ChainStateRecord& chain, ChainState state, const std::string& msg = "") {
  chain.state = state;
  if (!msg.empty()) {
    chain.fail_reason = msg;
  }
  if (state == ChainState::Running) {
    chain.started_at = Clock::now();
  }
  if (terminal(state)) {
    chain.finished_at = Clock::now();
  }
}

bool chainReady(const JobRecord& job, const ChainStateRecord& chain) {
  for (const auto& dep : chain.upstream_chain_ids) {
    auto it = job.chains.find(dep);
    if (it == job.chains.end()) return false;
    if (it->second.state.state != ChainState::Succeeded) return false;
  }
  return true;
}

}  // namespace

JobMaster& JobMaster::instance() {
  static JobMaster inst;
  return inst;
}

ChainId JobMaster::buildChains(const PlanNodePtr& plan, const JobId& job_id,
                               const std::shared_ptr<JobRecord>& job) {
  if (!plan) return {};

  if (plan->kind == PlanKind::Join) {
    const auto* join = static_cast<const JoinPlan*>(plan.get());
    auto left = buildChains(join->left, job_id, job);
    auto right = buildChains(join->right, job_id, job);

    const auto chain_id = makeChainId(*job);
    ChainRuntime runtime;
    runtime.state.chain_id = chain_id;
    runtime.state.job_id = job_id;
    runtime.state.state = ChainState::WaitingForDeps;
    runtime.state.retry_budget = std::max<Attempt>(1, job->options.retry.max_attempts);
    runtime.state.created_at = Clock::now();
    runtime.state.upstream_chain_ids = {left, right};
    runtime.execute_root = plan;
    job->chains.emplace(chain_id, std::move(runtime));
    job->job_state.chain_ids.push_back(chain_id);
    if (job->chains.find(left) != job->chains.end()) {
      job->chains[left].state.downstream_chain_ids.push_back(chain_id);
    }
    if (job->chains.find(right) != job->chains.end()) {
      job->chains[right].state.downstream_chain_ids.push_back(chain_id);
    }

    auto task_id = makeTaskId(*job);
    auto task = makeTaskRecord(task_id, job_id, chain_id, plan->kind,
                               job->chains[chain_id].state.retry_budget);
    task.attempt = 0;
    job->tasks.emplace(task_id, std::move(task));
    auto& chain_ref = job->chains[chain_id].state;
    chain_ref.task_ids.push_back(task_id);
    chain_ref.total_tasks = chain_ref.task_ids.size();
    return chain_id;
  }

  auto child = unaryChild(plan);
  ChainId chain_id;
  if (child) {
    chain_id = buildChains(child, job_id, job);
  } else {
    chain_id = makeChainId(*job);
    ChainRuntime runtime;
    runtime.state.chain_id = chain_id;
    runtime.state.job_id = job_id;
    runtime.state.state = ChainState::Ready;
    runtime.state.retry_budget = std::max<Attempt>(1, job->options.retry.max_attempts);
    runtime.state.created_at = Clock::now();
    runtime.execute_root = plan;
    job->chains.emplace(chain_id, std::move(runtime));
    job->job_state.chain_ids.push_back(chain_id);
  }

  auto it = job->chains.find(chain_id);
  if (it == job->chains.end()) {
    return {};
  }
  auto task_id = makeTaskId(*job);
  auto task = makeTaskRecord(task_id, job_id, chain_id, plan->kind, it->second.state.retry_budget);
  task.attempt = it->second.state.retry_round;
  job->tasks.emplace(task_id, std::move(task));

  auto& chain = it->second.state;
  chain.task_ids.push_back(task_id);
  chain.total_tasks = chain.task_ids.size();
  it->second.execute_root = plan;
  return chain_id;
}

DataflowJobHandle JobMaster::submit(const PlanNodePtr& root, std::shared_ptr<Executor> executor,
                                    const ExecutionOptions& options) {
  if (!root) {
    throw std::runtime_error("job submit: empty plan");
  }
  if (!executor) {
    executor = std::make_shared<LocalExecutor>();
  }

  auto job = std::make_shared<JobRecord>();
  {
    std::lock_guard<std::mutex> lk(mu_);
    job->job_id = "job_" + std::to_string(++job_seq_);
  }
  job->options = options;
  job->options.chain_parallelism = options.max_chain_parallelism();
  job->root_plan = root;
  job->executor = std::move(executor);
  job->job_state.job_id = job->job_id;
  job->job_state.submitted_at = Clock::now();
  job->job_state.state = JobState::Queued;

  job->root_chain_id = buildChains(root, job->job_id, job);
  job->job_state.total_chains = job->chains.size();
  if (job->job_state.total_chains == 0 || job->root_chain_id.empty()) {
    throw std::runtime_error("job submit: fail to build chain graph");
  }

  {
    std::lock_guard<std::mutex> lk(mu_);
    jobs_.emplace(job->job_id, job);
  }
  {
    std::lock_guard<std::mutex> lk(job->mtx);
    emitJobEvent("job_submitted", *job);
  }

  std::thread([this, job]() { runJob(job); }).detach();
  return DataflowJobHandle(job->job_id);
}

DataflowJobHandle JobMaster::submitRemote(const std::string& payload,
                                          const ExecutionOptions& options) {
  auto job = std::make_shared<JobRecord>();
  {
    std::lock_guard<std::mutex> lk(mu_);
    job->job_id = "job_" + std::to_string(++job_seq_);
  }
  job->options = options;
  job->options.chain_parallelism = 1;
  job->remote_mode = true;
  job->remote_payload = payload;
  job->job_state.job_id = job->job_id;
  job->job_state.submitted_at = Clock::now();
  job->job_state.state = JobState::Queued;

  job->root_chain_id = makeChainId(*job);
  ChainRuntime runtime;
  runtime.state.chain_id = job->root_chain_id;
  runtime.state.job_id = job->job_id;
  runtime.state.state = ChainState::Ready;
  runtime.state.retry_budget = std::max<Attempt>(1, job->options.retry.max_attempts);
  runtime.state.created_at = Clock::now();
  job->chains.emplace(job->root_chain_id, std::move(runtime));
  job->job_state.chain_ids.push_back(job->root_chain_id);
  job->job_state.total_chains = 1;

  const auto task_id = makeTaskId(*job);
  auto task = makeTaskRecord(task_id, job->job_id, job->root_chain_id, PlanKind::Source,
                             job->chains[job->root_chain_id].state.retry_budget);
  job->tasks.emplace(task_id, std::move(task));
  auto& chain = job->chains[job->root_chain_id].state;
  chain.task_ids.push_back(task_id);
  chain.total_tasks = 1;

  {
    std::lock_guard<std::mutex> lk(mu_);
    jobs_.emplace(job->job_id, job);
  }

  std::thread([this, job]() { runJob(job); }).detach();
  return DataflowJobHandle(job->job_id);
}

void JobMaster::runJob(const std::shared_ptr<JobRecord>& job) {
  {
    std::lock_guard<std::mutex> lk(job->mtx);
    job->job_state.state = JobState::Running;
    job->job_state.started_at = Clock::now();
    emitJobEvent("job_running", *job);
    job->cv.notify_all();
  }

  while (true) {
    std::vector<ChainId> launch;
    bool should_wait = false;

    {
      std::lock_guard<std::mutex> lk(job->mtx);

      if (job->cancelled || job->done) {
        job->job_state.state = JobState::Cancelled;
        job->job_state.finished_at = Clock::now();
        emitJobEvent("job_cancelled", *job);
        job->cv.notify_all();
        return;
      }

      // heartbeat best-effort: only mark non-terminal tasks in local heartbeat-disabled mode.
      if (job->options.heartbeat.heartbeat_timeout_ms > 0) {
        const auto now = Clock::now();
        const auto timeout = std::chrono::milliseconds(job->options.heartbeat.heartbeat_timeout_ms);
        const auto miss_threshold = job->options.heartbeat.heartbeat_miss_threshold;
        for (auto& chain_item : job->chains) {
          auto& chain = chain_item.second.state;
          if (chain.state != ChainState::Running) continue;
          for (const auto& task_id : chain.task_ids) {
            auto it = job->tasks.find(task_id);
            if (it == job->tasks.end()) continue;
            auto& task = it->second;
            if (task.state != TaskState::Running || task.worker_id.empty()) continue;
            const auto age = now - task.last_heartbeat_at;
            if (age > timeout) {
              task.heartbeat_miss_count++;
              task.state = TaskState::HeartbeatMiss;
              task.heartbeat_seq++;
              if (miss_threshold > 0 && task.heartbeat_miss_count >= miss_threshold) {
                task.error_message = "heartbeat timeout";
              }
              task.last_heartbeat_at = now;
            }
          }
        }
      }

      size_t active_limit = job->options.chain_parallelism;
      if (active_limit == 0) active_limit = 1;

      for (auto& chain_item : job->chains) {
        auto& chain = chain_item.second.state;
        if (chain.state == ChainState::Created) {
          chain.state = chain.upstream_chain_ids.empty() ? ChainState::Ready : ChainState::WaitingForDeps;
        }
        if (chain.state == ChainState::WaitingForDeps && chainReady(*job, chain)) {
          chain.state = ChainState::Ready;
        }
      }

      size_t terminal_count = 0;
      size_t failed_count = 0;
      size_t succeed_count = 0;
      for (auto& chain_item : job->chains) {
        const auto& chain = chain_item.second.state;
        if (terminal(chain.state)) {
          ++terminal_count;
          if (chain.state == ChainState::Failed) ++failed_count;
          if (chain.state == ChainState::Succeeded) ++succeed_count;
        }
      }

      if (terminal_count == job->chains.size()) {
        if (failed_count > 0) {
          job->job_state.state = JobState::Failed;
          job->job_state.error_summary = "one or more chains failed";
        } else {
          job->job_state.state = JobState::Succeeded;
          job->job_state.has_result = true;
          job->job_state.result = job->final_result;
        }
        job->job_state.succeeded_chains = succeed_count;
        job->job_state.failed_chains = failed_count;
        job->job_state.finished_at = Clock::now();
        job->done = true;
        emitJobEvent("job_finished", *job);
        job->cv.notify_all();
        return;
      }

      if (job->job_state.state == JobState::Retrying && failed_count == 0) {
        // keep retrying when failed chains are already scheduled for retry.
        job->job_state.state = JobState::Running;
        emitJobEvent("job_retry_recovered", *job);
      }

      for (auto& chain_item : job->chains) {
        if (launch.size() >= active_limit) break;
        auto& chain = chain_item.second.state;
        if (chain.state != ChainState::Ready && chain.state != ChainState::Retrying) continue;
        if (job->active_chain_count >= active_limit) break;
        setChainState(chain, ChainState::Scheduled);
        updateAllTaskState(chain, TaskState::Queued, *job);
        ++job->active_chain_count;
        launch.push_back(chain_item.first);
      }

      should_wait = launch.empty() && job->active_chain_count > 0;
    }

    for (const auto& chain_id : launch) {
      std::thread([this, job, chain_id]() { runChain(job, chain_id); }).detach();
    }

    if (!launch.empty()) {
      std::unique_lock<std::mutex> lk(job->mtx);
      job->cv.wait_for(lk, std::chrono::milliseconds(20));
    } else if (should_wait) {
      std::unique_lock<std::mutex> lk(job->mtx);
      job->cv.wait_for(lk, std::chrono::milliseconds(50));
    }
  }
}

void JobMaster::runChain(const std::shared_ptr<JobRecord>& job, const ChainId& chain_id) {
  auto chain_it = job->chains.find(chain_id);
  if (chain_it == job->chains.end()) return;

  auto& chain = chain_it->second.state;
  auto& chain_runtime = chain_it->second;
  const auto max_attempts = chain.retry_budget;
  const auto root_plan = chain_runtime.execute_root;
  for (Attempt attempt = chain.retry_round; attempt < max_attempts; ++attempt) {
    {
      std::lock_guard<std::mutex> lk(job->mtx);
      if (job->cancelled || job->done) {
        setChainState(chain, ChainState::Cancelled, "cancel requested");
        updateAllTaskState(chain, TaskState::Cancelled, *job, "cancel requested");
        if (job->active_chain_count > 0) --job->active_chain_count;
        job->cv.notify_all();
        return;
      }
      if (chain.state != ChainState::Scheduled && chain.state != ChainState::Retrying &&
          chain.state != ChainState::Ready) {
        if (job->active_chain_count > 0) --job->active_chain_count;
        job->cv.notify_all();
        return;
      }

      chain.retry_round = attempt;
      chain.running_tasks = chain.total_tasks;
      chain.failed_tasks = 0;
      chain.done_tasks = 0;
      setChainState(chain, ChainState::Running);
      for (const auto& task_id : chain.task_ids) {
        auto t_it = job->tasks.find(task_id);
        if (t_it == job->tasks.end()) continue;
        auto& t = t_it->second;
        t.attempt = attempt;
        t.error_message.clear();
        updateTaskState(*job, task_id, TaskState::Running);
      }
      emitJobEvent("chain_running", *job);
    }

    bool execute_succeeded = false;
    std::string err;
    try {
      Table out;
      if (job->remote_mode) {
        RemoteTaskSpec spec;
        {
          std::lock_guard<std::mutex> global_lk(mu_);
          const auto& task_id = chain.task_ids.front();
          spec.job_id = job->job_id;
          spec.chain_id = chain_id;
          spec.task_id = task_id;
          spec.attempt = attempt;
          spec.payload = job->remote_payload;
          pending_remote_tasks_.push_back(spec);
        }
        job->remote_task_dispatched = true;
        remote_cv_.notify_one();

        std::unique_lock<std::mutex> lk(job->mtx);
        job->cv.wait(lk, [&]() {
          return job->cancelled || job->done || job->remote_task_completed;
        });
        if (job->cancelled || job->done) {
          setChainState(chain, ChainState::Cancelled, "cancel requested");
          updateAllTaskState(chain, TaskState::Cancelled, *job, "cancel requested");
          if (job->active_chain_count > 0) --job->active_chain_count;
          job->cv.notify_all();
          return;
        }
        if (!job->remote_task_completed) {
          throw std::runtime_error("remote task completion missing");
        }
        auto& task = job->tasks.at(chain.task_ids.front());
        if (task.state != TaskState::Succeeded) {
          throw std::runtime_error(task.error_message.empty() ? "remote task failed" : task.error_message);
        }
        job->remote_task_completed = false;
        if (job->remote_has_result_table) {
          out = job->remote_result_table;
        }
        job->remote_has_result_table = false;
      } else {
        out = job->executor->execute(root_plan);
      }
      {
        std::lock_guard<std::mutex> lk(job->mtx);
        if (job->cancelled || job->done) {
          setChainState(chain, ChainState::Cancelled, "cancel requested");
          updateAllTaskState(chain, TaskState::Cancelled, *job, "cancel requested");
          if (job->active_chain_count > 0) --job->active_chain_count;
          job->cv.notify_all();
          return;
        }
        updateAllTaskState(chain, TaskState::Succeeded, *job);
        for (const auto& task_id : chain.task_ids) {
          auto& task = job->tasks.at(task_id);
          task.output_rows = out.rowCount();
        }
        chain.done_tasks = chain.total_tasks;
        chain.running_tasks = 0;
        setChainState(chain, ChainState::Succeeded);
        if (chain_id == job->root_chain_id) {
          job->final_result = out;
        }
        if (chain_id == job->root_chain_id) {
          job->job_state.has_result = true;
        }
        ++job->job_state.succeeded_chains;
        job->job_state.state = JobState::Running;
        emitJobEvent("chain_succeeded", *job);
        execute_succeeded = true;
      }
    } catch (const std::exception& e) {
      err = e.what();
    } catch (...) {
      err = "unknown execution error";
    }

    {
      std::lock_guard<std::mutex> lk(job->mtx);
      if (execute_succeeded) {
        if (job->active_chain_count > 0) --job->active_chain_count;
        job->cv.notify_all();
        return;
      }

      updateAllTaskState(chain, TaskState::Failed, *job, err);
      chain.failed_tasks = chain.total_tasks;
      chain.running_tasks = 0;
      bool will_retry =
          !job->cancelled && (attempt + 1 < max_attempts) && job->options.retry.retry_on_transient_error;

      if (will_retry) {
        setChainState(chain, ChainState::Retrying, err);
        updateAllTaskState(chain, TaskState::Queued, *job, "retrying");
        job->job_state.state = JobState::Retrying;
        emitJobEvent("chain_retrying", *job);
        if (job->active_chain_count > 0) --job->active_chain_count;
        job->cv.notify_all();
      } else {
        setChainState(chain, ChainState::Failed, err);
        ++job->job_state.failed_chains;
        job->job_state.state = JobState::Failed;
        job->job_state.error_summary = err;
        job->done = true;
        emitJobEvent("chain_failed", *job);
        if (job->active_chain_count > 0) --job->active_chain_count;
        job->cv.notify_all();
        return;
      }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(
        backoffMs(attempt + 1, job->options.retry)));
  }

  {
    std::lock_guard<std::mutex> lk(job->mtx);
    updateAllTaskState(chain, TaskState::Failed, *job, "retry limit exceeded");
    setChainState(chain, ChainState::Failed, "retry limit exceeded");
    ++job->job_state.failed_chains;
    job->job_state.state = JobState::Failed;
    job->job_state.error_summary = "retry limit exceeded";
    job->done = true;
    emitJobEvent("chain_retry_exhausted", *job);
    if (job->active_chain_count > 0) --job->active_chain_count;
  }
  job->cv.notify_all();
}

bool JobMaster::pollRemoteTask(RemoteTaskSpec* task, std::chrono::milliseconds timeout) {
  if (!task) return false;
  std::unique_lock<std::mutex> lk(mu_);
  if (!remote_cv_.wait_for(lk, timeout, [&]() { return !pending_remote_tasks_.empty(); })) {
    return false;
  }
  *task = pending_remote_tasks_.front();
  pending_remote_tasks_.pop_front();
  return true;
}

void JobMaster::completeRemoteTask(const RemoteTaskCompletion& completion) {
  std::shared_ptr<JobRecord> job;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = jobs_.find(completion.job_id);
    if (it == jobs_.end()) return;
    job = it->second;
  }
  std::lock_guard<std::mutex> lk(job->mtx);
  auto task_it = job->tasks.find(completion.task_id);
  if (task_it == job->tasks.end()) return;
  auto& task = task_it->second;
  if (task.attempt != completion.attempt) return;
  task.worker_id = completion.worker_id;
  task.output_rows = completion.has_result_table ? completion.result_table.rowCount() : completion.output_rows;
  task.error_message = completion.error_message;
  if (completion.ok) {
    updateTaskState(*job, completion.task_id, TaskState::Succeeded);
    job->remote_result_table = completion.result_table;
    job->remote_has_result_table = completion.has_result_table;
  } else {
    updateTaskState(*job, completion.task_id, TaskState::Failed, completion.error_message);
  }
  job->remote_task_completed = true;
  job->cv.notify_all();
}

JobStateRecord JobMaster::queryJob(const JobId& id) const {
  std::shared_ptr<JobRecord> job;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = jobs_.find(id);
    if (it == jobs_.end()) return JobStateRecord();
    job = it->second;
  }
  std::lock_guard<std::mutex> lk(job->mtx);
  return job->job_state;
}

ChainStateRecord JobMaster::queryChain(const JobId& id, const ChainId& chain_id) const {
  std::shared_ptr<JobRecord> job;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = jobs_.find(id);
    if (it == jobs_.end()) throw std::runtime_error("unknown job id");
    job = it->second;
  }
  std::lock_guard<std::mutex> lk(job->mtx);
  auto it = job->chains.find(chain_id);
  if (it == job->chains.end()) throw std::runtime_error("unknown chain id");
  return it->second.state;
}

TaskStateRecord JobMaster::queryTask(const JobId& id, const TaskId& task_id) const {
  std::shared_ptr<JobRecord> job;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = jobs_.find(id);
    if (it == jobs_.end()) throw std::runtime_error("unknown job id");
    job = it->second;
  }
  std::lock_guard<std::mutex> lk(job->mtx);
  auto it = job->tasks.find(task_id);
  if (it == job->tasks.end()) throw std::runtime_error("unknown task id");
  return it->second;
}

std::string JobMaster::snapshotJson(const JobId& id) const {
  std::shared_ptr<JobRecord> job;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = jobs_.find(id);
    if (it == jobs_.end()) return "{}";
    job = it->second;
  }
  std::lock_guard<std::mutex> lk(job->mtx);
  return serializeJobSnapshot(job->job_state, job->chains, job->tasks);
}

void JobMaster::heartbeat(const TaskHeartbeat& hb) {
  std::shared_ptr<JobRecord> job;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = jobs_.find(hb.job_id);
    if (it == jobs_.end()) return;
    job = it->second;
  }

  std::lock_guard<std::mutex> lk(job->mtx);
  auto it = job->tasks.find(hb.task_id);
  if (it == job->tasks.end()) return;
  auto& task = it->second;
  if (hb.attempt != task.attempt) return;
  if (hb.heartbeat_seq <= task.heartbeat_seq) return;
  task.heartbeat_seq = hb.heartbeat_seq;
  task.worker_id = hb.worker_id;
  task.last_heartbeat_at = hb.at;
  task.heartbeat_miss_count = 0;
  task.input_rows = hb.input_rows;
  task.output_rows = hb.output_rows;
  task.error_code = taskStatusCode(task.state);
  emitJobEvent("task_heartbeat", *job);
}

bool JobMaster::cancel(const JobId& id) {
  std::shared_ptr<JobRecord> job;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = jobs_.find(id);
    if (it == jobs_.end()) return false;
    job = it->second;
  }

  std::lock_guard<std::mutex> lk(job->mtx);
  if (job->done) return false;
  job->cancelled = true;
  job->job_state.state = JobState::Cancelled;
  for (auto& chain_item : job->chains) {
    auto& chain = chain_item.second.state;
    if (!terminal(chain.state)) {
      setChainState(chain, ChainState::Cancelled, "cancel requested");
    }
    updateAllTaskState(chain, TaskState::Cancelled, *job, "cancel requested");
  }
  job->job_state.error_summary = "cancelled";
  job->done = true;
  emitJobEvent("job_cancel_requested", *job);
  job->cv.notify_all();
  return true;
}

void JobMaster::shutdown() {
  std::lock_guard<std::mutex> lk(mu_);
  for (auto& item : jobs_) {
    auto& job = item.second;
    if (!job) continue;
    std::lock_guard<std::mutex> g(job->mtx);
    job->cancelled = true;
    job->job_state.state = JobState::Cancelled;
    job->job_state.error_summary = "shutdown";
    job->done = true;
    emitJobEvent("job_shutdown", *job);
    job->cv.notify_all();
  }
}

JobState DataflowJobHandle::state() const {
  return id_.empty() ? JobState::Failed : JobMaster::instance().queryJob(id_).state;
}

bool DataflowJobHandle::done() const {
  return terminal(state());
}

bool DataflowJobHandle::cancel() const {
  return !id_.empty() && JobMaster::instance().cancel(id_);
}

JobStateRecord DataflowJobHandle::queryJob() const {
  if (id_.empty()) return JobStateRecord();
  auto record = JobMaster::instance().queryJob(id_);
  materializeResultRowsIfPresent(record.has_result, &record.result);
  return record;
}

ChainStateRecord DataflowJobHandle::queryChain(const ChainId& chainId) const {
  if (id_.empty()) return ChainStateRecord();
  return JobMaster::instance().queryChain(id_, chainId);
}

TaskStateRecord DataflowJobHandle::queryTask(const TaskId& taskId) const {
  if (id_.empty()) return TaskStateRecord();
  return JobMaster::instance().queryTask(id_, taskId);
}

std::string DataflowJobHandle::snapshotJson() const {
  if (id_.empty()) return "{}";
  return JobMaster::instance().snapshotJson(id_);
}

JobHandleSnapshot DataflowJobHandle::wait(std::chrono::milliseconds timeout) const {
  JobHandleSnapshot snap;
  snap.job_id = id_;
  if (id_.empty()) {
    snap.state = JobState::Failed;
    snap.state_message = "empty job id";
    return snap;
  }

  const auto start = Clock::now();
  while (true) {
    auto rec = JobMaster::instance().queryJob(id_);
    snap.state = rec.state;
    snap.state_message = rec.error_summary;
    snap.status_code = jobStatusCode(rec.state);
    snap.has_result = rec.has_result;
    snap.result = rec.result;
    materializeResultRowsIfPresent(snap.has_result, &snap.result);
    if (terminal(rec.state)) {
      return snap;
    }
    if (timeout.count() > 0 && std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start) >= timeout) {
      return snap;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
}

Table DataflowJobHandle::result(std::chrono::milliseconds timeout) const {
  auto snap = wait(timeout);
  if (!snap.has_result) {
    throw std::runtime_error("job has no result");
  }
  return snap.result;
}

}  // namespace dataflow
