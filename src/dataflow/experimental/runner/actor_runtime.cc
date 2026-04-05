#include "src/dataflow/experimental/runner/actor_runtime.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cerrno>
#include <cstdint>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <unistd.h>

#include "src/dataflow/core/contract/api/dataframe.h"
#include "src/dataflow/core/contract/api/session.h"
#include "src/dataflow/core/execution/nanoarrow_ipc_codec.h"
#include "src/dataflow/experimental/rpc/actor_rpc_codec.h"
#include "src/dataflow/experimental/rpc/rpc_codec.h"
#include "src/dataflow/experimental/rpc/rpc_codec_ids.h"
#include "src/dataflow/experimental/runtime/job_master.h"
#include "src/dataflow/core/execution/runtime/observability.h"
#include "src/dataflow/core/execution/stream/binary_row_batch.h"
#include "src/dataflow/core/logical/sql/sql_parser.h"
#include "src/dataflow/experimental/transport/ipc_transport.h"

namespace dataflow {

namespace {

std::atomic<bool>* g_schedulerStopRequested = nullptr;

void onSchedulerStopSignal(int) {
  if (g_schedulerStopRequested) {
    g_schedulerStopRequested->store(true);
  }
}

bool parseConfigEndpoint(const std::string& endpoint,
                         std::string* host,
                         uint16_t* port) {
  return parseEndpoint(endpoint, host, port);
}

bool waitForReadable(int fd, std::chrono::milliseconds timeout) {
  if (fd < 0) return false;
  fd_set reads;
  FD_ZERO(&reads);
  FD_SET(fd, &reads);
  struct timeval tv;
  tv.tv_sec = static_cast<long>(timeout.count() / 1000);
  tv.tv_usec = static_cast<long>((timeout.count() % 1000) * 1000);
  const int ready = select(fd + 1, &reads, nullptr, nullptr, &tv);
  return ready > 0 && FD_ISSET(fd, &reads);
}

std::string flattenText(const std::string& input) {
  std::string out;
  out.reserve(input.size());
  bool last_space = false;
  for (char ch : input) {
    if (ch == '\n' || ch == '\r' || ch == '\t') ch = ' ';
    if (ch == ' ') {
      if (last_space) continue;
      last_space = true;
    } else {
      last_space = false;
    }
    out.push_back(ch);
  }
  return out;
}

std::string joinStrings(const std::vector<std::string>& values, const std::string& delim) {
  std::ostringstream out;
  for (std::size_t i = 0; i < values.size(); ++i) {
    if (i > 0) out << delim;
    out << values[i];
  }
  return out.str();
}

std::string summarizeRow(const Row& row) {
  std::vector<std::string> cells;
  cells.reserve(row.size());
  for (const auto& value : row) cells.push_back(value.toString());
  return joinStrings(cells, "|");
}

std::string summarizeTable(const Table& table) {
  std::ostringstream out;
  out << "rows=" << table.rowCount()
      << ", cols=" << table.schema.fields.size()
      << ", schema=" << joinStrings(table.schema.fields, ",");
  if (!table.rows.empty()) {
    out << ", first_row=" << summarizeRow(table.rows.front());
  }
  return out.str();
}

Table makeDemoInputTable(const std::string& payload) {
  Table table(Schema({"token", "bucket", "score"}), {});
  const int64_t base = static_cast<int64_t>(payload.size());
  table.rows.push_back({Value(payload), Value("primary"), Value(base)});
  table.rows.push_back({Value(payload), Value("primary"), Value(base + 2)});
  table.rows.push_back({Value(payload + "_shadow"), Value("shadow"), Value(base + 1)});
  return table;
}

DataFrame buildPayloadPlan(const std::string& payload) {
  DataFrame df(makeDemoInputTable(payload));
  std::vector<AggregateSpec> aggs;
  aggs.push_back({AggregateFunction::Sum, 2, "total_score"});
  aggs.push_back({AggregateFunction::Count, static_cast<std::size_t>(-1), "row_count"});
  return df.filterByIndex(2, ">=", Value(static_cast<int64_t>(0)))
      .aggregate({0}, aggs)
      .selectByIndices({0, 1, 2}, {"token", "total_score", "row_count"})
      .limit(10);
}

DataFrame buildSqlPlan(const std::string& sql, const std::string& payload) {
  auto& session = DataflowSession::builder();
  session.createTempView("rpc_input", DataFrame(makeDemoInputTable(payload)));
  return session.sql(sql);
}

DataFrame buildClientPlan(const std::string& payload, const std::string& sql) {
  if (!sql.empty()) return buildSqlPlan(sql, payload);
  return buildPayloadPlan(payload);
}

bool isLocalWorkerNodeId(const std::string& node_id, const std::string& scheduler_node_id) {
  const std::string prefix = scheduler_node_id + "-worker-";
  return node_id.compare(0, prefix.size(), prefix) == 0;
}

struct LocalWorkerProcess {
  pid_t pid;
  std::string node_id;
  bool registered;
};

struct RpcTaskSnapshot {
  std::string task_id;
  std::string state = "SUBMITTED";
  std::string status_code = "TASK_SUBMITTED";
  std::string worker_id;
  std::string fail_reason;
  std::chrono::steady_clock::time_point queued_at;
  std::chrono::steady_clock::time_point dispatched_at;
  std::chrono::steady_clock::time_point worker_started_at;
  std::chrono::steady_clock::time_point worker_finished_at;
  std::chrono::steady_clock::time_point result_returned_at;
};

struct RpcChainSnapshot {
  std::string chain_id;
  std::string state = "SUBMITTED";
  std::string status_code = "CHAIN_SUBMITTED";
  std::vector<std::string> task_ids;
};

struct RpcJobSnapshot {
  std::string job_id;
  std::string client_node;
  std::string worker_node;
  std::string state = "SUBMITTED";
  std::string status_code = "JOB_SUBMITTED";
  std::string payload;
  std::string sql;
  std::string result_payload;
  RpcChainSnapshot chain;
  RpcTaskSnapshot task;
};

std::string snapshotToJson(const RpcJobSnapshot& snapshot) {
  using namespace observability;
  const auto queue_wait_ms =
      epochMillis(snapshot.task.dispatched_at) - epochMillis(snapshot.task.queued_at);
  const auto worker_run_ms =
      epochMillis(snapshot.task.worker_finished_at) - epochMillis(snapshot.task.worker_started_at);
  const auto result_return_ms =
      epochMillis(snapshot.task.result_returned_at) - epochMillis(snapshot.task.dispatched_at);
  const std::string task_json = object({
      field("task_id", snapshot.task.task_id),
      field("state", snapshot.task.state),
      field("status_code", snapshot.task.status_code),
      field("worker_id", snapshot.task.worker_id),
      field("fail_reason", snapshot.task.fail_reason),
      field("queued_at_ms", epochMillis(snapshot.task.queued_at)),
      field("dispatched_at_ms", epochMillis(snapshot.task.dispatched_at)),
      field("worker_started_at_ms", epochMillis(snapshot.task.worker_started_at)),
      field("worker_finished_at_ms", epochMillis(snapshot.task.worker_finished_at)),
      field("result_returned_at_ms", epochMillis(snapshot.task.result_returned_at)),
      field("queue_wait_ms", queue_wait_ms > 0 ? queue_wait_ms : 0),
      field("worker_run_ms", worker_run_ms > 0 ? worker_run_ms : 0),
      field("result_return_ms", result_return_ms > 0 ? result_return_ms : 0),
  });
  const std::string chain_json = object({
      field("chain_id", snapshot.chain.chain_id),
      field("state", snapshot.chain.state),
      field("status_code", snapshot.chain.status_code),
      field("task_ids", arrayFromStrings(snapshot.chain.task_ids), true),
  });
  return object({
      field("job_id", snapshot.job_id),
      field("state", snapshot.state),
      field("status_code", snapshot.status_code),
      field("client_node", snapshot.client_node),
      field("worker_node", snapshot.worker_node),
      field("payload", snapshot.payload),
      field("sql", snapshot.sql),
      field("result_payload", snapshot.result_payload),
      field("chain", chain_json, true),
      field("task", task_json, true),
  });
}

RpcFrame makeFrameFromMessage(uint64_t msg_id,
                              uint64_t correlation_id,
                              const std::string& source,
                              const std::string& target,
                              const ActorRpcMessage& message) {
  RpcFrame frame;
  frame.header.protocol_version = 1;
  frame.header.type = RpcMessageType::Control;
  frame.header.message_id = msg_id;
  frame.header.correlation_id = correlation_id;
  frame.header.codec_id = "actor-rpc-v1";
  frame.header.source = source;
  frame.header.target = target;
  frame.payload = encodeActorRpcMessage(message);
  return frame;
}

RpcFrame makeFrameFromMessage(uint64_t msg_id,
                              const std::string& source,
                              const std::string& target,
                              const ActorRpcMessage& message) {
  return makeFrameFromMessage(msg_id, 0, source, target, message);
}

RpcFrame makeDataBatchFrame(uint64_t msg_id,
                            uint64_t correlation_id,
                            const std::string& source,
                            const std::string& target,
                            const Table& table) {
  RpcFrame frame;
  frame.header.protocol_version = 1;
  frame.header.type = RpcMessageType::DataBatch;
  frame.header.message_id = msg_id;
  frame.header.correlation_id = correlation_id;
  frame.header.codec_id = kRpcCodecIdTableArrowIpcV1;
  frame.header.source = source;
  frame.header.target = target;
  frame.payload = serialize_nanoarrow_ipc_table(table);
  return frame;
}

bool decodeDataBatchFrame(const RpcFrame& frame, Table* out) {
  if (out == nullptr || frame.header.type != RpcMessageType::DataBatch) {
    return false;
  }
  if (frame.header.codec_id == kRpcCodecIdTableArrowIpcV1) {
    *out = deserialize_nanoarrow_ipc_table(frame.payload, false);
    return true;
  }
  if (frame.header.codec_id != kRpcCodecIdTableBinV1) {
    return false;
  }
  BinaryRowBatchCodec codec;
  *out = codec.deserialize(frame.payload);
  return true;
}

void removeWorker(std::vector<int>* workers, int fd) {
  workers->erase(std::remove(workers->begin(), workers->end(), fd), workers->end());
}

void removePendingByClient(const std::unordered_map<std::string, int>& map,
                           int fd,
                           std::vector<std::string>* to_remove) {
  for (const auto& kv : map) {
    if (kv.second == fd) {
      to_remove->push_back(kv.first);
    }
  }
}

void emitRpcEvent(const std::string& component,
                  const std::string& event,
                  const std::string& node_id,
                  const std::string& status_code,
                  const std::string& job_id,
                  const std::vector<std::string>& extra = {}) {
  using namespace observability;
  std::vector<std::string> fields = {
      field("component", component),
      field("event", event),
      field("node_id", node_id),
      field("status_code", status_code),
      field("job_id", job_id),
  };
  fields.insert(fields.end(), extra.begin(), extra.end());
  std::cout << object(fields) << std::endl;
}

}  // namespace

int runActorScheduler(const ActorRuntimeConfig& config) {
  if (config.single_node) {
    std::cout << "[scheduler] single node mode not using sockets. use core demo directly."
              << std::endl;
    return 0;
  }

  std::string host;
  uint16_t port = 0;
  if (!parseConfigEndpoint(config.listen_address, &host, &port)) {
    std::cerr << "Invalid --listen endpoint: " << config.listen_address << "\n";
    return 1;
  }

  const int server_fd = createServerSocket(host, port);
  if (server_fd < 0) {
    std::cerr << "scheduler failed to listen on " << config.listen_address << "\n";
    return 1;
  }

  std::cout << "[scheduler] listen " << config.listen_address << "\n";

  LengthPrefixedFrameCodec codec;
  std::vector<int> conns;
  std::unordered_map<int, std::string> conn_role;
  std::unordered_map<int, std::string> conn_node;
  std::unordered_map<int, std::string> worker_node_by_fd;
  std::vector<int> workers;
  std::unordered_set<int> idle_workers;
  std::unordered_map<std::string, int> job_to_client;
  std::unordered_map<std::string, int> task_to_worker;
  std::unordered_map<int, ActorRpcMessage> pending_worker_result_msgs;
  std::unordered_map<std::string, RpcJobSnapshot> job_snapshots;
  uint64_t next_message_id = 1;
  std::unordered_set<std::string> local_worker_nodes;
  int launching_local_workers = 0;
  std::unordered_map<pid_t, LocalWorkerProcess> local_worker_processes;
  std::unordered_map<std::string, pid_t> local_worker_node_to_pid;
  int local_worker_seq = 1;
  const int target_local_workers = config.auto_worker ? std::max(1, config.local_worker_count) : 0;
  std::atomic<bool> stop_requested{false};
  g_schedulerStopRequested = &stop_requested;
  const auto old_sigint = std::signal(SIGINT, &onSchedulerStopSignal);
  const auto old_sigterm = std::signal(SIGTERM, &onSchedulerStopSignal);

  const auto exitStatusFromWaitStatus = [&](int status) {
    if (WIFEXITED(status)) {
      return WEXITSTATUS(status);
    }
    if (WIFSIGNALED(status)) {
      return 128 + WTERMSIG(status);
    }
    return -1;
  };

  const auto stopAllLocalWorkers = [&]() {
    std::vector<std::pair<pid_t, std::string>> targets;
    targets.reserve(local_worker_processes.size());
    for (const auto& kv : local_worker_processes) {
      targets.push_back({kv.first, kv.second.node_id});
      if (kv.first > 0) {
        kill(kv.first, SIGTERM);
      }
    }
    for (const auto& kv : targets) {
      const pid_t pid = kv.first;
      if (pid <= 0) continue;
      int status = 0;
      bool terminated = false;
      auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(250);
      while (std::chrono::steady_clock::now() < deadline) {
        const pid_t waited = waitpid(pid, &status, WNOHANG);
        if (waited == pid) {
          terminated = true;
          break;
        }
        if (waited < 0) {
          if (errno == ECHILD) {
            terminated = true;
          }
          break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
      if (!terminated) {
        kill(pid, SIGKILL);
        deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(250);
        while (std::chrono::steady_clock::now() < deadline) {
          const pid_t waited = waitpid(pid, &status, WNOHANG);
          if (waited == pid) {
            terminated = true;
            break;
          }
          if (waited < 0) {
            if (errno == ECHILD) {
              terminated = true;
            }
            break;
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
      }
      if (!terminated) {
        continue;
      }
      if (waitpid(pid, &status, 0) < 0 && errno != ECHILD) {
        continue;
      }
      emitRpcEvent("actor_scheduler", "local_worker_exit", config.node_id, "WORKER_EXIT", "",
                   {observability::field("worker_node", kv.second),
                    observability::field("pid", kv.first),
                    observability::field("return_code", exitStatusFromWaitStatus(status))});
    }
    local_worker_processes.clear();
    local_worker_node_to_pid.clear();
    local_worker_nodes.clear();
    launching_local_workers = 0;
  };

  const auto reapLocalWorkers = [&]() {
    std::vector<pid_t> exited;
    for (const auto& kv : local_worker_processes) {
      int status = 0;
      const pid_t pid = kv.first;
      const pid_t waited = waitpid(pid, &status, WNOHANG);
      if (waited != pid) {
        continue;
      }
      exited.push_back(pid);
      emitRpcEvent("actor_scheduler", "local_worker_exit", config.node_id, "WORKER_EXIT", "",
                   {observability::field("worker_node", kv.second.node_id),
                    observability::field("pid", kv.first),
                    observability::field("return_code", exitStatusFromWaitStatus(status))});
    }
    for (const pid_t pid : exited) {
      const auto it = local_worker_processes.find(pid);
      if (it != local_worker_processes.end()) {
        if (!it->second.registered && launching_local_workers > 0) {
          --launching_local_workers;
        }
        local_worker_node_to_pid.erase(it->second.node_id);
        local_worker_nodes.erase(it->second.node_id);
        local_worker_processes.erase(pid);
      }
    }
  };

  const auto ensureSignalRestored = [&]() {
    std::signal(SIGINT, old_sigint);
    std::signal(SIGTERM, old_sigterm);
    g_schedulerStopRequested = nullptr;
  };

  auto startLocalWorker = [&]() -> bool {
    if (target_local_workers <= 0) return false;
    ActorRuntimeConfig worker_config = config;
    const int worker_index = local_worker_seq++;
    worker_config.node_id = config.node_id + "-worker-" + std::to_string(worker_index);
    worker_config.connect_address = config.listen_address;
    worker_config.single_node = false;
    const pid_t pid = fork();
    if (pid < 0) {
      emitRpcEvent("actor_scheduler", "local_worker_spawn_failed", config.node_id,
                   "WORKER_SPAWN_FAILED", "",
                   {observability::field("worker_index", worker_index), observability::field("errno", errno)});
      return false;
    }
    if (pid == 0) {
      const int rc = runActorWorker(worker_config);
      _exit(rc);
    }

    ++launching_local_workers;
    local_worker_processes.emplace(pid, LocalWorkerProcess{pid, worker_config.node_id, false});
    local_worker_node_to_pid.emplace(worker_config.node_id, pid);

    emitRpcEvent("actor_scheduler", "local_worker_spawn", config.node_id, "WORKER_SPAWNED", "",
                 {observability::field("worker_index", worker_index),
                  observability::field("worker_node", worker_config.node_id),
                  observability::field("target", target_local_workers),
                  observability::field("pid", pid)});
    return true;
  };

  auto ensureLocalWorkers = [&]() {
    if (!config.auto_worker) return;
    while (static_cast<int>(local_worker_nodes.size()) + launching_local_workers < target_local_workers) {
      if (!startLocalWorker()) {
        break;
      }
    }
  };

  ensureLocalWorkers();

  auto emitSnapshotEvent = [&](const RpcJobSnapshot& snapshot) {
    emitRpcEvent("actor_scheduler", "job_snapshot", config.node_id, snapshot.status_code, snapshot.job_id,
                 {observability::field("snapshot", snapshotToJson(snapshot), true)});
  };

  auto sendTo = [&](int fd, const ActorRpcMessage& message) {
    const RpcFrame frame = makeFrameFromMessage(next_message_id++, config.node_id, "peer", message);
    return sendFrameOverSocket(fd, codec, frame);
  };

  auto cleanup = [&](int fd) {
    const std::string role = conn_role.count(fd) ? conn_role[fd] : "unknown";
    const std::string node = conn_node.count(fd) ? conn_node[fd] : "";
    const std::string worker_node = worker_node_by_fd.count(fd) ? worker_node_by_fd[fd] : "";
    conn_role.erase(fd);
    conn_node.erase(fd);
    worker_node_by_fd.erase(fd);
    pending_worker_result_msgs.erase(fd);
    if (role == "worker" && isLocalWorkerNodeId(worker_node, config.node_id)) {
      local_worker_nodes.erase(worker_node);
    }
    idle_workers.erase(fd);
    removeWorker(&workers, fd);
    std::vector<std::string> stale_jobs;
    removePendingByClient(job_to_client, fd, &stale_jobs);
    for (const auto& job : stale_jobs) {
      auto it = job_snapshots.find(job);
      if (it != job_snapshots.end()) {
        it->second.state = "FAILED";
        it->second.status_code = "JOB_CLIENT_DISCONNECTED";
        it->second.chain.state = "FAILED";
        it->second.chain.status_code = "CHAIN_FAILED";
        it->second.task.state = "FAILED";
        it->second.task.status_code = "TASK_FAILED";
        emitRpcEvent("actor_scheduler", "job_snapshot", config.node_id, it->second.status_code, it->second.job_id,
                     {observability::field("snapshot", snapshotToJson(it->second), true)});
      }
      job_to_client.erase(job);
    }
      emitRpcEvent("actor_scheduler", "connection_closed", config.node_id, "RPC_CONNECTION_CLOSED", "",
                   {observability::field("fd", fd), observability::field("role", role),
                    observability::field("peer_node", node)});
    conns.erase(std::remove(conns.begin(), conns.end(), fd), conns.end());
    ::close(fd);
  };

  auto dispatchPending = [&]() {
    ensureLocalWorkers();
    while (!idle_workers.empty()) {
      RemoteTaskSpec task;
      if (!JobMaster::instance().pollRemoteTask(&task, std::chrono::milliseconds(0))) {
        break;
      }
      const int worker_fd = *idle_workers.begin();
      idle_workers.erase(worker_fd);
      const std::string worker_node = worker_node_by_fd[worker_fd];
      task_to_worker[task.task_id] = worker_fd;

      auto snapshot_it = job_snapshots.find(task.job_id);
      if (snapshot_it != job_snapshots.end()) {
        snapshot_it->second.worker_node = worker_node;
        snapshot_it->second.chain.chain_id = task.chain_id;
        snapshot_it->second.chain.state = "SUBMITTED";
        snapshot_it->second.chain.status_code = "CHAIN_SUBMITTED";
        snapshot_it->second.task.task_id = task.task_id;
        snapshot_it->second.task.state = "SUBMITTED";
        snapshot_it->second.task.status_code = "TASK_SUBMITTED";
        snapshot_it->second.task.worker_id = worker_node;
        snapshot_it->second.task.fail_reason.clear();
        snapshot_it->second.chain.task_ids = {task.task_id};
        snapshot_it->second.task.dispatched_at = std::chrono::steady_clock::now();
        emitSnapshotEvent(snapshot_it->second);
      }

      ActorRpcMessage rpc;
      rpc.action = ActorRpcAction::SubmitJob;
      rpc.job_id = task.job_id;
      rpc.chain_id = task.chain_id;
      rpc.task_id = task.task_id;
      rpc.node_id = config.node_id;
      rpc.attempt = task.attempt;
      rpc.ok = true;
      rpc.state = "SUBMITTED";
      rpc.summary = snapshot_it == job_snapshots.end() ? "" : snapshot_it->second.payload;
      rpc.payload = task.payload;
      emitRpcEvent("actor_scheduler", "task_dispatched", config.node_id, "TASK_DISPATCHED", task.job_id,
                   {observability::field("task_id", task.task_id),
                    observability::field("chain_id", task.chain_id),
                    observability::field("worker_id", worker_node)});
      if (sendFrameOverSocket(worker_fd, codec,
                              makeFrameFromMessage(next_message_id++, config.node_id, worker_node, rpc))) {
        continue;
      }

      idle_workers.insert(worker_fd);
      task_to_worker.erase(task.task_id);

      RemoteTaskCompletion completion;
      completion.job_id = task.job_id;
      completion.chain_id = task.chain_id;
      completion.task_id = task.task_id;
      completion.attempt = task.attempt;
      completion.ok = false;
      completion.error_message = "scheduler dispatch to worker failed";
      completion.worker_id = worker_node;
      JobMaster::instance().completeRemoteTask(completion);

      if (snapshot_it != job_snapshots.end()) {
        snapshot_it->second.worker_node = worker_node;
        snapshot_it->second.state = "FAILED";
        snapshot_it->second.status_code = "JOB_FAILED";
        snapshot_it->second.chain.state = "FAILED";
        snapshot_it->second.chain.status_code = "CHAIN_FAILED";
        snapshot_it->second.task.state = "FAILED";
        snapshot_it->second.task.status_code = "TASK_FAILED";
        snapshot_it->second.task.worker_id = worker_node;
        snapshot_it->second.task.fail_reason = completion.error_message;
        snapshot_it->second.task.worker_finished_at = std::chrono::steady_clock::now();
        snapshot_it->second.task.result_returned_at = snapshot_it->second.task.worker_finished_at;
        snapshot_it->second.result_payload = completion.error_message;
        emitSnapshotEvent(snapshot_it->second);
      }

      auto client_it = job_to_client.find(task.job_id);
      if (client_it != job_to_client.end()) {
        ActorRpcMessage failed;
        failed.action = ActorRpcAction::Result;
        failed.job_id = task.job_id;
        failed.chain_id = task.chain_id;
        failed.task_id = task.task_id;
        failed.node_id = config.node_id;
        failed.attempt = task.attempt;
        failed.ok = false;
        failed.state = "FAILED";
        failed.reason = completion.error_message;
        failed.summary = completion.error_message;
        failed.payload = completion.error_message;
        sendFrameOverSocket(client_it->second, codec,
                            makeFrameFromMessage(next_message_id++, config.node_id,
                                                 std::to_string(client_it->second), failed));
        job_to_client.erase(client_it);
      }
    }
  };

  while (!stop_requested.load()) {
    reapLocalWorkers();
    fd_set reads;
    FD_ZERO(&reads);
    FD_SET(server_fd, &reads);
    int max_fd = server_fd;
    for (int fd : conns) {
      FD_SET(fd, &reads);
      if (fd > max_fd) max_fd = fd;
    }

    struct timeval tick;
    tick.tv_sec = 0;
    tick.tv_usec = 200 * 1000;
    const int ready = select(max_fd + 1, &reads, nullptr, nullptr, &tick);
    dispatchPending();
    if (ready <= 0) {
      if (stop_requested.load()) break;
      continue;
    }

    if (FD_ISSET(server_fd, &reads)) {
      const int conn_fd = accept(server_fd, nullptr, nullptr);
      if (conn_fd >= 0) {
        conns.push_back(conn_fd);
        conn_role[conn_fd] = "unknown";
        emitRpcEvent("actor_scheduler", "connection_accepted", config.node_id, "RPC_CONNECTION_ACCEPTED", "",
                     {observability::field("fd", conn_fd)});
      }
    }

    for (std::size_t i = 0; i < conns.size();) {
      const int fd = conns[i];
      if (!FD_ISSET(fd, &reads)) {
        ++i;
        continue;
      }
      RpcFrame frame;
      if (!recvFrameOverSocket(fd, codec, &frame)) {
        cleanup(fd);
        continue;
      }
      if (frame.header.type == RpcMessageType::DataBatch) {
        auto pending_it = pending_worker_result_msgs.find(fd);
        if (pending_it == pending_worker_result_msgs.end()) {
          emitRpcEvent("actor_scheduler", "unexpected_data_batch", config.node_id,
                       "RPC_UNEXPECTED_DATA_BATCH", "",
                       {observability::field("fd", fd),
                        observability::field("codec_id", frame.header.codec_id)});
          ++i;
          continue;
        }
        Table result_table;
        if (!decodeDataBatchFrame(frame, &result_table)) {
          emitRpcEvent("actor_scheduler", "data_batch_decode_error", config.node_id,
                       "RPC_DATA_BATCH_DECODE_ERROR", pending_it->second.job_id,
                       {observability::field("fd", fd),
                        observability::field("task_id", pending_it->second.task_id)});
          pending_worker_result_msgs.erase(pending_it);
          ++i;
          continue;
        }

        const ActorRpcMessage& result_msg = pending_it->second;
        RemoteTaskCompletion completion;
        completion.job_id = result_msg.job_id;
        completion.chain_id = result_msg.chain_id;
        completion.task_id = result_msg.task_id;
        completion.attempt = static_cast<Attempt>(result_msg.attempt);
        completion.ok = true;
        completion.payload.clear();
        completion.error_message = result_msg.reason;
        completion.output_rows = static_cast<std::size_t>(result_msg.output_rows);
        completion.worker_id = result_msg.node_id;
        completion.result_table = result_table;
        completion.has_result_table = true;
        if (completion.output_rows == 0) {
          completion.output_rows = completion.result_table.rowCount();
        }
        JobMaster::instance().completeRemoteTask(completion);

        const auto worker_it = task_to_worker.find(result_msg.task_id);
        if (worker_it != task_to_worker.end()) {
          idle_workers.insert(worker_it->second);
          auto snap_it = job_snapshots.find(result_msg.job_id);
          if (snap_it != job_snapshots.end()) {
            snap_it->second.worker_node = result_msg.node_id;
            snap_it->second.result_payload =
                result_msg.summary.empty() ? result_msg.reason : result_msg.summary;
            snap_it->second.state = "FINISHED";
            snap_it->second.status_code = "JOB_FINISHED";
            snap_it->second.chain.state = "FINISHED";
            snap_it->second.chain.status_code = "CHAIN_FINISHED";
            snap_it->second.task.state = "FINISHED";
            snap_it->second.task.status_code = "TASK_FINISHED";
            snap_it->second.task.worker_id = result_msg.node_id;
            snap_it->second.task.fail_reason.clear();
            snap_it->second.task.worker_finished_at = std::chrono::steady_clock::now();
            snap_it->second.task.result_returned_at = snap_it->second.task.worker_finished_at;
            emitSnapshotEvent(snap_it->second);
          }
          task_to_worker.erase(worker_it);
        }

        const auto client_it = job_to_client.find(result_msg.job_id);
        if (client_it != job_to_client.end()) {
          const uint64_t control_id = next_message_id++;
          ActorRpcMessage forward = result_msg;
          forward.payload.clear();
          if (sendFrameOverSocket(client_it->second, codec,
                                  makeFrameFromMessage(control_id, 0, config.node_id,
                                                       std::to_string(client_it->second),
                                                       forward))) {
            sendFrameOverSocket(client_it->second, codec,
                                makeDataBatchFrame(next_message_id++, control_id, config.node_id,
                                                   std::to_string(client_it->second),
                                                   completion.result_table));
          }
          job_to_client.erase(client_it);
        }
        pending_worker_result_msgs.erase(pending_it);
        dispatchPending();
        ++i;
        continue;
      }
      ActorRpcMessage msg;
      if (!decodeActorRpcMessage(frame.payload, &msg)) {
        emitRpcEvent("actor_scheduler", "decode_error", config.node_id, "RPC_DECODE_ERROR", "",
                     {observability::field("fd", fd)});
        ++i;
        continue;
      }

      if (msg.action == ActorRpcAction::RegisterWorker) {
        conn_role[fd] = "worker";
        conn_node[fd] = msg.node_id;
        worker_node_by_fd[fd] = msg.node_id.empty() ? ("worker-" + std::to_string(fd)) : msg.node_id;
        if (std::find(workers.begin(), workers.end(), fd) == workers.end()) {
          workers.push_back(fd);
        }
        if (isLocalWorkerNodeId(worker_node_by_fd[fd], config.node_id)) {
          const auto proc_it = local_worker_node_to_pid.find(worker_node_by_fd[fd]);
          if (proc_it != local_worker_node_to_pid.end()) {
            const auto status_it = local_worker_processes.find(proc_it->second);
            if (status_it != local_worker_processes.end() && !status_it->second.registered) {
              status_it->second.registered = true;
              if (launching_local_workers > 0) {
                --launching_local_workers;
              }
            }
          }
          local_worker_nodes.insert(worker_node_by_fd[fd]);
        }
        idle_workers.insert(fd);
        ActorRpcMessage ack;
        ack.action = ActorRpcAction::Ack;
        ack.job_id = msg.job_id;
        ack.node_id = config.node_id;
        ack.ok = true;
        ack.reason = "worker-registered";
        sendTo(fd, ack);
        emitRpcEvent("actor_scheduler", "worker_registered", config.node_id, "WORKER_REGISTERED", "",
                     {observability::field("fd", fd), observability::field("worker_node", worker_node_by_fd[fd])});
        dispatchPending();
      } else if (msg.action == ActorRpcAction::SubmitJob) {
        conn_role[fd] = "client";
        conn_node[fd] = msg.node_id;
        if (workers.empty()) {
          ensureLocalWorkers();
        }
        if (workers.empty() && !config.auto_worker) {
          ActorRpcMessage nack;
          nack.action = ActorRpcAction::Ack;
          nack.node_id = config.node_id;
          nack.ok = false;
          nack.state = "FAILED";
          nack.reason = "no-worker-available";
          nack.summary = "scheduler reject: no-worker-available";
          sendTo(fd, nack);
          ++i;
          continue;
        }

        auto handle = JobMaster::instance().submitRemote(msg.payload);
        msg.job_id = handle.id();
        auto& snapshot = job_snapshots[msg.job_id];
        snapshot.job_id = msg.job_id;
        snapshot.client_node = msg.node_id;
        snapshot.payload = msg.summary.empty() ? "plan submitted" : msg.summary;
        snapshot.state = "SUBMITTED";
        snapshot.status_code = "JOB_SUBMITTED";
        snapshot.task.queued_at = std::chrono::steady_clock::now();
        emitRpcEvent("actor_scheduler", "job_snapshot", config.node_id, snapshot.status_code, msg.job_id,
                     {observability::field("snapshot", snapshotToJson(snapshot), true)});

        job_to_client[msg.job_id] = fd;
        ActorRpcMessage ack;
        ack.action = ActorRpcAction::Ack;
        ack.job_id = msg.job_id;
        ack.node_id = config.node_id;
        ack.ok = true;
        ack.state = "SUBMITTED";
        ack.summary = snapshot.payload;
        ack.payload = snapshot.payload;
        ack.reason = "accepted";
        sendTo(fd, ack);
        emitRpcEvent("actor_scheduler", "job_accepted", config.node_id, "JOB_SUBMITTED", msg.job_id,
                     {observability::field("client_fd", fd)});
        dispatchPending();
      } else if (msg.action == ActorRpcAction::Result) {
        if (msg.ok && !msg.result_location.empty()) {
          pending_worker_result_msgs[fd] = msg;
          ++i;
          continue;
        }

        RemoteTaskCompletion completion;
        completion.job_id = msg.job_id;
        completion.chain_id = msg.chain_id;
        completion.task_id = msg.task_id;
        completion.attempt = static_cast<Attempt>(msg.attempt);
        completion.ok = msg.ok;
        completion.payload = msg.payload;
        completion.error_message = msg.reason;
        completion.output_rows = static_cast<std::size_t>(msg.output_rows);
        completion.worker_id = msg.node_id;
        JobMaster::instance().completeRemoteTask(completion);

        const auto worker_it = task_to_worker.find(msg.task_id);
        if (worker_it != task_to_worker.end()) {
          idle_workers.insert(worker_it->second);
          auto snap_it = job_snapshots.find(msg.job_id);
          if (snap_it != job_snapshots.end()) {
            snap_it->second.worker_node = msg.node_id;
            snap_it->second.result_payload = msg.summary.empty() ? msg.reason : msg.summary;
            snap_it->second.state = msg.ok ? "FINISHED" : "FAILED";
            snap_it->second.status_code = msg.ok ? "JOB_FINISHED" : "JOB_FAILED";
            snap_it->second.chain.state = msg.ok ? "FINISHED" : "FAILED";
            snap_it->second.chain.status_code = msg.ok ? "CHAIN_FINISHED" : "CHAIN_FAILED";
            snap_it->second.task.state = msg.ok ? "FINISHED" : "FAILED";
            snap_it->second.task.status_code = msg.ok ? "TASK_FINISHED" : "TASK_FAILED";
            snap_it->second.task.worker_id = msg.node_id;
            snap_it->second.task.fail_reason = msg.ok ? "" : msg.reason;
            snap_it->second.task.worker_finished_at = std::chrono::steady_clock::now();
            snap_it->second.task.result_returned_at = snap_it->second.task.worker_finished_at;
            emitSnapshotEvent(snap_it->second);
          }
          task_to_worker.erase(worker_it);
        }

        const auto client_it = job_to_client.find(msg.job_id);
        if (client_it == job_to_client.end()) {
          dispatchPending();
          ++i;
          continue;
        }
        ActorRpcMessage forward = msg;
        forward.payload = msg.ok ? msg.summary : (msg.summary.empty() ? msg.reason : msg.summary);
        sendFrameOverSocket(client_it->second, codec,
                            makeFrameFromMessage(next_message_id++, 0, config.node_id,
                                                 std::to_string(client_it->second), forward));
        job_to_client.erase(client_it);
        dispatchPending();
      } else if (msg.action == ActorRpcAction::Heartbeat) {
        if (!msg.job_id.empty() && !msg.task_id.empty()) {
          TaskHeartbeat heartbeat;
          heartbeat.job_id = msg.job_id;
          heartbeat.chain_id = msg.chain_id;
          heartbeat.task_id = msg.task_id;
          heartbeat.attempt = static_cast<Attempt>(msg.attempt);
          heartbeat.worker_id = msg.node_id;
          heartbeat.heartbeat_seq = msg.heartbeat_seq == 0 ? 1 : msg.heartbeat_seq;
          heartbeat.at = std::chrono::steady_clock::now();
          heartbeat.output_rows = static_cast<std::size_t>(msg.output_rows);
          JobMaster::instance().heartbeat(heartbeat);
          auto snap_it = job_snapshots.find(msg.job_id);
        if (snap_it != job_snapshots.end()) {
          snap_it->second.worker_node = msg.node_id;
            snap_it->second.chain.chain_id = msg.chain_id;
            snap_it->second.task.task_id = msg.task_id;
            snap_it->second.state = "RUNNING";
            snap_it->second.status_code = "JOB_RUNNING";
            snap_it->second.chain.state = "RUNNING";
            snap_it->second.chain.status_code = "CHAIN_RUNNING";
          snap_it->second.task.state = "RUNNING";
          snap_it->second.task.status_code = "TASK_RUNNING";
          snap_it->second.task.worker_id = msg.node_id;
          if (snap_it->second.task.worker_started_at == std::chrono::steady_clock::time_point()) {
            snap_it->second.task.worker_started_at = std::chrono::steady_clock::now();
          }
          emitSnapshotEvent(snap_it->second);
        }
        }
        ActorRpcMessage ack;
        ack.action = ActorRpcAction::Ack;
        ack.job_id = msg.job_id;
        ack.chain_id = msg.chain_id;
        ack.task_id = msg.task_id;
        ack.node_id = config.node_id;
        ack.ok = true;
        ack.state = "RUNNING";
        ack.payload = "pong";
        sendTo(fd, ack);
        emitRpcEvent("actor_scheduler", "heartbeat_ack", config.node_id, "RPC_HEARTBEAT_ACK", msg.job_id,
                     {observability::field("fd", fd)});
      }
      ++i;
    }

  }

  for (const int fd : conns) {
    close(fd);
  }
  ::close(server_fd);
  stopAllLocalWorkers();
  ensureSignalRestored();
  return 0;
}

int runActorWorker(const ActorRuntimeConfig& config) {
  if (config.single_node) {
    std::cout << "[worker] single node mode not using sockets. exit.\n";
    return 0;
  }

  std::string host;
  uint16_t port = 0;
  if (!parseConfigEndpoint(config.connect_address, &host, &port)) {
    std::cerr << "Invalid --connect endpoint: " << config.connect_address << "\n";
    return 1;
  }

  const int fd = createClientSocket(host, port);
  if (fd < 0) {
    std::cerr << "[worker] cannot connect " << config.connect_address << "\n";
    return 1;
  }

  std::cout << "[worker] connected " << config.connect_address << "\n";
  emitRpcEvent("actor_worker", "worker_connected", config.node_id, "WORKER_CONNECTED", "",
               {observability::field("scheduler", config.connect_address)});
  LengthPrefixedFrameCodec codec;
  std::atomic<uint64_t> next_message_id{1};

  ActorRpcMessage reg;
  reg.action = ActorRpcAction::RegisterWorker;
  reg.node_id = config.node_id;
  sendFrameOverSocket(fd, codec,
                      makeFrameFromMessage(next_message_id.fetch_add(1), config.node_id, "scheduler", reg));
  emitRpcEvent("actor_worker", "register_sent", config.node_id, "WORKER_REGISTER_SENT", "",
               {observability::field("scheduler", config.connect_address)});

  while (true) {
    RpcFrame frame;
    if (!recvFrameOverSocket(fd, codec, &frame)) {
      emitRpcEvent("actor_worker", "connection_closed", config.node_id, "RPC_CONNECTION_CLOSED", "");
      return 1;
    }
    ActorRpcMessage msg;
    if (!decodeActorRpcMessage(frame.payload, &msg)) {
      continue;
    }
    if (msg.action == ActorRpcAction::SubmitJob) {
      emitRpcEvent("actor_worker", "job_received", config.node_id, "TASK_RUNNING", msg.job_id,
                   {observability::field("task_id", msg.task_id),
                    observability::field("chain_id", msg.chain_id),
                    observability::field("summary", msg.summary),
                    observability::field("payload_bytes", msg.payload.size())});

      auto sendHeartbeat = [&](uint64_t seq, uint64_t output_rows) {
        ActorRpcMessage heartbeat;
        heartbeat.action = ActorRpcAction::Heartbeat;
        heartbeat.job_id = msg.job_id;
        heartbeat.chain_id = msg.chain_id;
        heartbeat.task_id = msg.task_id;
        heartbeat.node_id = config.node_id;
        heartbeat.attempt = msg.attempt;
        heartbeat.heartbeat_seq = seq;
        heartbeat.output_rows = output_rows;
        heartbeat.ok = true;
        heartbeat.state = "RUNNING";
        sendFrameOverSocket(fd, codec,
                            makeFrameFromMessage(next_message_id.fetch_add(1), config.node_id,
                                                 "scheduler", heartbeat));
      };

      std::atomic<bool> stop_heartbeat{false};
      std::atomic<uint64_t> heartbeat_seq{1};
      sendHeartbeat(heartbeat_seq.fetch_add(1), 0);
      std::thread heartbeat_thread([&]() {
        while (!stop_heartbeat.load()) {
          std::this_thread::sleep_for(std::chrono::milliseconds(1000));
          if (stop_heartbeat.load()) break;
          sendHeartbeat(heartbeat_seq.fetch_add(1), 0);
        }
      });

      ActorRpcMessage result;
      Table output_table;
      bool has_output_table = false;
      std::size_t result_batch_bytes = 0;
      RpcFrame result_batch_frame;
      bool has_result_batch_frame = false;
      result.action = ActorRpcAction::Result;
      result.job_id = msg.job_id;
      result.chain_id = msg.chain_id;
      result.task_id = msg.task_id;
      result.node_id = config.node_id;
      result.attempt = msg.attempt;
      try {
        const auto plan = deserializePlan(msg.payload);
        const LocalExecutor executor;
        output_table = executor.execute(plan);
        has_output_table = true;
        emitRpcEvent("actor_worker", "task_executed", config.node_id, "TASK_EXECUTED", msg.job_id,
                     {observability::field("task_id", msg.task_id),
                      observability::field("chain_id", msg.chain_id),
                      observability::field("output_rows", output_table.rowCount())});
        result.ok = true;
        result.state = "FINISHED";
        result.output_rows = output_table.rowCount();
        result.result_location = "inline://" + msg.job_id + "/" + msg.task_id;
        result.summary = summarizeTable(output_table);
        result.payload.clear();
      } catch (const std::exception& e) {
        result.ok = false;
        result.state = "FAILED";
        result.reason = e.what();
        result.summary = std::string("execution failed: ") + e.what();
      }

      stop_heartbeat.store(true);
      if (heartbeat_thread.joinable()) heartbeat_thread.join();
      const uint64_t control_id = next_message_id.fetch_add(1);
      if (result.ok && has_output_table) {
        result_batch_frame =
            makeDataBatchFrame(next_message_id.fetch_add(1), control_id, config.node_id,
                               "scheduler", output_table);
        result_batch_bytes = result_batch_frame.payload.size();
        has_result_batch_frame = true;
      }
      RpcFrame control_frame =
          makeFrameFromMessage(control_id, 0, config.node_id, "scheduler", result);
      emitRpcEvent("actor_worker", "result_serialized", config.node_id, "TASK_RESULT_SERIALIZED", msg.job_id,
                   {observability::field("task_id", msg.task_id),
                    observability::field("chain_id", msg.chain_id),
                    observability::field("control_payload_bytes", control_frame.payload.size()),
                    observability::field("result_batch_bytes", result_batch_bytes),
                    observability::field("summary", result.summary)});
      sendFrameOverSocket(fd, codec, std::move(control_frame));
      if (has_result_batch_frame) {
        sendFrameOverSocket(fd, codec, std::move(result_batch_frame));
      }
      emitRpcEvent("actor_worker", "job_completed", config.node_id,
                   result.ok ? "TASK_FINISHED" : "TASK_FAILED", msg.job_id,
                   {observability::field("task_id", msg.task_id),
                    observability::field("summary", result.summary)});
    } else if (msg.action == ActorRpcAction::Ack) {
      emitRpcEvent("actor_worker", "ack_received", config.node_id, msg.ok ? "ACK_OK" : "ACK_ERROR", msg.job_id,
                   {observability::field("reason", msg.reason)});
    } else if (msg.action == ActorRpcAction::Heartbeat) {
      ActorRpcMessage pong;
      pong.action = ActorRpcAction::Heartbeat;
      pong.job_id = msg.job_id;
      pong.node_id = config.node_id;
      pong.ok = true;
      pong.payload = "pong";
      sendFrameOverSocket(fd, codec,
                          makeFrameFromMessage(next_message_id.fetch_add(1), config.node_id,
                                               "scheduler", pong));
      emitRpcEvent("actor_worker", "heartbeat_pong", config.node_id, "RPC_HEARTBEAT_PONG", msg.job_id);
    }
  }
}

int runActorClient(const ActorRuntimeConfig& config,
                   const std::string& payload,
                   const std::string& sql) {
  if (config.single_node) {
    std::cout << "[client] single node mode: submit bypassed: " << payload << "\n";
    return 0;
  }

  std::string host;
  uint16_t port = 0;
  if (!parseConfigEndpoint(config.connect_address, &host, &port)) {
    std::cerr << "Invalid --connect endpoint: " << config.connect_address << "\n";
    return 1;
  }

  const int fd = createClientSocket(host, port);
  if (fd < 0) {
    std::cerr << "[client] cannot connect " << config.connect_address << "\n";
    return 1;
  }

  LengthPrefixedFrameCodec codec;
  uint64_t next_message_id = 1;
  DataFrame plan_df;
  try {
    plan_df = buildClientPlan(payload, sql);
  } catch (const std::exception& e) {
    std::cerr << "[client] failed to build plan: " << e.what() << "\n";
    return 1;
  }

  emitRpcEvent("actor_client", "client_connected", config.node_id, "CLIENT_CONNECTED", "",
               {observability::field("scheduler", config.connect_address)});
  ActorRpcMessage submit;
  submit.action = ActorRpcAction::SubmitJob;
  submit.job_id = "";
  submit.node_id = config.node_id;
  submit.ok = true;
  submit.state = "SUBMITTED";
  submit.summary = flattenText(sql.empty() ? plan_df.explain() : sql);
  submit.payload = plan_df.serializePlan();
  if (!sendFrameOverSocket(fd, codec,
                           makeFrameFromMessage(next_message_id++, config.node_id,
                                                "scheduler", submit))) {
    emitRpcEvent("actor_client", "submit_send_failed", config.node_id, "JOB_SUBMIT_SEND_FAILED", "");
    return 1;
  }
  emitRpcEvent("actor_client", "submit_sent", config.node_id, "JOB_SUBMIT_SENT", "",
               {observability::field("plan_summary", submit.summary)});

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  bool awaiting_result = true;
  bool awaiting_result_table = false;
  uint64_t expected_result_correlation = 0;
  ActorRpcMessage pending_result;
  while (awaiting_result) {
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
      emitRpcEvent("actor_client", "wait_timeout", config.node_id, "JOB_RESULT_TIMEOUT", submit.job_id);
      std::cerr << "[client] timed out waiting for job result within 10s\n";
      return 1;
    }
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
    if (!waitForReadable(fd, remaining)) {
      emitRpcEvent("actor_client", "wait_timeout", config.node_id, "JOB_RESULT_TIMEOUT", submit.job_id);
      std::cerr << "[client] timed out waiting for job result within 10s\n";
      return 1;
    }
    RpcFrame frame;
    if (!recvFrameOverSocket(fd, codec, &frame)) {
      emitRpcEvent("actor_client", "connection_closed", config.node_id, "RPC_CONNECTION_CLOSED", submit.job_id);
      return 1;
    }
    if (frame.header.type == RpcMessageType::DataBatch) {
      if (!awaiting_result_table || frame.header.correlation_id != expected_result_correlation) {
        continue;
      }
      Table result_table;
      if (!decodeDataBatchFrame(frame, &result_table)) {
        emitRpcEvent("actor_client", "result_decode_failed", config.node_id,
                     "JOB_RESULT_DECODE_FAILED", pending_result.job_id);
        std::cerr << "[client] job result decode failed\n";
        return 1;
      }
      const std::string summary = summarizeTable(result_table);
      emitRpcEvent("actor_client", "job_result", config.node_id, "JOB_RESULT_RECEIVED",
                   pending_result.job_id,
                   {observability::field("payload", summary),
                    observability::field("task_id", pending_result.task_id),
                    observability::field("chain_id", pending_result.chain_id),
                    observability::field("result_location", pending_result.result_location),
                    observability::field("summary", pending_result.summary)});
      std::cout << "[client] job result: " << summary;
      if (!pending_result.result_location.empty()) {
        std::cout << " @ " << pending_result.result_location;
      }
      std::cout << "\n";
      awaiting_result_table = false;
      awaiting_result = false;
      continue;
    }
    ActorRpcMessage msg;
    if (!decodeActorRpcMessage(frame.payload, &msg)) {
      continue;
    }
    if (msg.action == ActorRpcAction::Ack) {
      if (!msg.ok) {
        emitRpcEvent("actor_client", "job_rejected", config.node_id, "JOB_REJECTED", msg.job_id,
                     {observability::field("reason", msg.reason)});
        return 1;
      }
      emitRpcEvent("actor_client", "job_accepted", config.node_id, "JOB_ACCEPTED", msg.job_id,
                   {observability::field("reason", msg.reason)});
      std::cout << "[client] job accepted: " << msg.job_id << "\n";
    } else if (msg.action == ActorRpcAction::Result) {
      if (!msg.ok) {
        std::cerr << "[client] job failed: "
                  << (msg.payload.empty() ? msg.reason : msg.payload) << "\n";
        return 1;
      }
      if (!msg.result_location.empty()) {
        pending_result = msg;
        awaiting_result_table = true;
        expected_result_correlation = frame.header.message_id;
        continue;
      }
      emitRpcEvent("actor_client", "job_result", config.node_id, "JOB_RESULT_RECEIVED", msg.job_id,
                   {observability::field("payload", msg.summary.empty() ? msg.payload : msg.summary),
                    observability::field("task_id", msg.task_id),
                    observability::field("chain_id", msg.chain_id),
                    observability::field("result_location", msg.result_location),
                    observability::field("summary", msg.summary)});
      std::cout << "[client] job result: "
                << (msg.summary.empty() ? msg.payload : msg.summary);
      if (!msg.result_location.empty()) {
        std::cout << " @ " << msg.result_location;
      }
      std::cout << "\n";
      awaiting_result = false;
    }
  }
  return 0;
}

int runActorSmoke() {
  ActorRpcMessage origin;
  origin.action = ActorRpcAction::SubmitJob;
  origin.job_id = "smoke-job";
  origin.chain_id = "smoke-chain";
  origin.task_id = "smoke-task";
  origin.node_id = "smoke-node";
  origin.attempt = 2;
  origin.heartbeat_seq = 7;
  origin.output_rows = 3;
  origin.ok = true;
  origin.state = "FINISHED";
  origin.summary = "rows=3";
  origin.result_location = "inline://smoke-job/smoke-task";
  origin.payload = "hello";
  origin.reason = "smoke";
  const auto raw = encodeActorRpcMessage(origin);
  ActorRpcMessage copy;
  if (!decodeActorRpcMessage(raw, &copy)) {
    std::cerr << "[smoke] codec roundtrip failed\n";
    return 1;
  }
  if (copy.action != origin.action || copy.job_id != origin.job_id ||
      copy.chain_id != origin.chain_id || copy.task_id != origin.task_id ||
      copy.node_id != origin.node_id || copy.attempt != origin.attempt ||
      copy.heartbeat_seq != origin.heartbeat_seq || copy.output_rows != origin.output_rows ||
      copy.state != origin.state || copy.summary != origin.summary ||
      copy.result_location != origin.result_location || copy.payload != origin.payload ||
      copy.reason != origin.reason || copy.ok != origin.ok) {
    std::cerr << "[smoke] codec mismatch\n";
    return 1;
  }
  RpcFrame control_frame = makeFrameFromMessage(17, "smoke-worker", "smoke-client", origin);
  Table result_table;
  result_table.schema = Schema({"token", "score"});
  result_table.rows = {
      {Value("smoke"), Value(42.0)},
      {Value("payload"), Value(7.0)},
  };
  RpcFrame batch_frame =
      makeDataBatchFrame(18, control_frame.header.message_id, "smoke-worker", "smoke-client",
                         result_table);

  LengthPrefixedFrameCodec frame_codec;
  std::size_t consumed = 0;
  RpcFrame decoded_control;
  const auto control_wire = frame_codec.encode(control_frame);
  if (!frame_codec.decode(control_wire, &decoded_control, &consumed)) {
    std::cerr << "[smoke] control frame roundtrip failed\n";
    return 1;
  }
  ActorRpcMessage framed_copy;
  if (!decodeActorRpcMessage(decoded_control.payload, &framed_copy) ||
      framed_copy.summary != origin.summary) {
    std::cerr << "[smoke] control payload decode mismatch\n";
    return 1;
  }

  RpcFrame decoded_batch;
  const auto batch_wire = frame_codec.encode(batch_frame);
  if (!frame_codec.decode(batch_wire, &decoded_batch, &consumed)) {
    std::cerr << "[smoke] data batch frame roundtrip failed\n";
    return 1;
  }
  if (decoded_batch.header.correlation_id != decoded_control.header.message_id) {
    std::cerr << "[smoke] data batch correlation mismatch\n";
    return 1;
  }
  Table batch_copy;
  if (!decodeDataBatchFrame(decoded_batch, &batch_copy) || batch_copy.rowCount() != result_table.rowCount()) {
    std::cerr << "[smoke] data batch decode mismatch\n";
    return 1;
  }
  std::cout << "[smoke] actor rpc codec roundtrip ok (control/data-batch)\n";
  return 0;
}

}  // namespace dataflow
