#include "src/dataflow/runner/actor_runtime.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <cctype>
#include <string>
#include <sys/select.h>
#include <sys/socket.h>
#include <filesystem>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <unistd.h>

#include "src/dataflow/api/dataframe.h"
#include "src/dataflow/api/session.h"
#include "src/dataflow/rpc/actor_rpc_codec.h"
#include "src/dataflow/rpc/rpc_codec.h"
#include "src/dataflow/runtime/job_master.h"
#include "src/dataflow/runtime/observability.h"
#include "src/dataflow/serial/serializer.h"
#include "src/dataflow/transport/ipc_transport.h"

namespace dataflow {

namespace {

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

std::string toLowerCopy(std::string value) {
  for (char& ch : value) {
    ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
  }
  return value;
}

std::string toUpperCopy(std::string value) {
  for (char& ch : value) {
    ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
  }
  return value;
}

std::string urlDecode(const std::string& input) {
  std::string out;
  out.reserve(input.size());
  auto hexToInt = [](char h) -> int {
    if (h >= '0' && h <= '9') return h - '0';
    if (h >= 'a' && h <= 'f') return 10 + (h - 'a');
    if (h >= 'A' && h <= 'F') return 10 + (h - 'A');
    return -1;
  };
  for (std::size_t i = 0; i < input.size(); ++i) {
    const char ch = input[i];
    if (ch == '+') {
      out.push_back(' ');
      continue;
    }
    if (ch == '%' && i + 2 < input.size()) {
      const int hi = hexToInt(input[i + 1]);
      const int lo = hexToInt(input[i + 2]);
      if (hi >= 0 && lo >= 0) {
        out.push_back(static_cast<char>((hi << 4) | lo));
        i += 2;
        continue;
      }
    }
    out.push_back(ch);
  }
  return out;
}

std::unordered_map<std::string, std::string> parseFormBody(const std::string& body) {
  std::unordered_map<std::string, std::string> out;
  std::size_t start = 0;
  while (start < body.size()) {
    const std::size_t sep = body.find('&', start);
    const std::string pair = body.substr(start, sep == std::string::npos ? std::string::npos : sep - start);
    if (!pair.empty()) {
      const std::size_t eq = pair.find('=');
      if (eq == std::string::npos) {
        out[urlDecode(pair)] = "";
      } else {
        out[urlDecode(pair.substr(0, eq))] = urlDecode(pair.substr(eq + 1));
      }
    }
    if (sep == std::string::npos) break;
    start = sep + 1;
  }
  return out;
}

void sendHttpResponse(int fd, int code, const std::string& reason, const std::string& body,
                     const std::string& content_type) {
  std::ostringstream out;
  out << "HTTP/1.1 " << code << " " << reason << "\r\n"
      << "Content-Type: " << content_type << "; charset=utf-8\r\n"
      << "Content-Length: " << body.size() << "\r\n"
      << "Access-Control-Allow-Origin: *\r\n"
      << "Connection: close\r\n"
      << "\r\n"
      << body;
  const std::string text = out.str();
  sendAllBytes(fd, reinterpret_cast<const uint8_t*>(text.data()), text.size());
}

bool readHttpRequest(int fd, std::string* method, std::string* path, std::string* body) {
  if (!method || !path || !body) return false;
  method->clear();
  path->clear();
  body->clear();

  std::string request;
  std::size_t content_length = 0;
  std::size_t header_end = std::string::npos;
  while (true) {
    char buffer[4096];
    const ssize_t n = ::recv(fd, buffer, sizeof(buffer), 0);
    if (n <= 0) return false;
    request.append(buffer, n);
    if (header_end == std::string::npos) {
      header_end = request.find("\r\n\r\n");
      if (header_end == std::string::npos) {
        if (request.size() > 64 * 1024) return false;
        continue;
      }
      std::istringstream header_stream(request.substr(0, header_end));
      if (!(header_stream >> *method >> *path)) return false;
      std::string version;
      header_stream >> version;
      if (version.empty()) return false;
      std::string line;
      while (std::getline(header_stream, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        const std::size_t split = line.find(':');
        if (split == std::string::npos) continue;
        const std::string key = toLowerCopy(line.substr(0, split));
        const std::string value = line.substr(split + 1);
        if (key == "content-length") {
          try {
            content_length = static_cast<std::size_t>(std::stoul(value));
          } catch (...) {
            content_length = 0;
          }
        }
      }
      *method = toUpperCopy(*method);
    }
    const std::size_t header_pos = header_end + 4;
    if (request.size() < header_pos) return false;
    const std::size_t body_available = request.size() - header_pos;
    if (body_available < content_length) {
      if (request.size() > 128 * 1024) return false;
      continue;
    }
    body->assign(request.substr(header_pos, content_length));
    return true;
  }
}

std::string dashboardRoot() {
  static const std::string root = [] {
    auto fileExists = [](const std::string& path) {
      return std::ifstream(path).good();
    };
    auto appendDash = [](const std::string& dir) {
      return dir + "/src/dataflow/runner/dashboard";
    };

    auto testCandidate = [&](const std::string& candidate) {
      if (fileExists(candidate + "/index.html")) {
        return candidate;
      }
      return std::string();
    };

    std::string source_file(__FILE__);
    const std::size_t sep = source_file.find_last_of("/\\");
    if (sep != std::string::npos) {
      const std::string candidate = source_file.substr(0, sep) + "/dashboard";
      const std::string hit = testCandidate(candidate);
      if (!hit.empty()) return hit;
    }

    if (const char* workspace = std::getenv("BUILD_WORKSPACE_DIRECTORY")) {
      const std::string candidate = appendDash(workspace);
      const std::string hit = testCandidate(candidate);
      if (!hit.empty()) return hit;
    }

    const std::string cwd = std::filesystem::current_path().string();
    std::filesystem::path path = std::filesystem::path(cwd);
    for (int i = 0; i < 10; ++i) {
      const std::string candidate = appendDash(path.string());
      const std::string hit = testCandidate(candidate);
      if (!hit.empty()) return hit;
      if (!path.has_parent_path()) break;
      path = path.parent_path();
    }

    return appendDash(".");
  }();
  return root;
}

std::string dashboardMimeType(const std::string& path) {
  if (path.size() >= 5 && path.compare(path.size() - 5, 5, ".html") == 0) return "text/html";
  if (path.size() >= 3 && path.compare(path.size() - 3, 3, ".js") == 0) return "text/javascript";
  if (path.size() >= 3 && path.compare(path.size() - 3, 3, ".ts") == 0) return "text/plain";
  if (path.size() >= 4 && path.compare(path.size() - 4, 4, ".css") == 0) return "text/css";
  return "text/plain";
}

std::string dashboardReadFile(const std::string& relative_path, bool* ok) {
  std::string full_path = dashboardRoot() + relative_path;
  std::ifstream file(full_path, std::ios::binary);
  if (!file.is_open()) {
    if (ok) *ok = false;
    return {};
  }
  if (ok) *ok = true;
  std::ostringstream out;
  out << file.rdbuf();
  return out.str();
}

void serveDashboardAsset(int fd, const std::string& raw_path) {
  std::string path = raw_path;
  const std::size_t qmark = path.find('?');
  if (qmark != std::string::npos) path = path.substr(0, qmark);
  if (path.empty() || path == "/") path = "/index.html";
  if (path.front() != '/') path = "/" + path;

  if (path.find("..") != std::string::npos) {
    sendHttpResponse(fd, 403, "Forbidden", R"json({"ok":false,"message":"invalid file path"})json",
                     "application/json");
    return;
  }

  bool ok = false;
  const std::string content = dashboardReadFile(path, &ok);
  if (!ok) {
    sendHttpResponse(fd, 404, "Not Found", R"json({"ok":false,"message":"not found"})json",
                     "application/json");
    return;
  }
  sendHttpResponse(fd, 200, "OK", content, dashboardMimeType(path));
}

struct RpcTaskSnapshot {
  std::string task_id;
  std::string state = "SUBMITTED";
  std::string status_code = "TASK_SUBMITTED";
  std::string worker_id;
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
  std::string result_payload;
  RpcChainSnapshot chain;
  RpcTaskSnapshot task;
};

std::string snapshotToJson(const RpcJobSnapshot& snapshot) {
  using namespace observability;
  const std::string task_json = object({
      field("task_id", snapshot.task.task_id),
      field("state", snapshot.task.state),
      field("status_code", snapshot.task.status_code),
      field("worker_id", snapshot.task.worker_id),
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
      field("result_payload", snapshot.result_payload),
      field("chain", chain_json, true),
      field("task", task_json, true),
  });
}

std::string buildJobsJson(const std::unordered_map<std::string, RpcJobSnapshot>& job_snapshots) {
  std::vector<std::string> jobs;
  std::vector<std::string> job_ids;
  job_ids.reserve(job_snapshots.size());
  for (const auto& item : job_snapshots) {
    job_ids.push_back(item.first);
  }
  std::sort(job_ids.begin(), job_ids.end());
  for (const auto& job_id : job_ids) {
    const auto it = job_snapshots.find(job_id);
    if (it != job_snapshots.end()) {
      jobs.push_back(snapshotToJson(it->second));
    }
  }
  using namespace observability;
  return object({
      field("count", static_cast<uint64_t>(jobs.size())),
      field("jobs", array(jobs), true),
  });
}

RpcFrame makeFrameFromMessage(uint64_t msg_id,
                              const std::string& source,
                              const std::string& target,
                              const ActorRpcMessage& message) {
  RpcFrame frame;
  frame.header.protocol_version = 1;
  frame.header.type = RpcMessageType::Control;
  frame.header.message_id = msg_id;
  frame.header.correlation_id = 0;
  frame.header.codec_id = "actor-rpc-v1";
  frame.header.source = source;
  frame.header.target = target;
  frame.payload = encodeActorRpcMessage(message);
  return frame;
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

  std::string dashboard_host;
  uint16_t dashboard_port = 0;
  int dashboard_server_fd = -1;
  if (config.dashboard_enabled) {
    if (!parseConfigEndpoint(config.dashboard_listen_address, &dashboard_host, &dashboard_port)) {
      std::cerr << "Invalid --dashboard-listen endpoint: " << config.dashboard_listen_address << "\n";
      return 1;
    }
    dashboard_server_fd = createServerSocket(dashboard_host, dashboard_port);
    if (dashboard_server_fd < 0) {
      std::cerr << "scheduler failed to listen dashboard on " << config.dashboard_listen_address << "\n";
      return 1;
    }
    std::cout << "[dashboard] listen " << config.dashboard_listen_address << "\n";
  }

  LengthPrefixedFrameCodec codec;
  std::vector<int> conns;
  std::unordered_map<int, std::string> conn_role;
  std::unordered_map<int, std::string> conn_node;
  std::unordered_map<int, std::string> worker_node_by_fd;
  std::vector<int> workers;
  std::unordered_set<int> idle_workers;
  std::unordered_map<std::string, int> job_to_client;
  std::unordered_map<std::string, int> task_to_worker;
  std::unordered_map<std::string, RpcJobSnapshot> job_snapshots;
  std::vector<int> dashboard_conns;
  uint64_t next_message_id = 1;

  auto emitSnapshotEvent = [&](const RpcJobSnapshot& snapshot) {
    emitRpcEvent("actor_scheduler", "job_snapshot", config.node_id, snapshot.status_code, snapshot.job_id,
                 {observability::field("snapshot", snapshotToJson(snapshot), true)});
  };

  auto submitDashboardJob = [&](const std::string& payload_input,
                               const std::string& sql,
                               std::string* job_id_out,
                               std::string* summary_out) -> bool {
    if (workers.empty()) {
      if (job_id_out) job_id_out->clear();
      if (summary_out) summary_out->assign("scheduler reject: no worker available");
      emitRpcEvent("actor_scheduler", "dashboard_submit", config.node_id,
                   "JOB_SUBMIT_REJECTED", "",
                   {observability::field("reason", "no worker available")});
      return false;
    }

    DataFrame plan_df;
    try {
      plan_df = buildClientPlan(payload_input, sql);
    } catch (const std::exception& e) {
      if (job_id_out) job_id_out->clear();
      if (summary_out) summary_out->assign(e.what());
      return false;
    }

    const auto handle = JobMaster::instance().submitRemote(plan_df.serializePlan());
    const std::string job_id = handle.id();

    RpcJobSnapshot snapshot;
    snapshot.job_id = job_id;
    snapshot.client_node = "dashboard";
    snapshot.payload = flattenText(sql.empty() ? plan_df.explain() : sql);
    snapshot.state = "SUBMITTED";
    snapshot.status_code = "JOB_SUBMITTED";
    job_snapshots[job_id] = snapshot;

    if (job_id_out) *job_id_out = job_id;
    if (summary_out) *summary_out = snapshot.payload;
    emitSnapshotEvent(snapshot);
    return true;
  };

  auto handleDashboardConnection = [&](int fd) {
    std::string method;
    std::string path;
    std::string body;
    if (!readHttpRequest(fd, &method, &path, &body)) {
      sendHttpResponse(fd, 400, "Bad Request", R"json({"ok":false,"message":"invalid request"})json",
                       "application/json");
      return;
    }

    const std::size_t qmark = path.find('?');
    if (qmark != std::string::npos) path = path.substr(0, qmark);

    if (method == "GET" && (path == "/" || path == "/index.html" || path == "/app.js" || path == "/app.ts")) {
      serveDashboardAsset(fd, path);
      return;
    }

    if (method == "GET" && path == "/api/jobs") {
      sendHttpResponse(fd, 200, "OK", buildJobsJson(job_snapshots), "application/json");
      return;
    }

    if (method == "GET" && path.compare(0, 10, "/api/jobs/") == 0 && path.size() > 10) {
      const std::string job_id = path.substr(10);
      const auto it = job_snapshots.find(job_id);
      if (it == job_snapshots.end()) {
        sendHttpResponse(fd, 404, "Not Found", R"json({"ok":false,"message":"job not found"})json",
                         "application/json");
      } else {
        sendHttpResponse(fd, 200, "OK", snapshotToJson(it->second), "application/json");
      }
      return;
    }

    if (method == "POST" && path == "/api/jobs") {
      const auto fields = parseFormBody(body);
      const std::string payload_input = fields.count("payload") ? fields.at("payload") : "";
      const std::string sql = fields.count("sql") ? fields.at("sql") : "";
      if (payload_input.empty() && sql.empty()) {
        sendHttpResponse(fd, 400, "Bad Request", R"json({"ok":false,"message":"payload or sql required"})json",
                         "application/json");
        return;
      }

      std::string job_id;
      std::string summary;
      if (!submitDashboardJob(payload_input, sql, &job_id, &summary)) {
        const bool unavailable_worker = summary.find("no worker available") != std::string::npos;
        const std::string detail = observability::object({
            observability::field("ok", false),
            observability::field("message", summary.empty() ? "submit failed" : summary),
        });
        sendHttpResponse(fd, unavailable_worker ? 503 : 500,
                         unavailable_worker ? "Service Unavailable" : "Internal Server Error",
                         detail, "application/json");
        return;
      }

      const std::string detail = observability::object({
          observability::field("ok", true),
          observability::field("job_id", job_id),
          observability::field("state", "SUBMITTED"),
          observability::field("status_code", "JOB_SUBMITTED"),
          observability::field("payload", summary),
      });
      sendHttpResponse(fd, 200, "OK", detail, "application/json");
      emitRpcEvent("actor_scheduler", "dashboard_submit", config.node_id, "JOB_SUBMITTED", job_id,
                   {observability::field("job_id", job_id),
                    observability::field("client_node", "dashboard"),
                    observability::field("payload", summary)});
      return;
    }

    sendHttpResponse(fd, 404, "Not Found", R"json({"ok":false,"message":"not found"})json",
                     "application/json");
  };

  auto sendTo = [&](int fd, const ActorRpcMessage& message) {
    const RpcFrame frame = makeFrameFromMessage(next_message_id++, config.node_id, "peer", message);
    return sendFrameOverSocket(fd, codec, frame);
  };

  auto cleanup = [&](int fd) {
    const std::string role = conn_role.count(fd) ? conn_role[fd] : "unknown";
    const std::string node = conn_node.count(fd) ? conn_node[fd] : "";
    conn_role.erase(fd);
    conn_node.erase(fd);
    worker_node_by_fd.erase(fd);
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
        snapshot_it->second.chain.task_ids = {task.task_id};
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

  while (true) {
    fd_set reads;
    FD_ZERO(&reads);
    FD_SET(server_fd, &reads);
    int max_fd = server_fd;
    if (dashboard_server_fd >= 0) {
      FD_SET(dashboard_server_fd, &reads);
      if (dashboard_server_fd > max_fd) max_fd = dashboard_server_fd;
    }
    for (int fd : conns) {
      FD_SET(fd, &reads);
      if (fd > max_fd) max_fd = fd;
    }

    for (int fd : dashboard_conns) {
      FD_SET(fd, &reads);
      if (fd > max_fd) max_fd = fd;
    }

    struct timeval tick;
    tick.tv_sec = 0;
    tick.tv_usec = 200 * 1000;
    const int ready = select(max_fd + 1, &reads, nullptr, nullptr, &tick);
    dispatchPending();
    if (ready <= 0) {
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

    if (dashboard_server_fd >= 0 && FD_ISSET(dashboard_server_fd, &reads)) {
      const int conn_fd = accept(dashboard_server_fd, nullptr, nullptr);
      if (conn_fd >= 0) {
        dashboard_conns.push_back(conn_fd);
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
        if (msg.ok) {
          const ProtoLikeSerializer serializer;
          completion.result_table = serializer.deserialize(msg.payload);
          completion.has_result_table = true;
          if (completion.output_rows == 0) {
            completion.output_rows = completion.result_table.rowCount();
          }
        }
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
                            makeFrameFromMessage(next_message_id++, config.node_id,
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

    for (std::size_t i = 0; i < dashboard_conns.size();) {
      const int fd = dashboard_conns[i];
      if (!FD_ISSET(fd, &reads)) {
        ++i;
        continue;
      }
      handleDashboardConnection(fd);
      dashboard_conns.erase(std::remove(dashboard_conns.begin(), dashboard_conns.end(), fd), dashboard_conns.end());
      ::close(fd);
    }
  }
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
      result.action = ActorRpcAction::Result;
      result.job_id = msg.job_id;
      result.chain_id = msg.chain_id;
      result.task_id = msg.task_id;
      result.node_id = config.node_id;
      result.attempt = msg.attempt;
      try {
        const auto plan = deserializePlan(msg.payload);
        const LocalExecutor executor;
        const Table output = executor.execute(plan);
        emitRpcEvent("actor_worker", "task_executed", config.node_id, "TASK_EXECUTED", msg.job_id,
                     {observability::field("task_id", msg.task_id),
                      observability::field("chain_id", msg.chain_id),
                      observability::field("output_rows", output.rowCount())});
        const ProtoLikeSerializer serializer;
        const std::string serialized_output = serializer.serialize(output);
        result.ok = true;
        result.state = "FINISHED";
        result.output_rows = output.rowCount();
        result.result_location = "inline://" + msg.job_id + "/" + msg.task_id;
        result.summary = summarizeTable(output);
        result.payload = serialized_output;
        emitRpcEvent("actor_worker", "result_serialized", config.node_id, "TASK_RESULT_SERIALIZED", msg.job_id,
                     {observability::field("task_id", msg.task_id),
                      observability::field("chain_id", msg.chain_id),
                      observability::field("payload_bytes", serialized_output.size()),
                      observability::field("summary", result.summary)});
      } catch (const std::exception& e) {
        result.ok = false;
        result.state = "FAILED";
        result.reason = e.what();
        result.summary = std::string("execution failed: ") + e.what();
      }

      stop_heartbeat.store(true);
      if (heartbeat_thread.joinable()) heartbeat_thread.join();
      sendFrameOverSocket(fd, codec,
                          makeFrameFromMessage(next_message_id.fetch_add(1), config.node_id,
                                               "scheduler", result));
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
      emitRpcEvent("actor_client", "job_result", config.node_id,
                   msg.ok ? "JOB_RESULT_RECEIVED" : "JOB_RESULT_FAILED", msg.job_id,
                   {observability::field("payload", msg.payload),
                    observability::field("task_id", msg.task_id),
                    observability::field("chain_id", msg.chain_id),
                    observability::field("result_location", msg.result_location),
                    observability::field("summary", msg.summary)});
      if (!msg.ok) {
        std::cerr << "[client] job failed: "
                  << (msg.payload.empty() ? msg.reason : msg.payload) << "\n";
        return 1;
      }
      std::cout << "[client] job result: " << msg.payload;
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
  std::cout << "[smoke] actor rpc codec roundtrip ok\n";
  return 0;
}

}  // namespace dataflow
