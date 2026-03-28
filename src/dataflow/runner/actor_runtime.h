#pragma once

// Runner module boundary:
// - This header defines process-level entrypoints for actor runtime demos and
//   RPC executables, plus the configuration surface consumed by those runners.
// - It is responsible for wiring scheduler/worker/client processes around the
//   runtime, not for the runtime kernel message protocol or execution internals.

#include <string>

namespace dataflow {

// Process-level runner configuration for actor runtime binaries and demos.
// This config is owned by the runner/bootstrap layer and should not be treated
// as the internal runtime kernel configuration.
struct ActorRuntimeConfig {
  std::string node_id = "node";
  std::string connect_address = "127.0.0.1:61000";
  std::string listen_address = "127.0.0.1:61000";
  std::string dashboard_listen_address = "127.0.0.1:8080";
  bool auto_worker = true;
  int local_worker_count = 1;
  bool dashboard_enabled = false;
  bool single_node = false;
  bool print_help = false;
};

int runActorScheduler(const ActorRuntimeConfig& config);
int runActorWorker(const ActorRuntimeConfig& config);
int runActorClient(const ActorRuntimeConfig& config,
                   const std::string& payload,
                   const std::string& sql = "");
int runActorSmoke();

}  // namespace dataflow
