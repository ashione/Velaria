#include <string>

#include <cstdlib>

#include "src/dataflow/experimental/runner/actor_runtime.h"

int main(int argc, char* argv[]) {
  dataflow::ActorRuntimeConfig config;
  config.node_id = "scheduler";
  config.listen_address = "127.0.0.1:61000";

  for (int i = 1; i < argc; ++i) {
    if (std::string(argv[i]) == "--listen" && i + 1 < argc) {
      config.listen_address = argv[++i];
      continue;
    }
    if (std::string(argv[i]) == "--node-id" && i + 1 < argc) {
      config.node_id = argv[++i];
      continue;
    }
    if (std::string(argv[i]) == "--auto-worker") {
      config.auto_worker = true;
      continue;
    }
    if (std::string(argv[i]) == "--no-auto-worker") {
      config.auto_worker = false;
      continue;
    }
    if (std::string(argv[i]) == "--local-workers" && i + 1 < argc) {
      const int worker_count = std::atoi(argv[++i]);
      if (worker_count > 0) {
        config.local_worker_count = worker_count;
      }
      continue;
    }
    if (std::string(argv[i]) == "--single-node") {
      config.single_node = true;
      continue;
    }
    if (std::string(argv[i]) == "--help" || std::string(argv[i]) == "-h") {
      config.print_help = true;
    }
  }

  if (config.print_help) {
    return 0;
  }
  return dataflow::runActorScheduler(config);
}
