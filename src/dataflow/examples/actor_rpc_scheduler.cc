#include <string>

#include "src/dataflow/runner/actor_runtime.h"

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
    if (std::string(argv[i]) == "--dashboard-listen" && i + 1 < argc) {
      config.dashboard_listen_address = argv[++i];
      config.dashboard_enabled = true;
      continue;
    }
    if (std::string(argv[i]) == "--dashboard-enabled") {
      config.dashboard_enabled = true;
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
