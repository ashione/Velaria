#include <string>

#include "src/dataflow/runner/actor_runtime.h"

int main(int argc, char* argv[]) {
  dataflow::ActorRuntimeConfig config;
  config.node_id = "client-1";
  config.connect_address = "127.0.0.1:61000";
  std::string payload = "demo:sample-payload";
  std::string sql;

  for (int i = 1; i < argc; ++i) {
    if (std::string(argv[i]) == "--connect" && i + 1 < argc) {
      config.connect_address = argv[++i];
      continue;
    }
    if (std::string(argv[i]) == "--node-id" && i + 1 < argc) {
      config.node_id = argv[++i];
      continue;
    }
    if (std::string(argv[i]) == "--payload" && i + 1 < argc) {
      payload = argv[++i];
      continue;
    }
    if (std::string(argv[i]) == "--sql" && i + 1 < argc) {
      sql = argv[++i];
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
  return dataflow::runActorClient(config, payload, sql);
}
