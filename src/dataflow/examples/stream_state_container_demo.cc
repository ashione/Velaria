#include <iostream>
#include <vector>

#include "src/dataflow/stream/stream.h"

int main() {
  auto state = dataflow::makeStateStore("memory");

  // key-value
  state->put("offset", "42");
  std::string raw;
  if (state->get("offset", &raw)) {
    std::cout << "kv offset=" << raw << std::endl;
  }

  // key-map
  state->putMapField("user:stats", "last_ts", "2026-03-28T10:53:00");
  state->putMapField("user:stats", "count", "3");
  if (state->getMapField("user:stats", "count", &raw)) {
    std::cout << "map user:stats.count=" << raw << std::endl;
  }
  std::vector<std::string> fields;
  if (state->getMapFields("user:stats", &fields)) {
    std::cout << "map keys:";
    for (const auto& f : fields) {
      std::cout << " " << f;
    }
    std::cout << std::endl;
  }

  // value-list
  state->appendValueToList("last-events", "evt-a");
  state->appendValueToList("last-events", "evt-b");
  std::vector<std::string> events;
  if (state->getValueList("last-events", &events)) {
    std::cout << "list last-events:";
    for (const auto& e : events) {
      std::cout << " " << e;
    }
    std::cout << std::endl;
  }
  if (state->popValueFromList("last-events", &raw)) {
    std::cout << "pop last-events -> " << raw << std::endl;
  }

  // key list
  std::vector<std::string> keys;
  if (state->listKeys(&keys)) {
    std::cout << "all keys:";
    for (const auto& key : keys) {
      std::cout << " " << key;
    }
    std::cout << std::endl;
  }

  // cleanup
  state->remove("offset");
  state->removeMapField("user:stats", "count");
  state->remove("user:stats");

  if (state->listKeys(&keys)) {
    std::cout << "after cleanup keys:";
    for (const auto& key : keys) {
      std::cout << " " << key;
    }
    std::cout << std::endl;
  }

  return 0;
}
