#include <iostream>
#include <memory>
#include <stdexcept>

#include "src/dataflow/core/logical/planner/plan.h"
#include "src/dataflow/core/execution/runtime/executor.h"

namespace {

void expect(bool ok, const std::string& msg) {
  if (!ok) {
    throw std::runtime_error(msg);
  }
}

}  // namespace

int main() {
  dataflow::Schema schema({"ts", "value"});
  dataflow::Table source(schema, {{dataflow::Value(int64_t(12000)), dataflow::Value(int64_t(3))},
                                  {dataflow::Value(int64_t(17000)), dataflow::Value(int64_t(5))}});

  dataflow::PlanNodePtr plan = std::make_shared<dataflow::SourcePlan>("memory", source);
  plan = std::make_shared<dataflow::WindowAssignPlan>(plan, 0, 5000, "window_start");
  plan = std::make_shared<dataflow::SinkPlan>(plan, "file_append");

  const auto encoded = dataflow::serializePlan(plan);
  const auto decoded = dataflow::deserializePlan(encoded);
  expect(decoded != nullptr, "decoded plan should not be null");
  expect(decoded->kind == dataflow::PlanKind::Sink, "root should remain sink");

  dataflow::LocalExecutor executor;
  const auto out = executor.execute(decoded);
  expect(out.schema.has("window_start"), "window assign should append output column");
  const auto idx = out.schema.indexOf("window_start");
  expect(out.rows.size() == 2, "window output rows should match input rows");
  expect(out.rows[0][idx].asInt64() == 10000, "first row window start should be 10000");
  expect(out.rows[1][idx].asInt64() == 15000, "second row window start should be 15000");

  std::cout << "[test] planner v0.3 logical nodes ok" << std::endl;
  return 0;
}
