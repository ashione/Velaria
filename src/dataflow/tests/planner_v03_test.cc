#include <iostream>
#include <memory>
#include <stdexcept>

#include "src/dataflow/core/execution/columnar_batch.h"
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

  dataflow::Schema string_schema({"region"});
  dataflow::Table string_source(
      string_schema, {{dataflow::Value("APAC")}, {dataflow::Value()}, {dataflow::Value("EMEA")}});
  dataflow::PlanNodePtr string_plan =
      std::make_shared<dataflow::SourcePlan>("memory", string_source);
  dataflow::ComputedColumnArg region_arg;
  region_arg.is_literal = false;
  region_arg.source_column_index = 0;
  string_plan = std::make_shared<dataflow::WithColumnPlan>(
      string_plan, "region_lower", dataflow::ComputedColumnKind::StringLower,
      std::vector<dataflow::ComputedColumnArg>{region_arg});

  const auto encoded_string = dataflow::serializePlan(string_plan);
  const auto decoded_string = dataflow::deserializePlan(encoded_string);
  const auto string_out = executor.execute(decoded_string);
  expect(string_out.schema.has("region_lower"), "string withColumn should append output column");
  const auto string_idx = string_out.schema.indexOf("region_lower");
  expect(string_out.rows[0][string_idx].toString() == "apac",
         "string withColumn should transform first row");
  expect(string_out.rows[1][string_idx].isNull(),
         "string withColumn should preserve null semantics");
  expect(string_out.rows[2][string_idx].toString() == "emea",
         "string withColumn should transform third row");

  dataflow::Schema aggregate_schema({"region", "score"});
  dataflow::Table aggregate_source(
      aggregate_schema,
      {
          {dataflow::Value("apac"), dataflow::Value(int64_t(3))},
          {dataflow::Value("emea"), dataflow::Value(int64_t(5))},
          {dataflow::Value("apac"), dataflow::Value(int64_t(7))},
      });
  dataflow::PlanNodePtr aggregate_plan =
      std::make_shared<dataflow::SourcePlan>("memory", aggregate_source);
  dataflow::AggregateSpec sum_spec{dataflow::AggregateFunction::Sum, 1, "total_score"};
  aggregate_plan = std::make_shared<dataflow::AggregatePlan>(
      aggregate_plan, std::vector<std::size_t>{0}, std::vector<dataflow::AggregateSpec>{sum_spec});
  const auto aggregate_encoded = dataflow::serializePlan(aggregate_plan);
  const auto aggregate_decoded = dataflow::deserializePlan(aggregate_encoded);
  const auto aggregate_out = executor.execute(aggregate_decoded);
  expect(aggregate_out.rows.size() == 2, "aggregate should group two keys");
  expect(aggregate_out.schema.has("total_score"), "aggregate should expose output column");
  const auto total_idx = aggregate_out.schema.indexOf("total_score");
  expect(aggregate_out.rows[0][0].toString() == "apac", "aggregate first key mismatch");
  expect(aggregate_out.rows[0][total_idx].asDouble() == 10.0, "aggregate first sum mismatch");
  expect(aggregate_out.rows[1][0].toString() == "emea", "aggregate second key mismatch");
  expect(aggregate_out.rows[1][total_idx].asDouble() == 5.0, "aggregate second sum mismatch");
  expect(aggregate_out.columnar_cache != nullptr, "aggregate should retain columnar cache");
  const auto aggregate_total = dataflow::materializeValueColumn(aggregate_out, total_idx);
  expect(aggregate_total.values.size() == 2, "aggregate cached total size mismatch");
  expect(aggregate_total.values[0].asDouble() == 10.0, "aggregate cached total first row mismatch");

  dataflow::Schema left_schema({"user_id", "region"});
  dataflow::Table left_source(
      left_schema,
      {
          {dataflow::Value(int64_t(1)), dataflow::Value("apac")},
          {dataflow::Value(int64_t(2)), dataflow::Value("emea")},
      });
  dataflow::Schema right_schema({"user_id", "score"});
  dataflow::Table right_source(
      right_schema,
      {
          {dataflow::Value(int64_t(1)), dataflow::Value(int64_t(9))},
      });
  dataflow::PlanNodePtr join_left = std::make_shared<dataflow::SourcePlan>("left", left_source);
  dataflow::PlanNodePtr join_right = std::make_shared<dataflow::SourcePlan>("right", right_source);
  dataflow::PlanNodePtr join_plan = std::make_shared<dataflow::JoinPlan>(
      join_left, join_right, 0, 0, dataflow::JoinKind::Left);
  const auto join_encoded = dataflow::serializePlan(join_plan);
  const auto join_decoded = dataflow::deserializePlan(join_encoded);
  const auto join_out = executor.execute(join_decoded);
  expect(join_out.rows.size() == 2, "left join should retain unmatched rows");
  expect(join_out.rows[0][0].asInt64() == 1, "join first left key mismatch");
  expect(join_out.rows[0][3].asInt64() == 9, "join matched payload mismatch");
  expect(join_out.rows[1][0].asInt64() == 2, "join second left key mismatch");
  expect(join_out.rows[1][3].isNull(), "join unmatched payload should be null");
  expect(join_out.columnar_cache != nullptr, "join should retain columnar cache");
  const auto join_scores = dataflow::materializeValueColumn(join_out, 3);
  expect(join_scores.values.size() == 2, "join cached score size mismatch");
  expect(join_scores.values[0].asInt64() == 9, "join cached matched payload mismatch");
  expect(join_scores.values[1].isNull(), "join cached unmatched payload should be null");

  std::cout << "[test] planner v0.3 logical nodes ok" << std::endl;
  return 0;
}
