#include <iostream>
#include <limits>
#include <memory>
#include <stdexcept>

#include "src/dataflow/core/contract/api/dataframe.h"
#include "src/dataflow/core/execution/columnar_batch.h"
#include "src/dataflow/core/execution/runtime/execution_optimizer.h"
#include "src/dataflow/core/execution/runtime/executor.h"
#include "src/dataflow/core/logical/planner/plan.h"

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
  plan = std::make_shared<dataflow::WindowAssignPlan>(plan, 0, 5000, 5000, "window_start");
  plan = std::make_shared<dataflow::SinkPlan>(plan, "file_append");

  const auto encoded = dataflow::serializePlan(plan);
  const auto decoded = dataflow::deserializePlan(encoded);
  expect(decoded != nullptr, "decoded plan should not be null");
  expect(decoded->kind == dataflow::PlanKind::Sink, "root should remain sink");
  const auto* decoded_sink = static_cast<const dataflow::SinkPlan*>(decoded.get());
  expect(decoded_sink->child->kind == dataflow::PlanKind::WindowAssign,
         "child should remain window assign");
  const auto* decoded_window = static_cast<const dataflow::WindowAssignPlan*>(decoded_sink->child.get());
  expect(decoded_window->window_ms == 5000, "window assign should preserve window_ms");
  expect(decoded_window->slide_ms == 5000, "window assign should preserve slide_ms");

  dataflow::LocalExecutor executor;
  auto out = executor.execute(decoded);
  dataflow::materializeRows(&out);
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
  auto string_out = executor.execute(decoded_string);
  dataflow::materializeRows(&string_out);
  expect(string_out.schema.has("region_lower"), "string withColumn should append output column");
  const auto string_idx = string_out.schema.indexOf("region_lower");
  expect(string_out.rows[0][string_idx].toString() == "apac",
         "string withColumn should transform first row");
  expect(string_out.rows[1][string_idx].isNull(),
         "string withColumn should preserve null semantics");
  expect(string_out.rows[2][string_idx].toString() == "emea",
         "string withColumn should transform third row");

  dataflow::Schema stream_style_schema({"ts"});
  dataflow::Table stream_style_source(
      stream_style_schema,
      {{dataflow::Value("2026-03-29T10:00:00")}, {dataflow::Value("2026-03-30T08:30:00")}});
  dataflow::PlanNodePtr stream_style_plan =
      std::make_shared<dataflow::SourcePlan>("memory", stream_style_source);
  dataflow::ComputedColumnArg ts_arg;
  ts_arg.is_literal = false;
  ts_arg.source_column_index = static_cast<std::size_t>(-1);
  ts_arg.source_column_name = "ts";
  stream_style_plan = std::make_shared<dataflow::WithColumnPlan>(
      stream_style_plan, "event_year", dataflow::ComputedColumnKind::DateYear,
      std::vector<dataflow::ComputedColumnArg>{ts_arg});
  const auto encoded_stream_style = dataflow::serializePlan(stream_style_plan);
  const auto decoded_stream_style = dataflow::deserializePlan(encoded_stream_style);
  auto stream_style_out = executor.execute(decoded_stream_style);
  dataflow::materializeRows(&stream_style_out);
  expect(stream_style_out.schema.has("event_year"),
         "withColumn plan roundtrip should preserve named source column");
  const auto year_idx = stream_style_out.schema.indexOf("event_year");
  expect(stream_style_out.rows[0][year_idx].asInt64() == 2026,
         "named source column first derived year mismatch");
  expect(stream_style_out.rows[1][year_idx].asInt64() == 2026,
         "named source column second derived year mismatch");

  dataflow::Schema aggregate_schema({"region", "score"});
  dataflow::Table aggregate_source(
      aggregate_schema,
      {
          {dataflow::Value("apac"), dataflow::Value(int64_t(3))},
          {dataflow::Value("emea"), dataflow::Value(int64_t(5))},
          {dataflow::Value("apac"), dataflow::Value(int64_t(7))},
      });
  dataflow::PlanNodePtr order_plan =
      std::make_shared<dataflow::SourcePlan>("memory", aggregate_source);
  order_plan = std::make_shared<dataflow::OrderByPlan>(
      order_plan, std::vector<std::size_t>{1, 0}, std::vector<bool>{false, true});
  const auto order_encoded = dataflow::serializePlan(order_plan);
  const auto order_decoded = dataflow::deserializePlan(order_encoded);
  auto order_out = executor.execute(order_decoded);
  dataflow::materializeRows(&order_out);
  expect(order_out.rows.size() == 3, "order by should preserve row count");
  expect(order_out.rows[0][1].asInt64() == 7, "order by highest score first");
  expect(order_out.rows[1][1].asInt64() == 5, "order by second score");
  expect(order_out.rows[2][1].asInt64() == 3, "order by third score");

  auto filter_df = dataflow::DataFrame(aggregate_source).filter("score", ">", dataflow::Value(int64_t(4)));
  const auto filter_encoded = filter_df.serializePlan();
  const auto filter_decoded = dataflow::deserializePlan(filter_encoded);
  auto filter_out = executor.execute(filter_decoded);
  dataflow::materializeRows(&filter_out);
  expect(filter_out.rows.size() == 2, "filter plan roundtrip should preserve predicate rows");
  expect(filter_out.rows[0][1].asInt64() == 5, "filter roundtrip first score mismatch");
  expect(filter_out.rows[1][1].asInt64() == 7, "filter roundtrip second score mismatch");
  auto left_pred = std::make_shared<dataflow::PlanPredicateExpr>();
  left_pred->kind = dataflow::PlanPredicateExprKind::Comparison;
  left_pred->comparison.column_index = 0;
  left_pred->comparison.op = "=";
  left_pred->comparison.value = dataflow::Value("apac");
  auto right_pred = std::make_shared<dataflow::PlanPredicateExpr>();
  right_pred->kind = dataflow::PlanPredicateExprKind::Comparison;
  right_pred->comparison.column_index = 1;
  right_pred->comparison.op = ">";
  right_pred->comparison.value = dataflow::Value(int64_t(4));
  auto and_pred = std::make_shared<dataflow::PlanPredicateExpr>();
  and_pred->kind = dataflow::PlanPredicateExprKind::And;
  and_pred->left = left_pred;
  and_pred->right = right_pred;
  auto alt_pred = std::make_shared<dataflow::PlanPredicateExpr>();
  alt_pred->kind = dataflow::PlanPredicateExprKind::Comparison;
  alt_pred->comparison.column_index = 0;
  alt_pred->comparison.op = "=";
  alt_pred->comparison.value = dataflow::Value("emea");
  auto nested_pred = std::make_shared<dataflow::PlanPredicateExpr>();
  nested_pred->kind = dataflow::PlanPredicateExprKind::Or;
  nested_pred->left = and_pred;
  nested_pred->right = alt_pred;
  auto nested_filter_df = dataflow::DataFrame(aggregate_source).filterPredicate(nested_pred);
  const auto nested_filter_encoded = nested_filter_df.serializePlan();
  const auto nested_filter_decoded = dataflow::deserializePlan(nested_filter_encoded);
  auto nested_filter_out = executor.execute(nested_filter_decoded);
  dataflow::materializeRows(&nested_filter_out);
  expect(nested_filter_out.rows.size() == 2, "nested filter plan roundtrip should preserve rows");
  expect(nested_filter_out.rows[0][0].toString() == "emea", "nested filter first key mismatch");
  expect(nested_filter_out.rows[1][0].toString() == "apac", "nested filter second key mismatch");

  dataflow::PlanNodePtr aggregate_plan =
      std::make_shared<dataflow::SourcePlan>("memory", aggregate_source);
  dataflow::AggregateSpec sum_spec{dataflow::AggregateFunction::Sum, 1, "total_score"};
  aggregate_plan = std::make_shared<dataflow::AggregatePlan>(
      aggregate_plan, std::vector<std::size_t>{0}, std::vector<dataflow::AggregateSpec>{sum_spec});
  const auto aggregate_encoded = dataflow::serializePlan(aggregate_plan);
  const auto aggregate_decoded = dataflow::deserializePlan(aggregate_encoded);
  auto aggregate_out = executor.execute(aggregate_decoded);
  dataflow::materializeRows(&aggregate_out);
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

  dataflow::Table dense_sum_source(dataflow::Schema({"bucket", "amount"}), {});
  dense_sum_source.rows.reserve(128);
  for (std::size_t i = 0; i < 128; ++i) {
    dense_sum_source.rows.push_back(
        {dataflow::Value(static_cast<int64_t>(i % 8)), dataflow::Value(int64_t(2))});
  }
  dataflow::AggregateSpec dense_sum_spec{dataflow::AggregateFunction::Sum, 1, "total_amount"};
  const auto dense_sum_pattern =
      dataflow::analyzeAggregateExecution(dense_sum_source, {0}, {dense_sum_spec});
  expect(dense_sum_pattern.exec_spec.impl_kind == dataflow::AggImplKind::Dense,
         "compact int64 aggregate should use dense grouping");
  expect(dense_sum_pattern.shape == dataflow::AggregateExecutionShape::SumSingleInt64Key,
         "dense single int64 sum should use typed sum runtime shape");
  expect(dense_sum_pattern.exec_spec.partial_layout ==
             dataflow::AggregatePartialLayoutKind::StateColumnar,
         "dense single int64 sum should expose state-columnar partial layout");
  auto dense_sum_out =
      dataflow::executeAggregateTable(dense_sum_source, {0}, {dense_sum_spec},
                                      &dense_sum_pattern.exec_spec);
  dataflow::materializeRows(&dense_sum_out);
  expect(dense_sum_out.rows.size() == 8, "dense sum should group compact key domain");
  expect(dense_sum_out.columnar_cache != nullptr, "dense sum should retain columnar cache");
  dataflow::validateTableColumnarCache(dense_sum_out, "dense int64 sum aggregate");
  const auto dense_sum_total_idx = dense_sum_out.schema.indexOf("total_amount");
  for (const auto& row : dense_sum_out.rows) {
    expect(row[dense_sum_total_idx].asDouble() == 32.0,
           "dense sum should preserve numeric reducer semantics");
  }

  dataflow::AggregateSpec dense_count_spec{dataflow::AggregateFunction::Count, 0, "row_count"};
  const auto dense_count_pattern =
      dataflow::analyzeAggregateExecution(dense_sum_source, {0}, {dense_count_spec});
  expect(dense_count_pattern.exec_spec.impl_kind == dataflow::AggImplKind::Dense,
         "compact int64 count aggregate should use dense grouping");
  expect(dense_count_pattern.shape == dataflow::AggregateExecutionShape::CountSingleInt64Key,
         "dense single int64 count should use typed count runtime shape");
  expect(dense_count_pattern.exec_spec.partial_layout ==
             dataflow::AggregatePartialLayoutKind::StateColumnar,
         "dense single int64 count should expose state-columnar partial layout");
  auto dense_count_out =
      dataflow::executeAggregateTable(dense_sum_source, {0}, {dense_count_spec},
                                      &dense_count_pattern.exec_spec);
  dataflow::materializeRows(&dense_count_out);
  expect(dense_count_out.rows.size() == 8, "dense count should group compact key domain");
  expect(dense_count_out.columnar_cache != nullptr, "dense count should retain columnar cache");
  dataflow::validateTableColumnarCache(dense_count_out, "dense int64 count aggregate");
  const auto dense_count_idx = dense_count_out.schema.indexOf("row_count");
  for (const auto& row : dense_count_out.rows) {
    expect(row[dense_count_idx].asInt64() == 16,
           "dense count should preserve row-count reducer semantics");
  }

  dataflow::AggregateSpec dense_avg_spec{dataflow::AggregateFunction::Avg, 1, "avg_amount"};
  const auto dense_avg_pattern =
      dataflow::analyzeAggregateExecution(dense_sum_source, {0}, {dense_avg_spec});
  expect(dense_avg_pattern.exec_spec.impl_kind == dataflow::AggImplKind::Dense,
         "compact int64 avg aggregate should use dense grouping");
  expect(dense_avg_pattern.shape == dataflow::AggregateExecutionShape::AvgSingleInt64Key,
         "dense single int64 avg should use typed avg runtime shape");
  expect(dense_avg_pattern.exec_spec.partial_layout ==
             dataflow::AggregatePartialLayoutKind::StateColumnar,
         "dense single int64 avg should expose state-columnar partial layout");
  auto dense_avg_out =
      dataflow::executeAggregateTable(dense_sum_source, {0}, {dense_avg_spec},
                                      &dense_avg_pattern.exec_spec);
  dataflow::materializeRows(&dense_avg_out);
  expect(dense_avg_out.rows.size() == 8, "dense avg should group compact key domain");
  expect(dense_avg_out.columnar_cache != nullptr, "dense avg should retain columnar cache");
  dataflow::validateTableColumnarCache(dense_avg_out, "dense int64 avg aggregate");
  const auto dense_avg_idx = dense_avg_out.schema.indexOf("avg_amount");
  for (const auto& row : dense_avg_out.rows) {
    expect(row[dense_avg_idx].asDouble() == 2.0,
           "dense avg should preserve numeric reducer semantics");
  }

  dataflow::Table extreme_int64_source(
      dataflow::Schema({"bucket", "amount"}),
      {{dataflow::Value(std::numeric_limits<int64_t>::min()), dataflow::Value(int64_t(1))},
       {dataflow::Value(std::numeric_limits<int64_t>::max()), dataflow::Value(int64_t(2))}});
  dataflow::AggregateSpec extreme_sum_spec{dataflow::AggregateFunction::Sum, 1,
                                           "total_amount"};
  dataflow::AggregateExecSpec forced_dense_spec;
  forced_dense_spec.impl_kind = dataflow::AggImplKind::Dense;
  auto extreme_sum_out =
      dataflow::executeAggregateTable(extreme_int64_source, {0}, {extreme_sum_spec},
                                      &forced_dense_spec);
  dataflow::materializeRows(&extreme_sum_out);
  expect(extreme_sum_out.rows.size() == 2,
         "extreme int64 dense fallback should preserve both groups");
  dataflow::validateTableColumnarCache(extreme_sum_out, "extreme int64 dense fallback");

  dataflow::Table double_int64_sum_source(dataflow::Schema({"bucket", "shard", "amount"}), {});
  double_int64_sum_source.rows.reserve(128);
  for (std::size_t i = 0; i < 128; ++i) {
    double_int64_sum_source.rows.push_back(
        {dataflow::Value(static_cast<int64_t>(i % 4)),
         dataflow::Value(static_cast<int64_t>((i / 4) % 2)), dataflow::Value(int64_t(3))});
  }
  dataflow::AggregateSpec double_int64_sum_spec{dataflow::AggregateFunction::Sum, 2,
                                                "total_amount"};
  const auto double_int64_sum_pattern =
      dataflow::analyzeAggregateExecution(double_int64_sum_source, {0, 1},
                                          {double_int64_sum_spec});
  expect(double_int64_sum_pattern.exec_spec.impl_kind == dataflow::AggImplKind::HashPacked,
         "two int64 key sum should keep packed hash grouping");
  expect(double_int64_sum_pattern.shape == dataflow::AggregateExecutionShape::SumDoubleInt64Key,
         "two int64 key sum should use typed double-int64 sum runtime shape");
  expect(double_int64_sum_pattern.exec_spec.partial_layout ==
             dataflow::AggregatePartialLayoutKind::StateColumnar,
         "two int64 key sum should expose state-columnar partial layout");
  auto double_int64_sum_out =
      dataflow::executeAggregateTable(double_int64_sum_source, {0, 1},
                                      {double_int64_sum_spec},
                                      &double_int64_sum_pattern.exec_spec);
  dataflow::materializeRows(&double_int64_sum_out);
  expect(double_int64_sum_out.rows.size() == 8, "two int64 sum should group both keys");
  expect(double_int64_sum_out.columnar_cache != nullptr,
         "two int64 sum should retain columnar cache");
  dataflow::validateTableColumnarCache(double_int64_sum_out, "two int64 sum aggregate");
  const auto double_int64_sum_idx = double_int64_sum_out.schema.indexOf("total_amount");
  for (const auto& row : double_int64_sum_out.rows) {
    expect(row[double_int64_sum_idx].asDouble() == 48.0,
           "two int64 sum should preserve numeric reducer semantics");
  }

  dataflow::AggregateSpec double_int64_count_spec{dataflow::AggregateFunction::Count, 0,
                                                  "row_count"};
  const auto double_int64_count_pattern =
      dataflow::analyzeAggregateExecution(double_int64_sum_source, {0, 1},
                                          {double_int64_count_spec});
  expect(double_int64_count_pattern.exec_spec.impl_kind == dataflow::AggImplKind::HashPacked,
         "two int64 key count should keep packed hash grouping");
  expect(std::string(dataflow::aggregateExecutionShapeName(double_int64_count_pattern.shape)) ==
             "count-double-int64-key",
         "two int64 key count should use typed double-int64 count runtime shape");
  expect(double_int64_count_pattern.exec_spec.partial_layout ==
             dataflow::AggregatePartialLayoutKind::StateColumnar,
         "two int64 key count should expose state-columnar partial layout");
  auto double_int64_count_out =
      dataflow::executeAggregateTable(double_int64_sum_source, {0, 1},
                                      {double_int64_count_spec},
                                      &double_int64_count_pattern.exec_spec);
  dataflow::materializeRows(&double_int64_count_out);
  expect(double_int64_count_out.rows.size() == 8, "two int64 count should group both keys");
  dataflow::validateTableColumnarCache(double_int64_count_out, "two int64 count aggregate");
  const auto double_int64_count_idx = double_int64_count_out.schema.indexOf("row_count");
  for (const auto& row : double_int64_count_out.rows) {
    expect(row[double_int64_count_idx].asInt64() == 16,
           "two int64 count should preserve row-count reducer semantics");
  }

  dataflow::AggregateSpec double_int64_avg_spec{dataflow::AggregateFunction::Avg, 2,
                                                "avg_amount"};
  const auto double_int64_avg_pattern =
      dataflow::analyzeAggregateExecution(double_int64_sum_source, {0, 1},
                                          {double_int64_avg_spec});
  expect(double_int64_avg_pattern.exec_spec.impl_kind == dataflow::AggImplKind::HashPacked,
         "two int64 key avg should keep packed hash grouping");
  expect(std::string(dataflow::aggregateExecutionShapeName(double_int64_avg_pattern.shape)) ==
             "avg-double-int64-key",
         "two int64 key avg should use typed double-int64 avg runtime shape");
  expect(double_int64_avg_pattern.exec_spec.partial_layout ==
             dataflow::AggregatePartialLayoutKind::StateColumnar,
         "two int64 key avg should expose state-columnar partial layout");
  auto double_int64_avg_out =
      dataflow::executeAggregateTable(double_int64_sum_source, {0, 1},
                                      {double_int64_avg_spec},
                                      &double_int64_avg_pattern.exec_spec);
  dataflow::materializeRows(&double_int64_avg_out);
  expect(double_int64_avg_out.rows.size() == 8, "two int64 avg should group both keys");
  dataflow::validateTableColumnarCache(double_int64_avg_out, "two int64 avg aggregate");
  const auto double_int64_avg_idx = double_int64_avg_out.schema.indexOf("avg_amount");
  for (const auto& row : double_int64_avg_out.rows) {
    expect(row[double_int64_avg_idx].asDouble() == 3.0,
           "two int64 avg should preserve numeric reducer semantics");
  }

  auto source_grp_a = std::make_shared<dataflow::PlanPredicateExpr>();
  source_grp_a->kind = dataflow::PlanPredicateExprKind::Comparison;
  source_grp_a->comparison = {1, dataflow::Value("A"), "="};
  auto source_grp_c = std::make_shared<dataflow::PlanPredicateExpr>();
  source_grp_c->kind = dataflow::PlanPredicateExprKind::Comparison;
  source_grp_c->comparison = {1, dataflow::Value("C"), "="};
  auto source_predicate = std::make_shared<dataflow::PlanPredicateExpr>();
  source_predicate->kind = dataflow::PlanPredicateExprKind::Or;
  source_predicate->left = source_grp_a;
  source_predicate->right = source_grp_c;

  dataflow::SourcePushdownSpec source_count_pushdown;
  source_count_pushdown.predicate_expr = source_predicate;
  source_count_pushdown.has_aggregate = true;
  source_count_pushdown.aggregate.keys = {1};
  source_count_pushdown.aggregate.aggregates = {
      {dataflow::AggregateFunction::Count, 1, "cnt"},
  };
  expect(dataflow::classifySourcePushdownShape(source_count_pushdown) ==
             dataflow::SourcePushdownShape::SingleKeyCount,
         "single-key predicate source count should use typed count shape");

  dataflow::SourcePushdownSpec source_sum_pushdown;
  source_sum_pushdown.predicate_expr = source_predicate;
  source_sum_pushdown.has_aggregate = true;
  source_sum_pushdown.aggregate.keys = {1};
  source_sum_pushdown.aggregate.aggregates = {
      {dataflow::AggregateFunction::Sum, 2, "sum_val"},
  };
  expect(dataflow::classifySourcePushdownShape(source_sum_pushdown) ==
             dataflow::SourcePushdownShape::SingleKeyNumericAggregate,
         "single-key predicate source sum should use typed numeric shape");

  dataflow::Table empty_int64_source(dataflow::Schema({"user_id", "score"}), {});
  dataflow::PlanNodePtr empty_aggregate_plan =
      std::make_shared<dataflow::SourcePlan>("memory", empty_int64_source);
  empty_aggregate_plan = std::make_shared<dataflow::AggregatePlan>(
      empty_aggregate_plan, std::vector<std::size_t>{0},
      std::vector<dataflow::AggregateSpec>{
          {dataflow::AggregateFunction::Sum, 1, "total_score"}});
  auto empty_aggregate_out = executor.execute(empty_aggregate_plan);
  expect(empty_aggregate_out.rowCount() == 0, "empty int64 aggregate should stay empty");
  expect(empty_aggregate_out.columnar_cache != nullptr,
         "empty int64 aggregate should retain empty columnar cache");
  dataflow::validateTableColumnarCache(empty_aggregate_out, "empty int64 aggregate");

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
  auto join_out = executor.execute(join_decoded);
  dataflow::materializeRows(&join_out);
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

  dataflow::Schema limit_schema({"method", "score"});
  dataflow::Table limit_source(
      limit_schema,
      {
          {dataflow::Value("GET"), dataflow::Value(int64_t(10))},
          {dataflow::Value("POST"), dataflow::Value(int64_t(20))},
          {dataflow::Value("PUT"), dataflow::Value(int64_t(30))},
      });
  dataflow::ComputedColumnArg method_arg;
  method_arg.is_literal = false;
  method_arg.source_column_index = 0;
  dataflow::DataFrame limited_df(limit_source);
  limited_df = limited_df.filter("score", ">=", dataflow::Value(int64_t(10)))
                   .withColumn("method_lower", dataflow::ComputedColumnKind::StringLower,
                               {method_arg})
                   .select({"method_lower", "score"})
                   .limit(1);
  const auto limit_explain = limited_df.explain();
  expect(limit_explain.find("Select\n  WithColumn\n    added=method_lower\n    function=2\n    Limit\n") ==
             0,
         "limit should push below select and withColumn");
  auto limited_out = limited_df.toTable();
  expect(!limited_out.rows.empty(), "toTable should materialize rows");
  expect(limited_out.rows.size() == 1, "pushed limit query should retain single row");
  expect(limited_out.rows[0][0].toString() == "get", "pushed limit query should preserve value");

  std::cout << "[test] planner v0.3 logical nodes ok" << std::endl;
  return 0;
}
