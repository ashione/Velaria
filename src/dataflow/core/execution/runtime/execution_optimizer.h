#pragma once

#include <string>
#include <vector>

#include "src/dataflow/core/logical/planner/plan.h"

namespace dataflow {

struct Table;

struct PredicatePattern {
  PlanNodePtr base_child;
  std::shared_ptr<PlanPredicateExpr> predicate;
  // Populated only when the predicate tree is a pure AND of comparisons.
  std::vector<SourceFilterPushdownSpec> conjunctive_filters;
  bool is_conjunctive = false;
};

enum class SourceTokenizerMode {
  ScalarStateMachine = 0,
  SimdFastUnquoted = 1,
};

enum class SourceDecodeMode {
  ParseCapturedCells = 0,
  ParseFilterColumnsOnly = 1,
  ParseAggregateColumnsOnly = 2,
};

struct SourceExecutionPattern {
  SourceTokenizerMode tokenizer_mode = SourceTokenizerMode::ScalarStateMachine;
  SourceDecodeMode decode_mode = SourceDecodeMode::ParseCapturedCells;
  bool sample_has_quotes = false;
  std::string reason;
};

enum class AggregateExecutionShape {
  GenericSerializedKeys = 0,
  GenericNoKey = 1,
  GenericSingleStringKey = 2,
  GenericSingleInt64Key = 3,
  GenericDoubleInt64Key = 4,
  // Local fast-path for multi-key (2-3 keys) grouping when keys are mixed
  // String/Int64/Null. Avoids materializeSerializedKeys + splitKey roundtrip.
  GenericPackedKeys2 = 8,
  GenericPackedKeys3 = 9,
  SumNoKey = 5,
  SumSingleInt64Key = 6,
  SumDoubleInt64Key = 7,
};

struct AggregateExecutionPattern {
  AggregateExecSpec exec_spec;
  AggregateRuntimeFeedback feedback;
  AggregateExecutionShape shape = AggregateExecutionShape::GenericSerializedKeys;
};

struct LimitExecutionPattern {
  bool use_predicate_filter = false;
  PredicatePattern predicate_filter;
  bool use_topn = false;
  const OrderByPlan* order_by = nullptr;
};

PredicatePattern analyzePredicatePattern(const PlanNodePtr& plan);
LimitExecutionPattern analyzeLimitExecution(const LimitPlan& plan);
SourceExecutionPattern analyzeSourceExecution(const FileSourceConnectorSpec& spec,
                                             const Schema& schema,
                                             const SourcePushdownSpec& pushdown);
const char* aggregateExecKindName(AggImplKind kind);
const char* aggregatePartialLayoutName(AggregatePartialLayoutKind kind);
const char* aggregateExecutionShapeName(AggregateExecutionShape shape);
AggregateExecutionPattern analyzeAggregateExecution(
    const Table& input,
    const std::vector<std::size_t>& key_indices,
    const std::vector<AggregateSpec>& aggregates,
    bool ordered_input = false,
    bool partition_local = false);

}  // namespace dataflow
