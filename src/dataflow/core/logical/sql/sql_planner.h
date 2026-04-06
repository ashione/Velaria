#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "src/dataflow/core/contract/api/dataframe.h"
#include "src/dataflow/core/contract/catalog/catalog.h"
#include "src/dataflow/core/execution/stream/stream.h"
#include "src/dataflow/core/logical/sql/sql_ast.h"

namespace dataflow {
namespace sql {

enum class LogicalStepKind { Scan, Filter, Join, Aggregate, Having, WithColumn, Project, OrderBy, Limit };
enum class PhysicalStepKind { FusedUnary, Join, Aggregate, SourceOnly };

struct LogicalPlanStep {
  LogicalStepKind kind = LogicalStepKind::Scan;
  DataFrame source;
  std::string source_name;
  std::size_t filter_column = 0;
  std::string filter_op;
  Value filter_value;

  std::string join_left_column;
  std::string join_right_column;
  JoinKind join_kind = JoinKind::Inner;
  DataFrame join_right;

  std::vector<std::size_t> group_keys;
  std::vector<AggregateSpec> aggregates;
  std::string having_column;
  std::vector<std::size_t> project_indices;
  std::vector<std::string> project_aliases;
  std::vector<std::size_t> order_indices;
  std::vector<bool> order_ascending;
  std::string with_column_name;
  ComputedColumnKind with_function = ComputedColumnKind::Copy;
  std::vector<ComputedColumnArg> with_args;
  std::size_t limit = 0;
  bool limit_set = false;
};

struct LogicalPlan {
  DataFrame seed;
  std::vector<LogicalPlanStep> steps;
};

struct PhysicalPlanStep {
  LogicalStepKind logical_kind = LogicalStepKind::Scan;
  PhysicalStepKind physical_kind = PhysicalStepKind::SourceOnly;
  LogicalPlanStep logical;
  bool fused = false;
  bool pipeline_barrier = true;
  std::string reason;
};

struct PhysicalPlan {
  DataFrame seed;
  std::vector<PhysicalPlanStep> steps;
};

enum class StreamPlanNodeKind {
  Scan,
  Filter,
  Project,
  WithColumn,
  Limit,
  OrderBy,
  WindowAssign,
  Aggregate,
  Sink,
};

struct StreamPlanNode {
  StreamPlanNodeKind kind = StreamPlanNodeKind::Scan;
  std::string source_name;
  std::string sink_name;
  std::string column;
  std::string op;
  Value value;
  std::vector<std::string> columns;
  std::vector<std::string> order_columns;
  std::vector<bool> order_ascending;
  std::vector<std::pair<std::string, std::string>> aliases;
  std::vector<std::string> group_keys;
  std::vector<StreamAggregateSpec> aggregates;
  std::string value_column;
  std::string output_column;
  ComputedColumnKind with_function = ComputedColumnKind::Copy;
  std::vector<ComputedColumnArg> with_args;
  bool stateful = false;
  std::size_t limit = 0;
  uint64_t window_ms = 0;
};

struct StreamLogicalPlan {
  std::string source_name;
  std::string sink_name;
  bool writes_to_sink = false;
  std::vector<StreamPlanNode> nodes;
};

struct StreamPhysicalPlan {
  StreamLogicalPlan logical;
  bool has_window = false;
  bool has_stateful_ops = false;
  bool actor_eligible = false;
  std::size_t partition_local_prefix_nodes = 0;
  std::string actor_eligibility_reason;
};

class SqlPlanner {
 public:
  DataFrame plan(const SqlQuery& query, const ViewCatalog& catalog) const;
  LogicalPlan buildLogicalPlan(const SqlQuery& query, const ViewCatalog& catalog) const;
  PhysicalPlan buildPhysicalPlan(const LogicalPlan& logical) const;
  std::string explainLogicalPlan(const LogicalPlan& logical) const;
  std::string explainPhysicalPlan(const PhysicalPlan& physical) const;
  DataFrame materializeFromPhysical(const PhysicalPlan& physical) const;
  StreamLogicalPlan buildStreamLogicalPlan(const SqlQuery& query,
                                           const std::string& sink_name = "") const;
  StreamPhysicalPlan buildStreamPhysicalPlan(const StreamLogicalPlan& logical) const;
  std::string explainStreamLogicalPlan(const StreamLogicalPlan& logical) const;
  std::string explainStreamPhysicalPlan(const StreamPhysicalPlan& physical) const;
  StreamingDataFrame materializeStreamFromPhysical(
      const StreamPhysicalPlan& physical,
      const std::unordered_map<std::string, StreamingDataFrame>& stream_views) const;
  StreamingDataFrame planStream(
      const SqlQuery& query,
      const std::unordered_map<std::string, StreamingDataFrame>& stream_views) const;
};

}  // namespace sql
}  // namespace dataflow
