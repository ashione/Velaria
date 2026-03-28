#pragma once

#include <string>
#include <vector>

#include "src/dataflow/api/dataframe.h"
#include "src/dataflow/catalog/catalog.h"
#include "src/dataflow/sql/sql_ast.h"

namespace dataflow {
namespace sql {

enum class LogicalStepKind { Scan, Filter, Join, Aggregate, Having, Project, Limit };
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

class SqlPlanner {
 public:
  DataFrame plan(const SqlQuery& query, const ViewCatalog& catalog) const;
  LogicalPlan buildLogicalPlan(const SqlQuery& query, const ViewCatalog& catalog) const;
  PhysicalPlan buildPhysicalPlan(const LogicalPlan& logical) const;
  DataFrame materializeFromPhysical(const PhysicalPlan& physical) const;
};

}  // namespace sql
}  // namespace dataflow
