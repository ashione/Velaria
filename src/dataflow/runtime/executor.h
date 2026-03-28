#pragma once

#include <memory>

#include "src/dataflow/planner/plan.h"

namespace dataflow {

class DataFrame;  // fwd

class Executor {
 public:
  virtual ~Executor() = default;
  virtual Table execute(const PlanNodePtr& plan) const = 0;
};

class LocalExecutor : public Executor {
 public:
  Table execute(const PlanNodePtr& plan) const override;
};

}  // namespace dataflow
