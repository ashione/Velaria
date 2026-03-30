#pragma once

#include <atomic>
#include <memory>

#include "src/dataflow/core/logical/planner/plan.h"

namespace dataflow {

class DataFrame;  // fwd
class RpcRunner;

class Executor {
 public:
  virtual ~Executor() = default;
  virtual Table execute(const PlanNodePtr& plan) const = 0;
};

class LocalExecutor : public Executor {
 public:
  Table execute(const PlanNodePtr& plan) const override;
};

class RunnerExecutor : public Executor {
 public:
  explicit RunnerExecutor(std::shared_ptr<RpcRunner> runner,
                         std::shared_ptr<Executor> fallback = nullptr,
                         bool dual_path = true);
  Table execute(const PlanNodePtr& plan) const override;

 private:
  std::shared_ptr<RpcRunner> runner_;
  std::shared_ptr<Executor> fallback_;
  bool dual_path_;
  mutable std::atomic<uint64_t> next_message_id_{1};
};

}  // namespace dataflow
