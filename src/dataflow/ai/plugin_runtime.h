#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>
#include <string>
#include <vector>

namespace dataflow {
namespace ai {

enum class HookPoint {
  kBeforeSqlParse,
  kAfterSqlParse,
  kBeforePlanBuild,
  kAfterPlanBuild,
  kPlanBeforeExecute,
  kPlanAfterExecute,
  kStreamingBatchStart,
  kStreamingBatchEnd,
};

const char* toString(HookPoint point);

enum class PluginAction {
  Continue,
  Block,
  Fallback,
  Abort,
};

struct PluginContext {
  std::string trace_id;
  std::string session_id;
  std::string actor;
  std::string tenant_id;
  bool dry_run = false;
  std::size_t max_rows = 0;
  std::unordered_map<std::string, std::string> labels;
};

struct PluginPayload {
  std::string sql;
  std::string plan;
  std::string summary;
  std::size_t row_count = 0;
  std::string message;
  std::unordered_map<std::string, std::string> attributes;
};

struct PluginResult {
  PluginAction action = PluginAction::Continue;
  std::string rewritten_sql;
  std::string fallback_sql;
  std::string note;
};

struct PluginPolicy {
  bool enabled = true;
  bool fail_open = true;
  bool auto_disable_on_error = false;
};

class IExecutionPlugin {
 public:
  virtual ~IExecutionPlugin() = default;
  virtual const char* name() const = 0;
  virtual int priority() const { return 0; }
  virtual bool supports(HookPoint point) const { return true; }
  virtual void configure(const std::unordered_map<std::string, std::string>& config) { (void)config; }

  virtual PluginResult onBeforeSqlParse(const PluginContext&, PluginPayload&) { return {}; }
  virtual PluginResult onAfterSqlParse(const PluginContext&, PluginPayload&) { return {}; }
  virtual PluginResult onBeforePlanBuild(const PluginContext&, PluginPayload&) { return {}; }
  virtual PluginResult onAfterPlanBuild(const PluginContext&, PluginPayload&) { return {}; }
  virtual PluginResult onPlanBeforeExecute(const PluginContext&, PluginPayload&) { return {}; }
  virtual PluginResult onPlanAfterExecute(const PluginContext&, PluginPayload&) { return {}; }
  virtual PluginResult onStreamingBatchStart(const PluginContext&, PluginPayload&) { return {}; }
  virtual PluginResult onStreamingBatchEnd(const PluginContext&, PluginPayload&) { return {}; }
};

struct HookOutcome {
  PluginAction final_action = PluginAction::Continue;
  std::string reason;
  std::vector<std::string> plugin_trace;
  std::vector<std::string> warnings;

  bool continue_execution() const { return final_action == PluginAction::Continue || final_action == PluginAction::Fallback; }
  bool fallback_selected() const { return final_action == PluginAction::Fallback; }
};

class PluginManager {
 public:
  struct RegisteredPlugin {
    std::shared_ptr<IExecutionPlugin> plugin;
    PluginPolicy policy;
  };

  static PluginManager& instance();

  void registerPlugin(std::shared_ptr<IExecutionPlugin> plugin);
  void unregisterPlugin(const std::string& name);
  void setGlobalPolicy(const PluginPolicy& policy);
  PluginPolicy globalPolicy() const;

  void configurePlugin(const std::string& name, const std::unordered_map<std::string, std::string>& config);
  void setPluginPolicy(const std::string& name, const PluginPolicy& policy);
  bool isRegistered(const std::string& name) const;
  bool isEnabled(const std::string& name) const;

  HookOutcome runHook(HookPoint point, const PluginContext& ctx, PluginPayload* payload) const;

 private:
  PluginManager() = default;
  void sortByPriorityLocked() const;

  mutable std::mutex mu_;
  mutable std::vector<RegisteredPlugin> plugins_;
  PluginPolicy global_policy_;
};

}  // namespace ai
}  // namespace dataflow
