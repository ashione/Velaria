#include "src/dataflow/ai/plugin_runtime.h"

#include <algorithm>
#include <stdexcept>
#include <mutex>

namespace dataflow {
namespace ai {

namespace {

const char* hookPointToString(HookPoint point) {
  switch (point) {
    case HookPoint::kBeforeSqlParse:
      return "BeforeSqlParse";
    case HookPoint::kAfterSqlParse:
      return "AfterSqlParse";
    case HookPoint::kBeforePlanBuild:
      return "BeforePlanBuild";
    case HookPoint::kAfterPlanBuild:
      return "AfterPlanBuild";
    case HookPoint::kPlanBeforeExecute:
      return "PlanBeforeExecute";
    case HookPoint::kPlanAfterExecute:
      return "PlanAfterExecute";
    case HookPoint::kStreamingBatchStart:
      return "StreamingBatchStart";
    case HookPoint::kStreamingBatchEnd:
      return "StreamingBatchEnd";
  }
  return "Unknown";
}

PluginResult invokeHook(const std::shared_ptr<IExecutionPlugin>& plugin, HookPoint point,
                       const PluginContext& ctx, PluginPayload* payload) {
  switch (point) {
    case HookPoint::kBeforeSqlParse:
      return plugin->onBeforeSqlParse(ctx, *payload);
    case HookPoint::kAfterSqlParse:
      return plugin->onAfterSqlParse(ctx, *payload);
    case HookPoint::kBeforePlanBuild:
      return plugin->onBeforePlanBuild(ctx, *payload);
    case HookPoint::kAfterPlanBuild:
      return plugin->onAfterPlanBuild(ctx, *payload);
    case HookPoint::kPlanBeforeExecute:
      return plugin->onPlanBeforeExecute(ctx, *payload);
    case HookPoint::kPlanAfterExecute:
      return plugin->onPlanAfterExecute(ctx, *payload);
    case HookPoint::kStreamingBatchStart:
      return plugin->onStreamingBatchStart(ctx, *payload);
    case HookPoint::kStreamingBatchEnd:
      return plugin->onStreamingBatchEnd(ctx, *payload);
  }
  return {};
}

}  // namespace

const char* toString(HookPoint point) { return hookPointToString(point); }

PluginManager& PluginManager::instance() {
  static PluginManager* instance = nullptr;
  static std::mutex init_mu;
  std::lock_guard<std::mutex> lock(init_mu);
  if (instance == nullptr) {
    static PluginManager value;
    instance = &value;
  }
  return *instance;
}

void PluginManager::sortByPriorityLocked() const {
  std::sort(plugins_.begin(), plugins_.end(), [](const RegisteredPlugin& lhs, const RegisteredPlugin& rhs) {
    return lhs.plugin->priority() > rhs.plugin->priority();
  });
}

void PluginManager::registerPlugin(std::shared_ptr<IExecutionPlugin> plugin) {
  if (!plugin) {
    throw std::runtime_error("PluginManager::registerPlugin received null plugin");
  }
  std::lock_guard<std::mutex> lock(mu_);
  for (const auto& item : plugins_) {
    if (item.plugin->name() == plugin->name()) {
      return;
    }
  }
  plugins_.push_back({std::move(plugin), PluginPolicy()});
  sortByPriorityLocked();
}

void PluginManager::unregisterPlugin(const std::string& name) {
  std::lock_guard<std::mutex> lock(mu_);
  plugins_.erase(
      std::remove_if(plugins_.begin(), plugins_.end(),
                     [&](const RegisteredPlugin& item) { return item.plugin->name() == name; }),
      plugins_.end());
}

void PluginManager::setGlobalPolicy(const PluginPolicy& policy) {
  std::lock_guard<std::mutex> lock(mu_);
  global_policy_ = policy;
}

PluginPolicy PluginManager::globalPolicy() const {
  std::lock_guard<std::mutex> lock(mu_);
  return global_policy_;
}

void PluginManager::configurePlugin(const std::string& name,
                                   const std::unordered_map<std::string, std::string>& config) {
  std::lock_guard<std::mutex> lock(mu_);
  for (auto& item : plugins_) {
    if (item.plugin->name() == name) {
      item.plugin->configure(config);
      return;
    }
  }
}

void PluginManager::setPluginPolicy(const std::string& name, const PluginPolicy& policy) {
  std::lock_guard<std::mutex> lock(mu_);
  for (auto& item : plugins_) {
    if (item.plugin->name() == name) {
      item.policy = policy;
      return;
    }
  }
}

bool PluginManager::isRegistered(const std::string& name) const {
  std::lock_guard<std::mutex> lock(mu_);
  for (const auto& item : plugins_) {
    if (item.plugin->name() == name) {
      return true;
    }
  }
  return false;
}

bool PluginManager::isEnabled(const std::string& name) const {
  std::lock_guard<std::mutex> lock(mu_);
  for (const auto& item : plugins_) {
    if (item.plugin->name() == name) {
      return item.policy.enabled;
    }
  }
  return false;
}

HookOutcome PluginManager::runHook(HookPoint point, const PluginContext& ctx, PluginPayload* payload) const {
  HookOutcome out;
  if (!payload) {
    return out;
  }

  std::vector<RegisteredPlugin> local_plugins;
  PluginPolicy global_policy;
  {
    std::lock_guard<std::mutex> lock(mu_);
    local_plugins = plugins_;
    global_policy = global_policy_;
  }

  for (const auto& item : local_plugins) {
    if (!item.plugin || !item.policy.enabled) {
      continue;
    }
    if (!item.plugin->supports(point)) {
      continue;
    }

    PluginResult r;
    try {
      r = invokeHook(item.plugin, point, ctx, payload);
      if (!r.rewritten_sql.empty()) {
        payload->sql = r.rewritten_sql;
        payload->summary = "rewritten by plugin:" + std::string(item.plugin->name());
      }
      if (!r.note.empty()) {
        out.warnings.push_back(r.note);
      }
      out.plugin_trace.push_back(item.plugin->name());
    } catch (const std::exception& ex) {
      const std::string warn = std::string("plugin exception: ") + item.plugin->name() + ", " + ex.what();
      out.warnings.push_back(warn);
      if (!item.policy.fail_open && !global_policy.fail_open) {
        out.final_action = PluginAction::Abort;
        out.reason = warn;
        return out;
      }
      if (item.policy.auto_disable_on_error) {
        const_cast<PluginManager*>(this)->setPluginPolicy(item.plugin->name(),
                                                          PluginPolicy{false, item.policy.fail_open, false});
      }
      continue;
    }

    if (!r.fallback_sql.empty()) {
      payload->plan = r.fallback_sql;
      out.final_action = PluginAction::Fallback;
      out.reason = r.note;
    }
    if (r.action == PluginAction::Block) {
      out.final_action = PluginAction::Block;
      out.reason = r.note.empty() ? "blocked by plugin" : r.note;
      return out;
    }
    if (r.action == PluginAction::Abort) {
      out.final_action = PluginAction::Abort;
      out.reason = r.note.empty() ? "aborted by plugin" : r.note;
      return out;
    }
  }

  if (out.final_action == PluginAction::Fallback) {
    out.final_action = PluginAction::Fallback;
  }
  return out;
}

}  // namespace ai
}  // namespace dataflow
