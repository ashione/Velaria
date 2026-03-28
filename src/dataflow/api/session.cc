#include "src/dataflow/api/session.h"

#include <atomic>
#include <stdexcept>
#include <chrono>
#include <sstream>

#include "src/dataflow/core/csv.h"
#include "src/dataflow/ai/plugin_runtime.h"

namespace dataflow {

namespace {

std::string nextQueryId() {
  static std::atomic<std::size_t> id{1};
  std::ostringstream oss;
  oss << "query-" << id.fetch_add(1);
  return oss.str();
}

}  // namespace

DataflowSession& DataflowSession::builder() {
  static DataflowSession session;
  return session;
}

DataFrame DataflowSession::read_csv(const std::string& path, char delimiter) {
  return DataFrame(load_csv(path, delimiter));
}

DataFrame DataflowSession::createDataFrame(const Table& table) {
  return DataFrame(table);
}

namespace {

Table submitAndWait(const DataflowJobHandle& handle) {
  while (true) {
    auto snap = handle.wait(std::chrono::milliseconds(100));
    if (snap.state == JobState::Succeeded) {
      return snap.result;
    }
    if (snap.state == JobState::Failed || snap.state == JobState::Cancelled ||
        snap.state == JobState::Timeout) {
      throw std::runtime_error(snap.state_message.empty() ? "job execution failed"
                                                         : snap.state_message);
    }
  }
}

}  // namespace

DataflowJobHandle DataflowSession::submitAsync(const DataFrame& df, const ExecutionOptions& options) {
  return df.submitAsync(options);
}

DataflowJobHandle DataflowSession::submitAsync(const std::string& sql, const ExecutionOptions& options) {
  return submitAsync(this->sql(sql), options);
}

Table DataflowSession::submit(const DataFrame& df, const ExecutionOptions& options) {
  return submitAndWait(df.submitAsync(options));
}

Table DataflowSession::submit(const std::string& sql, const ExecutionOptions& options) {
  return submitAndWait(submitAsync(sql, options));
}

void DataflowSession::createTempView(const std::string& name, const DataFrame& df) {
  catalog_.createView(name, df);
}

DataFrame DataflowSession::sql(const std::string& sql) {
  ai::PluginContext ctx;
  ctx.trace_id = nextQueryId();
  ctx.session_id = "default";
  ctx.labels["api"] = "DataflowSession::sql";

  ai::PluginPayload payload;
  payload.sql = sql;
  payload.summary = "session.sql entry";

  auto before_parse =
      ai::PluginManager::instance().runHook(ai::HookPoint::kBeforeSqlParse, ctx, &payload);
  if (before_parse.final_action == ai::PluginAction::Block ||
      before_parse.final_action == ai::PluginAction::Abort) {
    throw std::runtime_error(before_parse.reason.empty() ? "sql was blocked by ai plugin"
                                                        : before_parse.reason);
  }

  std::string final_sql = payload.sql;
  auto query = sql::SqlParser::parse(final_sql);

  auto after_parse = ai::PluginManager::instance().runHook(ai::HookPoint::kAfterSqlParse, ctx, &payload);
  if (after_parse.final_action == ai::PluginAction::Block ||
      after_parse.final_action == ai::PluginAction::Abort) {
    throw std::runtime_error(after_parse.reason.empty() ? "sql was blocked by ai plugin after parse"
                                                       : after_parse.reason);
  }
  final_sql = payload.sql;

  ai::PluginPayload build_payload;
  build_payload.sql = final_sql;
  build_payload.summary = "plan building";
  auto before_build =
      ai::PluginManager::instance().runHook(ai::HookPoint::kBeforePlanBuild, ctx, &build_payload);
  if (before_build.final_action == ai::PluginAction::Block ||
      before_build.final_action == ai::PluginAction::Abort) {
    throw std::runtime_error(before_build.reason.empty() ? "plan build was blocked by ai plugin"
                                                        : before_build.reason);
  }

  sql::SqlPlanner planner;
  DataFrame result = planner.plan(query, catalog_);
  build_payload.plan = result.explain();
  build_payload.summary = "plan built";

  auto after_build = ai::PluginManager::instance().runHook(ai::HookPoint::kAfterPlanBuild, ctx,
                                                          &build_payload);
  if (after_build.final_action == ai::PluginAction::Block ||
      after_build.final_action == ai::PluginAction::Abort) {
    throw std::runtime_error(after_build.reason.empty() ? "plan rejected by ai plugin" : after_build.reason);
  }

  return result;
}

}  // namespace dataflow
