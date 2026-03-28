#include "src/dataflow/api/session.h"

#include <atomic>
#include <chrono>
#include <stdexcept>
#include <unordered_set>
#include <sstream>
#include <vector>

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

StreamingDataFrame DataflowSession::readStream(std::shared_ptr<StreamSource> source) {
  return StreamingDataFrame(std::move(source));
}

StreamingDataFrame DataflowSession::readStreamCsvDir(const std::string& directory, char delimiter) {
  return readStream(std::make_shared<DirectoryCsvStreamSource>(directory, delimiter));
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

DataFrame dmlResult(const std::string& message, std::size_t affected_rows = 0) {
  Schema statusSchema({"ok", "message", "affected_rows"});
  Row row;
  row.emplace_back(static_cast<int64_t>(1));
  row.emplace_back(message);
  row.emplace_back(static_cast<int64_t>(affected_rows));
  return DataFrame(Table(std::move(statusSchema), {row}));
}

DataFrame executeCreateTable(ViewCatalog& catalog, const sql::CreateTableStmt& stmt) {
  if (stmt.columns.empty()) {
    throw SQLSemanticError("CREATE TABLE requires at least one column");
  }
  std::vector<std::string> fields;
  fields.reserve(stmt.columns.size());
  std::unordered_set<std::string> names;
  for (const auto& col : stmt.columns) {
    if (col.name.empty()) {
      throw SQLSemanticError("column name cannot be empty");
    }
    if (!names.insert(col.name).second) {
      throw SQLSemanticError("duplicate column: " + col.name);
    }
    fields.push_back(col.name);
  }
  catalog.createTable(stmt.table, fields, stmt.kind);
  return dmlResult("create table done", 0);
}

std::size_t findNameIndex(const Schema& schema, const std::string& name) {
  for (std::size_t i = 0; i < schema.fields.size(); ++i) {
    if (schema.fields[i] == name) return i;
  }
  throw SQLSemanticError("column not found: " + name);
}

Table normalizeInsertValues(const Table& target, const std::vector<std::string>& columns,
                          const std::vector<Row>& values) {
  Table out(target.schema, {});
  std::unordered_set<std::string> targetCols;
  if (!columns.empty()) {
    std::vector<std::size_t> targets;
    targets.reserve(columns.size());
    for (const auto& col : columns) {
      if (!targetCols.insert(col).second) {
        throw SQLSemanticError("duplicate insert column: " + col);
      }
      targets.push_back(findNameIndex(target.schema, col));
    }
    for (const auto& row : values) {
      if (row.size() != targets.size()) {
        throw SQLSemanticError("INSERT VALUES column count mismatch");
      }
      Row outRow(target.schema.fields.size());
      for (std::size_t i = 0; i < targets.size(); ++i) {
        outRow[targets[i]] = row[i];
      }
      out.rows.push_back(std::move(outRow));
    }
    return out;
  }

  for (const auto& row : values) {
    if (row.size() != target.schema.fields.size()) {
      throw SQLSemanticError("INSERT VALUES column count mismatch");
    }
    out.rows.push_back(row);
  }
  return out;
}

Table normalizeInsertSelect(const Table& target, const std::vector<std::string>& columns,
                          const Table& source) {
  Table out(target.schema, {});
  if (columns.empty()) {
    if (source.schema.fields.size() != target.schema.fields.size()) {
      throw SQLSemanticError("INSERT SELECT column count mismatch");
    }
    for (std::size_t i = 0; i < source.schema.fields.size(); ++i) {
      if (source.schema.fields[i] != target.schema.fields[i]) {
        throw SQLSemanticError("INSERT SELECT column mismatch: " + source.schema.fields[i]);
      }
    }
    out.rows = source.rows;
    return out;
  }

  std::unordered_set<std::string> targetCols;
  std::vector<std::size_t> sourceIndices;
  std::vector<std::size_t> targetIndices;
  sourceIndices.reserve(columns.size());
  targetIndices.reserve(columns.size());
  for (const auto& col : columns) {
    if (!targetCols.insert(col).second) {
      throw SQLSemanticError("duplicate insert column: " + col);
    }
    sourceIndices.push_back(findNameIndex(source.schema, col));
    targetIndices.push_back(findNameIndex(target.schema, col));
  }
  for (const auto& row : source.rows) {
    if (row.size() != source.schema.fields.size()) {
      throw SQLSemanticError("INSERT SELECT column count mismatch");
    }
    Row outRow(target.schema.fields.size());
    for (std::size_t i = 0; i < columns.size(); ++i) {
      outRow[targetIndices[i]] = row[sourceIndices[i]];
    }
    out.rows.push_back(std::move(outRow));
  }
  return out;
}

DataFrame executeInsert(ViewCatalog& catalog, const sql::InsertStmt& stmt) {
  if (catalog.isSourceTable(stmt.table)) {
    throw SQLSemanticError("INSERT INTO is not allowed on SOURCE TABLE: " + stmt.table);
  }
  DataFrame& view = catalog.getViewMutable(stmt.table);
  Table target = view.toTable();
  Table toInsert;
  if (stmt.select_from) {
    sql::SqlPlanner planner;
    toInsert = planner.plan(stmt.query, catalog).toTable();
    toInsert = normalizeInsertSelect(target, stmt.columns, toInsert);
  } else {
    toInsert = normalizeInsertValues(target, stmt.columns, stmt.values);
  }
  auto rows = toInsert.rows.size();
  if (rows == 0) {
    return dmlResult("insert ignored: empty input", 0);
  }
  catalog.appendToView(stmt.table, toInsert);
  return dmlResult("insert done", rows);
}

void setSqlPayloadAfterParse(ai::PluginPayload& payload, const std::string& sql,
                            const sql::SqlStatement& statement) {
  payload.sql = sql;
  payload.summary = "session.sql entry";
  if (statement.kind == sql::SqlStatementKind::CreateTable) {
    payload.summary = "create table statement";
  } else if (statement.kind == sql::SqlStatementKind::InsertValues) {
    payload.summary = "insert values statement";
  } else if (statement.kind == sql::SqlStatementKind::InsertSelect) {
    payload.summary = "insert select statement";
  } else {
    payload.summary = "select statement";
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
  ai::PluginPayload build_payload;
  setSqlPayloadAfterParse(payload, sql, {});
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
  auto statement = sql::SqlParser::parse(final_sql);
  setSqlPayloadAfterParse(payload, final_sql, statement);

  auto after_parse = ai::PluginManager::instance().runHook(ai::HookPoint::kAfterSqlParse, ctx, &payload);
  if (after_parse.final_action == ai::PluginAction::Block ||
      after_parse.final_action == ai::PluginAction::Abort) {
    throw std::runtime_error(after_parse.reason.empty() ? "sql was blocked by ai plugin after parse"
                                                       : after_parse.reason);
  }
  final_sql = payload.sql;

  build_payload.sql = final_sql;
  build_payload.summary = "plan building";
  auto before_build =
      ai::PluginManager::instance().runHook(ai::HookPoint::kBeforePlanBuild, ctx, &build_payload);
  if (before_build.final_action == ai::PluginAction::Block ||
      before_build.final_action == ai::PluginAction::Abort) {
    throw std::runtime_error(before_build.reason.empty() ? "plan build was blocked by ai plugin"
                                                       : before_build.reason);
  }

  if (statement.kind == sql::SqlStatementKind::CreateTable) {
    const auto result = executeCreateTable(catalog_, statement.create);
    build_payload.plan = "CREATE TABLE";
    build_payload.summary = "create table completed";
    auto after_build = ai::PluginManager::instance().runHook(ai::HookPoint::kAfterPlanBuild, ctx,
                                                            &build_payload);
    if (after_build.final_action == ai::PluginAction::Block ||
        after_build.final_action == ai::PluginAction::Abort) {
      throw std::runtime_error(after_build.reason.empty() ? "create table rejected by ai plugin"
                                                         : after_build.reason);
    }
    return result;
  }
  if (statement.kind == sql::SqlStatementKind::InsertValues ||
      statement.kind == sql::SqlStatementKind::InsertSelect) {
    const auto result = executeInsert(catalog_, statement.insert);
    build_payload.plan = "INSERT";
    build_payload.summary = "insert completed";
    auto after_build = ai::PluginManager::instance().runHook(ai::HookPoint::kAfterPlanBuild, ctx,
                                                            &build_payload);
    if (after_build.final_action == ai::PluginAction::Block ||
        after_build.final_action == ai::PluginAction::Abort) {
      throw std::runtime_error(after_build.reason.empty() ? "insert rejected by ai plugin"
                                                         : after_build.reason);
    }
    return result;
  }

  sql::SqlPlanner planner;
  if (statement.kind != sql::SqlStatementKind::Select) {
    throw SQLSyntaxError("unsupported statement kind");
  }
  DataFrame result = planner.plan(statement.query, catalog_);
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
