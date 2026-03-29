#include "src/dataflow/api/session.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <memory>
#include <optional>
#include <stdexcept>
#include <sstream>
#include <unordered_set>
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

std::string formatStreamStrategyExplain(const StreamingStrategyDecision& strategy,
                                        const StreamingQueryOptions& options) {
  std::ostringstream out;
  out << "requested_mode=" << strategy.requested_execution_mode << "\n";
  out << "selected_mode=" << strategy.resolved_execution_mode << "\n";
  out << "reason=" << strategy.reason << "\n";
  out << "actor_eligible=" << (strategy.actor_eligible ? "true" : "false") << "\n";
  out << "used_actor_runtime=" << (strategy.used_actor_runtime ? "true" : "false") << "\n";
  out << "transport_mode=" << strategy.transport_mode << "\n";
  out << "has_window=" << (strategy.has_window ? "true" : "false") << "\n";
  out << "has_stateful_ops=" << (strategy.has_stateful_ops ? "true" : "false") << "\n";
  out << "sink_is_blocking=" << (strategy.sink_is_blocking ? "true" : "false") << "\n";
  out << "source_is_bounded=" << (strategy.source_is_bounded ? "true" : "false") << "\n";
  out << "estimated_state_size_bytes=" << strategy.estimated_state_size_bytes << "\n";
  out << "estimated_batch_cost=" << strategy.estimated_batch_cost << "\n";
  out << "backpressure_max_queue_batches=" << strategy.backpressure_max_queue_batches << "\n";
  out << "backpressure_high_watermark=" << strategy.backpressure_high_watermark << "\n";
  out << "backpressure_low_watermark=" << strategy.backpressure_low_watermark << "\n";
  out << "checkpoint_delivery_mode=" << strategy.checkpoint_delivery_mode << "\n";
  out << "actor_workers=" << std::max<size_t>(2, options.effectiveActorWorkers()) << "\n";
  out << "actor_max_inflight_partitions="
      << std::max<size_t>(1, options.actor_max_inflight_partitions > 0
                                 ? options.actor_max_inflight_partitions
                                 : options.effectiveActorWorkers())
      << "\n";
  out << "actor_shared_memory_transport="
      << (options.actor_shared_memory_transport ? "true" : "false") << "\n";
  out << "actor_shared_memory_min_payload_bytes="
      << options.actor_shared_memory_min_payload_bytes << "\n";
  out << "auto_sample_batches=" << options.actor_auto_options.sample_batches << "\n";
  out << "auto_min_rows_per_batch=" << options.actor_auto_options.min_rows_per_batch << "\n";
  out << "auto_min_projected_payload_bytes="
      << options.actor_auto_options.min_projected_payload_bytes << "\n";
  out << "auto_min_compute_to_overhead_ratio="
      << options.actor_auto_options.min_compute_to_overhead_ratio << "\n";
  out << "auto_min_actor_speedup=" << options.actor_auto_options.min_actor_speedup << "\n";
  out << "auto_strong_actor_speedup=" << options.actor_auto_options.strong_actor_speedup << "\n";
  return out.str();
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

std::vector<std::string> validateCreateColumns(const sql::CreateTableStmt& stmt) {
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
  return fields;
}

DataFrame executeCreateTable(ViewCatalog& catalog, const sql::CreateTableStmt& stmt) {
  const auto fields = validateCreateColumns(stmt);
  catalog.createTable(stmt.table, fields, stmt.kind);
  return dmlResult("create table done", 0);
}

std::string toLower(std::string value) {
  for (char& c : value) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  return value;
}

char csvDelimiter(const sql::CreateTableStmt& stmt) {
  auto it = stmt.options.find("delimiter");
  if (it == stmt.options.end()) {
    return ',';
  }
  if (it->second.empty()) {
    throw SQLSemanticError("csv delimiter cannot be empty");
  }
  return it->second[0];
}

std::string requireCreateOption(const sql::CreateTableStmt& stmt, const std::string& key) {
  auto it = stmt.options.find(key);
  if (it == stmt.options.end() || it->second.empty()) {
    throw SQLSemanticError("CREATE TABLE missing option: " + key);
  }
  return it->second;
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

std::string filterOp(sql::BinaryOperatorKind op) {
  switch (op) {
    case sql::BinaryOperatorKind::Eq:
      return "=";
    case sql::BinaryOperatorKind::Ne:
      return "!=";
    case sql::BinaryOperatorKind::Lt:
      return "<";
    case sql::BinaryOperatorKind::Lte:
      return "<=";
    case sql::BinaryOperatorKind::Gt:
      return ">";
    case sql::BinaryOperatorKind::Gte:
      return ">=";
  }
  throw SQLSemanticError("unsupported predicate operator");
}

void ensureSingleTableQuery(const sql::SqlQuery& query) {
  if (!query.has_from) {
    throw SQLSemanticError("stream SQL requires FROM");
  }
  if (query.join.has_value()) {
    throw SQLSemanticError("stream SQL does not support JOIN");
  }
  if (std::any_of(query.select_items.begin(), query.select_items.end(),
                  [](const sql::SelectItem& item) { return item.is_literal; })) {
    throw SQLSemanticError("stream SQL does not support literal projection");
  }
}

void ensureQualifierMatches(const sql::ColumnRef& ref, const sql::FromItem& from) {
  if (ref.qualifier.empty()) return;
  if (ref.qualifier == from.name) return;
  if (!from.alias.empty() && ref.qualifier == from.alias) return;
  throw SQLSemanticError("stream SQL qualifier does not match source: " + ref.qualifier);
}

std::string resolveColumnName(const sql::ColumnRef& ref, const sql::FromItem& from) {
  ensureQualifierMatches(ref, from);
  return ref.name;
}

bool isAggregateQuery(const sql::SqlQuery& query) {
  return !query.group_by.empty() ||
         std::any_of(query.select_items.begin(), query.select_items.end(),
                     [](const sql::SelectItem& item) { return item.is_aggregate; });
}

std::string aggregateOutputName(const sql::SelectItem& item) {
  if (!item.is_aggregate) {
    throw SQLSemanticError("expected aggregate select item");
  }
  if (!item.alias.empty()) return item.alias;
  switch (item.aggregate.function) {
    case sql::AggregateFunctionKind::Sum:
      return "sum";
    case sql::AggregateFunctionKind::Count:
      return "count";
    case sql::AggregateFunctionKind::Avg:
      return "avg";
    case sql::AggregateFunctionKind::Min:
      return "min";
    case sql::AggregateFunctionKind::Max:
      return "max";
  }
  throw SQLSemanticError("unsupported aggregate function");
}

void validateStreamAggregate(const sql::AggregateExpr& aggregate) {
  switch (aggregate.function) {
    case sql::AggregateFunctionKind::Sum:
      return;
    case sql::AggregateFunctionKind::Count:
      if (!aggregate.count_all) {
        throw SQLSemanticError("stream SQL only supports COUNT(*)");
      }
      return;
    case sql::AggregateFunctionKind::Avg:
    case sql::AggregateFunctionKind::Min:
    case sql::AggregateFunctionKind::Max:
      break;
  }
  throw SQLSemanticError("stream SQL aggregate only supports SUM and COUNT(*)");
}

StreamingDataFrame projectStreamSelect(const StreamingDataFrame& current, const sql::SqlQuery& query,
                                      const sql::FromItem& from) {
  bool select_all = false;
  std::vector<std::string> columns;
  std::vector<std::pair<std::string, std::string>> aliases;
  for (const auto& item : query.select_items) {
    if (item.is_all) {
      if (query.select_items.size() != 1) {
        throw SQLSemanticError("stream SQL SELECT * cannot be mixed with other projections");
      }
      select_all = true;
      break;
    }
    if (item.is_table_all) {
      if (query.select_items.size() != 1) {
        throw SQLSemanticError("stream SQL table.* cannot be mixed with other projections");
      }
      if (item.table_name_or_alias != from.name &&
          (from.alias.empty() || item.table_name_or_alias != from.alias)) {
        throw SQLSemanticError("stream SQL table.* does not match source: " + item.table_name_or_alias);
      }
      select_all = true;
      break;
    }
    if (item.is_aggregate) {
      throw SQLSemanticError("aggregate projection requires GROUP BY flow");
    }
    const auto column = resolveColumnName(item.column, from);
    if (!item.alias.empty() && item.alias != column) {
      aliases.push_back({item.alias, column});
      columns.push_back(item.alias);
    } else {
      columns.push_back(column);
    }
  }

  if (select_all) {
    return current;
  }

  StreamingDataFrame projected = current;
  for (const auto& alias : aliases) {
    projected = projected.withColumn(alias.first, alias.second);
  }
  return projected.select(columns);
}

StreamingDataFrame buildAggregateStream(const StreamingDataFrame& seed, const sql::SqlQuery& query,
                                       const sql::FromItem& from) {
  if (query.select_items.empty()) {
    throw SQLSemanticError("stream SQL aggregate query requires projections");
  }
  std::vector<std::string> group_keys;
  group_keys.reserve(query.group_by.size());
  for (const auto& key : query.group_by) {
    group_keys.push_back(resolveColumnName(key, from));
  }

  std::optional<sql::SelectItem> aggregate_item;
  for (const auto& item : query.select_items) {
    if (item.is_all || item.is_table_all) {
      throw SQLSemanticError("stream SQL aggregate query does not support star projection");
    }
    if (item.is_aggregate) {
      if (aggregate_item.has_value()) {
        throw SQLSemanticError("stream SQL aggregate query supports only one aggregate output");
      }
      validateStreamAggregate(item.aggregate);
      aggregate_item = item;
      continue;
    }
    const auto column = resolveColumnName(item.column, from);
    if (std::find(group_keys.begin(), group_keys.end(), column) == group_keys.end()) {
      throw SQLSemanticError("non-aggregate field must appear in GROUP BY: " + column);
    }
  }

  if (!aggregate_item.has_value()) {
    throw SQLSemanticError("stream SQL aggregate query requires one aggregate output");
  }

  const auto output_name = aggregateOutputName(*aggregate_item);
  auto grouped = seed.groupBy(group_keys);
  StreamingDataFrame aggregated =
      aggregate_item->aggregate.function == sql::AggregateFunctionKind::Count
          ? grouped.count(true, output_name)
          : grouped.sum(resolveColumnName(aggregate_item->aggregate.argument, from), true, output_name);

  if (query.having.has_value()) {
    const auto& predicate = *query.having;
    const std::string having_column =
        predicate.lhs_is_aggregate ? output_name : resolveColumnName(predicate.lhs, from);
    aggregated = aggregated.filter(having_column, filterOp(predicate.op), predicate.rhs);
  }

  std::vector<std::string> final_columns;
  final_columns.reserve(query.select_items.size());
  for (const auto& item : query.select_items) {
    if (item.is_aggregate) {
      final_columns.push_back(output_name);
    } else {
      final_columns.push_back(resolveColumnName(item.column, from));
    }
  }
  return aggregated.select(final_columns);
}

[[maybe_unused]] StreamingDataFrame buildStreamingSelect(
    const sql::SqlQuery& query,
    const std::unordered_map<std::string, StreamingDataFrame>& stream_views) {
  ensureSingleTableQuery(query);
  auto it = stream_views.find(query.from.name);
  if (it == stream_views.end()) {
    throw CatalogNotFoundError("stream view not found: " + query.from.name);
  }

  StreamingDataFrame current = it->second;
  if (query.where.has_value()) {
    const auto& predicate = *query.where;
    if (predicate.lhs_is_aggregate) {
      throw SQLSemanticError("WHERE does not support aggregate expressions");
    }
    current = current.filter(resolveColumnName(predicate.lhs, query.from),
                             filterOp(predicate.op), predicate.rhs);
  }

  if (isAggregateQuery(query)) {
    if (query.having.has_value() && query.group_by.empty()) {
      throw SQLSemanticError("GROUP BY required for HAVING");
    }
    current = buildAggregateStream(current, query, query.from);
  } else {
    if (!query.group_by.empty()) {
      throw SQLSemanticError("GROUP BY used without aggregate");
    }
    if (query.having.has_value()) {
      throw SQLSemanticError("HAVING used without aggregate");
    }
    current = projectStreamSelect(current, query, query.from);
  }

  if (query.limit.has_value()) {
    current = current.limit(*query.limit);
  }
  return current;
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
  if (stream_views_.find(name) != stream_views_.end() || stream_sinks_.find(name) != stream_sinks_.end()) {
    throw SQLSemanticError("table already exists: " + name);
  }
  catalog_.createView(name, df);
}

void DataflowSession::createTempView(const std::string& name, const StreamingDataFrame& df) {
  if (catalog_.hasView(name) || stream_views_.find(name) != stream_views_.end() ||
      stream_sinks_.find(name) != stream_sinks_.end()) {
    throw SQLSemanticError("table already exists: " + name);
  }
  stream_views_.emplace(name, df);
}

void DataflowSession::registerStreamSink(const std::string& name, std::shared_ptr<StreamSink> sink) {
  if (sink == nullptr) {
    throw std::invalid_argument("stream sink cannot be null");
  }
  if (catalog_.hasView(name) || stream_views_.find(name) != stream_views_.end() ||
      stream_sinks_.find(name) != stream_sinks_.end()) {
    throw SQLSemanticError("table already exists: " + name);
  }
  stream_sinks_.emplace(name, std::move(sink));
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
    DataFrame result;
    if (!statement.create.provider.empty()) {
      const auto provider = toLower(statement.create.provider);
      if (provider != "csv") {
        throw SQLSemanticError("stream CREATE TABLE only supports USING csv");
      }
      validateCreateColumns(statement.create);
      const auto path = requireCreateOption(statement.create, "path");
      const auto delimiter = csvDelimiter(statement.create);
      if (statement.create.kind == sql::TableKind::Source) {
        createTempView(statement.create.table, readStreamCsvDir(path, delimiter));
        result = dmlResult("create source table done", 0);
      } else if (statement.create.kind == sql::TableKind::Sink) {
        registerStreamSink(statement.create.table,
                           std::make_shared<FileAppendStreamSink>(path, delimiter));
        result = dmlResult("create sink table done", 0);
      } else {
        throw SQLSemanticError("USING csv requires CREATE SOURCE TABLE or CREATE SINK TABLE");
      }
    } else {
      result = executeCreateTable(catalog_, statement.create);
    }
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

StreamingDataFrame DataflowSession::streamSql(const std::string& sql) {
  const auto statement = sql::SqlParser::parse(sql);
  if (statement.kind != sql::SqlStatementKind::Select) {
    throw SQLSyntaxError("stream SQL only supports SELECT");
  }
  sql::SqlPlanner planner;
  return planner.planStream(statement.query, stream_views_);
}

std::string DataflowSession::explainStreamSql(const std::string& sql,
                                              const StreamingQueryOptions& options) {
  const auto statement = sql::SqlParser::parse(sql);
  const sql::SqlQuery* query = nullptr;
  std::shared_ptr<StreamSink> sink;
  if (statement.kind == sql::SqlStatementKind::Select) {
    query = &statement.query;
  } else if (statement.kind == sql::SqlStatementKind::InsertSelect) {
    query = &statement.insert.query;
    auto sink_it = stream_sinks_.find(statement.insert.table);
    if (sink_it == stream_sinks_.end()) {
      throw CatalogNotFoundError("stream sink not found: " + statement.insert.table);
    }
    sink = sink_it->second;
  } else {
    throw SQLSyntaxError("explainStreamSql only supports SELECT or INSERT INTO ... SELECT");
  }

  sql::SqlPlanner planner;
  const auto logical = planner.buildStreamLogicalPlan(*query);
  const auto physical = planner.buildStreamPhysicalPlan(logical);
  const auto stream = planner.materializeStreamFromPhysical(physical, stream_views_);
  const auto strategy = describeStreamingStrategy(stream, sink, options);

  std::ostringstream out;
  out << "logical\n";
  out << planner.explainStreamLogicalPlan(logical);
  out << "physical\n";
  out << planner.explainStreamPhysicalPlan(physical);
  out << "strategy\n";
  out << formatStreamStrategyExplain(strategy, options);
  return out.str();
}

StreamingQuery DataflowSession::startStreamSql(const std::string& sql,
                                               const StreamingQueryOptions& options) {
  const auto statement = sql::SqlParser::parse(sql);
  if (statement.kind != sql::SqlStatementKind::InsertSelect) {
    throw SQLSyntaxError("startStreamSql only supports INSERT INTO ... SELECT");
  }
  auto sink_it = stream_sinks_.find(statement.insert.table);
  if (sink_it == stream_sinks_.end()) {
    throw CatalogNotFoundError("stream sink not found: " + statement.insert.table);
  }
  if (!statement.insert.columns.empty()) {
    throw SQLSemanticError("startStreamSql does not support INSERT column list");
  }

  sql::SqlPlanner planner;
  auto query = planner.planStream(statement.insert.query, stream_views_)
                   .writeStream(sink_it->second, options);
  query.start();
  return query;
}

}  // namespace dataflow
