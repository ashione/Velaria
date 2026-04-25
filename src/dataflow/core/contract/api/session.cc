#include "src/dataflow/core/contract/api/session.h"

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

#include "src/dataflow/core/execution/csv.h"
#include "src/dataflow/core/execution/columnar_batch.h"
#include "src/dataflow/core/execution/runtime/execution_optimizer.h"
#include "src/dataflow/core/execution/runtime/simd_dispatch.h"
#include "src/dataflow/ai/plugin_runtime.h"

namespace dataflow {

namespace {

[[noreturn]] void throwUnsupportedSqlV1(const std::string& detail) {
  throw SQLUnsupportedError("not supported in SQL v1: " + detail);
}

[[noreturn]] void throwTableKindViolation(const std::string& detail) {
  throw SQLTableKindError("table-kind constraint violation: " + detail);
}

std::string nextQueryId() {
  static std::atomic<std::size_t> id{1};
  std::ostringstream oss;
  oss << "query-" << id.fetch_add(1);
  return oss.str();
}

std::string formatStreamStrategyExplain(const StreamingStrategyDecision& strategy,
                                        const StreamingQueryOptions& options) {
  const auto compiled_backends = compiledSimdBackendNames();
  std::ostringstream out;
  out << "requested_mode=" << strategy.requested_execution_mode << "\n";
  out << "selected_mode=" << strategy.resolved_execution_mode << "\n";
  out << "simd_backend=" << activeSimdBackendName() << "\n";
  out << "compiled_backends=";
  for (std::size_t i = 0; i < compiled_backends.size(); ++i) {
    if (i > 0) out << ",";
    out << compiled_backends[i];
  }
  out << "\n";
  out << "aggregate_impl=" << strategy.aggregate_impl << "\n";
  out << "aggregate_reason=" << strategy.aggregate_reason << "\n";
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
  out << "local_worker_count=" << std::max<size_t>(1, options.effectiveLocalWorkers()) << "\n";
  out << "max_inflight_partitions="
      << std::max<size_t>(1, options.max_inflight_partitions > 0
                                 ? options.max_inflight_partitions
                                 : options.effectiveLocalWorkers())
      << "\n";
  out << "shared_memory_transport="
      << (options.shared_memory_transport ? "true" : "false") << "\n";
  out << "shared_memory_min_payload_bytes="
      << options.shared_memory_min_payload_bytes << "\n";
  return out.str();
}

std::string formatBatchAggregateStrategyExplain(
    const Table& input, const std::vector<std::size_t>& group_keys,
    const std::vector<AggregateSpec>& aggregates) {
  const auto pattern = analyzeAggregateExecution(input, group_keys, aggregates);
  const auto compiled_backends = compiledSimdBackendNames();
  std::ostringstream out;
  out << "selected_impl=" << aggregateExecKindName(pattern.exec_spec.impl_kind) << "\n";
  out << "simd_backend=" << activeSimdBackendName() << "\n";
  out << "compiled_backends=";
  for (std::size_t i = 0; i < compiled_backends.size(); ++i) {
    if (i > 0) out << ",";
    out << compiled_backends[i];
  }
  out << "\n";
  out << "partial_layout=" << aggregatePartialLayoutName(pattern.exec_spec.partial_layout) << "\n";
  out << "runtime_shape=" << aggregateExecutionShapeName(pattern.shape) << "\n";
  out << "key_count=" << pattern.exec_spec.properties.key_count << "\n";
  out << "ordered_input="
      << (pattern.exec_spec.properties.ordered_input ? "true" : "false") << "\n";
  out << "partition_local="
      << (pattern.exec_spec.properties.partition_local ? "true" : "false") << "\n";
  out << "fixed_width_keys="
      << (pattern.exec_spec.properties.all_fixed_width ? "true" : "false") << "\n";
  out << "packable_keys="
      << (pattern.exec_spec.properties.packable ? "true" : "false") << "\n";
  out << "low_cardinality="
      << (pattern.exec_spec.properties.low_cardinality ? "true" : "false") << "\n";
  out << "reason=" << pattern.exec_spec.reason << "\n";
  if (!pattern.exec_spec.rejected_candidates.empty()) {
    out << "rejected_candidates=";
    for (std::size_t i = 0; i < pattern.exec_spec.rejected_candidates.size(); ++i) {
      if (i > 0) out << "; ";
      out << pattern.exec_spec.rejected_candidates[i];
    }
    out << "\n";
  }
  return out.str();
}

}  // namespace

DataflowSession& DataflowSession::builder() {
  static DataflowSession session;
  return session;
}

DataFrame DataflowSession::read_csv(const std::string& path, const SourceOptions& options) {
  return read_csv(path, ',', options);
}

DataFrame DataflowSession::read_csv(const std::string& path, char delimiter,
                                    const SourceOptions& options) {
  const auto schema = read_csv_schema(path, delimiter);
  auto plan = std::make_shared<SourcePlan>("csv", path, delimiter, schema, options);
  return DataFrame(plan, nullptr, std::make_shared<Schema>(schema));
}

DataFrame DataflowSession::read_csv(const std::string& path, char delimiter) {
  SourceOptions options;
  return read_csv(path, delimiter, options);
}

FileSourceProbeResult DataflowSession::probe(const std::string& path) const {
  return probe_file_source(path);
}

DataFrame DataflowSession::read(const std::string& path, const SourceOptions& options) {
  const auto probe_result = probe(path);
  switch (probe_result.kind) {
    case FileSourceKind::Csv:
      return read_csv(path, probe_result.csv_delimiter, options);
    case FileSourceKind::Line:
      return read_line_file(path, probe_result.line_options, options);
    case FileSourceKind::Json:
      return read_json(path, probe_result.json_options, options);
  }
  throw std::runtime_error("unsupported probed file source");
}

DataFrame DataflowSession::read(const std::string& path) {
  SourceOptions options;
  return read(path, options);
}

DataFrame DataflowSession::read_line_file(const std::string& path, const LineFileOptions& options,
                                          const SourceOptions& source_options) {
  auto schema = infer_line_file_schema(options);
  auto plan = std::make_shared<SourcePlan>("line", path, std::move(schema), options, source_options);
  return DataFrame(plan, nullptr, std::make_shared<Schema>(plan->schema));
}

DataFrame DataflowSession::read_line_file(const std::string& path, const LineFileOptions& options) {
  SourceOptions source_options;
  return read_line_file(path, options, source_options);
}

DataFrame DataflowSession::read_json(const std::string& path, const JsonFileOptions& options,
                                     const SourceOptions& source_options) {
  auto schema = infer_json_file_schema(options);
  auto plan = std::make_shared<SourcePlan>("json", path, std::move(schema), options, source_options);
  return DataFrame(plan, nullptr, std::make_shared<Schema>(plan->schema));
}

DataFrame DataflowSession::read_json(const std::string& path, const JsonFileOptions& options) {
  SourceOptions source_options;
  return read_json(path, options, source_options);
}

StreamingDataFrame DataflowSession::readStream(std::shared_ptr<StreamSource> source) {
  return StreamingDataFrame(std::move(source));
}

StreamingDataFrame DataflowSession::readStreamCsvDir(const std::string& directory, char delimiter) {
  return readStream(std::make_shared<DirectoryCsvStreamSource>(directory, delimiter));
}

DataFrame DataflowSession::createDataFrame(Table table) {
  return DataFrame(std::move(table));
}

namespace {

Table submitAndWait(const DataflowJobHandle& handle) {
  while (true) {
    auto snap = handle.wait(std::chrono::milliseconds(100));
    if (snap.state == JobState::Succeeded) {
      materializeRows(&snap.result);
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

std::vector<std::string> collectCreateColumns(const sql::CreateTableStmt& stmt,
                                             bool require_columns = true) {
  if (stmt.columns.empty()) {
    if (require_columns) {
      throw SQLSemanticError("CREATE TABLE requires at least one column");
    }
    return {};
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
  const auto fields = collectCreateColumns(stmt);
  catalog.createTable(stmt.table, fields, stmt.kind);
  return dmlResult("create table done", 0);
}

std::string toLower(std::string value) {
  for (char& c : value) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  return value;
}

std::string trim(std::string value) {
  const auto start = value.find_first_not_of(" \t\r\n");
  if (start == std::string::npos) {
    return "";
  }
  const auto end = value.find_last_not_of(" \t\r\n");
  return value.substr(start, end - start + 1);
}

std::optional<std::string> findCreateOption(const sql::CreateTableStmt& stmt,
                                            const std::string& key) {
  const auto expected = toLower(key);
  for (const auto& [name, value] : stmt.options) {
    if (toLower(name) == expected) {
      return value;
    }
  }
  return std::nullopt;
}

std::string requireCreateOption(const sql::CreateTableStmt& stmt, const std::string& key) {
  const auto value = findCreateOption(stmt, key);
  if (!value.has_value() || value->empty()) {
    throw SQLSemanticError("CREATE TABLE missing option: " + key);
  }
  return *value;
}

char csvDelimiter(const sql::CreateTableStmt& stmt, const std::string& option_key = "delimiter") {
  const auto value = findCreateOption(stmt, option_key);
  if (!value.has_value()) {
    return ',';
  }
  if (value->empty()) {
    throw SQLSemanticError(option_key + " cannot be empty");
  }
  return (*value)[0];
}

std::vector<std::string> splitOptionList(const std::string& value, const std::string& key) {
  std::vector<std::string> items;
  std::size_t start = 0;
  while (start <= value.size()) {
    const auto comma = value.find(',', start);
    const auto piece =
        trim(value.substr(start, comma == std::string::npos ? std::string::npos : comma - start));
    if (!piece.empty()) {
      items.push_back(piece);
    }
    if (comma == std::string::npos) {
      break;
    }
    start = comma + 1;
  }
  if (items.empty()) {
    throw SQLSemanticError("CREATE TABLE option " + key + " cannot be empty");
  }
  return items;
}

FileSourceKind parseCreateFileSourceType(const std::string& raw_value) {
  const auto value = toLower(trim(raw_value));
  if (value == "csv") {
    return FileSourceKind::Csv;
  }
  if (value == "line") {
    return FileSourceKind::Line;
  }
  if (value == "json") {
    return FileSourceKind::Json;
  }
  throw SQLSemanticError("CREATE TABLE type/provider must be csv, line, or json");
}

std::optional<FileSourceKind> resolveCreateFileSourceType(const sql::CreateTableStmt& stmt) {
  const auto provider = stmt.provider.empty()
                            ? std::optional<std::string>{}
                            : std::optional<std::string>{stmt.provider};
  if (!provider.has_value()) {
    return std::nullopt;
  }
  return parseCreateFileSourceType(*provider);
}

void validateDeclaredSchema(const sql::CreateTableStmt& stmt, const Schema& actual_schema) {
  const auto declared = collectCreateColumns(stmt, false);
  if (declared.empty()) {
    return;
  }
  if (declared.size() != actual_schema.fields.size()) {
    throw SQLSemanticError("CREATE TABLE column count does not match source schema");
  }
  for (std::size_t i = 0; i < declared.size(); ++i) {
    if (declared[i] != actual_schema.fields[i]) {
      throw SQLSemanticError("CREATE TABLE column mismatch: expected " + declared[i] + ", got " +
                             actual_schema.fields[i]);
    }
  }
}

LineFileOptions buildLineFileOptions(const sql::CreateTableStmt& stmt,
                                     const std::optional<FileSourceProbeResult>& probe) {
  LineFileOptions options = probe.has_value() ? probe->line_options : LineFileOptions{};
  const auto mode = findCreateOption(stmt, "mode");
  if (mode.has_value()) {
    const auto normalized = toLower(trim(*mode));
    if (normalized == "split") {
      options.mode = LineParseMode::Split;
    } else if (normalized == "regex") {
      options.mode = LineParseMode::Regex;
    } else {
      throw SQLSemanticError("CREATE TABLE option mode must be split or regex");
    }
  }
  if (findCreateOption(stmt, "split_delimiter").has_value()) {
    options.split_delimiter = csvDelimiter(stmt, "split_delimiter");
  } else if (findCreateOption(stmt, "delimiter").has_value()) {
    options.split_delimiter = csvDelimiter(stmt, "delimiter");
  }
  if (const auto regex = findCreateOption(stmt, "regex_pattern"); regex.has_value()) {
    options.regex_pattern = *regex;
  } else if (const auto regex = findCreateOption(stmt, "regex"); regex.has_value()) {
    options.regex_pattern = *regex;
  }

  if (const auto mappings = findCreateOption(stmt, "mappings"); mappings.has_value()) {
    for (const auto& mapping : splitOptionList(*mappings, "mappings")) {
      const auto sep = mapping.find(':');
      if (sep == std::string::npos) {
        throw SQLSemanticError("CREATE TABLE option mappings must use column:index pairs");
      }
      const auto column = trim(mapping.substr(0, sep));
      const auto index_text = trim(mapping.substr(sep + 1));
      if (column.empty() || index_text.empty()) {
        throw SQLSemanticError("CREATE TABLE option mappings must use column:index pairs");
      }
      try {
        options.mappings.push_back(LineColumnMapping{
            column,
            static_cast<std::size_t>(std::stoull(index_text)),
        });
      } catch (...) {
        throw SQLSemanticError("CREATE TABLE option mappings contains invalid source index: " +
                               index_text);
      }
    }
  } else {
    std::vector<std::string> columns;
    if (const auto raw_columns = findCreateOption(stmt, "columns"); raw_columns.has_value()) {
      columns = splitOptionList(*raw_columns, "columns");
    } else {
      columns = collectCreateColumns(stmt, false);
    }
    if (!columns.empty()) {
      options.mappings.clear();
      options.mappings.reserve(columns.size());
      for (std::size_t i = 0; i < columns.size(); ++i) {
        options.mappings.push_back(LineColumnMapping{columns[i], i});
      }
    } else if (options.mappings.empty()) {
      throw SQLSemanticError(
          "CREATE TABLE line source requires columns, OPTIONS(columns: 'name1,name2,...'), "
          "or OPTIONS(mappings: 'name:0,...')");
    }
  }

  if (options.mode == LineParseMode::Regex && options.regex_pattern.empty()) {
    throw SQLSemanticError("CREATE TABLE regex line source requires OPTIONS(regex_pattern: '...')");
  }
  return options;
}

JsonFileOptions buildJsonFileOptions(const sql::CreateTableStmt& stmt,
                                     const std::optional<FileSourceProbeResult>& probe) {
  JsonFileOptions options = probe.has_value() ? probe->json_options : JsonFileOptions{};
  if (const auto format = findCreateOption(stmt, "format"); format.has_value()) {
    const auto normalized = toLower(trim(*format));
    if (normalized == "json_lines" || normalized == "jsonl" || normalized == "lines") {
      options.format = JsonFileFormat::JsonLines;
    } else if (normalized == "json_array" || normalized == "array") {
      options.format = JsonFileFormat::JsonArray;
    } else {
      throw SQLSemanticError("CREATE TABLE option format must be json_lines or json_array");
    }
  }
  if (const auto columns = findCreateOption(stmt, "columns"); columns.has_value()) {
    options.columns = splitOptionList(*columns, "columns");
  } else {
    const auto declared = collectCreateColumns(stmt, false);
    if (!declared.empty()) {
      options.columns = declared;
    }
  }
  if (options.columns.empty()) {
    throw SQLSemanticError(
        "CREATE TABLE json source requires columns or OPTIONS(columns: 'name1,name2,...')");
  }
  return options;
}

DataFrame executeCreateExternalTable(DataflowSession* session, ViewCatalog& catalog,
                                     const sql::CreateTableStmt& stmt, FileSourceKind kind,
                                     const std::optional<FileSourceProbeResult>& probe) {
  if (session == nullptr) {
    throw std::invalid_argument("executeCreateExternalTable session is null");
  }
  if (stmt.kind == sql::TableKind::Sink) {
    if (kind != FileSourceKind::Csv) {
      throwUnsupportedSqlV1("CREATE SINK TABLE only supports csv");
    }
    collectCreateColumns(stmt);
    const auto path = requireCreateOption(stmt, "path");
    const auto delimiter = findCreateOption(stmt, "delimiter").has_value()
                               ? csvDelimiter(stmt)
                               : (probe.has_value() ? probe->csv_delimiter : ',');
    session->registerStreamSink(stmt.table,
                                std::make_shared<FileAppendStreamSink>(path, delimiter));
    return dmlResult("create sink table done", 0);
  }

  if (stmt.kind == sql::TableKind::Source) {
    if (kind != FileSourceKind::Csv) {
      throwUnsupportedSqlV1("CREATE SOURCE TABLE only supports csv");
    }
    collectCreateColumns(stmt);
    const auto path = requireCreateOption(stmt, "path");
    const auto delimiter = findCreateOption(stmt, "delimiter").has_value()
                               ? csvDelimiter(stmt)
                               : (probe.has_value() ? probe->csv_delimiter : ',');
    session->createTempView(stmt.table, session->readStreamCsvDir(path, delimiter));
    return dmlResult("create source table done", 0);
  }

  const auto path = requireCreateOption(stmt, "path");
  DataFrame source;
  switch (kind) {
    case FileSourceKind::Csv:
      source = session->read_csv(
          path, findCreateOption(stmt, "delimiter").has_value()
                    ? csvDelimiter(stmt)
                    : (probe.has_value() ? probe->csv_delimiter : ','));
      break;
    case FileSourceKind::Line:
      source = session->read_line_file(path, buildLineFileOptions(stmt, probe));
      break;
    case FileSourceKind::Json:
      source = session->read_json(path, buildJsonFileOptions(stmt, probe));
      break;
  }
  validateDeclaredSchema(stmt, source.schema());
  session->createTempView(stmt.table, source);
  return dmlResult("create table done", 0);
}

std::size_t findNameIndex(const Schema& schema, const std::string& name) {
  for (std::size_t i = 0; i < schema.fields.size(); ++i) {
    if (schema.fields[i] == name) return i;
  }
  throw SQLSemanticError("column not found: " + name);
}

std::shared_ptr<ColumnarTable> makeInsertColumnarCache(const Schema& schema,
                                                       std::size_t row_count) {
  auto cache = std::make_shared<ColumnarTable>();
  cache->schema = schema;
  cache->columns.resize(schema.fields.size());
  cache->arrow_formats.resize(schema.fields.size());
  cache->row_count = row_count;
  if (row_count > 0) {
    cache->batch_row_counts.push_back(row_count);
  }
  for (auto& column : cache->columns) {
    column.values.reserve(row_count);
  }
  return cache;
}

void appendInsertRow(ColumnarTable* cache, const Row& row) {
  if (cache == nullptr) {
    throw std::invalid_argument("appendInsertRow cache is null");
  }
  if (row.size() != cache->columns.size()) {
    throw SQLSemanticError("INSERT row column count mismatch");
  }
  for (std::size_t column_index = 0; column_index < row.size(); ++column_index) {
    cache->columns[column_index].values.push_back(row[column_index]);
  }
}

Table normalizeInsertValues(const Table& target, const std::vector<std::string>& columns,
                          const std::vector<Row>& values) {
  Table out;
  out.schema = target.schema;
  auto cache = makeInsertColumnarCache(target.schema, values.size());
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
      appendInsertRow(cache.get(), outRow);
    }
    out.columnar_cache = std::move(cache);
    return out;
  }

  for (const auto& row : values) {
    if (row.size() != target.schema.fields.size()) {
      throw SQLSemanticError("INSERT VALUES column count mismatch");
    }
    appendInsertRow(cache.get(), row);
  }
  out.columnar_cache = std::move(cache);
  return out;
}

Table normalizeInsertSelect(const Table& target, const std::vector<std::string>& columns,
                          const Table& source) {
  Table out;
  out.schema = target.schema;
  if (columns.empty()) {
    if (source.schema.fields.size() != target.schema.fields.size()) {
      throw SQLSemanticError("INSERT SELECT column count mismatch");
    }
    std::vector<std::size_t> indices(source.schema.fields.size());
    for (std::size_t i = 0; i < indices.size(); ++i) {
      indices[i] = i;
    }
    return projectTable(source, indices, target.schema.fields, false);
  }

  std::unordered_set<std::string> targetCols;
  std::vector<std::size_t> targetIndices;
  targetIndices.reserve(columns.size());
  if (source.schema.fields.size() != columns.size()) {
    throw SQLSemanticError("INSERT SELECT column count mismatch");
  }
  for (const auto& col : columns) {
    if (!targetCols.insert(col).second) {
      throw SQLSemanticError("duplicate insert column: " + col);
    }
    targetIndices.push_back(findNameIndex(target.schema, col));
  }
  const auto row_count = source.rowCount();
  auto cache = makeInsertColumnarCache(target.schema, row_count);
  std::vector<int64_t> source_by_target(target.schema.fields.size(), -1);
  for (std::size_t i = 0; i < targetIndices.size(); ++i) {
    source_by_target[targetIndices[i]] = static_cast<int64_t>(i);
  }
  for (std::size_t target_index = 0; target_index < target.schema.fields.size(); ++target_index) {
    const auto source_index = source_by_target[target_index];
    if (source_index < 0) {
      cache->columns[target_index].values.resize(row_count);
      continue;
    }
    cache->columns[target_index] =
        materializeValueColumn(source, static_cast<std::size_t>(source_index));
  }
  out.columnar_cache = std::move(cache);
  return out;
}

DataFrame executeInsert(ViewCatalog& catalog, const sql::InsertStmt& stmt) {
  if (catalog.isSourceTable(stmt.table)) {
    throwTableKindViolation("INSERT INTO is not allowed on SOURCE TABLE: " + stmt.table);
  }
  DataFrame& view = catalog.getViewMutable(stmt.table);
  const Table& target = view.materializedTable();
  Table toInsert;
  if (stmt.select_from) {
    sql::SqlPlanner planner;
    toInsert = planner.plan(stmt.query, catalog).materializedTable();
    toInsert = normalizeInsertSelect(target, stmt.columns, toInsert);
  } else {
    toInsert = normalizeInsertValues(target, stmt.columns, stmt.values);
  }
  auto rows = toInsert.rowCount();
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
    case sql::AggregateFunctionKind::StdDev:
      return "stddev";
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
    case sql::AggregateFunctionKind::StdDev:
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

void validateStreamingSource(const sql::SqlQuery& query, const ViewCatalog& catalog,
                           const std::unordered_map<std::string, StreamingDataFrame>& stream_views,
                           const std::unordered_map<std::string, std::shared_ptr<StreamSink>>&
                               stream_sinks) {
  if (!query.has_from) {
    throw SQLSemanticError("stream SQL requires FROM");
  }
  const auto& source = query.from.name;
  if (stream_views.find(source) != stream_views.end()) {
    return;
  }
  if (stream_sinks.find(source) != stream_sinks.end()) {
    throw SQLTableKindError("table-kind constraint violation: stream SQL source table cannot be SINK TABLE: " +
                           source);
  }
  if (catalog.hasView(source)) {
    throw SQLTableKindError(
        "table-kind constraint violation: stream SQL source must be created with CREATE SOURCE TABLE: " +
        source);
  }
  throw CatalogNotFoundError("stream view not found: " + source);
}

void validateStreamingSink(const std::string& sink_name, const ViewCatalog& catalog,
                          const std::unordered_map<std::string, StreamingDataFrame>& stream_views,
                          const std::unordered_map<std::string, std::shared_ptr<StreamSink>>&
                              stream_sinks) {
  const auto sink_it = stream_sinks.find(sink_name);
  if (sink_it != stream_sinks.end()) {
    return;
  }
  if (catalog.hasView(sink_name)) {
    throw SQLTableKindError(
        "table-kind constraint violation: stream SQL sink must be created with CREATE SINK TABLE: " +
        sink_name);
  }
  if (stream_views.find(sink_name) != stream_views.end()) {
    throw SQLTableKindError(
        "table-kind constraint violation: stream SQL sink cannot be SOURCE TABLE: " + sink_name);
  }
  throw CatalogNotFoundError("stream sink not found: " + sink_name);
}

StreamingDataFrame buildAggregateStream(const StreamingDataFrame& seed, const sql::SqlQuery& query,
                                       const sql::FromItem& from) {
  if (query.select_items.empty()) {
    throw SQLSemanticError("stream SQL aggregate query requires projections");
  }
  std::vector<std::string> group_keys;
  group_keys.reserve(query.group_by.size());
  for (const auto& key : query.group_by) {
    if (key.is_string_function) {
      throw SQLUnsupportedError("stream SQL does not support scalar expressions in GROUP BY");
    }
    group_keys.push_back(resolveColumnName(key.column, from));
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

  if (query.having) {
    if (query.having->kind != sql::PredicateExprKind::Comparison) {
      throwUnsupportedSqlV1("stream SQL HAVING does not support AND/OR");
    }
    const auto& predicate = query.having->predicate;
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
  if (query.where) {
    if (query.where->kind != sql::PredicateExprKind::Comparison) {
      throwUnsupportedSqlV1("stream SQL WHERE does not support AND/OR");
    }
    const auto& predicate = query.where->predicate;
    if (predicate.lhs_is_aggregate) {
      throw SQLSemanticError("WHERE does not support aggregate expressions");
    }
    current = current.filter(resolveColumnName(predicate.lhs, query.from),
                             filterOp(predicate.op), predicate.rhs);
  }

  if (isAggregateQuery(query)) {
    if (query.having && query.group_by.empty()) {
      throw SQLSemanticError("GROUP BY required for HAVING");
    }
    current = buildAggregateStream(current, query, query.from);
  } else {
    if (!query.group_by.empty()) {
      throw SQLSemanticError("GROUP BY used without aggregate");
    }
    if (query.having) {
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

enum class StreamSqlStatementMode { SelectOnly, SelectOrInsert, InsertOnly };

struct PreparedStreamSqlStatement {
  sql::SqlQuery query;
  std::string sink_name;
  std::shared_ptr<StreamSink> sink;
};

PreparedStreamSqlStatement prepareStreamSqlStatement(
    const std::string& sql_text, StreamSqlStatementMode mode,
    const ViewCatalog& catalog,
    const std::unordered_map<std::string, StreamingDataFrame>& stream_views,
    const std::unordered_map<std::string, std::shared_ptr<StreamSink>>& stream_sinks) {
  const auto statement = sql::SqlParser::parse(sql_text);

  if (statement.kind == sql::SqlStatementKind::Select) {
    if (mode == StreamSqlStatementMode::InsertOnly) {
      throwUnsupportedSqlV1("startStreamSql only supports INSERT INTO ... SELECT");
    }
    validateStreamingSource(statement.query, catalog, stream_views, stream_sinks);
    return PreparedStreamSqlStatement{statement.query, "", nullptr};
  }

  if (statement.kind == sql::SqlStatementKind::InsertSelect) {
    if (mode == StreamSqlStatementMode::SelectOnly) {
      throwUnsupportedSqlV1("streamSql only supports SELECT");
    }
    if (!statement.insert.columns.empty()) {
      throwUnsupportedSqlV1("stream SQL does not support INSERT column list");
    }
    validateStreamingSource(statement.insert.query, catalog, stream_views, stream_sinks);
    validateStreamingSink(statement.insert.table, catalog, stream_views, stream_sinks);
    auto sink_it = stream_sinks.find(statement.insert.table);
    return PreparedStreamSqlStatement{statement.insert.query, statement.insert.table,
                                      sink_it->second};
  }

  if (mode == StreamSqlStatementMode::InsertOnly) {
    throwUnsupportedSqlV1("startStreamSql only supports INSERT INTO ... SELECT");
  }
  if (mode == StreamSqlStatementMode::SelectOrInsert) {
    throwUnsupportedSqlV1("explainStreamSql only supports SELECT or INSERT INTO ... SELECT");
  }
  throwUnsupportedSqlV1("streamSql only supports SELECT");
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
    const auto path_option = findCreateOption(statement.create, "path");
    const auto explicit_kind = resolveCreateFileSourceType(statement.create);
    if (path_option.has_value()) {
      std::optional<FileSourceProbeResult> probe_result;
      if (statement.create.kind == sql::TableKind::Regular || !explicit_kind.has_value()) {
        probe_result = probe(*path_option);
      }
      const auto file_source_kind =
          explicit_kind.value_or(probe_result.has_value() ? probe_result->kind : FileSourceKind::Csv);
      result = executeCreateExternalTable(this, catalog_, statement.create, file_source_kind,
                                          probe_result);
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
    throwUnsupportedSqlV1("unsupported statement kind");
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

std::string DataflowSession::explainSql(const std::string& sql_text) {
  const auto statement = sql::SqlParser::parse(sql_text);
  if (statement.kind != sql::SqlStatementKind::Select) {
    throwUnsupportedSqlV1("explainSql only supports SELECT");
  }

  sql::SqlPlanner planner;
  const auto logical = planner.buildLogicalPlan(statement.query, catalog_);
  const auto physical = planner.buildPhysicalPlan(logical);

  DataFrame current = physical.seed;
  std::ostringstream strategy;
  bool saw_aggregate = false;
  for (const auto& step : physical.steps) {
    switch (step.logical_kind) {
      case sql::LogicalStepKind::Scan:
        break;
      case sql::LogicalStepKind::Filter:
        current = current.filterByIndex(step.logical.filter_column, step.logical.filter_op,
                                       step.logical.filter_value);
        break;
      case sql::LogicalStepKind::PredicateFilter:
        current = planner.materializeFromPhysical(
            sql::PhysicalPlan{current, std::vector<sql::PhysicalPlanStep>{step}});
        break;
      case sql::LogicalStepKind::KeywordSearch:
        current = current.keywordSearch(step.logical.keyword_columns,
                                        step.logical.keyword_query_text,
                                        step.logical.keyword_top_k);
        break;
      case sql::LogicalStepKind::HybridSearch:
        current = current.hybridSearch(step.logical.hybrid_vector_column,
                                       step.logical.hybrid_query_vector,
                                       step.logical.hybrid_options);
        break;
      case sql::LogicalStepKind::Union:
        current = current.unionWith(step.logical.union_right, step.logical.union_distinct);
        break;
      case sql::LogicalStepKind::Join:
        current =
            current.join(step.logical.join_right, step.logical.join_left_column,
                         step.logical.join_right_column, step.logical.join_kind);
        break;
      case sql::LogicalStepKind::Aggregate: {
        const Table aggregate_input = current.toTable();
        strategy << formatBatchAggregateStrategyExplain(aggregate_input, step.logical.group_keys,
                                                        step.logical.aggregates);
        current = current.aggregate(step.logical.group_keys, step.logical.aggregates);
        saw_aggregate = true;
        break;
      }
      case sql::LogicalStepKind::Having: {
        const auto idx = current.schema().indexOf(step.logical.having_column);
        current = current.filterByIndex(idx, step.logical.filter_op, step.logical.filter_value);
        break;
      }
      case sql::LogicalStepKind::Project:
        current = current.selectByIndices(step.logical.project_indices,
                                          step.logical.project_aliases);
        break;
      case sql::LogicalStepKind::OrderBy: {
        std::vector<std::string> columns;
        columns.reserve(step.logical.order_indices.size());
        for (auto index : step.logical.order_indices) {
          columns.push_back(current.schema().fields[index]);
        }
        current = current.orderBy(columns, step.logical.order_ascending);
        break;
      }
      case sql::LogicalStepKind::WithColumn:
        current =
            current.withColumn(step.logical.with_column_name, step.logical.with_function,
                              step.logical.with_args);
        break;
      case sql::LogicalStepKind::Limit:
        if (step.logical.limit_set) {
          current = current.limit(step.logical.limit);
        }
        break;
    }
  }
  if (!saw_aggregate) {
    strategy << "selected_impl=not-applicable\n";
    strategy << "reason=query does not contain GROUP BY aggregate planning\n";
  }

  std::ostringstream out;
  out << "logical\n";
  out << planner.explainLogicalPlan(logical);
  out << "physical\n";
  out << planner.explainPhysicalPlan(physical);
  out << "strategy\n";
  out << strategy.str();
  return out.str();
}

DataFrame DataflowSession::vectorQuery(const std::string& table, const std::string& vector_column,
                                       const std::vector<float>& query_vector, size_t top_k,
                                       VectorDistanceMetric metric) {
  return catalog_.getView(table).vectorQuery(vector_column, query_vector, top_k, metric);
}

DataFrame DataflowSession::hybridSearch(const std::string& table,
                                        const std::string& vector_column,
                                        const std::vector<float>& query_vector,
                                        const HybridSearchOptions& options) {
  return catalog_.getView(table).hybridSearch(vector_column, query_vector, options);
}

std::string DataflowSession::explainVectorQuery(const std::string& table,
                                                const std::string& vector_column,
                                                const std::vector<float>& query_vector,
                                                size_t top_k, VectorDistanceMetric metric) {
  return catalog_.getView(table).explainVectorQuery(vector_column, query_vector, top_k, metric);
}

std::string DataflowSession::explainHybridSearch(const std::string& table,
                                                 const std::string& vector_column,
                                                 const std::vector<float>& query_vector,
                                                 const HybridSearchOptions& options) {
  return catalog_.getView(table).explainHybridSearch(vector_column, query_vector, options);
}

StreamingDataFrame DataflowSession::streamSql(const std::string& sql) {
  const auto prepared =
      prepareStreamSqlStatement(sql, StreamSqlStatementMode::SelectOnly, catalog_, stream_views_,
                               stream_sinks_);
  sql::SqlPlanner planner;
  return planner.planStream(prepared.query, stream_views_);
}

std::string DataflowSession::explainStreamSql(const std::string& sql,
                                              const StreamingQueryOptions& options) {
  const auto prepared =
      prepareStreamSqlStatement(sql, StreamSqlStatementMode::SelectOrInsert, catalog_, stream_views_,
                               stream_sinks_);

  sql::SqlPlanner planner;
  const auto logical = planner.buildStreamLogicalPlan(prepared.query, prepared.sink_name);
  const auto physical = planner.buildStreamPhysicalPlan(logical);
  const auto stream = planner.materializeStreamFromPhysical(physical, stream_views_);
  validateStreamingOrderRequirements(stream);
  const auto strategy = describeStreamingStrategy(stream, prepared.sink, options);

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
  const auto prepared =
      prepareStreamSqlStatement(sql, StreamSqlStatementMode::InsertOnly, catalog_, stream_views_,
                               stream_sinks_);

  sql::SqlPlanner planner;
  auto query = planner.planStream(prepared.query, stream_views_)
                   .writeStream(prepared.sink, options);
  query.start();
  return query;
}

}  // namespace dataflow
