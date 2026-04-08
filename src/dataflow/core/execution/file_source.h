#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "src/dataflow/core/execution/table.h"

namespace dataflow {

struct SourcePushdownSpec;

enum class LineParseMode {
  Split = 0,
  Regex = 1,
};

struct LineColumnMapping {
  std::string column;
  std::size_t source_index = 0;
};

struct LineFileOptions {
  LineParseMode mode = LineParseMode::Split;
  char split_delimiter = ',';
  std::string regex_pattern;
  std::vector<LineColumnMapping> mappings;
  bool skip_empty_lines = true;
};

enum class JsonFileFormat {
  JsonArray = 0,
  JsonLines = 1,
};

enum class FileSourceKind {
  Csv = 0,
  Line = 1,
  Json = 2,
};

struct JsonFileOptions {
  JsonFileFormat format = JsonFileFormat::JsonLines;
  std::vector<std::string> columns;
};

struct FileSourceProbeResult {
  FileSourceKind kind = FileSourceKind::Csv;
  std::string path;
  Schema schema;
  char csv_delimiter = ',';
  LineFileOptions line_options;
  JsonFileOptions json_options;
  std::string suggested_table_name;
};

struct FileSourceConnectorSpec {
  FileSourceKind kind = FileSourceKind::Csv;
  std::string path;
  char csv_delimiter = ',';
  LineFileOptions line_options;
  JsonFileOptions json_options;
};

FileSourceProbeResult probe_file_source(const std::string& path);
Schema infer_line_file_schema(const LineFileOptions& options);
Schema infer_json_file_schema(const JsonFileOptions& options);
Table load_line_file(const std::string& path, const LineFileOptions& options);
Table load_json_file(const std::string& path, const JsonFileOptions& options);
bool execute_file_source_pushdown(const FileSourceConnectorSpec& spec, const Schema& schema,
                                  const SourcePushdownSpec& pushdown, bool materialize_rows,
                                  Table* out);
std::string file_source_format_name(const FileSourceConnectorSpec& spec);
std::string file_source_options_signature(const FileSourceConnectorSpec& spec);

}  // namespace dataflow
