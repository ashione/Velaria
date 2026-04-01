#include "src/dataflow/core/execution/nanoarrow_ipc_codec.h"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "nanoarrow/nanoarrow.h"
#include "nanoarrow/nanoarrow_ipc.h"

namespace dataflow {

namespace {

enum class ColumnKind {
  Int64,
  Double,
  String,
  FixedVector,
};

struct ColumnLayout {
  ColumnKind kind = ColumnKind::String;
  int32_t fixed_vector_size = 0;
};

struct ArrowSchemaGuard {
  ArrowSchemaGuard() { std::memset(&value, 0, sizeof(value)); }
  ~ArrowSchemaGuard() {
    if (value.release != nullptr) {
      value.release(&value);
    }
  }
  ArrowSchema value;
};

struct ArrowArrayGuard {
  ArrowArrayGuard() { std::memset(&value, 0, sizeof(value)); }
  ~ArrowArrayGuard() {
    if (value.release != nullptr) {
      value.release(&value);
    }
  }
  ArrowArray value;
};

struct ArrowArrayViewGuard {
  ArrowArrayViewGuard() {
    ArrowArrayViewInitFromType(&value, NANOARROW_TYPE_UNINITIALIZED);
  }
  ~ArrowArrayViewGuard() { ArrowArrayViewReset(&value); }
  ArrowArrayView value;
};

struct ArrowArrayStreamGuard {
  ArrowArrayStreamGuard() { std::memset(&value, 0, sizeof(value)); }
  ~ArrowArrayStreamGuard() {
    if (value.release != nullptr) {
      value.release(&value);
    }
  }
  ArrowArrayStream value;
};

struct ArrowIpcInputStreamGuard {
  ArrowIpcInputStreamGuard() { std::memset(&value, 0, sizeof(value)); }
  ~ArrowIpcInputStreamGuard() {
    if (value.release != nullptr) {
      value.release(&value);
    }
  }
  ArrowIpcInputStream value;
};

struct ArrowIpcOutputStreamGuard {
  ArrowIpcOutputStreamGuard() { std::memset(&value, 0, sizeof(value)); }
  ~ArrowIpcOutputStreamGuard() {
    if (value.release != nullptr) {
      value.release(&value);
    }
  }
  ArrowIpcOutputStream value;
};

struct ArrowIpcWriterGuard {
  ArrowIpcWriterGuard() { std::memset(&value, 0, sizeof(value)); }
  ~ArrowIpcWriterGuard() { ArrowIpcWriterReset(&value); }
  ArrowIpcWriter value;
};

void throw_nanoarrow_status(ArrowErrorCode code, ArrowError* error,
                            const std::string& context) {
  if (code == NANOARROW_OK) {
    return;
  }
  std::string message = context;
  if (error != nullptr && ArrowErrorMessage(error)[0] != '\0') {
    message += ": ";
    message += ArrowErrorMessage(error);
  } else {
    message += ": nanoarrow error code ";
    message += std::to_string(static_cast<int>(code));
  }
  throw std::runtime_error(message);
}

void throw_stream_status(ArrowErrorCode code, ArrowArrayStream* stream,
                         const std::string& context) {
  if (code == NANOARROW_OK) {
    return;
  }
  std::string message = context;
  const char* last_error =
      (stream != nullptr && stream->get_last_error != nullptr) ? stream->get_last_error(stream)
                                                               : nullptr;
  if (last_error != nullptr && last_error[0] != '\0') {
    message += ": ";
    message += last_error;
  } else {
    message += ": nanoarrow stream error code ";
    message += std::to_string(static_cast<int>(code));
  }
  throw std::runtime_error(message);
}

struct FileCloser {
  void operator()(std::FILE* file) const {
    if (file != nullptr) {
      std::fclose(file);
    }
  }
};

using FilePtr = std::unique_ptr<std::FILE, FileCloser>;

std::FILE* open_file_or_throw(const std::string& path, const char* mode) {
  std::FILE* file = std::fopen(path.c_str(), mode);
  if (file == nullptr) {
    throw std::runtime_error("cannot open nanoarrow ipc file: " + path);
  }
  return file;
}

ColumnLayout infer_column_layout(const Table& table, std::size_t column) {
  bool seen_non_null = false;
  ColumnLayout layout;

  for (const auto& row : table.rows) {
    if (column >= row.size()) {
      throw std::runtime_error("row width is smaller than schema width");
    }

    const auto& value = row[column];
    if (value.isNull()) {
      continue;
    }

    ColumnLayout candidate;
    switch (value.type()) {
      case DataType::Nil:
        continue;
      case DataType::Int64:
        candidate.kind = ColumnKind::Int64;
        break;
      case DataType::Double:
        candidate.kind = ColumnKind::Double;
        break;
      case DataType::String:
        candidate.kind = ColumnKind::String;
        break;
      case DataType::FixedVector:
        candidate.kind = ColumnKind::FixedVector;
        candidate.fixed_vector_size = static_cast<int32_t>(value.asFixedVector().size());
        break;
    }

    if (!seen_non_null) {
      layout = candidate;
      seen_non_null = true;
      continue;
    }

    if (layout.kind == candidate.kind) {
      if (layout.kind == ColumnKind::FixedVector &&
          layout.fixed_vector_size != candidate.fixed_vector_size) {
        throw std::runtime_error("nanoarrow materialization requires stable vector dimensions");
      }
      continue;
    }

    if ((layout.kind == ColumnKind::Int64 && candidate.kind == ColumnKind::Double) ||
        (layout.kind == ColumnKind::Double && candidate.kind == ColumnKind::Int64)) {
      layout.kind = ColumnKind::Double;
      continue;
    }

    throw std::runtime_error("nanoarrow materialization requires stable column types");
  }

  if (!seen_non_null) {
    layout.kind = ColumnKind::String;
  }
  return layout;
}

ArrowType arrow_type_for_layout(const ColumnLayout& layout) {
  switch (layout.kind) {
    case ColumnKind::Int64:
      return NANOARROW_TYPE_INT64;
    case ColumnKind::Double:
      return NANOARROW_TYPE_DOUBLE;
    case ColumnKind::String:
      return NANOARROW_TYPE_STRING;
    case ColumnKind::FixedVector:
      return NANOARROW_TYPE_FIXED_SIZE_LIST;
  }
  return NANOARROW_TYPE_STRING;
}

ArrowStringView to_string_view(const std::string& value) {
  ArrowStringView out;
  out.data = value.data();
  out.size_bytes = static_cast<int64_t>(value.size());
  return out;
}

void build_schema(const Table& table, const std::vector<ColumnLayout>& layouts,
                  ArrowSchema* schema) {
  ArrowSchemaInit(schema);
  throw_nanoarrow_status(
      ArrowSchemaSetTypeStruct(schema, static_cast<int64_t>(table.schema.fields.size())),
      nullptr, "set struct schema type");

  for (std::size_t i = 0; i < table.schema.fields.size(); ++i) {
    ArrowSchema* child = schema->children[i];
    throw_nanoarrow_status(ArrowSchemaSetName(child, table.schema.fields[i].c_str()), nullptr,
                           "set child schema name");
    if (layouts[i].kind == ColumnKind::FixedVector) {
      throw_nanoarrow_status(
          ArrowSchemaSetTypeFixedSize(child, NANOARROW_TYPE_FIXED_SIZE_LIST,
                                      layouts[i].fixed_vector_size),
          nullptr, "set fixed-size-list schema type");
      throw_nanoarrow_status(ArrowSchemaSetName(child->children[0], "item"), nullptr,
                             "set fixed-size-list child schema name");
      throw_nanoarrow_status(ArrowSchemaSetType(child->children[0], NANOARROW_TYPE_FLOAT),
                             nullptr, "set fixed-size-list child schema type");
    } else {
      throw_nanoarrow_status(ArrowSchemaSetType(child, arrow_type_for_layout(layouts[i])),
                             nullptr, "set child schema type");
    }
  }
}

void append_row_value(ArrowArray* child, const ColumnLayout& layout, const Value& value) {
  if (value.isNull()) {
    throw_nanoarrow_status(ArrowArrayAppendNull(child, 1), nullptr,
                           "append null nanoarrow value");
    return;
  }

  switch (layout.kind) {
    case ColumnKind::Int64:
      throw_nanoarrow_status(ArrowArrayAppendInt(child, value.asInt64()), nullptr,
                             "append int64 nanoarrow value");
      return;
    case ColumnKind::Double:
      throw_nanoarrow_status(ArrowArrayAppendDouble(child, value.asDouble()), nullptr,
                             "append double nanoarrow value");
      return;
    case ColumnKind::String:
      throw_nanoarrow_status(ArrowArrayAppendString(child, to_string_view(value.asString())),
                             nullptr, "append string nanoarrow value");
      return;
    case ColumnKind::FixedVector: {
      const auto& vec = value.asFixedVector();
      if (static_cast<int32_t>(vec.size()) != layout.fixed_vector_size) {
        throw std::runtime_error("nanoarrow materialization vector dimension mismatch");
      }
      ArrowArray* values = child->children[0];
      for (float item : vec) {
        throw_nanoarrow_status(ArrowArrayAppendDouble(values, static_cast<double>(item)),
                               nullptr, "append fixed vector item");
      }
      throw_nanoarrow_status(ArrowArrayFinishElement(child), nullptr,
                             "finish fixed vector element");
      return;
    }
  }
}

Table table_from_nanoarrow_batch(const ArrowSchema* schema, const ArrowArray* array) {
  ArrowError error;
  std::memset(&error, 0, sizeof(error));

  ArrowArrayViewGuard root_view;
  throw_nanoarrow_status(ArrowArrayViewInitFromSchema(&root_view.value, schema, &error), &error,
                         "init nanoarrow array view");
  throw_nanoarrow_status(ArrowArrayViewSetArray(&root_view.value, array, &error), &error,
                         "bind nanoarrow array view");

  Table table;
  table.schema.fields.reserve(static_cast<std::size_t>(schema->n_children));
  for (int64_t i = 0; i < schema->n_children; ++i) {
    table.schema.fields.emplace_back(schema->children[i]->name);
    table.schema.index[table.schema.fields.back()] = static_cast<std::size_t>(i);
  }

  std::vector<ArrowSchemaView> child_schema_views(static_cast<std::size_t>(schema->n_children));
  for (int64_t i = 0; i < schema->n_children; ++i) {
    throw_nanoarrow_status(
        ArrowSchemaViewInit(&child_schema_views[static_cast<std::size_t>(i)],
                            schema->children[i], &error),
        &error, "init child nanoarrow schema view");
  }

  table.rows.reserve(static_cast<std::size_t>(array->length));
  for (int64_t row = 0; row < array->length; ++row) {
    Row out_row;
    out_row.reserve(static_cast<std::size_t>(schema->n_children));
    for (int64_t col = 0; col < schema->n_children; ++col) {
      const auto* child_view = root_view.value.children[col];
      if (ArrowArrayViewIsNull(child_view, row)) {
        out_row.emplace_back();
        continue;
      }

      const auto& child_schema_view = child_schema_views[static_cast<std::size_t>(col)];
      switch (child_schema_view.storage_type) {
        case NANOARROW_TYPE_INT64:
          out_row.emplace_back(ArrowArrayViewGetIntUnsafe(child_view, row));
          break;
        case NANOARROW_TYPE_DOUBLE:
          out_row.emplace_back(ArrowArrayViewGetDoubleUnsafe(child_view, row));
          break;
        case NANOARROW_TYPE_STRING: {
          const auto string_view = ArrowArrayViewGetStringUnsafe(child_view, row);
          out_row.emplace_back(
              std::string(string_view.data, static_cast<std::size_t>(string_view.size_bytes)));
          break;
        }
        case NANOARROW_TYPE_FIXED_SIZE_LIST: {
          std::vector<float> vec;
          vec.reserve(static_cast<std::size_t>(child_schema_view.fixed_size));
          const auto* values_view = child_view->children[0];
          const int64_t base = row * static_cast<int64_t>(child_schema_view.fixed_size);
          for (int32_t i = 0; i < child_schema_view.fixed_size; ++i) {
            vec.push_back(
                static_cast<float>(ArrowArrayViewGetDoubleUnsafe(values_view, base + i)));
          }
          out_row.emplace_back(std::move(vec));
          break;
        }
        default:
          throw std::runtime_error("unsupported nanoarrow column type in materialization");
      }
    }
    table.rows.push_back(std::move(out_row));
  }

  return table;
}

}  // namespace

void save_nanoarrow_ipc_table(const Table& table, const std::string& path) {
  ArrowError error;
  std::memset(&error, 0, sizeof(error));

  std::vector<ColumnLayout> layouts;
  layouts.reserve(table.schema.fields.size());
  for (std::size_t i = 0; i < table.schema.fields.size(); ++i) {
    layouts.push_back(infer_column_layout(table, i));
  }

  ArrowSchemaGuard schema;
  build_schema(table, layouts, &schema.value);

  ArrowArrayGuard array;
  throw_nanoarrow_status(ArrowArrayInitFromSchema(&array.value, &schema.value, &error), &error,
                         "init nanoarrow array from schema");
  throw_nanoarrow_status(ArrowArrayStartAppending(&array.value), nullptr,
                         "start appending nanoarrow array");

  for (const auto& row : table.rows) {
    if (row.size() < table.schema.fields.size()) {
      throw std::runtime_error("row width is smaller than schema width");
    }
    for (std::size_t i = 0; i < table.schema.fields.size(); ++i) {
      append_row_value(array.value.children[i], layouts[i], row[i]);
    }
    throw_nanoarrow_status(ArrowArrayFinishElement(&array.value), nullptr,
                           "finish struct row");
  }
  throw_nanoarrow_status(ArrowArrayFinishBuildingDefault(&array.value, &error), &error,
                         "finish nanoarrow array");

  FilePtr file(open_file_or_throw(path, "wb"));
  ArrowIpcOutputStreamGuard output_stream;
  throw_nanoarrow_status(ArrowIpcOutputStreamInitFile(&output_stream.value, file.get(), 1),
                         nullptr, "init nanoarrow output stream");
  file.release();

  ArrowIpcWriterGuard writer;
  throw_nanoarrow_status(ArrowIpcWriterInit(&writer.value, &output_stream.value), nullptr,
                         "init nanoarrow writer");

  ArrowArrayViewGuard array_view;
  throw_nanoarrow_status(ArrowArrayViewInitFromSchema(&array_view.value, &schema.value, &error),
                         &error, "init nanoarrow array view");
  throw_nanoarrow_status(ArrowArrayViewSetArray(&array_view.value, &array.value, &error), &error,
                         "set nanoarrow array view");

  throw_nanoarrow_status(ArrowIpcWriterWriteSchema(&writer.value, &schema.value, &error), &error,
                         "write nanoarrow schema");
  throw_nanoarrow_status(
      ArrowIpcWriterWriteArrayView(&writer.value, &array_view.value, &error), &error,
      "write nanoarrow record batch");
  throw_nanoarrow_status(ArrowIpcWriterWriteArrayView(&writer.value, nullptr, &error), &error,
                         "write nanoarrow end-of-stream");
}

Table load_nanoarrow_ipc_table(const std::string& path) {
  FilePtr file(open_file_or_throw(path, "rb"));
  ArrowIpcInputStreamGuard input_stream;
  throw_nanoarrow_status(ArrowIpcInputStreamInitFile(&input_stream.value, file.get(), 1),
                         nullptr, "init nanoarrow input stream");
  file.release();

  ArrowIpcArrayStreamReaderOptions options;
  std::memset(&options, 0, sizeof(options));
  options.field_index = -1;
  options.use_shared_buffers = 0;

  ArrowArrayStreamGuard stream;
  throw_nanoarrow_status(
      ArrowIpcArrayStreamReaderInit(&stream.value, &input_stream.value, &options), nullptr,
      "init nanoarrow ipc reader");

  ArrowSchemaGuard schema;
  throw_stream_status(stream.value.get_schema(&stream.value, &schema.value), &stream.value,
                      "read nanoarrow schema");

  Table table;
  while (true) {
    ArrowArrayGuard array;
    throw_stream_status(stream.value.get_next(&stream.value, &array.value), &stream.value,
                        "read nanoarrow record batch");
    if (array.value.release == nullptr) {
      break;
    }

    Table batch = table_from_nanoarrow_batch(&schema.value, &array.value);
    if (table.schema.fields.empty()) {
      table.schema = batch.schema;
    }
    table.rows.insert(table.rows.end(), std::make_move_iterator(batch.rows.begin()),
                      std::make_move_iterator(batch.rows.end()));
  }

  return table;
}

}  // namespace dataflow
