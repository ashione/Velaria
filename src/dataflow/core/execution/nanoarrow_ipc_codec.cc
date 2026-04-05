#include "src/dataflow/core/execution/nanoarrow_ipc_codec.h"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "src/dataflow/core/execution/columnar_batch.h"
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
  const auto values = viewValueColumn(table, column).values();
  for (const auto& value : values) {
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

bool bitmapHasValue(const uint8_t* bitmap, int64_t index) {
  if (bitmap == nullptr) {
    return true;
  }
  return ((bitmap[index >> 3] >> (index & 7)) & 0x01u) != 0;
}

std::shared_ptr<void> copyBitmapBuffer(const uint8_t* source, int64_t offset, int64_t length) {
  if (source == nullptr) {
    return {};
  }
  const std::size_t byte_count =
      length <= 0 ? 0 : static_cast<std::size_t>((length + 7) / 8);
  auto* out = static_cast<uint8_t*>(std::calloc(byte_count == 0 ? 1 : byte_count, 1));
  for (int64_t row = 0; row < length; ++row) {
    if (bitmapHasValue(source, offset + row)) {
      out[row >> 3] |= static_cast<uint8_t>(1U << (row & 7));
    }
  }
  return std::shared_ptr<void>(out, std::free);
}

template <typename T>
std::shared_ptr<void> copyValueBuffer(const T* source, int64_t offset, int64_t length) {
  auto* out = static_cast<T*>(std::calloc(length <= 0 ? 1 : static_cast<std::size_t>(length), sizeof(T)));
  if (source != nullptr && length > 0) {
    std::memcpy(out, source + offset, sizeof(T) * static_cast<std::size_t>(length));
  }
  return std::shared_ptr<void>(out, std::free);
}

std::shared_ptr<void> copyBooleanValueBuffer(const uint8_t* source, int64_t offset, int64_t length) {
  const std::size_t byte_count =
      length <= 0 ? 0 : static_cast<std::size_t>((length + 7) / 8);
  auto* out = static_cast<uint8_t*>(std::calloc(byte_count == 0 ? 1 : byte_count, 1));
  if (source != nullptr) {
    for (int64_t row = 0; row < length; ++row) {
      if (bitmapHasValue(source, offset + row)) {
        out[row >> 3] |= static_cast<uint8_t>(1U << (row & 7));
      }
    }
  }
  return std::shared_ptr<void>(out, std::free);
}

std::pair<std::shared_ptr<void>, std::shared_ptr<void>> copyUtf8Buffers(const int32_t* offsets,
                                                                        const char* data,
                                                                        int64_t offset,
                                                                        int64_t length) {
  auto* out_offsets = static_cast<int32_t*>(
      std::calloc(length < 0 ? 1 : static_cast<std::size_t>(length + 1), sizeof(int32_t)));
  const int32_t base = offsets[offset];
  const int32_t end = offsets[offset + length];
  for (int64_t row = 0; row <= length; ++row) {
    out_offsets[row] = offsets[offset + row] - base;
  }
  const auto byte_count = static_cast<std::size_t>(end - base);
  auto* out_data = static_cast<char*>(std::malloc(byte_count == 0 ? 1 : byte_count));
  if (byte_count > 0) {
    std::memcpy(out_data, data + base, byte_count);
  }
  return {
      std::shared_ptr<void>(out_offsets, std::free),
      std::shared_ptr<void>(out_data, std::free),
  };
}

std::pair<std::shared_ptr<void>, std::shared_ptr<void>> copyLargeUtf8Buffers(const int64_t* offsets,
                                                                             const char* data,
                                                                             int64_t offset,
                                                                             int64_t length) {
  auto* out_offsets = static_cast<int64_t*>(
      std::calloc(length < 0 ? 1 : static_cast<std::size_t>(length + 1), sizeof(int64_t)));
  const int64_t base = offsets[offset];
  const int64_t end = offsets[offset + length];
  for (int64_t row = 0; row <= length; ++row) {
    out_offsets[row] = offsets[offset + row] - base;
  }
  const auto byte_count = static_cast<std::size_t>(end - base);
  auto* out_data = static_cast<char*>(std::malloc(byte_count == 0 ? 1 : byte_count));
  if (byte_count > 0) {
    std::memcpy(out_data, data + base, byte_count);
  }
  return {
      std::shared_ptr<void>(out_offsets, std::free),
      std::shared_ptr<void>(out_data, std::free),
  };
}

std::shared_ptr<void> copyFixedSizeListFloat32Buffer(const ArrowArray* child_array,
                                                     int32_t fixed_size,
                                                     int64_t row_offset,
                                                     int64_t row_length) {
  const auto* source = static_cast<const float*>(child_array->buffers[1]);
  const int64_t item_offset =
      child_array->offset + row_offset * static_cast<int64_t>(fixed_size);
  const int64_t item_length = row_length * static_cast<int64_t>(fixed_size);
  return copyValueBuffer<float>(source, item_offset, item_length);
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

Table table_from_nanoarrow_batch(const ArrowSchema* schema, const ArrowArray* array,
                                 bool materialize_rows) {
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
  table.columnar_cache = std::make_shared<ColumnarTable>();
  table.columnar_cache->schema = table.schema;
  table.columnar_cache->columns.resize(static_cast<std::size_t>(schema->n_children));
  table.columnar_cache->arrow_formats.reserve(static_cast<std::size_t>(schema->n_children));
  table.columnar_cache->batch_row_counts.push_back(static_cast<std::size_t>(array->length));

  std::vector<ArrowSchemaView> child_schema_views(static_cast<std::size_t>(schema->n_children));
  for (int64_t i = 0; i < schema->n_children; ++i) {
    throw_nanoarrow_status(
        ArrowSchemaViewInit(&child_schema_views[static_cast<std::size_t>(i)],
                            schema->children[i], &error),
        &error, "init child nanoarrow schema view");
  }

  for (int64_t col = 0; col < schema->n_children; ++col) {
    const auto* child_schema = schema->children[col];
    const auto* child_array = array->children[col];
    const auto& child_schema_view = child_schema_views[static_cast<std::size_t>(col)];
    if (child_array == nullptr) {
      throw std::runtime_error("nanoarrow materialization missing child array");
    }

    auto backing = std::make_shared<ArrowColumnBacking>();
    backing->format = child_schema != nullptr && child_schema->format != nullptr
                          ? child_schema->format
                          : "u";
    backing->length = static_cast<std::size_t>(array->length);
    backing->null_count = child_array->null_count;
    backing->null_bitmap = copyBitmapBuffer(
        static_cast<const uint8_t*>(child_array->buffers[0]), child_array->offset, child_array->length);

    switch (child_schema_view.storage_type) {
      case NANOARROW_TYPE_BOOL:
        backing->value_buffer = copyBooleanValueBuffer(
            static_cast<const uint8_t*>(child_array->buffers[1]), child_array->offset,
            child_array->length);
        if (backing->format.empty()) backing->format = "b";
        break;
      case NANOARROW_TYPE_INT32:
        backing->value_buffer = copyValueBuffer<int32_t>(
            static_cast<const int32_t*>(child_array->buffers[1]), child_array->offset,
            child_array->length);
        if (backing->format.empty()) backing->format = "i";
        break;
      case NANOARROW_TYPE_UINT32:
        backing->value_buffer = copyValueBuffer<uint32_t>(
            static_cast<const uint32_t*>(child_array->buffers[1]), child_array->offset,
            child_array->length);
        if (backing->format.empty()) backing->format = "I";
        break;
      case NANOARROW_TYPE_INT64:
        backing->value_buffer = copyValueBuffer<int64_t>(
            static_cast<const int64_t*>(child_array->buffers[1]), child_array->offset,
            child_array->length);
        if (backing->format.empty()) backing->format = "l";
        break;
      case NANOARROW_TYPE_UINT64:
        backing->value_buffer = copyValueBuffer<uint64_t>(
            static_cast<const uint64_t*>(child_array->buffers[1]), child_array->offset,
            child_array->length);
        if (backing->format.empty()) backing->format = "L";
        break;
      case NANOARROW_TYPE_FLOAT:
        backing->value_buffer = copyValueBuffer<float>(
            static_cast<const float*>(child_array->buffers[1]), child_array->offset,
            child_array->length);
        if (backing->format.empty()) backing->format = "f";
        break;
      case NANOARROW_TYPE_DOUBLE:
        backing->value_buffer = copyValueBuffer<double>(
            static_cast<const double*>(child_array->buffers[1]), child_array->offset,
            child_array->length);
        if (backing->format.empty()) backing->format = "g";
        break;
      case NANOARROW_TYPE_STRING: {
        auto buffers = copyUtf8Buffers(
            static_cast<const int32_t*>(child_array->buffers[1]),
            static_cast<const char*>(child_array->buffers[2]), child_array->offset,
            child_array->length);
        backing->value_buffer = std::move(buffers.first);
        backing->extra_buffer = std::move(buffers.second);
        if (backing->format.empty()) backing->format = "u";
        break;
      }
      case NANOARROW_TYPE_LARGE_STRING: {
        auto buffers = copyLargeUtf8Buffers(
            static_cast<const int64_t*>(child_array->buffers[1]),
            static_cast<const char*>(child_array->buffers[2]), child_array->offset,
            child_array->length);
        backing->value_buffer = std::move(buffers.first);
        backing->extra_buffer = std::move(buffers.second);
        if (backing->format.empty()) backing->format = "U";
        break;
      }
      case NANOARROW_TYPE_FIXED_SIZE_LIST: {
        backing->fixed_list_size = child_schema_view.fixed_size;
        backing->child_format = "f";
        backing->child_length =
            static_cast<std::size_t>(child_array->length) * static_cast<std::size_t>(backing->fixed_list_size);
        backing->child_value_buffer =
            copyFixedSizeListFloat32Buffer(child_array->children[0], backing->fixed_list_size,
                                           child_array->offset, child_array->length);
        if (backing->format.empty()) {
          backing->format = "+w:" + std::to_string(backing->fixed_list_size);
        }
        break;
      }
      default:
        throw std::runtime_error("unsupported nanoarrow column type in Arrow-backed materialization");
    }

    table.columnar_cache->arrow_formats.push_back(backing->format);
    table.columnar_cache->columns[static_cast<std::size_t>(col)].arrow_backing = std::move(backing);
  }

  if (materialize_rows) {
    materializeRows(&table);
  }

  return table;
}

std::vector<ColumnLayout> infer_column_layouts(const Table& table) {
  std::vector<ColumnLayout> layouts;
  layouts.reserve(table.schema.fields.size());
  for (std::size_t i = 0; i < table.schema.fields.size(); ++i) {
    layouts.push_back(infer_column_layout(table, i));
  }
  return layouts;
}

void build_arrow_array_from_table(const Table& table, const std::vector<ColumnLayout>& layouts,
                                  ArrowSchema* schema, ArrowArray* array) {
  ArrowError error;
  std::memset(&error, 0, sizeof(error));

  build_schema(table, layouts, schema);
  throw_nanoarrow_status(ArrowArrayInitFromSchema(array, schema, &error), &error,
                         "init nanoarrow array from schema");
  throw_nanoarrow_status(ArrowArrayStartAppending(array), nullptr,
                         "start appending nanoarrow array");

  std::vector<std::size_t> all_indices;
  all_indices.reserve(table.schema.fields.size());
  for (std::size_t i = 0; i < table.schema.fields.size(); ++i) {
    all_indices.push_back(i);
  }
  const auto columns = viewValueColumns(table, all_indices);
  for (std::size_t row_index = 0; row_index < table.rowCount(); ++row_index) {
    for (std::size_t i = 0; i < table.schema.fields.size(); ++i) {
      append_row_value(array->children[i], layouts[i], columns[i].values()[row_index]);
    }
    throw_nanoarrow_status(ArrowArrayFinishElement(array), nullptr, "finish struct row");
  }
  throw_nanoarrow_status(ArrowArrayFinishBuildingDefault(array, &error), &error,
                         "finish nanoarrow array");
}

void write_nanoarrow_stream(const ArrowSchema* schema, const ArrowArray* array,
                            ArrowIpcOutputStream* output_stream) {
  ArrowError error;
  std::memset(&error, 0, sizeof(error));

  ArrowIpcWriterGuard writer;
  throw_nanoarrow_status(ArrowIpcWriterInit(&writer.value, output_stream), nullptr,
                         "init nanoarrow writer");

  ArrowArrayViewGuard array_view;
  throw_nanoarrow_status(ArrowArrayViewInitFromSchema(&array_view.value, schema, &error), &error,
                         "init nanoarrow array view");
  throw_nanoarrow_status(ArrowArrayViewSetArray(&array_view.value, array, &error), &error,
                         "set nanoarrow array view");

  throw_nanoarrow_status(ArrowIpcWriterWriteSchema(&writer.value, schema, &error), &error,
                         "write nanoarrow schema");
  throw_nanoarrow_status(
      ArrowIpcWriterWriteArrayView(&writer.value, &array_view.value, &error), &error,
      "write nanoarrow record batch");
  throw_nanoarrow_status(ArrowIpcWriterWriteArrayView(&writer.value, nullptr, &error), &error,
                         "write nanoarrow end-of-stream");
}

Table read_nanoarrow_stream(ArrowIpcInputStream* input_stream, bool materialize_rows) {
  ArrowIpcArrayStreamReaderOptions options;
  std::memset(&options, 0, sizeof(options));
  options.field_index = -1;
  options.use_shared_buffers = 0;

  ArrowArrayStreamGuard stream;
  throw_nanoarrow_status(
      ArrowIpcArrayStreamReaderInit(&stream.value, input_stream, &options), nullptr,
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

    Table batch = table_from_nanoarrow_batch(&schema.value, &array.value, materialize_rows);
    if (table.schema.fields.empty()) {
      table = std::move(batch);
      continue;
    }
    if (table.schema.fields != batch.schema.fields) {
      throw std::runtime_error("nanoarrow materialization schema mismatch across record batches");
    }
    if (table.columnar_cache && batch.columnar_cache) {
      for (auto& column : table.columnar_cache->columns) {
        materializeValueBuffer(&column);
        column.arrow_backing.reset();
      }
      for (auto& column : batch.columnar_cache->columns) {
        materializeValueBuffer(&column);
        column.arrow_backing.reset();
      }
      for (std::size_t column = 0; column < table.columnar_cache->columns.size(); ++column) {
        auto& out_values = table.columnar_cache->columns[column].values;
        auto& batch_values = batch.columnar_cache->columns[column].values;
        out_values.insert(out_values.end(), std::make_move_iterator(batch_values.begin()),
                          std::make_move_iterator(batch_values.end()));
      }
      table.columnar_cache->row_count += batch.rowCount();
      table.columnar_cache->batch_row_counts.push_back(batch.rowCount());
      table.columnar_cache->arrow_formats.assign(table.columnar_cache->columns.size(), "");
    }
    if (materialize_rows) {
      materializeRows(&table);
      materializeRows(&batch);
      table.rows.insert(table.rows.end(), std::make_move_iterator(batch.rows.begin()),
                        std::make_move_iterator(batch.rows.end()));
    } else {
      table.rows.clear();
    }
  }

  return table;
}

}  // namespace

std::vector<uint8_t> serialize_nanoarrow_ipc_table(const Table& table) {
  return serialize_nanoarrow_ipc_table(table, nullptr);
}

std::vector<uint8_t> serialize_nanoarrow_ipc_table(const Table& table, std::size_t* payload_size) {
  ArrowSchemaGuard schema;
  ArrowArrayGuard array;
  build_arrow_array_from_table(table, infer_column_layouts(table), &schema.value, &array.value);

  ArrowBuffer output;
  ArrowBufferInit(&output);
  ArrowIpcOutputStreamGuard output_stream;
  throw_nanoarrow_status(ArrowIpcOutputStreamInitBuffer(&output_stream.value, &output), nullptr,
                         "init nanoarrow output buffer");
  write_nanoarrow_stream(&schema.value, &array.value, &output_stream.value);

  std::vector<uint8_t> payload;
  payload.assign(output.data, output.data + output.size_bytes);
  if (payload_size != nullptr) {
    *payload_size = payload.size();
  }
  ArrowBufferReset(&output);
  return payload;
}

Table deserialize_nanoarrow_ipc_table(const std::vector<uint8_t>& payload, bool materialize_rows) {
  return deserialize_nanoarrow_ipc_table(payload.data(), payload.size(), materialize_rows);
}

Table deserialize_nanoarrow_ipc_table(const uint8_t* payload, std::size_t size,
                                      bool materialize_rows) {
  ArrowBuffer input;
  ArrowBufferInit(&input);
  throw_nanoarrow_status(
      ArrowBufferAppend(&input, payload, static_cast<int64_t>(size)),
      nullptr, "append nanoarrow input payload");

  ArrowIpcInputStreamGuard input_stream;
  throw_nanoarrow_status(ArrowIpcInputStreamInitBuffer(&input_stream.value, &input), nullptr,
                         "init nanoarrow input buffer");
  return read_nanoarrow_stream(&input_stream.value, materialize_rows);
}

void save_nanoarrow_ipc_table(const Table& table, const std::string& path) {
  ArrowSchemaGuard schema;
  ArrowArrayGuard array;
  build_arrow_array_from_table(table, infer_column_layouts(table), &schema.value, &array.value);

  FilePtr file(open_file_or_throw(path, "wb"));
  ArrowIpcOutputStreamGuard output_stream;
  throw_nanoarrow_status(ArrowIpcOutputStreamInitFile(&output_stream.value, file.get(), 1),
                         nullptr, "init nanoarrow output stream");
  file.release();
  write_nanoarrow_stream(&schema.value, &array.value, &output_stream.value);
}

Table load_nanoarrow_ipc_table(const std::string& path, bool materialize_rows) {
  FilePtr file(open_file_or_throw(path, "rb"));
  ArrowIpcInputStreamGuard input_stream;
  throw_nanoarrow_status(ArrowIpcInputStreamInitFile(&input_stream.value, file.get(), 1),
                         nullptr, "init nanoarrow input stream");
  file.release();
  return read_nanoarrow_stream(&input_stream.value, materialize_rows);
}

}  // namespace dataflow
