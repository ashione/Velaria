#include <Python.h>

#include <cstring>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "src/dataflow/core/contract/api/dataframe.h"
#include "src/dataflow/core/contract/api/session.h"
#include "src/dataflow/core/execution/columnar_batch.h"
#include "src/dataflow/core/execution/table.h"
#include "src/dataflow/core/execution/value.h"
#include "src/dataflow/core/execution/stream/stream.h"

namespace {

namespace df = dataflow;

struct ArrowSchema {
  const char* format;
  const char* name;
  const char* metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema** children;
  struct ArrowSchema* dictionary;
  void (*release)(struct ArrowSchema*);
  void* private_data;
};

struct ArrowArray {
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void** buffers;
  struct ArrowArray** children;
  struct ArrowArray* dictionary;
  void (*release)(struct ArrowArray*);
  void* private_data;
};

class AllowThreads {
 public:
  AllowThreads() : state_(PyEval_SaveThread()) {}
  ~AllowThreads() { PyEval_RestoreThread(state_); }

 private:
  PyThreadState* state_;
};

typedef struct {
  PyObject_HEAD df::DataflowSession* session;
} PyVelariaSession;

typedef struct {
  PyObject_HEAD df::DataFrame* df_ptr;
} PyVelariaDataFrame;

typedef struct {
  PyObject_HEAD df::StreamingDataFrame* sdf_ptr;
} PyVelariaStreamingDataFrame;

typedef struct {
  PyObject_HEAD df::StreamingQuery* query_ptr;
} PyVelariaStreamingQuery;

static PyTypeObject PyVelariaSessionType = {PyVarObject_HEAD_INIT(nullptr, 0)};
static PyTypeObject PyVelariaDataFrameType = {PyVarObject_HEAD_INIT(nullptr, 0)};
static PyTypeObject PyVelariaStreamingDataFrameType = {PyVarObject_HEAD_INIT(nullptr, 0)};
static PyTypeObject PyVelariaStreamingQueryType = {PyVarObject_HEAD_INIT(nullptr, 0)};

struct OwnedArrowSchema {
  std::string format;
  std::string name;
  std::vector<std::unique_ptr<OwnedArrowSchema>> children_owned;
  std::vector<ArrowSchema*> child_ptrs;
  ArrowSchema schema{};
};

struct OwnedArrowArray {
  std::vector<std::shared_ptr<void>> buffer_holders;
  std::vector<const void*> buffer_ptrs;
  std::vector<std::unique_ptr<OwnedArrowArray>> children_owned;
  std::vector<ArrowArray*> child_ptrs;
  ArrowArray array{};
};

void releaseArrowSchema(ArrowSchema* schema) {
  if (schema == nullptr || schema->release == nullptr) {
    return;
  }
  auto* owned = reinterpret_cast<OwnedArrowSchema*>(schema->private_data);
  schema->release = nullptr;
  delete owned;
}

void releaseArrowArray(ArrowArray* array) {
  if (array == nullptr || array->release == nullptr) {
    return;
  }
  auto* owned = reinterpret_cast<OwnedArrowArray*>(array->private_data);
  array->release = nullptr;
  delete owned;
}

void releaseArrowSchemaCapsule(PyObject* capsule) {
  auto* schema =
      reinterpret_cast<ArrowSchema*>(PyCapsule_GetPointer(capsule, "arrow_schema"));
  if (schema != nullptr && schema->release != nullptr) {
    schema->release(schema);
  }
  std::free(schema);
}

void releaseArrowArrayCapsule(PyObject* capsule) {
  auto* array = reinterpret_cast<ArrowArray*>(PyCapsule_GetPointer(capsule, "arrow_array"));
  if (array != nullptr && array->release != nullptr) {
    array->release(array);
  }
  std::free(array);
}

template <typename Fn>
PyObject* withExceptionTranslation(Fn&& fn) {
  try {
    return fn();
  } catch (const std::exception& ex) {
    PyErr_SetString(PyExc_RuntimeError, ex.what());
    return nullptr;
  } catch (...) {
    PyErr_SetString(PyExc_RuntimeError, "unknown velaria error");
    return nullptr;
  }
}

void setDictItem(PyObject* dict, const char* key, PyObject* value) {
  PyDict_SetItemString(dict, key, value);
  Py_DECREF(value);
}

char parseDelimiter(const char* text) {
  if (text == nullptr || text[0] == '\0' || text[1] != '\0') {
    throw std::runtime_error("delimiter must be a single character");
  }
  return text[0];
}

df::MaterializationDataFormat parseMaterializationDataFormat(const char* text) {
  if (text == nullptr || text[0] == '\0') {
    return df::default_source_materialization_data_format();
  }
  const std::string value(text);
  if (value == "binary" || value == "binary_row_batch") {
    return df::MaterializationDataFormat::BinaryRowBatch;
  }
  if (value == "nanoarrow" || value == "nanoarrow_ipc") {
    return df::MaterializationDataFormat::NanoArrowIpc;
  }
  throw std::runtime_error(
      "materialization_format must be one of: binary_row_batch, nanoarrow_ipc");
}

std::vector<std::string> parseStringList(PyObject* obj, const char* arg_name) {
  if (!PySequence_Check(obj)) {
    throw std::runtime_error(std::string(arg_name) + " must be a sequence of strings");
  }
  PyObject* seq = PySequence_Fast(obj, arg_name);
  if (seq == nullptr) {
    throw std::runtime_error(std::string("failed to read ") + arg_name);
  }
  std::vector<std::string> values;
  const Py_ssize_t count = PySequence_Fast_GET_SIZE(seq);
  values.reserve(static_cast<size_t>(count));
  PyObject** items = PySequence_Fast_ITEMS(seq);
  for (Py_ssize_t i = 0; i < count; ++i) {
    if (!PyUnicode_Check(items[i])) {
      Py_DECREF(seq);
      throw std::runtime_error(std::string(arg_name) + " must contain only strings");
    }
    values.emplace_back(PyUnicode_AsUTF8(items[i]));
  }
  Py_DECREF(seq);
  return values;
}

std::vector<float> parseFloatVector(PyObject* obj, const char* arg_name) {
  if (!PySequence_Check(obj)) {
    throw std::runtime_error(std::string(arg_name) + " must be a sequence of floats");
  }
  PyObject* seq = PySequence_Fast(obj, arg_name);
  if (seq == nullptr) {
    throw std::runtime_error(std::string("failed to read ") + arg_name);
  }
  std::vector<float> values;
  const Py_ssize_t count = PySequence_Fast_GET_SIZE(seq);
  values.reserve(static_cast<size_t>(count));
  PyObject** items = PySequence_Fast_ITEMS(seq);
  for (Py_ssize_t i = 0; i < count; ++i) {
    if (!PyFloat_Check(items[i]) && !PyLong_Check(items[i])) {
      Py_DECREF(seq);
      throw std::runtime_error(std::string(arg_name) + " must contain only numbers");
    }
    values.push_back(static_cast<float>(PyFloat_AsDouble(items[i])));
  }
  Py_DECREF(seq);
  return values;
}

df::Value valueFromPy(PyObject* obj) {
  if (obj == Py_None) {
    return df::Value();
  }
  if (PyBool_Check(obj)) {
    return df::Value(static_cast<int64_t>(PyObject_IsTrue(obj)));
  }
  if (PyLong_Check(obj)) {
    return df::Value(static_cast<int64_t>(PyLong_AsLongLong(obj)));
  }
  if (PyFloat_Check(obj)) {
    return df::Value(PyFloat_AsDouble(obj));
  }
  if (PyUnicode_Check(obj)) {
    return df::Value(std::string(PyUnicode_AsUTF8(obj)));
  }
  if (PyList_Check(obj) || PyTuple_Check(obj)) {
    return df::Value(parseFloatVector(obj, "value"));
  }
  throw std::runtime_error("value must be None, int, float, bool, or string");
}

enum class FastArrowColumnKind {
  Unsupported,
  Bool,
  Int32,
  UInt32,
  Int64,
  UInt64,
  Float32,
  Float64,
  Utf8,
  LargeUtf8,
  FixedSizeListFloat32,
};

struct FastArrowColumnSpec {
  FastArrowColumnKind kind = FastArrowColumnKind::Unsupported;
  int32_t fixed_list_size = 0;
};

bool bitmapHasValue(const uint8_t* bitmap, int64_t index) {
  if (bitmap == nullptr) {
    return true;
  }
  return ((bitmap[index >> 3] >> (index & 7)) & 0x01u) != 0;
}

FastArrowColumnSpec fastArrowColumnSpec(const ArrowSchema* schema) {
  FastArrowColumnSpec spec;
  if (schema == nullptr || schema->format == nullptr) {
    return spec;
  }
  const std::string format(schema->format);
  if (format == "b") spec.kind = FastArrowColumnKind::Bool;
  if (format == "i") spec.kind = FastArrowColumnKind::Int32;
  if (format == "I") spec.kind = FastArrowColumnKind::UInt32;
  if (format == "l") spec.kind = FastArrowColumnKind::Int64;
  if (format == "L") spec.kind = FastArrowColumnKind::UInt64;
  if (format == "f") spec.kind = FastArrowColumnKind::Float32;
  if (format == "g") spec.kind = FastArrowColumnKind::Float64;
  if (format == "u") spec.kind = FastArrowColumnKind::Utf8;
  if (format == "U") spec.kind = FastArrowColumnKind::LargeUtf8;
  if (spec.kind != FastArrowColumnKind::Unsupported) return spec;

  // Arrow C data interface fixed-size list format: +w:<list_size>
  if (format.rfind("+w:", 0) == 0 && schema->n_children == 1 && schema->children != nullptr &&
      schema->children[0] != nullptr && std::string(schema->children[0]->format) == "f") {
    spec.kind = FastArrowColumnKind::FixedSizeListFloat32;
    spec.fixed_list_size = static_cast<int32_t>(std::stoi(format.substr(3)));
    return spec;
  }
  return spec;
}

df::Value valueFromFastArrowColumn(const FastArrowColumnSpec& spec, const ArrowArray* array,
                                   int64_t row_index) {
  const int64_t offset_row = row_index + array->offset;
  const auto* validity = reinterpret_cast<const uint8_t*>(array->n_buffers > 0 ? array->buffers[0] : nullptr);
  if (array->null_count != 0 && !bitmapHasValue(validity, offset_row)) {
    return df::Value();
  }

  switch (spec.kind) {
    case FastArrowColumnKind::Bool: {
      const auto* bits = reinterpret_cast<const uint8_t*>(array->buffers[1]);
      return df::Value(static_cast<int64_t>(((bits[offset_row >> 3] >> (offset_row & 7)) & 0x01u) != 0));
    }
    case FastArrowColumnKind::Int32:
      return df::Value(static_cast<int64_t>(reinterpret_cast<const int32_t*>(array->buffers[1])[offset_row]));
    case FastArrowColumnKind::UInt32:
      return df::Value(static_cast<int64_t>(reinterpret_cast<const uint32_t*>(array->buffers[1])[offset_row]));
    case FastArrowColumnKind::Int64:
      return df::Value(static_cast<int64_t>(reinterpret_cast<const int64_t*>(array->buffers[1])[offset_row]));
    case FastArrowColumnKind::UInt64:
      return df::Value(static_cast<int64_t>(reinterpret_cast<const uint64_t*>(array->buffers[1])[offset_row]));
    case FastArrowColumnKind::Float32:
      return df::Value(static_cast<double>(reinterpret_cast<const float*>(array->buffers[1])[offset_row]));
    case FastArrowColumnKind::Float64:
      return df::Value(reinterpret_cast<const double*>(array->buffers[1])[offset_row]);
    case FastArrowColumnKind::Utf8: {
      const auto* offsets = reinterpret_cast<const int32_t*>(array->buffers[1]);
      const auto* data = reinterpret_cast<const char*>(array->buffers[2]);
      const auto begin = offsets[offset_row];
      const auto end = offsets[offset_row + 1];
      return df::Value(std::string(data + begin, data + end));
    }
    case FastArrowColumnKind::LargeUtf8: {
      const auto* offsets = reinterpret_cast<const int64_t*>(array->buffers[1]);
      const auto* data = reinterpret_cast<const char*>(array->buffers[2]);
      const auto begin = offsets[offset_row];
      const auto end = offsets[offset_row + 1];
      return df::Value(std::string(data + begin, data + end));
    }
    case FastArrowColumnKind::FixedSizeListFloat32: {
      if (array->n_children != 1 || array->children == nullptr || array->children[0] == nullptr ||
          spec.fixed_list_size <= 0) {
        throw std::runtime_error("invalid Arrow fixed-size-list<float32> column");
      }
      const ArrowArray* child = array->children[0];
      const int64_t base_index = (offset_row * static_cast<int64_t>(spec.fixed_list_size)) + child->offset;
      const auto* values = reinterpret_cast<const float*>(child->buffers[1]);
      if (values == nullptr) {
        throw std::runtime_error("missing child value buffer for fixed-size-list<float32>");
      }
      std::vector<float> out(static_cast<size_t>(spec.fixed_list_size));
      for (int32_t i = 0; i < spec.fixed_list_size; ++i) {
        out[static_cast<size_t>(i)] = values[base_index + i];
      }
      return df::Value(std::move(out));
    }
    case FastArrowColumnKind::Unsupported:
      break;
  }
  throw std::runtime_error("unsupported Arrow fast-path column");
}

std::vector<df::Value> materializeFastArrowColumn(const FastArrowColumnSpec& spec,
                                                  const ArrowArray* array) {
  if (array == nullptr) {
    throw std::runtime_error("missing Arrow child array");
  }
  std::vector<df::Value> out(static_cast<size_t>(array->length));
  for (int64_t row = 0; row < array->length; ++row) {
    out[static_cast<size_t>(row)] = valueFromFastArrowColumn(spec, array, row);
  }
  return out;
}

std::vector<df::Value> materializePyListColumn(PyObject* pylist, const char* column_name) {
  if (pylist == nullptr || !PyList_Check(pylist)) {
    throw std::runtime_error(std::string("failed to decode pyarrow column: ") + column_name);
  }
  const auto row_count = PyList_GET_SIZE(pylist);
  std::vector<df::Value> out;
  out.reserve(static_cast<size_t>(row_count));
  for (Py_ssize_t row = 0; row < row_count; ++row) {
    out.push_back(valueFromPy(PyList_GET_ITEM(pylist, row)));
  }
  return out;
}

bool tryTableFromArrowCapsules(PyObject* obj, df::Table* table) {
  if (table == nullptr || !PyObject_HasAttrString(obj, "__arrow_c_array__")) {
    return false;
  }

  PyObject* pair = PyObject_CallMethod(obj, "__arrow_c_array__", nullptr);
  if (pair == nullptr) {
    PyErr_Clear();
    return false;
  }
  if (!PyTuple_Check(pair) || PyTuple_GET_SIZE(pair) != 2) {
    Py_DECREF(pair);
    return false;
  }

  auto* schema =
      reinterpret_cast<ArrowSchema*>(PyCapsule_GetPointer(PyTuple_GET_ITEM(pair, 0), "arrow_schema"));
  auto* array =
      reinterpret_cast<ArrowArray*>(PyCapsule_GetPointer(PyTuple_GET_ITEM(pair, 1), "arrow_array"));
  if (schema == nullptr || array == nullptr || schema->n_children != array->n_children) {
    PyErr_Clear();
    Py_DECREF(pair);
    return false;
  }

  std::vector<std::string> names;
  names.reserve(static_cast<size_t>(schema->n_children));
  std::vector<FastArrowColumnSpec> specs;
  specs.reserve(static_cast<size_t>(schema->n_children));
  for (int64_t i = 0; i < schema->n_children; ++i) {
    const auto* child_schema = schema->children[i];
    const auto spec = fastArrowColumnSpec(child_schema);
    if (spec.kind == FastArrowColumnKind::Unsupported) {
      Py_DECREF(pair);
      return false;
    }
    names.emplace_back(child_schema != nullptr && child_schema->name != nullptr
                           ? child_schema->name
                           : ("col_" + std::to_string(static_cast<long long>(i))));
    specs.push_back(spec);
  }

  df::Table out(df::Schema(std::move(names)), {});
  out.columnar_cache = std::make_shared<df::ColumnarTable>();
  out.columnar_cache->schema = out.schema;
  out.columnar_cache->columns.reserve(static_cast<size_t>(schema->n_children));
  out.columnar_cache->arrow_formats.reserve(static_cast<size_t>(schema->n_children));
  out.columnar_cache->batch_row_counts.push_back(static_cast<std::size_t>(array->length));
  for (int64_t column = 0; column < array->n_children; ++column) {
    const auto* child_array = array->children[column];
    if (child_array == nullptr) {
      Py_DECREF(pair);
      return false;
    }
    out.columnar_cache->arrow_formats.emplace_back(schema->children[column]->format != nullptr
                                                       ? schema->children[column]->format
                                                       : "");
    df::appendColumn(&out, materializeFastArrowColumn(specs[static_cast<size_t>(column)], child_array));
  }

  Py_DECREF(pair);
  *table = std::move(out);
  return true;
}

df::Table tableFromArrowSlow(PyObject* obj) {
  PyObject* pyarrow = PyImport_ImportModule("pyarrow");
  if (pyarrow == nullptr) {
    throw std::runtime_error("pyarrow is required for create_dataframe_from_arrow()");
  }
  PyObject* record_batch_type = PyObject_GetAttrString(pyarrow, "RecordBatch");
  PyObject* table_type = PyObject_GetAttrString(pyarrow, "Table");
  PyObject* table_fn = PyObject_GetAttrString(pyarrow, "table");
  if (record_batch_type == nullptr || table_type == nullptr || table_fn == nullptr) {
    Py_XDECREF(record_batch_type);
    Py_XDECREF(table_type);
    Py_XDECREF(table_fn);
    Py_DECREF(pyarrow);
    throw std::runtime_error("failed to resolve pyarrow table helpers");
  }

  PyObject* table_obj = nullptr;
  if (PyObject_IsInstance(obj, record_batch_type) == 1) {
    PyObject* from_batches = PyObject_GetAttrString(table_type, "from_batches");
    if (from_batches != nullptr) {
      PyObject* batches = PyList_New(1);
      Py_INCREF(obj);
      PyList_SET_ITEM(batches, 0, obj);
      table_obj = PyObject_CallFunctionObjArgs(from_batches, batches, nullptr);
      Py_DECREF(batches);
      Py_DECREF(from_batches);
    }
  } else {
    table_obj = PyObject_CallFunctionObjArgs(table_fn, obj, nullptr);
  }
  Py_DECREF(record_batch_type);
  Py_DECREF(table_type);
  Py_DECREF(table_fn);
  Py_DECREF(pyarrow);
  if (table_obj == nullptr) {
    throw std::runtime_error("failed to normalize input as pyarrow.Table");
  }

  PyObject* schema_obj = PyObject_GetAttrString(table_obj, "schema");
  PyObject* names_obj = schema_obj == nullptr ? nullptr : PyObject_GetAttrString(schema_obj, "names");
  if (schema_obj == nullptr || names_obj == nullptr || !PySequence_Check(names_obj)) {
    Py_XDECREF(names_obj);
    Py_XDECREF(schema_obj);
    Py_DECREF(table_obj);
    throw std::runtime_error("failed to read pyarrow schema names");
  }
  auto names = parseStringList(names_obj, "schema.names");
  Py_DECREF(names_obj);
  Py_DECREF(schema_obj);

  PyObject* num_rows_obj = PyObject_GetAttrString(table_obj, "num_rows");
  if (num_rows_obj == nullptr) {
    Py_DECREF(table_obj);
    throw std::runtime_error("failed to read pyarrow num_rows");
  }
  const Py_ssize_t row_count = static_cast<Py_ssize_t>(PyLong_AsSsize_t(num_rows_obj));
  Py_DECREF(num_rows_obj);

  const auto column_count = names.size();
  df::Table out(df::Schema(std::move(names)), {});
  out.columnar_cache = std::make_shared<df::ColumnarTable>();
  out.columnar_cache->schema = out.schema;
  out.columnar_cache->columns.reserve(column_count);
  out.columnar_cache->arrow_formats.resize(column_count);
  out.columnar_cache->batch_row_counts.push_back(static_cast<std::size_t>(row_count));
  for (size_t column = 0; column < column_count; ++column) {
    PyObject* column_obj =
        PyObject_CallMethod(table_obj, "column", "n", static_cast<Py_ssize_t>(column));
    if (column_obj == nullptr) {
      Py_DECREF(table_obj);
      throw std::runtime_error("failed to read pyarrow column");
    }
    PyObject* pylist = PyObject_CallMethod(column_obj, "to_pylist", nullptr);
    Py_DECREF(column_obj);
    if (pylist == nullptr || !PyList_Check(pylist) || PyList_GET_SIZE(pylist) != row_count) {
      Py_XDECREF(pylist);
      Py_DECREF(table_obj);
      throw std::runtime_error("failed to convert pyarrow column to python list");
    }
    df::appendColumn(&out, materializePyListColumn(pylist, out.schema.fields[column].c_str()));
    Py_DECREF(pylist);
  }

  Py_DECREF(table_obj);
  return out;
}

df::Table mergeArrowTables(const std::vector<df::Table>& tables) {
  if (tables.empty()) {
    return df::Table();
  }
  df::Table merged;
  merged.schema = tables.front().schema;
  merged.columnar_cache = std::make_shared<df::ColumnarTable>();
  merged.columnar_cache->schema = merged.schema;
  const auto first_cache = df::ensureColumnarCache(&tables.front());
  merged.columnar_cache->columns.resize(first_cache->columns.size());
  merged.columnar_cache->arrow_formats = first_cache->arrow_formats;

  for (const auto& table : tables) {
    if (table.schema.fields != merged.schema.fields) {
      throw std::runtime_error("Arrow stream batches must share the same schema");
    }
    const auto cache = df::ensureColumnarCache(&table);
    if (cache->columns.size() != merged.columnar_cache->columns.size()) {
      throw std::runtime_error("Arrow stream batches must share the same schema width");
    }
    if (!cache->arrow_formats.empty()) {
      if (merged.columnar_cache->arrow_formats.empty()) {
        merged.columnar_cache->arrow_formats = cache->arrow_formats;
      } else if (cache->arrow_formats != merged.columnar_cache->arrow_formats) {
        merged.columnar_cache->arrow_formats.assign(merged.columnar_cache->columns.size(), "");
      }
    }
    for (std::size_t column = 0; column < cache->columns.size(); ++column) {
      auto& out_values = merged.columnar_cache->columns[column].values;
      const auto& in_values = cache->columns[column].values;
      out_values.insert(out_values.end(), in_values.begin(), in_values.end());
    }
    if (!cache->batch_row_counts.empty()) {
      merged.columnar_cache->batch_row_counts.insert(merged.columnar_cache->batch_row_counts.end(),
                                                     cache->batch_row_counts.begin(),
                                                     cache->batch_row_counts.end());
    } else if (!cache->columns.empty()) {
      merged.columnar_cache->batch_row_counts.push_back(cache->columns.front().values.size());
    }
    if (!table.rows.empty()) {
      merged.rows.insert(merged.rows.end(), table.rows.begin(), table.rows.end());
    }
  }
  if (merged.rows.empty() && !merged.columnar_cache->columns.empty()) {
    const auto row_count = merged.columnar_cache->columns.front().values.size();
    if (row_count > 0 && merged.columnar_cache->batch_row_counts.empty()) {
      merged.columnar_cache->batch_row_counts.push_back(row_count);
    }
  }
  if (!merged.rows.empty()) {
    for (std::size_t column = 0; column < merged.columnar_cache->columns.size(); ++column) {
      if (merged.columnar_cache->columns[column].values.size() != merged.rows.size()) {
        throw std::runtime_error("Arrow stream merged column row count mismatch");
      }
    }
  }
  if (merged.columnar_cache->arrow_formats.size() != merged.columnar_cache->columns.size()) {
    merged.columnar_cache->arrow_formats.assign(merged.columnar_cache->columns.size(), "");
  }
  return merged;
}

PyObject* importRecordBatchReaderFromArrowObject(PyObject* pyarrow, PyObject* obj) {
  PyObject* reader_type = PyObject_GetAttrString(pyarrow, "RecordBatchReader");
  if (reader_type == nullptr) {
    throw std::runtime_error("failed to resolve pyarrow.RecordBatchReader");
  }

  PyObject* reader = nullptr;
  if (PyObject_IsInstance(obj, reader_type) == 1) {
    Py_INCREF(obj);
    reader = obj;
  } else if (PyObject_HasAttrString(obj, "__arrow_c_stream__")) {
    PyObject* capsule = PyObject_CallMethod(obj, "__arrow_c_stream__", nullptr);
    if (capsule == nullptr) {
      Py_DECREF(reader_type);
      throw std::runtime_error("failed to export Arrow C stream");
    }
    PyObject* importer = PyObject_GetAttrString(reader_type, "_import_from_c_capsule");
    if (importer == nullptr) {
      Py_DECREF(capsule);
      Py_DECREF(reader_type);
      throw std::runtime_error("pyarrow.RecordBatchReader._import_from_c_capsule() is unavailable");
    }
    reader = PyObject_CallFunctionObjArgs(importer, capsule, nullptr);
    Py_DECREF(importer);
    Py_DECREF(capsule);
    if (reader == nullptr) {
      Py_DECREF(reader_type);
      throw std::runtime_error("failed to import Arrow C stream as RecordBatchReader");
    }
  }
  Py_DECREF(reader_type);
  return reader;
}

std::vector<df::Table> tablesFromArrowObject(PyObject* obj) {
  if (PyUnicode_Check(obj) || PyBytes_Check(obj) || PyByteArray_Check(obj)) {
    return {tableFromArrowSlow(obj)};
  }
  if (PyList_Check(obj) || PyTuple_Check(obj)) {
    PyObject* seq = PySequence_Fast(obj, "arrow batches");
    if (seq == nullptr) {
      throw std::runtime_error("failed to read arrow batch sequence");
    }
    std::vector<df::Table> tables;
    const Py_ssize_t count = PySequence_Fast_GET_SIZE(seq);
    tables.reserve(static_cast<size_t>(count));
    PyObject** items = PySequence_Fast_ITEMS(seq);
    for (Py_ssize_t i = 0; i < count; ++i) {
      const auto nested = tablesFromArrowObject(items[i]);
      tables.insert(tables.end(), nested.begin(), nested.end());
    }
    Py_DECREF(seq);
    return tables;
  }

  PyObject* pyarrow = PyImport_ImportModule("pyarrow");
  if (pyarrow == nullptr) {
    throw std::runtime_error("pyarrow is required for create_stream_from_arrow()");
  }

  PyObject* reader = importRecordBatchReaderFromArrowObject(pyarrow, obj);
  if (reader != nullptr) {
    std::vector<df::Table> tables;
    while (true) {
      PyObject* batch = PyObject_CallMethod(reader, "read_next_batch", nullptr);
      if (batch == nullptr) {
        if (PyErr_ExceptionMatches(PyExc_StopIteration)) {
          PyErr_Clear();
          break;
        }
        Py_DECREF(reader);
        Py_DECREF(pyarrow);
        throw std::runtime_error("failed to read next RecordBatch from Arrow stream");
      }
      df::Table table;
      if (!tryTableFromArrowCapsules(batch, &table)) {
        table = tableFromArrowSlow(batch);
      }
      tables.push_back(std::move(table));
      Py_DECREF(batch);
    }
    Py_DECREF(reader);
    Py_DECREF(pyarrow);
    return tables;
  }

  Py_DECREF(pyarrow);
  df::Table table;
  if (!tryTableFromArrowCapsules(obj, &table)) {
    table = tableFromArrowSlow(obj);
  }
  return {std::move(table)};
}

df::Table tableFromArrowObject(PyObject* obj) {
  return mergeArrowTables(tablesFromArrowObject(obj));
}

PyObject* pyFromValue(const df::Value& value) {
  switch (value.type()) {
    case df::DataType::Nil:
      Py_RETURN_NONE;
    case df::DataType::Int64:
      return PyLong_FromLongLong(value.asInt64());
    case df::DataType::Double:
      return PyFloat_FromDouble(value.asDouble());
    case df::DataType::String:
      return PyUnicode_FromString(value.asString().c_str());
    case df::DataType::FixedVector: {
      const auto& vec = value.asFixedVector();
      PyObject* out = PyList_New(static_cast<Py_ssize_t>(vec.size()));
      for (size_t i = 0; i < vec.size(); ++i) {
        PyList_SET_ITEM(out, static_cast<Py_ssize_t>(i), PyFloat_FromDouble(vec[i]));
      }
      return out;
    }
  }
  Py_RETURN_NONE;
}

PyObject* pyRowsFromTable(const df::Table& table) {
  df::Table materialized = table;
  df::materializeRows(&materialized);
  PyObject* out = PyDict_New();
  PyObject* schema = PyList_New(static_cast<Py_ssize_t>(materialized.schema.fields.size()));
  for (size_t i = 0; i < materialized.schema.fields.size(); ++i) {
    PyObject* item = PyUnicode_FromString(materialized.schema.fields[i].c_str());
    PyList_SET_ITEM(schema, static_cast<Py_ssize_t>(i), item);
  }
  PyObject* rows = PyList_New(static_cast<Py_ssize_t>(materialized.rows.size()));
  for (size_t r = 0; r < materialized.rows.size(); ++r) {
    const auto& row = materialized.rows[r];
    PyObject* py_row = PyList_New(static_cast<Py_ssize_t>(row.size()));
    for (size_t c = 0; c < row.size(); ++c) {
      PyObject* cell = pyFromValue(row[c]);
      PyList_SET_ITEM(py_row, static_cast<Py_ssize_t>(c), cell);
    }
    PyList_SET_ITEM(rows, static_cast<Py_ssize_t>(r), py_row);
  }
  PyDict_SetItemString(out, "schema", schema);
  PyDict_SetItemString(out, "rows", rows);
  Py_DECREF(schema);
  Py_DECREF(rows);
  return out;
}

std::string arrowFormatForValue(const df::Value& value) {
  switch (value.type()) {
    case df::DataType::Nil:
    case df::DataType::String:
      return "u";
    case df::DataType::Int64:
      return "l";
    case df::DataType::Double:
      return "g";
    case df::DataType::FixedVector: {
      return "+w:" + std::to_string(value.asFixedVector().size());
    }
  }
  return "u";
}

bool supportsArrowExportFormat(const std::string& format) {
  return format == "b" || format == "i" || format == "I" || format == "l" || format == "L" ||
         format == "f" || format == "g" || format == "u" || format == "U" ||
         format.rfind("+w:", 0) == 0;
}

std::string arrowFormatForColumn(const df::ValueColumnBuffer& column,
                                 const std::string* preferred_format = nullptr) {
  if (column.arrow_backing != nullptr && !column.arrow_backing->format.empty() &&
      supportsArrowExportFormat(column.arrow_backing->format)) {
    return column.arrow_backing->format;
  }
  if (preferred_format != nullptr && !preferred_format->empty() &&
      supportsArrowExportFormat(*preferred_format)) {
    return *preferred_format;
  }
  for (const auto& value : column.values) {
    if (!value.isNull()) {
      return arrowFormatForValue(value);
    }
  }
  return "u";
}

template <typename T, typename ConvertFn>
std::pair<std::shared_ptr<void>, std::shared_ptr<void>> makeNumericBuffers(
    const df::ValueColumnBuffer& column, ConvertFn&& convert, int64_t* null_count) {
  const size_t row_count = column.values.size();
  const size_t bitmap_bytes = row_count == 0 ? 0 : (row_count + 7) / 8;
  auto* validity = static_cast<uint8_t*>(std::calloc(bitmap_bytes == 0 ? 1 : bitmap_bytes, 1));
  auto* raw = static_cast<T*>(std::calloc(row_count == 0 ? 1 : row_count, sizeof(T)));
  int64_t local_nulls = 0;
  for (size_t row = 0; row < row_count; ++row) {
    const auto& value = column.values[row];
    if (value.isNull()) {
      ++local_nulls;
      continue;
    }
    validity[row / 8] |= static_cast<uint8_t>(1U << (row % 8));
    raw[row] = convert(value);
  }
  *null_count = local_nulls;
  return {
      std::shared_ptr<void>(validity, std::free),
      std::shared_ptr<void>(raw, std::free),
  };
}

std::pair<std::shared_ptr<void>, std::shared_ptr<void>> makeBooleanBuffers(
    const df::ValueColumnBuffer& column, int64_t* null_count) {
  const size_t row_count = column.values.size();
  const size_t bitmap_bytes = row_count == 0 ? 0 : (row_count + 7) / 8;
  auto* validity = static_cast<uint8_t*>(std::calloc(bitmap_bytes == 0 ? 1 : bitmap_bytes, 1));
  auto* bits = static_cast<uint8_t*>(std::calloc(bitmap_bytes == 0 ? 1 : bitmap_bytes, 1));
  int64_t local_nulls = 0;
  for (size_t row = 0; row < row_count; ++row) {
    const auto& value = column.values[row];
    if (value.isNull()) {
      ++local_nulls;
      continue;
    }
    validity[row / 8] |= static_cast<uint8_t>(1U << (row % 8));
    if (value.asInt64() != 0) {
      bits[row / 8] |= static_cast<uint8_t>(1U << (row % 8));
    }
  }
  *null_count = local_nulls;
  return {
      std::shared_ptr<void>(validity, std::free),
      std::shared_ptr<void>(bits, std::free),
  };
}

struct StringBuffers {
  std::shared_ptr<void> offsets;
  std::shared_ptr<void> data;
};

template <typename OffsetT>
StringBuffers makeStringBuffers(const df::ValueColumnBuffer& column,
                                std::shared_ptr<void> null_bitmap,
                                int64_t* null_count) {
  const size_t row_count = column.values.size();
  auto* offsets = static_cast<OffsetT*>(
      std::calloc((row_count + 1) == 0 ? 1 : (row_count + 1), sizeof(OffsetT)));
  auto* validity = static_cast<uint8_t*>(null_bitmap.get());
  OffsetT current = 0;
  int64_t local_nulls = 0;
  std::size_t total_bytes = 0;
  for (size_t row = 0; row < row_count; ++row) {
    offsets[row] = current;
    const auto& value = column.values[row];
    if (value.isNull()) {
      ++local_nulls;
      continue;
    }
    validity[row / 8] |= static_cast<uint8_t>(1U << (row % 8));
    const std::string& s = value.asString();
    current += static_cast<OffsetT>(s.size());
    total_bytes += s.size();
  }
  offsets[row_count] = current;
  *null_count = local_nulls;
  auto* data = static_cast<char*>(std::malloc(total_bytes == 0 ? 1 : total_bytes));
  std::size_t write_offset = 0;
  for (size_t row = 0; row < row_count; ++row) {
    if (column.values[row].isNull()) {
      continue;
    }
    const std::string& s = column.values[row].asString();
    if (!s.empty()) {
      std::memcpy(data + write_offset, s.data(), s.size());
      write_offset += s.size();
    }
  }
  return {
      std::shared_ptr<void>(offsets, std::free),
      std::shared_ptr<void>(data, std::free),
  };
}

std::shared_ptr<void> makeFixedSizeListFloat32Buffer(const df::ValueColumnBuffer& column,
                                                     int32_t list_size,
                                                     std::shared_ptr<void> null_bitmap,
                                                     int64_t* null_count,
                                                     int64_t* child_length) {
  const size_t row_count = column.values.size();
  auto* validity = static_cast<uint8_t*>(null_bitmap.get());
  auto* raw = static_cast<float*>(
      std::calloc(row_count == 0 ? 1 : row_count * static_cast<size_t>(list_size), sizeof(float)));
  int64_t local_nulls = 0;
  for (size_t row = 0; row < row_count; ++row) {
    const auto& value = column.values[row];
    if (value.isNull()) {
      ++local_nulls;
      continue;
    }
    const auto& vec = value.asFixedVector();
    if (static_cast<int32_t>(vec.size()) != list_size) {
      throw std::runtime_error("fixed-size-list<float32> column dimension mismatch");
    }
    validity[row / 8] |= static_cast<uint8_t>(1U << (row % 8));
    std::memcpy(raw + row * static_cast<size_t>(list_size), vec.data(),
                sizeof(float) * static_cast<size_t>(list_size));
  }
  *null_count = local_nulls;
  *child_length = static_cast<int64_t>(row_count) * static_cast<int64_t>(list_size);
  return std::shared_ptr<void>(raw, std::free);
}

struct PreparedArrowColumn {
  std::string format;
  std::string child_format;
  int32_t fixed_list_size = 0;
  int64_t null_count = 0;
  std::shared_ptr<void> null_bitmap;
  std::shared_ptr<void> value_buffer;
  std::shared_ptr<void> extra_buffer;
  std::shared_ptr<void> child_value_buffer;
  int64_t n_buffers = 0;
  int64_t child_length = 0;
};

std::vector<PreparedArrowColumn> prepareArrowColumns(const df::ColumnarTable& cache) {
  std::vector<PreparedArrowColumn> prepared;
  prepared.reserve(cache.columns.size());
  for (std::size_t index = 0; index < cache.columns.size(); ++index) {
    const auto& column = cache.columns[index];
    PreparedArrowColumn item;
    const std::string* preferred_format =
        index < cache.arrow_formats.size() ? &cache.arrow_formats[index] : nullptr;
    item.format = arrowFormatForColumn(column, preferred_format);
    if (column.arrow_backing != nullptr && column.arrow_backing->format == item.format) {
      item.null_count = column.arrow_backing->null_count;
      item.null_bitmap = column.arrow_backing->null_bitmap;
      item.value_buffer = column.arrow_backing->value_buffer;
      item.extra_buffer = column.arrow_backing->extra_buffer;
      item.child_value_buffer = column.arrow_backing->child_value_buffer;
      item.child_format = column.arrow_backing->child_format;
      item.fixed_list_size = column.arrow_backing->fixed_list_size;
      item.child_length = static_cast<int64_t>(column.arrow_backing->child_length);
      item.n_buffers = item.fixed_list_size > 0 ? 1 : (item.extra_buffer ? 3 : 2);
      if (item.format == "u" || item.format == "U") {
        item.n_buffers = 3;
      }
      if (item.format.rfind("+w:", 0) == 0) {
        item.n_buffers = 1;
      }
      prepared.push_back(std::move(item));
      continue;
    }
    df::ValueColumnBuffer materialized_column;
    if (column.values.empty()) {
      materialized_column.values = df::materializeValueBuffer(&column);
    }
    const auto& source_column = materialized_column.values.empty() ? column : materialized_column;
    if (item.format == "b") {
      auto buffers = makeBooleanBuffers(source_column, &item.null_count);
      item.null_bitmap = std::move(buffers.first);
      item.value_buffer = std::move(buffers.second);
      item.n_buffers = 2;
    } else if (item.format == "i") {
      auto buffers = makeNumericBuffers<int32_t>(
          source_column,
          [](const df::Value& value) { return static_cast<int32_t>(value.asInt64()); },
          &item.null_count);
      item.null_bitmap = std::move(buffers.first);
      item.value_buffer = std::move(buffers.second);
      item.n_buffers = 2;
    } else if (item.format == "I") {
      auto buffers = makeNumericBuffers<uint32_t>(
          source_column,
          [](const df::Value& value) { return static_cast<uint32_t>(value.asInt64()); },
          &item.null_count);
      item.null_bitmap = std::move(buffers.first);
      item.value_buffer = std::move(buffers.second);
      item.n_buffers = 2;
    } else if (item.format == "l") {
      auto buffers = makeNumericBuffers<int64_t>(
          source_column, [](const df::Value& value) { return value.asInt64(); }, &item.null_count);
      item.null_bitmap = std::move(buffers.first);
      item.value_buffer = std::move(buffers.second);
      item.n_buffers = 2;
    } else if (item.format == "L") {
      auto buffers = makeNumericBuffers<uint64_t>(
          source_column,
          [](const df::Value& value) { return static_cast<uint64_t>(value.asInt64()); },
          &item.null_count);
      item.null_bitmap = std::move(buffers.first);
      item.value_buffer = std::move(buffers.second);
      item.n_buffers = 2;
    } else if (item.format == "f") {
      auto buffers = makeNumericBuffers<float>(
          source_column,
          [](const df::Value& value) { return static_cast<float>(value.asDouble()); },
          &item.null_count);
      item.null_bitmap = std::move(buffers.first);
      item.value_buffer = std::move(buffers.second);
      item.n_buffers = 2;
    } else if (item.format == "g") {
      auto buffers = makeNumericBuffers<double>(
          source_column, [](const df::Value& value) { return value.asDouble(); }, &item.null_count);
      item.null_bitmap = std::move(buffers.first);
      item.value_buffer = std::move(buffers.second);
      item.n_buffers = 2;
    } else if (item.format == "U") {
      item.null_bitmap = std::shared_ptr<void>(
          std::calloc(source_column.values.empty() ? 1 : (source_column.values.size() + 7) / 8, 1), std::free);
      auto strings = makeStringBuffers<int64_t>(source_column, item.null_bitmap, &item.null_count);
      item.value_buffer = std::move(strings.offsets);
      item.extra_buffer = std::move(strings.data);
      item.n_buffers = 3;
    } else if (item.format.rfind("+w:", 0) == 0) {
      item.fixed_list_size = static_cast<int32_t>(std::stoi(item.format.substr(3)));
      item.child_format = "f";
      item.null_bitmap = std::shared_ptr<void>(
          std::calloc(source_column.values.empty() ? 1 : (source_column.values.size() + 7) / 8, 1), std::free);
      item.child_value_buffer = makeFixedSizeListFloat32Buffer(
          source_column, item.fixed_list_size, item.null_bitmap, &item.null_count, &item.child_length);
      item.n_buffers = 1;
    } else {
      item.null_bitmap = std::shared_ptr<void>(
          std::calloc(source_column.values.empty() ? 1 : (source_column.values.size() + 7) / 8, 1), std::free);
      auto strings = makeStringBuffers<int32_t>(source_column, item.null_bitmap, &item.null_count);
      item.value_buffer = std::move(strings.offsets);
      item.extra_buffer = std::move(strings.data);
      item.n_buffers = 3;
    }
    prepared.push_back(std::move(item));
  }
  return prepared;
}

OwnedArrowSchema* buildArrowSchema(const df::Table& table,
                                   const std::vector<PreparedArrowColumn>& prepared) {
  auto* root = new OwnedArrowSchema();
  root->format = "+s";
  root->schema.format = root->format.c_str();
  root->schema.name = "";
  root->schema.metadata = nullptr;
  root->schema.flags = 0;
  root->schema.n_children = static_cast<int64_t>(table.schema.fields.size());
  root->children_owned.reserve(table.schema.fields.size());
  root->child_ptrs.reserve(table.schema.fields.size());

  for (size_t i = 0; i < table.schema.fields.size(); ++i) {
    auto child = std::make_unique<OwnedArrowSchema>();
    child->format = prepared[i].format;
    child->name = table.schema.fields[i];
    child->schema.format = child->format.c_str();
    child->schema.name = child->name.c_str();
    child->schema.metadata = nullptr;
    child->schema.flags = 2;
    if (prepared[i].fixed_list_size > 0) {
      auto grandchild = std::make_unique<OwnedArrowSchema>();
      grandchild->format = prepared[i].child_format;
      grandchild->name = "item";
      grandchild->schema.format = grandchild->format.c_str();
      grandchild->schema.name = grandchild->name.c_str();
      grandchild->schema.metadata = nullptr;
      grandchild->schema.flags = 2;
      grandchild->schema.n_children = 0;
      grandchild->schema.children = nullptr;
      grandchild->schema.dictionary = nullptr;
      grandchild->schema.release = releaseArrowSchema;
      grandchild->schema.private_data = grandchild.get();
      child->child_ptrs.push_back(&grandchild->schema);
      child->children_owned.push_back(std::move(grandchild));
      child->schema.n_children = 1;
      child->schema.children = child->child_ptrs.data();
    } else {
      child->schema.n_children = 0;
      child->schema.children = nullptr;
    }
    child->schema.dictionary = nullptr;
    child->schema.release = releaseArrowSchema;
    child->schema.private_data = child.get();
    root->child_ptrs.push_back(&child->schema);
    root->children_owned.push_back(std::move(child));
  }

  root->schema.children = root->child_ptrs.empty() ? nullptr : root->child_ptrs.data();
  root->schema.dictionary = nullptr;
  root->schema.release = releaseArrowSchema;
  root->schema.private_data = root;
  return root;
}

OwnedArrowArray* buildArrowArray(const df::Table& table,
                                 const std::vector<PreparedArrowColumn>& prepared) {
  const auto row_count = table.rowCount();
  auto* root = new OwnedArrowArray();
  root->array.length = static_cast<int64_t>(row_count);
  root->array.null_count = 0;
  root->array.offset = 0;
  root->array.n_buffers = 1;
  root->buffer_ptrs.push_back(nullptr);
  root->array.buffers = root->buffer_ptrs.data();
  root->array.n_children = static_cast<int64_t>(table.schema.fields.size());
  root->children_owned.reserve(table.schema.fields.size());
  root->child_ptrs.reserve(table.schema.fields.size());

  for (size_t column = 0; column < table.schema.fields.size(); ++column) {
    auto child = std::make_unique<OwnedArrowArray>();
    child->array.length = static_cast<int64_t>(row_count);
    child->array.offset = 0;
    child->array.n_children = 0;
    child->array.children = nullptr;
    child->array.dictionary = nullptr;
    child->array.release = releaseArrowArray;
    child->array.private_data = child.get();
    child->array.null_count = prepared[column].null_count;
    child->array.n_buffers = prepared[column].n_buffers;
    child->buffer_holders.push_back(prepared[column].null_bitmap);
    child->buffer_ptrs.push_back(prepared[column].null_bitmap.get());
    if (prepared[column].fixed_list_size > 0) {
      auto values = std::make_unique<OwnedArrowArray>();
      values->array.length = prepared[column].child_length;
      values->array.offset = 0;
      values->array.n_children = 0;
      values->array.children = nullptr;
      values->array.dictionary = nullptr;
      values->array.release = releaseArrowArray;
      values->array.private_data = values.get();
      values->array.null_count = 0;
      values->array.n_buffers = 2;
      values->buffer_ptrs.push_back(nullptr);
      values->buffer_holders.push_back(prepared[column].child_value_buffer);
      values->buffer_ptrs.push_back(prepared[column].child_value_buffer.get());
      values->array.buffers = values->buffer_ptrs.data();
      child->child_ptrs.push_back(&values->array);
      child->children_owned.push_back(std::move(values));
      child->array.n_children = 1;
      child->array.children = child->child_ptrs.data();
    } else {
      child->buffer_holders.push_back(prepared[column].value_buffer);
      child->buffer_ptrs.push_back(prepared[column].value_buffer.get());
      if (prepared[column].n_buffers == 3) {
        child->buffer_holders.push_back(prepared[column].extra_buffer);
        child->buffer_ptrs.push_back(prepared[column].extra_buffer.get());
      }
      child->array.n_children = 0;
      child->array.children = nullptr;
    }
    child->array.buffers = child->buffer_ptrs.data();
    root->child_ptrs.push_back(&child->array);
    root->children_owned.push_back(std::move(child));
  }

  root->array.children = root->child_ptrs.empty() ? nullptr : root->child_ptrs.data();
  root->array.dictionary = nullptr;
  root->array.release = releaseArrowArray;
  root->array.private_data = root;
  return root;
}

PyObject* exportArrowCapsules(const df::Table& table) {
  const auto cache = df::ensureColumnarCache(&table);
  const auto prepared = prepareArrowColumns(*cache);
  auto* owned_schema = buildArrowSchema(table, prepared);
  auto* owned_array = buildArrowArray(table, prepared);

  auto* schema = static_cast<ArrowSchema*>(std::malloc(sizeof(ArrowSchema)));
  auto* array = static_cast<ArrowArray*>(std::malloc(sizeof(ArrowArray)));
  if (schema == nullptr || array == nullptr) {
    delete owned_schema;
    delete owned_array;
    std::free(schema);
    std::free(array);
    throw std::runtime_error("failed to allocate Arrow export structs");
  }

  *schema = owned_schema->schema;
  *array = owned_array->array;

  PyObject* schema_capsule = PyCapsule_New(schema, "arrow_schema", releaseArrowSchemaCapsule);
  if (schema_capsule == nullptr) {
    owned_schema->schema.release(&owned_schema->schema);
    owned_array->array.release(&owned_array->array);
    std::free(schema);
    std::free(array);
    return nullptr;
  }

  PyObject* array_capsule = PyCapsule_New(array, "arrow_array", releaseArrowArrayCapsule);
  if (array_capsule == nullptr) {
    Py_DECREF(schema_capsule);
    owned_schema->schema.release(&owned_schema->schema);
    owned_array->array.release(&owned_array->array);
    std::free(array);
    return nullptr;
  }

  PyObject* result = PyTuple_New(2);
  PyTuple_SET_ITEM(result, 0, schema_capsule);
  PyTuple_SET_ITEM(result, 1, array_capsule);
  return result;
}

PyObject* pyProgressFromNative(const df::StreamingQueryProgress& progress) {
  PyObject* out = PyDict_New();
  setDictItem(out, "query_id", PyUnicode_FromString(progress.query_id.c_str()));
  setDictItem(out, "status", PyUnicode_FromString(progress.status.c_str()));
  setDictItem(out, "requested_execution_mode",
              PyUnicode_FromString(progress.requested_execution_mode.c_str()));
  setDictItem(out, "execution_mode", PyUnicode_FromString(progress.execution_mode.c_str()));
  setDictItem(out, "execution_reason", PyUnicode_FromString(progress.execution_reason.c_str()));
  setDictItem(out, "transport_mode", PyUnicode_FromString(progress.transport_mode.c_str()));
  setDictItem(out, "checkpoint_delivery_mode",
              PyUnicode_FromString(progress.checkpoint_delivery_mode.c_str()));
  setDictItem(out, "batches_pulled", PyLong_FromUnsignedLongLong(progress.batches_pulled));
  setDictItem(out, "batches_processed", PyLong_FromUnsignedLongLong(progress.batches_processed));
  setDictItem(out, "blocked_count", PyLong_FromUnsignedLongLong(progress.blocked_count));
  setDictItem(out, "max_backlog_batches",
              PyLong_FromUnsignedLongLong(progress.max_backlog_batches));
  setDictItem(out, "inflight_batches", PyLong_FromUnsignedLongLong(progress.inflight_batches));
  setDictItem(out, "inflight_partitions",
              PyLong_FromUnsignedLongLong(progress.inflight_partitions));
  setDictItem(out, "last_batch_latency_ms",
              PyLong_FromUnsignedLongLong(progress.last_batch_latency_ms));
  setDictItem(out, "last_sink_latency_ms",
              PyLong_FromUnsignedLongLong(progress.last_sink_latency_ms));
  setDictItem(out, "last_state_latency_ms",
              PyLong_FromUnsignedLongLong(progress.last_state_latency_ms));
  setDictItem(out, "last_source_offset",
              PyUnicode_FromString(progress.last_source_offset.c_str()));
  PyDict_SetItemString(out, "backpressure_active",
                       progress.backpressure_active ? Py_True : Py_False);
  PyDict_SetItemString(out, "actor_eligible", progress.actor_eligible ? Py_True : Py_False);
  PyDict_SetItemString(out, "used_actor_runtime",
                       progress.used_actor_runtime ? Py_True : Py_False);
  PyDict_SetItemString(out, "used_shared_memory",
                       progress.used_shared_memory ? Py_True : Py_False);
  PyDict_SetItemString(out, "has_stateful_ops",
                       progress.has_stateful_ops ? Py_True : Py_False);
  PyDict_SetItemString(out, "has_window", progress.has_window ? Py_True : Py_False);
  PyDict_SetItemString(out, "sink_is_blocking",
                       progress.sink_is_blocking ? Py_True : Py_False);
  PyDict_SetItemString(out, "source_is_bounded",
                       progress.source_is_bounded ? Py_True : Py_False);
  setDictItem(out, "estimated_partitions",
              PyLong_FromUnsignedLongLong(progress.estimated_partitions));
  setDictItem(out, "projected_payload_bytes",
              PyLong_FromUnsignedLongLong(progress.projected_payload_bytes));
  setDictItem(out, "sampled_batches", PyLong_FromUnsignedLongLong(progress.sampled_batches));
  setDictItem(out, "sampled_rows_per_batch",
              PyLong_FromUnsignedLongLong(progress.sampled_rows_per_batch));
  setDictItem(out, "average_projected_payload_bytes",
              PyLong_FromUnsignedLongLong(progress.average_projected_payload_bytes));
  setDictItem(out, "actor_speedup", PyFloat_FromDouble(progress.actor_speedup));
  setDictItem(out, "compute_to_overhead_ratio",
              PyFloat_FromDouble(progress.compute_to_overhead_ratio));
  setDictItem(out, "estimated_state_size_bytes",
              PyLong_FromUnsignedLongLong(progress.estimated_state_size_bytes));
  setDictItem(out, "estimated_batch_cost",
              PyLong_FromUnsignedLongLong(progress.estimated_batch_cost));
  setDictItem(out, "backpressure_max_queue_batches",
              PyLong_FromUnsignedLongLong(progress.backpressure_max_queue_batches));
  setDictItem(out, "backpressure_high_watermark",
              PyLong_FromUnsignedLongLong(progress.backpressure_high_watermark));
  setDictItem(out, "backpressure_low_watermark",
              PyLong_FromUnsignedLongLong(progress.backpressure_low_watermark));
  return out;
}

PyObject* wrapDataFrame(df::DataFrame value) {
  auto* obj = reinterpret_cast<PyVelariaDataFrame*>(
      PyVelariaDataFrameType.tp_alloc(&PyVelariaDataFrameType, 0));
  if (obj == nullptr) {
    return nullptr;
  }
  obj->df_ptr = new df::DataFrame(std::move(value));
  return reinterpret_cast<PyObject*>(obj);
}

PyObject* wrapStreamingDataFrame(df::StreamingDataFrame value) {
  auto* obj = reinterpret_cast<PyVelariaStreamingDataFrame*>(
      PyVelariaStreamingDataFrameType.tp_alloc(&PyVelariaStreamingDataFrameType, 0));
  if (obj == nullptr) {
    return nullptr;
  }
  obj->sdf_ptr = new df::StreamingDataFrame(std::move(value));
  return reinterpret_cast<PyObject*>(obj);
}

PyObject* wrapStreamingQuery(df::StreamingQuery value) {
  auto* obj = reinterpret_cast<PyVelariaStreamingQuery*>(
      PyVelariaStreamingQueryType.tp_alloc(&PyVelariaStreamingQueryType, 0));
  if (obj == nullptr) {
    return nullptr;
  }
  obj->query_ptr = new df::StreamingQuery(std::move(value));
  return reinterpret_cast<PyObject*>(obj);
}

void sessionDealloc(PyVelariaSession* self) { Py_TYPE(self)->tp_free(reinterpret_cast<PyObject*>(self)); }
void dataFrameDealloc(PyVelariaDataFrame* self) {
  delete self->df_ptr;
  Py_TYPE(self)->tp_free(reinterpret_cast<PyObject*>(self));
}
void streamingDataFrameDealloc(PyVelariaStreamingDataFrame* self) {
  delete self->sdf_ptr;
  Py_TYPE(self)->tp_free(reinterpret_cast<PyObject*>(self));
}
void streamingQueryDealloc(PyVelariaStreamingQuery* self) {
  delete self->query_ptr;
  Py_TYPE(self)->tp_free(reinterpret_cast<PyObject*>(self));
}

PyObject* sessionNew(PyTypeObject* type, PyObject*, PyObject*) {
  auto* self = reinterpret_cast<PyVelariaSession*>(type->tp_alloc(type, 0));
  if (self == nullptr) {
    return nullptr;
  }
  self->session = &df::DataflowSession::builder();
  return reinterpret_cast<PyObject*>(self);
}

df::StreamingExecutionMode parseExecutionMode(const char* execution_mode) {
  const std::string mode = execution_mode == nullptr ? "single-process" : execution_mode;
  if (mode == "single-process") {
    return df::StreamingExecutionMode::SingleProcess;
  }
  if (mode == "local-workers") {
    return df::StreamingExecutionMode::LocalWorkers;
  }
  throw std::runtime_error("execution_mode must be 'single-process' or 'local-workers'");
}

df::StreamingQueryOptions parseQueryOptions(uint64_t trigger_interval_ms,
                                            const char* checkpoint_path,
                                            const char* checkpoint_delivery_mode,
                                            const char* execution_mode = "single-process",
                                            unsigned long long local_workers = 1,
                                            unsigned long long max_inflight_partitions = 0) {
  df::StreamingQueryOptions options;
  options.trigger_interval_ms = trigger_interval_ms;
  if (checkpoint_path != nullptr) {
    options.checkpoint_path = checkpoint_path;
  }
  options.execution_mode = parseExecutionMode(execution_mode);
  options.local_workers = static_cast<size_t>(local_workers);
  options.max_inflight_partitions = static_cast<size_t>(max_inflight_partitions);
  const std::string mode = checkpoint_delivery_mode == nullptr ? "at-least-once"
                                                               : checkpoint_delivery_mode;
  if (mode == "at-least-once") {
    options.checkpoint_delivery_mode = df::CheckpointDeliveryMode::AtLeastOnce;
  } else if (mode == "best-effort") {
    options.checkpoint_delivery_mode = df::CheckpointDeliveryMode::BestEffort;
  } else {
    throw std::runtime_error(
        "checkpoint_delivery_mode must be 'at-least-once' or 'best-effort'");
  }
  return options;
}

PyObject* sessionReadCsv(PyVelariaSession* self, PyObject* args, PyObject* kwargs) {
  return withExceptionTranslation([&]() -> PyObject* {
    const char* path = nullptr;
    const char* delimiter_text = ",";
    int materialization_enabled = 0;
    const char* materialization_dir = nullptr;
    const char* materialization_format = nullptr;
    static const char* kwlist[] = {"path", "delimiter", "materialization",
                                   "materialization_dir", "materialization_format", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|spzz", const_cast<char**>(kwlist), &path,
                                     &delimiter_text, &materialization_enabled,
                                     &materialization_dir, &materialization_format)) {
      return nullptr;
    }
    df::SourceOptions options;
    const auto delimiter = parseDelimiter(delimiter_text);
    options.materialization.enabled =
        materialization_enabled != 0 || materialization_dir != nullptr ||
        materialization_format != nullptr;
    if (materialization_dir != nullptr) {
      options.materialization.root = materialization_dir;
    }
    if (materialization_format != nullptr) {
      options.materialization.data_format = parseMaterializationDataFormat(materialization_format);
    }
    std::unique_ptr<df::DataFrame> out;
    {
      AllowThreads allow;
      out = std::make_unique<df::DataFrame>(self->session->read_csv(path, delimiter, options));
    }
    return wrapDataFrame(std::move(*out));
  });
}

PyObject* sessionSql(PyVelariaSession* self, PyObject* args) {
  return withExceptionTranslation([&]() -> PyObject* {
    const char* sql = nullptr;
    if (!PyArg_ParseTuple(args, "s", &sql)) {
      return nullptr;
    }
    std::unique_ptr<df::DataFrame> out;
    {
      AllowThreads allow;
      out = std::make_unique<df::DataFrame>(self->session->sql(sql));
    }
    return wrapDataFrame(std::move(*out));
  });
}

PyObject* sessionCreateTempView(PyVelariaSession* self, PyObject* args) {
  return withExceptionTranslation([&]() -> PyObject* {
    const char* name = nullptr;
    PyObject* view = nullptr;
    if (!PyArg_ParseTuple(args, "sO", &name, &view)) {
      return nullptr;
    }
    if (PyObject_TypeCheck(view, &PyVelariaDataFrameType)) {
      auto* df_obj = reinterpret_cast<PyVelariaDataFrame*>(view);
      self->session->createTempView(name, *df_obj->df_ptr);
    } else if (PyObject_TypeCheck(view, &PyVelariaStreamingDataFrameType)) {
      auto* sdf_obj = reinterpret_cast<PyVelariaStreamingDataFrame*>(view);
      self->session->createTempView(name, *sdf_obj->sdf_ptr);
    } else {
      PyErr_SetString(PyExc_TypeError,
                      "create_temp_view expects a DataFrame or StreamingDataFrame");
      return nullptr;
    }
    Py_RETURN_NONE;
  });
}

PyObject* sessionReadStreamCsvDir(PyVelariaSession* self, PyObject* args, PyObject* kwargs) {
  return withExceptionTranslation([&]() -> PyObject* {
    const char* path = nullptr;
    const char* delimiter_text = ",";
    static const char* kwlist[] = {"path", "delimiter", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|s", const_cast<char**>(kwlist), &path,
                                     &delimiter_text)) {
      return nullptr;
    }
    return wrapStreamingDataFrame(
        self->session->readStreamCsvDir(path, parseDelimiter(delimiter_text)));
  });
}

PyObject* sessionCreateDataFrameFromArrow(PyVelariaSession* self, PyObject* args) {
  return withExceptionTranslation([&]() -> PyObject* {
    PyObject* arrow_obj = nullptr;
    if (!PyArg_ParseTuple(args, "O", &arrow_obj)) {
      return nullptr;
    }
    df::Table table = tableFromArrowObject(arrow_obj);
    return wrapDataFrame(self->session->createDataFrame(table));
  });
}

PyObject* sessionCreateStreamFromArrow(PyVelariaSession* self, PyObject* args) {
  return withExceptionTranslation([&]() -> PyObject* {
    PyObject* arrow_obj = nullptr;
    if (!PyArg_ParseTuple(args, "O", &arrow_obj)) {
      return nullptr;
    }
    std::vector<df::Table> batches = tablesFromArrowObject(arrow_obj);
    return wrapStreamingDataFrame(
        self->session->readStream(std::make_shared<df::MemoryStreamSource>(std::move(batches))));
  });
}

PyObject* sessionStreamSql(PyVelariaSession* self, PyObject* args) {
  return withExceptionTranslation([&]() -> PyObject* {
    const char* sql = nullptr;
    if (!PyArg_ParseTuple(args, "s", &sql)) {
      return nullptr;
    }
    std::unique_ptr<df::StreamingDataFrame> out;
    {
      AllowThreads allow;
      out = std::make_unique<df::StreamingDataFrame>(self->session->streamSql(sql));
    }
    return wrapStreamingDataFrame(std::move(*out));
  });
}

PyObject* sessionStartStreamSql(PyVelariaSession* self, PyObject* args, PyObject* kwargs) {
  return withExceptionTranslation([&]() -> PyObject* {
    const char* sql = nullptr;
    unsigned long long trigger_interval_ms = 1000;
    const char* checkpoint_path = "";
    const char* checkpoint_delivery_mode = "at-least-once";
    const char* execution_mode = "single-process";
    unsigned long long local_workers = 1;
    unsigned long long max_inflight_partitions = 0;
    static const char* kwlist[] = {"sql", "trigger_interval_ms", "checkpoint_path",
                                   "checkpoint_delivery_mode", "execution_mode",
                                   "local_workers", "max_inflight_partitions", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|KsssKK", const_cast<char**>(kwlist), &sql,
                                     &trigger_interval_ms, &checkpoint_path,
                                     &checkpoint_delivery_mode, &execution_mode, &local_workers,
                                     &max_inflight_partitions)) {
      return nullptr;
    }
    std::unique_ptr<df::StreamingQuery> out;
    {
      AllowThreads allow;
      out = std::make_unique<df::StreamingQuery>(self->session->startStreamSql(
          sql, parseQueryOptions(static_cast<uint64_t>(trigger_interval_ms), checkpoint_path,
                                 checkpoint_delivery_mode, execution_mode, local_workers,
                                 max_inflight_partitions)));
    }
    return wrapStreamingQuery(std::move(*out));
  });
}

PyObject* sessionExplainStreamSql(PyVelariaSession* self, PyObject* args, PyObject* kwargs) {
  return withExceptionTranslation([&]() -> PyObject* {
    const char* sql = nullptr;
    unsigned long long trigger_interval_ms = 1000;
    const char* checkpoint_path = "";
    const char* checkpoint_delivery_mode = "at-least-once";
    const char* execution_mode = "single-process";
    unsigned long long local_workers = 1;
    unsigned long long max_inflight_partitions = 0;
    static const char* kwlist[] = {"sql", "trigger_interval_ms", "checkpoint_path",
                                   "checkpoint_delivery_mode", "execution_mode",
                                   "local_workers", "max_inflight_partitions", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|KsssKK", const_cast<char**>(kwlist), &sql,
                                     &trigger_interval_ms, &checkpoint_path,
                                     &checkpoint_delivery_mode, &execution_mode, &local_workers,
                                     &max_inflight_partitions)) {
      return nullptr;
    }
    std::string explain;
    {
      AllowThreads allow;
      explain = self->session->explainStreamSql(
          sql, parseQueryOptions(static_cast<uint64_t>(trigger_interval_ms), checkpoint_path,
                                 checkpoint_delivery_mode, execution_mode, local_workers,
                                 max_inflight_partitions));
    }
    return PyUnicode_FromStringAndSize(explain.c_str(),
                                       static_cast<Py_ssize_t>(explain.size()));
  });
}

df::VectorDistanceMetric parseVectorMetric(const std::string& metric) {
  if (metric == "cosine" || metric == "cosin") return df::VectorDistanceMetric::Cosine;
  if (metric == "dot") return df::VectorDistanceMetric::Dot;
  if (metric == "l2") return df::VectorDistanceMetric::L2;
  throw std::runtime_error("metric must be one of: cosine/cosin, dot, l2");
}

PyObject* sessionVectorSearch(PyVelariaSession* self, PyObject* args, PyObject* kwargs) {
  return withExceptionTranslation([&]() -> PyObject* {
    const char* table = nullptr;
    const char* vector_column = nullptr;
    PyObject* query_vector = nullptr;
    unsigned long long top_k = 10;
    const char* metric = "cosine";
    static const char* kwlist[] = {"table", "vector_column", "query_vector", "top_k", "metric",
                                   nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ssO|Ks", const_cast<char**>(kwlist), &table,
                                     &vector_column, &query_vector, &top_k, &metric)) {
      return nullptr;
    }
    const auto query = parseFloatVector(query_vector, "query_vector");
    df::DataFrame result;
    {
      AllowThreads allow;
      result = self->session->vectorQuery(table, vector_column, query, static_cast<size_t>(top_k),
                                          parseVectorMetric(metric));
    }
    return wrapDataFrame(result);
  });
}

PyObject* sessionExplainVectorSearch(PyVelariaSession* self, PyObject* args, PyObject* kwargs) {
  return withExceptionTranslation([&]() -> PyObject* {
    const char* table = nullptr;
    const char* vector_column = nullptr;
    PyObject* query_vector = nullptr;
    unsigned long long top_k = 10;
    const char* metric = "cosine";
    static const char* kwlist[] = {"table", "vector_column", "query_vector", "top_k", "metric",
                                   nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ssO|Ks", const_cast<char**>(kwlist), &table,
                                     &vector_column, &query_vector, &top_k, &metric)) {
      return nullptr;
    }
    const auto query = parseFloatVector(query_vector, "query_vector");
    std::string explain;
    {
      AllowThreads allow;
      explain = self->session->explainVectorQuery(table, vector_column, query,
                                                  static_cast<size_t>(top_k),
                                                  parseVectorMetric(metric));
    }
    return PyUnicode_FromString(explain.c_str());
  });
}

PyObject* dataFrameToRows(PyVelariaDataFrame* self, PyObject*) {
  return withExceptionTranslation([&]() -> PyObject* {
    const df::Table* table = nullptr;
    {
      AllowThreads allow;
      table = &self->df_ptr->materializedTable();
    }
    return pyRowsFromTable(*table);
  });
}

PyObject* dataFrameCount(PyVelariaDataFrame* self, PyObject*) {
  return withExceptionTranslation([&]() -> PyObject* {
    size_t count = 0;
    {
      AllowThreads allow;
      count = self->df_ptr->count();
    }
    return PyLong_FromUnsignedLongLong(count);
  });
}

PyObject* dataFrameExplain(PyVelariaDataFrame* self, PyObject*) {
  return withExceptionTranslation([&]() -> PyObject* {
    std::string explain;
    {
      AllowThreads allow;
      explain = self->df_ptr->explain();
    }
    return PyUnicode_FromStringAndSize(explain.c_str(), static_cast<Py_ssize_t>(explain.size()));
  });
}

PyObject* dataFrameShow(PyVelariaDataFrame* self, PyObject* args, PyObject* kwargs) {
  return withExceptionTranslation([&]() -> PyObject* {
    unsigned long long max_rows = 20;
    static const char* kwlist[] = {"max_rows", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|K", const_cast<char**>(kwlist), &max_rows)) {
      return nullptr;
    }
    {
      AllowThreads allow;
      self->df_ptr->show(static_cast<size_t>(max_rows));
    }
    Py_RETURN_NONE;
  });
}

PyObject* dataFrameArrowCapsules(PyVelariaDataFrame* self, PyObject* args, PyObject* kwargs) {
  return withExceptionTranslation([&]() -> PyObject* {
    PyObject* requested_schema = Py_None;
    static const char* kwlist[] = {"requested_schema", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|O", const_cast<char**>(kwlist),
                                     &requested_schema)) {
      return nullptr;
    }
    if (requested_schema != Py_None) {
      throw std::runtime_error("requested_schema is not supported yet");
    }
    const df::Table* table = nullptr;
    {
      AllowThreads allow;
      table = &self->df_ptr->materializedTable();
    }
    return exportArrowCapsules(*table);
  });
}

PyObject* dataFrameToArrow(PyVelariaDataFrame* self, PyObject*) {
  return withExceptionTranslation([&]() -> PyObject* {
    PyObject* pyarrow = PyImport_ImportModule("pyarrow");
    if (pyarrow == nullptr) {
      PyErr_SetString(PyExc_ImportError, "pyarrow is required for DataFrame.to_arrow()");
      return nullptr;
    }
    PyObject* record_batch_fn = PyObject_GetAttrString(pyarrow, "record_batch");
    if (record_batch_fn == nullptr) {
      Py_DECREF(pyarrow);
      return nullptr;
    }
    PyObject* batch = PyObject_CallFunctionObjArgs(record_batch_fn,
                                                   reinterpret_cast<PyObject*>(self), nullptr);
    Py_DECREF(record_batch_fn);
    if (batch == nullptr) {
      Py_DECREF(pyarrow);
      return nullptr;
    }
    PyObject* table_type = PyObject_GetAttrString(pyarrow, "Table");
    Py_DECREF(pyarrow);
    if (table_type == nullptr) {
      Py_DECREF(batch);
      return nullptr;
    }
    PyObject* from_batches = PyObject_GetAttrString(table_type, "from_batches");
    Py_DECREF(table_type);
    if (from_batches == nullptr) {
      Py_DECREF(batch);
      return nullptr;
    }
    PyObject* batches = PyList_New(1);
    PyList_SET_ITEM(batches, 0, batch);
    PyObject* table = PyObject_CallFunctionObjArgs(from_batches, batches, nullptr);
    Py_DECREF(from_batches);
    Py_DECREF(batches);
    Py_DECREF(batch);
    return table;
  });
}

PyObject* streamSelect(PyVelariaStreamingDataFrame* self, PyObject* args) {
  return withExceptionTranslation([&]() -> PyObject* {
    PyObject* columns = nullptr;
    if (!PyArg_ParseTuple(args, "O", &columns)) {
      return nullptr;
    }
    return wrapStreamingDataFrame(self->sdf_ptr->select(parseStringList(columns, "columns")));
  });
}

PyObject* streamFilter(PyVelariaStreamingDataFrame* self, PyObject* args) {
  return withExceptionTranslation([&]() -> PyObject* {
    const char* column = nullptr;
    const char* op = nullptr;
    PyObject* value = nullptr;
    if (!PyArg_ParseTuple(args, "ssO", &column, &op, &value)) {
      return nullptr;
    }
    return wrapStreamingDataFrame(self->sdf_ptr->filter(column, op, valueFromPy(value)));
  });
}

PyObject* streamWithColumn(PyVelariaStreamingDataFrame* self, PyObject* args) {
  return withExceptionTranslation([&]() -> PyObject* {
    const char* name = nullptr;
    const char* source = nullptr;
    if (!PyArg_ParseTuple(args, "ss", &name, &source)) {
      return nullptr;
    }
    return wrapStreamingDataFrame(self->sdf_ptr->withColumn(name, source));
  });
}

PyObject* streamDrop(PyVelariaStreamingDataFrame* self, PyObject* args) {
  return withExceptionTranslation([&]() -> PyObject* {
    const char* column = nullptr;
    if (!PyArg_ParseTuple(args, "s", &column)) {
      return nullptr;
    }
    return wrapStreamingDataFrame(self->sdf_ptr->drop(column));
  });
}

PyObject* streamLimit(PyVelariaStreamingDataFrame* self, PyObject* args) {
  return withExceptionTranslation([&]() -> PyObject* {
    unsigned long long n = 0;
    if (!PyArg_ParseTuple(args, "K", &n)) {
      return nullptr;
    }
    return wrapStreamingDataFrame(self->sdf_ptr->limit(static_cast<size_t>(n)));
  });
}

PyObject* streamWindow(PyVelariaStreamingDataFrame* self, PyObject* args, PyObject* kwargs) {
  return withExceptionTranslation([&]() -> PyObject* {
    const char* time_column = nullptr;
    unsigned long long window_ms = 0;
    const char* output_column = "window_start";
    static const char* kwlist[] = {"time_column", "window_ms", "output_column", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "sK|s", const_cast<char**>(kwlist),
                                     &time_column, &window_ms, &output_column)) {
      return nullptr;
    }
    return wrapStreamingDataFrame(
        self->sdf_ptr->window(time_column, static_cast<uint64_t>(window_ms), output_column));
  });
}

PyObject* streamGroupSum(PyVelariaStreamingDataFrame* self, PyObject* args, PyObject* kwargs) {
  return withExceptionTranslation([&]() -> PyObject* {
    PyObject* keys = nullptr;
    const char* value_column = nullptr;
    int stateful = 0;
    const char* output_column = "sum";
    static const char* kwlist[] = {"keys", "value_column", "stateful", "output_column", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "Os|ps", const_cast<char**>(kwlist), &keys,
                                     &value_column, &stateful, &output_column)) {
      return nullptr;
    }
    auto grouped = self->sdf_ptr->groupBy(parseStringList(keys, "keys"));
    return wrapStreamingDataFrame(grouped.sum(value_column, stateful != 0, output_column));
  });
}

PyObject* streamGroupCount(PyVelariaStreamingDataFrame* self, PyObject* args, PyObject* kwargs) {
  return withExceptionTranslation([&]() -> PyObject* {
    PyObject* keys = nullptr;
    int stateful = 0;
    const char* output_column = "count";
    static const char* kwlist[] = {"keys", "stateful", "output_column", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|ps", const_cast<char**>(kwlist), &keys,
                                     &stateful, &output_column)) {
      return nullptr;
    }
    auto grouped = self->sdf_ptr->groupBy(parseStringList(keys, "keys"));
    return wrapStreamingDataFrame(grouped.count(stateful != 0, output_column));
  });
}

PyObject* streamWriteStreamCsv(PyVelariaStreamingDataFrame* self, PyObject* args, PyObject* kwargs) {
  return withExceptionTranslation([&]() -> PyObject* {
    const char* path = nullptr;
    const char* delimiter_text = ",";
    unsigned long long trigger_interval_ms = 1000;
    const char* checkpoint_path = "";
    static const char* kwlist[] = {"path", "delimiter", "trigger_interval_ms", "checkpoint_path",
                                   nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|sKs", const_cast<char**>(kwlist), &path,
                                     &delimiter_text, &trigger_interval_ms, &checkpoint_path)) {
      return nullptr;
    }
    auto sink =
        std::make_shared<df::FileAppendStreamSink>(path, parseDelimiter(delimiter_text));
    return wrapStreamingQuery(self->sdf_ptr->writeStream(
        sink, parseQueryOptions(static_cast<uint64_t>(trigger_interval_ms), checkpoint_path,
                                "at-least-once")));
  });
}

PyObject* streamWriteStreamConsole(PyVelariaStreamingDataFrame* self, PyObject* args,
                                   PyObject* kwargs) {
  return withExceptionTranslation([&]() -> PyObject* {
    unsigned long long trigger_interval_ms = 1000;
    const char* checkpoint_path = "";
    static const char* kwlist[] = {"trigger_interval_ms", "checkpoint_path", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|Ks", const_cast<char**>(kwlist),
                                     &trigger_interval_ms, &checkpoint_path)) {
      return nullptr;
    }
    return wrapStreamingQuery(self->sdf_ptr->writeStreamToConsole(
        parseQueryOptions(static_cast<uint64_t>(trigger_interval_ms), checkpoint_path,
                          "at-least-once")));
  });
}

PyObject* queryStart(PyVelariaStreamingQuery* self, PyObject*) {
  return withExceptionTranslation([&]() -> PyObject* {
    {
      AllowThreads allow;
      self->query_ptr->start();
    }
    Py_INCREF(reinterpret_cast<PyObject*>(self));
    return reinterpret_cast<PyObject*>(self);
  });
}

PyObject* queryAwaitTermination(PyVelariaStreamingQuery* self, PyObject* args, PyObject* kwargs) {
  return withExceptionTranslation([&]() -> PyObject* {
    unsigned long long max_batches = 0;
    static const char* kwlist[] = {"max_batches", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|K", const_cast<char**>(kwlist),
                                     &max_batches)) {
      return nullptr;
    }
    size_t processed = 0;
    {
      AllowThreads allow;
      processed = self->query_ptr->awaitTermination(static_cast<size_t>(max_batches));
    }
    return PyLong_FromUnsignedLongLong(processed);
  });
}

PyObject* queryStop(PyVelariaStreamingQuery* self, PyObject*) {
  return withExceptionTranslation([&]() -> PyObject* {
    {
      AllowThreads allow;
      self->query_ptr->stop();
    }
    Py_RETURN_NONE;
  });
}

PyObject* queryProgress(PyVelariaStreamingQuery* self, PyObject*) {
  return withExceptionTranslation([&]() -> PyObject* {
    return pyProgressFromNative(self->query_ptr->progress());
  });
}

PyObject* querySnapshotJson(PyVelariaStreamingQuery* self, PyObject*) {
  return withExceptionTranslation([&]() -> PyObject* {
    std::string snapshot;
    {
      AllowThreads allow;
      snapshot = self->query_ptr->snapshotJson();
    }
    return PyUnicode_FromStringAndSize(snapshot.c_str(), static_cast<Py_ssize_t>(snapshot.size()));
  });
}

PyMethodDef sessionMethods[] = {
    {"read_csv", reinterpret_cast<PyCFunction>(sessionReadCsv), METH_VARARGS | METH_KEYWORDS,
     "Read a CSV file into a DataFrame."},
    {"sql", reinterpret_cast<PyCFunction>(sessionSql), METH_VARARGS, "Run batch SQL or SQL DDL."},
    {"create_dataframe_from_arrow", reinterpret_cast<PyCFunction>(sessionCreateDataFrameFromArrow),
     METH_VARARGS, "Create a DataFrame from a pyarrow.Table-compatible object."},
    {"create_stream_from_arrow", reinterpret_cast<PyCFunction>(sessionCreateStreamFromArrow),
     METH_VARARGS, "Create a StreamingDataFrame from a pyarrow.Table-compatible object."},
    {"create_temp_view", reinterpret_cast<PyCFunction>(sessionCreateTempView), METH_VARARGS,
     "Register a DataFrame or StreamingDataFrame temp view."},
    {"read_stream_csv_dir", reinterpret_cast<PyCFunction>(sessionReadStreamCsvDir),
     METH_VARARGS | METH_KEYWORDS, "Create a StreamingDataFrame from a CSV directory source."},
    {"stream_sql", reinterpret_cast<PyCFunction>(sessionStreamSql), METH_VARARGS,
     "Build a StreamingDataFrame from stream SQL."},
    {"start_stream_sql", reinterpret_cast<PyCFunction>(sessionStartStreamSql),
     METH_VARARGS | METH_KEYWORDS, "Start an INSERT INTO ... SELECT ... streaming SQL query."},
    {"explain_stream_sql", reinterpret_cast<PyCFunction>(sessionExplainStreamSql),
     METH_VARARGS | METH_KEYWORDS,
     "Explain a SELECT or INSERT INTO ... SELECT ... streaming SQL query."},
    {"vector_search", reinterpret_cast<PyCFunction>(sessionVectorSearch),
     METH_VARARGS | METH_KEYWORDS, "Run exact local vector search on a temp view."},
    {"explain_vector_search", reinterpret_cast<PyCFunction>(sessionExplainVectorSearch),
     METH_VARARGS | METH_KEYWORDS, "Explain exact local vector search strategy."},
    {nullptr, nullptr, 0, nullptr},
};

PyMethodDef dataFrameMethods[] = {
    {"to_rows", reinterpret_cast<PyCFunction>(dataFrameToRows), METH_NOARGS,
     "Materialize the DataFrame as schema plus rows."},
    {"to_arrow", reinterpret_cast<PyCFunction>(dataFrameToArrow), METH_NOARGS,
     "Materialize the DataFrame as a pyarrow.Table."},
    {"__arrow_c_array__", reinterpret_cast<PyCFunction>(dataFrameArrowCapsules),
     METH_VARARGS | METH_KEYWORDS, "Export the DataFrame through the Arrow PyCapsule interface."},
    {"count", reinterpret_cast<PyCFunction>(dataFrameCount), METH_NOARGS, "Return the row count."},
    {"explain", reinterpret_cast<PyCFunction>(dataFrameExplain), METH_NOARGS,
     "Return the logical plan explain string."},
    {"show", reinterpret_cast<PyCFunction>(dataFrameShow), METH_VARARGS | METH_KEYWORDS,
     "Print the DataFrame."},
    {nullptr, nullptr, 0, nullptr},
};

PyMethodDef streamingDataFrameMethods[] = {
    {"select", reinterpret_cast<PyCFunction>(streamSelect), METH_VARARGS,
     "Select stream columns."},
    {"filter", reinterpret_cast<PyCFunction>(streamFilter), METH_VARARGS,
     "Filter a stream by column predicate."},
    {"with_column", reinterpret_cast<PyCFunction>(streamWithColumn), METH_VARARGS,
     "Alias a stream column."},
    {"drop", reinterpret_cast<PyCFunction>(streamDrop), METH_VARARGS, "Drop a stream column."},
    {"limit", reinterpret_cast<PyCFunction>(streamLimit), METH_VARARGS,
     "Apply a stream limit transform."},
    {"window", reinterpret_cast<PyCFunction>(streamWindow), METH_VARARGS | METH_KEYWORDS,
     "Add a fixed tumbling window column."},
    {"group_sum", reinterpret_cast<PyCFunction>(streamGroupSum), METH_VARARGS | METH_KEYWORDS,
     "Group by keys and compute a streaming sum."},
    {"group_count", reinterpret_cast<PyCFunction>(streamGroupCount),
     METH_VARARGS | METH_KEYWORDS, "Group by keys and compute a streaming count."},
    {"write_stream_csv", reinterpret_cast<PyCFunction>(streamWriteStreamCsv),
     METH_VARARGS | METH_KEYWORDS, "Create a streaming query writing to a CSV sink."},
    {"write_stream_console", reinterpret_cast<PyCFunction>(streamWriteStreamConsole),
     METH_VARARGS | METH_KEYWORDS, "Create a streaming query writing to the console."},
    {nullptr, nullptr, 0, nullptr},
};

PyMethodDef streamingQueryMethods[] = {
    {"start", reinterpret_cast<PyCFunction>(queryStart), METH_NOARGS,
     "Start the streaming query."},
    {"await_termination", reinterpret_cast<PyCFunction>(queryAwaitTermination),
     METH_VARARGS | METH_KEYWORDS, "Wait for up to max_batches batches to complete."},
    {"stop", reinterpret_cast<PyCFunction>(queryStop), METH_NOARGS,
     "Stop the streaming query."},
    {"progress", reinterpret_cast<PyCFunction>(queryProgress), METH_NOARGS,
     "Return the current query progress snapshot."},
    {"snapshot_json", reinterpret_cast<PyCFunction>(querySnapshotJson), METH_NOARGS,
     "Return the current query snapshotJson() payload."},
    {nullptr, nullptr, 0, nullptr},
};

bool prepareType(PyTypeObject* type, const char* name, Py_ssize_t basicsize, destructor dealloc,
                 PyMethodDef* methods, newfunc new_fn = nullptr) {
  type->tp_name = name;
  type->tp_basicsize = basicsize;
  type->tp_flags = Py_TPFLAGS_DEFAULT;
  type->tp_doc = "Velaria Python binding type";
  type->tp_methods = methods;
  type->tp_dealloc = dealloc;
  type->tp_new = new_fn;
  return PyType_Ready(type) >= 0;
}

PyModuleDef velariaModule = {
    PyModuleDef_HEAD_INIT,
    "_velaria",
    "Velaria CPython extension.",
    -1,
    nullptr,
};

}  // namespace

PyMODINIT_FUNC PyInit__velaria(void) {
  if (!prepareType(&PyVelariaSessionType, "velaria.Session", sizeof(PyVelariaSession),
                   reinterpret_cast<destructor>(sessionDealloc), sessionMethods, sessionNew) ||
      !prepareType(&PyVelariaDataFrameType, "velaria.DataFrame", sizeof(PyVelariaDataFrame),
                   reinterpret_cast<destructor>(dataFrameDealloc), dataFrameMethods) ||
      !prepareType(&PyVelariaStreamingDataFrameType, "velaria.StreamingDataFrame",
                   sizeof(PyVelariaStreamingDataFrame),
                   reinterpret_cast<destructor>(streamingDataFrameDealloc),
                   streamingDataFrameMethods) ||
      !prepareType(&PyVelariaStreamingQueryType, "velaria.StreamingQuery",
                   sizeof(PyVelariaStreamingQuery),
                   reinterpret_cast<destructor>(streamingQueryDealloc), streamingQueryMethods)) {
    return nullptr;
  }

  PyObject* module = PyModule_Create(&velariaModule);
  if (module == nullptr) {
    return nullptr;
  }

  Py_INCREF(&PyVelariaSessionType);
  Py_INCREF(&PyVelariaDataFrameType);
  Py_INCREF(&PyVelariaStreamingDataFrameType);
  Py_INCREF(&PyVelariaStreamingQueryType);

  if (PyModule_AddObject(module, "Session", reinterpret_cast<PyObject*>(&PyVelariaSessionType)) <
          0 ||
      PyModule_AddObject(module, "DataFrame",
                         reinterpret_cast<PyObject*>(&PyVelariaDataFrameType)) < 0 ||
      PyModule_AddObject(module, "StreamingDataFrame",
                         reinterpret_cast<PyObject*>(&PyVelariaStreamingDataFrameType)) < 0 ||
      PyModule_AddObject(module, "StreamingQuery",
                         reinterpret_cast<PyObject*>(&PyVelariaStreamingQueryType)) < 0) {
    Py_DECREF(module);
    return nullptr;
  }

  return module;
}
