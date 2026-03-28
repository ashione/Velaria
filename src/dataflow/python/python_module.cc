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

#include "src/dataflow/api/dataframe.h"
#include "src/dataflow/api/session.h"
#include "src/dataflow/core/table.h"
#include "src/dataflow/core/value.h"
#include "src/dataflow/stream/stream.h"

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
  throw std::runtime_error("value must be None, int, float, bool, or string");
}

df::Table tableFromArrowObject(PyObject* obj) {
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

  std::vector<df::Row> rows(static_cast<size_t>(row_count), df::Row(names.size(), df::Value()));
  for (size_t column = 0; column < names.size(); ++column) {
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
    for (Py_ssize_t row = 0; row < row_count; ++row) {
      rows[static_cast<size_t>(row)][column] = valueFromPy(PyList_GET_ITEM(pylist, row));
    }
    Py_DECREF(pylist);
  }

  Py_DECREF(table_obj);
  return df::Table(df::Schema(std::move(names)), std::move(rows));
}

std::vector<df::Table> tablesFromArrowObject(PyObject* obj) {
  if (PyUnicode_Check(obj) || PyBytes_Check(obj) || PyByteArray_Check(obj)) {
    return {tableFromArrowObject(obj)};
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
      tables.push_back(tableFromArrowObject(items[i]));
    }
    Py_DECREF(seq);
    return tables;
  }
  return {tableFromArrowObject(obj)};
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
  }
  Py_RETURN_NONE;
}

PyObject* pyRowsFromTable(const df::Table& table) {
  PyObject* out = PyDict_New();
  PyObject* schema = PyList_New(static_cast<Py_ssize_t>(table.schema.fields.size()));
  for (size_t i = 0; i < table.schema.fields.size(); ++i) {
    PyObject* item = PyUnicode_FromString(table.schema.fields[i].c_str());
    PyList_SET_ITEM(schema, static_cast<Py_ssize_t>(i), item);
  }
  PyObject* rows = PyList_New(static_cast<Py_ssize_t>(table.rows.size()));
  for (size_t r = 0; r < table.rows.size(); ++r) {
    const auto& row = table.rows[r];
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
  }
  return "u";
}

std::string arrowFormatForColumn(const df::Table& table, size_t column) {
  for (const auto& row : table.rows) {
    if (column < row.size() && !row[column].isNull()) {
      return arrowFormatForValue(row[column]);
    }
  }
  return "u";
}

OwnedArrowSchema* buildArrowSchema(const df::Table& table) {
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
    child->format = arrowFormatForColumn(table, i);
    child->name = table.schema.fields[i];
    child->schema.format = child->format.c_str();
    child->schema.name = child->name.c_str();
    child->schema.metadata = nullptr;
    child->schema.flags = 2;
    child->schema.n_children = 0;
    child->schema.children = nullptr;
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

std::shared_ptr<void> makeNullBitmap(const df::Table& table, size_t column, int64_t* null_count) {
  const size_t row_count = table.rows.size();
  const size_t bytes = row_count == 0 ? 0 : (row_count + 7) / 8;
  auto* raw = static_cast<uint8_t*>(std::calloc(bytes == 0 ? 1 : bytes, 1));
  int64_t local_nulls = 0;
  for (size_t row = 0; row < row_count; ++row) {
    const bool valid = column < table.rows[row].size() && !table.rows[row][column].isNull();
    if (valid) {
      raw[row / 8] |= static_cast<uint8_t>(1U << (row % 8));
    } else {
      ++local_nulls;
    }
  }
  *null_count = local_nulls;
  return std::shared_ptr<void>(raw, std::free);
}

std::shared_ptr<void> makeInt64Buffer(const df::Table& table, size_t column) {
  const size_t row_count = table.rows.size();
  auto* raw = static_cast<int64_t*>(std::calloc(row_count == 0 ? 1 : row_count, sizeof(int64_t)));
  for (size_t row = 0; row < row_count; ++row) {
    if (column < table.rows[row].size() && !table.rows[row][column].isNull()) {
      raw[row] = table.rows[row][column].asInt64();
    }
  }
  return std::shared_ptr<void>(raw, std::free);
}

std::shared_ptr<void> makeDoubleBuffer(const df::Table& table, size_t column) {
  const size_t row_count = table.rows.size();
  auto* raw = static_cast<double*>(std::calloc(row_count == 0 ? 1 : row_count, sizeof(double)));
  for (size_t row = 0; row < row_count; ++row) {
    if (column < table.rows[row].size() && !table.rows[row][column].isNull()) {
      raw[row] = table.rows[row][column].asDouble();
    }
  }
  return std::shared_ptr<void>(raw, std::free);
}

struct StringBuffers {
  std::shared_ptr<void> offsets;
  std::shared_ptr<void> data;
};

StringBuffers makeStringBuffers(const df::Table& table, size_t column) {
  const size_t row_count = table.rows.size();
  auto* offsets = static_cast<int32_t*>(
      std::calloc((row_count + 1) == 0 ? 1 : (row_count + 1), sizeof(int32_t)));
  std::ostringstream joined;
  int32_t current = 0;
  for (size_t row = 0; row < row_count; ++row) {
    offsets[row] = current;
    if (column < table.rows[row].size() && !table.rows[row][column].isNull()) {
      const std::string& s = table.rows[row][column].asString();
      joined.write(s.data(), static_cast<std::streamsize>(s.size()));
      current += static_cast<int32_t>(s.size());
    }
  }
  offsets[row_count] = current;
  std::string blob = joined.str();
  auto* data = static_cast<char*>(std::malloc(blob.empty() ? 1 : blob.size()));
  if (!blob.empty()) {
    std::memcpy(data, blob.data(), blob.size());
  }
  return {
      std::shared_ptr<void>(offsets, std::free),
      std::shared_ptr<void>(data, std::free),
  };
}

OwnedArrowArray* buildArrowArray(const df::Table& table) {
  auto* root = new OwnedArrowArray();
  root->array.length = static_cast<int64_t>(table.rows.size());
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
    child->array.length = static_cast<int64_t>(table.rows.size());
    child->array.offset = 0;
    child->array.n_children = 0;
    child->array.children = nullptr;
    child->array.dictionary = nullptr;
    child->array.release = releaseArrowArray;
    child->array.private_data = child.get();

    int64_t null_count = 0;
    auto null_bitmap = makeNullBitmap(table, column, &null_count);
    child->array.null_count = null_count;
    child->buffer_holders.push_back(null_bitmap);
    child->buffer_ptrs.push_back(null_bitmap.get());

    const std::string format = arrowFormatForColumn(table, column);
    if (format == "l") {
      auto values = makeInt64Buffer(table, column);
      child->array.n_buffers = 2;
      child->buffer_holders.push_back(values);
      child->buffer_ptrs.push_back(values.get());
    } else if (format == "g") {
      auto values = makeDoubleBuffer(table, column);
      child->array.n_buffers = 2;
      child->buffer_holders.push_back(values);
      child->buffer_ptrs.push_back(values.get());
    } else {
      auto strings = makeStringBuffers(table, column);
      child->array.n_buffers = 3;
      child->buffer_holders.push_back(strings.offsets);
      child->buffer_holders.push_back(strings.data);
      child->buffer_ptrs.push_back(strings.offsets.get());
      child->buffer_ptrs.push_back(strings.data.get());
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
  auto* owned_schema = buildArrowSchema(table);
  auto* owned_array = buildArrowArray(table);

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

df::StreamingQueryOptions parseQueryOptions(uint64_t trigger_interval_ms,
                                            const char* checkpoint_path) {
  df::StreamingQueryOptions options;
  options.trigger_interval_ms = trigger_interval_ms;
  if (checkpoint_path != nullptr) {
    options.checkpoint_path = checkpoint_path;
  }
  return options;
}

PyObject* sessionReadCsv(PyVelariaSession* self, PyObject* args, PyObject* kwargs) {
  return withExceptionTranslation([&]() -> PyObject* {
    const char* path = nullptr;
    const char* delimiter_text = ",";
    static const char* kwlist[] = {"path", "delimiter", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|s", const_cast<char**>(kwlist), &path,
                                     &delimiter_text)) {
      return nullptr;
    }
    std::unique_ptr<df::DataFrame> out;
    {
      AllowThreads allow;
      out = std::make_unique<df::DataFrame>(
          self->session->read_csv(path, parseDelimiter(delimiter_text)));
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
    static const char* kwlist[] = {"sql", "trigger_interval_ms", "checkpoint_path", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|Ks", const_cast<char**>(kwlist), &sql,
                                     &trigger_interval_ms, &checkpoint_path)) {
      return nullptr;
    }
    std::unique_ptr<df::StreamingQuery> out;
    {
      AllowThreads allow;
      out = std::make_unique<df::StreamingQuery>(self->session->startStreamSql(
          sql, parseQueryOptions(static_cast<uint64_t>(trigger_interval_ms), checkpoint_path)));
    }
    return wrapStreamingQuery(std::move(*out));
  });
}

PyObject* dataFrameToRows(PyVelariaDataFrame* self, PyObject*) {
  return withExceptionTranslation([&]() -> PyObject* {
    std::unique_ptr<df::Table> table;
    {
      AllowThreads allow;
      table = std::make_unique<df::Table>(self->df_ptr->toTable());
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
    std::unique_ptr<df::Table> table;
    {
      AllowThreads allow;
      table = std::make_unique<df::Table>(self->df_ptr->toTable());
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
        sink, parseQueryOptions(static_cast<uint64_t>(trigger_interval_ms), checkpoint_path)));
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
        parseQueryOptions(static_cast<uint64_t>(trigger_interval_ms), checkpoint_path)));
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
