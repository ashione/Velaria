#include "src/dataflow/core/execution/columnar_exec.h"

#include <stdexcept>
#include <utility>

namespace dataflow {

namespace {

DataType inferColumnType(const ValueColumnBuffer& column) {
  const auto row_count = valueColumnRowCount(column);
  for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
    if (!valueColumnIsNullAt(column, row_index)) {
      return valueColumnValueAt(column, row_index).type();
    }
  }
  return DataType::Nil;
}

bool inferNullable(const ValueColumnBuffer& column) {
  const auto row_count = valueColumnRowCount(column);
  for (std::size_t row_index = 0; row_index < row_count; ++row_index) {
    if (valueColumnIsNullAt(column, row_index)) {
      return true;
    }
  }
  return false;
}

std::size_t checkedIndex(const std::vector<std::size_t>& indices, std::size_t row_index,
                         std::size_t row_count, const std::string& context) {
  if (row_index >= indices.size()) {
    throw std::out_of_range(context + ": row index out of range");
  }
  const std::size_t mapped = indices[row_index];
  if (mapped >= row_count) {
    throw std::out_of_range(context + ": mapped row index out of range");
  }
  return mapped;
}

}  // namespace

bool ColumnarExecColumn::isNullAt(std::size_t row_index) const {
  if (row_index >= row_count) {
    throw std::out_of_range("ColumnarExecColumn::isNullAt row index out of range");
  }
  switch (encoding) {
    case ColumnarExecEncoding::Constant:
      return constant.isNull();
    case ColumnarExecEncoding::DictionaryView: {
      if (values.buffer == nullptr) {
        throw std::runtime_error("ColumnarExecColumn::isNullAt missing dictionary value buffer");
      }
      const std::size_t mapped =
          checkedIndex(dictionary_indices, row_index, valueColumnRowCount(*values.buffer),
                       "ColumnarExecColumn::isNullAt dictionary view");
      return valueColumnIsNullAt(*values.buffer, mapped);
    }
    case ColumnarExecEncoding::Flat:
    case ColumnarExecEncoding::ArrowBacked:
    case ColumnarExecEncoding::ValueFallback:
      if (values.buffer == nullptr) {
        throw std::runtime_error("ColumnarExecColumn::isNullAt missing value buffer");
      }
      return valueColumnIsNullAt(*values.buffer, row_index);
  }
  return true;
}

Value ColumnarExecColumn::valueAt(std::size_t row_index) const {
  if (row_index >= row_count) {
    throw std::out_of_range("ColumnarExecColumn::valueAt row index out of range");
  }
  switch (encoding) {
    case ColumnarExecEncoding::Constant:
      return constant;
    case ColumnarExecEncoding::DictionaryView: {
      if (values.buffer == nullptr) {
        throw std::runtime_error("ColumnarExecColumn::valueAt missing dictionary value buffer");
      }
      const std::size_t mapped =
          checkedIndex(dictionary_indices, row_index, valueColumnRowCount(*values.buffer),
                       "ColumnarExecColumn::valueAt dictionary view");
      return valueColumnValueAt(*values.buffer, mapped);
    }
    case ColumnarExecEncoding::Flat:
    case ColumnarExecEncoding::ArrowBacked:
    case ColumnarExecEncoding::ValueFallback:
      if (values.buffer == nullptr) {
        throw std::runtime_error("ColumnarExecColumn::valueAt missing value buffer");
      }
      return valueColumnValueAt(*values.buffer, row_index);
  }
  return Value();
}

void ColumnarExecBatch::validate(const std::string& context) const {
  if (schema.fields.size() != columns.size()) {
    throw std::runtime_error(context + ": column count does not match schema");
  }
  for (std::size_t i = 0; i < columns.size(); ++i) {
    const auto& column = columns[i];
    if (column.row_count != row_count) {
      throw std::runtime_error(context + ": column row count mismatch");
    }
    if (column.encoding == ColumnarExecEncoding::DictionaryView &&
        column.dictionary_indices.size() != row_count) {
      throw std::runtime_error(context + ": dictionary view index count mismatch");
    }
    if (column.encoding != ColumnarExecEncoding::Constant && column.values.buffer == nullptr) {
      throw std::runtime_error(context + ": column missing value buffer");
    }
  }
}

std::size_t ColumnarExecView::rowCount() const {
  if (!batch) {
    return 0;
  }
  return has_selection ? selection.size() : batch->row_count;
}

std::size_t ColumnarExecView::columnCount() const {
  if (!batch) {
    return 0;
  }
  return projected_columns.empty() ? batch->columnCount() : projected_columns.size();
}

const ColumnarExecColumn& ColumnarExecView::column(std::size_t view_column_index) const {
  if (!batch) {
    throw std::runtime_error("ColumnarExecView::column missing batch");
  }
  if (view_column_index >= columnCount()) {
    throw std::out_of_range("ColumnarExecView::column column index out of range");
  }
  const std::size_t batch_column =
      projected_columns.empty() ? view_column_index : projected_columns[view_column_index];
  if (batch_column >= batch->columns.size()) {
    throw std::out_of_range("ColumnarExecView::column mapped column index out of range");
  }
  return batch->columns[batch_column];
}

Value ColumnarExecView::valueAt(std::size_t view_column_index, std::size_t view_row_index) const {
  if (!batch) {
    throw std::runtime_error("ColumnarExecView::valueAt missing batch");
  }
  if (view_row_index >= rowCount()) {
    throw std::out_of_range("ColumnarExecView::valueAt row index out of range");
  }
  const std::size_t batch_row =
      has_selection ? checkedIndex(selection, view_row_index, batch->row_count,
                                   "ColumnarExecView::valueAt selection")
                    : view_row_index;
  return column(view_column_index).valueAt(batch_row);
}

Schema ColumnarExecView::schema() const {
  if (!batch) {
    return Schema();
  }
  if (projected_columns.empty()) {
    return batch->schema;
  }
  std::vector<std::string> fields;
  fields.reserve(projected_columns.size());
  for (const auto column_index : projected_columns) {
    if (column_index >= batch->schema.fields.size()) {
      throw std::out_of_range("ColumnarExecView::schema projected column index out of range");
    }
    fields.push_back(batch->schema.fields[column_index]);
  }
  return Schema(std::move(fields));
}

ColumnarExecBatch makeColumnarExecBatch(const Table& table, std::string provenance) {
  ColumnarExecBatch batch;
  batch.schema = table.schema;
  batch.row_count = table.rowCount();
  batch.source_cache = ensureColumnarCache(&table);
  batch.provenance = std::move(provenance);
  batch.columns.reserve(batch.source_cache->columns.size());
  for (std::size_t i = 0; i < batch.source_cache->columns.size(); ++i) {
    const auto& value_column = batch.source_cache->columns[i];
    ColumnarExecColumn column;
    column.type = inferColumnType(value_column);
    column.row_count = batch.row_count;
    column.nullable = inferNullable(value_column);
    column.values.owner = batch.source_cache;
    column.values.buffer = &value_column;
    column.encoding = value_column.arrow_backing != nullptr ? ColumnarExecEncoding::ArrowBacked
                                                            : ColumnarExecEncoding::Flat;
    batch.columns.push_back(std::move(column));
  }
  batch.validate("makeColumnarExecBatch");
  return batch;
}

std::shared_ptr<const ColumnarExecBatch> makeSharedColumnarExecBatch(const Table& table,
                                                                    std::string provenance) {
  return std::make_shared<ColumnarExecBatch>(
      makeColumnarExecBatch(table, std::move(provenance)));
}

ColumnarExecView makeColumnarExecView(std::shared_ptr<const ColumnarExecBatch> batch,
                                      std::vector<std::size_t> projected_columns,
                                      std::vector<std::size_t> selection,
                                      bool has_selection) {
  ColumnarExecView view;
  view.batch = std::move(batch);
  view.projected_columns = std::move(projected_columns);
  view.selection = std::move(selection);
  view.has_selection = has_selection || !view.selection.empty();
  if (view.batch) {
    for (const auto column_index : view.projected_columns) {
      if (column_index >= view.batch->columns.size()) {
        throw std::out_of_range("makeColumnarExecView projected column index out of range");
      }
    }
    for (const auto row_index : view.selection) {
      if (row_index >= view.batch->row_count) {
        throw std::out_of_range("makeColumnarExecView selection row index out of range");
      }
    }
  }
  return view;
}

Table materializeColumnarExecBatch(const ColumnarExecBatch& batch, bool materialize_rows) {
  batch.validate("materializeColumnarExecBatch");
  Table table;
  table.schema = batch.schema;
  auto cache = std::make_shared<ColumnarTable>();
  cache->schema = batch.schema;
  cache->row_count = batch.row_count;
  cache->columns.reserve(batch.columns.size());
  for (const auto& column : batch.columns) {
    ValueColumnBuffer out;
    out.values.reserve(batch.row_count);
    for (std::size_t row_index = 0; row_index < batch.row_count; ++row_index) {
      out.values.push_back(column.valueAt(row_index));
    }
    cache->columns.push_back(std::move(out));
  }
  table.columnar_cache = std::move(cache);
  if (materialize_rows) {
    materializeRows(&table);
  }
  return table;
}

Table materializeColumnarExecView(const ColumnarExecView& view, bool materialize_rows) {
  Table table;
  table.schema = view.schema();
  auto cache = std::make_shared<ColumnarTable>();
  cache->schema = table.schema;
  cache->row_count = view.rowCount();
  cache->columns.reserve(view.columnCount());
  for (std::size_t column_index = 0; column_index < view.columnCount(); ++column_index) {
    ValueColumnBuffer out;
    out.values.reserve(view.rowCount());
    for (std::size_t row_index = 0; row_index < view.rowCount(); ++row_index) {
      out.values.push_back(view.valueAt(column_index, row_index));
    }
    cache->columns.push_back(std::move(out));
  }
  table.columnar_cache = std::move(cache);
  if (materialize_rows) {
    materializeRows(&table);
  }
  return table;
}

const char* columnarExecEncodingName(ColumnarExecEncoding encoding) {
  switch (encoding) {
    case ColumnarExecEncoding::Flat:
      return "flat";
    case ColumnarExecEncoding::Constant:
      return "constant";
    case ColumnarExecEncoding::DictionaryView:
      return "dictionary-view";
    case ColumnarExecEncoding::ArrowBacked:
      return "arrow-backed";
    case ColumnarExecEncoding::ValueFallback:
      return "value-fallback";
  }
  return "value-fallback";
}

}  // namespace dataflow
