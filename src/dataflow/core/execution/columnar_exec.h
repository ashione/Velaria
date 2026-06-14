#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "src/dataflow/core/execution/columnar_batch.h"
#include "src/dataflow/core/execution/table.h"
#include "src/dataflow/core/execution/value.h"

namespace dataflow {

enum class ColumnarExecEncoding {
  Flat = 0,
  Constant = 1,
  DictionaryView = 2,
  ArrowBacked = 3,
  ValueFallback = 4,
};

struct ColumnarExecColumn {
  DataType type = DataType::Nil;
  ColumnarExecEncoding encoding = ColumnarExecEncoding::ValueFallback;
  std::size_t row_count = 0;
  bool nullable = false;
  ValueColumnView values;
  Value constant;
  std::vector<std::size_t> dictionary_indices;

  bool isNullAt(std::size_t row_index) const;
  Value valueAt(std::size_t row_index) const;
};

struct ColumnarExecBatch {
  Schema schema;
  std::vector<ColumnarExecColumn> columns;
  std::size_t row_count = 0;
  std::shared_ptr<const ColumnarTable> source_cache;
  std::string provenance;

  std::size_t columnCount() const { return columns.size(); }
  void validate(const std::string& context) const;
};

struct ColumnarExecView {
  std::shared_ptr<const ColumnarExecBatch> batch;
  std::vector<std::size_t> projected_columns;
  std::vector<std::size_t> selection;
  bool has_selection = false;

  std::size_t rowCount() const;
  std::size_t columnCount() const;
  const ColumnarExecColumn& column(std::size_t view_column_index) const;
  Value valueAt(std::size_t view_column_index, std::size_t view_row_index) const;
  Schema schema() const;
};

ColumnarExecBatch makeColumnarExecBatch(const Table& table, std::string provenance = {});
std::shared_ptr<const ColumnarExecBatch> makeSharedColumnarExecBatch(
    const Table& table, std::string provenance = {});
ColumnarExecView makeColumnarExecView(std::shared_ptr<const ColumnarExecBatch> batch,
                                      std::vector<std::size_t> projected_columns = {},
                                      std::vector<std::size_t> selection = {},
                                      bool has_selection = false);
Table materializeColumnarExecBatch(const ColumnarExecBatch& batch,
                                   bool materialize_rows = true);
Table materializeColumnarExecView(const ColumnarExecView& view,
                                  bool materialize_rows = true);

const char* columnarExecEncodingName(ColumnarExecEncoding encoding);

}  // namespace dataflow
