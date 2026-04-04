#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "src/dataflow/core/execution/table.h"

namespace dataflow {

enum class MaterializationDataFormat {
  BinaryRowBatch = 0,
  NanoArrowIpc = 1,
};

struct MaterializedSourceFingerprint {
  std::string source_format;
  std::string source_options;
  std::string abs_path;
  std::uint64_t file_size = 0;
  std::uint64_t mtime_ns = 0;
};

struct SourceMaterializationEntry {
  std::string source_id;
  MaterializationDataFormat data_format = MaterializationDataFormat::BinaryRowBatch;
  MaterializedSourceFingerprint fingerprint;
  std::string metadata_path;
  std::string data_path;
};

struct SourceMaterializationOptions {
  bool enabled = false;
  std::string root;
  MaterializationDataFormat data_format = MaterializationDataFormat::BinaryRowBatch;
};

struct SourceOptions {
  SourceMaterializationOptions materialization;
};

std::string default_source_materialization_root();
MaterializationDataFormat default_source_materialization_data_format();
std::string materialization_data_format_name(MaterializationDataFormat format);
bool materialization_data_format_is_available(MaterializationDataFormat format);
MaterializedSourceFingerprint capture_file_source_fingerprint(const std::string& path,
                                                              std::string source_format,
                                                              std::string source_options);

class SourceMaterializationStore {
 public:
  explicit SourceMaterializationStore(
      std::string root,
      MaterializationDataFormat data_format = default_source_materialization_data_format());

  std::optional<SourceMaterializationEntry> lookup(
      const MaterializedSourceFingerprint& fingerprint) const;
  void save(const MaterializedSourceFingerprint& fingerprint, const Table& table) const;
  Table load(const SourceMaterializationEntry& entry, bool materialize_rows = true) const;

  MaterializationDataFormat data_format() const { return data_format_; }
  std::string source_id(const MaterializedSourceFingerprint& fingerprint) const;
  std::string metadata_path(const std::string& source_id) const;
  std::string data_path(const std::string& source_id) const {
    return data_path(source_id, data_format_);
  }
  std::string data_path(const std::string& source_id, MaterializationDataFormat data_format) const;

 private:
  std::string root_;
  MaterializationDataFormat data_format_ = MaterializationDataFormat::BinaryRowBatch;
};

}  // namespace dataflow
