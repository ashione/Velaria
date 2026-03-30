#include "src/dataflow/core/execution/source_materialization.h"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <system_error>
#include <vector>

#include "src/dataflow/core/execution/stream/binary_row_batch.h"

namespace dataflow {

namespace {

constexpr const char* kRootEnv = "VELARIA_MATERIALIZATION_DIR";
constexpr const char* kDataFormatEnv = "VELARIA_MATERIALIZATION_FORMAT";
constexpr const char* kMetadataFile = "meta.txt";

std::uint64_t fnv1a64(const std::string& input) {
  std::uint64_t hash = 1469598103934665603ULL;
  for (unsigned char ch : input) {
    hash ^= static_cast<std::uint64_t>(ch);
    hash *= 1099511628211ULL;
  }
  return hash;
}

std::string hex64(std::uint64_t value) {
  std::ostringstream out;
  out << std::hex;
  out.width(16);
  out.fill('0');
  out << value;
  return out.str();
}

std::uint64_t stat_mtime_ns(const struct stat& info) {
#if defined(__APPLE__)
  return static_cast<std::uint64_t>(info.st_mtimespec.tv_sec) * 1000000000ULL +
         static_cast<std::uint64_t>(info.st_mtimespec.tv_nsec);
#elif defined(_WIN32)
  return static_cast<std::uint64_t>(info.st_mtime) * 1000000000ULL;
#else
  return static_cast<std::uint64_t>(info.st_mtim.tv_sec) * 1000000000ULL +
         static_cast<std::uint64_t>(info.st_mtim.tv_nsec);
#endif
}

std::optional<MaterializationDataFormat> parse_materialization_data_format(
    const std::string& value) {
  if (value == "binary" || value == "binary_row_batch") {
    return MaterializationDataFormat::BinaryRowBatch;
  }
  if (value == "nanoarrow" || value == "nanoarrow_ipc") {
    return MaterializationDataFormat::NanoArrowIpc;
  }
  return std::nullopt;
}

std::string data_file_name(MaterializationDataFormat format) {
  switch (format) {
    case MaterializationDataFormat::BinaryRowBatch:
      return "table.bin";
    case MaterializationDataFormat::NanoArrowIpc:
      return "table.nanoarrow";
  }
  return "table.bin";
}

std::optional<SourceMaterializationEntry> read_entry(const std::string& metadata_path,
                                                     const std::string& root,
                                                     const std::string& source_id) {
  std::ifstream in(metadata_path);
  if (!in.is_open()) {
    return std::nullopt;
  }

  SourceMaterializationEntry entry;
  entry.metadata_path = metadata_path;

  std::string line;
  while (std::getline(in, line)) {
    const auto pos = line.find('=');
    if (pos == std::string::npos) {
      continue;
    }
    const auto key = line.substr(0, pos);
    const auto value = line.substr(pos + 1);
    if (key == "source_id") {
      entry.source_id = value;
    } else if (key == "data_format") {
      if (const auto parsed = parse_materialization_data_format(value)) {
        entry.data_format = *parsed;
      } else {
        return std::nullopt;
      }
    } else if (key == "source_format") {
      entry.fingerprint.source_format = value;
    } else if (key == "source_options") {
      entry.fingerprint.source_options = value;
    } else if (key == "abs_path") {
      entry.fingerprint.abs_path = value;
    } else if (key == "file_size") {
      entry.fingerprint.file_size = static_cast<std::uint64_t>(std::stoull(value));
    } else if (key == "mtime_ns") {
      entry.fingerprint.mtime_ns = static_cast<std::uint64_t>(std::stoull(value));
    }
  }

  if (entry.source_id.empty() || entry.fingerprint.source_format.empty() ||
      entry.fingerprint.abs_path.empty()) {
    return std::nullopt;
  }
  if (entry.source_id != source_id) {
    return std::nullopt;
  }
  entry.data_path =
      (std::filesystem::path(root) / source_id / data_file_name(entry.data_format)).string();
  return entry;
}

void write_atomic_text(const std::string& path, const std::string& content) {
  namespace fs = std::filesystem;
  const fs::path target(path);
  const fs::path tmp = target.string() + ".tmp";
  {
    std::ofstream out(tmp);
    if (!out.is_open()) {
      throw std::runtime_error("cannot open metadata temp file: " + tmp.string());
    }
    out << content;
  }
  std::error_code ec;
  fs::rename(tmp, target, ec);
  if (ec) {
    fs::remove(target, ec);
    ec.clear();
    fs::rename(tmp, target, ec);
    if (ec) {
      throw std::runtime_error("cannot atomically publish metadata file: " + path);
    }
  }
}

std::vector<uint8_t> read_binary_file(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in.is_open()) {
    throw std::runtime_error("cannot open materialized data file: " + path);
  }
  in.seekg(0, std::ios::end);
  const auto end = in.tellg();
  if (end < 0) {
    throw std::runtime_error("cannot size materialized data file: " + path);
  }
  std::vector<uint8_t> payload(static_cast<std::size_t>(end));
  in.seekg(0, std::ios::beg);
  if (!payload.empty() &&
      !in.read(reinterpret_cast<char*>(payload.data()), static_cast<std::streamsize>(payload.size()))) {
    throw std::runtime_error("cannot read materialized data file: " + path);
  }
  return payload;
}

void write_atomic_binary(const std::string& path, const std::vector<uint8_t>& payload) {
  namespace fs = std::filesystem;
  const fs::path target(path);
  const fs::path tmp = target.string() + ".tmp";
  {
    std::ofstream out(tmp, std::ios::binary);
    if (!out.is_open()) {
      throw std::runtime_error("cannot open materialized data temp file: " + tmp.string());
    }
    if (!payload.empty()) {
      out.write(reinterpret_cast<const char*>(payload.data()),
                static_cast<std::streamsize>(payload.size()));
      if (!out.good()) {
        throw std::runtime_error("cannot write materialized data temp file: " + tmp.string());
      }
    }
  }
  std::error_code ec;
  fs::rename(tmp, target, ec);
  if (ec) {
    fs::remove(target, ec);
    ec.clear();
    fs::rename(tmp, target, ec);
    if (ec) {
      throw std::runtime_error("cannot atomically publish materialized data file: " + path);
    }
  }
}

void serialize_materialized_table(MaterializationDataFormat format, const Table& table,
                                  const std::string& path) {
  switch (format) {
    case MaterializationDataFormat::BinaryRowBatch: {
      BinaryRowBatchCodec codec;
      std::vector<uint8_t> payload;
      codec.serialize(table, &payload);
      write_atomic_binary(path, payload);
      return;
    }
    case MaterializationDataFormat::NanoArrowIpc:
      throw std::runtime_error(
          "materialization data format nanoarrow_ipc is not wired yet");
  }
}

Table deserialize_materialized_table(MaterializationDataFormat format,
                                     const std::string& path) {
  switch (format) {
    case MaterializationDataFormat::BinaryRowBatch: {
      const auto payload = read_binary_file(path);
      BinaryRowBatchCodec codec;
      return codec.deserialize(payload);
    }
    case MaterializationDataFormat::NanoArrowIpc:
      throw std::runtime_error(
          "materialization data format nanoarrow_ipc is not wired yet");
  }
  throw std::runtime_error("unsupported materialization data format");
}

}  // namespace

std::string default_source_materialization_root() {
  if (const char* env = std::getenv(kRootEnv)) {
    if (env[0] != '\0') {
      return env;
    }
  }
  return (std::filesystem::temp_directory_path() / "velaria_source_materialization_cache")
      .string();
}

MaterializationDataFormat default_source_materialization_data_format() {
  if (const char* env = std::getenv(kDataFormatEnv)) {
    if (env[0] != '\0') {
      if (const auto parsed = parse_materialization_data_format(env)) {
        return *parsed;
      }
    }
  }
  return MaterializationDataFormat::BinaryRowBatch;
}

std::string materialization_data_format_name(MaterializationDataFormat format) {
  switch (format) {
    case MaterializationDataFormat::BinaryRowBatch:
      return "binary_row_batch";
    case MaterializationDataFormat::NanoArrowIpc:
      return "nanoarrow_ipc";
  }
  return "binary_row_batch";
}

bool materialization_data_format_is_available(MaterializationDataFormat format) {
  switch (format) {
    case MaterializationDataFormat::BinaryRowBatch:
      return true;
    case MaterializationDataFormat::NanoArrowIpc:
      return false;
  }
  return false;
}

MaterializedSourceFingerprint capture_file_source_fingerprint(const std::string& path,
                                                              std::string source_format,
                                                              std::string source_options) {
  namespace fs = std::filesystem;
  const auto abs_path = fs::absolute(path).string();
  struct stat info {};
  if (::stat(abs_path.c_str(), &info) != 0) {
    throw std::runtime_error("cannot stat source file: " + abs_path);
  }

  MaterializedSourceFingerprint fingerprint;
  fingerprint.source_format = std::move(source_format);
  fingerprint.source_options = std::move(source_options);
  fingerprint.abs_path = abs_path;
  fingerprint.file_size = static_cast<std::uint64_t>(info.st_size);
  fingerprint.mtime_ns = stat_mtime_ns(info);
  return fingerprint;
}

SourceMaterializationStore::SourceMaterializationStore(std::string root,
                                                       MaterializationDataFormat data_format)
    : root_(std::move(root)), data_format_(data_format) {}

std::string SourceMaterializationStore::source_id(
    const MaterializedSourceFingerprint& fingerprint) const {
  std::string key = fingerprint.source_format;
  key.push_back('\0');
  key += fingerprint.source_options;
  key.push_back('\0');
  key += fingerprint.abs_path;
  return hex64(fnv1a64(key));
}

std::string SourceMaterializationStore::metadata_path(const std::string& source_id) const {
  return (std::filesystem::path(root_) / source_id / kMetadataFile).string();
}

std::string SourceMaterializationStore::data_path(const std::string& source_id,
                                                  MaterializationDataFormat data_format) const {
  return (std::filesystem::path(root_) / source_id / data_file_name(data_format)).string();
}

std::optional<SourceMaterializationEntry> SourceMaterializationStore::lookup(
    const MaterializedSourceFingerprint& fingerprint) const {
  const auto id = source_id(fingerprint);
  auto entry = read_entry(metadata_path(id), root_, id);
  if (!entry.has_value()) {
    return std::nullopt;
  }
  if (entry->source_id != id || entry->data_format != data_format_) {
    return std::nullopt;
  }
  if (entry->fingerprint.source_format != fingerprint.source_format ||
      entry->fingerprint.source_options != fingerprint.source_options ||
      entry->fingerprint.abs_path != fingerprint.abs_path ||
      entry->fingerprint.file_size != fingerprint.file_size ||
      entry->fingerprint.mtime_ns != fingerprint.mtime_ns) {
    return std::nullopt;
  }
  if (!std::filesystem::exists(entry->data_path)) {
    return std::nullopt;
  }
  return entry;
}

void SourceMaterializationStore::save(const MaterializedSourceFingerprint& fingerprint,
                                      const Table& table) const {
  namespace fs = std::filesystem;
  const auto id = source_id(fingerprint);
  const fs::path dir = fs::path(root_) / id;
  std::error_code ec;
  fs::create_directories(dir, ec);

  serialize_materialized_table(data_format_, table, data_path(id, data_format_));

  std::ostringstream metadata;
  metadata << "source_id=" << id << "\n";
  metadata << "data_format=" << materialization_data_format_name(data_format_) << "\n";
  metadata << "source_format=" << fingerprint.source_format << "\n";
  metadata << "source_options=" << fingerprint.source_options << "\n";
  metadata << "abs_path=" << fingerprint.abs_path << "\n";
  metadata << "file_size=" << fingerprint.file_size << "\n";
  metadata << "mtime_ns=" << fingerprint.mtime_ns << "\n";
  write_atomic_text(metadata_path(id), metadata.str());
}

Table SourceMaterializationStore::load(const SourceMaterializationEntry& entry) const {
  return deserialize_materialized_table(entry.data_format, entry.data_path);
}

}  // namespace dataflow
