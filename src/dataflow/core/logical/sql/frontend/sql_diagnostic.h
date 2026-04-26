#pragma once

#include <optional>
#include <string>
#include <vector>

namespace dataflow {
namespace sql {

struct SqlDiagnostic {
  enum class Phase {
    Parse,
    Validate,
    Lower,
    Bind,
    Optimize,
    Execute
  };

  Phase phase = Phase::Parse;
  std::string error_type;
  std::string message;
  std::optional<int> byte_offset;
  std::optional<int> line;
  std::optional<int> column_utf8;
  std::optional<int> column_utf16;
  std::string hint;
  std::vector<std::string> candidates;

  bool ok() const { return error_type.empty(); }
};

}  // namespace sql
}  // namespace dataflow
