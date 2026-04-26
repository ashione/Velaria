#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace dataflow {
namespace sql {

struct SourcePosition {
  int line = 0;
  int column_utf8 = 0;
  int column_utf16 = 0;
};

class SourceOffsetMap {
 public:
  explicit SourceOffsetMap(std::string_view source);
  std::optional<SourcePosition> positionAt(std::size_t byte_offset) const;
  int lineCount() const { return static_cast<int>(line_starts_.size()); }

 private:
  std::vector<std::size_t> line_starts_;
  std::string source_;
};

}  // namespace sql
}  // namespace dataflow
