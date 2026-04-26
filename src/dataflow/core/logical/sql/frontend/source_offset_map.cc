#include "src/dataflow/core/logical/sql/frontend/source_offset_map.h"
#include <algorithm>
#include <cstdint>

namespace dataflow {
namespace sql {

namespace {
int utf16CodeUnits(char32_t cp) { return (cp >= 0x10000) ? 2 : 1; }

char32_t decodeUtf8(const char*& p, const char* end) {
  unsigned char c = static_cast<unsigned char>(*p);
  if (c < 0x80) { p++; return c; }
  std::size_t len = 0;
  char32_t cp = 0;
  if ((c & 0xE0) == 0xC0) { len = 2; cp = c & 0x1F; }
  else if ((c & 0xF0) == 0xE0) { len = 3; cp = c & 0x0F; }
  else if ((c & 0xF8) == 0xF0) { len = 4; cp = c & 0x07; }
  else { p++; return 0xFFFD; }
  p++;
  for (std::size_t i = 1; i < len && p < end; i++) {
    c = static_cast<unsigned char>(*p);
    if ((c & 0xC0) != 0x80) break;
    cp = (cp << 6) | (c & 0x3F);
    p++;
  }
  return cp;
}
}  // namespace

SourceOffsetMap::SourceOffsetMap(std::string_view source) : source_(source) {
  line_starts_.push_back(0);
  for (std::size_t i = 0; i < source.size(); ++i) {
    if (source[i] == '\n') line_starts_.push_back(i + 1);
  }
}

std::optional<SourcePosition> SourceOffsetMap::positionAt(std::size_t byte_offset) const {
  if (byte_offset > source_.size()) return std::nullopt;
  auto it = std::upper_bound(line_starts_.begin(), line_starts_.end(), byte_offset);
  int line = static_cast<int>(it - line_starts_.begin());
  std::size_t line_start = *(it - 1);
  int col_utf8 = 1, col_utf16 = 1;
  const char* p = source_.data() + line_start;
  const char* end = source_.data() + byte_offset;
  while (p < end) {
    char32_t cp = decodeUtf8(p, end);
    col_utf8++;
    col_utf16 += utf16CodeUnits(cp);
  }
  return SourcePosition{line, col_utf8, col_utf16};
}

}  // namespace sql
}  // namespace dataflow
