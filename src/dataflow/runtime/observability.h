#pragma once

#include <chrono>
#include <cstdint>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>

namespace dataflow {
namespace observability {

inline std::string escapeJson(const std::string& input) {
  std::ostringstream out;
  for (char c : input) {
    switch (c) {
      case '\\':
        out << "\\\\";
        break;
      case '"':
        out << "\\\"";
        break;
      case '\n':
        out << "\\n";
        break;
      case '\r':
        out << "\\r";
        break;
      case '\t':
        out << "\\t";
        break;
      default:
        if (static_cast<unsigned char>(c) < 0x20) {
          out << "\\u00";
          const char hex[] = "0123456789abcdef";
          out << hex[(c >> 4) & 0x0f] << hex[c & 0x0f];
        } else {
          out << c;
        }
    }
  }
  return out.str();
}

inline std::string quote(const std::string& value) { return "\"" + escapeJson(value) + "\""; }
inline std::string boolJson(bool value) { return value ? "true" : "false"; }

inline int64_t epochMillis(std::chrono::steady_clock::time_point value) {
  if (value == std::chrono::steady_clock::time_point()) return 0;
  return std::chrono::duration_cast<std::chrono::milliseconds>(value.time_since_epoch()).count();
}

inline std::string field(const std::string& key, const std::string& value, bool already_json = false) {
  return quote(key) + ":" + (already_json ? value : quote(value));
}

inline std::string field(const std::string& key, const char* value) { return field(key, std::string(value)); }
inline std::string field(const std::string& key, bool value) { return quote(key) + ":" + boolJson(value); }
template <typename Int,
          typename = std::enable_if_t<std::is_integral_v<Int> && !std::is_same_v<Int, bool>>>
inline std::string field(const std::string& key, Int value) {
  return quote(key) + ":" + std::to_string(value);
}

inline std::string object(const std::vector<std::string>& fields) {
  std::ostringstream out;
  out << "{";
  for (size_t i = 0; i < fields.size(); ++i) {
    if (i > 0) out << ",";
    out << fields[i];
  }
  out << "}";
  return out.str();
}

inline std::string array(const std::vector<std::string>& values, bool already_json = true) {
  std::ostringstream out;
  out << "[";
  for (size_t i = 0; i < values.size(); ++i) {
    if (i > 0) out << ",";
    out << (already_json ? values[i] : quote(values[i]));
  }
  out << "]";
  return out.str();
}

inline std::string arrayFromStrings(const std::vector<std::string>& values) { return array(values, false); }

}  // namespace observability
}  // namespace dataflow
