#pragma once

#include <string_view>

namespace dataflow {

inline constexpr char kArrowFormatNull[] = "n";
inline constexpr char kArrowFormatBool[] = "b";
inline constexpr char kArrowFormatInt32[] = "i";
inline constexpr char kArrowFormatUInt32[] = "I";
inline constexpr char kArrowFormatInt64[] = "l";
inline constexpr char kArrowFormatUInt64[] = "L";
inline constexpr char kArrowFormatFloat32[] = "f";
inline constexpr char kArrowFormatFloat64[] = "g";
inline constexpr char kArrowFormatUtf8[] = "u";
inline constexpr char kArrowFormatLargeUtf8[] = "U";
inline constexpr char kArrowFormatFixedSizeListPrefix[] = "+w:";

inline bool isArrowUtf8Format(std::string_view format) {
  return format == kArrowFormatUtf8 || format == kArrowFormatLargeUtf8;
}

inline bool isArrowIntegerLikeFormat(std::string_view format) {
  return format == kArrowFormatBool || format == kArrowFormatInt32 ||
         format == kArrowFormatUInt32 || format == kArrowFormatInt64 ||
         format == kArrowFormatUInt64;
}

inline bool isArrowPrimitiveNumericFormat(std::string_view format) {
  return isArrowIntegerLikeFormat(format) || format == kArrowFormatFloat32 ||
         format == kArrowFormatFloat64;
}

inline bool isArrowFixedSizeListFormat(std::string_view format) {
  return format.rfind(kArrowFormatFixedSizeListPrefix, 0) == 0;
}

}  // namespace dataflow
