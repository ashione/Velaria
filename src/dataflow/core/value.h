#pragma once

#include <cstdint>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>

namespace dataflow {

enum class DataType { Nil = 0, Int64 = 1, Double = 2, String = 3 };

class Value {
 public:
  Value() : type_(DataType::Nil), i64_(0), d_(0.0), s_("") {}
  Value(int64_t v) : type_(DataType::Int64), i64_(v), d_(static_cast<double>(v)), s_("") {}
  Value(double v) : type_(DataType::Double), i64_(0), d_(v), s_("") {}
  Value(const char* s) : type_(DataType::String), i64_(0), d_(0.0), s_(s) {}
  Value(std::string s) : type_(DataType::String), i64_(0), d_(0.0), s_(std::move(s)) {}

  DataType type() const { return type_; }

  bool isNull() const { return type_ == DataType::Nil; }
  bool isNumber() const { return type_ == DataType::Int64 || type_ == DataType::Double; }

  int64_t asInt64() const {
    switch (type_) {
      case DataType::Int64:
        return i64_;
      case DataType::Double:
        return static_cast<int64_t>(d_);
      default:
        throw std::runtime_error("value is not numeric");
    }
  }

  double asDouble() const {
    switch (type_) {
      case DataType::Int64:
        return static_cast<double>(i64_);
      case DataType::Double:
        return d_;
      default:
        throw std::runtime_error("value is not numeric");
    }
  }

  const std::string& asString() const {
    if (type_ != DataType::String) {
      throw std::runtime_error("value is not string");
    }
    return s_;
  }

  std::string toString() const {
    switch (type_) {
      case DataType::Nil:
        return "null";
      case DataType::Int64:
        return std::to_string(i64_);
      case DataType::Double: {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(6) << d_;
        return oss.str();
      }
      case DataType::String:
        return s_;
    }
    return "";
  }

  bool operator==(const Value& rhs) const { return compare(rhs) == 0; }
  bool operator!=(const Value& rhs) const { return !(*this == rhs); }
  bool operator<(const Value& rhs) const { return compare(rhs) < 0; }
  bool operator>(const Value& rhs) const { return compare(rhs) > 0; }

 private:
  DataType type_;
  int64_t i64_;
  double d_;
  std::string s_;

  int compare(const Value& rhs) const {
    if (type_ != rhs.type_) {
      if (isNumber() && rhs.isNumber()) {
        const auto lhs = asDouble();
        const auto rr = rhs.asDouble();
        return (lhs < rr) ? -1 : (lhs > rr ? 1 : 0);
      }
      throw std::runtime_error("type mismatch in compare");
    }
    switch (type_) {
      case DataType::Nil:
        return 0;
      case DataType::Int64:
        return (i64_ < rhs.i64_) ? -1 : (i64_ > rhs.i64_ ? 1 : 0);
      case DataType::Double:
        return (d_ < rhs.d_) ? -1 : (d_ > rhs.d_ ? 1 : 0);
      case DataType::String:
        return (s_ < rhs.s_) ? -1 : (s_ > rhs.s_ ? 1 : 0);
    }
    return 0;
  }
};

}  // namespace dataflow
