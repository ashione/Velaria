#pragma once

#include <cstdint>
#include <iomanip>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace dataflow {

enum class DataType { Nil = 0, Bool = 1, Int64 = 2, Double = 3, String = 4, FixedVector = 5 };

class Value {
 public:
  Value() : type_(DataType::Nil), b_(false), i64_(0), d_(0.0), s_("") {}
  Value(bool v) : type_(DataType::Bool), b_(v), i64_(v ? 1 : 0), d_(v ? 1.0 : 0.0), s_("") {}
  Value(int64_t v) : type_(DataType::Int64), b_(false), i64_(v), d_(static_cast<double>(v)), s_("") {}
  Value(double v) : type_(DataType::Double), b_(false), i64_(0), d_(v), s_("") {}
  Value(const char* s) : type_(DataType::String), b_(false), i64_(0), d_(0.0), s_(s) {}
  Value(std::string s) : type_(DataType::String), b_(false), i64_(0), d_(0.0), s_(std::move(s)) {}
  Value(std::vector<float> v)
      : type_(DataType::FixedVector), b_(false), i64_(0), d_(0.0), s_(""), vec_(std::move(v)) {}

  DataType type() const { return type_; }

  bool isNull() const { return type_ == DataType::Nil; }
  bool isBool() const { return type_ == DataType::Bool; }
  bool isNumber() const { return type_ == DataType::Int64 || type_ == DataType::Double; }

  bool asBool() const {
    switch (type_) {
      case DataType::Bool:
        return b_;
      case DataType::Int64:
        return i64_ != 0;
      case DataType::Double:
        return d_ != 0.0;
      default:
        throw std::runtime_error("value is not boolean");
    }
  }

  int64_t asInt64() const {
    switch (type_) {
      case DataType::Bool:
        return b_ ? 1 : 0;
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
      case DataType::Bool:
        return b_ ? 1.0 : 0.0;
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

  const std::vector<float>& asFixedVector() const {
    if (type_ != DataType::FixedVector) {
      throw std::runtime_error("value is not fixed vector");
    }
    return vec_;
  }

  static std::vector<float> parseFixedVector(const std::string& raw) {
    std::string text = raw;
    if (!text.empty() && text.front() == '[' && text.back() == ']') {
      text = text.substr(1, text.size() - 2);
    }
    for (char& ch : text) {
      if (ch == ',') ch = ' ';
    }
    std::vector<float> out;
    std::stringstream ss(text);
    float value = 0.0f;
    while (ss >> value) {
      out.push_back(value);
    }
    return out;
  }

  std::string toString() const {
    switch (type_) {
      case DataType::Nil:
        return "null";
      case DataType::Bool:
        return b_ ? "true" : "false";
      case DataType::Int64:
        return std::to_string(i64_);
      case DataType::Double: {
        std::ostringstream oss;
        oss << std::setprecision(std::numeric_limits<double>::max_digits10) << d_;
        return oss.str();
      }
      case DataType::String:
        return s_;
      case DataType::FixedVector: {
        std::ostringstream oss;
        oss << "[";
        for (std::size_t i = 0; i < vec_.size(); ++i) {
          if (i > 0) oss << ",";
          oss << std::setprecision(std::numeric_limits<float>::max_digits10) << vec_[i];
        }
        oss << "]";
        return oss.str();
      }
    }
    return "";
  }

  bool operator==(const Value& rhs) const { return compare(rhs) == 0; }
  bool operator!=(const Value& rhs) const { return !(*this == rhs); }
  bool operator<(const Value& rhs) const { return compare(rhs) < 0; }
  bool operator>(const Value& rhs) const { return compare(rhs) > 0; }

 private:
  DataType type_;
  bool b_;
  int64_t i64_;
  double d_;
  std::string s_;
  std::vector<float> vec_;

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
      case DataType::Bool:
        return (b_ == rhs.b_) ? 0 : (b_ ? 1 : -1);
      case DataType::Int64:
        return (i64_ < rhs.i64_) ? -1 : (i64_ > rhs.i64_ ? 1 : 0);
      case DataType::Double:
        return (d_ < rhs.d_) ? -1 : (d_ > rhs.d_ ? 1 : 0);
      case DataType::String:
        return (s_ < rhs.s_) ? -1 : (s_ > rhs.s_ ? 1 : 0);
      case DataType::FixedVector: {
        if (vec_.size() != rhs.vec_.size()) {
          return vec_.size() < rhs.vec_.size() ? -1 : 1;
        }
        for (std::size_t i = 0; i < vec_.size(); ++i) {
          if (vec_[i] < rhs.vec_[i]) return -1;
          if (vec_[i] > rhs.vec_[i]) return 1;
        }
        return 0;
      }
    }
    return 0;
  }
};

}  // namespace dataflow
