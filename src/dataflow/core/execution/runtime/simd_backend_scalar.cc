#include "src/dataflow/core/execution/runtime/simd_dispatch.h"

#include "src/dataflow/core/execution/runtime/simd_backend_internal.h"

#include <algorithm>
#include <cmath>

namespace dataflow {

namespace {

const char* scalarFindByte(const char* begin, const char* end, char needle) {
  if (begin == nullptr || end == nullptr || begin >= end) {
    return nullptr;
  }
  for (const char* ptr = begin; ptr < end; ++ptr) {
    if (*ptr == needle) {
      return ptr;
    }
  }
  return nullptr;
}

bool matchesCompare(double lhs, double rhs, NumericCompareOp op) {
  switch (op) {
    case NumericCompareOp::Eq:
      return lhs == rhs;
    case NumericCompareOp::Ne:
      return lhs != rhs;
    case NumericCompareOp::Lt:
      return lhs < rhs;
    case NumericCompareOp::Le:
      return lhs <= rhs;
    case NumericCompareOp::Gt:
      return lhs > rhs;
    case NumericCompareOp::Ge:
      return lhs >= rhs;
  }
  return false;
}

NumericSelectionResult scalarSelectDouble(const double* values, const uint8_t* is_null,
                                          std::size_t row_count, double rhs,
                                          NumericCompareOp op, std::size_t max_selected) {
  NumericSelectionResult out;
  out.selected.assign(row_count, 0);
  out.indices.reserve(row_count);
  const bool bounded = max_selected != 0;
  for (std::size_t i = 0; i < row_count; ++i) {
    if (is_null != nullptr && is_null[i] != 0) {
      continue;
    }
    if (!matchesCompare(values[i], rhs, op)) {
      continue;
    }
    out.selected[i] = 1;
    out.indices.push_back(i);
    ++out.selected_count;
    if (bounded && out.selected_count >= max_selected) {
      break;
    }
  }
  return out;
}

double scalarSumDouble(const double* values, const uint8_t* is_null, std::size_t row_count) {
  double sum = 0.0;
  for (std::size_t i = 0; i < row_count; ++i) {
    if (is_null != nullptr && is_null[i] != 0) {
      continue;
    }
    sum += values[i];
  }
  return sum;
}

void scalarAccumulateDouble(double* dst, const double* src, std::size_t count) {
  for (std::size_t i = 0; i < count; ++i) {
    dst[i] += src[i];
  }
}

void scalarCombineDouble(double* dst, const double* src, std::size_t count, NumericCombineOp op) {
  switch (op) {
    case NumericCombineOp::Sum:
      scalarAccumulateDouble(dst, src, count);
      return;
    case NumericCombineOp::Min:
      for (std::size_t i = 0; i < count; ++i) {
        dst[i] = std::min(dst[i], src[i]);
      }
      return;
    case NumericCombineOp::Max:
      for (std::size_t i = 0; i < count; ++i) {
        dst[i] = std::max(dst[i], src[i]);
      }
      return;
  }
}

double scalarDotF32(const float* lhs, const float* rhs, std::size_t size) {
  double dot = 0.0;
  for (std::size_t i = 0; i < size; ++i) {
    dot += static_cast<double>(lhs[i]) * static_cast<double>(rhs[i]);
  }
  return dot;
}

double scalarSquaredL2F32(const float* lhs, const float* rhs, std::size_t size) {
  double squared = 0.0;
  for (std::size_t i = 0; i < size; ++i) {
    const double diff = static_cast<double>(lhs[i]) - static_cast<double>(rhs[i]);
    squared += diff * diff;
  }
  return squared;
}

const SimdKernelDispatch kScalarDispatch = {
    SimdBackendKind::Scalar,
    simdBackendName(SimdBackendKind::Scalar),
    &scalarFindByte,
    &scalarSelectDouble,
    &scalarSumDouble,
    &scalarAccumulateDouble,
    &scalarCombineDouble,
    &scalarDotF32,
    &scalarSquaredL2F32,
};

}  // namespace

const SimdKernelDispatch& scalarDispatch() {
  return kScalarDispatch;
}

}  // namespace dataflow
