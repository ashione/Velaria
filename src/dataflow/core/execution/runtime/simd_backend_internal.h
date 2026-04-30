#pragma once

#include "src/dataflow/core/execution/runtime/simd_dispatch.h"

namespace dataflow {

inline bool matchesCompare(double lhs, double rhs, NumericCompareOp op) {
  switch (op) {
    case NumericCompareOp::Eq: return lhs == rhs;
    case NumericCompareOp::Ne: return lhs != rhs;
    case NumericCompareOp::Lt: return lhs < rhs;
    case NumericCompareOp::Le: return lhs <= rhs;
    case NumericCompareOp::Gt: return lhs > rhs;
    case NumericCompareOp::Ge: return lhs >= rhs;
  }
  return false;
}

inline bool matchesCompareF32(float lhs, float rhs, NumericCompareOp op) {
  switch (op) {
    case NumericCompareOp::Eq: return lhs == rhs;
    case NumericCompareOp::Ne: return lhs != rhs;
    case NumericCompareOp::Lt: return lhs < rhs;
    case NumericCompareOp::Le: return lhs <= rhs;
    case NumericCompareOp::Gt: return lhs > rhs;
    case NumericCompareOp::Ge: return lhs >= rhs;
  }
  return false;
}

const SimdKernelDispatch& scalarDispatch();
bool avx2DispatchCompiled();
const SimdKernelDispatch& avx2Dispatch();
bool neonDispatchCompiled();
const SimdKernelDispatch& neonDispatch();

}  // namespace dataflow
