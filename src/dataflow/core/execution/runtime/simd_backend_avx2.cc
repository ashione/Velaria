#include "src/dataflow/core/execution/runtime/simd_dispatch.h"

#include "src/dataflow/core/execution/runtime/simd_backend_internal.h"

#if defined(__AVX2__)
#include <immintrin.h>
#endif

namespace dataflow {

namespace {
NumericSelectionResult avx2SelectDouble(const double* values, const uint8_t* is_null,
                                        std::size_t row_count, double rhs,
                                        NumericCompareOp op, std::size_t max_selected) {
  return scalarDispatch().select_double(values, is_null, row_count, rhs, op, max_selected);
}

double avx2SumDouble(const double* values, const uint8_t* is_null, std::size_t row_count) {
  return scalarDispatch().sum_double(values, is_null, row_count);
}

double avx2DotF32(const float* lhs, const float* rhs, std::size_t size) {
#if defined(__AVX2__)
  constexpr std::size_t kAvx2FloatLaneCount = 8;
  constexpr std::size_t kAvx2AlignmentBytes = 32;
  std::size_t i = 0;
  __m256 acc = _mm256_setzero_ps();
  for (; i + kAvx2FloatLaneCount <= size; i += kAvx2FloatLaneCount) {
    const __m256 lhs_vec = _mm256_loadu_ps(lhs + i);
    const __m256 rhs_vec = _mm256_loadu_ps(rhs + i);
    acc = _mm256_add_ps(acc, _mm256_mul_ps(lhs_vec, rhs_vec));
  }
  alignas(kAvx2AlignmentBytes) float lane_sum[kAvx2FloatLaneCount];
  _mm256_store_ps(lane_sum, acc);
  double dot = 0.0;
  for (float lane : lane_sum) {
    dot += static_cast<double>(lane);
  }
  for (; i < size; ++i) {
    dot += static_cast<double>(lhs[i]) * static_cast<double>(rhs[i]);
  }
  return dot;
#else
  return scalarDispatch().dot_f32(lhs, rhs, size);
#endif
}

double avx2SquaredL2F32(const float* lhs, const float* rhs, std::size_t size) {
#if defined(__AVX2__)
  constexpr std::size_t kAvx2FloatLaneCount = 8;
  constexpr std::size_t kAvx2AlignmentBytes = 32;
  std::size_t i = 0;
  __m256 acc = _mm256_setzero_ps();
  for (; i + kAvx2FloatLaneCount <= size; i += kAvx2FloatLaneCount) {
    const __m256 lhs_vec = _mm256_loadu_ps(lhs + i);
    const __m256 rhs_vec = _mm256_loadu_ps(rhs + i);
    const __m256 diff = _mm256_sub_ps(lhs_vec, rhs_vec);
    acc = _mm256_add_ps(acc, _mm256_mul_ps(diff, diff));
  }
  alignas(kAvx2AlignmentBytes) float lane_sum[kAvx2FloatLaneCount];
  _mm256_store_ps(lane_sum, acc);
  double squared = 0.0;
  for (float lane : lane_sum) {
    squared += static_cast<double>(lane);
  }
  for (; i < size; ++i) {
    const double diff = static_cast<double>(lhs[i]) - static_cast<double>(rhs[i]);
    squared += diff * diff;
  }
  return squared;
#else
  return scalarDispatch().squared_l2_f32(lhs, rhs, size);
#endif
}

const SimdKernelDispatch kAvx2Dispatch = {
    SimdBackendKind::Avx2,
    simdBackendName(SimdBackendKind::Avx2),
    &avx2SelectDouble,
    &avx2SumDouble,
    &avx2DotF32,
    &avx2SquaredL2F32,
};

}  // namespace

bool avx2DispatchCompiled() {
#if defined(__AVX2__)
  return true;
#else
  return false;
#endif
}

const SimdKernelDispatch& avx2Dispatch() {
  return kAvx2Dispatch;
}

}  // namespace dataflow
