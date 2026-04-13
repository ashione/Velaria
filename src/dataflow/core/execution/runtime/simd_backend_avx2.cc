#include "src/dataflow/core/execution/runtime/simd_dispatch.h"

#include "src/dataflow/core/execution/runtime/simd_backend_internal.h"

#include <algorithm>
#if defined(__AVX2__)
#include <immintrin.h>
#endif

namespace dataflow {

namespace {
const char* avx2FindByte(const char* begin, const char* end, char needle) {
#if defined(__AVX2__)
  if (begin == nullptr || end == nullptr || begin >= end) {
    return nullptr;
  }
  constexpr std::size_t kLaneCount = 32;
  const __m256i needle_vec = _mm256_set1_epi8(needle);
  const char* ptr = begin;
  for (; ptr + kLaneCount <= end; ptr += kLaneCount) {
    const __m256i bytes = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ptr));
    const __m256i matches = _mm256_cmpeq_epi8(bytes, needle_vec);
    const uint32_t mask = static_cast<uint32_t>(_mm256_movemask_epi8(matches));
    if (mask != 0) {
      return ptr + __builtin_ctz(mask);
    }
  }
  return scalarDispatch().find_byte(ptr, end, needle);
#else
  return scalarDispatch().find_byte(begin, end, needle);
#endif
}

const char* avx2FindFirstOf(const char* begin, const char* end, const char* needles,
                            std::size_t needle_count, char* matched_needle) {
#if defined(__AVX2__)
  if (begin == nullptr || end == nullptr || begin >= end || needles == nullptr ||
      needle_count == 0) {
    return nullptr;
  }
  constexpr std::size_t kLaneCount = 32;
  const std::size_t compare_count = std::min<std::size_t>(needle_count, 4);
  const __m256i needle0 = _mm256_set1_epi8(compare_count > 0 ? needles[0] : 0);
  const __m256i needle1 = _mm256_set1_epi8(compare_count > 1 ? needles[1] : 0);
  const __m256i needle2 = _mm256_set1_epi8(compare_count > 2 ? needles[2] : 0);
  const __m256i needle3 = _mm256_set1_epi8(compare_count > 3 ? needles[3] : 0);
  const char* ptr = begin;
  for (; ptr + kLaneCount <= end; ptr += kLaneCount) {
    const __m256i bytes = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ptr));
    __m256i matches = _mm256_cmpeq_epi8(bytes, needle0);
    if (compare_count > 1) {
      matches = _mm256_or_si256(matches, _mm256_cmpeq_epi8(bytes, needle1));
    }
    if (compare_count > 2) {
      matches = _mm256_or_si256(matches, _mm256_cmpeq_epi8(bytes, needle2));
    }
    if (compare_count > 3) {
      matches = _mm256_or_si256(matches, _mm256_cmpeq_epi8(bytes, needle3));
    }
    const uint32_t mask = static_cast<uint32_t>(_mm256_movemask_epi8(matches));
    if (mask != 0) {
      const char* matched_ptr = ptr + __builtin_ctz(mask);
      if (matched_needle != nullptr) {
        *matched_needle = *matched_ptr;
      }
      return matched_ptr;
    }
  }
  return scalarDispatch().find_first_of(ptr, end, needles, needle_count, matched_needle);
#else
  return scalarDispatch().find_first_of(begin, end, needles, needle_count, matched_needle);
#endif
}

NumericSelectionResult avx2SelectDouble(const double* values, const uint8_t* is_null,
                                        std::size_t row_count, double rhs,
                                        NumericCompareOp op, std::size_t max_selected) {
  return scalarDispatch().select_double(values, is_null, row_count, rhs, op, max_selected);
}

double avx2SumDouble(const double* values, const uint8_t* is_null, std::size_t row_count) {
  return scalarDispatch().sum_double(values, is_null, row_count);
}

void avx2AccumulateDouble(double* dst, const double* src, std::size_t count) {
#if defined(__AVX2__)
  std::size_t i = 0;
  constexpr std::size_t kLaneCount = 4;
  for (; i + kLaneCount <= count; i += kLaneCount) {
    const __m256d lhs = _mm256_loadu_pd(dst + i);
    const __m256d rhs = _mm256_loadu_pd(src + i);
    _mm256_storeu_pd(dst + i, _mm256_add_pd(lhs, rhs));
  }
  for (; i < count; ++i) {
    dst[i] += src[i];
  }
#else
  scalarDispatch().accumulate_double(dst, src, count);
#endif
}

void avx2CombineDouble(double* dst, const double* src, std::size_t count, NumericCombineOp op) {
#if defined(__AVX2__)
  if (op == NumericCombineOp::Sum) {
    avx2AccumulateDouble(dst, src, count);
    return;
  }
  std::size_t i = 0;
  constexpr std::size_t kLaneCount = 4;
  for (; i + kLaneCount <= count; i += kLaneCount) {
    const __m256d lhs = _mm256_loadu_pd(dst + i);
    const __m256d rhs = _mm256_loadu_pd(src + i);
    const __m256d combined = op == NumericCombineOp::Min ? _mm256_min_pd(lhs, rhs)
                                                         : _mm256_max_pd(lhs, rhs);
    _mm256_storeu_pd(dst + i, combined);
  }
  for (; i < count; ++i) {
    dst[i] = op == NumericCombineOp::Min ? std::min(dst[i], src[i]) : std::max(dst[i], src[i]);
  }
#else
  scalarDispatch().combine_double(dst, src, count, op);
#endif
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
    &avx2FindByte,
    &avx2FindFirstOf,
    &avx2SelectDouble,
    &avx2SumDouble,
    &avx2AccumulateDouble,
    &avx2CombineDouble,
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
