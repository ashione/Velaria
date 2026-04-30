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
#if defined(__AVX2__)
  NumericSelectionResult out;
  out.selected.assign(row_count, 0);
  out.indices.reserve(row_count);
  const bool bounded = max_selected != 0;
  const __m256d rhs_vec = _mm256_set1_pd(rhs);
  std::size_t i = 0;
  constexpr std::size_t kLaneCount = 4;

  for (; i + kLaneCount <= row_count; i += kLaneCount) {
    const __m256d vals = _mm256_loadu_pd(values + i);
    int cmp_mask = 0;
    switch (op) {
      case NumericCompareOp::Eq:
        cmp_mask = _mm256_movemask_pd(_mm256_cmp_pd(vals, rhs_vec, _CMP_EQ_OQ));
        break;
      case NumericCompareOp::Ne:
        cmp_mask = _mm256_movemask_pd(_mm256_cmp_pd(vals, rhs_vec, _CMP_NEQ_OQ));
        break;
      case NumericCompareOp::Lt:
        cmp_mask = _mm256_movemask_pd(_mm256_cmp_pd(vals, rhs_vec, _CMP_LT_OQ));
        break;
      case NumericCompareOp::Le:
        cmp_mask = _mm256_movemask_pd(_mm256_cmp_pd(vals, rhs_vec, _CMP_LE_OQ));
        break;
      case NumericCompareOp::Gt:
        cmp_mask = _mm256_movemask_pd(_mm256_cmp_pd(vals, rhs_vec, _CMP_GT_OQ));
        break;
      case NumericCompareOp::Ge:
        cmp_mask = _mm256_movemask_pd(_mm256_cmp_pd(vals, rhs_vec, _CMP_GE_OQ));
        break;
    }
    if (cmp_mask != 0) {
      for (int j = 0; j < static_cast<int>(kLaneCount); ++j) {
        if ((cmp_mask >> j) & 1) {
          if (is_null != nullptr && is_null[i + j] != 0) continue;
          out.selected[i + j] = 1;
          out.indices.push_back(i + j);
          ++out.selected_count;
          if (bounded && out.selected_count >= max_selected) goto scalar_tail;
        }
      }
    }
  }

scalar_tail:
  for (; i < row_count; ++i) {
    if (is_null != nullptr && is_null[i] != 0) continue;
    if (!matchesCompare(values[i], rhs, op)) continue;
    out.selected[i] = 1;
    out.indices.push_back(i);
    ++out.selected_count;
    if (bounded && out.selected_count >= max_selected) break;
  }
  return out;
#else
  return scalarDispatch().select_double(values, is_null, row_count, rhs, op, max_selected);
#endif
}

double avx2SumDouble(const double* values, const uint8_t* is_null, std::size_t row_count) {
#if defined(__AVX2__)
  if (is_null == nullptr) {
    __m256d acc = _mm256_setzero_pd();
    std::size_t i = 0;
    constexpr std::size_t kLaneCount = 4;
    for (; i + kLaneCount <= row_count; i += kLaneCount) {
      acc = _mm256_add_pd(acc, _mm256_loadu_pd(values + i));
    }
    __m128d lo = _mm256_castpd256_pd128(acc);
    __m128d hi = _mm256_extractf128_pd(acc, 1);
    __m128d sum128 = _mm_add_pd(lo, hi);
    double result = _mm_cvtsd_f64(_mm_hadd_pd(sum128, sum128));
    for (; i < row_count; ++i) result += values[i];
    return result;
  }
  return scalarDispatch().sum_double(values, is_null, row_count);
#else
  return scalarDispatch().sum_double(values, is_null, row_count);
#endif
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
  __m256 hadd = _mm256_hadd_ps(acc, acc);
  __m256 hadd2 = _mm256_hadd_ps(hadd, hadd);
  __m128 lo = _mm256_castps256_ps128(hadd2);
  __m128 hi = _mm256_extractf128_ps(hadd2, 1);
  double dot = static_cast<double>(_mm_cvtss_f32(_mm_add_ss(lo, hi)));
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
  __m256 hadd = _mm256_hadd_ps(acc, acc);
  __m256 hadd2 = _mm256_hadd_ps(hadd, hadd);
  __m128 lo = _mm256_castps256_ps128(hadd2);
  __m128 hi = _mm256_extractf128_ps(hadd2, 1);
  double squared = static_cast<double>(_mm_cvtss_f32(_mm_add_ss(lo, hi)));
  for (; i < size; ++i) {
    const double diff = static_cast<double>(lhs[i]) - static_cast<double>(rhs[i]);
    squared += diff * diff;
  }
  return squared;
#else
  return scalarDispatch().squared_l2_f32(lhs, rhs, size);
#endif
}


NumericSelectionResult avx2SelectFloat(const float* values, const uint8_t* is_null,
                                       std::size_t row_count, float rhs,
                                       NumericCompareOp op, std::size_t max_selected) {
#if defined(__AVX2__)
  NumericSelectionResult out;
  out.selected.assign(row_count, 0);
  out.indices.reserve(row_count);
  const bool bounded = max_selected != 0;
  const __m256 rhs_vec = _mm256_set1_ps(rhs);
  std::size_t i = 0;
  constexpr std::size_t kLaneCount = 8;

  for (; i + kLaneCount <= row_count; i += kLaneCount) {
    const __m256 vals = _mm256_loadu_ps(values + i);
    int cmp_mask = 0;
    switch (op) {
      case NumericCompareOp::Eq:
        cmp_mask = _mm256_movemask_ps(_mm256_cmp_ps(vals, rhs_vec, _CMP_EQ_OQ));
        break;
      case NumericCompareOp::Ne:
        cmp_mask = _mm256_movemask_ps(_mm256_cmp_ps(vals, rhs_vec, _CMP_NEQ_OQ));
        break;
      case NumericCompareOp::Lt:
        cmp_mask = _mm256_movemask_ps(_mm256_cmp_ps(vals, rhs_vec, _CMP_LT_OQ));
        break;
      case NumericCompareOp::Le:
        cmp_mask = _mm256_movemask_ps(_mm256_cmp_ps(vals, rhs_vec, _CMP_LE_OQ));
        break;
      case NumericCompareOp::Gt:
        cmp_mask = _mm256_movemask_ps(_mm256_cmp_ps(vals, rhs_vec, _CMP_GT_OQ));
        break;
      case NumericCompareOp::Ge:
        cmp_mask = _mm256_movemask_ps(_mm256_cmp_ps(vals, rhs_vec, _CMP_GE_OQ));
        break;
    }
    if (cmp_mask != 0) {
      for (int j = 0; j < static_cast<int>(kLaneCount); ++j) {
        if ((cmp_mask >> j) & 1) {
          if (is_null != nullptr && is_null[i + j] != 0) continue;
          out.selected[i + j] = 1;
          out.indices.push_back(i + j);
          ++out.selected_count;
          if (bounded && out.selected_count >= max_selected) goto scalar_tail;
        }
      }
    }
  }

scalar_tail:
  for (; i < row_count; ++i) {
    if (is_null != nullptr && is_null[i] != 0) continue;
    if (!matchesCompareF32(values[i], rhs, op)) continue;
    out.selected[i] = 1;
    out.indices.push_back(i);
    ++out.selected_count;
    if (bounded && out.selected_count >= max_selected) break;
  }
  return out;
#else
  return scalarDispatch().select_float(values, is_null, row_count, rhs, op, max_selected);
#endif
}

float avx2SumFloat(const float* values, const uint8_t* is_null, std::size_t row_count) {
#if defined(__AVX2__)
  if (is_null == nullptr) {
    __m256 acc = _mm256_setzero_ps();
    std::size_t i = 0;
    constexpr std::size_t kLaneCount = 8;
    for (; i + kLaneCount <= row_count; i += kLaneCount) {
      acc = _mm256_add_ps(acc, _mm256_loadu_ps(values + i));
    }
    __m256 hadd = _mm256_hadd_ps(acc, acc);
    __m256 hadd2 = _mm256_hadd_ps(hadd, hadd);
    __m128 lo = _mm256_castps256_ps128(hadd2);
    __m128 hi = _mm256_extractf128_ps(hadd2, 1);
    float result = _mm_cvtss_f32(_mm_add_ss(lo, hi));
    for (; i < row_count; ++i) result += values[i];
    return result;
  }
  return scalarDispatch().sum_float(values, is_null, row_count);
#else
  return scalarDispatch().sum_float(values, is_null, row_count);
#endif
}

void avx2AccumulateFloat(float* dst, const float* src, std::size_t count) {
#if defined(__AVX2__)
  std::size_t i = 0;
  constexpr std::size_t kLaneCount = 8;
  for (; i + kLaneCount <= count; i += kLaneCount) {
    _mm256_storeu_ps(dst + i,
                     _mm256_add_ps(_mm256_loadu_ps(dst + i), _mm256_loadu_ps(src + i)));
  }
  for (; i < count; ++i) dst[i] += src[i];
#else
  scalarDispatch().accumulate_float(dst, src, count);
#endif
}

void avx2CombineFloat(float* dst, const float* src, std::size_t count, NumericCombineOp op) {
#if defined(__AVX2__)
  if (op == NumericCombineOp::Sum) {
    avx2AccumulateFloat(dst, src, count);
    return;
  }
  std::size_t i = 0;
  constexpr std::size_t kLaneCount = 8;
  for (; i + kLaneCount <= count; i += kLaneCount) {
    const __m256 lhs = _mm256_loadu_ps(dst + i);
    const __m256 rhs = _mm256_loadu_ps(src + i);
    const __m256 result = (op == NumericCombineOp::Min) ? _mm256_min_ps(lhs, rhs)
                                                         : _mm256_max_ps(lhs, rhs);
    _mm256_storeu_ps(dst + i, result);
  }
  for (; i < count; ++i) {
    dst[i] = op == NumericCombineOp::Min ? std::min(dst[i], src[i]) : std::max(dst[i], src[i]);
  }
#else
  scalarDispatch().combine_float(dst, src, count, op);
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
    &avx2SelectFloat,
    &avx2SumFloat,
    &avx2AccumulateFloat,
    &avx2CombineFloat,
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
