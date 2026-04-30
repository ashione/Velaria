#include "src/dataflow/core/execution/runtime/simd_dispatch.h"

#include "src/dataflow/core/execution/runtime/simd_backend_internal.h"

#if defined(__ARM_NEON) || defined(__aarch64__)
#include <arm_neon.h>
#endif

namespace dataflow {

namespace {
constexpr std::size_t kNeonFloatLaneCount = 4;
constexpr std::size_t kNeonByteLaneCount = 16;

const char* neonFindByte(const char* begin, const char* end, char needle) {
#if defined(__ARM_NEON) || defined(__aarch64__)
  if (begin == nullptr || end == nullptr || begin >= end) {
    return nullptr;
  }
  const uint8x16_t needle_vec = vdupq_n_u8(static_cast<uint8_t>(needle));
  const char* ptr = begin;
  for (; ptr + kNeonByteLaneCount <= end; ptr += kNeonByteLaneCount) {
    const uint8x16_t bytes =
        vld1q_u8(reinterpret_cast<const uint8_t*>(ptr));
    const uint8x16_t matches = vceqq_u8(bytes, needle_vec);
#if defined(__aarch64__)
    const uint64x2_t lanes = vreinterpretq_u64_u8(matches);
    if ((vgetq_lane_u64(lanes, 0) | vgetq_lane_u64(lanes, 1)) != 0) {
      for (std::size_t i = 0; i < kNeonByteLaneCount; ++i) {
        if (ptr[i] == needle) {
          return ptr + i;
        }
      }
    }
#else
    uint8_t tmp[kNeonByteLaneCount];
    vst1q_u8(tmp, matches);
    for (std::size_t i = 0; i < kNeonByteLaneCount; ++i) {
      if (tmp[i] != 0) {
        return ptr + i;
      }
    }
#endif
  }
  return scalarDispatch().find_byte(ptr, end, needle);
#else
  return scalarDispatch().find_byte(begin, end, needle);
#endif
}

const char* neonFindFirstOf(const char* begin, const char* end, const char* needles,
                            std::size_t needle_count, char* matched_needle) {
#if defined(__ARM_NEON) || defined(__aarch64__)
  if (begin == nullptr || end == nullptr || begin >= end || needles == nullptr ||
      needle_count == 0) {
    return nullptr;
  }
  const std::size_t compare_count = std::min<std::size_t>(needle_count, 4);
  const uint8x16_t needle0 = vdupq_n_u8(static_cast<uint8_t>(compare_count > 0 ? needles[0] : 0));
  const uint8x16_t needle1 = vdupq_n_u8(static_cast<uint8_t>(compare_count > 1 ? needles[1] : 0));
  const uint8x16_t needle2 = vdupq_n_u8(static_cast<uint8_t>(compare_count > 2 ? needles[2] : 0));
  const uint8x16_t needle3 = vdupq_n_u8(static_cast<uint8_t>(compare_count > 3 ? needles[3] : 0));
  const char* ptr = begin;
  for (; ptr + kNeonByteLaneCount <= end; ptr += kNeonByteLaneCount) {
    const uint8x16_t bytes = vld1q_u8(reinterpret_cast<const uint8_t*>(ptr));
    uint8x16_t matches = vceqq_u8(bytes, needle0);
    if (compare_count > 1) {
      matches = vorrq_u8(matches, vceqq_u8(bytes, needle1));
    }
    if (compare_count > 2) {
      matches = vorrq_u8(matches, vceqq_u8(bytes, needle2));
    }
    if (compare_count > 3) {
      matches = vorrq_u8(matches, vceqq_u8(bytes, needle3));
    }
#if defined(__aarch64__)
    const uint64x2_t lanes = vreinterpretq_u64_u8(matches);
    if ((vgetq_lane_u64(lanes, 0) | vgetq_lane_u64(lanes, 1)) != 0) {
      for (std::size_t i = 0; i < kNeonByteLaneCount; ++i) {
        const char value = ptr[i];
        for (std::size_t needle_index = 0; needle_index < needle_count; ++needle_index) {
          if (value == needles[needle_index]) {
            if (matched_needle != nullptr) {
              *matched_needle = value;
            }
            return ptr + i;
          }
        }
      }
    }
#else
    uint8_t tmp[kNeonByteLaneCount];
    vst1q_u8(tmp, matches);
    for (std::size_t i = 0; i < kNeonByteLaneCount; ++i) {
      if (tmp[i] == 0) {
        continue;
      }
      const char value = ptr[i];
      for (std::size_t needle_index = 0; needle_index < needle_count; ++needle_index) {
        if (value == needles[needle_index]) {
          if (matched_needle != nullptr) {
            *matched_needle = value;
          }
          return ptr + i;
        }
      }
    }
#endif
  }
  return scalarDispatch().find_first_of(ptr, end, needles, needle_count, matched_needle);
#else
  return scalarDispatch().find_first_of(begin, end, needles, needle_count, matched_needle);
#endif
}

NumericSelectionResult neonSelectDouble(const double* values, const uint8_t* is_null,
                                        std::size_t row_count, double rhs,
                                        NumericCompareOp op, std::size_t max_selected) {
#if defined(__ARM_NEON) || defined(__aarch64__)
  NumericSelectionResult out;
  out.selected.assign(row_count, 0);
  out.indices.reserve(row_count);
  const bool bounded = max_selected != 0;
  const float64x2_t rhs_vec = vdupq_n_f64(rhs);
  std::size_t i = 0;
  constexpr std::size_t kLaneCount = 2;

  for (; i + kLaneCount <= row_count; i += kLaneCount) {
    const float64x2_t vals = vld1q_f64(values + i);
    uint64x2_t cmp;
    switch (op) {
      case NumericCompareOp::Eq: cmp = vceqq_f64(vals, rhs_vec); break;
      case NumericCompareOp::Ne: cmp = vceqq_f64(vals, rhs_vec); break;
      case NumericCompareOp::Lt: cmp = vcltq_f64(vals, rhs_vec); break;
      case NumericCompareOp::Le: cmp = vcleq_f64(vals, rhs_vec); break;
      case NumericCompareOp::Gt: cmp = vcgtq_f64(vals, rhs_vec); break;
      case NumericCompareOp::Ge: cmp = vcgeq_f64(vals, rhs_vec); break;
    }
    // Lane 0
    {
      bool match = (op == NumericCompareOp::Ne) ? (vgetq_lane_u64(cmp, 0) == 0)
                                                 : (vgetq_lane_u64(cmp, 0) != 0);
      if (match) {
        if (is_null == nullptr || is_null[i] == 0) {
          out.selected[i] = 1;
          out.indices.push_back(i);
          ++out.selected_count;
          if (bounded && out.selected_count >= max_selected) goto scalar_tail;
        }
      }
    }
    // Lane 1
    {
      bool match = (op == NumericCompareOp::Ne) ? (vgetq_lane_u64(cmp, 1) == 0)
                                                 : (vgetq_lane_u64(cmp, 1) != 0);
      if (match) {
        if (is_null == nullptr || is_null[i + 1] == 0) {
          out.selected[i + 1] = 1;
          out.indices.push_back(i + 1);
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

double neonSumDouble(const double* values, const uint8_t* is_null, std::size_t row_count) {
#if defined(__ARM_NEON) || defined(__aarch64__)
  if (is_null == nullptr) {
    // 4 independent accumulators to break the vaddq_f64 latency chain (2-wide double).
    float64x2_t acc0 = vdupq_n_f64(0.0);
    float64x2_t acc1 = vdupq_n_f64(0.0);
    float64x2_t acc2 = vdupq_n_f64(0.0);
    float64x2_t acc3 = vdupq_n_f64(0.0);
    std::size_t i = 0;
    constexpr std::size_t kChunk = 8;
    for (; i + kChunk <= row_count; i += kChunk) {
      acc0 = vaddq_f64(acc0, vld1q_f64(values + i));
      acc1 = vaddq_f64(acc1, vld1q_f64(values + i + 2));
      acc2 = vaddq_f64(acc2, vld1q_f64(values + i + 4));
      acc3 = vaddq_f64(acc3, vld1q_f64(values + i + 6));
    }
    acc0 = vaddq_f64(acc0, acc1);
    acc2 = vaddq_f64(acc2, acc3);
    acc0 = vaddq_f64(acc0, acc2);
    double result = vgetq_lane_f64(acc0, 0) + vgetq_lane_f64(acc0, 1);
    for (; i < row_count; ++i) result += values[i];
    return result;
  }
  return scalarDispatch().sum_double(values, is_null, row_count);
#else
  return scalarDispatch().sum_double(values, is_null, row_count);
#endif
}

void neonAccumulateDouble(double* dst, const double* src, std::size_t count) {
#if defined(__ARM_NEON) || defined(__aarch64__)
  std::size_t i = 0;
  constexpr std::size_t kLaneCount = 2;
  for (; i + kLaneCount <= count; i += kLaneCount) {
    vst1q_f64(dst + i, vaddq_f64(vld1q_f64(dst + i), vld1q_f64(src + i)));
  }
  for (; i < count; ++i) dst[i] += src[i];
#else
  scalarDispatch().accumulate_double(dst, src, count);
#endif
}

void neonCombineDouble(double* dst, const double* src, std::size_t count, NumericCombineOp op) {
#if defined(__ARM_NEON) || defined(__aarch64__)
  if (op == NumericCombineOp::Sum) {
    neonAccumulateDouble(dst, src, count);
    return;
  }
  std::size_t i = 0;
  constexpr std::size_t kLaneCount = 2;
  for (; i + kLaneCount <= count; i += kLaneCount) {
    const float64x2_t lhs = vld1q_f64(dst + i);
    const float64x2_t rhs = vld1q_f64(src + i);
    const float64x2_t result = (op == NumericCombineOp::Min) ? vminq_f64(lhs, rhs)
                                                              : vmaxq_f64(lhs, rhs);
    vst1q_f64(dst + i, result);
  }
  for (; i < count; ++i) {
    dst[i] = op == NumericCombineOp::Min ? std::min(dst[i], src[i]) : std::max(dst[i], src[i]);
  }
#else
  scalarDispatch().combine_double(dst, src, count, op);
#endif
}

double neonDotF32(const float* lhs, const float* rhs, std::size_t size) {
#if defined(__ARM_NEON) || defined(__aarch64__)
  std::size_t i = 0;
  float32x4_t acc = vdupq_n_f32(0.0f);
  for (; i + kNeonFloatLaneCount <= size; i += kNeonFloatLaneCount) {
    acc = vmlaq_f32(acc, vld1q_f32(lhs + i), vld1q_f32(rhs + i));
  }
#if defined(__aarch64__)
  double dot = static_cast<double>(vaddvq_f32(acc));
#else
  float32x2_t sum2 = vadd_f32(vget_high_f32(acc), vget_low_f32(acc));
  sum2 = vpadd_f32(sum2, sum2);
  double dot = static_cast<double>(vget_lane_f32(sum2, 0));
#endif
  for (; i < size; ++i) {
    dot += static_cast<double>(lhs[i]) * static_cast<double>(rhs[i]);
  }
  return dot;
#else
  return scalarDispatch().dot_f32(lhs, rhs, size);
#endif
}

double neonSquaredL2F32(const float* lhs, const float* rhs, std::size_t size) {
#if defined(__ARM_NEON) || defined(__aarch64__)
  std::size_t i = 0;
  float32x4_t acc = vdupq_n_f32(0.0f);
  for (; i + kNeonFloatLaneCount <= size; i += kNeonFloatLaneCount) {
    const float32x4_t diff = vsubq_f32(vld1q_f32(lhs + i), vld1q_f32(rhs + i));
    acc = vmlaq_f32(acc, diff, diff);
  }
#if defined(__aarch64__)
  double squared = static_cast<double>(vaddvq_f32(acc));
#else
  float32x2_t sum2 = vadd_f32(vget_high_f32(acc), vget_low_f32(acc));
  sum2 = vpadd_f32(sum2, sum2);
  double squared = static_cast<double>(vget_lane_f32(sum2, 0));
#endif
  for (; i < size; ++i) {
    const double diff = static_cast<double>(lhs[i]) - static_cast<double>(rhs[i]);
    squared += diff * diff;
  }
  return squared;
#else
  return scalarDispatch().squared_l2_f32(lhs, rhs, size);
#endif
}


NumericSelectionResult neonSelectFloat(const float* values, const uint8_t* is_null,
                                       std::size_t row_count, float rhs,
                                       NumericCompareOp op, std::size_t max_selected) {
#if defined(__ARM_NEON) || defined(__aarch64__)
  NumericSelectionResult out;
  out.selected.assign(row_count, 0);
  out.indices.reserve(row_count);
  const bool bounded = max_selected != 0;
  const float32x4_t rhs_vec = vdupq_n_f32(rhs);
  std::size_t i = 0;
  constexpr std::size_t kLaneCount = 4;

  for (; i + kLaneCount <= row_count; i += kLaneCount) {
    const float32x4_t vals = vld1q_f32(values + i);
    uint32x4_t cmp;
    switch (op) {
      case NumericCompareOp::Eq: cmp = vceqq_f32(vals, rhs_vec); break;
      case NumericCompareOp::Ne: cmp = vceqq_f32(vals, rhs_vec); break;
      case NumericCompareOp::Lt: cmp = vcltq_f32(vals, rhs_vec); break;
      case NumericCompareOp::Le: cmp = vcleq_f32(vals, rhs_vec); break;
      case NumericCompareOp::Gt: cmp = vcgtq_f32(vals, rhs_vec); break;
      case NumericCompareOp::Ge: cmp = vcgeq_f32(vals, rhs_vec); break;
    }
    // Lane 0
    {
      bool match = (op == NumericCompareOp::Ne) ? (vgetq_lane_u32(cmp, 0) == 0)
                                                 : (vgetq_lane_u32(cmp, 0) != 0);
      if (match) {
        if (is_null == nullptr || is_null[i] == 0) {
          out.selected[i] = 1; out.indices.push_back(i); ++out.selected_count;
          if (bounded && out.selected_count >= max_selected) goto scalar_tail;
        }
      }
    }
    // Lane 1
    {
      bool match = (op == NumericCompareOp::Ne) ? (vgetq_lane_u32(cmp, 1) == 0)
                                                 : (vgetq_lane_u32(cmp, 1) != 0);
      if (match) {
        if (is_null == nullptr || is_null[i + 1] == 0) {
          out.selected[i + 1] = 1; out.indices.push_back(i + 1); ++out.selected_count;
          if (bounded && out.selected_count >= max_selected) goto scalar_tail;
        }
      }
    }
    // Lane 2
    {
      bool match = (op == NumericCompareOp::Ne) ? (vgetq_lane_u32(cmp, 2) == 0)
                                                 : (vgetq_lane_u32(cmp, 2) != 0);
      if (match) {
        if (is_null == nullptr || is_null[i + 2] == 0) {
          out.selected[i + 2] = 1; out.indices.push_back(i + 2); ++out.selected_count;
          if (bounded && out.selected_count >= max_selected) goto scalar_tail;
        }
      }
    }
    // Lane 3
    {
      bool match = (op == NumericCompareOp::Ne) ? (vgetq_lane_u32(cmp, 3) == 0)
                                                 : (vgetq_lane_u32(cmp, 3) != 0);
      if (match) {
        if (is_null == nullptr || is_null[i + 3] == 0) {
          out.selected[i + 3] = 1; out.indices.push_back(i + 3); ++out.selected_count;
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

float neonSumFloat(const float* values, const uint8_t* is_null, std::size_t row_count) {
#if defined(__ARM_NEON) || defined(__aarch64__)
  if (is_null == nullptr) {
    // 4 independent accumulators to break the vaddq_f32 3-cycle latency chain.
    // Throughput: 4 accum * 4 floats / 3 cycles ≈ 5.3 floats/cycle ≈ L2 bandwidth limit.
    float32x4_t acc0 = vdupq_n_f32(0.0f);
    float32x4_t acc1 = vdupq_n_f32(0.0f);
    float32x4_t acc2 = vdupq_n_f32(0.0f);
    float32x4_t acc3 = vdupq_n_f32(0.0f);
    std::size_t i = 0;
    constexpr std::size_t kChunk = 16;
    for (; i + kChunk <= row_count; i += kChunk) {
      acc0 = vaddq_f32(acc0, vld1q_f32(values + i));
      acc1 = vaddq_f32(acc1, vld1q_f32(values + i + 4));
      acc2 = vaddq_f32(acc2, vld1q_f32(values + i + 8));
      acc3 = vaddq_f32(acc3, vld1q_f32(values + i + 12));
    }
    acc0 = vaddq_f32(acc0, acc1);
    acc2 = vaddq_f32(acc2, acc3);
    acc0 = vaddq_f32(acc0, acc2);
#if defined(__aarch64__)
    float result = vaddvq_f32(acc0);
#else
    float32x2_t sum2 = vadd_f32(vget_high_f32(acc0), vget_low_f32(acc0));
    sum2 = vpadd_f32(sum2, sum2);
    float result = vget_lane_f32(sum2, 0);
#endif
    // Scalar tail for remaining < 16 elements
    for (; i < row_count; ++i) result += values[i];
    return result;
  }
  return scalarDispatch().sum_float(values, is_null, row_count);
#else
  return scalarDispatch().sum_float(values, is_null, row_count);
#endif
}

void neonAccumulateFloat(float* dst, const float* src, std::size_t count) {
#if defined(__ARM_NEON) || defined(__aarch64__)
  std::size_t i = 0;
  constexpr std::size_t kLaneCount = 4;
  for (; i + kLaneCount <= count; i += kLaneCount) {
    vst1q_f32(dst + i, vaddq_f32(vld1q_f32(dst + i), vld1q_f32(src + i)));
  }
  for (; i < count; ++i) dst[i] += src[i];
#else
  scalarDispatch().accumulate_float(dst, src, count);
#endif
}

void neonCombineFloat(float* dst, const float* src, std::size_t count, NumericCombineOp op) {
#if defined(__ARM_NEON) || defined(__aarch64__)
  if (op == NumericCombineOp::Sum) {
    neonAccumulateFloat(dst, src, count);
    return;
  }
  std::size_t i = 0;
  constexpr std::size_t kLaneCount = 4;
  for (; i + kLaneCount <= count; i += kLaneCount) {
    const float32x4_t lhs = vld1q_f32(dst + i);
    const float32x4_t rhs = vld1q_f32(src + i);
    const float32x4_t result = (op == NumericCombineOp::Min) ? vminq_f32(lhs, rhs)
                                                              : vmaxq_f32(lhs, rhs);
    vst1q_f32(dst + i, result);
  }
  for (; i < count; ++i) {
    dst[i] = op == NumericCombineOp::Min ? std::min(dst[i], src[i]) : std::max(dst[i], src[i]);
  }
#else
  scalarDispatch().combine_float(dst, src, count, op);
#endif
}

const SimdKernelDispatch kNeonDispatch = {
    SimdBackendKind::Neon,
    simdBackendName(SimdBackendKind::Neon),
    &neonFindByte,
    &neonFindFirstOf,
    &neonSelectDouble,
    &neonSumDouble,
    &neonAccumulateDouble,
    &neonCombineDouble,
    &neonSelectFloat,
    &neonSumFloat,
    &neonAccumulateFloat,
    &neonCombineFloat,
    &neonDotF32,
    &neonSquaredL2F32,
};

}  // namespace

bool neonDispatchCompiled() {
#if defined(__ARM_NEON) || defined(__aarch64__)
  return true;
#else
  return false;
#endif
}

const SimdKernelDispatch& neonDispatch() {
  return kNeonDispatch;
}

}  // namespace dataflow
