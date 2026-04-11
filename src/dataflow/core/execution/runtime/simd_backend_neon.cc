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

NumericSelectionResult neonSelectDouble(const double* values, const uint8_t* is_null,
                                        std::size_t row_count, double rhs,
                                        NumericCompareOp op, std::size_t max_selected) {
  return scalarDispatch().select_double(values, is_null, row_count, rhs, op, max_selected);
}

double neonSumDouble(const double* values, const uint8_t* is_null, std::size_t row_count) {
  return scalarDispatch().sum_double(values, is_null, row_count);
}

void neonAccumulateDouble(double* dst, const double* src, std::size_t count) {
  scalarDispatch().accumulate_double(dst, src, count);
}

void neonCombineDouble(double* dst, const double* src, std::size_t count, NumericCombineOp op) {
  scalarDispatch().combine_double(dst, src, count, op);
}

double neonDotF32(const float* lhs, const float* rhs, std::size_t size) {
#if defined(__ARM_NEON) || defined(__aarch64__)
  std::size_t i = 0;
  float32x4_t acc = vdupq_n_f32(0.0f);
  for (; i + kNeonFloatLaneCount <= size; i += kNeonFloatLaneCount) {
    acc = vmlaq_f32(acc, vld1q_f32(lhs + i), vld1q_f32(rhs + i));
  }
  float lane_sum[kNeonFloatLaneCount];
  vst1q_f32(lane_sum, acc);
  double dot = static_cast<double>(lane_sum[0]) + static_cast<double>(lane_sum[1]) +
               static_cast<double>(lane_sum[2]) + static_cast<double>(lane_sum[3]);
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
  float lane_sum[kNeonFloatLaneCount];
  vst1q_f32(lane_sum, acc);
  double squared = static_cast<double>(lane_sum[0]) + static_cast<double>(lane_sum[1]) +
                   static_cast<double>(lane_sum[2]) + static_cast<double>(lane_sum[3]);
  for (; i < size; ++i) {
    const double diff = static_cast<double>(lhs[i]) - static_cast<double>(rhs[i]);
    squared += diff * diff;
  }
  return squared;
#else
  return scalarDispatch().squared_l2_f32(lhs, rhs, size);
#endif
}

const SimdKernelDispatch kNeonDispatch = {
    SimdBackendKind::Neon,
    simdBackendName(SimdBackendKind::Neon),
    &neonFindByte,
    &neonSelectDouble,
    &neonSumDouble,
    &neonAccumulateDouble,
    &neonCombineDouble,
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
