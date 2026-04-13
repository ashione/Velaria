#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace dataflow {

inline constexpr char kSimdBackendEnvVar[] = "VELARIA_SIMD_BACKEND";
inline constexpr char kSimdBackendNameAuto[] = "auto";
inline constexpr char kSimdBackendNameScalar[] = "scalar";
inline constexpr char kSimdBackendNameAvx2[] = "avx2";
inline constexpr char kSimdBackendNameNeon[] = "neon";

enum class SimdBackendKind : uint8_t {
  Scalar = 0,
  Avx2 = 1,
  Neon = 2,
};

enum class NumericCompareOp : uint8_t {
  Eq = 0,
  Ne = 1,
  Lt = 2,
  Le = 3,
  Gt = 4,
  Ge = 5,
};

enum class NumericCombineOp : uint8_t {
  Sum = 0,
  Min = 1,
  Max = 2,
};

struct NumericSelectionResult {
  std::vector<uint8_t> selected;
  std::vector<std::size_t> indices;
  std::size_t selected_count = 0;
};

struct SimdKernelDispatch {
  SimdBackendKind backend = SimdBackendKind::Scalar;
  const char* backend_name = kSimdBackendNameScalar;
  const char* (*find_byte)(const char* begin, const char* end, char needle) = nullptr;
  const char* (*find_first_of)(const char* begin, const char* end, const char* needles,
                               std::size_t needle_count, char* matched_needle) = nullptr;
  NumericSelectionResult (*select_double)(const double* values, const uint8_t* is_null,
                                          std::size_t row_count, double rhs,
                                          NumericCompareOp op,
                                          std::size_t max_selected) = nullptr;
  double (*sum_double)(const double* values, const uint8_t* is_null,
                       std::size_t row_count) = nullptr;
  void (*accumulate_double)(double* dst, const double* src, std::size_t count) = nullptr;
  void (*combine_double)(double* dst, const double* src, std::size_t count,
                         NumericCombineOp op) = nullptr;
  double (*dot_f32)(const float* lhs, const float* rhs, std::size_t size) = nullptr;
  double (*squared_l2_f32)(const float* lhs, const float* rhs, std::size_t size) = nullptr;
};

const char* simdBackendName(SimdBackendKind kind);
const SimdKernelDispatch& simdDispatch();
std::string activeSimdBackendName();
std::vector<std::string> compiledSimdBackendNames();
void resetSimdDispatchForTest();

}  // namespace dataflow
