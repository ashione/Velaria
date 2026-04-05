#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace dataflow {

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

struct NumericSelectionResult {
  std::vector<uint8_t> selected;
  std::vector<std::size_t> indices;
  std::size_t selected_count = 0;
};

struct SimdKernelDispatch {
  SimdBackendKind backend = SimdBackendKind::Scalar;
  const char* backend_name = "scalar";
  NumericSelectionResult (*select_double)(const double* values, const uint8_t* is_null,
                                          std::size_t row_count, double rhs,
                                          NumericCompareOp op,
                                          std::size_t max_selected) = nullptr;
  double (*sum_double)(const double* values, const uint8_t* is_null,
                       std::size_t row_count) = nullptr;
  double (*dot_f32)(const float* lhs, const float* rhs, std::size_t size) = nullptr;
  double (*squared_l2_f32)(const float* lhs, const float* rhs, std::size_t size) = nullptr;
};

const SimdKernelDispatch& simdDispatch();
std::string activeSimdBackendName();
void resetSimdDispatchForTest();

}  // namespace dataflow
