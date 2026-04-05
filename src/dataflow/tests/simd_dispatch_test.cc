#include <cmath>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "src/dataflow/core/execution/runtime/simd_dispatch.h"

namespace {

void expect(bool cond, const std::string& msg) {
  if (!cond) throw std::runtime_error(msg);
}

}  // namespace

int main() {
  try {
    std::vector<float> lhs = {1.0f, 2.0f, 3.0f, 4.0f, -2.0f, 0.5f, 8.0f, -1.0f};
    std::vector<float> rhs = {0.5f, -1.0f, 4.0f, 0.0f, 2.0f, 1.5f, -3.0f, 2.0f};
    std::vector<double> values = {1.0, 5.0, -3.0, 5.0, 9.0};
    std::vector<uint8_t> is_null = {0, 0, 1, 0, 0};

    setenv("VELARIA_SIMD_BACKEND", "scalar", 1);
    dataflow::resetSimdDispatchForTest();
    expect(dataflow::activeSimdBackendName() == "scalar", "forced scalar backend mismatch");
    const auto scalar_dot = dataflow::simdDispatch().dot_f32(lhs.data(), rhs.data(), lhs.size());
    const auto scalar_l2 =
        dataflow::simdDispatch().squared_l2_f32(lhs.data(), rhs.data(), lhs.size());
    const auto scalar_selection = dataflow::simdDispatch().select_double(
        values.data(), is_null.data(), values.size(), 5.0, dataflow::NumericCompareOp::Ge, 0);
    expect(scalar_selection.selected_count == 3, "scalar selection count mismatch");

    setenv("VELARIA_SIMD_BACKEND", "definitely-not-a-backend", 1);
    dataflow::resetSimdDispatchForTest();
    expect(!dataflow::activeSimdBackendName().empty(), "fallback backend should be initialized");
    const auto fallback_dot = dataflow::simdDispatch().dot_f32(lhs.data(), rhs.data(), lhs.size());
    const auto fallback_l2 =
        dataflow::simdDispatch().squared_l2_f32(lhs.data(), rhs.data(), lhs.size());
    const auto fallback_selection = dataflow::simdDispatch().select_double(
        values.data(), is_null.data(), values.size(), 5.0, dataflow::NumericCompareOp::Ge, 0);
    expect(std::fabs(fallback_dot - scalar_dot) < 1e-6, "fallback dot mismatch");
    expect(std::fabs(fallback_l2 - scalar_l2) < 1e-6, "fallback l2 mismatch");
    expect(fallback_selection.selected_count == scalar_selection.selected_count,
           "fallback selection mismatch");

    unsetenv("VELARIA_SIMD_BACKEND");
    dataflow::resetSimdDispatchForTest();
    expect(!dataflow::activeSimdBackendName().empty(), "auto backend should be initialized");

    std::cout << "[test] simd dispatch fallback ok" << std::endl;
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "[test] simd dispatch fallback failed: " << ex.what() << std::endl;
    return 1;
  }
}
