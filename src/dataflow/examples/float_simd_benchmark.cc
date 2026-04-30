#include "src/dataflow/core/execution/runtime/simd_dispatch.h"
#include "src/dataflow/core/execution/runtime/simd_backend_internal.h"

#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

namespace {

using clock = std::chrono::high_resolution_clock;

struct BenchResult {
  std::string name;
  std::string backend;
  long long ns_per_call;
  std::size_t rows;
};

BenchResult runSumFloatNoNull(const dataflow::SimdKernelDispatch& d, std::size_t N,
                               int warmup, int iters) {
  std::vector<float> values(N);
  for (std::size_t i = 0; i < N; ++i) values[i] = static_cast<float>(i % 1000) * 0.5f;

  volatile float sum = 0;
  for (int r = 0; r < warmup; ++r) sum += d.sum_float(values.data(), nullptr, N);
  auto t1 = clock::now();
  for (int r = 0; r < iters; ++r) sum += d.sum_float(values.data(), nullptr, N);
  auto t2 = clock::now();
  return {"sum_float_no_null", d.backend_name,
          std::chrono::duration_cast<std::chrono::nanoseconds>(t2 - t1).count() / iters, N};
}

BenchResult runSumFloatNull(const dataflow::SimdKernelDispatch& d, std::size_t N,
                             int warmup, int iters) {
  std::vector<float> values(N);
  std::vector<uint8_t> is_null(N, 0);
  for (std::size_t i = 0; i < N; ++i) {
    values[i] = static_cast<float>(i % 1000) * 0.5f;
    if (i % 3 == 0) is_null[i] = 1;
  }

  volatile float sum = 0;
  for (int r = 0; r < warmup; ++r) sum += d.sum_float(values.data(), is_null.data(), N);
  auto t1 = clock::now();
  for (int r = 0; r < iters; ++r) sum += d.sum_float(values.data(), is_null.data(), N);
  auto t2 = clock::now();
  return {"sum_float_33p_null", d.backend_name,
          std::chrono::duration_cast<std::chrono::nanoseconds>(t2 - t1).count() / iters, N};
}

BenchResult runAccumulateFloat(const dataflow::SimdKernelDispatch& d, std::size_t N,
                                int warmup, int iters) {
  std::vector<float> values(N);
  std::vector<float> dst(N);
  for (std::size_t i = 0; i < N; ++i) {
    values[i] = static_cast<float>(i % 1000) * 0.5f;
    dst[i] = static_cast<float>(i % 100) * 0.1f;
  }

  for (int r = 0; r < warmup; ++r) d.accumulate_float(dst.data(), values.data(), N);
  auto t1 = clock::now();
  for (int r = 0; r < iters; ++r) d.accumulate_float(dst.data(), values.data(), N);
  auto t2 = clock::now();
  return {"accumulate_float", d.backend_name,
          std::chrono::duration_cast<std::chrono::nanoseconds>(t2 - t1).count() / iters, N};
}

BenchResult runCombineFloat(const dataflow::SimdKernelDispatch& d, std::size_t N,
                             int warmup, int iters) {
  std::vector<float> values(N);
  std::vector<float> dst(N);
  for (std::size_t i = 0; i < N; ++i) {
    values[i] = static_cast<float>(i % 1000) * 0.5f;
    dst[i] = static_cast<float>(i % 100) * 0.1f;
  }

  for (int r = 0; r < warmup; ++r)
    d.combine_float(dst.data(), values.data(), N, dataflow::NumericCombineOp::Min);
  auto t1 = clock::now();
  for (int r = 0; r < iters; ++r)
    d.combine_float(dst.data(), values.data(), N, dataflow::NumericCombineOp::Min);
  auto t2 = clock::now();
  return {"combine_float_min", d.backend_name,
          std::chrono::duration_cast<std::chrono::nanoseconds>(t2 - t1).count() / iters, N};
}

BenchResult runSelectFloat(const dataflow::SimdKernelDispatch& d, std::size_t N,
                            int warmup, int iters) {
  std::vector<float> values(N);
  for (std::size_t i = 0; i < N; ++i) values[i] = static_cast<float>(i % 1000) * 0.5f;

  volatile int matched = 0;
  for (int r = 0; r < warmup; ++r)
    matched += d.select_float(values.data(), nullptr, N, 250.0f,
                               dataflow::NumericCompareOp::Gt, 0).selected_count;
  auto t1 = clock::now();
  for (int r = 0; r < iters; ++r)
    matched += d.select_float(values.data(), nullptr, N, 250.0f,
                               dataflow::NumericCompareOp::Gt, 0).selected_count;
  auto t2 = clock::now();
  return {"select_float_gt", d.backend_name,
          std::chrono::duration_cast<std::chrono::nanoseconds>(t2 - t1).count() / iters, N};
}

void printJson(const BenchResult& r) {
  printf("{\"bench\":\"%s\",\"backend\":\"%s\",\"rows\":%zu,\"ns_per_call\":%lld}\n",
         r.name.c_str(), r.backend.c_str(), r.rows, (long long)r.ns_per_call);
}

}  // namespace

int main() {
  const std::size_t N = 1000000;
  const int warmup = 30;
  const int iters = 100;
  const auto& neon = dataflow::neonDispatch();
  const auto& scalar = dataflow::scalarDispatch();

  // Run with NEON
  printJson(runSumFloatNoNull(neon, N, warmup, iters));
  printJson(runSumFloatNull(neon, N, warmup, iters));
  printJson(runAccumulateFloat(neon, N, warmup, iters));
  printJson(runCombineFloat(neon, N, warmup, iters));
  printJson(runSelectFloat(neon, N, warmup, iters));

  // Run with scalar for baseline
  printJson(runSumFloatNoNull(scalar, N, warmup, iters));
  printJson(runSumFloatNull(scalar, N, warmup, iters));
  printJson(runAccumulateFloat(scalar, N, warmup, iters));
  printJson(runCombineFloat(scalar, N, warmup, iters));
  printJson(runSelectFloat(scalar, N, warmup, iters));

  return 0;
}
