#pragma once

#include "src/dataflow/core/execution/runtime/simd_dispatch.h"

namespace dataflow {

const SimdKernelDispatch& scalarDispatch();
bool avx2DispatchCompiled();
const SimdKernelDispatch& avx2Dispatch();
bool neonDispatchCompiled();
const SimdKernelDispatch& neonDispatch();

}  // namespace dataflow
