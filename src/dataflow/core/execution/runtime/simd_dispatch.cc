#include "src/dataflow/core/execution/runtime/simd_dispatch.h"

#include "src/dataflow/core/execution/runtime/simd_backend_internal.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstdlib>
#include <mutex>
#include <string>

namespace dataflow {

namespace {
std::mutex& stateMutex() {
  static std::mutex mu;
  return mu;
}

std::atomic<const SimdKernelDispatch*>& stateDispatch() {
  static std::atomic<const SimdKernelDispatch*> dispatch{nullptr};
  return dispatch;
}

std::string lowerCopy(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
  return value;
}

bool runtimeSupportsAvx2() {
#if defined(__x86_64__) || defined(__i386__)
#if defined(__clang__) || defined(__GNUC__)
  return __builtin_cpu_supports("avx2");
#else
  return false;
#endif
#else
  return false;
#endif
}

bool runtimeSupportsNeon() {
#if defined(__aarch64__) || defined(__ARM_NEON)
  return true;
#else
  return false;
#endif
}

const SimdKernelDispatch& chooseDispatch() {
  const char* env = std::getenv(kSimdBackendEnvVar);
  const std::string requested = env == nullptr ? kSimdBackendNameAuto : lowerCopy(env);

  if (requested == kSimdBackendNameScalar) {
    return scalarDispatch();
  }

  if (requested == kSimdBackendNameAvx2) {
    if (avx2DispatchCompiled() && runtimeSupportsAvx2()) {
      return avx2Dispatch();
    }
    return scalarDispatch();
  }

  if (requested == kSimdBackendNameNeon) {
    if (neonDispatchCompiled() && runtimeSupportsNeon()) {
      return neonDispatch();
    }
    return scalarDispatch();
  }

  if (neonDispatchCompiled() && runtimeSupportsNeon()) {
    return neonDispatch();
  }
  if (avx2DispatchCompiled() && runtimeSupportsAvx2()) {
    return avx2Dispatch();
  }
  return scalarDispatch();
}

}  // namespace

const char* simdBackendName(SimdBackendKind kind) {
  switch (kind) {
    case SimdBackendKind::Scalar:
      return kSimdBackendNameScalar;
    case SimdBackendKind::Avx2:
      return kSimdBackendNameAvx2;
    case SimdBackendKind::Neon:
      return kSimdBackendNameNeon;
  }
  return kSimdBackendNameScalar;
}

const SimdKernelDispatch& simdDispatch() {
  const SimdKernelDispatch* dispatch = stateDispatch().load(std::memory_order_acquire);
  if (dispatch != nullptr) {
    return *dispatch;
  }

  std::lock_guard<std::mutex> lock(stateMutex());
  dispatch = stateDispatch().load(std::memory_order_relaxed);
  if (dispatch == nullptr) {
    dispatch = &chooseDispatch();
    stateDispatch().store(dispatch, std::memory_order_release);
  }
  return *dispatch;
}

std::string activeSimdBackendName() {
  return simdDispatch().backend_name;
}

void resetSimdDispatchForTest() {
  std::lock_guard<std::mutex> lock(stateMutex());
  stateDispatch().store(nullptr, std::memory_order_release);
}

}  // namespace dataflow
