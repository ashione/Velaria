#pragma once

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <string>

namespace dataflow {

// ---------------------------------------------------------------------------
// Timing harness: runs a callable `rounds` times and tracks the best (fastest)
// elapsed wall-clock time in milliseconds.
// ---------------------------------------------------------------------------
template <typename Fn>
uint64_t runBenchmarked(std::size_t rounds, Fn&& fn) {
  uint64_t best_ms = std::numeric_limits<uint64_t>::max();
  for (std::size_t r = 0; r < rounds; ++r) {
    const auto started = std::chrono::steady_clock::now();
    fn();
    const auto elapsed_ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - started).count());
    best_ms = std::min(best_ms, elapsed_ms);
  }
  return best_ms;
}

// ---------------------------------------------------------------------------
// CLI argument parsing helpers.
// ---------------------------------------------------------------------------
inline std::size_t parseSizeTArg(char** argv, int index,
                                  std::size_t default_val) {
  if (argv != nullptr) {
    return static_cast<std::size_t>(std::strtoull(argv[index], nullptr, 10));
  }
  return default_val;
}

// ---------------------------------------------------------------------------
// Throughput calculation (rows / second).
// ---------------------------------------------------------------------------
inline double rowsPerSecond(std::size_t row_count, uint64_t elapsed_ms) {
  return elapsed_ms == 0
             ? 0.0
             : (static_cast<double>(row_count) /
                (static_cast<double>(elapsed_ms) / 1000.0));
}

}  // namespace dataflow
