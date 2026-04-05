#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

namespace dataflow {

inline constexpr std::size_t kDefaultVectorSearchTopK = 10;

enum class VectorSearchMetric { Cosine, Dot, L2 };

struct VectorSearchOptions {
  VectorSearchMetric metric = VectorSearchMetric::Cosine;
  std::size_t top_k = kDefaultVectorSearchTopK;
};

struct VectorSearchResult {
  std::size_t row_id = 0;
  double score = 0.0;
};

class VectorIndex {
 public:
  virtual ~VectorIndex() = default;

  virtual std::size_t dimension() const = 0;
  virtual std::size_t size() const = 0;
  virtual std::vector<VectorSearchResult> search(
      const std::vector<float>& query,
      const VectorSearchOptions& options) const = 0;
  virtual std::string explain(const VectorSearchOptions& options) const = 0;
};

std::unique_ptr<VectorIndex> makeExactScanVectorIndex(std::vector<std::vector<float>> vectors);

}  // namespace dataflow
