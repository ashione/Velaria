#include "src/dataflow/core/execution/runtime/vector_index.h"

#include <algorithm>
#include <cmath>
#include <queue>
#include <sstream>
#include <stdexcept>

#include "src/dataflow/core/execution/runtime/simd_dispatch.h"

namespace dataflow {
namespace {
constexpr double kMinCosineSimilarity = -1.0;
constexpr double kMaxCosineSimilarity = 1.0;
constexpr double kCosineDistanceForZeroNorm = 1.0;
constexpr std::size_t kMinimumTopK = 1;
constexpr char kExplainModeExactScan[] = "exact-scan";
constexpr char kExplainAccelerationSimdTopK[] = "flat-buffer+simd-topk";

const char* metricName(VectorSearchMetric metric) {
  switch (metric) {
    case VectorSearchMetric::Cosine:
      return "cosine";
    case VectorSearchMetric::Dot:
      return "dot";
    case VectorSearchMetric::L2:
      return "l2";
  }
  return metricName(VectorSearchMetric::Cosine);
}

class ExactScanVectorIndex : public VectorIndex {
 public:
  explicit ExactScanVectorIndex(std::vector<std::vector<float>> vectors)
      : row_count_(vectors.size()), dimension_(vectors.empty() ? 0 : vectors.front().size()) {
    if (vectors.empty()) return;
    for (const auto& row : vectors) {
      if (row.size() != dimension_) {
        throw std::invalid_argument("exact scan vector index requires fixed-length vectors");
      }
    }
    data_.resize(row_count_ * dimension_);
    norms_.resize(row_count_, 0.0);
    for (std::size_t row = 0; row < row_count_; ++row) {
      for (std::size_t col = 0; col < dimension_; ++col) {
        const float v = vectors[row][col];
        data_[row * dimension_ + col] = v;
      }
      const float* base = data_.data() + row * dimension_;
      norms_[row] = std::sqrt(simdDispatch().dot_f32(base, base, dimension_));
    }
  }

  std::size_t dimension() const override { return dimension_; }
  std::size_t size() const override { return row_count_; }

  std::vector<VectorSearchResult> search(const std::vector<float>& query,
                                         const VectorSearchOptions& options) const override {
    if (row_count_ == 0) return {};
    if (query.size() != dimension()) {
      throw std::invalid_argument("query vector dimension mismatch");
    }

    const std::size_t k = std::max<std::size_t>(kMinimumTopK, options.top_k);
    auto cmp_max = [](const VectorSearchResult& lhs, const VectorSearchResult& rhs) {
      return lhs.score < rhs.score;
    };
    auto cmp_min = [](const VectorSearchResult& lhs, const VectorSearchResult& rhs) {
      return lhs.score > rhs.score;
    };
    std::vector<VectorSearchResult> scored;
    scored.reserve(std::min(k, row_count_));
    if (options.metric == VectorSearchMetric::Dot) {
      std::vector<VectorSearchResult> heap_storage;
      heap_storage.reserve(k);
      std::priority_queue<VectorSearchResult, std::vector<VectorSearchResult>, decltype(cmp_min)>
          min_heap(cmp_min, std::move(heap_storage));
      for (std::size_t row = 0; row < row_count_; ++row) {
        const float* base = data_.data() + row * dimension_;
        const double dot = simdDispatch().dot_f32(base, query.data(), dimension_);
        VectorSearchResult current{row, dot};
        if (min_heap.size() < k) {
          min_heap.push(current);
        } else if (current.score > min_heap.top().score) {
          min_heap.pop();
          min_heap.push(current);
        }
      }
      while (!min_heap.empty()) {
        scored.push_back(min_heap.top());
        min_heap.pop();
      }
      std::sort(scored.begin(), scored.end(),
                [](const VectorSearchResult& lhs, const VectorSearchResult& rhs) {
                  return lhs.score > rhs.score;
                });
      return scored;
    }

    std::vector<VectorSearchResult> heap_storage;
    heap_storage.reserve(k);
    std::priority_queue<VectorSearchResult, std::vector<VectorSearchResult>, decltype(cmp_max)>
        max_heap(cmp_max, std::move(heap_storage));
    if (options.metric == VectorSearchMetric::L2) {
      for (std::size_t row = 0; row < row_count_; ++row) {
        const float* base = data_.data() + row * dimension_;
        const double squared_l2 = simdDispatch().squared_l2_f32(base, query.data(), dimension_);
        VectorSearchResult current{row, squared_l2};
        if (max_heap.size() < k) {
          max_heap.push(current);
        } else if (current.score < max_heap.top().score) {
          max_heap.pop();
          max_heap.push(current);
        }
      }
      while (!max_heap.empty()) {
        scored.push_back(max_heap.top());
        max_heap.pop();
      }
      std::sort(scored.begin(), scored.end(),
                [](const VectorSearchResult& lhs, const VectorSearchResult& rhs) {
                  return lhs.score < rhs.score;
                });
      for (auto& item : scored) {
        item.score = std::sqrt(item.score);
      }
      return scored;
    }

    const double query_norm = std::sqrt(simdDispatch().dot_f32(query.data(), query.data(), dimension_));
    for (std::size_t row = 0; row < row_count_; ++row) {
      const float* base = data_.data() + row * dimension_;
      const double dot = simdDispatch().dot_f32(base, query.data(), dimension_);
      const double denom = norms_[row] * query_norm;
      const double cosine_distance =
          denom == 0.0
              ? kCosineDistanceForZeroNorm
              : (1.0 - std::max(kMinCosineSimilarity,
                                std::min(kMaxCosineSimilarity, dot / denom)));
      VectorSearchResult current{row, cosine_distance};
      if (max_heap.size() < k) {
        max_heap.push(current);
      } else if (current.score < max_heap.top().score) {
        max_heap.pop();
        max_heap.push(current);
      }
    }
    while (!max_heap.empty()) {
      scored.push_back(max_heap.top());
      max_heap.pop();
    }
    std::sort(scored.begin(), scored.end(),
              [](const VectorSearchResult& lhs, const VectorSearchResult& rhs) {
                return lhs.score < rhs.score;
              });
    return scored;
  }

  std::string explain(const VectorSearchOptions& options) const override {
    std::ostringstream out;
    out << "mode=" << kExplainModeExactScan << "\n";
    out << "metric=" << metricName(options.metric) << "\n";
    out << "dimension=" << dimension() << "\n";
    out << "top_k=" << options.top_k << "\n";
    out << "candidate_rows=" << size() << "\n";
    out << "acceleration=" << kExplainAccelerationSimdTopK << "\n";
    out << "backend=" << activeSimdBackendName() << "\n";
    out << "filter_pushdown=false\n";
    return out.str();
  }

 private:
  std::size_t row_count_ = 0;
  std::size_t dimension_ = 0;
  std::vector<float> data_;
  std::vector<double> norms_;
};

}  // namespace

std::unique_ptr<VectorIndex> makeExactScanVectorIndex(std::vector<std::vector<float>> vectors) {
  return std::unique_ptr<VectorIndex>(new ExactScanVectorIndex(std::move(vectors)));
}

}  // namespace dataflow
