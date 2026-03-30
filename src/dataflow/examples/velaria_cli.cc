#include <iostream>
#include <cmath>
#include <algorithm>
#include <stdexcept>
#include <string>
#include <vector>
#include <sstream>

#include "src/dataflow/api/session.h"
#include "src/dataflow/core/value.h"

namespace {

std::string escapeJson(const std::string& input) {
  std::string out;
  out.reserve(input.size());
  for (char c : input) {
    switch (c) {
      case '\\':
        out += "\\\\";
        break;
      case '"':
        out += "\\\"";
        break;
      case '\n':
        out += "\\n";
        break;
      case '\r':
        out += "\\r";
        break;
      case '\t':
        out += "\\t";
        break;
      default:
        out += c;
        break;
    }
  }
  return out;
}

std::string valueToJson(const dataflow::Value& value) {
  switch (value.type()) {
    case dataflow::DataType::Nil:
      return "null";
    case dataflow::DataType::Int64:
      return std::to_string(value.asInt64());
    case dataflow::DataType::Double:
      return value.toString();
    case dataflow::DataType::String:
      return "\"" + escapeJson(value.asString()) + "\"";
  }
  return "null";
}

void printUsage(const char* program) {
  std::cerr << "Usage: " << program
            << " --csv <path> [--query <sql>] [--table <name>] [--delimiter <char>]\n"
            << "       " << program
            << " --csv <path> --vector-column <name> --query-vector <v1,v2,...>\n"
            << "       [--metric cosine|cosin|dot|l2] [--top-k <n>]\n";
}

std::vector<double> parseVectorText(const std::string& raw) {
  std::string input = raw;
  if (!input.empty() && input.front() == '[' && input.back() == ']') {
    input = input.substr(1, input.size() - 2);
  }
  std::vector<double> out;
  std::stringstream ss(input);
  std::string item;
  while (std::getline(ss, item, ',')) {
    if (item.empty()) continue;
    out.push_back(std::stod(item));
  }
  return out;
}

std::vector<double> parseVectorValue(const dataflow::Value& value) {
  if (value.type() == dataflow::DataType::String) {
    return parseVectorText(value.asString());
  }
  return parseVectorText(value.toString());
}

double l2Distance(const std::vector<double>& lhs, const std::vector<double>& rhs) {
  double sum = 0.0;
  for (std::size_t i = 0; i < lhs.size(); ++i) {
    const double diff = lhs[i] - rhs[i];
    sum += diff * diff;
  }
  return std::sqrt(sum);
}

double cosineDistance(const std::vector<double>& lhs, const std::vector<double>& rhs) {
  double dot = 0.0;
  double lhs_norm = 0.0;
  double rhs_norm = 0.0;
  for (std::size_t i = 0; i < lhs.size(); ++i) {
    dot += lhs[i] * rhs[i];
    lhs_norm += lhs[i] * lhs[i];
    rhs_norm += rhs[i] * rhs[i];
  }
  if (lhs_norm == 0.0 || rhs_norm == 0.0) {
    return 1.0;
  }
  double similarity = dot / (std::sqrt(lhs_norm) * std::sqrt(rhs_norm));
  if (similarity > 1.0) similarity = 1.0;
  if (similarity < -1.0) similarity = -1.0;
  return 1.0 - similarity;
}

double dotScore(const std::vector<double>& lhs, const std::vector<double>& rhs) {
  double dot = 0.0;
  for (std::size_t i = 0; i < lhs.size(); ++i) {
    dot += lhs[i] * rhs[i];
  }
  return dot;
}

}  // namespace

int main(int argc, char** argv) {
  std::string csv_path;
  std::string query;
  std::string table = "input_table";
  std::string vector_column;
  std::string query_vector;
  std::string metric = "cosine";
  std::size_t top_k = 5;
  char delimiter = ',';

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--csv" && i + 1 < argc) {
      csv_path = argv[++i];
    } else if (arg == "--query" && i + 1 < argc) {
      query = argv[++i];
    } else if (arg == "--table" && i + 1 < argc) {
      table = argv[++i];
    } else if (arg == "--delimiter" && i + 1 < argc) {
      std::string value = argv[++i];
      if (value.size() != 1) {
        std::cerr << "--delimiter expects a single character\n";
        return 2;
      }
      delimiter = value[0];
    } else if (arg == "--vector-column" && i + 1 < argc) {
      vector_column = argv[++i];
    } else if (arg == "--query-vector" && i + 1 < argc) {
      query_vector = argv[++i];
    } else if (arg == "--metric" && i + 1 < argc) {
      metric = argv[++i];
    } else if (arg == "--top-k" && i + 1 < argc) {
      top_k = static_cast<std::size_t>(std::stoull(argv[++i]));
    } else if (arg == "-h" || arg == "--help") {
      printUsage(argv[0]);
      return 0;
    } else {
      std::cerr << "Unknown argument: " << arg << "\n";
      printUsage(argv[0]);
      return 2;
    }
  }

  if (csv_path.empty()) {
    printUsage(argv[0]);
    return 2;
  }

  const bool vector_mode = !vector_column.empty() || !query_vector.empty();
  if (!vector_mode && query.empty()) {
    printUsage(argv[0]);
    return 2;
  }

  try {
    auto& session = dataflow::DataflowSession::builder();
    auto df = session.read_csv(csv_path, delimiter);

    if (vector_mode) {
      auto result = df.toTable();
      if (!result.schema.has(vector_column)) {
        throw std::runtime_error("vector column not found: " + vector_column);
      }
      const auto vector_index = result.schema.indexOf(vector_column);
      const auto needle = parseVectorText(query_vector);
      if (needle.empty()) {
        throw std::runtime_error("query vector cannot be empty");
      }

      struct Candidate {
        std::size_t row_index;
        double distance;
      };
      std::vector<Candidate> candidates;
      candidates.reserve(result.rows.size());

      for (std::size_t i = 0; i < result.rows.size(); ++i) {
        auto vec = parseVectorValue(result.rows[i][vector_index]);
        if (vec.size() != needle.size()) {
          throw std::runtime_error("fixed length vector mismatch at row " + std::to_string(i));
        }
        double distance = 0.0;
        if (metric == "cosine" || metric == "cosin") {
          distance = cosineDistance(vec, needle);
        } else if (metric == "dot") {
          distance = dotScore(vec, needle);
        } else if (metric == "l2") {
          distance = l2Distance(vec, needle);
        } else {
          throw std::runtime_error("unsupported metric: " + metric);
        }
        candidates.push_back(Candidate{i, distance});
      }

      if (metric == "dot") {
        std::sort(candidates.begin(), candidates.end(), [](const Candidate& lhs, const Candidate& rhs) {
          return lhs.distance > rhs.distance;
        });
      } else {
        std::sort(candidates.begin(), candidates.end(), [](const Candidate& lhs, const Candidate& rhs) {
          return lhs.distance < rhs.distance;
        });
      }

      const std::size_t emit = std::min(top_k, candidates.size());
      std::cout << "{\n";
      std::cout << "  \"metric\": \"" << (metric == "cosin" ? "cosine" : metric) << "\",\n";
      std::cout << "  \"top_k\": " << top_k << ",\n";
      std::cout << "  \"rows\": [\n";
      for (std::size_t i = 0; i < emit; ++i) {
        const auto& c = candidates[i];
        std::cout << "    {\"row_index\": " << c.row_index << ", \"distance\": " << c.distance
                  << ", \"row\": [";
        const auto& row = result.rows[c.row_index];
        for (std::size_t j = 0; j < row.size(); ++j) {
          if (j > 0) std::cout << ", ";
          std::cout << valueToJson(row[j]);
        }
        std::cout << "]}";
        if (i + 1 < emit) std::cout << ",";
        std::cout << "\n";
      }
      std::cout << "  ]\n";
      std::cout << "}\n";
      return 0;
    }

    session.createTempView(table, df);
    auto result = session.sql(query).toTable();

    std::cout << "{\n";
    std::cout << "  \"table\": \"" << escapeJson(table) << "\",\n";
    std::cout << "  \"query\": \"" << escapeJson(query) << "\",\n";
    std::cout << "  \"schema\": [";
    for (std::size_t i = 0; i < result.schema.fields.size(); ++i) {
      if (i > 0) {
        std::cout << ", ";
      }
      std::cout << "\"" << escapeJson(result.schema.fields[i]) << "\"";
    }
    std::cout << "],\n";

    std::cout << "  \"rows\": [\n";
    for (std::size_t r = 0; r < result.rows.size(); ++r) {
      std::cout << "    [";
      for (std::size_t c = 0; c < result.rows[r].size(); ++c) {
        if (c > 0) {
          std::cout << ", ";
        }
        std::cout << valueToJson(result.rows[r][c]);
      }
      std::cout << "]";
      if (r + 1 < result.rows.size()) {
        std::cout << ",";
      }
      std::cout << "\n";
    }
    std::cout << "  ]\n";
    std::cout << "}\n";
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "velaria_cli failed: " << ex.what() << "\n";
    return 1;
  }
}
