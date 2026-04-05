#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "src/dataflow/core/contract/api/session.h"
#include "src/dataflow/core/execution/value.h"

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
    case dataflow::DataType::FixedVector:
      return "\"" + escapeJson(value.toString()) + "\"";
  }
  return "null";
}

void printUsage(const char* program) {
  std::cerr << "Usage: " << program
            << " --csv <path> [--query <sql>] [--table <name>] [--delimiter <char>]\n"
            << "       " << program
            << " --csv <path> --vector-column <name> --query-vector <v1,v2,...>\n"
            << "       [--metric cosine|cosin|dot|l2] [--top-k <n>]\n"
            << "       vector CSV cells should use bracketed values like '[1 2 3]' or '[1,2,3]'\n";
}

std::vector<float> parseVectorText(const std::string& raw) {
  std::string input = raw;
  if (!input.empty() && input.front() == '[' && input.back() == ']') {
    input = input.substr(1, input.size() - 2);
  }
  std::vector<float> out;
  std::string item;
  std::size_t start = 0;
  while (start <= input.size()) {
    const std::size_t end = input.find(',', start);
    item = input.substr(start, end == std::string::npos ? std::string::npos : end - start);
    if (!item.empty()) {
      out.push_back(static_cast<float>(std::stof(item)));
    }
    if (end == std::string::npos) {
      break;
    }
    start = end + 1;
  }
  return out;
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
    session.createTempView(table, df);

    if (vector_mode) {
      const auto needle = parseVectorText(query_vector);
      if (needle.empty()) {
        throw std::runtime_error("query vector cannot be empty");
      }
      dataflow::VectorDistanceMetric runtime_metric = dataflow::VectorDistanceMetric::Cosine;
      if (metric == "dot") {
        runtime_metric = dataflow::VectorDistanceMetric::Dot;
      } else if (metric == "l2") {
        runtime_metric = dataflow::VectorDistanceMetric::L2;
      } else if (metric != "cosine" && metric != "cosin") {
        throw std::runtime_error("unsupported metric: " + metric);
      }
      const auto result =
          session.vectorQuery(table, vector_column, needle, top_k, runtime_metric).toTable();
      auto materialized = result;
      dataflow::materializeRows(&materialized);
      const auto explain =
          session.explainVectorQuery(table, vector_column, needle, top_k, runtime_metric);
      std::cout << "{\n";
      std::cout << "  \"metric\": \"" << (metric == "cosin" ? "cosine" : metric) << "\",\n";
      std::cout << "  \"top_k\": " << top_k << ",\n";
      std::cout << "  \"explain\": \"" << escapeJson(explain) << "\",\n";
      std::cout << "  \"rows\": [\n";
      for (std::size_t i = 0; i < materialized.rows.size(); ++i) {
        const auto& row = materialized.rows[i];
        std::cout << "    {";
        for (std::size_t j = 0; j < row.size(); ++j) {
          if (j > 0) std::cout << ", ";
          std::cout << "\"" << escapeJson(materialized.schema.fields[j]) << "\": "
                    << valueToJson(row[j]);
        }
        std::cout << "}";
        if (i + 1 < materialized.rows.size()) {
          std::cout << ",";
        }
        std::cout << "\n";
      }
      std::cout << "  ]\n";
      std::cout << "}\n";
      return 0;
    }

    auto result = session.sql(query).toTable();
    dataflow::materializeRows(&result);

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
