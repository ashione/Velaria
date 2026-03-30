#pragma once

#include <stdexcept>
#include <string>

namespace dataflow {

struct SQLSyntaxError : std::runtime_error {
  explicit SQLSyntaxError(const std::string& msg) : std::runtime_error(msg) {}
};

struct SQLSemanticError : std::runtime_error {
  explicit SQLSemanticError(const std::string& msg) : std::runtime_error(msg) {}
};

struct CatalogNotFoundError : std::runtime_error {
  explicit CatalogNotFoundError(const std::string& msg) : std::runtime_error(msg) {}
};

}  // namespace dataflow
