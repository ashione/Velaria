#include "src/dataflow/catalog/catalog.h"

namespace dataflow {

void ViewCatalog::createView(const std::string& name, const DataFrame& df) {
  views_[name] = df;
}

bool ViewCatalog::hasView(const std::string& name) const {
  return views_.find(name) != views_.end();
}

const DataFrame& ViewCatalog::getView(const std::string& name) const {
  auto it = views_.find(name);
  if (it == views_.end()) {
    throw CatalogNotFoundError("view not found: " + name);
  }
  return it->second;
}

}  // namespace dataflow
