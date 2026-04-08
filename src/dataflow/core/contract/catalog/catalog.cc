#include "src/dataflow/core/contract/catalog/catalog.h"

#include "src/dataflow/core/execution/columnar_batch.h"

namespace dataflow {

void ViewCatalog::createView(const std::string& name, const DataFrame& df, sql::TableKind kind) {
  views_[name] = df;
  table_kinds_[name] = kind;
}

void ViewCatalog::createTable(const std::string& name, const std::vector<std::string>& columns,
                             sql::TableKind kind) {
  if (hasView(name)) {
    throw SQLSemanticError("table already exists: " + name);
  }
  views_[name] = DataFrame(Table(Schema(columns), {}));
  table_kinds_[name] = kind;
}

bool ViewCatalog::hasView(const std::string& name) const {
  return views_.find(name) != views_.end();
}

bool ViewCatalog::hasTable(const std::string& name) const {
  return hasView(name);
}

sql::TableKind ViewCatalog::tableKind(const std::string& name) const {
  auto it = table_kinds_.find(name);
  if (it == table_kinds_.end()) {
    if (hasView(name)) {
      return sql::TableKind::Regular;
    }
    throw CatalogNotFoundError("view not found: " + name);
  }
  return it->second;
}

bool ViewCatalog::isSourceTable(const std::string& name) const {
  return tableKind(name) == sql::TableKind::Source;
}

bool ViewCatalog::isSinkTable(const std::string& name) const {
  return tableKind(name) == sql::TableKind::Sink;
}

const DataFrame& ViewCatalog::getView(const std::string& name) const {
  auto it = views_.find(name);
  if (it == views_.end()) {
    throw CatalogNotFoundError("view not found: " + name);
  }
  return it->second;
}

DataFrame& ViewCatalog::getViewMutable(const std::string& name) {
  auto it = views_.find(name);
  if (it == views_.end()) {
    throw CatalogNotFoundError("view not found: " + name);
  }
  return it->second;
}

void ViewCatalog::appendToView(const std::string& name, const Table& appendTable) {
  auto& view = getViewMutable(name);
  const Table& current = view.materializedTable();
  if (!appendTable.schema.fields.empty()) {
    if (current.schema.fields.size() != appendTable.schema.fields.size()) {
      throw SQLSemanticError("column count mismatch");
    }
    for (std::size_t i = 0; i < current.schema.fields.size(); ++i) {
      if (current.schema.fields[i] != appendTable.schema.fields[i]) {
        throw SQLSemanticError("column mismatch: " + appendTable.schema.fields[i]);
      }
    }
  }
  views_[name] = DataFrame(concatenateTables({current, appendTable}, false));
}

}  // namespace dataflow
