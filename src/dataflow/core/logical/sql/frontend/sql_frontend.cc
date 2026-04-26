#include "src/dataflow/core/logical/sql/frontend/sql_frontend.h"
#include <cstdlib>
#include <cstring>

namespace dataflow {
namespace sql {

SqlFrontendConfig sqlFrontendConfigFromEnv() {
  SqlFrontendConfig config;
  const char* env = std::getenv("VELARIA_SQL_FRONTEND");
  if (env == nullptr) return config;
  if (std::strcmp(env, "pg_query") == 0) config.kind = SqlFrontendKind::PgQuery;
  else if (std::strcmp(env, "dual") == 0) config.kind = SqlFrontendKind::Dual;
  return config;
}

}  // namespace sql
}  // namespace dataflow
