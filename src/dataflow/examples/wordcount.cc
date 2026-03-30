#include "src/dataflow/core/contract/api/dataframe.h"

#include <string>
#include <unordered_map>

int main() {
  dataflow::Table t;
  t.schema = dataflow::Schema({"word", "count"});
  t.rows = {
      {dataflow::Value("cpp"), dataflow::Value(int64_t(1))},
      {dataflow::Value("cpp"), dataflow::Value(int64_t(2))},
      {dataflow::Value("cpp"), dataflow::Value(int64_t(3))},
  };

  auto df = dataflow::DataFrame(t);
  auto agg = df.groupBy({"word"}).sum("count");
  agg.show();

  return 0;
}
