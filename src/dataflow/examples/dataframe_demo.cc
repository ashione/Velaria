#include "src/dataflow/core/contract/api/dataframe.h"
#include "src/dataflow/core/contract/api/session.h"

#include <fstream>

int main() {
  const char* path = "/tmp/dataflow_demo.csv";
  {
    std::ofstream f(path);
    f << "name,score\n"
         "alice,10\n"
         "bob,20\n"
         "alice,30\n"
         "eve,40\n";
  }

  auto& session = dataflow::DataflowSession::builder();
  auto df = session.read_csv(path);

  session.createTempView("people", df);

  auto top2 = session.sql("SELECT * FROM people LIMIT 2");
  top2.show(10);

  auto g = session.sql("SELECT name, SUM(score) AS total FROM people GROUP BY name HAVING total > 20 LIMIT 10");
  g.show(10);

  return 0;
}
