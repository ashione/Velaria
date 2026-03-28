#include "src/dataflow/api/dataframe.h"
#include "src/dataflow/api/session.h"

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

  auto& spark = dataflow::SparkSession::builder();
  auto df = spark.read_csv(path);

  spark.createTempView("people", df);

  auto top2 = df.select({"name", "score"}).filter("score", ">", dataflow::Value(int64_t(15))).limit(10);
  top2.show();

  auto g = df.groupBy({"name"}).sum("score");
  g.show();

  auto limited = spark.sql("SELECT * FROM people LIMIT 2");
  limited.show(10);

  return 0;
}
