#include "src/dataflow/core/contract/api/dataframe.h"
#include "src/dataflow/core/contract/api/session.h"

#include <fstream>

int main() {
  const char* peoplePath = "/tmp/dataflow_people.csv";
  {
    std::ofstream f(peoplePath);
    f << "name,dept,score\n"
         "alice,eng,10\n"
         "bob,sales,20\n"
         "alice,eng,30\n"
         "eve,sales,40\n"
         "eve,eng,50\n";
  }

  const char* bonusPath = "/tmp/dataflow_bonus.csv";
  {
    std::ofstream f(bonusPath);
    f << "dept,bonus\n"
         "eng,5\n"
         "sales,7\n";
  }

  auto& session = dataflow::DataflowSession::builder();
  session.createTempView("people", session.read_csv(peoplePath));
  session.createTempView("bonus", session.read_csv(bonusPath));

  auto grouped = session.sql(
      "SELECT name, SUM(score) AS total FROM people WHERE score > 0 GROUP BY name HAVING total > 20 LIMIT 10");
  grouped.show();

  auto joined = session.sql(
      "SELECT a.name, a.dept, b.bonus FROM people a INNER JOIN bonus b ON a.dept = b.dept");
  joined.show();

  auto complex_query = session.sql(
      "SELECT a.dept, a.name, SUM(a.score) AS total_score, COUNT(*) AS cnt, "
      "MAX(a.score) AS max_score "
      "FROM people a INNER JOIN bonus b ON a.dept = b.dept "
      "WHERE a.score > 10 "
      "GROUP BY a.dept, a.name "
      "HAVING total_score > 20 "
      "LIMIT 20");
  complex_query.show();

  auto create_rollup = session.sql(
      "CREATE TABLE people_rollup (dept STRING, name STRING, total_score INT)");
  create_rollup.show();

  auto load_rollup = session.sql(
      "INSERT INTO people_rollup "
      "SELECT a.dept AS dept, a.name AS name, SUM(a.score) AS total_score "
      "FROM people a "
      "INNER JOIN bonus b ON a.dept = b.dept "
      "WHERE a.score > 10 "
      "GROUP BY a.dept, a.name "
      "HAVING total_score > 20");
  load_rollup.show();

  auto final_report = session.sql(
      "SELECT dept, name, total_score FROM people_rollup WHERE total_score >= 30 LIMIT 10");
  final_report.show();

  return 0;
}
