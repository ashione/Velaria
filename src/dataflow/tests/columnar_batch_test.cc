#include <stdexcept>
#include <string>

#include "src/dataflow/core/execution/columnar_batch.h"

namespace {

void expect(bool ok, const std::string& message) {
  if (!ok) {
    throw std::runtime_error(message);
  }
}

bool gteValue(const dataflow::Value& lhs, const dataflow::Value& rhs) {
  return lhs > rhs || lhs == rhs;
}

}  // namespace

int main() {
  dataflow::Table table(
      dataflow::Schema({"region", "n", "needle"}),
      {
          {dataflow::Value(" APAC "), dataflow::Value(int64_t(2)), dataflow::Value("A")},
          {dataflow::Value(), dataflow::Value(int64_t(1)), dataflow::Value("E")},
          {dataflow::Value("EMEA"), dataflow::Value(int64_t(3)), dataflow::Value("M")},
      });

  const auto region = dataflow::materializeStringColumn(table, 0);
  const auto n = dataflow::materializeInt64Column(table, 1);
  const auto needle = dataflow::materializeStringColumn(table, 2);

  const auto lower = dataflow::vectorizedStringLower(region);
  expect(lower.size() == 3, "lower result size mismatch");
  expect(lower[0].toString() == " apac ", "lower first row mismatch");
  expect(lower[1].isNull(), "lower should preserve null");

  const auto trim = dataflow::vectorizedStringTrim(region);
  expect(trim[0].toString() == "APAC", "trim first row mismatch");

  const auto left = dataflow::vectorizedStringLeft(region, n);
  expect(left[0].toString() == " A", "left first row mismatch");
  expect(left[1].isNull(), "left should preserve null");
  expect(left[2].toString() == "EME", "left third row mismatch");

  const auto pos = dataflow::vectorizedStringPosition(needle, region);
  expect(pos[0].asInt64() == 2, "position first row mismatch");
  expect(pos[1].isNull(), "position should preserve null");
  expect(pos[2].asInt64() == 2, "position third row mismatch");

  const auto value_column = dataflow::materializeValueColumn(table, 1);
  const auto selection = dataflow::vectorizedFilterSelection(
      value_column, dataflow::Value(int64_t(2)), gteValue);
  expect(selection.selected_count == 2, "selection count mismatch");

  const auto filtered = dataflow::filterTable(table, selection);
  expect(filtered.rows.size() == 2, "filterTable row count mismatch");
  expect(filtered.rows[0][0].toString() == " APAC ", "filterTable first row mismatch");
  expect(filtered.rows[1][0].toString() == "EMEA", "filterTable second row mismatch");

  const auto projected = dataflow::projectTable(filtered, {2, 0}, {"needle_alias", "region_alias"});
  expect(projected.schema.fields.size() == 2, "projectTable schema width mismatch");
  expect(projected.schema.fields[0] == "needle_alias", "projectTable first alias mismatch");
  expect(projected.schema.fields[1] == "region_alias", "projectTable second alias mismatch");
  expect(projected.rows[1][1].toString() == "EMEA", "projectTable projected row mismatch");
  const auto limited = dataflow::limitTable(projected, 1);
  expect(limited.rows.size() == 1, "limitTable row count mismatch");
  expect(limited.rows[0][0].toString() == "A", "limitTable row value mismatch");

  const auto suffix = dataflow::makeConstantStringColumn(table.rows.size(), "_x");
  const auto concat = dataflow::vectorizedStringConcat({region, suffix});
  expect(concat[0].toString() == " APAC _x", "concat first row mismatch");
  expect(concat[1].isNull(), "concat should preserve null");

  dataflow::Table window_table(
      dataflow::Schema({"ts"}),
      {
          {dataflow::Value(int64_t(12000))},
          {dataflow::Value("17000")},
      });
  const auto ts = dataflow::materializeValueColumn(window_table, 0);
  const auto window = dataflow::vectorizedWindowStart(ts, 5000);
  expect(window.size() == 2, "window result size mismatch");
  expect(window[0].asInt64() == 10000, "window first row mismatch");
  expect(window[1].asInt64() == 15000, "window second row mismatch");

  const auto serialized_keys = dataflow::materializeSerializedKeys(table, {0, 2});
  expect(serialized_keys.size() == 3, "serialized key count mismatch");
  expect(serialized_keys[0] == (std::string(" APAC ") + '\x1f' + "A"),
         "serialized key first row mismatch");
  expect(serialized_keys[1] == (std::string("null") + '\x1f' + "E"),
         "serialized key second row mismatch");
  const auto buckets = dataflow::buildHashBuckets(serialized_keys);
  expect(buckets.size() == 3, "hash bucket size mismatch");
  expect(buckets.at(std::string("EMEA") + '\x1f' + "M").size() == 1,
         "hash bucket entry mismatch");

  dataflow::Table lazy_cache_table(
      dataflow::Schema({"id", "label"}),
      {
          {dataflow::Value(int64_t(1)), dataflow::Value("one")},
          {dataflow::Value(int64_t(2)), dataflow::Value("two")},
      });
  expect(!lazy_cache_table.columnar_cache, "lazy cache should start empty");
  const auto lazy_values = dataflow::materializeValueColumn(lazy_cache_table, 1);
  expect(lazy_cache_table.columnar_cache != nullptr, "materializeValueColumn should hydrate cache");
  expect(lazy_values.values[0].toString() == "one", "lazy cache first value mismatch");
  expect(lazy_values.values[1].toString() == "two", "lazy cache second value mismatch");

  table.columnar_cache = dataflow::makeColumnarCache(table);
  dataflow::Table table_copy = table;
  table_copy.schema.fields.push_back("region_copy");
  table_copy.schema.index["region_copy"] = table_copy.schema.fields.size() - 1;
  dataflow::appendColumn(&table_copy, dataflow::materializeValueColumn(table_copy, 0).values);
  expect(table.schema.fields.size() == 3, "table copy should not mutate original schema");
  expect(table.rows[0].size() == 3, "table copy should not mutate original rows");
  const auto original_region = dataflow::materializeValueColumn(table, 0);
  expect(original_region.values.size() == 3, "original cached column size mismatch");
  expect(original_region.values[0].toString() == " APAC ", "original cached column value mismatch");
  expect(table_copy.rows[0].size() == 4, "table copy append should extend copied rows");

  dataflow::appendColumn(&table, dataflow::vectorizedStringUpper(region));
  expect(table.schema.fields.size() == 3, "appendColumn should not alter schema");
  expect(table.rows[0].size() == 4, "appendColumn should extend rows");
  expect(table.rows[0][3].toString() == " APAC ", "upper preserves spaces");

  dataflow::Table snapshot_table(
      dataflow::Schema({"name"}),
      {
          {dataflow::Value("alpha")},
          {dataflow::Value("beta")},
      });
  const auto name_view = dataflow::viewValueColumn(snapshot_table, 0);
  dataflow::appendNamedColumn(
      &snapshot_table, "name_upper",
      dataflow::vectorizedStringUpper(dataflow::materializeStringColumn(snapshot_table, 0)));
  expect(snapshot_table.schema.has("name_upper"), "appendNamedColumn should update schema");
  expect(snapshot_table.columnar_cache != nullptr, "appendNamedColumn should hydrate cache");
  expect(name_view.values()[0].toString() == "alpha",
         "column view snapshot should remain stable after append");
  const auto upper_snapshot = dataflow::materializeValueColumn(snapshot_table, 1);
  expect(upper_snapshot.values[0].toString() == "ALPHA",
         "appendNamedColumn should append cached values");

  return 0;
}
