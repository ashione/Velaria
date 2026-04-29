#include <stdexcept>
#include <cmath>
#include <string>

#include "src/dataflow/core/execution/columnar_batch.h"
#include "src/dataflow/core/execution/nanoarrow_ipc_codec.h"

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
  dataflow::validateTableColumnarCache(table, "columnar test input");

  const auto filtered = dataflow::filterTable(table, selection);
  dataflow::validateTableColumnarCache(filtered, "filtered output");
  expect(filtered.rowCount() == 2, "filterTable row count mismatch");
  auto filtered_rows = filtered;
  dataflow::materializeRows(&filtered_rows);
  expect(filtered_rows.rows[0][0].toString() == " APAC ", "filterTable first row mismatch");
  expect(filtered_rows.rows[1][0].toString() == "EMEA", "filterTable second row mismatch");

  const auto projected = dataflow::projectTable(filtered, {2, 0}, {"needle_alias", "region_alias"});
  expect(projected.schema.fields.size() == 2, "projectTable schema width mismatch");
  expect(projected.schema.fields[0] == "needle_alias", "projectTable first alias mismatch");
  expect(projected.schema.fields[1] == "region_alias", "projectTable second alias mismatch");
  auto projected_rows = projected;
  dataflow::materializeRows(&projected_rows);
  expect(projected_rows.rows[1][1].toString() == "EMEA", "projectTable projected row mismatch");
  const auto limited = dataflow::limitTable(projected, 1);
  expect(limited.rowCount() == 1, "limitTable row count mismatch");
  auto limited_rows = limited;
  dataflow::materializeRows(&limited_rows);
  expect(limited_rows.rows[0][0].toString() == "A", "limitTable row value mismatch");
  auto invalid_cache = *limited.columnar_cache;
  invalid_cache.row_count += 1;
  bool saw_invalid_cache = false;
  try {
    dataflow::validateColumnarCache(invalid_cache, "invalid test cache");
  } catch (const std::runtime_error&) {
    saw_invalid_cache = true;
  }
  expect(saw_invalid_cache, "validateColumnarCache should reject row count mismatch");

  const auto suffix = dataflow::makeConstantStringColumn(table.rowCount(), "_x");
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

  dataflow::Table sliding_window_table(
      dataflow::Schema({"ts", "key"}),
      {{dataflow::Value(int64_t(12000)), dataflow::Value("a")},
       {dataflow::Value("17000"), dataflow::Value("b")},
       {dataflow::Value(), dataflow::Value("null")}})
  ;
  sliding_window_table.columnar_cache = dataflow::makeColumnarCache(sliding_window_table);
  sliding_window_table.rows.clear();

  const auto sliding_out =
      dataflow::assignSlidingWindow(sliding_window_table, 0, 60000, 30000, "window_start", false, false);
  expect(sliding_out.rows.empty(), "sliding window should keep columnar output lazy");
  expect(sliding_out.rowCount() == 5, "sliding window output row count mismatch");
  const auto sliding_ts = dataflow::materializeValueColumn(sliding_out, 2);
  const auto sliding_key = dataflow::materializeValueColumn(sliding_out, 1);
  expect(sliding_ts.values[0].asInt64() == 0, "sliding window first row start mismatch");
  expect(sliding_ts.values[1].asInt64() == -30000, "sliding window second row start mismatch");
  expect(sliding_ts.values[4].isNull(), "sliding window null input should produce null bucket");
  expect(sliding_key.values[0].toString() == "a", "sliding window first output key mismatch");
  expect(sliding_key.values[4].toString() == "null", "sliding window null input key mismatch");

  auto sliding_out_rows = dataflow::assignSlidingWindow(sliding_window_table, 0, 60000, 30000,
                                                       "window_start", true, false);
  expect(sliding_out_rows.rows.size() == 5, "materialized sliding window row count mismatch");

  const auto sliding_out_ts = dataflow::assignSlidingWindow(
      sliding_window_table, 0, 60000, 30000, "window_start_ts", false, true);
  expect(!sliding_out_ts.rows.size(),
         "timestamp sliding window should keep columnar output lazy");
  const auto sliding_ts_ts = dataflow::materializeValueColumn(sliding_out_ts, 2);
  expect(sliding_ts_ts.values[0].type() == dataflow::DataType::String,
         "timestamp sliding window should output string timestamps");
  expect(sliding_ts_ts.values[1].type() == dataflow::DataType::String,
         "timestamp sliding window should output string timestamps");
  expect(sliding_ts_ts.values[4].isNull(),
         "timestamp sliding window null input should still produce null bucket");

  dataflow::Table abs_table(
      dataflow::Schema({"value"}),
      {
          {dataflow::Value(-10.5)},
          {dataflow::Value(2.25)},
          {dataflow::Value()},
          {dataflow::Value(-7.2)},
      });
  const auto abs_input = dataflow::materializeDoubleColumn(abs_table, 0);
  const auto abs_output = dataflow::vectorizedAbs(abs_input);
  expect(abs_output.size() == 4, "abs result size mismatch");
  expect(std::abs(abs_output[0].asDouble() - 10.5) < 1e-9, "abs first row mismatch");
  expect(std::abs(abs_output[1].asDouble() - 2.25) < 1e-9, "abs second row mismatch");
  expect(abs_output[2].isNull(), "abs third row should preserve null");
  expect(std::abs(abs_output[3].asDouble() - 7.2) < 1e-9, "abs fourth row mismatch");

  const auto ceil_output = dataflow::vectorizedCeil(abs_input);
  expect(std::abs(ceil_output[0].asDouble() - (-10.0)) < 1e-9, "ceil first row mismatch");
  expect(std::abs(ceil_output[1].asDouble() - 3.0) < 1e-9, "ceil second row mismatch");
  const auto floor_output = dataflow::vectorizedFloor(abs_input);
  expect(std::abs(floor_output[0].asDouble() - (-11.0)) < 1e-9, "floor first row mismatch");
  expect(std::abs(floor_output[1].asDouble() - 2.0) < 1e-9, "floor second row mismatch");
  const auto round_output = dataflow::vectorizedRound(abs_input);
  expect(std::abs(round_output[0].asDouble() - (-11.0)) < 1e-9, "round first row mismatch");
  expect(std::abs(round_output[1].asDouble() - 2.0) < 1e-9, "round second row mismatch");

  dataflow::Table sort_table(
      dataflow::Schema({"id", "score", "region"}),
      {
          {dataflow::Value(int64_t(1)), dataflow::Value(2.0), dataflow::Value("b")},
          {dataflow::Value(int64_t(2)), dataflow::Value(), dataflow::Value("a")},
          {dataflow::Value(int64_t(3)), dataflow::Value(2.0), dataflow::Value("a")},
          {dataflow::Value(int64_t(4)), dataflow::Value(5.0), dataflow::Value("c")},
      });
  const auto sorted = dataflow::sortTable(sort_table, {1, 2}, {false, true});
  expect(sorted.rowCount() == 4, "sortTable row count mismatch");
  auto sorted_rows = sorted;
  dataflow::materializeRows(&sorted_rows);
  expect(sorted_rows.rows[0][0].asInt64() == 4, "sortTable first row mismatch");
  expect(sorted_rows.rows[1][0].asInt64() == 3, "sortTable second row mismatch");
  expect(sorted_rows.rows[2][0].asInt64() == 1, "sortTable third row mismatch");
  expect(sorted_rows.rows[3][0].asInt64() == 2, "sortTable null row should sort last");

  dataflow::Table lazy_sort_table(
      dataflow::Schema({"id", "score", "region"}),
      {
          {dataflow::Value(int64_t(1)), dataflow::Value(2.0), dataflow::Value("b")},
          {dataflow::Value(int64_t(2)), dataflow::Value(), dataflow::Value("a")},
          {dataflow::Value(int64_t(3)), dataflow::Value(2.0), dataflow::Value("a")},
          {dataflow::Value(int64_t(4)), dataflow::Value(5.0), dataflow::Value("c")},
      });
  lazy_sort_table.columnar_cache = dataflow::makeColumnarCache(lazy_sort_table);
  lazy_sort_table.rows.clear();
  const auto lazy_sorted = dataflow::sortTable(lazy_sort_table, {1, 2}, {false, true});
  expect(lazy_sorted.rowCount() == 4, "lazy sortTable row count mismatch");
  const auto lazy_ids = dataflow::materializeValueColumn(lazy_sorted, 0);
  expect(lazy_ids.values[0].asInt64() == 4, "lazy sortTable first row mismatch");
  expect(lazy_ids.values[1].asInt64() == 3, "lazy sortTable second row mismatch");
  expect(lazy_ids.values[2].asInt64() == 1, "lazy sortTable third row mismatch");
  expect(lazy_ids.values[3].asInt64() == 2, "lazy sortTable null row should sort last");

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
  expect(dataflow::valueColumnValueAt(*name_view.buffer, 0).toString() == "alpha",
         "column view snapshot should remain stable after append");
  const auto upper_snapshot = dataflow::materializeValueColumn(snapshot_table, 1);
  expect(upper_snapshot.values[0].toString() == "ALPHA",
         "appendNamedColumn should append cached values");

  dataflow::Table arrow_source(
      dataflow::Schema({"id", "name"}),
      {
          {dataflow::Value(int64_t(7)), dataflow::Value("seven")},
          {dataflow::Value(int64_t(8)), dataflow::Value("eight")},
      });
  const auto arrow_payload = dataflow::serialize_nanoarrow_ipc_table(arrow_source);
  auto arrow_lazy = dataflow::deserialize_nanoarrow_ipc_table(arrow_payload, false);
  expect(arrow_lazy.rows.empty(), "arrow lazy table should start rowless");
  expect(arrow_lazy.columnar_cache != nullptr, "arrow lazy table should have columnar cache");
  expect(arrow_lazy.columnar_cache->columns[0].values.empty(),
         "arrow lazy table should not pre-materialize value cache");
  dataflow::materializeRows(&arrow_lazy);
  expect(arrow_lazy.rows.size() == 2, "arrow lazy table materialized row count mismatch");
  expect(arrow_lazy.rows[0][0].asInt64() == 7, "arrow lazy first id mismatch");
  expect(arrow_lazy.rows[1][1].toString() == "eight", "arrow lazy second name mismatch");
  expect(arrow_lazy.columnar_cache->columns[0].values.empty(),
         "materializeRows should not force boxed value cache for arrow-backed int column");
  expect(arrow_lazy.columnar_cache->columns[1].values.empty(),
         "materializeRows should not force boxed value cache for arrow-backed string column");
  const auto arrow_limited = dataflow::limitTable(arrow_lazy, 1, false);
  expect(arrow_limited.columnar_cache != nullptr, "arrow limit should preserve columnar cache");
  expect(arrow_limited.columnar_cache->columns[0].arrow_backing != nullptr,
         "arrow limit should preserve arrow backing");
  expect(arrow_limited.columnar_cache->columns[0].arrow_backing->value_buffer.get() ==
             arrow_lazy.columnar_cache->columns[0].arrow_backing->value_buffer.get(),
         "arrow limit should zero-copy share numeric value buffer");
  expect(arrow_limited.columnar_cache->columns[1].arrow_backing->value_buffer.get() ==
             arrow_lazy.columnar_cache->columns[1].arrow_backing->value_buffer.get(),
         "arrow limit should zero-copy share string offsets buffer");
  expect(arrow_limited.columnar_cache->columns[1].arrow_backing->extra_buffer.get() ==
             arrow_lazy.columnar_cache->columns[1].arrow_backing->extra_buffer.get(),
         "arrow limit should zero-copy share string data buffer");

  return 0;
}
