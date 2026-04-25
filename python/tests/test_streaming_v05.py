import csv
import pathlib
import tempfile
import unittest

import pyarrow as pa

import velaria  # noqa: E402


class StreamingV05Test(unittest.TestCase):
    def _run_batch_sql(self, session, query):
        try:
            return session.sql(query)
        except RuntimeError as exc:
            message = str(exc)
            if "not supported in SQL v1" in message and "scalar function" in message:
                self.skipTest(message)
            raise

    def _run_stream_sql(self, session, query, **kwargs):
        try:
            return session.start_stream_sql(query, **kwargs)
        except RuntimeError as exc:
            message = str(exc)
            unsupported = (
                "not supported in SQL v1" in message
                or "stream SQL does not support string functions" in message
                or "stream SQL does not support string functions in aggregate query" in message
            )
            if unsupported:
                self.skipTest(message)
            raise

    def test_batch_arrow_roundtrip_and_sql(self):
        session = velaria.Session()
        table = pa.table({"id": [1, 2, 3], "value": [10, 20, 30]})
        df = session.create_dataframe_from_arrow(table)
        session.create_temp_view("batch_input", df)

        out = session.sql("SELECT id, value FROM batch_input WHERE value > 15")
        rows = out.to_rows()
        self.assertEqual(rows["schema"], ["id", "value"])
        self.assertEqual(rows["rows"], [[2, 20], [3, 30]])

    def test_batch_sql_supports_numeric_date_and_string_functions(self):
        session = velaria.Session()
        table = pa.table(
            {
                "id": [1, 2, 3],
                "ts": [
                    "2026-03-29T10:00:00",
                    "2026-03-29T10:01:00",
                    "2026-04-01T08:00:00",
                ],
                "value": [-1.5, 2.25, 0.0],
                "name": ["Alice", "Bob", "cARl"],
            }
        )
        df = session.create_dataframe_from_arrow(table)
        session.create_temp_view("batch_function_input", df)

        rows = self._run_batch_sql(
            session,
            "SELECT id, ABS(value) AS abs_value, YEAR(ts) AS year, MONTH(ts) AS month, DAY(ts) AS day, LOWER(name) AS lower_name "
            "FROM batch_function_input",
        ).to_rows()

        self.assertEqual(rows["schema"], ["id", "abs_value", "year", "month", "day", "lower_name"])
        expected = {
            1: (1.5, 2026, 3, 29, "alice"),
            2: (2.25, 2026, 3, 29, "bob"),
            3: (0.0, 2026, 4, 1, "carl"),
        }
        for row in rows["rows"]:
            row_id = row[0]
            abs_value, year, month, day, lower_name = row[1], row[2], row[3], row[4], row[5]
            exp = expected[row_id]
            self.assertAlmostEqual(float(abs_value), exp[0], places=6)
            self.assertEqual(int(year), exp[1])
            self.assertEqual(int(month), exp[2])
            self.assertEqual(int(day), exp[3])
            self.assertEqual(lower_name, exp[4])

    def test_batch_sql_supports_order_by(self):
        session = velaria.Session()
        table = pa.table(
            {
                "id": [1, 2, 3, 4],
                "value": [-1.5, 2.25, 0.0, 7.4],
                "name": ["cc", "bb", "dd", "aa"],
            }
        )
        df = session.create_dataframe_from_arrow(table)
        session.create_temp_view("batch_order_input", df)

        rows = self._run_batch_sql(
            session,
            "SELECT id, ABS(value) AS abs_value, LOWER(name) AS lower_name "
            "FROM batch_order_input ORDER BY abs_value DESC, lower_name ASC",
        ).to_rows()

        self.assertEqual([row[0] for row in rows["rows"]], [4, 2, 1, 3])

    def test_batch_sql_supports_iso_week_group_by_expressions(self):
        session = velaria.Session()
        table = pa.table(
            {
                "ts": [
                    "2025-12-29",
                    "2025-12-31",
                    "2026-01-01",
                    "2026-01-05",
                ],
                "value": [10, 20, 30, 40],
            }
        )
        df = session.create_dataframe_from_arrow(table)
        session.create_temp_view("batch_week_input", df)

        rows = self._run_batch_sql(
            session,
            "SELECT ISO_YEAR(ts) AS iso_year, WEEK(ts) AS iso_week, YEARWEEK(ts) AS year_week, "
            "COUNT(*) AS n, MIN(value) AS min_value, MAX(value) AS max_value, AVG(value) AS avg_value "
            "FROM batch_week_input "
            "GROUP BY ISO_YEAR(ts), WEEK(ts), YEARWEEK(ts) "
            "ORDER BY iso_year, iso_week",
        ).to_rows()

        self.assertEqual(
            rows["schema"],
            ["iso_year", "iso_week", "year_week", "n", "min_value", "max_value", "avg_value"],
        )
        self.assertEqual(len(rows["rows"]), 2)
        self.assertEqual(rows["rows"][0][:6], [2026, 1, 202601, 3, 10, 30])
        self.assertAlmostEqual(float(rows["rows"][0][6]), 20.0, places=6)
        self.assertEqual(rows["rows"][1][:6], [2026, 2, 202602, 1, 40, 40])
        self.assertAlmostEqual(float(rows["rows"][1][6]), 40.0, places=6)

    def test_stream_progress_contract_with_start_stream_sql(self):
        session = velaria.Session()
        table = pa.table(
            {
                "ts": ["2026-03-29T10:00:00", "2026-03-29T10:00:10", "2026-03-29T10:01:00"],
                "key": ["u1", "u1", "u2"],
                "value": [1, 2, 3],
            }
        )
        stream_df = session.create_stream_from_arrow(table)
        session.create_temp_view("events_stream", stream_df)

        with tempfile.TemporaryDirectory(prefix="velaria-py-sink-") as tmp:
            sink_path = str(pathlib.Path(tmp) / "sink.csv")
            session.sql(
                f"CREATE SINK TABLE sink_out (key STRING, value_sum INT) USING csv OPTIONS(path: '{sink_path}', delimiter: ',')"
            )
            query = session.start_stream_sql(
                "INSERT INTO sink_out SELECT key, SUM(value) AS value_sum FROM events_stream GROUP BY key",
                trigger_interval_ms=0,
                checkpoint_delivery_mode="best-effort",
            )
            query.start()
            processed = query.await_termination()
            progress = query.progress()

            self.assertEqual(processed, 1)
            self.assertEqual(progress["status"], "stopped")
            self.assertEqual(progress["execution_mode"], "single-process")
            self.assertIn("execution_reason", progress)
            self.assertIn("transport_mode", progress)
            self.assertEqual(progress["checkpoint_delivery_mode"], "best-effort")
            self.assertGreaterEqual(progress["batches_processed"], 1)
            self.assertIn(progress["source_is_bounded"], [True, False])
            self.assertIn("estimated_state_size_bytes", progress)
            self.assertIn("estimated_batch_cost", progress)
            self.assertIn("backpressure_max_queue_batches", progress)
            self.assertIn("backpressure_high_watermark", progress)
            self.assertIn("backpressure_low_watermark", progress)

            snapshot_json = query.snapshot_json()
            self.assertIn("execution_mode", snapshot_json)
            self.assertIn("checkpoint_delivery_mode", snapshot_json)

    def test_start_stream_sql_supports_multi_aggregate_and_having_alias(self):
        session = velaria.Session()
        table = pa.table(
            {
                "ts": [
                    "2026-03-29T10:00:00",
                    "2026-03-29T10:00:10",
                    "2026-03-29T10:00:20",
                    "2026-03-29T10:00:30",
                ],
                "key": ["u1", "u1", "u1", "u2"],
                "value": [10, 5, 7, 4],
            }
        )
        stream_df = session.create_stream_from_arrow(table)
        session.create_temp_view("events_stream_multi", stream_df)

        with tempfile.TemporaryDirectory(prefix="velaria-py-stream-multi-") as tmp:
            sink_path = str(pathlib.Path(tmp) / "sink.csv")
            session.sql(
                "CREATE SINK TABLE sink_multi "
                "(window_start STRING, key STRING, event_count INT, value_sum INT, "
                "min_value INT, max_value INT, avg_value DOUBLE) "
                f"USING csv OPTIONS(path: '{sink_path}', delimiter: ',')"
            )
            query = session.start_stream_sql(
                "INSERT INTO sink_multi "
                "SELECT window_start, key, COUNT(*) AS event_count, SUM(value) AS value_sum, "
                "MIN(value) AS min_value, MAX(value) AS max_value, AVG(value) AS avg_value "
                "FROM events_stream_multi "
                "WINDOW BY ts EVERY 60000 AS window_start "
                "GROUP BY window_start, key HAVING avg_value > 6",
                trigger_interval_ms=0,
            )
            query.start()
            processed = query.await_termination()

            self.assertEqual(processed, 1)

            with open(sink_path, newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["window_start"], "2026-03-29T10:00:00")
            self.assertEqual(rows[0]["key"], "u1")
            self.assertEqual(float(rows[0]["event_count"]), 3.0)
            self.assertEqual(float(rows[0]["value_sum"]), 22.0)
            self.assertEqual(float(rows[0]["min_value"]), 5.0)
            self.assertEqual(float(rows[0]["max_value"]), 10.0)
            self.assertAlmostEqual(float(rows[0]["avg_value"]), 22.0 / 3.0, places=5)

    def test_start_stream_sql_hits_local_workers_grouped_hot_path(self):
        session = velaria.Session()
        table = pa.table(
            {
                "segment": ["alpha", "alpha", "alpha", "beta", "beta"],
                "bucket": [1, 1, 2, 1, 1],
                "value": [10, 14, 3, 6, 8],
            }
        )
        stream_df = session.create_stream_from_arrow(table)
        session.create_temp_view("events_stream_local_workers", stream_df)

        with tempfile.TemporaryDirectory(prefix="velaria-py-stream-hot-path-") as tmp:
            sink_path = str(pathlib.Path(tmp) / "sink.csv")
            session.sql(
                "CREATE SINK TABLE sink_local_workers "
                "(segment STRING, bucket INT, value_sum INT, event_count INT, "
                "min_value INT, max_value INT, avg_value DOUBLE) "
                f"USING csv OPTIONS(path: '{sink_path}', delimiter: ',')"
            )
            query = session.start_stream_sql(
                "INSERT INTO sink_local_workers "
                "SELECT segment, bucket, SUM(value) AS value_sum, COUNT(*) AS event_count, "
                "MIN(value) AS min_value, MAX(value) AS max_value, AVG(value) AS avg_value "
                "FROM events_stream_local_workers GROUP BY segment, bucket",
                trigger_interval_ms=0,
                execution_mode="local-workers",
                local_workers=4,
                max_inflight_partitions=4,
            )
            query.start()
            processed = query.await_termination()
            progress = query.progress()

            self.assertEqual(processed, 1)
            self.assertEqual(progress["execution_mode"], "local-workers")
            self.assertTrue(progress["actor_eligible"])
            self.assertTrue(progress["used_actor_runtime"])
            self.assertIn(progress["transport_mode"], ["shared-memory", "rpc-copy"])

            with open(sink_path, newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

            keyed = {(row["segment"], int(float(row["bucket"]))): row for row in rows}
            self.assertEqual(len(keyed), 3)
            self.assertEqual(float(keyed[("alpha", 1)]["value_sum"]), 24.0)
            self.assertEqual(float(keyed[("alpha", 1)]["event_count"]), 2.0)
            self.assertEqual(float(keyed[("alpha", 1)]["min_value"]), 10.0)
            self.assertEqual(float(keyed[("alpha", 1)]["max_value"]), 14.0)
            self.assertAlmostEqual(float(keyed[("alpha", 1)]["avg_value"]), 12.0, places=5)
            self.assertEqual(float(keyed[("alpha", 2)]["value_sum"]), 3.0)
            self.assertEqual(float(keyed[("beta", 1)]["value_sum"]), 14.0)
            self.assertAlmostEqual(float(keyed[("beta", 1)]["avg_value"]), 7.0, places=5)

    def test_start_stream_sql_supports_sliding_window_outputs(self):
        session = velaria.Session()
        table = pa.table(
            {
                "ts": [
                    "2026-03-29T10:00:00",
                    "2026-03-29T10:00:10",
                    "2026-03-29T10:00:35",
                ],
                "key": ["u1", "u1", "u1"],
                "value": [1, 1, 1],
            }
        )
        stream_df = session.create_stream_from_arrow(table)
        session.create_temp_view("events_stream_sliding", stream_df)

        with tempfile.TemporaryDirectory(prefix="velaria-py-stream-sliding-") as tmp:
            sink_path = str(pathlib.Path(tmp) / "sink.csv")
            session.sql(
                "CREATE SINK TABLE sink_sliding "
                "(window_start STRING, key STRING, event_count INT) "
                f"USING csv OPTIONS(path: '{sink_path}', delimiter: ',')"
            )
            query = session.start_stream_sql(
                "INSERT INTO sink_sliding "
                "SELECT window_start, key, COUNT(*) AS event_count "
                "FROM events_stream_sliding "
                "WINDOW BY ts EVERY 60000 SLIDE 30000 AS window_start "
                "GROUP BY window_start, key",
                trigger_interval_ms=0,
            )
            query.start()
            processed = query.await_termination()

            self.assertEqual(processed, 1)

            with open(sink_path, newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

            got = {(row["window_start"], row["key"], int(float(row["event_count"]))) for row in rows}
            self.assertEqual(
                got,
                {
                    ("2026-03-29T09:59:30", "u1", 2),
                    ("2026-03-29T10:00:00", "u1", 3),
                    ("2026-03-29T10:00:30", "u1", 1),
                },
            )

    def test_start_stream_sql_zero_worker_settings_fall_back_to_inproc_local_workers(self):
        session = velaria.Session()
        table = pa.table(
            {
                "ts": ["2026-03-29T10:00:00", "2026-03-29T10:00:10"],
                "key": ["u1", "u1"],
                "value": [1, 2],
            }
        )
        stream_df = session.create_stream_from_arrow(table)
        session.create_temp_view("events_stream_zero_workers", stream_df)

        with tempfile.TemporaryDirectory(prefix="velaria-py-stream-zero-workers-") as tmp:
            sink_path = str(pathlib.Path(tmp) / "sink.csv")
            session.sql(
                "CREATE SINK TABLE sink_zero_workers "
                "(window_start STRING, key STRING, value_sum INT) "
                f"USING csv OPTIONS(path: '{sink_path}', delimiter: ',')"
            )
            query = session.start_stream_sql(
                "INSERT INTO sink_zero_workers "
                "SELECT window_start, key, SUM(value) AS value_sum "
                "FROM events_stream_zero_workers "
                "WINDOW BY ts EVERY 60000 AS window_start "
                "GROUP BY window_start, key",
                trigger_interval_ms=0,
                execution_mode="local-workers",
                local_workers=0,
                max_inflight_partitions=0,
            )
            query.start()
            processed = query.await_termination()
            progress = query.progress()

            self.assertEqual(processed, 1)
            self.assertEqual(progress["execution_mode"], "local-workers")
            self.assertFalse(progress["used_actor_runtime"])
            self.assertEqual(progress["transport_mode"], "inproc")
            self.assertIn("local_workers > 1", progress["execution_reason"])

            with open(sink_path, newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["window_start"], "2026-03-29T10:00:00")
            self.assertEqual(rows[0]["key"], "u1")
            self.assertEqual(float(rows[0]["value_sum"]), 3.0)

    def test_explain_stream_sql_returns_strategy_sections(self):
        session = velaria.Session()
        table = pa.table(
            {
                "ts": ["2026-03-29T10:00:00", "2026-03-29T10:00:10"],
                "key": ["u1", "u1"],
                "value": [1, 2],
            }
        )
        stream_df = session.create_stream_from_arrow(table)
        session.create_temp_view("events_stream_explain", stream_df)

        explain = session.explain_stream_sql(
            "SELECT window_start, key, SUM(value) AS value_sum "
            "FROM events_stream_explain "
            "WINDOW BY ts EVERY 60000 AS window_start "
            "GROUP BY window_start, key",
            trigger_interval_ms=0,
            checkpoint_delivery_mode="best-effort",
        )

        self.assertIn("logical\n", explain)
        self.assertIn("physical\n", explain)
        self.assertIn("strategy\n", explain)
        self.assertIn("selected_mode", explain)
        self.assertIn("checkpoint_delivery_mode=best-effort", explain)

    def test_explain_stream_sql_reports_local_workers_grouped_hot_path(self):
        session = velaria.Session()
        table = pa.table(
            {
                "segment": ["alpha", "alpha", "beta"],
                "bucket": [1, 1, 1],
                "value": [10, 14, 6],
            }
        )
        stream_df = session.create_stream_from_arrow(table)
        session.create_temp_view("events_stream_local_workers_explain", stream_df)

        explain = session.explain_stream_sql(
            "SELECT segment, bucket, SUM(value) AS value_sum, COUNT(*) AS event_count, "
            "MIN(value) AS min_value, MAX(value) AS max_value, AVG(value) AS avg_value "
            "FROM events_stream_local_workers_explain "
            "GROUP BY segment, bucket",
            trigger_interval_ms=0,
            execution_mode="local-workers",
            local_workers=4,
            max_inflight_partitions=4,
        )

        self.assertIn("Aggregate keys=[segment, bucket]", explain)
        self.assertIn("actor_eligible=true", explain)
        self.assertIn("selected_mode=local-workers", explain)

    def test_explain_stream_sql_reports_having_actor_fallback_reason(self):
        session = velaria.Session()
        table = pa.table(
            {
                "ts": ["2026-03-29T10:00:00", "2026-03-29T10:00:10"],
                "key": ["u1", "u1"],
                "value": [1, 2],
            }
        )
        stream_df = session.create_stream_from_arrow(table)
        session.create_temp_view("events_stream_having_explain", stream_df)

        explain = session.explain_stream_sql(
            "SELECT window_start, key, SUM(value) AS value_sum "
            "FROM events_stream_having_explain "
            "WINDOW BY ts EVERY 60000 AS window_start "
            "GROUP BY window_start, key HAVING value_sum > 0",
            trigger_interval_ms=0,
        )

        reason = (
            "actor acceleration requires the eligible grouped aggregate to be the final stream transform"
        )
        self.assertIn("actor_eligible=false", explain)
        self.assertIn(f"actor_eligibility_reason={reason}", explain)
        self.assertIn(f"reason={reason}", explain)

    def test_start_stream_sql_supports_numeric_date_and_string_functions(self):
        session = velaria.Session()
        table = pa.table(
            {
                "id": [1, 2, 3, 4],
                "ts": [
                    "2026-03-29T10:00:00",
                    "2026-03-29T10:01:00",
                    "2026-03-30T11:15:00",
                    "2026-04-01T08:00:00",
                ],
                "value": [-1.5, 2.25, -0.0, 7.4],
                "key": ["aa", "bb", "cc", "dd"],
            }
        )
        stream_df = session.create_stream_from_arrow(table)
        session.create_temp_view("batchless_stream_function_input", stream_df)

        with tempfile.TemporaryDirectory(prefix="velaria-py-stream-function-") as tmp:
            sink_path = str(pathlib.Path(tmp) / "sink.csv")
            session.sql(
                "CREATE SINK TABLE stream_function_out "
                "(id INT, abs_value DOUBLE, year INT, month INT, day INT, lower_name STRING) "
                f"USING csv OPTIONS(path: '{sink_path}', delimiter: ',')"
            )
            query = self._run_stream_sql(
                session,
                "INSERT INTO stream_function_out "
                "SELECT id, ABS(value) AS abs_value, YEAR(ts) AS year, MONTH(ts) AS month, DAY(ts) AS day, LOWER(key) AS lower_name "
                "FROM batchless_stream_function_input",
                trigger_interval_ms=0,
                execution_mode="local-workers",
                local_workers=4,
                max_inflight_partitions=4,
                checkpoint_delivery_mode="best-effort",
            )
            query.start()
            processed = query.await_termination()
            self.assertEqual(processed, 1)

            with open(sink_path, newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(len(rows), 4)
            rows_by_id = {
                int(float(row["id"])): row for row in rows
            }
            self.assertAlmostEqual(float(rows_by_id[1]["abs_value"]), 1.5, places=6)
            self.assertEqual(int(float(rows_by_id[1]["year"])), 2026)
            self.assertEqual(int(float(rows_by_id[1]["month"])), 3)
            self.assertEqual(int(float(rows_by_id[1]["day"])), 29)
            self.assertAlmostEqual(float(rows_by_id[2]["abs_value"]), 2.25, places=6)
            self.assertEqual(int(float(rows_by_id[2]["year"])), 2026)
            self.assertEqual(int(float(rows_by_id[2]["month"])), 3)
            self.assertEqual(int(float(rows_by_id[2]["day"])), 29)
            self.assertAlmostEqual(float(rows_by_id[3]["abs_value"]), 0.0, places=6)
            self.assertEqual(int(float(rows_by_id[3]["year"])), 2026)
            self.assertEqual(int(float(rows_by_id[3]["month"])), 3)
            self.assertEqual(int(float(rows_by_id[3]["day"])), 30)
            self.assertAlmostEqual(float(rows_by_id[4]["abs_value"]), 7.4, places=6)
            self.assertEqual(int(float(rows_by_id[4]["year"])), 2026)
            self.assertEqual(int(float(rows_by_id[4]["month"])), 4)
            self.assertEqual(int(float(rows_by_id[4]["day"])), 1)
            self.assertEqual(rows_by_id[1]["lower_name"], "aa")
            self.assertEqual(rows_by_id[2]["lower_name"], "bb")
            self.assertEqual(rows_by_id[3]["lower_name"], "cc")
            self.assertEqual(rows_by_id[4]["lower_name"], "dd")

    def test_start_stream_sql_supports_order_by(self):
        session = velaria.Session()
        table = pa.table(
            {
                "id": [1, 2, 3, 4],
                "value": [-1.5, 2.25, 0.0, 7.4],
                "key": ["cc", "bb", "dd", "aa"],
            }
        )
        stream_df = session.create_stream_from_arrow(table)
        session.create_temp_view("stream_order_input", stream_df)

        with tempfile.TemporaryDirectory(prefix="velaria-py-stream-order-") as tmp:
            sink_path = str(pathlib.Path(tmp) / "sink.csv")
            session.sql(
                "CREATE SINK TABLE stream_order_out "
                "(id INT, abs_value DOUBLE, lower_name STRING) "
                f"USING csv OPTIONS(path: '{sink_path}', delimiter: ',')"
            )
            query = self._run_stream_sql(
                session,
                "INSERT INTO stream_order_out "
                "SELECT id, ABS(value) AS abs_value, LOWER(key) AS lower_name "
                "FROM stream_order_input ORDER BY abs_value DESC, lower_name ASC",
                trigger_interval_ms=0,
                execution_mode="local-workers",
                local_workers=4,
                max_inflight_partitions=4,
                checkpoint_delivery_mode="best-effort",
            )
            query.start()
            processed = query.await_termination()
            self.assertEqual(processed, 1)

            with open(sink_path, newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual([int(float(row["id"])) for row in rows], [4, 2, 1, 3])


if __name__ == "__main__":
    unittest.main()
