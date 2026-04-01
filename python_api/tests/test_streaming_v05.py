import csv
import pathlib
import tempfile
import unittest

import pyarrow as pa

import velaria  # noqa: E402


class StreamingV05Test(unittest.TestCase):
    def test_batch_arrow_roundtrip_and_sql(self):
        session = velaria.Session()
        table = pa.table({"id": [1, 2, 3], "value": [10, 20, 30]})
        df = session.create_dataframe_from_arrow(table)
        session.create_temp_view("batch_input", df)

        out = session.sql("SELECT id, value FROM batch_input WHERE value > 15")
        rows = out.to_rows()
        self.assertEqual(rows["schema"], ["id", "value"])
        self.assertEqual(rows["rows"], [[2, 20], [3, 30]])

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
                f"CREATE SINK TABLE sink_out (key STRING, value_sum INT) USING csv OPTIONS(path '{sink_path}', delimiter ',')"
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
                f"USING csv OPTIONS(path '{sink_path}', delimiter ',')"
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
                f"USING csv OPTIONS(path '{sink_path}', delimiter ',')"
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
                f"USING csv OPTIONS(path '{sink_path}', delimiter ',')"
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
            "actor acceleration requires the aggregate hot path to be the final stream transform"
        )
        self.assertIn("actor_eligible=false", explain)
        self.assertIn(f"actor_eligibility_reason={reason}", explain)
        self.assertIn(f"reason={reason}", explain)


if __name__ == "__main__":
    unittest.main()
