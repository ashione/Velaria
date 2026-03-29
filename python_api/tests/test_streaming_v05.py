import json
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

            snapshot_json = json.dumps(progress)
            self.assertIn("execution_mode", snapshot_json)

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


if __name__ == "__main__":
    unittest.main()
