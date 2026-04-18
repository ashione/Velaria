import pathlib
import tempfile
import threading
import time
import unittest

import pyarrow as pa

import velaria


class ArrowStreamWrapper:
    def __init__(self, source):
        self._source = source

    def __arrow_c_stream__(self):
        return self._source.__arrow_c_stream__()


class ArrowStreamIngestionTest(unittest.TestCase):
    def test_create_dataframe_from_record_batch_reader_merges_batches(self):
        session = velaria.Session()
        schema = pa.schema([("id", pa.int64()), ("value", pa.int64())])
        reader = pa.RecordBatchReader.from_batches(
            schema,
            [
                pa.record_batch([[1, 2], [10, 20]], schema=schema),
                pa.record_batch([[3], [30]], schema=schema),
            ],
        )

        df = session.create_dataframe_from_arrow(reader)
        rows = df.to_rows()
        self.assertEqual(rows["schema"], ["id", "value"])
        self.assertEqual(rows["rows"], [[1, 10], [2, 20], [3, 30]])

    def test_create_dataframe_from_record_batch_reader_to_arrow_merges_batches(self):
        session = velaria.Session()
        schema = pa.schema([("id", pa.int64()), ("value", pa.int64())])
        reader = pa.RecordBatchReader.from_batches(
            schema,
            [
                pa.record_batch([[1, 2], [10, 20]], schema=schema),
                pa.record_batch([[3, 4], [30, 40]], schema=schema),
            ],
        )

        arrow_out = session.create_dataframe_from_arrow(reader).to_arrow()
        self.assertEqual(arrow_out.column_names, ["id", "value"])
        self.assertEqual(arrow_out.column("id").to_pylist(), [1, 2, 3, 4])
        self.assertEqual(arrow_out.column("value").to_pylist(), [10, 20, 30, 40])

    def test_create_dataframe_from_arrow_c_stream_wrapper(self):
        session = velaria.Session()
        table = pa.table({"id": [1, 2], "name": ["alice", "bob"]})

        df = session.create_dataframe_from_arrow(ArrowStreamWrapper(table))
        rows = df.to_rows()
        self.assertEqual(rows["schema"], ["id", "name"])
        self.assertEqual(rows["rows"], [[1, "alice"], [2, "bob"]])

    def test_create_dataframe_from_arrow_fast_path_preserves_nulls_and_bools(self):
        session = velaria.Session()
        table = pa.table(
            {
                "flag": pa.array([True, False, None], type=pa.bool_()),
                "name": pa.array(["alice", None, "carol"], type=pa.string()),
                "score": pa.array([1, 2, 3], type=pa.int64()),
            }
        )

        df = session.create_dataframe_from_arrow(table)
        rows = df.to_rows()
        self.assertEqual(rows["schema"], ["flag", "name", "score"])
        self.assertEqual(
            rows["rows"],
            [
                [1, "alice", 1],
                [0, None, 2],
                [None, "carol", 3],
            ],
        )

    def test_create_dataframe_from_arrow_slow_path_uses_columnar_append(self):
        session = velaria.Session()
        df = session.create_dataframe_from_arrow(
            {
                "flag": [True, False, None],
                "name": ["alice", None, "carol"],
                "score": [1, 2, 3],
            }
        )
        rows = df.to_rows()
        self.assertEqual(rows["schema"], ["flag", "name", "score"])
        self.assertEqual(
            rows["rows"],
            [
                [1, "alice", 1],
                [0, None, 2],
                [None, "carol", 3],
            ],
        )

    def test_create_stream_from_arrow_reader_preserves_batch_boundaries(self):
        session = velaria.Session()
        schema = pa.schema([("key", pa.string()), ("value", pa.int64())])
        reader = pa.RecordBatchReader.from_batches(
            schema,
            [
                pa.record_batch([["u1", "u1"], [1, 2]], schema=schema),
                pa.record_batch([["u2"], [3]], schema=schema),
            ],
        )
        stream_df = session.create_stream_from_arrow(reader)
        session.create_temp_view("arrow_stream_reader_input", stream_df)

        with tempfile.TemporaryDirectory(prefix="velaria-arrow-stream-") as tmp:
            sink_path = str(pathlib.Path(tmp) / "sink.csv")
            session.sql(
                f"CREATE SINK TABLE arrow_stream_reader_sink (key STRING, value_sum INT) "
                f"USING csv OPTIONS(path: '{sink_path}', delimiter: ',')"
            )
            query = session.start_stream_sql(
                "INSERT INTO arrow_stream_reader_sink "
                "SELECT key, SUM(value) AS value_sum FROM arrow_stream_reader_input GROUP BY key",
                trigger_interval_ms=0,
            )
            query.start()
            processed = query.await_termination()
            progress = query.progress()

            self.assertEqual(processed, 2)
            self.assertEqual(progress["batches_processed"], 2)
            self.assertEqual(progress["last_source_offset"], "2")

    def test_dataframe_to_arrow_roundtrip_preserves_scalar_columns(self):
        session = velaria.Session()
        source = pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.int64()),
                "name": pa.array(["alice", None, "carol"], type=pa.string()),
                "score": pa.array([1.5, None, 3.25], type=pa.float64()),
            }
        )

        arrow_out = session.create_dataframe_from_arrow(source).to_arrow()
        self.assertEqual(arrow_out.column_names, ["id", "name", "score"])
        self.assertEqual(arrow_out.column("id").to_pylist(), [1, 2, 3])
        self.assertEqual(arrow_out.column("name").to_pylist(), ["alice", None, "carol"])
        self.assertEqual(arrow_out.column("score").to_pylist(), [1.5, None, 3.25])

    def test_dataframe_to_arrow_preserves_arrow_native_formats(self):
        session = velaria.Session()
        embedding = pa.FixedSizeListArray.from_arrays(
            pa.array([1.0, 2.0, 3.0, 4.0], type=pa.float32()),
            2,
        )
        source = pa.table(
            {
                "flag": pa.array([True, None], type=pa.bool_()),
                "small_id": pa.array([1, 2], type=pa.int32()),
                "score32": pa.array([1.25, None], type=pa.float32()),
                "note": pa.array(["alice", "bob"], type=pa.large_string()),
                "embedding": embedding,
            }
        )

        arrow_out = session.create_dataframe_from_arrow(source).to_arrow()
        self.assertEqual(arrow_out.schema.field("flag").type, pa.bool_())
        self.assertEqual(arrow_out.schema.field("small_id").type, pa.int32())
        self.assertEqual(arrow_out.schema.field("score32").type, pa.float32())
        self.assertEqual(arrow_out.schema.field("note").type, pa.large_string())
        self.assertEqual(arrow_out.schema.field("embedding").type, pa.list_(pa.float32(), 2))
        self.assertEqual(arrow_out.column("flag").to_pylist(), [True, None])
        self.assertEqual(arrow_out.column("small_id").to_pylist(), [1, 2])
        self.assertEqual(arrow_out.column("score32").to_pylist(), [1.25, None])
        self.assertEqual(arrow_out.column("note").to_pylist(), ["alice", "bob"])
        self.assertEqual(arrow_out.column("embedding").to_pylist(), [[1.0, 2.0], [3.0, 4.0]])

    def test_read_csv_to_arrow_roundtrip_preserves_quoted_and_null_cells(self):
        session = velaria.Session()
        with tempfile.TemporaryDirectory(prefix="velaria-read-csv-arrow-") as tmp:
            csv_path = pathlib.Path(tmp) / "input.csv"
            csv_path.write_text(
                'id,name,score\n'
                '1,alice,1.5\n'
                '2,,\n'
                '3,"carol,inc",3.25\n',
                encoding="utf-8",
            )

            arrow_out = session.read_csv(str(csv_path)).to_arrow()
            self.assertEqual(arrow_out.column_names, ["id", "name", "score"])
            self.assertEqual(arrow_out.column("id").to_pylist(), [1, 2, 3])
            self.assertEqual(arrow_out.column("name").to_pylist(), ["alice", None, "carol,inc"])
            self.assertEqual(arrow_out.column("score").to_pylist(), [1.5, None, 3.25])

    def test_realtime_stream_source_and_sink_support_direct_arrow_ingest(self):
        session = velaria.Session()
        source = session.create_realtime_stream_source(["ts", "key", "value"])
        stream_df = session.read_realtime_stream_source(source)
        session.create_temp_view("realtime_events", stream_df)
        sink = session.create_realtime_stream_sink()
        query_df = session.stream_sql(
            "SELECT window_start, key, COUNT(*) AS event_count "
            "FROM realtime_events "
            "WINDOW BY ts EVERY 60000 SLIDE 30000 AS window_start "
            "GROUP BY window_start, key"
        )
        query = query_df.write_stream_queue_sink(
            sink,
            trigger_interval_ms=0,
            execution_mode="local-workers",
            local_workers=2,
            max_inflight_partitions=2,
        )
        query.start()
        worker = threading.Thread(target=lambda: query.await_termination(), daemon=True)
        worker.start()
        try:
            source.push_arrow(
                pa.table(
                    {
                        "ts": ["2026-03-29T10:00:00", "2026-03-29T10:00:10", "2026-03-29T10:00:35"],
                        "key": ["u1", "u1", "u1"],
                        "value": [1, 1, 1],
                    }
                )
            )
            observed = None
            for _ in range(50):
                batch = sink.poll_arrow()
                if batch is not None:
                    observed = batch
                    break
                time.sleep(0.05)
            self.assertIsNotNone(observed)
            rows = {(row["window_start"], row["key"], row["event_count"]) for row in observed.to_pylist()}
            self.assertEqual(
                rows,
                {
                    ("2026-03-29T09:59:30", "u1", 2),
                    ("2026-03-29T10:00:00", "u1", 3),
                    ("2026-03-29T10:00:30", "u1", 1),
                },
            )
        finally:
            source.close()
            query.stop()
            worker.join(timeout=5)

    def test_realtime_stream_source_and_sink_support_python_row_ingest(self):
        session = velaria.Session()
        source = session.create_realtime_stream_source(["ts", "key", "value"])
        stream_df = session.read_realtime_stream_source(source)
        session.create_temp_view("realtime_events_rows", stream_df)
        sink = session.create_realtime_stream_sink()
        query_df = session.stream_sql(
            "SELECT key, COUNT(*) AS event_count "
            "FROM realtime_events_rows "
            "GROUP BY key HAVING event_count >= 2"
        )
        query = query_df.write_stream_queue_sink(sink, trigger_interval_ms=0)
        query.start()
        worker = threading.Thread(target=lambda: query.await_termination(max_batches=2), daemon=True)
        worker.start()
        try:
            source.push_rows(
                [
                    {"ts": "2026-03-29T10:00:00", "key": "u1", "value": 1},
                    {"ts": "2026-03-29T10:00:10", "key": "u1", "value": 2},
                ]
            )
            observed = None
            for _ in range(50):
                batch = sink.poll_arrow()
                if batch is not None:
                    observed = batch
                    break
                time.sleep(0.05)
            self.assertIsNotNone(observed)
            self.assertEqual(observed.to_pylist(), [{"key": "u1", "event_count": 2}])
        finally:
            source.close()
            query.stop()
            worker.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
