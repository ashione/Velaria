import pathlib
import tempfile
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
                f"USING csv OPTIONS(path '{sink_path}', delimiter ',')"
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


if __name__ == "__main__":
    unittest.main()
