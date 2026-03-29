import unittest

import pyarrow as pa

from velaria import (
    CustomArrowStreamSink,
    CustomArrowStreamSource,
    consume_arrow_batches_with_custom_sink,
    create_stream_from_custom_source,
)


class _FakeSession:
    def __init__(self):
        self.batches = None

    def create_stream_from_arrow(self, batches):
        self.batches = batches
        return {"ok": True, "batches": len(batches)}


class CustomStreamSourceTest(unittest.TestCase):
    def test_emit_by_row_count(self):
        source = CustomArrowStreamSource(emit_interval_seconds=10.0, emit_rows=2)
        rows = [
            {"id": 1, "value": 10},
            {"id": 2, "value": 20},
            {"id": 3, "value": 30},
        ]
        batches = source.to_arrow_batches(rows)
        self.assertEqual(len(batches), 2)
        self.assertEqual(batches[0].num_rows, 2)
        self.assertEqual(batches[1].num_rows, 1)

    def test_sequence_rows_require_schema(self):
        schema = pa.schema([("id", pa.int64()), ("value", pa.int64())])
        source = CustomArrowStreamSource(schema=schema, emit_interval_seconds=10.0, emit_rows=3)
        batches = source.to_arrow_batches([(1, 10), (2, 20)])
        self.assertEqual(len(batches), 1)
        self.assertEqual(batches[0].schema.names, ["id", "value"])


    def test_custom_sink_emit_by_row_count(self):
        emitted = []

        sink = CustomArrowStreamSink(
            lambda table: emitted.append(table.num_rows),
            emit_interval_seconds=10.0,
            emit_rows=3,
        )
        batches = [
            pa.table({"id": [1, 2], "value": [10, 20]}),
            pa.table({"id": [3], "value": [30]}),
            pa.table({"id": [4], "value": [40]}),
        ]
        consume_arrow_batches_with_custom_sink(batches, sink)
        self.assertEqual(emitted, [3, 1])

    def test_reuse_with_session_helper(self):
        session = _FakeSession()
        rows = [{"id": i, "value": i * 10} for i in range(5)]
        out = create_stream_from_custom_source(
            session,
            rows,
            emit_interval_seconds=10.0,
            emit_rows=2,
        )
        self.assertEqual(out["ok"], True)
        self.assertEqual(out["batches"], 3)
        self.assertEqual([b.num_rows for b in session.batches], [2, 2, 1])


if __name__ == "__main__":
    unittest.main()
