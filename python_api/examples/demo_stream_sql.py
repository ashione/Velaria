import json
import pathlib
import tempfile

import pyarrow as pa

from velaria import Session


def main():
    session = Session()
    with tempfile.TemporaryDirectory(prefix="velaria-py-stream-") as tmp:
        base = pathlib.Path(tmp)
        sink_path = base / "summary.csv"

        arrow_source = [
            pa.record_batch(
                {
                    "key": ["userA", "userA"],
                    "value": [7, 10],
                }
            ),
            pa.record_batch(
                {
                    "key": ["userB", "userC"],
                    "value": [20, 2],
                }
            ),
        ]

        session.sql(
            f"CREATE SINK TABLE stream_summary_py (key STRING, value_sum INT) "
            f"USING csv OPTIONS(path '{sink_path}', delimiter ',')"
        )

        stream = session.create_stream_from_arrow(arrow_source)
        session.create_temp_view("stream_events_py", stream)

        query = session.start_stream_sql(
            """
            INSERT INTO stream_summary_py
            SELECT key, SUM(value) AS value_sum
            FROM stream_events_py
            WHERE value > 6
            GROUP BY key
            HAVING value_sum > 15
            LIMIT 10
            """,
            trigger_interval_ms=50,
        )
        query.start()
        query.await_termination(max_batches=1)

        result = session.read_csv(str(sink_path)).to_arrow()
        roundtrip = session.create_dataframe_from_arrow(
            pa.table({"key": ["userZ"], "value_sum": [99]})
        ).to_rows()
        print(
            json.dumps(
                {
                    "progress": query.progress(),
                    "schema": result.schema.names,
                    "roundtrip_rows": roundtrip["rows"],
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
