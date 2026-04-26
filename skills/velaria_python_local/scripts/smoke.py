#!/usr/bin/env python3
"""Local smoke test for the Velaria Python usage skill."""

try:
    import pyarrow as pa
except ModuleNotFoundError as exc:
    raise SystemExit("Smoke test requires pyarrow. Install with `pyarrow==23.*` in the same env.") from exc

import pathlib
import tempfile

from velaria import Session


def _run_batch_sql(session: Session) -> None:
    with tempfile.TemporaryDirectory(prefix="velaria-skill-smoke-") as temp_dir:
        csv_path = pathlib.Path(temp_dir) / "sample.csv"
        csv_path.write_text("region,amount\nNA,10\nNA,20\nEU,5\n")

        df = session.read_csv(str(csv_path))
        session.create_temp_view("sales", df)
        result = session.sql("SELECT region, SUM(amount) AS amount_sum FROM sales GROUP BY region").to_arrow()
        rows = {row["region"]: row["amount_sum"] for row in result.to_pylist()}
        assert rows["NA"] == 30.0 and rows["EU"] == 5.0


def _run_stream_sql(session: Session) -> None:
    with tempfile.TemporaryDirectory(prefix="velaria-stream-smoke-") as temp_dir:
        sink_path = pathlib.Path(temp_dir) / "agg.csv"
        session.sql(
            f"CREATE SINK TABLE agg_sink (team STRING, cnt BIGINT) USING csv OPTIONS(path: '{sink_path}', delimiter: ',')"
        )

        stream = session.create_stream_from_arrow(
            [
                pa.record_batch({"team": ["A", "A"], "cnt": [1, 1]}),
                pa.record_batch({"team": ["B"], "cnt": [2]}),
            ]
        )
        session.create_temp_view("events", stream)

        query = session.start_stream_sql(
            """
            INSERT INTO agg_sink
            SELECT team, SUM(cnt) AS cnt
            FROM events
            GROUP BY team
            """,
            trigger_interval_ms=100,
        )
        query.start()
        query.await_termination(max_batches=1)

        out = session.read_csv(str(sink_path)).to_arrow().to_pylist()
        assert {"team": "A", "cnt": 2.0} in out


def main() -> None:
    session = Session()
    _run_batch_sql(session)
    _run_stream_sql(session)
    print("ok")


if __name__ == "__main__":
    main()
