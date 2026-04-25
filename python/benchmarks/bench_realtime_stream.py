from __future__ import annotations

import json
import pathlib
import sys
import threading
import time

import pyarrow as pa

import velaria


def main(argv: list[str] | None = None) -> int:
    argv = list(argv or sys.argv[1:])
    batch_count = int(argv[0]) if len(argv) >= 1 else 16
    rows_per_batch = int(argv[1]) if len(argv) >= 2 else 1024
    slide_ms = int(argv[2]) if len(argv) >= 3 else 30000

    session = velaria.Session()
    source = session.create_realtime_stream_source(["ts", "key", "value"])
    stream_df = session.read_realtime_stream_source(source)
    session.create_temp_view("bench_realtime_events", stream_df)
    sink = session.create_realtime_stream_sink()
    query_df = session.stream_sql(
        f"SELECT window_start, key, COUNT(*) AS event_count, SUM(value) AS value_sum "
        f"FROM bench_realtime_events "
        f"WINDOW BY ts EVERY 60000 SLIDE {slide_ms} AS window_start "
        f"GROUP BY window_start, key"
    )
    query = query_df.write_stream_queue_sink(
        sink,
        trigger_interval_ms=0,
        execution_mode="local-workers",
        local_workers=4,
        max_inflight_partitions=4,
    )
    query.start()
    worker = threading.Thread(target=lambda: query.await_termination(), daemon=True)
    worker.start()

    try:
        started = time.monotonic()
        ts_base = 1711616400000
        for batch_idx in range(batch_count):
            rows = {
                "ts": [],
                "key": [],
                "value": [],
            }
            for row_idx in range(rows_per_batch):
                ts = ts_base + ((batch_idx * rows_per_batch + row_idx) * 1000)
                rows["ts"].append(time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ts / 1000)))
                rows["key"].append(f"user_{row_idx % 64}")
                rows["value"].append((row_idx % 7) + 1)
            source.push_arrow(pa.table(rows))

        observed_batches = 0
        observed_rows = 0
        while observed_batches < batch_count:
            table = sink.poll_arrow()
            if table is None:
                time.sleep(0.01)
                continue
            observed_batches += 1
            observed_rows += table.num_rows
        elapsed = time.monotonic() - started
        print(
            json.dumps(
                {
                    "batch_count": batch_count,
                    "rows_per_batch": rows_per_batch,
                    "slide_ms": slide_ms,
                    "elapsed_s": elapsed,
                    "input_rows": batch_count * rows_per_batch,
                    "output_rows": observed_rows,
                    "batches_per_s": batch_count / elapsed if elapsed > 0 else 0.0,
                    "input_rows_per_s": (batch_count * rows_per_batch) / elapsed if elapsed > 0 else 0.0,
                },
                ensure_ascii=False,
            )
        )
        return 0
    finally:
        source.close()
        query.stop()
        worker.join(timeout=5)


if __name__ == "__main__":
    raise SystemExit(main())
