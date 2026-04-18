from __future__ import annotations

import gc
import json
import sys
import threading
import time

import pyarrow as pa

import velaria


def run_once(iteration: int) -> None:
    session = velaria.Session()
    source = session.create_realtime_stream_source(["event_time", "event_type", "source_key", "payload_json"])
    stream_df = session.read_realtime_stream_source(source)
    view_name = f"rt_input_{iteration}"
    session.create_temp_view(view_name, stream_df)
    sink = session.create_realtime_stream_sink()
    query_df = session.stream_sql(
        f"SELECT source_key, event_type, COUNT(*) AS cnt "
        f"FROM {view_name} GROUP BY source_key, event_type HAVING cnt >= 2"
    )
    query = query_df.write_stream_queue_sink(sink, trigger_interval_ms=0)
    query.start()
    worker = threading.Thread(target=lambda: query.await_termination(max_batches=2), daemon=True)
    worker.start()
    try:
        source.push_arrow(
            pa.table(
                {
                    "event_time": ["2026-01-01T00:00:00Z", "2026-01-01T00:00:01Z"],
                    "event_type": ["tick", "tick"],
                    "source_key": ["BTC", "BTC"],
                    "payload_json": ["{}", "{}"],
                }
            )
        )
        deadline = time.time() + 5.0
        while time.time() < deadline:
            batch = sink.poll_arrow()
            if batch is not None:
                _ = batch.to_pylist()
                break
            time.sleep(0.01)
    finally:
        source.close()
        query.stop()
        worker.join(timeout=5)


def main(argv: list[str] | None = None) -> int:
    argv = list(argv or sys.argv[1:])
    iterations = int(argv[0]) if argv else 200
    for index in range(iterations):
        run_once(index)
        gc.collect()
    print(json.dumps({"ok": True, "iterations": iterations}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
