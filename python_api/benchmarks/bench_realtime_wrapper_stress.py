from __future__ import annotations

import gc
import importlib.util
import json
import pathlib
import sys
import threading
import time


def _load_native_module():
    ext_path = pathlib.Path(__file__).resolve().parents[1] / "velaria" / "_velaria.so"
    spec = importlib.util.spec_from_file_location("_velaria", ext_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load velaria native extension from {ext_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_native = _load_native_module()
Session = _native.Session


def run_once(iteration: int, *, mode: str) -> None:
    session = Session()
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
        rows = [
            {
                "event_time": "2026-01-01T00:00:00Z",
                "event_type": "tick",
                "source_key": "BTC",
                "payload_json": "{}",
            },
            {
                "event_time": "2026-01-01T00:00:01Z",
                "event_type": "tick",
                "source_key": "BTC",
                "payload_json": "{}",
            },
        ]
        if mode == "arrow":
            import pyarrow as pa

            source.push_arrow(
                pa.table(
                    {
                        "event_time": [row["event_time"] for row in rows],
                        "event_type": [row["event_type"] for row in rows],
                        "source_key": [row["source_key"] for row in rows],
                        "payload_json": [row["payload_json"] for row in rows],
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
        else:
            source.push_rows(rows)
            deadline = time.time() + 5.0
            while time.time() < deadline:
                progress = query.progress()
                if int(progress.get("batches_processed", 0)) >= 1:
                    break
                time.sleep(0.01)
            else:
                raise RuntimeError("rows mode did not process a batch in time")
    finally:
        source.close()
        query.stop()
        worker.join(timeout=5)


def main(argv: list[str] | None = None) -> int:
    argv = list(argv or sys.argv[1:])
    iterations = int(argv[0]) if argv else 200
    mode = argv[1] if len(argv) >= 2 else "rows"
    for index in range(iterations):
        run_once(index, mode=mode)
        gc.collect()
    print(json.dumps({"ok": True, "iterations": iterations, "mode": mode}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
