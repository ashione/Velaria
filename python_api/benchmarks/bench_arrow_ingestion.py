import statistics
import time

import pyarrow as pa

import velaria


class ArrowStreamWrapper:
    def __init__(self, source):
        self._source = source

    def __arrow_c_stream__(self):
        return self._source.__arrow_c_stream__()


def build_reader(batch_count: int, rows_per_batch: int) -> pa.RecordBatchReader:
    schema = pa.schema([("id", pa.int64()), ("value", pa.int64()), ("name", pa.string())])
    batches = []
    for batch_id in range(batch_count):
        base = batch_id * rows_per_batch
        batches.append(
            pa.record_batch(
                [
                    list(range(base, base + rows_per_batch)),
                    [i % 17 for i in range(rows_per_batch)],
                    [f"user_{i % 32}" for i in range(rows_per_batch)],
                ],
                schema=schema,
            )
        )
    return pa.RecordBatchReader.from_batches(schema, batches)


def measure(label: str, factory, fn, rounds: int = 5):
    samples = []
    for _ in range(rounds):
        source = factory()
        started = time.perf_counter()
        result = fn(source)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        samples.append(elapsed_ms)
        rows = result.to_rows()["rows"]
        print(f"[bench] {label} rows={len(rows)} elapsed_ms={elapsed_ms:.2f}")
    print(
        f"[bench-summary] {label} mean_ms={statistics.mean(samples):.2f} "
        f"min_ms={min(samples):.2f} max_ms={max(samples):.2f}"
    )


def main():
    session = velaria.Session()
    batch_count = 8
    rows_per_batch = 4096

    def table_factory():
        return build_reader(batch_count, rows_per_batch).read_all()

    def reader_factory():
        return build_reader(batch_count, rows_per_batch)

    def wrapper_factory():
        return ArrowStreamWrapper(build_reader(batch_count, rows_per_batch))

    measure("table", table_factory, session.create_dataframe_from_arrow)
    measure("record-batch-reader", reader_factory, session.create_dataframe_from_arrow)
    measure("arrow-c-stream-wrapper", wrapper_factory, session.create_dataframe_from_arrow)


if __name__ == "__main__":
    main()
