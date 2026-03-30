import time
from dataclasses import dataclass
from typing import Callable, Iterable, List, Mapping, Optional, Sequence, Union

import pyarrow as pa

RowType = Union[Mapping[str, object], Sequence[object]]


@dataclass
class CustomStreamEmitOptions:
    emit_interval_seconds: float = 1.0
    emit_rows: int = 1024

    def validate(self) -> None:
        if self.emit_interval_seconds <= 0:
            raise ValueError("emit_interval_seconds must be > 0")
        if self.emit_rows <= 0:
            raise ValueError("emit_rows must be > 0")


class CustomArrowStreamSource:
    """Reusable custom stream source that converts Python rows to Arrow micro-batches.

    Emit strategy:
    - Emit when buffered rows reach `emit_rows`
    - Or when elapsed wall-clock time reaches `emit_interval_seconds`
    """

    def __init__(
        self,
        schema: Optional[pa.Schema] = None,
        *,
        emit_interval_seconds: float = 1.0,
        emit_rows: int = 1024,
    ) -> None:
        self.schema = schema
        self.options = CustomStreamEmitOptions(
            emit_interval_seconds=emit_interval_seconds,
            emit_rows=emit_rows,
        )
        self.options.validate()

    def to_arrow_batches(self, rows: Iterable[RowType]) -> List[pa.Table]:
        batches: List[pa.Table] = []
        buffer: List[RowType] = []
        window_start = time.monotonic()

        for row in rows:
            buffer.append(row)
            now = time.monotonic()
            elapsed = now - window_start
            if len(buffer) >= self.options.emit_rows or elapsed >= self.options.emit_interval_seconds:
                batches.append(self._build_table(buffer))
                buffer = []
                window_start = now

        if buffer:
            batches.append(self._build_table(buffer))
        return batches

    def to_stream_dataframe(self, session, rows: Iterable[RowType]):
        """Convert rows to Arrow batches and create Velaria StreamingDataFrame."""
        return session.create_stream_from_arrow(self.to_arrow_batches(rows))

    def _build_table(self, rows: Sequence[RowType]) -> pa.Table:
        if not rows:
            return pa.table({})

        first = rows[0]
        if isinstance(first, Mapping):
            if self.schema is not None:
                return pa.Table.from_pylist(list(rows), schema=self.schema)
            return pa.Table.from_pylist(list(rows))

        if self.schema is None:
            raise ValueError("schema is required when rows are sequence-based")
        names = list(self.schema.names)
        columns = {name: [] for name in names}
        for row in rows:
            if not isinstance(row, Sequence):
                raise ValueError("mixed row types are not supported")
            if len(row) != len(names):
                raise ValueError("row width does not match schema")
            for idx, name in enumerate(names):
                columns[name].append(row[idx])
        return pa.table(columns, schema=self.schema)


class CustomArrowStreamSink:
    """Reusable sink that consumes Arrow micro-batches with emit-by-time/rows policy."""

    def __init__(
        self,
        on_emit: Callable[[pa.Table], None],
        *,
        emit_interval_seconds: float = 1.0,
        emit_rows: int = 1024,
    ) -> None:
        if not callable(on_emit):
            raise ValueError("on_emit must be callable")
        self.on_emit = on_emit
        self.options = CustomStreamEmitOptions(
            emit_interval_seconds=emit_interval_seconds,
            emit_rows=emit_rows,
        )
        self.options.validate()
        self._pending: List[pa.Table] = []
        self._pending_rows = 0
        self._window_start: Optional[float] = None

    def write_batch(self, batch: pa.Table) -> None:
        if self._window_start is None:
            self._window_start = time.monotonic()
        self._pending.append(batch)
        self._pending_rows += batch.num_rows
        now = time.monotonic()
        elapsed = now - self._window_start
        if self._pending_rows >= self.options.emit_rows or elapsed >= self.options.emit_interval_seconds:
            self._emit_pending(now)

    def close(self) -> None:
        if self._pending_rows > 0:
            self._emit_pending(time.monotonic())

    def _emit_pending(self, now: float) -> None:
        if not self._pending:
            return
        merged = pa.concat_tables(self._pending, promote_options="none")
        self.on_emit(merged)
        self._pending = []
        self._pending_rows = 0
        self._window_start = None if not self._pending else now


def create_stream_from_custom_source(
    session,
    rows: Iterable[RowType],
    *,
    schema: Optional[pa.Schema] = None,
    emit_interval_seconds: float = 1.0,
    emit_rows: int = 1024,
):
    source = CustomArrowStreamSource(
        schema=schema,
        emit_interval_seconds=emit_interval_seconds,
        emit_rows=emit_rows,
    )
    return source.to_stream_dataframe(session, rows)


def consume_arrow_batches_with_custom_sink(
    batches: Iterable[pa.Table],
    sink: CustomArrowStreamSink,
) -> None:
    for batch in batches:
        sink.write_batch(batch)
    sink.close()
