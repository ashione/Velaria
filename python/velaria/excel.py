"""Excel bridge helpers for Velaria Python API."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def _normalize_datetime_columns(df: Any, date_format: str) -> Any:
    try:
        import pandas as pd
    except Exception as exc:
        raise RuntimeError("pandas is required for reading Excel files") from exc

    for col in df.columns:
        series = df[col]
        if pd.api.types.is_datetime64_any_dtype(series.dtype):
            df[col] = series.dt.strftime(date_format).where(series.notna(), None)
    return df


def read_excel(session: Any, path: str, *, sheet_name: int = 0, date_format: str = "%Y-%m-%d",
               coerce_datetime_to_string: bool = True, **pandas_kwargs) -> Any:
    """Read an Excel file and convert it into a Velaria DataFrame.

    The helper uses pandas to read Excel and pyarrow to convert it to the input format
    accepted by `Session.create_dataframe_from_arrow(...)`.
    """

    try:
        import pandas as pd
    except Exception as exc:
        raise RuntimeError("pandas is required to read xlsx files") from exc

    try:
        import pyarrow as pa
    except Exception as exc:
        raise RuntimeError("pyarrow is required for Velaria DataFrame conversion") from exc

    file_path = str(Path(path))
    try:
        frame = pd.read_excel(file_path, sheet_name=sheet_name, **pandas_kwargs)
    except ModuleNotFoundError as exc:
        raise RuntimeError("openpyxl is required to read .xlsx files") from exc

    if coerce_datetime_to_string:
        frame = _normalize_datetime_columns(frame, date_format=date_format)

    table = pa.Table.from_pandas(frame, preserve_index=False)
    return session.create_dataframe_from_arrow(table)
