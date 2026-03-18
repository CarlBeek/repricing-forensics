from __future__ import annotations

from pathlib import Path

import duckdb


def connect(database_path: Path) -> duckdb.DuckDBPyConnection:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(database_path))
    conn.execute("PRAGMA threads=8")
    conn.execute("PRAGMA enable_progress_bar")
    return conn
