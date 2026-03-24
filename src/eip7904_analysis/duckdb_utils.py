from __future__ import annotations

import os
from pathlib import Path

import duckdb


def connect(database_path: Path) -> duckdb.DuckDBPyConnection:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(database_path))
    threads = os.environ.get("DUCKDB_THREADS", str(os.cpu_count() or 4))
    conn.execute(f"PRAGMA threads={threads}")
    conn.execute("PRAGMA enable_progress_bar")
    return conn
