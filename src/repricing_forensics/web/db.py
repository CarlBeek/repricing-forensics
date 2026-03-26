"""Read-only DuckDB connection and query helpers for the web server."""
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from repricing_forensics.config import default_paths
from repricing_forensics.labels import ADDRESS_PROJECT_LABELS
from repricing_forensics.sql import create_views_sql

SCHEDULE_NAME = os.environ.get("SCHEDULE_NAME", "7904-prelim")

_paths = default_paths()
_conn: duckdb.DuckDBPyConnection | None = None
_labels: dict[str, str] = {}
_csv_cache: dict[str, tuple[float, pd.DataFrame]] = {}


def get_conn() -> duckdb.DuckDBPyConnection:
    """Return a shared read-only DuckDB connection, creating views if needed."""
    global _conn
    if _conn is None:
        os.chdir(_paths.repo_root)
        _conn = duckdb.connect(str(_paths.duckdb_path), read_only=True)
        threads = os.environ.get("DUCKDB_THREADS", str(os.cpu_count() or 4))
        _conn.execute(f"PRAGMA threads={threads}")
    return _conn


def close_conn() -> None:
    global _conn
    if _conn is not None:
        _conn.close()
        _conn = None


def query(sql: str) -> list[dict[str, Any]]:
    """Execute SQL and return a list of dicts."""
    df = get_conn().execute(sql).df()
    return df.where(df.notna(), None).to_dict(orient="records")


def query_df(sql: str) -> pd.DataFrame:
    """Execute SQL and return a DataFrame."""
    return get_conn().execute(sql).df()


def query_scalar(sql: str) -> Any:
    """Execute SQL and return the single scalar result."""
    return get_conn().execute(sql).fetchone()[0]


def read_csv(name: str) -> pd.DataFrame:
    """Read a CSV from artifacts/tables/ with mtime-based caching."""
    csv_path = _paths.artifacts_dir / "tables" / name
    if not csv_path.exists():
        return pd.DataFrame()
    mtime = csv_path.stat().st_mtime
    cached = _csv_cache.get(name)
    if cached and cached[0] == mtime:
        return cached[1]
    df = pd.read_csv(csv_path)
    _csv_cache[name] = (mtime, df)
    return df


def load_labels() -> dict[str, str]:
    """Load contract labels from CSV + hardcoded mappings."""
    global _labels
    if _labels:
        return _labels
    _labels = dict(ADDRESS_PROJECT_LABELS)
    labels_csv = _paths.cache_dir / "contract_labels.csv"
    if labels_csv.exists():
        df = pd.read_csv(labels_csv)
        for _, row in df.iterrows():
            _labels[str(row["address"]).lower()] = row["name"]
    return _labels


def label_address(addr: str | None) -> str:
    """Return project label for an address, or the address itself."""
    if addr is None:
        return "unknown"
    labels = load_labels()
    return labels.get(addr.lower(), addr)


def db_mtime() -> datetime:
    """Return the last-modified time of the DuckDB file."""
    p = _paths.duckdb_path
    if p.exists():
        return datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
    return datetime.now(tz=timezone.utc)
