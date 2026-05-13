"""Read-only DuckDB session and query helpers for the web server.

Opens an in-memory consumer DB that ATTACHes the producer's DuckDB file
read-only (see `source_db.py`). The producer file is the single source
of truth; the consumer never materializes anything.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, TypeVar

import duckdb
import pandas as pd

from repricing_forensics.config import default_paths, default_schedule_name
from repricing_forensics.labels import ADDRESS_PROJECT_LABELS
from repricing_forensics.source_db import open_session, resolve_producer_db_path

SCHEDULE_NAME = default_schedule_name()
_log = logging.getLogger(__name__)

_paths = default_paths()
_conn: duckdb.DuckDBPyConnection | None = None
_conn_producer_mtime: float | None = None
_labels: dict[str, str] = {}
_db_lock = threading.Lock()

T = TypeVar("T")

# Per-key TTL cache. When reth is mid-replay, reads through DuckDB's
# sqlite_scanner can fight the writer for SQLite shared locks; even a
# `SELECT count(*) FROM block_coverage` ends up tens-of-seconds slow.
# We cache aggregate results with a short TTL so the dashboard stays
# responsive even when the underlying query is slow.
_cache_lock = threading.Lock()
_cache: dict[str, tuple[float, Any]] = {}


def cached(key: str, ttl_seconds: float, fn: Callable[[], T]) -> T:
    """Return a cached value for `key` if it's fresh; otherwise run
    `fn()` and cache the result.

    On `fn()` failure, return the stale value if we have one, else
    re-raise — this keeps the dashboard rendering during a transient
    producer outage without hiding a persistent fault."""
    now = time.monotonic()
    with _cache_lock:
        hit = _cache.get(key)
    if hit is not None:
        expiry, value = hit
        if expiry > now:
            return value
    # Cache miss or stale.
    try:
        value = fn()
    except Exception as exc:
        if hit is not None:
            _log.warning(
                "cached(%s) refresh failed, serving stale value: %s",
                key, exc,
            )
            return hit[1]
        raise
    with _cache_lock:
        _cache[key] = (now + ttl_seconds, value)
    return value


def cache_invalidate_all() -> None:
    """Drop every cached entry. Called on connection reset / shutdown."""
    with _cache_lock:
        _cache.clear()


def cache_endpoint(ttl_seconds: float):
    """Decorator: cache the wrapped endpoint's return value by
    (function name, sorted kwargs) tuple for `ttl_seconds`.

    Use on /api/* handlers whose underlying SQL is slow under heavy
    producer write load — the dashboard reads through DuckDB
    sqlite_scanner, which races SQLite's writer lock and ends up
    tens-of-seconds slow during reth replay.

    Returns serialized-result-shaped objects (list/dict/etc.); the
    cache stores them as-is and FastAPI re-encodes per request.
    """
    def decorator(fn):
        import functools

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            # Args are typically empty (FastAPI passes via kwargs); join
            # both for safety. Path-param values stringify cleanly.
            arg_key = ",".join(repr(a) for a in args)
            kw_key = ",".join(f"{k}={v!r}" for k, v in sorted(kwargs.items()))
            key = f"{fn.__module__}.{fn.__qualname__}({arg_key};{kw_key})"
            return cached(key, ttl_seconds, lambda: fn(*args, **kwargs))
        return wrapper
    return decorator


def get_conn() -> duckdb.DuckDBPyConnection:
    """Return a shared read-only consumer session.

    The producer file's mtime changes constantly while reth is replaying
    — every block commit bumps it — so we *don't* tear down the
    connection on mtime drift. DuckDB's sqlite_scanner sees fresh data
    on every query through SQLite WAL anyway. The connection only gets
    reopened on explicit `close_conn()`."""
    global _conn
    if _conn is None:
        producer_path = resolve_producer_db_path()
        _conn = open_session(producer_path, SCHEDULE_NAME)
        cache_invalidate_all()
    return _conn


def close_conn() -> None:
    global _conn, _conn_producer_mtime
    if _conn is not None:
        _conn.close()
        _conn = None
        _conn_producer_mtime = None
    cache_invalidate_all()


def query(sql: str) -> list[dict[str, Any]]:
    """Execute SQL and return a list of dicts."""
    with _db_lock:
        df = get_conn().execute(sql).df()
    # `df.where(df.notna(), None)` does not coerce NaN to None on float columns
    # (pandas preserves the float dtype, so None round-trips back to NaN). Cast
    # to object first so None survives `to_dict`.
    return df.astype(object).where(df.notna(), None).to_dict(orient="records")


def query_df(sql: str) -> pd.DataFrame:
    """Execute SQL and return a DataFrame."""
    with _db_lock:
        return get_conn().execute(sql).df()


def query_scalar(sql: str, default: Any = None) -> Any:
    """Execute SQL and return the single scalar result, or default if empty."""
    with _db_lock:
        row = get_conn().execute(sql).fetchone()
    if row is None:
        return default
    return row[0]


def load_labels() -> dict[str, str]:
    """Load contract labels from the hardcoded map + the optional
    `cache/contract_labels.csv` enrichment.

    The CSV used to be generated by `scripts/build_contract_labels.py`
    (sourcify + etherscan lookups), which was removed in the storage
    redesign cleanup — there's no in-repo generator anymore. Existing
    cached CSVs from a prior run are still honored. Once the producer
    ships `contract_metadata`, the dashboard can prefer those labels and
    this path becomes optional.
    """
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
    if not isinstance(addr, str) or not addr:
        return "unknown"
    labels = load_labels()
    return labels.get(addr.lower(), addr)


def db_mtime() -> datetime:
    """Return the last-modified time of the producer DuckDB file."""
    p = resolve_producer_db_path()
    if p.exists():
        return datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
    return datetime.now(tz=timezone.utc)
