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

from repricing_forensics.config import default_paths
from repricing_forensics.labels import ADDRESS_PROJECT_LABELS
from repricing_forensics.source_db import open_session, resolve_producer_db_path

_log = logging.getLogger(__name__)

_paths = default_paths()
_conn: duckdb.DuckDBPyConnection | None = None
_conn_producer_mtime: float | None = None
_conn_opened_at: float | None = None
_labels: dict[str, str] = {}

# Protects the `_conn` / `_conn_opened_at` / `_retired_conns` globals
# only — NOT held during SQL execution. Per-query work runs on a fresh
# DuckDB cursor (a duplicate connection sharing the catalog) so the
# FastAPI threadpool can serve concurrent /api/* requests in parallel.
# Holding a single lock across SQL execution previously serialized the
# ~10 parallel fetchJSON calls each page fans out.
_conn_lock = threading.Lock()

# When the session recycles, we don't close the old connection
# immediately — cursors created from it may still be mid-query in
# other threads. Park the old conn here and close it on the *next*
# recycle, by which point any in-flight queries have long since
# completed. Bound to ≤1 entry so we don't grow unbounded.
_retired_conns: list[duckdb.DuckDBPyConnection] = []

# How long to hold the DuckDB session before recycling it. DuckDB
# sqlite_scanner keeps a long-lived SQLite shared lock on the attached
# file; while the lock is held, the producer's `PRAGMA wal_checkpoint`
# can't TRUNCATE the WAL, and it grows unbounded (we observed 101 GB
# WAL against a 20 GB main file in production). Recycling the session
# periodically drops the lock briefly so checkpoints can complete.
#
# Tradeoff: each recycle costs an ATTACH + view creation (~5-10s). 5
# minutes is short enough to keep WAL bounded, long enough to amortize
# the reattach cost across many requests.
_CONN_MAX_AGE_SECONDS = 300.0

T = TypeVar("T")

# Per-key TTL cache. When reth is mid-replay, reads through DuckDB's
# sqlite_scanner can fight the writer for SQLite shared locks; even a
# `SELECT count(*) FROM block_coverage` ends up tens-of-seconds slow.
# We cache aggregate results with a short TTL so the dashboard stays
# responsive even when the underlying query is slow.
_cache_lock = threading.Lock()
_cache: dict[str, tuple[float, Any]] = {}
_refreshing: set[str] = set()
_refresh_sem = threading.BoundedSemaphore(4)


def _refresh_cache_key(key: str, ttl_seconds: float, fn: Callable[[], T]) -> None:
    """Refresh one stale cache key in the background."""
    try:
        with _refresh_sem:
            t0 = time.monotonic()
            value = fn()
            expiry = time.monotonic() + ttl_seconds
            with _cache_lock:
                _cache[key] = (expiry, value)
            _log.info("refreshed stale cache key %s in %.2fs", key, time.monotonic() - t0)
    except Exception as exc:
        _log.warning("background refresh for %s failed; keeping stale value: %s", key, exc)
    finally:
        with _cache_lock:
            _refreshing.discard(key)


def _start_refresh_once(key: str, ttl_seconds: float, fn: Callable[[], T]) -> None:
    with _cache_lock:
        if key in _refreshing:
            return
        _refreshing.add(key)
    threading.Thread(
        target=_refresh_cache_key,
        args=(key, ttl_seconds, fn),
        name="cache-refresh",
        daemon=True,
    ).start()


def cached(key: str, ttl_seconds: float, fn: Callable[[], T]) -> T:
    """Return a cached value for `key`.

    Fresh hits return immediately. Expired hits also return immediately,
    but trigger a single bounded background refresh. This stale-while-
    revalidate behavior is important for the dashboard: a viewer should
    not wait tens of seconds just because the live SQLite/DuckDB refresh
    is fighting the producer or paying a cold attach cost.

    First misses still compute synchronously because there is no value to
    serve yet. On refresh failure, return stale if present; otherwise
    re-raise.
    """
    now = time.monotonic()
    with _cache_lock:
        hit = _cache.get(key)
    if hit is not None:
        expiry, value = hit
        if expiry > now:
            return value
        _start_refresh_once(key, ttl_seconds, fn)
        return value

    try:
        value = fn()
    except Exception as exc:
        if hit is not None:
            _log.warning("cached(%s) refresh failed, serving stale value: %s", key, exc)
            return hit[1]
        raise
    with _cache_lock:
        _cache[key] = (time.monotonic() + ttl_seconds, value)
    return value


def cache_invalidate_all() -> None:
    """Drop every cached entry. Called on shutdown."""
    with _cache_lock:
        _cache.clear()
        _refreshing.clear()


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
    """Return a shared read-only consumer session, recycling it after
    `_CONN_MAX_AGE_SECONDS` so the underlying SQLite shared lock gets
    released and the producer's WAL can checkpoint.

    DuckDB sees fresh data on every query via SQLite WAL anyway, so we
    don't need to reopen on producer mtime changes — but we *do* need
    to release the lock periodically to let the producer's WAL not
    grow forever (we observed 101 GB WAL when the lock was held for
    hours).

    Callers should not run SQL on the returned connection directly —
    use `.cursor()` (see `query()` / `query_df()` / `query_scalar()`)
    so concurrent threads don't trample each other on a shared
    connection's transaction state."""
    global _conn, _conn_opened_at
    with _conn_lock:
        now = time.monotonic()
        if _conn is not None and _conn_opened_at is not None:
            if now - _conn_opened_at > _CONN_MAX_AGE_SECONDS:
                _log.info(
                    "recycling consumer DuckDB session after %.1fs to release "
                    "SQLite shared lock (so producer can checkpoint its WAL)",
                    now - _conn_opened_at,
                )
                # Don't close right now — in-flight cursors from this
                # conn (running on other threads, lock-free) would die.
                # Park it; close on next recycle when any in-flight
                # queries have long since drained.
                _retired_conns.append(_conn)
                _conn = None
        # Close anything retired a previous cycle ago.
        while len(_retired_conns) > 1:
            old = _retired_conns.pop(0)
            try:
                old.close()
            except Exception:
                pass
        if _conn is None:
            producer_path = resolve_producer_db_path()
            _conn = open_session(producer_path)
            _conn_opened_at = time.monotonic()
        return _conn


def close_conn() -> None:
    global _conn, _conn_producer_mtime, _conn_opened_at
    with _conn_lock:
        if _conn is not None:
            try:
                _conn.close()
            except Exception:
                pass
            _conn = None
            _conn_producer_mtime = None
            _conn_opened_at = None
        while _retired_conns:
            old = _retired_conns.pop()
            try:
                old.close()
            except Exception:
                pass
    cache_invalidate_all()


def _cursor() -> duckdb.DuckDBPyConnection:
    """Return a fresh DuckDB cursor (a per-call duplicate connection
    sharing the same catalog). Concurrent threads must each use their
    own cursor — a single DuckDBPyConnection is not safe for
    simultaneous SQL execution. Using a cursor here is what makes the
    ~10 parallel fetches each page issues actually run in parallel."""
    return get_conn().cursor()


def query(sql: str) -> list[dict[str, Any]]:
    """Execute SQL and return a list of dicts."""
    cur = _cursor()
    try:
        df = cur.execute(sql).df()
    finally:
        cur.close()
    # `df.where(df.notna(), None)` does not coerce NaN to None on float columns
    # (pandas preserves the float dtype, so None round-trips back to NaN). Cast
    # to object first so None survives `to_dict`.
    return df.astype(object).where(df.notna(), None).to_dict(orient="records")


def query_df(sql: str) -> pd.DataFrame:
    """Execute SQL and return a DataFrame."""
    cur = _cursor()
    try:
        return cur.execute(sql).df()
    finally:
        cur.close()


def query_scalar(sql: str, default: Any = None) -> Any:
    """Execute SQL and return the single scalar result, or default if empty."""
    cur = _cursor()
    try:
        row = cur.execute(sql).fetchone()
    finally:
        cur.close()
    if row is None:
        return default
    return row[0]


# ── Schedule routing ─────────────────────────────────────────────────────
#
# The producer can write multiple schedules into one SQLite file (e.g.
# `7904-prelim` and `eip-8037` running in parallel). The consumer routes
# every /api/* call to one specific schedule via a `?schedule=NAME`
# query param. `resolve_schedule()` picks a sensible default when the
# caller omits the param (typically the producer's only / most recently
# active schedule) so single-schedule deployments and ad-hoc curl probes
# keep working without explicit configuration.

_SAFE_SCHEDULE_NAME = __import__("re").compile(r"^[A-Za-z0-9._\-]+$")


def list_schedules() -> list[str]:
    """Distinct schedule_name values the producer has data for, ordered
    most-recent first.

    Reads from `block_coverage` rather than `analysis_runs` because the
    producer populates block_coverage as soon as it commits block-level
    data, while `analysis_runs` rows are written at run-boundary time
    only. A producer that's mid-first-replay can have millions of
    block_coverage rows and zero analysis_runs rows, and we want the
    consumer to surface those schedules right away. Ordering by
    `max(block_number)` per schedule keeps the most-recently-active
    schedule first, which matches the prior `max(run_id)` ordering.
    """
    rows = query("""
        SELECT schedule_name, max(block_number) AS last_block
        FROM block_coverage
        GROUP BY schedule_name
        ORDER BY last_block DESC
    """)
    return [r["schedule_name"] for r in rows]


def resolve_schedule(schedule: str | None) -> str:
    """Validate or pick a schedule name.

    - If `schedule` is non-empty and matches `[A-Za-z0-9._-]+`, use it.
      Anything else risks SQL injection since we interpolate the name
      into WHERE clauses (we can't bind via params here because some
      queries are wrapped in views / generated dynamically).
    - If `schedule` is None / empty, fall back to the most recent
      schedule in `analysis_runs`. This makes single-schedule
      deployments and ad-hoc probes work without an explicit param.
    """
    if schedule:
        if not _SAFE_SCHEDULE_NAME.fullmatch(schedule):
            raise ValueError(f"invalid schedule name: {schedule!r}")
        return schedule
    candidates = list_schedules()
    if not candidates:
        raise RuntimeError(
            "no analysis_runs rows in the producer DB; can't pick a "
            "default schedule. Pass ?schedule=NAME explicitly or wait "
            "for the producer to write at least one run."
        )
    return candidates[0]


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
