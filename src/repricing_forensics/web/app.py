"""FastAPI application for the gas repricing analysis web server."""
from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .db import close_conn, get_conn, load_labels
from .routes_api import router as api_router
from .routes_pages import router as pages_router

_WEB_DIR = Path(__file__).resolve().parent
_CACHE_BUST = str(int(time.time()))
_log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Try to warm the producer-DB connection, but never fail startup
    # over it — a freshly-deployed server with no producer DB yet
    # should still come up so /healthz and static assets work. The
    # exception handler below turns subsequent connection failures
    # into 503s on /api/* requests.
    try:
        get_conn()
    except Exception as e:
        _log.warning("Producer DB not available at startup: %s", e)
    try:
        load_labels()
    except Exception as e:
        _log.warning("Label load failed at startup: %s", e)
    yield
    close_conn()


app = FastAPI(title="Gas Repricing Impact Analysis", lifespan=lifespan)


@app.get("/healthz", include_in_schema=False)
def healthz():
    """Liveness probe. Returns 200 even if the producer DB is missing
    so systemd / load balancers don't mark the service unhealthy
    during the window between deploy and first producer DB write."""
    return {"status": "ok"}


@app.get("/api/_debug/producer-info", include_in_schema=False)
def producer_info():
    """Where is PRODUCER_DB_PATH pointing? Does the file exist? Can we
    open it? Cheap diagnostic for production triage."""
    import os
    from repricing_forensics.source_db import resolve_producer_db_path, open_session
    from .db import list_schedules

    env_value = os.environ.get("PRODUCER_DB_PATH")
    path = resolve_producer_db_path()
    info = {
        "env_PRODUCER_DB_PATH": env_value,
        "resolved_path": str(path),
        "exists": path.exists(),
        "size_bytes": path.stat().st_size if path.exists() else None,
    }
    try:
        conn = open_session(path)
        tables = [r[0] for r in conn.execute(
            "SELECT table_name FROM duckdb_tables() WHERE database_name = 'producer'"
        ).fetchall()]
        rows = conn.execute("SELECT count(*) FROM divergences").fetchone()
        info["producer_tables"] = tables
        info["divergences_count"] = int(rows[0]) if rows else None
        info["open_ok"] = True
        conn.close()
    except Exception as e:
        info["open_ok"] = False
        info["open_error"] = f"{type(e).__name__}: {e}"
    try:
        info["schedules"] = list_schedules()
    except Exception as e:
        info["schedules_error"] = f"{type(e).__name__}: {e}"
    return info


@app.get("/api/_debug/perf", include_in_schema=False)
def perf():
    """Time the same aggregate via raw sqlite3 vs DuckDB sqlite_scanner,
    against the live producer file. Surfaces which layer is slow.

    Also reports WAL/SHM sidecar sizes so we can see whether the WAL
    has grown large (producer not checkpointing).
    """
    import sqlite3
    import time
    from repricing_forensics.source_db import resolve_producer_db_path
    from .db import get_conn, resolve_schedule

    path = resolve_producer_db_path()
    schedule_name = resolve_schedule(None)
    if not path.exists():
        return {"error": "producer DB not found", "resolved_path": str(path)}

    def file_size(p):
        try:
            return p.stat().st_size
        except FileNotFoundError:
            return None

    # File sizes — the WAL growing past ~hundreds of MB suggests the
    # producer isn't checkpointing.
    files = {
        "sqlite": file_size(path),
        "wal":    file_size(path.with_name(path.name + "-wal")),
        "shm":    file_size(path.with_name(path.name + "-shm")),
    }

    # The same aggregate run three ways. Each is a single sample; for a
    # noisy production system run it a few times and use the lowest.
    timings: dict[str, dict] = {}

    # 1. Raw sqlite3, mode=ro URI so we don't trigger journal creation.
    t0 = time.monotonic()
    try:
        ro_uri = f"file:{path}?mode=ro"
        with sqlite3.connect(ro_uri, uri=True, timeout=120) as raw:
            n = raw.execute("SELECT COUNT(*) FROM block_coverage").fetchone()[0]
        timings["sqlite3_raw_count_block_coverage"] = {
            "rows": int(n), "seconds": round(time.monotonic() - t0, 3),
        }
    except Exception as e:
        timings["sqlite3_raw_count_block_coverage"] = {
            "error": f"{type(e).__name__}: {e}",
            "seconds": round(time.monotonic() - t0, 3),
        }

    t0 = time.monotonic()
    try:
        ro_uri = f"file:{path}?mode=ro"
        with sqlite3.connect(ro_uri, uri=True, timeout=120) as raw:
            n = raw.execute(
                "SELECT SUM(tx_count) FROM block_coverage WHERE schedule_name = ?",
                (schedule_name,),
            ).fetchone()[0]
        timings["sqlite3_raw_sum_tx_count"] = {
            "result": int(n) if n is not None else None,
            "seconds": round(time.monotonic() - t0, 3),
        }
    except Exception as e:
        timings["sqlite3_raw_sum_tx_count"] = {
            "error": f"{type(e).__name__}: {e}",
            "seconds": round(time.monotonic() - t0, 3),
        }

    # 2. DuckDB through the cached sqlite_scanner attach (what /api/* uses).
    t0 = time.monotonic()
    try:
        conn = get_conn()
        n = conn.execute(
            "SELECT COUNT(*) FROM block_coverage"
        ).fetchone()[0]
        timings["duckdb_scanner_count_block_coverage"] = {
            "rows": int(n), "seconds": round(time.monotonic() - t0, 3),
        }
    except Exception as e:
        timings["duckdb_scanner_count_block_coverage"] = {
            "error": f"{type(e).__name__}: {e}",
            "seconds": round(time.monotonic() - t0, 3),
        }

    t0 = time.monotonic()
    try:
        conn = get_conn()
        n = conn.execute(
            "SELECT SUM(tx_count) FROM block_coverage"
        ).fetchone()[0]
        timings["duckdb_scanner_sum_tx_count"] = {
            "result": int(n) if n is not None else None,
            "seconds": round(time.monotonic() - t0, 3),
        }
    except Exception as e:
        timings["duckdb_scanner_sum_tx_count"] = {
            "error": f"{type(e).__name__}: {e}",
            "seconds": round(time.monotonic() - t0, 3),
        }

    # 3. Producer activity proxy — file mtime. If mtime is very recent
    # (sub-second), the writer is actively committing.
    mtime = path.stat().st_mtime
    now = time.time()
    return {
        "file_sizes_bytes": files,
        "producer_mtime_seconds_ago": round(now - mtime, 3),
        "timings_seconds": timings,
    }


@app.get("/api/_debug/chain-walk-coverage", include_in_schema=False)
def chain_walk_coverage():
    """Diagnose why so many contract-broken rows end up bucketed as
    'Unclassified' (NULL oog_bottleneck_kind). The chain-walk
    classifier in reth-research emits NULL when any frame on the
    root→OOG path is missing `gas_requested_on_stack` or
    `parent_gas_at_call`. This endpoint reports how often each field
    is populated."""
    from .db import query

    div_summary = query("""
        SELECT
            count(*)                                                  AS divergences_drill_in,
            count(*) FILTER (WHERE oog_chain_proportional = 1)        AS proportional,
            count(*) FILTER (WHERE oog_chain_proportional = 0)        AS throttled,
            count(*) FILTER (WHERE oog_chain_proportional IS NULL)    AS classifier_did_not_run,
            count(*) FILTER (WHERE oog_call_depth IS NOT NULL)        AS has_oog_info,
            count(*) FILTER (WHERE oog_call_depth IS NULL)            AS no_oog_info,
            count(*) FILTER (WHERE oog_call_depth IS NOT NULL
                              AND oog_chain_proportional IS NULL)     AS oog_but_classifier_returned_null,
            count(*) FILTER (WHERE oog_bottleneck_kind IS NOT NULL)   AS has_bottleneck_kind,
            count(*) FILTER (WHERE oog_bottleneck_kind IS NULL
                              AND oog_chain_proportional = 0)         AS throttled_unclassified
        FROM divergences
        WHERE bucket = 'contract_broken'
    """)[0]

    frame_summary = query("""
        SELECT
            count(*)                                                AS frames_total,
            count(*) FILTER (WHERE depth = 0)                       AS root_frames,
            count(*) FILTER (WHERE depth > 0)                       AS non_root_frames,
            count(*) FILTER (WHERE depth > 0
                              AND gas_requested_on_stack IS NULL)   AS non_root_missing_stack_gas,
            count(*) FILTER (WHERE depth > 0
                              AND parent_gas_at_call IS NULL)       AS non_root_missing_parent_gas,
            count(*) FILTER (WHERE depth > 0
                              AND call_type IN ('Create', 'Create2')) AS non_root_creates
        FROM call_frames cf
        JOIN divergences d USING (divergence_id)
        WHERE d.bucket = 'contract_broken'
    """)[0]

    # Per-call-type breakdown: how often does each kind of call have
    # missing chain-walk data?
    by_call_type = query("""
        SELECT cf.call_type,
               count(*) AS n,
               count(*) FILTER (WHERE cf.gas_requested_on_stack IS NULL) AS missing_stack_gas,
               count(*) FILTER (WHERE cf.parent_gas_at_call IS NULL)     AS missing_parent_gas
        FROM call_frames cf
        JOIN divergences d USING (divergence_id)
        WHERE d.bucket = 'contract_broken' AND cf.depth > 0
        GROUP BY cf.call_type
        ORDER BY n DESC
    """)

    return {
        "divergences": div_summary,
        "call_frames": frame_summary,
        "by_call_type": by_call_type,
    }


@app.exception_handler(ValueError)
async def value_error_handler(request: Request, exc: ValueError):
    """Surface bad client input (e.g. malformed `?schedule=` values)
    as a 400 instead of a 500. The schedule guard in
    `db.resolve_schedule` raises ValueError to reject names that don't
    match the safe-charset regex."""
    if request.url.path.startswith("/api/"):
        return JSONResponse(status_code=400, content={"detail": str(exc)})
    raise exc


@app.exception_handler(Exception)
async def producer_unavailable_handler(request: Request, exc: Exception):
    """Convert producer-DB-access failures (missing file, ATTACH errors,
    missing tables) into 503s on /api/* so the dashboard JS sees a
    structured error instead of a 500. Other exceptions get re-raised
    via FastAPI's default 500 path."""
    msg = str(exc).lower()
    looks_like_producer_issue = any(s in msg for s in (
        "no files found",        # DuckDB: missing file
        "io error",              # DuckDB: file open failure
        "catalog error",         # DuckDB: missing table/view
        "binder error",          # missing column on stale schema
        "does not exist",        # generic
    ))
    if looks_like_producer_issue and request.url.path.startswith("/api/"):
        _log.warning("Producer DB unavailable on %s: %s", request.url.path, exc)
        return JSONResponse(
            status_code=503,
            content={
                "detail": "Producer DB not available yet",
                "hint": (
                    "Set PRODUCER_DB_PATH to a populated DuckDB or run "
                    "scripts/build_synthetic_producer_db.py to generate "
                    "a fixture. See docs/storage-redesign.md."
                ),
            },
        )
    raise exc

# Templates and static files — inject cache_bust into all template contexts
templates = Jinja2Templates(directory=str(_WEB_DIR / "templates"))
templates.env.globals["cache_bust"] = _CACHE_BUST
app.state.templates = templates
app.mount("/static", StaticFiles(directory=str(_WEB_DIR / "static")), name="static")

# Routers — API first (more specific prefix), then pages (catch-all)
app.include_router(api_router)
app.include_router(pages_router)
