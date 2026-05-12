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
    from .db import SCHEDULE_NAME

    env_value = os.environ.get("PRODUCER_DB_PATH")
    path = resolve_producer_db_path()
    info = {
        "env_PRODUCER_DB_PATH": env_value,
        "resolved_path": str(path),
        "exists": path.exists(),
        "size_bytes": path.stat().st_size if path.exists() else None,
        "schedule_name": SCHEDULE_NAME,
    }
    try:
        conn = open_session(path, SCHEDULE_NAME)
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
    return info


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
