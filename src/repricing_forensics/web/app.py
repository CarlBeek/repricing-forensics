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
