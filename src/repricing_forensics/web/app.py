"""FastAPI application for the EIP-7904 analysis web server."""
from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .db import close_conn, get_conn, load_labels
from .routes_api import router as api_router
from .routes_pages import router as pages_router

_WEB_DIR = Path(__file__).resolve().parent


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: open DuckDB connection and load labels
    get_conn()
    load_labels()
    yield
    # Shutdown: close DuckDB
    close_conn()


app = FastAPI(title="EIP-7904 Repricing Analysis", lifespan=lifespan)

# Templates and static files
app.state.templates = Jinja2Templates(directory=str(_WEB_DIR / "templates"))
app.mount("/static", StaticFiles(directory=str(_WEB_DIR / "static")), name="static")

# Routers — API first (more specific prefix), then pages (catch-all)
app.include_router(api_router)
app.include_router(pages_router)
