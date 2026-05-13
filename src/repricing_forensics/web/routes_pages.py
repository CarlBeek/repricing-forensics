"""HTML page routes for the gas repricing analysis web server."""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

router = APIRouter()

# Maps each EIP-specific page to the producer schedule name whose data
# it should display. Hardcoded because each page's content is fixed —
# /eip7904 is always about the 7904 schedule, /eip8037 always about
# 8037. The producer can run both schedules into the same SQLite file
# concurrently (e.g. `7904-prelim` + `eip-8037`); pages render the
# constants below into a JS variable and every fetchJSON URL carries
# `?schedule=...` so the consumer routes the query to the right one.
#
# Keep these in sync with whatever the reth-research producer is
# actually writing (see its `--research.csv NAME=...` / `--research.eip8037`
# flags). If a producer ships a renamed schedule, update both values.
SCHEDULE_7904 = "7904-prelim"
SCHEDULE_8037 = "eip-8037"


def _page_context(active: str, **extra) -> dict:
    """Shared context every page template gets so its JS can construct
    `?schedule=` URLs without hardcoding names per-template."""
    return {
        "active": active,
        "schedule_7904": SCHEDULE_7904,
        "schedule_8037": SCHEDULE_8037,
        **extra,
    }


@router.get("/", response_class=HTMLResponse)
async def landing(request: Request):
    return request.app.state.templates.TemplateResponse(
        request=request, name="landing.html", context=_page_context("home"),
    )


@router.get("/eip7904", response_class=HTMLResponse)
async def eip7904(request: Request):
    return request.app.state.templates.TemplateResponse(
        request=request, name="eip7904.html",
        context=_page_context("eip7904", schedule=SCHEDULE_7904),
    )


@router.get("/eip7904/forensics", response_class=HTMLResponse)
async def eip7904_forensics(request: Request):
    return request.app.state.templates.TemplateResponse(
        request=request, name="eip7904_forensics.html",
        context=_page_context("eip7904", schedule=SCHEDULE_7904),
    )


@router.get("/eip8037", response_class=HTMLResponse)
async def eip8037(request: Request):
    return request.app.state.templates.TemplateResponse(
        request=request, name="eip8037.html",
        context=_page_context("eip8037", schedule=SCHEDULE_8037),
    )


@router.get("/affected", response_class=HTMLResponse)
async def affected(request: Request):
    return request.app.state.templates.TemplateResponse(
        request=request, name="affected.html", context=_page_context("affected"),
    )


@router.get("/affected/{address}", response_class=HTMLResponse)
async def affected_contract(request: Request, address: str):
    return request.app.state.templates.TemplateResponse(
        request=request, name="contract.html",
        context=_page_context("affected", address=address),
    )


@router.get("/tx/{tx_hash}", response_class=HTMLResponse)
async def tx_detail(request: Request, tx_hash: str):
    return request.app.state.templates.TemplateResponse(
        request=request, name="tx.html",
        context=_page_context("affected", tx_hash=tx_hash),
    )


@router.get("/about", response_class=HTMLResponse)
async def about(request: Request):
    return request.app.state.templates.TemplateResponse(
        request=request, name="about.html", context=_page_context("about"),
    )
