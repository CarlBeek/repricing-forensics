"""HTML page routes for the EIP-7904 analysis web server."""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def briefing(request: Request):
    return request.app.state.templates.TemplateResponse(
        request=request, name="briefing.html", context={"active": "overview"}
    )


@router.get("/forensics", response_class=HTMLResponse)
async def forensics(request: Request):
    return request.app.state.templates.TemplateResponse(
        request=request, name="forensics.html", context={"active": "forensics"}
    )


@router.get("/affected", response_class=HTMLResponse)
async def affected(request: Request):
    return request.app.state.templates.TemplateResponse(
        request=request, name="affected.html", context={"active": "affected"}
    )


@router.get("/affected/{address}", response_class=HTMLResponse)
async def affected_contract(request: Request, address: str):
    return request.app.state.templates.TemplateResponse(
        request=request, name="contract.html", context={"active": "affected", "address": address}
    )


@router.get("/tx/{tx_hash}", response_class=HTMLResponse)
async def tx_detail(request: Request, tx_hash: str):
    return request.app.state.templates.TemplateResponse(
        request=request, name="tx.html", context={"active": "affected", "tx_hash": tx_hash}
    )
