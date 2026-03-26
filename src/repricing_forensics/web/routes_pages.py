"""HTML page routes for the EIP-7904 analysis web server."""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def briefing(request: Request):
    return request.app.state.templates.TemplateResponse(
        request=request, name="briefing.html", context={"active": "briefing"}
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
