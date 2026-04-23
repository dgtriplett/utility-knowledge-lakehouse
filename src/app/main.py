"""Utility Knowledge Assistant — Databricks App backend.

Serves a single-page chat UI (static/index.html) and proxies requests to
the agent serving endpoint. Inherits the caller's Unity Catalog
identity — row filters and column masks on the underlying tables apply
automatically.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from databricks.sdk import WorkspaceClient
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
log = logging.getLogger("utility-knowledge-app")

AGENT_ENDPOINT = os.environ.get("AGENT_ENDPOINT")
if not AGENT_ENDPOINT:
    raise RuntimeError("AGENT_ENDPOINT env var is required.")

app = FastAPI(title="Utility Knowledge Assistant")
workspace = WorkspaceClient()

static_dir = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=static_dir), name="static")


class ChatTurn(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    messages: list[ChatTurn]


@app.get("/")
def root() -> FileResponse:
    return FileResponse(static_dir / "index.html")


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok", "endpoint": AGENT_ENDPOINT}


@app.post("/api/chat")
def chat(req: ChatRequest, request: Request) -> dict:
    user = request.headers.get("X-Forwarded-Email") or "unknown"
    log.info("chat request from %s (%d turns)", user, len(req.messages))
    try:
        response = workspace.serving_endpoints.query(
            name=AGENT_ENDPOINT,
            messages=[{"role": m.role, "content": m.content} for m in req.messages],
        )
    except Exception as exc:
        log.exception("agent call failed")
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    message = response.choices[0].message
    custom_outputs = getattr(response, "custom_outputs", None) or {}
    citations = custom_outputs.get("citations", []) if isinstance(custom_outputs, dict) else []

    return {
        "role": message.role,
        "content": message.content,
        "citations": citations,
    }
