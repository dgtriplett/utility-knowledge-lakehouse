"""Utility Knowledge Assistant — Databricks App backend.

Serves a single-page chat UI (static/index.html) and calls the agent
serving endpoint directly via REST. Inherits the caller's Unity Catalog
identity — row filters on the underlying tables apply automatically.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import requests
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

# WorkspaceClient lazily resolves the host + auth from the runtime's
# built-in credentials. We use it purely to obtain those for a direct
# REST call — the SDK's .query() method has a dict-vs-ChatMessage type
# mismatch that breaks agents endpoints in recent releases.
workspace = WorkspaceClient()
HOST = workspace.config.host.rstrip("/")

app = FastAPI(title="Utility Knowledge Assistant")

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
    return {"status": "ok", "endpoint": AGENT_ENDPOINT, "host": HOST}


def _auth_headers() -> dict[str, str]:
    # `authenticate()` returns headers including Authorization. This
    # works both in Databricks Apps (OAuth service principal) and when
    # run locally against a PAT.
    return workspace.config.authenticate()


@app.post("/api/chat")
def chat(req: ChatRequest, request: Request) -> dict:
    user = request.headers.get("X-Forwarded-Email") or "unknown"
    log.info("chat request from %s (%d turns)", user, len(req.messages))

    payload = {
        "messages": [{"role": m.role, "content": m.content} for m in req.messages],
    }
    url = f"{HOST}/serving-endpoints/{AGENT_ENDPOINT}/invocations"

    try:
        r = requests.post(url, headers=_auth_headers(), json=payload, timeout=120)
        r.raise_for_status()
    except requests.HTTPError:
        log.exception("agent call failed: %s", r.text[:1000])
        raise HTTPException(status_code=r.status_code, detail=r.text[:400])
    except Exception as exc:
        log.exception("agent call failed")
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    body = r.json()
    # ChatAgentResponse shape: {"messages": [{"role": "assistant", "content": "..."}, ...], "custom_outputs": {...}}
    messages = body.get("messages") or []
    last = next((m for m in reversed(messages) if m.get("role") == "assistant"), None)
    content = (last or {}).get("content", "")

    custom_outputs = body.get("custom_outputs") or {}
    citations = custom_outputs.get("citations", []) if isinstance(custom_outputs, dict) else []

    return {"role": "assistant", "content": content, "citations": citations}
