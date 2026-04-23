"""Utility Knowledge Agent.

A ChatAgent implementation that combines Vector Search retrieval with
direct chat-model reasoning. Citations are preserved end-to-end: every
retrieved chunk carries its source path, page number, and chunk id, and
the agent returns them alongside the response so the app can render
citation chips.

This file is logged as an MLflow pyfunc by `deploy_agent.py`.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

import mlflow
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import (
    ChatAgentChunk,
    ChatAgentMessage,
    ChatAgentResponse,
    ChatContext,
)


RETRIEVER_TOOL = "search_utility_knowledge"

SYSTEM_PROMPT = """You are a utility knowledge assistant. You help engineers,
operators, and field crews find authoritative information about substations,
equipment, procedures, and historical decisions.

Ground rules you must follow every time:

1. For any factual claim about equipment, settings, procedures, or decisions,
   call the `search_utility_knowledge` tool first. Do not answer from general
   knowledge on utility-specific topics.
2. Cite every claim with the chunk_id returned by the tool. Format each
   citation as [doc:CHUNK_ID].
3. If the search returns nothing relevant, say so plainly. Do not guess.
4. If a retrieved SME debrief contradicts a document, surface the
   contradiction — the SME context is usually the *reason* for an apparent
   discrepancy.
5. Keep answers concise. Prefer lists for equipment-level details.
"""


@dataclass
class RetrievedChunk:
    chunk_id: str
    doc_id: str
    source_path: str
    source_kind: str
    page_number: int
    chunk_text: str
    score: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "chunk_id": self.chunk_id,
            "doc_id": self.doc_id,
            "source_path": self.source_path,
            "source_kind": self.source_kind,
            "page_number": self.page_number,
            "snippet": self.chunk_text[:280],
            "score": self.score,
        }


class UtilityKnowledgeAgent(ChatAgent):
    def __init__(
        self,
        llm_endpoint: str,
        vs_endpoint_name: str,
        index_name: str,
        k: int = 6,
    ) -> None:
        self.llm_endpoint = llm_endpoint
        self.vs_endpoint_name = vs_endpoint_name
        self.index_name = index_name
        self.k = k
        self._vs_client = None
        self._workspace_client = None

    # Lazy init so MLflow model loading stays cheap.
    @property
    def vs_client(self):
        if self._vs_client is None:
            from databricks.vector_search.client import VectorSearchClient

            self._vs_client = VectorSearchClient(disable_notice=True)
        return self._vs_client

    @property
    def workspace_client(self):
        if self._workspace_client is None:
            from databricks.sdk import WorkspaceClient

            self._workspace_client = WorkspaceClient()
        return self._workspace_client

    def _retrieve(self, query: str) -> list[RetrievedChunk]:
        index = self.vs_client.get_index(
            endpoint_name=self.vs_endpoint_name, index_name=self.index_name
        )
        result = index.similarity_search(
            query_text=query,
            columns=[
                "chunk_id",
                "doc_id",
                "source_path",
                "source_kind",
                "page_number",
                "chunk_text",
            ],
            num_results=self.k,
            query_type="HYBRID",
        )
        data = result.get("result", {}).get("data_array", [])
        # Column order matches the `columns` list above plus a trailing score.
        out: list[RetrievedChunk] = []
        for row in data:
            out.append(
                RetrievedChunk(
                    chunk_id=row[0],
                    doc_id=row[1],
                    source_path=row[2],
                    source_kind=row[3],
                    page_number=row[4],
                    chunk_text=row[5],
                    score=float(row[6]),
                )
            )
        return out

    def _format_context(self, chunks: list[RetrievedChunk]) -> str:
        blocks = []
        for c in chunks:
            blocks.append(
                f"[doc:{c.chunk_id}]  source={os.path.basename(c.source_path)}  "
                f"kind={c.source_kind}  page={c.page_number}\n{c.chunk_text}"
            )
        return "\n\n---\n\n".join(blocks)

    def _call_llm(self, messages: list[dict[str, Any]]) -> str:
        response = self.workspace_client.serving_endpoints.query(
            name=self.llm_endpoint, messages=messages
        )
        return response.choices[0].message.content

    def predict(
        self,
        messages: list[ChatAgentMessage],
        context: ChatContext | None = None,
        custom_inputs: dict[str, Any] | None = None,
    ) -> ChatAgentResponse:
        last_user = next(
            (m.content for m in reversed(messages) if m.role == "user"), ""
        )

        chunks = self._retrieve(last_user)
        context_block = self._format_context(chunks) or "(no relevant results)"

        llm_messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            *[
                {"role": m.role, "content": m.content}
                for m in messages
                if m.role in ("user", "assistant")
            ],
            {
                "role": "system",
                "content": (
                    "Retrieved context follows. Cite chunk_ids that you use "
                    "with [doc:CHUNK_ID].\n\n" + context_block
                ),
            },
        ]

        answer = self._call_llm(llm_messages)

        return ChatAgentResponse(
            messages=[
                ChatAgentMessage(
                    role="assistant",
                    content=answer,
                    name="utility_assistant",
                )
            ],
            custom_outputs={
                "citations": [c.to_dict() for c in chunks],
            },
        )

    def predict_stream(
        self,
        messages: list[ChatAgentMessage],
        context: ChatContext | None = None,
        custom_inputs: dict[str, Any] | None = None,
    ):
        # Streaming omitted for brevity in the reference impl — the non-stream
        # path is the contract the app depends on.
        resp = self.predict(messages, context, custom_inputs)
        for msg in resp.messages:
            yield ChatAgentChunk(delta=msg)


# Entrypoint that MLflow's pyfunc loader instantiates.
def get_agent() -> UtilityKnowledgeAgent:
    return UtilityKnowledgeAgent(
        llm_endpoint=os.environ.get("LLM_ENDPOINT", "databricks-claude-sonnet-4-6"),
        vs_endpoint_name=os.environ.get("VS_ENDPOINT_NAME", "utility-knowledge-vs"),
        index_name=os.environ.get(
            "VS_INDEX_NAME", "utility_knowledge.curated.document_chunks_idx"
        ),
    )


mlflow.models.set_model(get_agent())
