"""Utility Knowledge Agent.

A ChatAgent implementation that combines Vector Search retrieval with a
chat-model call. Citations survive end-to-end: every retrieved chunk
carries its source path, page number, and chunk id, and the agent
returns them alongside the response via `custom_outputs` so the app
can render citation chips.

Config is read via `mlflow.models.ModelConfig` so the logged model
picks up the endpoint and index names passed to `log_model(model_config=...)`.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from typing import Any

import mlflow
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import ChatAgentChunk, ChatAgentMessage, ChatAgentResponse, ChatContext


SYSTEM_PROMPT = """You are a utility knowledge assistant. You help engineers,
operators, and field crews find authoritative information about substations,
equipment, procedures, and historical decisions.

Ground rules you must follow every time:

1. Ground every factual claim about equipment, settings, procedures, or
   decisions in the retrieved context provided below. Do not answer from
   general knowledge on utility-specific topics.
2. Cite every claim with the chunk_id. Format citations as [doc:CHUNK_ID].
3. If retrieval returns nothing relevant, say so plainly. Do not guess.
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

    @property
    def deploy_client(self):
        # MLflow's deployment client handles dict→SDK type conversion cleanly
        # and is the Databricks-recommended path for agent-to-endpoint calls.
        if getattr(self, "_deploy_client", None) is None:
            from mlflow.deployments import get_deploy_client

            self._deploy_client = get_deploy_client("databricks")
        return self._deploy_client

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
        data = result.get("result", {}).get("data_array", []) or []
        out: list[RetrievedChunk] = []
        for row in data:
            # Column order is the `columns` list above, with the score appended.
            out.append(
                RetrievedChunk(
                    chunk_id=row[0],
                    doc_id=row[1],
                    source_path=row[2] or "",
                    source_kind=row[3] or "",
                    page_number=int(row[4]) if row[4] is not None else 0,
                    chunk_text=row[5] or "",
                    score=float(row[6]) if len(row) > 6 and row[6] is not None else 0.0,
                )
            )
        return out

    def _format_context(self, chunks: list[RetrievedChunk]) -> str:
        blocks = []
        for c in chunks:
            label = os.path.basename(c.source_path) if c.source_path else c.chunk_id
            blocks.append(
                f"[doc:{c.chunk_id}]  source={label}  kind={c.source_kind}  "
                f"page={c.page_number}\n{c.chunk_text}"
            )
        return "\n\n---\n\n".join(blocks)

    def _call_llm(self, messages: list[dict[str, Any]]) -> str:
        response = self.deploy_client.predict(
            endpoint=self.llm_endpoint,
            inputs={"messages": messages, "max_tokens": 1024},
        )
        return response["choices"][0]["message"]["content"]

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
                    "Retrieved context follows. Cite chunk_ids that you use with "
                    "[doc:CHUNK_ID].\n\n" + context_block
                ),
            },
        ]

        answer = self._call_llm(llm_messages)

        return ChatAgentResponse(
            messages=[
                ChatAgentMessage(
                    id=str(uuid.uuid4()),
                    role="assistant",
                    content=answer,
                    name="utility_assistant",
                )
            ],
            custom_outputs={"citations": [c.to_dict() for c in chunks]},
        )

    def predict_stream(
        self,
        messages: list[ChatAgentMessage],
        context: ChatContext | None = None,
        custom_inputs: dict[str, Any] | None = None,
    ):
        # Non-streaming fallback — the app contracts on predict.
        resp = self.predict(messages, context, custom_inputs)
        for msg in resp.messages:
            yield ChatAgentChunk(delta=msg)


# Read config passed via `mlflow.pyfunc.log_model(model_config=...)`.
# `development_config` is the fallback used when the module is run outside a
# logged model (e.g. during local iteration).
_config = mlflow.models.ModelConfig(
    development_config={
        "LLM_ENDPOINT": "databricks-claude-sonnet-4-6",
        "VS_ENDPOINT_NAME": "utility-knowledge-vs",
        "VS_INDEX_NAME": "utility_knowledge.curated.document_chunks_idx",
    }
)

AGENT = UtilityKnowledgeAgent(
    llm_endpoint=_config.get("LLM_ENDPOINT"),
    vs_endpoint_name=_config.get("VS_ENDPOINT_NAME"),
    index_name=_config.get("VS_INDEX_NAME"),
)

mlflow.models.set_model(AGENT)
