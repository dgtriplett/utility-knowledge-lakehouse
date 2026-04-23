# Architecture

This document covers the shape of the system, what each layer writes, and why the seams are where they are.

## Diagram

```
 Sources              Layer 1              Layer 2              Layer 3              Layer 4
 -------              -------              -------              -------              -------
 SharePoint   ─┐                                                ┌─ Audio recordings
 Google Drive ─┤   raw_documents ──┐                            │   (raw_audio Volume)
 File shares  ─┤   raw_audio       │                            │
 Email        ─┘     (Volumes)     ▼                            ▼
                              ai_parse_document           (Whisper on Serving)
                              ai_extract                        │
                                    │                           ▼
                                    ▼                    ai_query (Claude)
                            raw.parsed_elements          decisions + rationale
                            curated.document_fields             │
                                    │                           │
                                    ▼                           ▼
                              curated.documents  ◄─────  curated.sme_debriefs
                                    │
                                    ▼
                              curated.document_chunks  ── (debrief chunks appended)
                                    │
                                    ▼
                              Vector Search index (hybrid)
                                    │
                                    ▼
                          MLflow-logged ChatAgent
                                    │
                                    ▼
                          Model Serving endpoint  ◄─── Databricks App (FastAPI)
                                    │
                                    ▼
                              MLflow tracing +
                              eval_runs table
```

## Unity Catalog layout

Everything lives under one catalog (`utility_knowledge` by default), three schemas:

| Schema | Purpose | Example objects |
| --- | --- | --- |
| `raw` | Landed data. Volumes for binary content, Delta tables for parsed structure. | `raw_documents` (Volume), `raw_audio` (Volume), `parsed_documents`, `parsed_elements` |
| `curated` | Governed, deduped, consumer-facing. Row filters and masks go here. | `documents`, `document_fields`, `document_chunks`, `sme_debriefs`, `document_chunks_idx` (VS index) |
| `agents` | Registered agents, evaluation artifacts. | `utility_assistant` (registered model), `eval_runs` |

## Provenance contract

Every derived row carries enough to recover its source:

| Column | Meaning |
| --- | --- |
| `doc_id` | Deterministic hash of the source path. Stable across re-ingests. |
| `source_path` | Volume path to the original file. |
| `source_kind` | `oneline`, `protection_study`, `debrief`, etc. |
| `page_number` | For paged documents, the page the content came from. |
| `bbox` | Bounding box from `ai_parse_document` (kept on `parsed_elements`). |
| `chunk_id` | For chunks: stable composite of `doc_id_page_chunk`. |
| `extraction_model_version` | Which model produced the extracted field. |

The agent returns `chunk_id`, `doc_id`, `page_number`, and `source_path` on every retrieved chunk so the app can render citation chips linked back to the source.

## Seams worth knowing about

**Between Layer 1 and Layer 2.** Extraction writes to `curated.document_fields` and `curated.extraction_needs_review` — not straight to `curated.documents`. Anything with ambiguous fields lands in review. Promote from review to curated with a SQL `INSERT INTO … SELECT` once reviewed.

**Between Layer 2 and Layer 4.** The agent reads from the Vector Search index, not the Delta table directly. This is a governance choice: the index is the published, permission-filtered surface. Any row filter on `document_chunks` flows through because the delta-sync index inherits from the source table.

**Between Layer 3 and Layer 2.** Debrief chunks are appended into the same `document_chunks` table as document chunks. That way the retriever surfaces both in one call, and the agent can cross-reference a study's setting recommendation with the SME explanation of why that setting exists.

**Between Layer 4 and the app.** The app never holds a service principal token. It uses the user's workspace identity via OAuth. That's how Unity Catalog row filters applied to `document_chunks` translate into different answers for different users, without a line of app-level permission logic.

## What's *not* included in the reference impl

- **Row-level security** is declared in documentation but not turned on — utilities have very different regional/departmental permission models and a baked-in example would be wrong for most. See `docs/ROW_SECURITY.md` for the pattern.
- **Genie space** — the blog post recommends adding one over the structured tables for quantitative questions. It's a click in the UI, not a Terraform resource, so it's not in the bundle. Docs walk through it.
- **A Whisper transcription endpoint** — we ship debriefs as text so the pipeline demos without needing audio setup. The commented code in `src/layer3_implicit/06_process_debriefs.py` shows the real pattern.
- **Lakeflow Connect sources** — the bundle models consolidation but doesn't provision SharePoint/Drive connectors. Adding one is ~15 lines of YAML; see `docs/REAL_SOURCES.md`.
