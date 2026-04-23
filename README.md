# Utility Knowledge Lakehouse

A reference implementation of an intelligent knowledge system for utilities, built entirely on Databricks. This is the companion repo to *[Future-Proofing Utility Expertise, Part 2: Building It on Databricks](https://medium.com/power-systems-evolving-futures)*.

The system takes the four-layer framework — **digitize the analog, consolidate the digital, capture the implicit, serve the knowledge** — and turns it into a working end-to-end pipeline you can stand up in a fresh workspace in about an hour. It ships with synthetic sample data (substation one-lines, protection studies, SME debrief transcripts) so you can see it work without touching real asset data.

---

## What you get

- A Databricks Asset Bundle that provisions every resource: catalog, schemas, volumes, jobs, vector search endpoint, agent, and Databricks App
- Synthetic sample data generators (no real utility data required)
- Document parsing and field extraction using `ai_parse_document` and `ai_extract`
- Consolidation, dedup, chunking, and a hybrid vector search index
- SME debrief processing pipeline (structured decision/rationale extraction)
- A Mosaic AI Agent Framework agent with retrieval + Genie tools, registered via MLflow and deployed to Model Serving
- A Databricks App front-end (FastAPI + simple chat UI) that inherits Unity Catalog permissions
- An MLflow evaluation harness with a small hand-labeled test set

## Prerequisites

- A Databricks workspace on AWS, Azure, or GCP with Unity Catalog enabled
- Serverless compute enabled (for AI functions)
- Permission to create catalogs, or an existing catalog you can use
- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/install.html) v0.230+ configured (`databricks configure`)
- Python 3.11+ locally (only needed for data generation; everything else runs in the workspace)

That's it. No external API keys, no separate vector DB, no third-party OCR service.

## Quickstart

```bash
git clone https://github.com/dgtriplett/utility-knowledge-lakehouse.git
cd utility-knowledge-lakehouse

# Generate the synthetic data locally
pip install -r requirements.txt
python -m src.sample_data.generate_data --out ./sample_data

# Deploy everything to your workspace
databricks bundle deploy --target dev

# Run the end-to-end pipeline (bootstrap -> ingest -> parse -> index -> agent)
databricks bundle run end_to_end --target dev
```

When that job finishes (~25 min on a fresh workspace), you'll have:

- A catalog `utility_knowledge` with curated tables
- A vector search index `utility_knowledge.curated.document_chunks_idx`
- A registered model `utility_knowledge.agents.utility_assistant` deployed to a serving endpoint
- A Databricks App at `<workspace-url>/apps/utility-knowledge-assistant`

Open the app, ask *"what do you know about breaker 234L-7"* or *"summarize the protection study for Oak Ridge substation"*, and you should see grounded answers with citations back to the source PDFs.

## Repository layout

```
.
├── databricks.yml                    # Asset Bundle root
├── resources/                        # Bundle resource definitions
│   ├── schemas.yml                   # Catalog, schemas, volumes
│   ├── jobs.yml                      # All pipeline jobs
│   ├── endpoints.yml                 # Vector search + serving endpoints
│   └── app.yml                       # Databricks App
├── src/
│   ├── setup/
│   │   └── 00_bootstrap.py           # Create UC resources
│   ├── sample_data/
│   │   └── generate_data.py          # Synthetic PDFs + transcripts
│   ├── layer1_digitize/
│   │   ├── 01_parse_documents.py
│   │   └── 02_extract_fields.py
│   ├── layer2_consolidate/
│   │   ├── 03_consolidate_sources.py
│   │   ├── 04_chunk_documents.py
│   │   └── 05_create_vector_index.py
│   ├── layer3_implicit/
│   │   └── 06_process_debriefs.py
│   ├── layer4_serve/
│   │   ├── agent.py                  # ChatAgent implementation
│   │   ├── deploy_agent.py           # Register + deploy
│   │   └── eval_harness.py
│   └── app/
│       ├── app.yaml                  # App manifest
│       ├── main.py                   # FastAPI backend
│       ├── requirements.txt
│       └── static/index.html         # Chat UI
├── tests/
├── docs/
│   ├── ARCHITECTURE.md
│   ├── SETUP.md
│   └── TROUBLESHOOTING.md
└── requirements.txt
```

## The four layers, mapped to code

| Layer | What it does | Where it lives |
| --- | --- | --- |
| 1. Digitize the analog | Parse scanned PDFs, extract typed fields with citations | `src/layer1_digitize/` |
| 2. Consolidate the digital | Dedup, chunk, index into hybrid vector search | `src/layer2_consolidate/` |
| 3. Capture the implicit | Transcribe + structure SME debriefs into decisions + rationale | `src/layer3_implicit/` |
| 4. Serve the knowledge | Agent with retrieval + Genie tools, deployed behind an app | `src/layer4_serve/` + `src/app/` |

Each layer's notebooks are self-contained and idempotent — run them in order the first time, then re-run any single one as content changes.

## What's synthetic vs. what's real

The pipeline and infrastructure are real and production-shaped. The **data** is synthetic: a generator in `src/sample_data/generate_data.py` produces ~50 fake substation one-line PDFs, ~30 protection coordination studies, and ~20 SME debrief transcripts modeled after the kinds of documents utilities actually have. None of it is scraped or derived from any real utility's records.

Replacing the synthetic corpus with your own is a two-line change: point `src/layer1_digitize/01_parse_documents.py` at your Volume. Everything downstream is schema-compatible.

## Cost expectations

Running the full quickstart on a fresh workspace, end-to-end:

| Component | Approximate cost |
| --- | --- |
| Document parsing + extraction (50 docs, sample) | ~$2 |
| Vector Search endpoint (8 hours idle + usage) | ~$5 |
| Agent serving endpoint (scale-to-zero, light use) | <$1 |
| Compute (serverless, ~25 min pipeline) | ~$3 |
| **Total for a day of playing with it** | **~$10–15** |

When you're done, `databricks bundle destroy --target dev` tears it all down cleanly.

## Configuration

The bundle reads a few variables from `databricks.yml`. Override them per-target or via `--var`:

| Variable | Default | What it controls |
| --- | --- | --- |
| `catalog` | `utility_knowledge` | UC catalog name |
| `llm_endpoint` | `databricks-claude-sonnet-4-6` | Chat model for extraction + agent |
| `embedding_endpoint` | `databricks-gte-large-en` | Embeddings for vector search |
| `vs_endpoint_name` | `utility-knowledge-vs` | Vector Search endpoint |
| `app_name` | `utility-knowledge-assistant` | Databricks App name |

## Going beyond the sample

Once you have the sample running, the natural next steps are:

1. **Swap in Lakeflow Connect** for one real source (SharePoint, Google Drive, a file share). See `docs/REAL_SOURCES.md`.
2. **Turn on row-level security** via Unity Catalog row filters. A template is in `resources/schemas.yml`.
3. **Add a Genie space** over the structured tables so the agent can answer quantitative questions.
4. **Wire embedded context** into EAM or GIS using the same serving endpoint. An example curl pattern is in `docs/EMBEDDED.md`.

## Contributing / issues

If something doesn't work the way the blog describes, please open an issue. If you're a utility doing this work and want to compare notes, reach out — there's a lot of room for the sector to share patterns here.

## License

Apache 2.0. See [LICENSE](LICENSE).
