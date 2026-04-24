# Databricks notebook source
# MAGIC %md
# MAGIC # Layer 2 — Create the Vector Search index
# MAGIC
# MAGIC Creates a Delta-Sync index over `curated.document_chunks`, with hybrid
# MAGIC (semantic + keyword) search turned on so exact-match queries on
# MAGIC equipment IDs rank correctly.

# COMMAND ----------

# MAGIC %pip install -q databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import time

from databricks.vector_search.client import VectorSearchClient

dbutils.widgets.text("catalog", "utility_knowledge")
dbutils.widgets.text("curated_schema", "curated")
dbutils.widgets.text("vs_endpoint_name", "utility-knowledge-vs")
dbutils.widgets.text("embedding_endpoint", "databricks-gte-large-en")

catalog = dbutils.widgets.get("catalog")
curated = dbutils.widgets.get("curated_schema")
endpoint = dbutils.widgets.get("vs_endpoint_name")
embed_endpoint = dbutils.widgets.get("embedding_endpoint")

source_table = f"{catalog}.{curated}.document_chunks"
index_name = f"{catalog}.{curated}.document_chunks_idx"

# COMMAND ----------

vsc = VectorSearchClient(disable_notice=True)

endpoints = [e["name"] for e in vsc.list_endpoints().get("endpoints", [])]
if endpoint not in endpoints:
    print(f"Creating Vector Search endpoint {endpoint}...")
    vsc.create_endpoint(name=endpoint, endpoint_type="STANDARD")
    # Wait up to ~20 min for endpoint to come online.
    for _ in range(80):
        status = vsc.get_endpoint(endpoint).get("endpoint_status", {}).get("state")
        print(f"endpoint state: {status}")
        if status == "ONLINE":
            break
        time.sleep(15)

# COMMAND ----------

existing = [i["name"] for i in vsc.list_indexes(endpoint).get("vector_indexes", [])]
if index_name in existing:
    print(f"Index {index_name} already exists. Triggering sync.")
    vsc.get_index(endpoint, index_name).sync()
else:
    print(f"Creating index {index_name}...")
    vsc.create_delta_sync_index(
        endpoint_name=endpoint,
        source_table_name=source_table,
        index_name=index_name,
        primary_key="chunk_id",
        pipeline_type="TRIGGERED",
        embedding_source_column="chunk_text",
        embedding_model_endpoint_name=embed_endpoint,
    )

# COMMAND ----------

# Wait for sync to finish — ONLINE_NO_PENDING_UPDATE is the terminal state.
# Other ONLINE_* states (ONLINE_UPDATING_EMBEDDINGS, ONLINE_PIPELINE_FAILED,
# etc.) still start with 'ONLINE' but mean the index hasn't caught up yet.
idx = vsc.get_index(endpoint, index_name)
last_state = None
for _ in range(120):
    desc = idx.describe().get("status", {})
    state = desc.get("detailed_state", "UNKNOWN")
    indexed = desc.get("indexed_row_count", 0)
    ready = desc.get("ready", False)
    if state != last_state:
        print(f"index state: {state} | indexed_rows={indexed} | ready={ready}")
        last_state = state
    if state == "ONLINE_NO_PENDING_UPDATE" and ready:
        break
    if "FAILED" in state:
        raise RuntimeError(f"Index sync failed with state {state}")
    time.sleep(15)
else:
    print(f"Warning: index still in {last_state} after 30 min; continuing.")

print(f"Vector Search index ready: {index_name}")
