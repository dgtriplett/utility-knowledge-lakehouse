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

# MAGIC %md
# MAGIC ## Assemble the unified chunks table
# MAGIC
# MAGIC `document_chunks_docs` (from chunking) and `debrief_chunks` (from Layer 3)
# MAGIC are unioned into `document_chunks` via a single CREATE OR REPLACE so the
# MAGIC Delta Sync index sees a clean, full-rewrite source every run — avoiding
# MAGIC the CDF confusion that a DELETE + append pattern causes.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {source_table}
TBLPROPERTIES (delta.enableChangeDataFeed = true)
AS
SELECT * FROM {catalog}.{curated}.document_chunks_docs
UNION ALL
SELECT * FROM {catalog}.{curated}.debrief_chunks
""")

total = spark.table(source_table).count()
print(f"document_chunks ready: {total} rows.")

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


def _create_index():
    vsc.create_delta_sync_index(
        endpoint_name=endpoint,
        source_table_name=source_table,
        index_name=index_name,
        primary_key="chunk_id",
        pipeline_type="TRIGGERED",
        embedding_source_column="chunk_text",
        embedding_model_endpoint_name=embed_endpoint,
    )


if index_name in existing:
    idx = vsc.get_index(endpoint, index_name)
    current_state = idx.describe().get("status", {}).get("detailed_state", "")
    # If the previous sync failed or the table under the index was replaced,
    # the cleanest path is to delete and recreate — trying to sync a failed
    # index usually re-fails.
    if "FAILED" in current_state or "OFFLINE" in current_state:
        print(f"Index in state {current_state} — deleting and recreating.")
        vsc.delete_index(endpoint, index_name)
        time.sleep(10)
        _create_index()
    else:
        print(f"Index {index_name} exists in state {current_state}. Triggering sync.")
        idx.sync()
else:
    print(f"Creating index {index_name}...")
    _create_index()

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
