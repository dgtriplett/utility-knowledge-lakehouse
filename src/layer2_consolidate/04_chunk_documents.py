# Databricks notebook source
# MAGIC %md
# MAGIC # Layer 2 — Chunk documents for retrieval
# MAGIC
# MAGIC Writes to `document_chunks_docs`. The final, unified `document_chunks`
# MAGIC table (which the Vector Search index sources from) is assembled in
# MAGIC `05_create_vector_index.py` by unioning this table with `debrief_chunks`.
# MAGIC Keeping the union to a single `CREATE OR REPLACE` avoids DELETE + append
# MAGIC patterns that confuse Delta Sync's change data feed tracking.

# COMMAND ----------

dbutils.widgets.text("catalog", "utility_knowledge")
dbutils.widgets.text("curated_schema", "curated")

catalog = dbutils.widgets.get("catalog")
curated = dbutils.widgets.get("curated_schema")

CHUNK_CHARS = 1200
OVERLAP = 200
STEP = CHUNK_CHARS - OVERLAP

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{curated}.document_chunks_docs AS
WITH sizes AS (
  SELECT
    *,
    GREATEST(CAST(ceil(length(page_text) / {STEP}.0) AS INT), 1) AS n_chunks
  FROM {catalog}.{curated}.documents
),
exploded AS (
  SELECT
    *,
    explode(sequence(0, n_chunks - 1)) AS chunk_index
  FROM sizes
)
SELECT
  concat_ws('_', doc_id, CAST(page_number AS STRING), CAST(chunk_index AS STRING)) AS chunk_id,
  doc_id,
  source_path,
  source_kind,
  page_number,
  chunk_index,
  substring(page_text, chunk_index * {STEP} + 1, {CHUNK_CHARS}) AS chunk_text,
  substation_name,
  voltage_class_kv,
  equipment_ids
FROM exploded
WHERE length(substring(page_text, chunk_index * {STEP} + 1, {CHUNK_CHARS})) > 0
""")

count = spark.table(f"{catalog}.{curated}.document_chunks_docs").count()
print(f"Wrote {count} doc chunks to document_chunks_docs.")
