# Databricks notebook source
# MAGIC %md
# MAGIC # Layer 2 — Chunk documents for retrieval
# MAGIC
# MAGIC A sliding-window chunk with overlap, page anchor preserved for citations.
# MAGIC Implemented in pure SQL so it runs anywhere, including serverless
# MAGIC clusters that restrict Python UDFs.

# COMMAND ----------

dbutils.widgets.text("catalog", "utility_knowledge")
dbutils.widgets.text("curated_schema", "curated")

catalog = dbutils.widgets.get("catalog")
curated = dbutils.widgets.get("curated_schema")

CHUNK_CHARS = 1200
OVERLAP = 200
STEP = CHUNK_CHARS - OVERLAP  # distance between successive chunk starts

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{curated}.document_chunks (
  chunk_id STRING NOT NULL,
  doc_id STRING,
  source_path STRING,
  source_kind STRING,
  page_number INT,
  chunk_index INT,
  chunk_text STRING,
  substation_name STRING,
  voltage_class_kv DOUBLE,
  equipment_ids STRING
)
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

spark.sql(f"""
INSERT OVERWRITE {catalog}.{curated}.document_chunks
WITH sizes AS (
  SELECT
    *,
    GREATEST(CAST(ceil(length(page_text) / {STEP}.0) AS INT), 1) AS n_chunks
  FROM {catalog}.{curated}.documents
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
FROM (
  SELECT
    *,
    explode(sequence(0, n_chunks - 1)) AS chunk_index
  FROM sizes
)
WHERE length(substring(page_text, chunk_index * {STEP} + 1, {CHUNK_CHARS})) > 0
""")

count = spark.table(f"{catalog}.{curated}.document_chunks").count()
print(f"Wrote {count} chunks.")
