# Databricks notebook source
# MAGIC %md
# MAGIC # Layer 2 — Chunk documents for retrieval
# MAGIC
# MAGIC Pages are a good retrieval grain for one-lines and debriefs; protection
# MAGIC studies benefit from tighter chunks. This notebook does both: a short
# MAGIC sliding-window chunk with overlap, while preserving the page anchor for
# MAGIC citations.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, IntegerType, StructField, StructType

dbutils.widgets.text("catalog", "utility_knowledge")
dbutils.widgets.text("curated_schema", "curated")

catalog = dbutils.widgets.get("catalog")
curated = dbutils.widgets.get("curated_schema")

CHUNK_CHARS = 1200
OVERLAP = 200

# COMMAND ----------

chunk_schema = ArrayType(
    StructType(
        [
            StructField("chunk_index", IntegerType()),
            StructField("chunk_text", StringType()),
        ]
    )
)


def chunk(text: str):
    if not text:
        return []
    out = []
    i = 0
    idx = 0
    while i < len(text):
        out.append((idx, text[i : i + CHUNK_CHARS]))
        i += CHUNK_CHARS - OVERLAP
        idx += 1
    return out


chunk_udf = F.udf(chunk, chunk_schema)

# COMMAND ----------

chunked = (
    spark.table(f"{catalog}.{curated}.documents")
    .withColumn("chunks", chunk_udf("page_text"))
    .withColumn("chunk", F.explode("chunks"))
    .select(
        F.concat_ws(
            "_",
            F.col("doc_id"),
            F.col("page_number").cast("string"),
            F.col("chunk.chunk_index").cast("string"),
        ).alias("chunk_id"),
        "doc_id",
        "source_path",
        "source_kind",
        "page_number",
        F.col("chunk.chunk_index").alias("chunk_index"),
        F.col("chunk.chunk_text").alias("chunk_text"),
        "substation_name",
        "voltage_class_kv",
        "equipment_ids",
    )
)

(
    chunked.write.mode("overwrite").option("overwriteSchema", "true")
    # Change Data Feed is required for Vector Search delta-sync.
    .option("delta.enableChangeDataFeed", "true")
    .saveAsTable(f"{catalog}.{curated}.document_chunks")
)

# Turning on CDF after-the-fact is also supported — the option above handles
# first-write and re-writes both.
spark.sql(
    f"ALTER TABLE {catalog}.{curated}.document_chunks "
    "SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
)

print(f"Wrote {chunked.count()} chunks.")
