# Databricks notebook source
# MAGIC %md
# MAGIC # Layer 2 — Consolidate sources
# MAGIC
# MAGIC In a real deployment, Lakeflow Connect pulls from SharePoint, Google
# MAGIC Drive, file shares, email archives, etc. into `raw.*` tables. This
# MAGIC notebook models the consolidation step: dedup by content hash, apply
# MAGIC a canonical schema, and land everything in `curated.documents`.
# MAGIC
# MAGIC For the sample pipeline we only have parsed PDFs. The pattern is the
# MAGIC same regardless of source count.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "utility_knowledge")
dbutils.widgets.text("raw_schema", "raw")
dbutils.widgets.text("curated_schema", "curated")

catalog = dbutils.widgets.get("catalog")
raw = dbutils.widgets.get("raw_schema")
curated = dbutils.widgets.get("curated_schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unify at the page grain
# MAGIC
# MAGIC One row per (document, page), concatenated text, content hash on the text.

# COMMAND ----------

pages = (
    spark.table(f"{catalog}.{raw}.parsed_elements")
    .groupBy("doc_id", "path", "source_kind", "page_number")
    .agg(F.concat_ws("\n", F.collect_list("element_text")).alias("page_text"))
    .withColumn("content_hash", F.sha2(F.col("page_text"), 256))
)

# COMMAND ----------

# Dedup: same page text in two different files is the same content.
# Keep the canonical path (first by lexicographic order) as the provenance.
deduped = (
    pages.withColumn(
        "canonical_path",
        F.min("path").over(F.partitionBy("content_hash").orderBy("path")),
    )
    .where(F.col("path") == F.col("canonical_path"))
    .drop("canonical_path")
)

# COMMAND ----------

# Join in the extracted fields so curated.documents is self-describing.
fields = spark.table(f"{catalog}.{curated}.document_fields").drop("extracted_at")

documents = (
    deduped.alias("p")
    .join(
        fields.alias("f"),
        on=["doc_id", "page_number"],
        how="left",
    )
    .select(
        "p.doc_id",
        F.col("p.path").alias("source_path"),
        "p.source_kind",
        "p.page_number",
        "p.page_text",
        "p.content_hash",
        "f.substation_name",
        "f.voltage_class_kv",
        "f.equipment_ids",
        "f.study_date",
        "f.approving_engineer",
        F.current_timestamp().alias("consolidated_at"),
    )
)

(
    documents.write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{curated}.documents")
)

print(f"Consolidated {documents.count()} page-level rows into curated.documents.")
