# Databricks notebook source
# MAGIC %md
# MAGIC # Layer 1 — Parse scanned documents
# MAGIC
# MAGIC Reads PDFs landed in the Volume, parses them with `ai_parse_document`,
# MAGIC and writes page- and element-level structure to a Delta table.
# MAGIC
# MAGIC Every downstream row carries `doc_id`, page number, and bounding box so
# MAGIC citations survive all the way to the agent response.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "utility_knowledge")
dbutils.widgets.text("raw_schema", "raw")

catalog = dbutils.widgets.get("catalog")
raw = dbutils.widgets.get("raw_schema")
docs_volume = f"/Volumes/{catalog}/{raw}/raw_documents"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load binary files from the Volume
# MAGIC
# MAGIC We load everything under `/onelines/` and `/studies/` as binary and
# MAGIC hash the path into a stable `doc_id`.

# COMMAND ----------

raw_binary = (
    spark.read.format("binaryFile")
    .option("recursiveFileLookup", "true")
    .option("pathGlobFilter", "*.pdf")
    .load(docs_volume)
    .withColumn("doc_id", F.sha2(F.col("path"), 256))
    .withColumn(
        "source_kind",
        F.when(F.col("path").contains("/onelines/"), F.lit("oneline"))
         .when(F.col("path").contains("/studies/"), F.lit("protection_study"))
         .otherwise(F.lit("unknown")),
    )
)
print(f"Loaded {raw_binary.count()} PDF files.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse with `ai_parse_document`
# MAGIC
# MAGIC Returns structured JSON with pages, elements, bounding boxes, and
# MAGIC parse metadata. Runs on serverless compute under your workspace's
# MAGIC governance — no data leaves the account.

# COMMAND ----------

parsed = raw_binary.withColumn("parsed", F.expr("ai_parse_document(content)"))

elements = (
    parsed
    .select(
        "doc_id",
        "path",
        "source_kind",
        F.col("parsed.document.pages").alias("pages"),
        F.col("parsed.document.elements").alias("elements"),
        F.col("parsed.metadata").alias("parse_meta"),
        F.current_timestamp().alias("ingested_at"),
    )
)

(
    elements.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{raw}.parsed_documents")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explode to element-level rows
# MAGIC
# MAGIC Flatten pages/elements so downstream chunking, extraction, and retrieval
# MAGIC all operate on the same primary grain: one element per row, with page
# MAGIC and bounding box preserved.

# COMMAND ----------

element_rows = (
    spark.table(f"{catalog}.{raw}.parsed_documents")
    .select(
        "doc_id",
        "path",
        "source_kind",
        F.explode("elements").alias("el"),
    )
    .select(
        "doc_id",
        "path",
        "source_kind",
        F.col("el.id").alias("element_id"),
        F.col("el.type").alias("element_type"),
        F.col("el.text").alias("element_text"),
        F.col("el.page_number").alias("page_number"),
        F.col("el.bbox").alias("bbox"),
    )
    .where(F.col("element_text").isNotNull() & (F.length("element_text") > 0))
)

(
    element_rows.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{raw}.parsed_elements")
)

print(f"Wrote {element_rows.count()} element rows.")
