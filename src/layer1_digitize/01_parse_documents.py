# Databricks notebook source
# MAGIC %md
# MAGIC # Layer 1 — Parse scanned documents
# MAGIC
# MAGIC Reads PDFs landed in the Volume, parses them with `ai_parse_document`,
# MAGIC and writes page- and element-level structure to a Delta table.
# MAGIC
# MAGIC `ai_parse_document` returns a VARIANT. We store the raw VARIANT in
# MAGIC `parsed_documents` and explode it into typed rows in
# MAGIC `parsed_elements` using VARIANT path syntax.

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
# MAGIC Returns a VARIANT with pages, elements, and bounding boxes.
# MAGIC Runs on serverless compute under your workspace's governance — no
# MAGIC data leaves the account.

# COMMAND ----------

parsed = raw_binary.withColumn("parsed", F.expr("ai_parse_document(content)"))

(
    parsed.select(
        "doc_id",
        "path",
        "source_kind",
        "parsed",
        F.current_timestamp().alias("ingested_at"),
    ).write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{raw}.parsed_documents")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explode to element-level rows
# MAGIC
# MAGIC One element per row, with page and bounding box preserved. `ai_parse_document`
# MAGIC returns VARIANT; we use colon-path syntax + `::` casts to extract typed
# MAGIC fields. The element schema varies slightly across runtime versions — we
# MAGIC `COALESCE` between `content` and `text` so both are supported.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{raw}.parsed_elements AS
SELECT
  doc_id,
  path,
  source_kind,
  element:id::STRING AS element_id,
  element:type::STRING AS element_type,
  COALESCE(element:content::STRING, element:text::STRING) AS element_text,
  COALESCE(element:page_number::INT, element:page::INT, 1) AS page_number,
  element:bbox AS bbox
FROM (
  SELECT
    doc_id,
    path,
    source_kind,
    explode(
      CAST(parsed:document:elements AS ARRAY<VARIANT>)
    ) AS element
  FROM {catalog}.{raw}.parsed_documents
)
WHERE COALESCE(element:content::STRING, element:text::STRING) IS NOT NULL
  AND length(COALESCE(element:content::STRING, element:text::STRING)) > 0
""")

count = spark.table(f"{catalog}.{raw}.parsed_elements").count()
print(f"Wrote {count} element rows.")
