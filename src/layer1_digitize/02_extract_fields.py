# Databricks notebook source
# MAGIC %md
# MAGIC # Layer 1 — Extract typed fields
# MAGIC
# MAGIC Takes the parsed elements and pulls out typed fields — equipment IDs,
# MAGIC voltage classes, engineer names, dates — using `ai_extract` with a
# MAGIC declared schema.
# MAGIC
# MAGIC Low-confidence rows are routed to a `needs_review` table. Nothing flows
# MAGIC to curated until an SME has seen it (or an automated confidence rule
# MAGIC has approved it).

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "utility_knowledge")
dbutils.widgets.text("raw_schema", "raw")
dbutils.widgets.text("curated_schema", "curated")
dbutils.widgets.text("llm_endpoint", "databricks-claude-haiku-4-5")

catalog = dbutils.widgets.get("catalog")
raw = dbutils.widgets.get("raw_schema")
curated = dbutils.widgets.get("curated_schema")
llm_endpoint = dbutils.widgets.get("llm_endpoint")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Page-level text for extraction
# MAGIC
# MAGIC We extract from concatenated element text per page — most of the fields
# MAGIC we care about appear on a single page, and the page anchor keeps
# MAGIC citations tight.

# COMMAND ----------

pages = (
    spark.table(f"{catalog}.{raw}.parsed_elements")
    .groupBy("doc_id", "path", "source_kind", "page_number")
    .agg(F.concat_ws("\n", F.collect_list("element_text")).alias("page_text"))
)

# COMMAND ----------

extracted = pages.withColumn(
    "fields",
    F.expr(
        f"""
        ai_extract(
            page_text,
            array(
                'substation_name',
                'voltage_class_kv',
                'equipment_ids',
                'study_date',
                'approving_engineer',
                'bus_arrangement',
                'scada_rtu_model',
                'last_thermal_inspection_year'
            )
        )
        """
    ),
)

extracted_flat = extracted.select(
    "doc_id",
    "path",
    "source_kind",
    "page_number",
    F.col("fields.substation_name").alias("substation_name"),
    F.col("fields.voltage_class_kv").cast("double").alias("voltage_class_kv"),
    F.col("fields.equipment_ids").alias("equipment_ids"),
    F.to_date(F.col("fields.study_date")).alias("study_date"),
    F.col("fields.approving_engineer").alias("approving_engineer"),
    F.col("fields.bus_arrangement").alias("bus_arrangement"),
    F.col("fields.scada_rtu_model").alias("scada_rtu_model"),
    F.col("fields.last_thermal_inspection_year").cast("int").alias("last_thermal_inspection_year"),
    F.current_timestamp().alias("extracted_at"),
    F.lit("ai_extract").alias("extraction_model_version"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Confidence gate
# MAGIC
# MAGIC A trivial heuristic: if the field is null on a page that clearly should
# MAGIC contain it, send the row to `needs_review` instead of curated. Real
# MAGIC deployments should back this with an LLM-as-judge call or a classifier.

# COMMAND ----------

needs_review = extracted_flat.where(
    (F.col("source_kind") == "protection_study") & F.col("study_date").isNull()
    | (F.col("substation_name").isNull())
)

curated_rows = extracted_flat.exceptAll(needs_review)

(
    needs_review.write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{curated}.extraction_needs_review")
)
(
    curated_rows.write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{curated}.document_fields")
)

print(
    f"Curated: {curated_rows.count()} rows | "
    f"Needs review: {needs_review.count()} rows"
)
