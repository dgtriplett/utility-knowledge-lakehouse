# Databricks notebook source
# MAGIC %md
# MAGIC # Layer 1 — Extract typed fields
# MAGIC
# MAGIC Takes the parsed elements and pulls out typed fields — equipment IDs,
# MAGIC voltage classes, engineer names, dates — using `ai_extract`.
# MAGIC
# MAGIC Writes two tables:
# MAGIC   - `document_fields` — typed, curated (passes confidence gate)
# MAGIC   - `extraction_needs_review` — typed, failed the gate, awaits SME review

# COMMAND ----------

dbutils.widgets.text("catalog", "utility_knowledge")
dbutils.widgets.text("raw_schema", "raw")
dbutils.widgets.text("curated_schema", "curated")
dbutils.widgets.text("llm_endpoint", "databricks-claude-haiku-4-5")

catalog = dbutils.widgets.get("catalog")
raw = dbutils.widgets.get("raw_schema")
curated = dbutils.widgets.get("curated_schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Page-level text, then `ai_extract`
# MAGIC
# MAGIC `ai_extract` returns VARIANT. We cast each field out with its expected type
# MAGIC and materialize a typed Delta table in a single SQL statement.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{curated}.document_fields_typed AS
WITH pages AS (
  SELECT
    doc_id,
    path,
    source_kind,
    page_number,
    concat_ws('\\n', collect_list(element_text)) AS page_text
  FROM {catalog}.{raw}.parsed_elements
  GROUP BY doc_id, path, source_kind, page_number
),
extracted AS (
  SELECT
    doc_id,
    path,
    source_kind,
    page_number,
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
    ) AS fields
  FROM pages
)
SELECT
  doc_id,
  path,
  source_kind,
  page_number,
  fields.substation_name                              AS substation_name,
  TRY_CAST(fields.voltage_class_kv AS DOUBLE)         AS voltage_class_kv,
  fields.equipment_ids                                AS equipment_ids,
  TRY_CAST(fields.study_date AS DATE)                 AS study_date,
  fields.approving_engineer                           AS approving_engineer,
  fields.bus_arrangement                              AS bus_arrangement,
  fields.scada_rtu_model                              AS scada_rtu_model,
  TRY_CAST(fields.last_thermal_inspection_year AS INT) AS last_thermal_inspection_year,
  current_timestamp()                                 AS extracted_at,
  'ai_extract'                                        AS extraction_model_version
FROM extracted
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Confidence gate → curated vs. needs_review
# MAGIC
# MAGIC Trivial heuristic — protection study pages need a date, every page needs
# MAGIC a substation name. Real deployments back this with an LLM-as-judge call.

# COMMAND ----------

FAIL_CONDITION = """
  (source_kind = 'protection_study' AND study_date IS NULL)
  OR substation_name IS NULL
"""

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{curated}.extraction_needs_review AS
SELECT * FROM {catalog}.{curated}.document_fields_typed
WHERE {FAIL_CONDITION}
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{curated}.document_fields AS
SELECT * FROM {catalog}.{curated}.document_fields_typed
WHERE NOT ({FAIL_CONDITION})
""")

spark.sql(f"DROP TABLE IF EXISTS {catalog}.{curated}.document_fields_typed")

curated_count = spark.table(f"{catalog}.{curated}.document_fields").count()
review_count = spark.table(f"{catalog}.{curated}.extraction_needs_review").count()
print(f"Curated: {curated_count} rows | Needs review: {review_count} rows")
