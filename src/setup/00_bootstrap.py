# Databricks notebook source
# MAGIC %md
# MAGIC # Bootstrap — Catalog, schemas, volumes
# MAGIC
# MAGIC Idempotent. Safe to re-run. Declares the Unity Catalog objects the rest of
# MAGIC the pipeline writes into.
# MAGIC
# MAGIC The Asset Bundle also declares these in `resources/schemas.yml` — this
# MAGIC notebook exists so the pipeline works when run manually without the bundle.

# COMMAND ----------

dbutils.widgets.text("catalog", "utility_knowledge")
dbutils.widgets.text("raw_schema", "raw")
dbutils.widgets.text("curated_schema", "curated")
dbutils.widgets.text("agents_schema", "agents")

catalog = dbutils.widgets.get("catalog")
raw = dbutils.widgets.get("raw_schema")
curated = dbutils.widgets.get("curated_schema")
agents = dbutils.widgets.get("agents_schema")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
for schema in (raw, curated, agents):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{raw}.raw_documents")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{raw}.raw_audio")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{raw}.checkpoints")

print(f"Bootstrap complete for catalog `{catalog}`.")
