# Databricks notebook source
# MAGIC %md
# MAGIC # Bootstrap — Catalog, schemas, volumes
# MAGIC
# MAGIC Idempotent. Safe to re-run. Declares the Unity Catalog objects the rest of
# MAGIC the pipeline writes into.
# MAGIC
# MAGIC If the target catalog doesn't exist, this notebook tries to create it.
# MAGIC In workspaces with Default Storage enabled (including FEVM sandboxes),
# MAGIC catalog creation via SQL may fail — in that case, pass `catalog=<an
# MAGIC existing catalog>` as a job parameter and the notebook will use it.

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

# Does the catalog already exist? If yes, skip creation.
existing = [
    r["catalog"]
    for r in spark.sql("SHOW CATALOGS").collect()
]
if catalog in existing:
    print(f"Catalog `{catalog}` already exists — skipping creation.")
else:
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
        print(f"Created catalog `{catalog}`.")
    except Exception as exc:
        raise RuntimeError(
            f"Could not create catalog `{catalog}`. This is common in "
            "workspaces with Default Storage but no metastore storage root. "
            "Re-run with `catalog=<existing catalog>` as a parameter, or "
            "create the catalog in the UI first."
        ) from exc

# COMMAND ----------

for schema in (raw, curated, agents):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    print(f"Schema ready: {catalog}.{schema}")

for vol in ("raw_documents", "raw_audio", "checkpoints"):
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{raw}.{vol}")
    print(f"Volume ready: {catalog}.{raw}.{vol}")

print(f"Bootstrap complete for catalog `{catalog}`.")
