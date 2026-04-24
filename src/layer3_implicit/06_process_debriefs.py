# Databricks notebook source
# MAGIC %md
# MAGIC # Layer 3 — Process SME debriefs
# MAGIC
# MAGIC Takes transcripts from `/Volumes/<catalog>/raw/raw_audio/debriefs_as_text/`
# MAGIC and pulls out structured decisions, rationale, equipment mentions, and
# MAGIC cross-references.
# MAGIC
# MAGIC In a real deployment you'd replace the text-load step with a Whisper
# MAGIC transcription call (commented example below) against audio files in the
# MAGIC `raw_audio` Volume. The sample pipeline ships transcripts directly so
# MAGIC you can demo the structuring step without a Whisper endpoint.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "utility_knowledge")
dbutils.widgets.text("raw_schema", "raw")
dbutils.widgets.text("curated_schema", "curated")
dbutils.widgets.text("llm_endpoint", "databricks-claude-sonnet-4-6")

catalog = dbutils.widgets.get("catalog")
raw = dbutils.widgets.get("raw_schema")
curated = dbutils.widgets.get("curated_schema")
llm_endpoint = dbutils.widgets.get("llm_endpoint")

audio_volume = f"/Volumes/{catalog}/{raw}/raw_audio"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load transcripts
# MAGIC
# MAGIC In production this would be:
# MAGIC
# MAGIC ```python
# MAGIC audio = spark.read.format("binaryFile").load(f"{audio_volume}/debriefs/")
# MAGIC transcribed = audio.withColumn(
# MAGIC     "transcript",
# MAGIC     F.expr("ai_query('whisper-endpoint', struct(content as audio))")
# MAGIC )
# MAGIC ```

# COMMAND ----------

# Use binaryFile + decode to guarantee one row per file. The text reader's
# `wholetext` option isn't reliably honored on all runtimes.
transcripts = (
    spark.read.format("binaryFile")
    .load(f"{audio_volume}/debriefs_as_text/")
    .select(
        F.col("path").alias("source_path"),
        F.decode(F.col("content"), "UTF-8").alias("transcript"),
    )
    .withColumn("debrief_id", F.sha2(F.col("source_path"), 256))
)

print(f"Loaded {transcripts.count()} debrief transcripts.")
transcripts.createOrReplaceTempView("_debrief_transcripts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract structure with typed output
# MAGIC
# MAGIC We want decisions + rationale pairs, equipment mentions, failure modes,
# MAGIC and cross-references — not a summary. Using `ai_query` with a `returnType`
# MAGIC gives us typed output directly, no JSON parsing needed.

# COMMAND ----------

RETURN_TYPE = (
    "STRUCT<"
    "topics ARRAY<STRING>, "
    "equipment_mentioned ARRAY<STRING>, "
    "decisions ARRAY<STRUCT<decision STRING, rationale STRING>>, "
    "failure_modes ARRAY<STRING>, "
    "cross_references ARRAY<STRING>, "
    "handshake_relationships ARRAY<STRING>"
    ">"
)

PROMPT = (
    "You are extracting structured knowledge from a utility SME debrief "
    "transcript. Fill in every field faithfully from the transcript. Do not "
    "invent. Use empty arrays if a category does not appear. "
    "Focus on decisions and their rationale — the engineer's explanation for "
    "*why* something is set or done the way it is — not summaries. "
    "Transcript: "
)

# Escape single quotes for SQL literal embedding.
prompt_sql = PROMPT.replace("'", "''")

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{curated}.sme_debriefs AS
SELECT
  debrief_id,
  source_path,
  transcript,
  ai_query(
    '{llm_endpoint}',
    concat('{prompt_sql}', transcript),
    returnType => '{RETURN_TYPE}'
  ) AS structured,
  current_timestamp() AS processed_at
FROM _debrief_transcripts
""")

count = spark.table(f"{catalog}.{curated}.sme_debriefs").count()
print(f"Processed {count} debriefs into curated.sme_debriefs.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Emit debriefs as retrievable chunks
# MAGIC
# MAGIC Append into the same `document_chunks` table so the agent's retriever
# MAGIC surfaces document content and SME context in one call.

# COMMAND ----------

# Write debrief rows to their own table. `05_create_vector_index.py` unions
# this with `document_chunks_docs` into the final `document_chunks` table
# via a single `CREATE OR REPLACE`, which keeps the Delta Sync index happy.

debrief_chunks = (
    spark.table(f"{catalog}.{curated}.sme_debriefs")
    .select(
        F.concat(F.lit("debrief_"), F.col("debrief_id")).alias("chunk_id"),
        F.col("debrief_id").alias("doc_id"),
        F.col("source_path"),
        F.lit("debrief").alias("source_kind"),
        F.lit(1).alias("page_number"),
        F.lit(0).alias("chunk_index"),
        F.col("transcript").alias("chunk_text"),
        F.lit(None).cast("string").alias("substation_name"),
        F.lit(None).cast("double").alias("voltage_class_kv"),
        F.lit(None).cast("string").alias("equipment_ids"),
    )
)

(
    debrief_chunks.write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{curated}.debrief_chunks")
)

print(f"Wrote {debrief_chunks.count()} debrief chunks to debrief_chunks.")
