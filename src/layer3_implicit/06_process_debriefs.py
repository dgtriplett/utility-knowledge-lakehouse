# Databricks notebook source
# MAGIC %md
# MAGIC # Layer 3 — Process SME debriefs
# MAGIC
# MAGIC Takes transcripts from `/Volumes/<catalog>/raw/raw_audio/debriefs_as_text/`
# MAGIC and pulls out structured decisions, rationale, equipment mentions, and
# MAGIC cross-references.
# MAGIC
# MAGIC For a real deployment you'd replace the text-load step with a Whisper
# MAGIC transcription call (commented example included below) against audio
# MAGIC files in the `raw_audio` Volume. The sample pipeline ships transcripts
# MAGIC directly so you can demo the structuring step without a Whisper
# MAGIC endpoint set up.

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

transcripts = (
    spark.read.text(f"{audio_volume}/debriefs_as_text/", wholetext=True)
    .select(
        F.input_file_name().alias("source_path"),
        F.col("value").alias("transcript"),
    )
    .withColumn("debrief_id", F.sha2(F.col("source_path"), 256))
)

print(f"Loaded {transcripts.count()} debrief transcripts.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract structure
# MAGIC
# MAGIC We want decisions + rationale pairs, equipment mentions, failure modes,
# MAGIC and cross-references — not a summary. Summaries throw away the thing
# MAGIC that makes tacit knowledge valuable.

# COMMAND ----------

structured = transcripts.withColumn(
    "structured",
    F.expr(
        f"""
        ai_query(
          '{llm_endpoint}',
          concat(
            'You are extracting structured knowledge from a utility SME debrief ',
            'transcript. Return JSON with keys: ',
            '- topics (array of strings) ',
            '- equipment_mentioned (array of strings — substations, breakers, relays, transformers) ',
            '- decisions (array of objects with decision and rationale — the engineer''s ',
            'explanation for *why* something is set or done the way it is) ',
            '- failure_modes (array of strings — what went wrong, or could go wrong) ',
            '- cross_references (array of strings — other procedures, studies, or documents ',
            'the engineer points to) ',
            '- handshake_relationships (array of strings — informal agreements not in any SOP). ',
            'Be faithful to the transcript. Do not invent. ',
            'Transcript: ',
            transcript
          ),
          responseFormat => 'JSON_OBJECT'
        )
        """
    ),
)

flattened = structured.select(
    "debrief_id",
    "source_path",
    "transcript",
    F.get_json_object("structured", "$.topics").alias("topics_json"),
    F.get_json_object("structured", "$.equipment_mentioned").alias("equipment_json"),
    F.get_json_object("structured", "$.decisions").alias("decisions_json"),
    F.get_json_object("structured", "$.failure_modes").alias("failure_modes_json"),
    F.get_json_object("structured", "$.cross_references").alias("cross_references_json"),
    F.get_json_object("structured", "$.handshake_relationships").alias("handshakes_json"),
    F.current_timestamp().alias("processed_at"),
)

(
    flattened.write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{curated}.sme_debriefs")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Also emit debriefs as retrievable chunks
# MAGIC
# MAGIC So the agent can pull in SME context alongside document content using
# MAGIC the same vector index.

# COMMAND ----------

debrief_chunks = (
    flattened.select(
        F.concat(F.lit("debrief_"), F.col("debrief_id")).alias("chunk_id"),
        F.col("debrief_id").alias("doc_id"),
        F.col("source_path"),
        F.lit("debrief").alias("source_kind"),
        F.lit(1).alias("page_number"),
        F.lit(0).alias("chunk_index"),
        F.col("transcript").alias("chunk_text"),
        F.lit(None).cast("string").alias("substation_name"),
        F.lit(None).cast("double").alias("voltage_class_kv"),
        F.lit(None).cast("array<string>").alias("equipment_ids"),
    )
)

# Append into the same chunks table the document pipeline writes to.
(
    debrief_chunks.write.mode("append")
    .saveAsTable(f"{catalog}.{curated}.document_chunks")
)

print(f"Appended {debrief_chunks.count()} debrief chunks to document_chunks.")
