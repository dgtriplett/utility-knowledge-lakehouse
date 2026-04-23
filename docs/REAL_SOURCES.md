# Swapping synthetic data for real sources

The synthetic corpus is just a warm-up. Here's how to point the pipeline at real sources.

## SharePoint via Lakeflow Connect

Add to `resources/`:

```yaml
# resources/sources.yml
resources:
  pipelines:
    sharepoint_knowledge:
      name: sharepoint-knowledge-ingest
      catalog: ${var.catalog}
      target: ${var.raw_schema}
      configuration:
        source.type: "sharepoint"
        source.site_url: "https://<tenant>.sharepoint.com/sites/engineering"
        source.document_library: "Engineering Standards"
        destination.table: "sharepoint_documents"
      # Credentials via service principal — see docs.
      # Refresh every 6 hours.
      continuous: false
      trigger:
        cron:
          expression: "0 0 */6 * * ?"
          timezone_id: "UTC"
```

The pipeline lands each file as a row in `raw.sharepoint_documents` with `content` as a binary column. Point Layer 1's `01_parse_documents.py` at that table instead of the Volume:

```python
raw_binary = (
    spark.table(f"{catalog}.{raw}.sharepoint_documents")
    .withColumn("doc_id", F.sha2(F.col("path"), 256))
    ...
)
```

## Google Drive

Same pattern, different source type:

```yaml
configuration:
  source.type: "google_drive"
  source.folder_id: "0AAAAAAAAAAAAAAaaaaaa"
  destination.table: "gdrive_documents"
```

## File share or S3 bucket

If you already have content in a bucket or mounted share, skip Lakeflow Connect and read binaryFile directly:

```python
raw_binary = (
    spark.read.format("binaryFile")
    .option("recursiveFileLookup", "true")
    .load("s3://utility-knowledge-raw/engineering-standards/")
)
```

Grant the cluster's service principal `READ FILES` on the external location in Unity Catalog. No credentials in notebooks.

## Real audio for debriefs

Replace the text-load step in `src/layer3_implicit/06_process_debriefs.py`:

```python
audio = (
    spark.read.format("binaryFile")
    .load(f"{audio_volume}/debriefs/")
)
transcribed = audio.withColumn(
    "transcript",
    F.expr("ai_query('<your-whisper-endpoint>', struct(content as audio))")
)
```

You need a Whisper (or equivalent) endpoint on Model Serving. The Databricks Marketplace has pre-built ones; a custom deployment of `openai/whisper-large-v3` is ~15 minutes of work.
