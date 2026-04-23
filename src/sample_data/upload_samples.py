# Databricks notebook source
# MAGIC %md
# MAGIC # Upload synthetic samples to Volumes
# MAGIC
# MAGIC Expects `sample_data/` (produced by `python -m src.sample_data.generate_data`)
# MAGIC to have been uploaded to the workspace files alongside this bundle.
# MAGIC
# MAGIC In `bundle deploy` the `src/sample_data/` directory is synced with the bundle,
# MAGIC so the generated PDFs live next to this notebook as siblings. We copy them
# MAGIC into the Volumes created in 00_bootstrap.
# MAGIC
# MAGIC If you ran `generate_data` locally and want to upload a larger corpus, use
# MAGIC `databricks fs cp` instead.

# COMMAND ----------

import os
import shutil
from pathlib import Path

dbutils.widgets.text("catalog", "utility_knowledge")
dbutils.widgets.text("raw_schema", "raw")

catalog = dbutils.widgets.get("catalog")
raw = dbutils.widgets.get("raw_schema")

docs_volume = f"/Volumes/{catalog}/{raw}/raw_documents"
audio_volume = f"/Volumes/{catalog}/{raw}/raw_audio"

# COMMAND ----------

# When the bundle syncs this file into /Workspace/..., the generated sample_data/
# directory comes along with it as a sibling. Resolve relative to this notebook.
here = Path(os.getcwd())
candidates = [
    here.parent / "sample_data",          # running as a notebook from src/sample_data/
    here.parent.parent / "sample_data",   # running from bundled workspace root
    Path("/Workspace/Repos") / "sample_data",
]
root = next((c for c in candidates if c.exists()), None)

if root is None:
    print(
        "No local ./sample_data/ found. Falling back to an in-notebook synthetic "
        "corpus so this step doesn't block the rest of the pipeline."
    )
    # Minimal fallback — write a couple of text docs into the Volume so downstream
    # steps have something to process. Real corpus is produced locally via
    # `python -m src.sample_data.generate_data --out ./sample_data` then uploaded.
    os.makedirs(docs_volume, exist_ok=True)
    for i in range(3):
        with open(f"{docs_volume}/fallback_doc_{i}.txt", "w") as fh:
            fh.write(
                f"Fallback synthetic document {i}. Equipment 138L-{i + 1} at Oak Ridge substation.\n"
            )
else:
    print(f"Found sample data at {root}")
    for kind in ("onelines", "studies"):
        src = root / kind
        if not src.exists():
            continue
        dst_root = Path(docs_volume) / kind
        dst_root.mkdir(parents=True, exist_ok=True)
        for f in src.iterdir():
            shutil.copy(f, dst_root / f.name)
    debriefs = root / "debriefs"
    if debriefs.exists():
        dst_root = Path(audio_volume) / "debriefs_as_text"
        dst_root.mkdir(parents=True, exist_ok=True)
        for f in debriefs.iterdir():
            shutil.copy(f, dst_root / f.name)
    print("Uploaded samples to Volumes.")

print(f"Documents volume: {docs_volume}")
print(f"Audio volume:     {audio_volume}")
