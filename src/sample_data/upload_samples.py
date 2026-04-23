# Databricks notebook source
# MAGIC %md
# MAGIC # Generate + upload synthetic samples (in-workspace)
# MAGIC
# MAGIC Generates the synthetic corpus directly into Unity Catalog Volumes so
# MAGIC users don't need any local Python setup. `reportlab` is installed here;
# MAGIC the generator logic itself is imported from `generate_data.py`.

# COMMAND ----------

# MAGIC %pip install -q reportlab==4.2.2
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
import shutil
import sys
from pathlib import Path

dbutils.widgets.text("catalog", "utility_knowledge")
dbutils.widgets.text("raw_schema", "raw")

catalog = dbutils.widgets.get("catalog")
raw = dbutils.widgets.get("raw_schema")

docs_volume = f"/Volumes/{catalog}/{raw}/raw_documents"
audio_volume = f"/Volumes/{catalog}/{raw}/raw_audio"

# COMMAND ----------

# Import the generator functions from the sibling module.
# The bundle syncs `src/sample_data/` as a directory, so generate_data.py lives
# next to this notebook.
nb_dir = os.path.dirname(
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)
nb_dir_fs = f"/Workspace/{nb_dir.lstrip('/')}"
if nb_dir_fs not in sys.path:
    sys.path.insert(0, nb_dir_fs)

from generate_data import generate

# COMMAND ----------

# Generate into a tmp path first, then shutil.copy into the Volume. Writing
# reportlab PDFs directly to the Volume path works too, but tmp-then-copy keeps
# the generator's path handling unchanged and avoids any weird FUSE quirks.
tmp_out = Path("/tmp/utility_sample_data")
if tmp_out.exists():
    shutil.rmtree(tmp_out)

summary = generate(tmp_out, n_substations=15, n_debriefs=20)
print(summary)

# COMMAND ----------

def copy_tree(src: Path, dst: str) -> int:
    count = 0
    os.makedirs(dst, exist_ok=True)
    for item in src.iterdir():
        shutil.copy(item, Path(dst) / item.name)
        count += 1
    return count


onelines = copy_tree(tmp_out / "onelines", f"{docs_volume}/onelines")
studies = copy_tree(tmp_out / "studies", f"{docs_volume}/studies")
debriefs = copy_tree(tmp_out / "debriefs", f"{audio_volume}/debriefs_as_text")

print(f"Uploaded {onelines} one-lines, {studies} studies, {debriefs} debriefs.")
print(f"Documents volume: {docs_volume}")
print(f"Audio volume:     {audio_volume}")
