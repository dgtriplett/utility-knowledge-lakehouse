# Setup — from zero to a running assistant

This walkthrough assumes a brand-new Databricks workspace and no prior setup. Target time: about one hour.

## 1. Prerequisites

- **Databricks workspace** on AWS, Azure, or GCP with Unity Catalog enabled and Serverless Compute turned on. (Serverless is required for `ai_parse_document` and `ai_extract` to run efficiently.)
- **Databricks CLI** v0.230 or newer. Install from [docs](https://docs.databricks.com/en/dev-tools/cli/install.html) and authenticate:
  ```bash
  databricks auth login --host https://<your-workspace>.cloud.databricks.com
  ```
- **Python 3.11+** locally, only used for generating the synthetic sample data.
- **Permissions.** You'll need the ability to:
  - Create a catalog (or use an existing one — override `catalog` in `databricks.yml`)
  - Create serving endpoints and Vector Search endpoints
  - Deploy Databricks Apps

If you can't create a catalog, ask your workspace admin to grant you `CREATE SCHEMA`, `CREATE VOLUME`, and `USE CATALOG` on an existing catalog, then set `catalog` to that catalog's name.

## 2. Generate sample data locally

```bash
git clone https://github.com/dgtriplett/utility-knowledge-lakehouse.git
cd utility-knowledge-lakehouse

python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python -m src.sample_data.generate_data --out ./sample_data
```

You should end up with `./sample_data/{onelines,studies,debriefs}/` populated with synthetic PDFs and transcripts, plus `manifest.json`.

## 3. Deploy the bundle

```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev
```

The first deploy syncs the `src/` tree (including `sample_data/`) into your workspace, provisions the Unity Catalog resources, the Vector Search endpoint, and the Databricks App skeleton.

## 4. Run the end-to-end pipeline

```bash
databricks bundle run end_to_end --target dev
```

The job runs nine tasks in sequence/parallel: bootstrap → upload samples → parse → extract → consolidate → chunk + index → process debriefs → deploy agent → evaluate. Expect ~25 minutes on the sample corpus. The longest single step is usually the first Vector Search sync.

Watch it live:

```bash
databricks bundle run end_to_end --target dev --output json | jq .run_page_url
```

## 5. Open the app

Once the job completes successfully:

```bash
databricks apps list | grep utility-knowledge-assistant
```

Open the URL in the `url` field. Click one of the suggested prompts to confirm everything is wired up. Responses should come back in ~5–10 seconds with citation chips underneath.

## 6. (Optional) Run eval

The pipeline includes an `evaluate` task that logs metrics to MLflow and appends a row to `<catalog>.agents.eval_runs`. To re-run just eval without the whole pipeline:

```bash
databricks bundle run end_to_end --target dev --task evaluate
```

## 7. Tear it down

```bash
databricks bundle destroy --target dev
```

This removes jobs, the app, the Vector Search index, and the Model Serving endpoint. Volumes and tables in Unity Catalog survive — drop them manually if you want a clean slate:

```sql
DROP SCHEMA utility_knowledge.curated CASCADE;
DROP SCHEMA utility_knowledge.raw CASCADE;
DROP SCHEMA utility_knowledge.agents CASCADE;
-- DROP CATALOG utility_knowledge;
```
