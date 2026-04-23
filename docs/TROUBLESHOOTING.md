# Troubleshooting

## `ai_parse_document` / `ai_extract` fails with "AI functions not enabled"

Serverless compute needs to be enabled for your workspace and you need to be running on a cluster or DBR Serverless runtime that supports AI functions (DBR 16.x+). Fix: use Serverless SQL or a Serverless job cluster. The bundle jobs target Serverless by default.

## Vector Search endpoint stuck in `PROVISIONING`

First-time endpoint creation can take up to 20 minutes. The `create_vector_index` notebook polls for up to that long. If it's still provisioning past that, check the workspace UI under Compute → Vector Search and open a support ticket if it's been more than an hour.

## Agent endpoint returns 503 "Model not ready"

Scale-to-zero endpoints take ~60s to warm up from cold. The app will show the error on the first request after idle. Try again — second request should work. If it never warms, check the serving endpoint event logs.

## "Permission denied" creating catalog

The `catalog` variable in `databricks.yml` defaults to `utility_knowledge`. If you can't create new catalogs, override it:

```bash
databricks bundle deploy --target dev --var "catalog=my_existing_catalog"
```

Make sure you have `CREATE SCHEMA` and `CREATE VOLUME` on that catalog.

## Sample data didn't upload to the Volume

The `upload_samples.py` notebook tries to find the generated `sample_data/` directory relative to where the bundle synced it. If it can't, it falls back to writing three placeholder text files so the rest of the pipeline still runs. For the full experience, regenerate the sample data and re-deploy:

```bash
python -m src.sample_data.generate_data --out ./sample_data
databricks bundle deploy --target dev
databricks bundle run end_to_end --target dev
```

## App shows "AGENT_ENDPOINT env var is required"

The bundle's `resources/app.yml` wires the serving endpoint into the app via a resource reference. If you deployed the agent to a differently-named endpoint, override `app.yml`'s `serving_endpoint.name` to match, and re-deploy.

## Citations come back but the snippets are wrong

The agent returns the top-K chunks from hybrid search, and the model decides which to cite. If you see high-scoring retrievals that the model didn't use, it usually means the prompt needs tuning — the `SYSTEM_PROMPT` in `src/layer4_serve/agent.py` is the right place to iterate.

## Eval run shows contradicting answers between runs

Expected. Claude responses are non-deterministic by default. The `contains_score` metric is a shallow sanity check, not a regression gate. For tighter eval, seed the model or use MLflow's deterministic evaluator with fixed examples.
