# Databricks notebook source
# MAGIC %md
# MAGIC # Layer 4 — Register and deploy the agent
# MAGIC
# MAGIC Logs `agent.py` as an MLflow pyfunc, registers it to Unity Catalog, and
# MAGIC deploys it to a Model Serving endpoint via `databricks.agents.deploy()`.

# COMMAND ----------

# MAGIC %pip install -q "mlflow[databricks]>=3.0.0" databricks-agents databricks-vectorsearch databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os

import mlflow
from databricks import agents
from mlflow.models.resources import (
    DatabricksServingEndpoint,
    DatabricksVectorSearchIndex,
)
from mlflow.tracking import MlflowClient

dbutils.widgets.text("catalog", "utility_knowledge")
dbutils.widgets.text("curated_schema", "curated")
dbutils.widgets.text("agents_schema", "agents")
dbutils.widgets.text("llm_endpoint", "databricks-claude-sonnet-4-6")
dbutils.widgets.text("vs_endpoint_name", "utility-knowledge-vs")
dbutils.widgets.text(
    "agent_model_name", "utility_knowledge.agents.utility_assistant"
)

catalog = dbutils.widgets.get("catalog")
curated = dbutils.widgets.get("curated_schema")
agents_schema = dbutils.widgets.get("agents_schema")
llm_endpoint = dbutils.widgets.get("llm_endpoint")
vs_endpoint = dbutils.widgets.get("vs_endpoint_name")
model_name = dbutils.widgets.get("agent_model_name")

index_name = f"{catalog}.{curated}.document_chunks_idx"

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

# agent.py is a sibling of this notebook after the bundle syncs it.
nb_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)
agent_file = os.path.join("/Workspace", os.path.dirname(nb_path).lstrip("/"), "agent.py")
assert os.path.exists(agent_file), (
    f"Expected agent.py next to deploy_agent.py but {agent_file} does not exist"
)
print(f"Logging agent from: {agent_file}")

# COMMAND ----------

with mlflow.start_run(run_name="utility_assistant"):
    logged = mlflow.pyfunc.log_model(
        python_model=agent_file,
        artifact_path="agent",
        registered_model_name=model_name,
        pip_requirements=[
            "mlflow[databricks]>=3.0.0",
            "databricks-sdk>=0.39.0",
            "databricks-vectorsearch>=0.40",
        ],
        model_config={
            "LLM_ENDPOINT": llm_endpoint,
            "VS_ENDPOINT_NAME": vs_endpoint,
            "VS_INDEX_NAME": index_name,
        },
        resources=[
            DatabricksServingEndpoint(endpoint_name=llm_endpoint),
            DatabricksVectorSearchIndex(index_name=index_name),
        ],
    )
    print(f"Logged model: {logged.model_uri}")

# COMMAND ----------

client = MlflowClient(registry_uri="databricks-uc")
versions = client.search_model_versions(f"name='{model_name}'")
latest = max(versions, key=lambda v: int(v.version))
print(f"Deploying version {latest.version} of {model_name}...")

deployment = agents.deploy(
    model_name=model_name,
    model_version=int(latest.version),
    scale_to_zero=True,
)

print(f"Endpoint: {deployment.endpoint_name}")
print(f"Review app: {deployment.review_app_url}")
