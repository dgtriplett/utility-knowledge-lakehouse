# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy the Databricks App
# MAGIC
# MAGIC Runs after `deploy_agent` so the serving endpoint exists before the app
# MAGIC tries to bind to it. Creates the app if it doesn't already exist, deploys
# MAGIC the source from the bundle-synced workspace path, and starts it.

# COMMAND ----------

# MAGIC %pip install -q databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import App, AppResource, AppResourceServingEndpoint, AppResourceServingEndpointServingEndpointPermission

dbutils.widgets.text("app_name", "utility-knowledge-assistant")
dbutils.widgets.text("agent_endpoint", "agents_utility_knowledge-agents-utility_assistant")
dbutils.widgets.text(
    "source_code_path",
    "",
    label="Workspace path containing the app source (main.py, app.yaml, etc.)",
)

app_name = dbutils.widgets.get("app_name")
agent_endpoint = dbutils.widgets.get("agent_endpoint")
source_code_path = dbutils.widgets.get("source_code_path")

if not source_code_path:
    # The bundle syncs this notebook and app.yaml/main.py into the same dir.
    nb_path = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )
    source_code_path = "/Workspace" + os.path.dirname(nb_path)

print(f"App name:         {app_name}")
print(f"Agent endpoint:   {agent_endpoint}")
print(f"Source path:      {source_code_path}")

# COMMAND ----------

w = WorkspaceClient()

# Ensure the endpoint exists before going further. If deploy_agent hasn't run
# yet, fail fast instead of creating a broken app.
try:
    ep = w.serving_endpoints.get(agent_endpoint)
    print(f"Endpoint ready: {ep.name} (state={ep.state.ready})")
except Exception as exc:
    raise RuntimeError(
        f"Expected serving endpoint '{agent_endpoint}' to exist. "
        "Run the deploy_agent task first."
    ) from exc

# COMMAND ----------

# Create the app if it doesn't already exist. Resource binding is declared
# here so the app gets CAN_QUERY permission on the agent endpoint.
resources = [
    AppResource(
        name="agent_endpoint",
        description="Serving endpoint hosting the utility knowledge agent.",
        serving_endpoint=AppResourceServingEndpoint(
            name=agent_endpoint,
            permission=AppResourceServingEndpointServingEndpointPermission.CAN_QUERY,
        ),
    )
]

try:
    existing = w.apps.get(app_name)
    print(f"App {app_name} already exists — updating.")
    w.apps.update(name=app_name, app=App(name=app_name, resources=resources))
except Exception:
    print(f"Creating app {app_name}...")
    create_op = w.apps.create_and_wait(app=App(name=app_name, resources=resources))
    print(f"App created: {create_op.name}")

# COMMAND ----------

print(f"Deploying source from {source_code_path}...")
deployment = w.apps.deploy_and_wait(
    app_name=app_name,
    app_deployment={"source_code_path": source_code_path},
)
print(f"Deployment status: {deployment.status.state}")
print(f"Deployment message: {deployment.status.message}")

# COMMAND ----------

# Start the app compute if it isn't already running.
app = w.apps.get(app_name)
if app.compute_status and app.compute_status.state != "ACTIVE":
    print("Starting app compute...")
    w.apps.start_and_wait(name=app_name)
    app = w.apps.get(app_name)

print(f"App URL: {app.url}")
print(f"Compute state: {app.compute_status.state if app.compute_status else 'unknown'}")
