# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy the Databricks App
# MAGIC
# MAGIC Runs after `deploy_agent` so the serving endpoint exists before the app
# MAGIC tries to bind to it. Creates the app if needed, deploys the source from
# MAGIC the bundle-synced workspace path, and starts the app compute.

# COMMAND ----------

# MAGIC %pip install -q "databricks-sdk>=0.39.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import (
    App,
    AppDeployment,
    AppResource,
    AppResourceServingEndpoint,
    AppResourceServingEndpointServingEndpointPermission,
)

dbutils.widgets.text("app_name", "utility-knowledge-assistant")
dbutils.widgets.text("agent_endpoint", "agents_utility_knowledge-agents-utility_assistant")
dbutils.widgets.text("source_code_path", "")

app_name = dbutils.widgets.get("app_name")
agent_endpoint = dbutils.widgets.get("agent_endpoint")
source_code_path = dbutils.widgets.get("source_code_path")

if not source_code_path:
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

# Sanity-check the endpoint exists before anything else.
ep = w.serving_endpoints.get(agent_endpoint)
print(f"Endpoint ready: {ep.name} (state={ep.state.ready})")

# COMMAND ----------

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
    print(f"App {app_name} already exists — updating resources.")
    w.apps.update(name=app_name, app=App(name=app_name, resources=resources))
except Exception as exc:
    print(f"App not found — creating {app_name}. ({exc.__class__.__name__})")
    w.apps.create_and_wait(app=App(name=app_name, resources=resources))

# COMMAND ----------

print(f"Deploying source from {source_code_path}...")
deployment = w.apps.deploy_and_wait(
    app_name=app_name,
    app_deployment=AppDeployment(source_code_path=source_code_path),
)
print(f"Deployment status: {deployment.status.state}")
if deployment.status.message:
    print(f"Deployment message: {deployment.status.message}")

# COMMAND ----------

app = w.apps.get(app_name)
state_str = str(app.compute_status.state).upper() if app.compute_status else ""
if "ACTIVE" in state_str or "STARTING" in state_str:
    print(f"App compute already running ({state_str}) — skipping start.")
else:
    print(f"Starting app compute (current state: {state_str or 'unknown'})...")
    w.apps.start_and_wait(name=app_name)
    app = w.apps.get(app_name)

print(f"App URL:       {app.url}")
print(f"Compute state: {app.compute_status.state if app.compute_status else 'unknown'}")
