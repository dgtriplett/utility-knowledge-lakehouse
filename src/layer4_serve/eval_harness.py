# Databricks notebook source
# MAGIC %md
# MAGIC # Layer 4 — Evaluation harness
# MAGIC
# MAGIC A small hand-labeled eval set + MLflow's built-in LLM-as-judge. The
# MAGIC goal isn't a perfect score; it's a repeatable baseline you can diff
# MAGIC against every time the corpus, prompts, or model changes.

# COMMAND ----------

# MAGIC %pip install -q "mlflow[databricks]>=3.0.0" databricks-agents
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
import pandas as pd
from databricks.sdk import WorkspaceClient

dbutils.widgets.text("catalog", "utility_knowledge")
dbutils.widgets.text("agents_schema", "agents")
dbutils.widgets.text(
    "agent_model_name", "utility_knowledge.agents.utility_assistant"
)

catalog = dbutils.widgets.get("catalog")
agents_schema = dbutils.widgets.get("agents_schema")
model_name = dbutils.widgets.get("agent_model_name")

endpoint_name = f"agents_{model_name.replace('.', '-')}"

# COMMAND ----------

eval_rows = [
    {
        "request": "What is the bus arrangement at Oak Ridge substation?",
        "expected_kind": "fact_lookup",
        "expected_contains_any": ["ring", "breaker-and-a-half", "double-bus"],
    },
    {
        "request": "Why was the time dial on breaker 138L-1 left unchanged in the last study?",
        "expected_kind": "rationale_lookup",
        "expected_contains_any": ["coordination", "miscoordinat", "recloser"],
    },
    {
        "request": "List the equipment at Pine Hollow substation.",
        "expected_kind": "list_lookup",
        "expected_contains_any": ["L-", "B-", "C-", "T-"],
    },
    {
        "request": "Are there any known firmware quirks on the relays?",
        "expected_kind": "tacit_knowledge",
        "expected_contains_any": ["1.7", "timestamp", "GPS"],
    },
    {
        "request": "What is the capital of France?",
        "expected_kind": "refusal",
        "expected_contains_any": ["don't have", "no relevant", "not in"],
    },
]

eval_df = pd.DataFrame(eval_rows)
display(eval_df)

# COMMAND ----------

w = WorkspaceClient()


def query_agent(request: str) -> str:
    response = w.serving_endpoints.query(
        name=endpoint_name,
        messages=[{"role": "user", "content": request}],
    )
    return response.choices[0].message.content


eval_df["response"] = eval_df["request"].apply(query_agent)

# COMMAND ----------

def simple_contains(row) -> float:
    resp = (row["response"] or "").lower()
    hits = sum(1 for t in row["expected_contains_any"] if t.lower() in resp)
    return float(hits > 0)


eval_df["contains_score"] = eval_df.apply(simple_contains, axis=1)

# LLM-as-judge for correctness + groundedness.
with mlflow.start_run(run_name="utility_assistant_eval"):
    results = mlflow.evaluate(
        data=eval_df,
        predictions="response",
        targets="expected_contains_any",
        model_type="question-answering",
        extra_metrics=[
            mlflow.metrics.genai.answer_correctness(),
            mlflow.metrics.genai.answer_relevance(),
        ],
    )
    mlflow.log_metric("contains_score_mean", eval_df["contains_score"].mean())
    print(results.metrics)

# COMMAND ----------

# Persist the eval run as a Delta table so regressions can be diffed over time.
spark.createDataFrame(eval_df).write.mode("append").option(
    "mergeSchema", "true"
).saveAsTable(f"{catalog}.{agents_schema}.eval_runs")
