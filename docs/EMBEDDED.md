# Embedded agents

Conversational chat is one surface. The other — often the more valuable one — is surfacing context *inside the tools people already use*. An EAM work order screen, a GIS map popup, an outage management console.

The same Model Serving endpoint does both. Here's the minimal pattern.

## Calling the agent from an external app

```bash
curl -X POST \
  "https://<workspace>.cloud.databricks.com/serving-endpoints/agents_utility_knowledge-agents-utility_assistant/invocations" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [
      {"role": "user", "content": "Summarize known issues for asset 138L-7 at Oak Ridge substation."}
    ]
  }'
```

Response includes `choices[0].message.content` (the answer) and `custom_outputs.citations` (the list of chunks used). Render the answer in your side panel, render the citations as links back to your document viewer.

## Python helper for in-flow prompts

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

def summarize_for_asset(asset_id: str, substation: str) -> dict:
    prompt = (
        f"Summarize in under 100 words what an operator should know about "
        f"asset {asset_id} at {substation} before dispatching a crew. "
        f"Include any open issues, recent decisions, or SME notes."
    )
    response = w.serving_endpoints.query(
        name="agents_utility_knowledge-agents-utility_assistant",
        messages=[{"role": "user", "content": prompt}],
    )
    return {
        "summary": response.choices[0].message.content,
        "citations": (response.custom_outputs or {}).get("citations", []),
    }
```

## Trigger patterns

- **Page load.** Side panel shows the summary as soon as the work order or asset screen opens.
- **On demand.** Button that says "Ask about this asset" — cheaper if traffic is bursty.
- **Proactive flags.** Background job runs `summarize_for_asset` for every asset with an open work order overnight, caches the result in Lakebase, serves the cached result instantly on page load.

The proactive flag pattern is where "embedded agents" becomes meaningfully different from "chat." The user never has to ask. The context just shows up.
