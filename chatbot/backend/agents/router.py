import json
from client import generate_llm

ROUTER_SYSTEM_PROMPT = """
You are the Router Agent for a multi-agent YouTube Data Pipeline chatbot.
Your job is to:
1. Classify the user's question into one or more of these specialized agent categories:
   - "support": Questions about pipeline runs, task failures, DAG status, row counts, schema details, rejected records count, or help/how-to guides about the project itself.
   - "data": Data exploration queries, e.g., ranking channels, showing most popular videos, counts of records in gold/silver schemas, views/likes/comments metrics comparison.
   - "ml": Numerical forecasting ("predict views/likes"), anomaly detection ("any spikes in views?"), or churn/drop likelihood.

2. Extract key parameters from the user's question, if present:
   - "table_name": Name of the table being queried if mentioned.
   - "date_range": Any dates, ranges (e.g. 'last month', 'yesterday', 'next 30 days').
   - "metric": E.g. views, likes, dislikes, comment count, duration, records inserted/rejected.
   - "entity": Target of the query (e.g. specific channel title, video title, category).

3. Return ONLY a valid JSON object. Do not include markdown code block formatting. The JSON object must match this schema:
{
  "intents": ["support" | "data" | "ml"],
  "parameters": {
    "table_name": string or null,
    "date_range": string or null,
    "metric": string or null,
    "entity": string or null
  }
}

Few-shot examples:
Q: "Did the pipeline succeed yesterday?"
A: {"intents": ["support"], "parameters": {"table_name": "metadata.pipeline_runs", "date_range": "yesterday", "metric": "status", "entity": null}}

Q: "Show me the top 10 channels by views."
A: {"intents": ["data"], "parameters": {"table_name": "gold.channel_performance_mart", "date_range": null, "metric": "views", "entity": "channels"}}

Q: "Can you forecast the views for 5-Minute Crafts over the next month?"
A: {"intents": ["ml"], "parameters": {"table_name": null, "date_range": "next month", "metric": "views", "entity": "5-Minute Crafts"}}

Q: "Find top trending categories by likes and check if the scheduler failed today."
A: {"intents": ["data", "support"], "parameters": {"table_name": null, "date_range": "today", "metric": "likes", "entity": "categories"}}
"""

def route_question(user_message: str) -> dict:
    """Classifies user intent and extracts parameters into structured JSON."""
    raw_response = generate_llm(
        system_prompt=ROUTER_SYSTEM_PROMPT,
        user_message=f"Question: \"{user_message}\""
    )
    # Clean output if model included markdown wraps
    cleaned = raw_response.strip().strip("`").replace("json\n", "", 1).strip()
    try:
        return json.loads(cleaned)
    except Exception:
        # Fallback in case of parse error
        return {
            "intents": ["data"],
            "parameters": {
                "table_name": None,
                "date_range": None,
                "metric": None,
                "entity": None
            }
        }
