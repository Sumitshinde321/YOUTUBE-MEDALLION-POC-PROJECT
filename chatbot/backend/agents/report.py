import os
import uuid
from datetime import datetime
from client import generate_llm

REPORT_SYSTEM_PROMPT = """
You are the Report Agent for the YouTube Medallion Data Engineering Pipeline chatbot.
Your job is to write a professional, 1-2 sentence introductory summary for the user based on the query they asked and the results retrieved.
Keep it direct and objective.
"""

def dicts_to_markdown_table(rows: list) -> str:
    """Converts a list of dicts to a clean Markdown table with pretty numbers formatting."""
    if not rows:
        return "No tabular data retrieved."
    
    # Extract keys as column headers
    headers = list(rows[0].keys())
    header_line = "| " + " | ".join(headers) + " |"
    separator_line = "| " + " | ".join(["---"] * len(headers)) + " |"
    
    data_lines = []
    for r in rows:
        row_values = []
        for h in headers:
            val = r.get(h)
            if isinstance(val, (int, float)):
                if "revenue" in h.lower() or "likes_per_video" in h.lower():
                    row_values.append(f"${val:,.2f}")
                else:
                    row_values.append(f"{val:,}")
            elif val is None:
                row_values.append("")
            else:
                row_values.append(str(val))
        data_lines.append("| " + " | ".join(row_values) + " |")
        
    return "\n".join([header_line, separator_line] + data_lines)

def build_report_payload(user_message: str, agent_outputs: dict, insight_res: dict) -> dict:
    """Formats final response and compiles optional downloadable files."""
    
    # 1. Programmatically extract tabular rows and convert to markdown
    table_md = ""
    # Support Agent rows
    if "raw_data" in agent_outputs and isinstance(agent_outputs["raw_data"], list) and agent_outputs["raw_data"]:
        table_md = dicts_to_markdown_table(agent_outputs["raw_data"])
    # Data Agent rows
    elif "rows" in agent_outputs and isinstance(agent_outputs["rows"], list) and agent_outputs["rows"]:
        table_md = dicts_to_markdown_table(agent_outputs["rows"])
    # ML Agent forecast
    elif "forecast" in agent_outputs.get("raw_data", {}):
        forecast_rows = agent_outputs["raw_data"]["forecast"]
        table_md = dicts_to_markdown_table(forecast_rows)
    # ML Agent anomalies
    elif "anomalies" in agent_outputs.get("raw_data", {}):
        anomalies_rows = agent_outputs["raw_data"]["anomalies"]
        table_md = dicts_to_markdown_table(anomalies_rows)
        
    # 2. Call Gemini to create an intro sentence
    summary_message = (
        f"User Query: '{user_message}'\n\n"
        f"Agent Output Summary: '{agent_outputs.get('result', '')}'"
    )
    intro = generate_llm(
        system_prompt=REPORT_SYSTEM_PROMPT,
        user_message=summary_message
    )
    
    insight_text = insight_res.get("insight", "")
    
    # 3. Check if user wanted to generate a report file
    report_url = None
    wants_report = any(word in user_message.lower() for word in ["generate report", "download report", "export report", "generate a report", "create a report", "create report", "downloadable report"])
    if wants_report:
        # Create reports folder
        reports_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "reports")
        os.makedirs(reports_dir, exist_ok=True)
        
        filename = f"report_{uuid.uuid4().hex[:8]}.md"
        filepath = os.path.join(reports_dir, filename)
        
        report_content = f"""# Pipeline Analysis Report
Generated on: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Query: "{user_message}"

## Executive Summary
{intro}

## Performance Insight Summary
{insight_text}

## Retracted Data Table
{table_md}
"""
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(report_content)
        
        # This will map to our static route `/reports/<filename>`
        report_url = f"/reports/{filename}"

    return {
        "intro": intro,
        "table_markdown": table_md,
        "insight": insight_text,
        "raw_data": agent_outputs,
        "report_url": report_url
    }
