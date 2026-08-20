import json
from client import generate_llm
from db import run_query

SUPPORT_SYSTEM_PROMPT = """
You are the Support Agent for the YouTube Medallion Data Engineering Pipeline.
You handle system, operational, and pipeline metadata questions.
You have access to the following metadata tables:

1. metadata.pipeline_runs:
   - run_id (varchar): Unique identifier for a DAG run
   - pipeline_name (varchar): E.g., 'youtube_medallion_etl'
   - execution_date (timestamp): Execution timestamp of the run
   - status (varchar): SUCCESS, FAILED, RUNNING
   - start_time (timestamp)
   - end_time (timestamp)
   - duration_seconds (decimal)
   - records_inserted (int)
   - records_updated (int)
   - records_rejected (int)

2. metadata.audit_log:
   - log_id (serial): PK
   - run_id (varchar): FK to pipeline_runs
   - task_name (varchar): E.g., 'ingest_csv_to_bronze', 'validate_and_clean_to_silver', 'process_scd_dimensions'
   - step_name (varchar)
   - status (varchar): STARTED, COMPLETED, FAILED
   - affected_rows (int)
   - log_message (text)
   - log_timestamp (timestamp)

3. silver.rejected_records:
   - rejected_id (serial): PK
   - run_id (varchar)
   - table_name (varchar)
   - raw_record_json (text)
   - rejection_reason (varchar): E.g., 'MISSING_VIDEO_ID', 'INVALID_DATE_FORMATS'
   - rejected_at (timestamp)

Additionally, you answer general project structure and setup questions, such as:
- How to trigger the DAG
- What schemas exist (bronze, silver, gold, metadata)
- How data flows (Bronze raw -> Silver cleansed & rejected -> Gold SCD & marts)

Your response must be a JSON object with one of the following formats:
- If you need database query:
  {"action": "query", "sql": "SELECT ... LIMIT 10"}
- If you can answer directly:
  {"action": "answer", "response": "Your structured markdown answer here."}

Rules:
1. Always write read-only SELECT statements. Never write modifications (INSERT, UPDATE, DELETE).
2. Limit all SQL results to at most 20 rows.
3. Order query results logically (e.g. log_timestamp DESC or execution_date DESC) to show recent information first.
4. Reply ONLY with the JSON block. Do not include markdown code block formatting.
"""

def handle_support_query(user_message: str) -> dict:
    """Decides to execute SQL against metadata tables or answer general help directly."""
    raw_response = generate_llm(
        system_prompt=SUPPORT_SYSTEM_PROMPT,
        user_message=f"Question: \"{user_message}\""
    )
    cleaned = raw_response.strip().strip("`").replace("json\n", "", 1).strip()
    try:
        data = json.loads(cleaned)
    except Exception:
        # Fallback if parsing fails
        return {
            "result": "I'm having trouble analyzing that system query. Can you please rephrase it?",
            "raw_data": None
        }

    if data.get("action") == "query":
        sql = data.get("sql")
        try:
            db_result = run_query(sql)
            rows = db_result.get("rows", [])
            row_count = db_result.get("row_count", 0)
            
            # Ground the results into a conversational summary
            summary_prompt = (
                f"The user asked: '{user_message}'\n"
                f"We executed the SQL: '{sql}'\n"
                f"And received {row_count} rows: {rows}\n"
                f"Explain the status or rows conversationally in plain English."
            )
            summary = generate_llm(
                system_prompt="You are a data engineering support bot explaining operational logs. Be helpful and clear.",
                user_message=summary_prompt
            )
            return {
                "result": summary,
                "raw_data": rows,
                "sql": sql
            }
        except Exception as e:
            return {
                "result": f"Failed to execute system check query: {str(e)}",
                "raw_data": None,
                "sql": sql
            }
    else:
        return {
            "result": data.get("response", "No response generated."),
            "raw_data": None
        }
