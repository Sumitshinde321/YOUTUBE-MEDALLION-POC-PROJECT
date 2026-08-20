import re
from client import generate_llm
from db import run_query

DATA_AGENT_SYSTEM_PROMPT = """
You are the Data Agent for the YouTube Medallion Data Engineering Pipeline.
You translate natural language questions into a single PostgreSQL SELECT query against the `silver` or `gold` schemas.

DATABASE SCHEMAS & METADATA:

--- SILVER LAYER SCHEMA ---
- silver.videos:
  - video_id (varchar): Primary video identifier
  - trending_date (date): Date video was trending
  - title (text): Title of video
  - channel_title (text): Title of channel
  - category_id (int): Category ID reference
  - publish_time (timestamp): Video publish timestamp
  - tags (text): Comma-separated video tags
  - views (bigint): Total views count
  - likes (bigint): Total likes count
  - dislikes (bigint): Total dislikes count
  - comment_count (bigint): Total comments count
  - comments_disabled (boolean)
  - ratings_disabled (boolean)
  - video_error_or_removed (boolean)
  - description (text)

- silver.categories:
  - category_id (int): Category ID (PK)
  - category_title (varchar): Category name

- silver.channels:
  - channel_id (varchar): Channel natural ID (PK)
  - channel_title (varchar): Channel name
  - subscriber_count (bigint)
  - country (varchar)
  - total_videos (int)
  - total_views (bigint)

--- GOLD LAYER SCHEMA (BI & Marts) ---
- gold.dim_video (SCD Type 2):
  - dim_video_key (serial): Primary surrogate key
  - video_id (varchar): Natural video ID
  - title (text)
  - channel_title (text)
  - category_id (int)
  - publish_time (timestamp)
  - tags (text)
  - comments_disabled (boolean)
  - ratings_disabled (boolean)
  - video_error_or_removed (boolean)
  - description (text)
  - version_number (int)
  - effective_start_date (timestamp)
  - effective_end_date (timestamp)
  - is_current (boolean): Use 'is_current = true' to get current records

- gold.dim_category (SCD Type 1):
  - category_id (int): PK
  - category_title (varchar)

- gold.dim_channel (SCD Type 2):
  - dim_channel_key (serial): Primary surrogate key
  - channel_id (varchar): Natural channel ID
  - channel_title (varchar)
  - subscriber_count (bigint)
  - country (varchar)
  - total_videos (int)
  - total_views (bigint)
  - version_number (int)
  - effective_start_date (timestamp)
  - effective_end_date (timestamp)
  - is_current (boolean): Use 'is_current = true' to get current records

- gold.fact_video_metrics (Daily snapshots):
  - fact_key (serial): PK
  - video_id (varchar)
  - trending_date (date)
  - dim_video_key (int): FK to gold.dim_video
  - dim_category_id (int): FK to gold.dim_category
  - dim_channel_key (int): FK to gold.dim_channel
  - views (bigint)
  - likes (bigint)
  - dislikes (bigint)
  - comment_count (bigint)

- gold.channel_performance_mart (Pre-aggregated channel KPIs):
  - channel_title (varchar): PK
  - total_trending_videos (int)
  - total_views (bigint)
  - total_likes (bigint)
  - avg_likes_per_video (decimal)

- gold.trending_kpi_summary_mart (Pre-aggregated daily summaries):
  - trending_date (date): PK
  - total_trending_videos (int)
  - total_views (bigint)
  - total_likes (bigint)
  - most_trending_category (varchar)

- gold.trending_trends_mart (Pre-aggregated running totals & DoD changes):
  - trending_date (date): PK
  - total_views (bigint)
  - running_total_views (bigint)
  - previous_day_views (bigint)
  - pct_change_day_over_day (decimal)

QUERY WRITING RULES:
1. Return ONLY the raw PostgreSQL SQL statement. Do not wrap it in markdown or comments.
2. For dim_video and dim_channel, always include `is_current = true` unless historical snapshots are explicitly requested.
3. Use case-insensitive matching for string searches: e.g. `channel_title ILIKE '%5-minute%'` or `LOWER(channel_title) = 'cocomelon'`.
4. Join tables correctly using keys: `fact_video_metrics` joins to dimensions using surrogate keys (e.g. `dim_video_key` and `dim_channel_key`).
5. Prefer pre-aggregated marts (e.g. `gold.channel_performance_mart`, `gold.trending_trends_mart`) for high-level summaries.
6. SQL MUST be read-only (SELECT or WITH statements). Never write INSERT, UPDATE, DELETE, or DROP.
"""

def generate_sql(user_message: str) -> str:
    """Uses Gemini to translate natural language question to read-only SQL."""
    raw_sql = generate_llm(
        system_prompt=DATA_AGENT_SYSTEM_PROMPT,
        user_message=f"Question: \"{user_message}\""
    )
    # Clean SQL formatting output
    sql = raw_sql.strip().strip("`").replace("sql\n", "", 1).strip()
    return sql

def handle_data_query(user_message: str) -> dict:
    """Generates and executes SQL query, returning rows and metrics."""
    sql = generate_sql(user_message)
    try:
        db_result = run_query(sql)
        return {
            "sql": sql,
            "rows": db_result.get("rows", []),
            "row_count": db_result.get("row_count", 0),
            "status": "success"
        }
    except Exception as e:
        return {
            "sql": sql,
            "rows": [],
            "row_count": 0,
            "status": "error",
            "error": str(e)
        }
