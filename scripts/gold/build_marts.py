import argparse
import sys
import os
from datetime import datetime

# Adjust Python path to load config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from pipeline_config.config import get_db_connection, log_audit

def load_fact_table(run_id, conn):
    task_name = "gold_mart_building"
    step_name = "load_fact_table"
    
    print("Loading facts into gold.fact_video_metrics...")
    log_audit(run_id, task_name, step_name, "STARTED", 0, "Mapping Silver records to Gold dim keys and inserting facts")
    
    try:
        with conn.cursor() as cur:
            # 1. Handle late-arriving / inferred members for channels
            cur.execute("""
                INSERT INTO gold.dim_channel (
                    channel_id, channel_title, subscriber_count, country, 
                    total_videos, total_views, version_number, effective_start_date, is_current
                )
                SELECT DISTINCT 
                    'inferred_' || md5(s.channel_title),
                    s.channel_title,
                    0, 'Unknown', 0, 0,
                    1, CURRENT_TIMESTAMP, TRUE
                FROM silver.videos s
                LEFT JOIN gold.dim_channel g ON s.channel_title = g.channel_title AND g.is_current = TRUE
                WHERE g.dim_channel_key IS NULL 
                  AND s.channel_title IS NOT NULL 
                  AND s.channel_title != '';
            """)
            inferred_channels = cur.rowcount
            
            # 2. Handle late-arriving / inferred members for videos
            cur.execute("""
                INSERT INTO gold.dim_video (
                    video_id, title, channel_title, category_id, publish_time, tags, 
                    comments_disabled, ratings_disabled, video_error_or_removed, description, 
                    version_number, effective_start_date, is_current
                )
                SELECT DISTINCT 
                    s.video_id, s.title, s.channel_title, s.category_id, s.publish_time, s.tags, 
                    s.comments_disabled, s.ratings_disabled, s.video_error_or_removed, s.description,
                    1, CURRENT_TIMESTAMP, TRUE
                FROM silver.videos s
                LEFT JOIN gold.dim_video g ON s.video_id = g.video_id AND g.is_current = TRUE
                WHERE g.dim_video_key IS NULL;
            """)
            inferred_videos = cur.rowcount
            
            # 3. Handle late-arriving / inferred members for categories
            cur.execute("""
                INSERT INTO gold.dim_category (category_id, category_title, load_timestamp)
                SELECT DISTINCT s.category_id, 'Inferred Category ' || s.category_id, CURRENT_TIMESTAMP
                FROM silver.videos s
                LEFT JOIN gold.dim_category g ON s.category_id = g.category_id
                WHERE g.category_id IS NULL;
            """)
            inferred_categories = cur.rowcount
            
            print(f"Late-arriving members processed. Channels: {inferred_channels}, Videos: {inferred_videos}, Categories: {inferred_categories}")
            
            # 4. Idempotent deletion of facts matching the dates of current silver records
            cur.execute("""
                DELETE FROM gold.fact_video_metrics
                WHERE trending_date IN (SELECT DISTINCT trending_date FROM silver.videos);
            """)
            deleted_facts = cur.rowcount
            
            # 5. Insert fact records mapped to active dimension surrogate keys
            cur.execute("""
                INSERT INTO gold.fact_video_metrics (
                    video_id, trending_date, dim_video_key, dim_category_id, dim_channel_key, 
                    views, likes, dislikes, comment_count, load_timestamp
                )
                SELECT 
                    s.video_id,
                    s.trending_date,
                    v.dim_video_key,
                    c.category_id,
                    ch.dim_channel_key,
                    s.views,
                    s.likes,
                    s.dislikes,
                    s.comment_count,
                    CURRENT_TIMESTAMP
                FROM silver.videos s
                JOIN gold.dim_video v ON s.video_id = v.video_id AND v.is_current = TRUE
                JOIN gold.dim_category c ON s.category_id = c.category_id
                JOIN gold.dim_channel ch ON s.channel_title = ch.channel_title AND ch.is_current = TRUE;
            """)
            inserted_facts = cur.rowcount
            
        conn.commit()
        log_audit(run_id, task_name, step_name, "COMPLETED", inserted_facts, 
                  f"Fact load complete. Inferred [Ch: {inferred_channels}, Vid: {inferred_videos}, Cat: {inferred_categories}], Deleted: {deleted_facts}, Loaded: {inserted_facts}")
        print(f"Fact table load complete. Loaded: {inserted_facts} rows.")
        return inserted_facts
    except Exception as e:
        conn.rollback()
        log_audit(run_id, task_name, step_name, "FAILED", 0, f"Error: {str(e)}")
        raise e

def build_channel_performance_mart(run_id, conn):
    task_name = "gold_mart_building"
    step_name = "channel_performance_mart"
    
    print("Rebuilding gold.channel_performance_mart...")
    log_audit(run_id, task_name, step_name, "STARTED", 0, "Truncating and recalculating channel performance mart")
    
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE gold.channel_performance_mart;")
            cur.execute("""
                INSERT INTO gold.channel_performance_mart (
                    channel_title, total_trending_videos, total_views, total_likes, avg_likes_per_video, load_timestamp
                )
                SELECT 
                    ch.channel_title,
                    COUNT(DISTINCT f.video_id) as total_trending_videos,
                    SUM(f.views) as total_views,
                    SUM(f.likes) as total_likes,
                    ROUND(AVG(f.likes), 2) as avg_likes_per_video,
                    CURRENT_TIMESTAMP
                FROM gold.fact_video_metrics f
                JOIN gold.dim_channel ch ON f.dim_channel_key = ch.dim_channel_key
                GROUP BY ch.channel_title;
            """)
            affected = cur.rowcount
        conn.commit()
        log_audit(run_id, task_name, step_name, "COMPLETED", affected, f"Channel performance mart complete. Rows: {affected}")
        print(f"Channel performance mart complete. Rows: {affected}")
        return affected
    except Exception as e:
        conn.rollback()
        log_audit(run_id, task_name, step_name, "FAILED", 0, f"Error: {str(e)}")
        raise e

def build_kpi_summary_mart(run_id, conn):
    task_name = "gold_mart_building"
    step_name = "trending_kpi_summary_mart"
    
    print("Rebuilding gold.trending_kpi_summary_mart...")
    log_audit(run_id, task_name, step_name, "STARTED", 0, "Truncating and recalculating KPI summary mart using ROW_NUMBER window function")
    
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE gold.trending_kpi_summary_mart;")
            cur.execute("""
                INSERT INTO gold.trending_kpi_summary_mart (
                    trending_date, total_trending_videos, total_views, total_likes, most_trending_category, load_timestamp
                )
                WITH category_ranks AS (
                    SELECT 
                        f.trending_date,
                        c.category_title,
                        ROW_NUMBER() OVER (PARTITION BY f.trending_date ORDER BY SUM(f.views) DESC) as rank
                    FROM gold.fact_video_metrics f
                    JOIN gold.dim_category c ON f.dim_category_id = c.category_id
                    GROUP BY f.trending_date, c.category_title
                ),
                top_categories AS (
                    SELECT trending_date, category_title 
                    FROM category_ranks 
                    WHERE rank = 1
                ),
                daily_aggregates AS (
                    SELECT 
                        f.trending_date,
                        COUNT(DISTINCT f.video_id) as total_trending_videos,
                        SUM(f.views) as total_views,
                        SUM(f.likes) as total_likes
                    FROM gold.fact_video_metrics f
                    GROUP BY f.trending_date
                )
                SELECT 
                    da.trending_date,
                    da.total_trending_videos,
                    da.total_views,
                    da.total_likes,
                    tc.category_title as most_trending_category,
                    CURRENT_TIMESTAMP
                FROM daily_aggregates da
                LEFT JOIN top_categories tc ON da.trending_date = tc.trending_date;
            """)
            affected = cur.rowcount
        conn.commit()
        log_audit(run_id, task_name, step_name, "COMPLETED", affected, f"KPI summary mart complete. Rows: {affected}")
        print(f"KPI summary mart complete. Rows: {affected}")
        return affected
    except Exception as e:
        conn.rollback()
        log_audit(run_id, task_name, step_name, "FAILED", 0, f"Error: {str(e)}")
        raise e

def build_trends_mart(run_id, conn):
    task_name = "gold_mart_building"
    step_name = "trending_trends_mart"
    
    print("Rebuilding gold.trending_trends_mart...")
    log_audit(run_id, task_name, step_name, "STARTED", 0, "Truncating and recalculating trends mart using SUM OVER and LAG window functions")
    
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE gold.trending_trends_mart;")
            cur.execute("""
                INSERT INTO gold.trending_trends_mart (
                    trending_date, total_views, running_total_views, previous_day_views, pct_change_day_over_day, load_timestamp
                )
                WITH daily_totals AS (
                    SELECT 
                        trending_date,
                        SUM(views) as total_views
                    FROM gold.fact_video_metrics
                    GROUP BY trending_date
                ),
                windowed_totals AS (
                    SELECT 
                        trending_date,
                        total_views,
                        SUM(total_views) OVER (ORDER BY trending_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total_views,
                        LAG(total_views, 1) OVER (ORDER BY trending_date) as previous_day_views
                    FROM daily_totals
                )
                SELECT 
                    trending_date,
                    total_views,
                    running_total_views,
                    previous_day_views,
                    CASE 
                        WHEN previous_day_views IS NULL OR previous_day_views = 0 THEN 0.00
                        ELSE ROUND(((total_views - previous_day_views)::DECIMAL / previous_day_views) * 100, 2)
                    END as pct_change_day_over_day,
                    CURRENT_TIMESTAMP
                FROM windowed_totals;
            """)
            affected = cur.rowcount
        conn.commit()
        log_audit(run_id, task_name, step_name, "COMPLETED", affected, f"Trends mart complete. Rows: {affected}")
        print(f"Trends mart complete. Rows: {affected}")
        return affected
    except Exception as e:
        conn.rollback()
        log_audit(run_id, task_name, step_name, "FAILED", 0, f"Error: {str(e)}")
        raise e

def run_transformations(run_id):
    conn = get_db_connection()
    try:
        load_fact_table(run_id, conn)
        build_channel_performance_mart(run_id, conn)
        build_kpi_summary_mart(run_id, conn)
        build_trends_mart(run_id, conn)
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    run_transformations(args.run_id)
