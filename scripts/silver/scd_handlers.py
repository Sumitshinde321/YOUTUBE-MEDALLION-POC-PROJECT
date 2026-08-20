import argparse
import sys
import os
from datetime import datetime

# Adjust Python path to load config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from pipeline_config.config import get_db_connection, log_audit

def run_scd_type1_categories(run_id, conn):
    task_name = "scd_processing"
    step_name = "scd1_categories"
    
    print("Running SCD Type 1 for categories...")
    log_audit(run_id, task_name, step_name, "STARTED", 0, "Upserting categories using SCD Type 1")
    
    query = """
        INSERT INTO gold.dim_category (category_id, category_title, load_timestamp)
        SELECT category_id, category_title, load_timestamp
        FROM silver.categories
        ON CONFLICT (category_id) DO UPDATE
        SET category_title = EXCLUDED.category_title,
            load_timestamp = EXCLUDED.load_timestamp;
    """
    
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            affected = cur.rowcount
        conn.commit()
        log_audit(run_id, task_name, step_name, "COMPLETED", affected, f"SCD Type 1 categories completed. Affected: {affected}")
        print(f"SCD Type 1 categories completed. Affected: {affected}")
        return affected
    except Exception as e:
        conn.rollback()
        log_audit(run_id, task_name, step_name, "FAILED", 0, f"Error: {str(e)}")
        raise e

def run_scd_type2_videos(run_id, conn):
    task_name = "scd_processing"
    step_name = "scd2_videos"
    
    print("Running SCD Type 2 for videos...")
    log_audit(run_id, task_name, step_name, "STARTED", 0, "Historizing videos using SCD Type 2")
    
    try:
        with conn.cursor() as cur:
            # 1. Close changed records
            cur.execute("""
                CREATE TEMP TABLE changed_videos AS
                SELECT 
                    s.video_id, 
                    g.version_number
                FROM silver.videos s
                JOIN gold.dim_video g ON s.video_id = g.video_id
                WHERE g.is_current = TRUE 
                  AND (
                      s.title IS DISTINCT FROM g.title OR
                      s.channel_title IS DISTINCT FROM g.channel_title OR
                      s.category_id IS DISTINCT FROM g.category_id OR
                      s.tags IS DISTINCT FROM g.tags OR
                      s.comments_disabled IS DISTINCT FROM g.comments_disabled OR
                      s.ratings_disabled IS DISTINCT FROM g.ratings_disabled OR
                      s.video_error_or_removed IS DISTINCT FROM g.video_error_or_removed OR
                      s.description IS DISTINCT FROM g.description
                  );
            """)
            
            cur.execute("""
                UPDATE gold.dim_video g
                SET is_current = FALSE,
                    effective_end_date = CURRENT_TIMESTAMP
                FROM changed_videos c
                WHERE g.video_id = c.video_id AND g.is_current = TRUE;
            """)
            closed_records = cur.rowcount
            
            cur.execute("DROP TABLE IF EXISTS changed_videos;")
            
            # 2. Insert new versions (both brand new videos and updated versions of existing videos)
            cur.execute("""
                INSERT INTO gold.dim_video (
                    video_id, title, channel_title, category_id, publish_time, tags, 
                    comments_disabled, ratings_disabled, video_error_or_removed, description, 
                    version_number, effective_start_date, effective_end_date, is_current
                )
                SELECT 
                    s.video_id, s.title, s.channel_title, s.category_id, s.publish_time, s.tags, 
                    s.comments_disabled, s.ratings_disabled, s.video_error_or_removed, s.description,
                    COALESCE(prev.version_number, 0) + 1,
                    CURRENT_TIMESTAMP,
                    NULL,
                    TRUE
                FROM (
                    -- De-duplicate silver records to only load the latest version per run
                    SELECT DISTINCT ON (video_id) *
                    FROM silver.videos
                    ORDER BY video_id, load_timestamp DESC
                ) s
                LEFT JOIN (
                    SELECT video_id, MAX(version_number) as version_number 
                    FROM gold.dim_video 
                    GROUP BY video_id
                ) prev ON s.video_id = prev.video_id
                LEFT JOIN gold.dim_video curr ON s.video_id = curr.video_id AND curr.is_current = TRUE
                WHERE curr.dim_video_key IS NULL;
            """)
            inserted_records = cur.rowcount
            
        conn.commit()
        log_audit(run_id, task_name, step_name, "COMPLETED", inserted_records, 
                  f"SCD Type 2 videos completed. Closed: {closed_records}, Inserted: {inserted_records}")
        print(f"SCD Type 2 videos completed. Closed: {closed_records}, Inserted: {inserted_records}")
        return inserted_records
    except Exception as e:
        conn.rollback()
        log_audit(run_id, task_name, step_name, "FAILED", 0, f"Error: {str(e)}")
        raise e

def run_scd_type2_channels(run_id, conn):
    task_name = "scd_processing"
    step_name = "scd2_channels"
    
    print("Running SCD Type 2 for channels...")
    log_audit(run_id, task_name, step_name, "STARTED", 0, "Historizing channels using SCD Type 2")
    
    try:
        with conn.cursor() as cur:
            # 1. Close changed records
            cur.execute("""
                CREATE TEMP TABLE changed_channels AS
                SELECT 
                    s.channel_id, 
                    g.version_number
                FROM silver.channels s
                JOIN gold.dim_channel g ON s.channel_id = g.channel_id
                WHERE g.is_current = TRUE 
                  AND (
                      s.channel_title IS DISTINCT FROM g.channel_title OR
                      s.subscriber_count IS DISTINCT FROM g.subscriber_count OR
                      s.country IS DISTINCT FROM g.country OR
                      s.total_videos IS DISTINCT FROM g.total_videos OR
                      s.total_views IS DISTINCT FROM g.total_views
                  );
            """)
            
            cur.execute("""
                UPDATE gold.dim_channel g
                SET is_current = FALSE,
                    effective_end_date = CURRENT_TIMESTAMP
                FROM changed_channels c
                WHERE g.channel_id = c.channel_id AND g.is_current = TRUE;
            """)
            closed_records = cur.rowcount
            
            cur.execute("DROP TABLE IF EXISTS changed_channels;")
            
            # 2. Insert new versions
            cur.execute("""
                INSERT INTO gold.dim_channel (
                    channel_id, channel_title, subscriber_count, country, total_videos, total_views,
                    version_number, effective_start_date, effective_end_date, is_current
                )
                SELECT 
                    s.channel_id, s.channel_title, s.subscriber_count, s.country, s.total_videos, s.total_views,
                    COALESCE(prev.version_number, 0) + 1,
                    CURRENT_TIMESTAMP,
                    NULL,
                    TRUE
                FROM silver.channels s
                LEFT JOIN (
                    SELECT channel_id, MAX(version_number) as version_number 
                    FROM gold.dim_channel 
                    GROUP BY channel_id
                ) prev ON s.channel_id = prev.channel_id
                LEFT JOIN gold.dim_channel curr ON s.channel_id = curr.channel_id AND curr.is_current = TRUE
                WHERE curr.dim_channel_key IS NULL;
            """)
            inserted_records = cur.rowcount
            
        conn.commit()
        log_audit(run_id, task_name, step_name, "COMPLETED", inserted_records, 
                  f"SCD Type 2 channels completed. Closed: {closed_records}, Inserted: {inserted_records}")
        print(f"SCD Type 2 channels completed. Closed: {closed_records}, Inserted: {inserted_records}")
        return inserted_records
    except Exception as e:
        conn.rollback()
        log_audit(run_id, task_name, step_name, "FAILED", 0, f"Error: {str(e)}")
        raise e

def run_scd(run_id):
    conn = get_db_connection()
    try:
        run_scd_type1_categories(run_id, conn)
        run_scd_type2_videos(run_id, conn)
        run_scd_type2_channels(run_id, conn)
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    run_scd(args.run_id)
