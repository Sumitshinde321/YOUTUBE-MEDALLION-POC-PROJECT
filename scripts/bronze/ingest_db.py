import argparse
import sys
import os
import pandas as pd
from datetime import datetime

# Adjust Python path to load config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from pipeline_config.config import get_db_engine, get_db_connection, log_audit

def seed_source_db():
    """Simulates the source system DB by creating and seeding an external schema/table."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Create external schema & table simulating an operational DB
            cur.execute("CREATE SCHEMA IF NOT EXISTS external_source;")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS external_source.channels_extra (
                    channel_id VARCHAR(255) PRIMARY KEY,
                    channel_title VARCHAR(255),
                    total_videos INT,
                    total_views BIGINT,
                    last_updated TIMESTAMP
                );
            """)
            
            # Check if seeded
            cur.execute("SELECT COUNT(*) FROM external_source.channels_extra;")
            count = cur.fetchone()[0]
            
            if count == 0:
                print("Seeding simulated database source channels_extra...")
                seed_data = [
                    ("UCqgUR0M9T3yFkwm1P-J8pww", "CaseyNeistat", 1200, 3100000000, datetime(2026, 8, 19, 0, 0, 0)),
                    ("UC3XTzVzaHQEd30rQbuvAQLQ", "LastWeekTonight", 380, 1850000000, datetime(2026, 8, 18, 12, 0, 0)),
                    ("UC-9-kyTW8ZkZNDHQJ6FgpwQ", "Rudy Mancuso", 160, 1280000000, datetime(2026, 8, 17, 9, 0, 0)),
                    ("UCi1O33A4sVn20A415s6XNtw", "Good Mythical Morning", 2900, 8300000000, datetime(2026, 8, 19, 6, 0, 0)),
                    ("UC0C-w0YjGpqDXGB8IHb66AV", "YouTube Spotlight", 410, 2600000000, datetime(2026, 8, 19, 0, 0, 0))
                ]
                cur.executemany("""
                    INSERT INTO external_source.channels_extra (channel_id, channel_title, total_videos, total_views, last_updated)
                    VALUES (%s, %s, %s, %s, %s);
                """, seed_data)
                conn.commit()
                print("Seeding complete.")
    except Exception as e:
        print(f"Error seeding simulated source: {e}")
        conn.rollback()
    finally:
        conn.close()

def ingest_db(run_id):
    task_name = "bronze_ingestion"
    step_name = "ingest_db"
    
    print("Starting simulated database ingestion (incremental)...")
    log_audit(run_id, task_name, step_name, "STARTED", 0, "Querying incremental source DB")
    
    # Ensure source DB is seeded
    seed_source_db()
    
    try:
        conn = get_db_connection()
        engine = get_db_engine()
        
        # 1. Fetch watermark (max last_updated in bronze)
        max_watermark = None
        with conn.cursor() as cur:
            # Check if table has data to establish a watermark
            cur.execute("""
                SELECT MAX(last_updated::timestamp) 
                FROM bronze.raw_channels_extra;
            """)
            res = cur.fetchone()
            if res and res[0] is not None:
                max_watermark = res[0]
                
        print(f"Established incremental watermark: {max_watermark}")
        
        # 2. Query source DB for rows newer than the watermark
        if max_watermark:
            query = "SELECT * FROM external_source.channels_extra WHERE last_updated > %s"
            params = (max_watermark,)
        else:
            query = "SELECT * FROM external_source.channels_extra"
            params = None
            
        df = pd.read_sql(query, con=engine, params=params)
        
        if df.empty:
            print("No new data since watermark. Skipping load.")
            log_audit(run_id, task_name, step_name, "COMPLETED", 0, f"No new records to ingest since {max_watermark}")
            return 0
            
        # 3. Add metadata
        df['source_system'] = 'postgres_external_db'
        df['load_timestamp'] = datetime.now()
        
        # Convert columns to string for Bronze raw storage mapping
        for col in df.columns:
            if col not in ['load_timestamp']:
                df[col] = df[col].astype(str)
                
        # 4. Save to Bronze
        df.to_sql(
            name='raw_channels_extra',
            con=engine,
            schema='bronze',
            if_exists='append',
            index=False,
            method='multi'
        )
        
        log_audit(run_id, task_name, step_name, "COMPLETED", len(df), f"Ingested {len(df)} incremental records from DB")
        print(f"Simulated DB Ingestion complete: {len(df)} rows written.")
        return len(df)
    except Exception as e:
        log_audit(run_id, task_name, step_name, "FAILED", 0, f"Error: {str(e)}")
        raise e

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    ingest_db(args.run_id)
