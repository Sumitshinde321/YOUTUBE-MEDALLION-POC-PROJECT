import argparse
import sys
import os
import pandas as pd
from datetime import datetime

# Adjust Python path to load config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from pipeline_config.config import get_db_engine, log_audit, CSV_FILE_PATH, CHUNK_SIZE

def ingest_csv(run_id):
    task_name = "bronze_ingestion"
    step_name = "ingest_csv"
    
    print(f"Starting CSV ingestion from {CSV_FILE_PATH}")
    log_audit(run_id, task_name, step_name, "STARTED", 0, f"Reading CSV: {CSV_FILE_PATH}")
    
    if not os.path.exists(CSV_FILE_PATH):
        err_msg = f"Source CSV file not found at {CSV_FILE_PATH}"
        log_audit(run_id, task_name, step_name, "FAILED", 0, err_msg)
        raise FileNotFoundError(err_msg)
        
    try:
        engine = get_db_engine()
        # For idempotency, clear existing records loaded today
        with engine.begin() as conn:
            conn.execute("DELETE FROM bronze.raw_youtube_videos WHERE ingestion_date = CURRENT_DATE")
            
        total_rows = 0
        # Use keep_default_na=False to avoid interpreting empty values as NaN (keeps them as empty strings)
        chunks = pd.read_csv(CSV_FILE_PATH, chunksize=CHUNK_SIZE, keep_default_na=False, dtype=str)
        for chunk in chunks:
            # Map headers exactly to the database columns
            chunk['source_system'] = 'USvideos_csv'
            chunk['load_timestamp'] = datetime.now()
            chunk['ingestion_date'] = datetime.now().date()
            
            # Save to PostgreSQL
            chunk.to_sql(
                name='raw_youtube_videos',
                con=engine,
                schema='bronze',
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            total_rows += len(chunk)
            print(f"Ingested {total_rows} rows so far...")
            
        log_audit(run_id, task_name, step_name, "COMPLETED", total_rows, f"Successfully ingested {total_rows} rows from CSV")
        print(f"CSV Ingestion complete: {total_rows} rows written.")
        return total_rows
    except Exception as e:
        log_audit(run_id, task_name, step_name, "FAILED", 0, f"Error: {str(e)}")
        raise e

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    ingest_csv(args.run_id)
