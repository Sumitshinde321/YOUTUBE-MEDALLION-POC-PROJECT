import argparse
import sys
import os
import json
import pandas as pd
from datetime import datetime

# Adjust Python path to load config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from pipeline_config.config import get_db_engine, log_audit, JSON_FILE_PATH

def ingest_json(run_id):
    task_name = "bronze_ingestion"
    step_name = "ingest_json"
    
    print(f"Starting JSON ingestion from {JSON_FILE_PATH}")
    log_audit(run_id, task_name, step_name, "STARTED", 0, f"Reading JSON: {JSON_FILE_PATH}")
    
    if not os.path.exists(JSON_FILE_PATH):
        err_msg = f"Source JSON file not found at {JSON_FILE_PATH}"
        log_audit(run_id, task_name, step_name, "FAILED", 0, err_msg)
        raise FileNotFoundError(err_msg)
        
    try:
        with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        items = data.get("items", [])
        records = []
        for item in items:
            cat_id = item.get("id")
            title = item.get("snippet", {}).get("title")
            if cat_id and title:
                records.append({
                    "category_id": str(cat_id),
                    "category_title": str(title),
                    "source_system": "US_category_json",
                    "load_timestamp": datetime.now()
                })
                
        df = pd.DataFrame(records)
        
        engine = get_db_engine()
        # Idempotency: clear existing category records
        with engine.begin() as conn:
            conn.execute("TRUNCATE TABLE bronze.raw_youtube_categories")
            
        df.to_sql(
            name='raw_youtube_categories',
            con=engine,
            schema='bronze',
            if_exists='append',
            index=False,
            method='multi'
        )
        
        log_audit(run_id, task_name, step_name, "COMPLETED", len(df), f"Successfully ingested {len(df)} rows from JSON")
        print(f"JSON Ingestion complete: {len(df)} rows written.")
        return len(df)
    except Exception as e:
        log_audit(run_id, task_name, step_name, "FAILED", 0, f"Error: {str(e)}")
        raise e

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    ingest_json(args.run_id)
