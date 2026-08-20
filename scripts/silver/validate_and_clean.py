import argparse
import sys
import os
import json
import pandas as pd
import numpy as np
from datetime import datetime

# Adjust Python path to load config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from pipeline_config.config import get_db_engine, log_audit, CHUNK_SIZE

def clean_trending_date(val):
    """Parses trending_date from YY.DD.MM format to datetime date."""
    if not val or pd.isna(val):
        return None
    try:
        # e.g., 17.14.11 -> 2017-11-14
        return datetime.strptime(str(val).strip(), "%y.%d.%m").date()
    except Exception:
        return None

def clean_publish_time(val):
    """Parses publish_time from ISO format to timestamp."""
    if not val or pd.isna(val):
        return None
    try:
        # e.g. 2017-11-13T17:13:01.000Z
        return pd.to_datetime(str(val).strip())
    except Exception:
        return None

def parse_int(val):
    """Parses numeric field safely to integer."""
    if not val or pd.isna(val):
        return None
    try:
        return int(float(str(val).strip()))
    except Exception:
        return None

def parse_bool(val):
    """Parses boolean field safely."""
    if not val or pd.isna(val):
        return None
    val_str = str(val).strip().lower()
    if val_str in ['true', 't', '1', 'yes']:
        return True
    if val_str in ['false', 'f', '0', 'no']:
        return False
    return None

def process_videos(run_id, engine):
    task_name = "silver_validation"
    step_name = "validate_clean_videos"
    
    print("Validating and cleaning videos...")
    log_audit(run_id, task_name, step_name, "STARTED", 0, "Processing bronze.raw_youtube_videos")
    
    # Idempotency: delete records loaded today from silver.videos before starting
    with engine.begin() as conn:
        conn.execute("DELETE FROM silver.videos WHERE DATE(load_timestamp) = CURRENT_DATE;")
    
    total_valid = 0
    total_rejected = 0
    
    query = "SELECT * FROM bronze.raw_youtube_videos"
    try:
        with engine.connect() as conn:
            # Chunked database read for memory efficiency
            for chunk in pd.read_sql_query(query, conn, chunksize=CHUNK_SIZE):
                valid_rows = []
                rejected_rows = []
                
                # Check for duplicates within this chunk based on video_id + trending_date
                # Note: To be production-grade, we do a primary check
                chunk['dup_check'] = chunk.duplicated(subset=['video_id', 'trending_date'], keep='first')
                
                for idx, row in chunk.iterrows():
                    raw_dict = row.to_dict()
                    # Filter out metadata fields for serialization
                    raw_record_json = json.dumps({k: v for k, v in raw_dict.items() if k not in ['load_timestamp', 'ingestion_date']})
                    
                    # 1. Primary Key Checks
                    video_id = str(row['video_id']).strip() if row['video_id'] else ''
                    trending_date_raw = str(row['trending_date']).strip() if row['trending_date'] else ''
                    
                    if not video_id or video_id == 'nan':
                        rejected_rows.append({
                            "run_id": run_id, "table_name": "raw_youtube_videos",
                            "raw_record_json": raw_record_json, "rejection_reason": "MISSING_VIDEO_ID"
                        })
                        continue
                        
                    if not trending_date_raw or trending_date_raw == 'nan':
                        rejected_rows.append({
                            "run_id": run_id, "table_name": "raw_youtube_videos",
                            "raw_record_json": raw_record_json, "rejection_reason": "MISSING_TRENDING_DATE"
                        })
                        continue
                        
                    # Duplicate check inside chunk
                    if row['dup_check']:
                        rejected_rows.append({
                            "run_id": run_id, "table_name": "raw_youtube_videos",
                            "raw_record_json": raw_record_json, "rejection_reason": "DUPLICATE_VIDEO_ID_AND_TRENDING_DATE"
                        })
                        continue
                        
                    # 2. Category ID Check
                    cat_id_raw = str(row['category_id']).strip() if row['category_id'] else ''
                    cat_id = parse_int(cat_id_raw)
                    if cat_id is None:
                        rejected_rows.append({
                            "run_id": run_id, "table_name": "raw_youtube_videos",
                            "raw_record_json": raw_record_json, "rejection_reason": f"INVALID_CATEGORY_ID_DATATYPE: '{cat_id_raw}'"
                        })
                        continue
                        
                    # 3. Numeric conversions & checks
                    views = parse_int(row['views'])
                    likes = parse_int(row['likes'])
                    dislikes = parse_int(row['dislikes'])
                    comment_count = parse_int(row['comment_count'])
                    
                    if views is None or likes is None or dislikes is None or comment_count is None:
                        rejected_rows.append({
                            "run_id": run_id, "table_name": "raw_youtube_videos",
                            "raw_record_json": raw_record_json, "rejection_reason": "INVALID_METRIC_DATATYPES"
                        })
                        continue
                        
                    # 4. Date validation
                    publish_time = clean_publish_time(row['publish_time'])
                    trending_date = clean_trending_date(row['trending_date'])
                    
                    if not publish_time or not trending_date:
                        rejected_rows.append({
                            "run_id": run_id, "table_name": "raw_youtube_videos",
                            "raw_record_json": raw_record_json, "rejection_reason": "INVALID_DATE_FORMATS"
                        })
                        continue
                        
                    # 5. Clean booleans
                    comm_disabled = parse_bool(row['comments_disabled'])
                    ratings_disabled = parse_bool(row['ratings_disabled'])
                    err_removed = parse_bool(row['video_error_or_removed'])
                    
                    # If all passed, it is valid!
                    valid_rows.append({
                        "video_id": video_id,
                        "trending_date": trending_date,
                        "title": str(row['title']).strip() if row['title'] else '',
                        "channel_title": str(row['channel_title']).strip() if row['channel_title'] else '',
                        "category_id": cat_id,
                        "publish_time": publish_time,
                        "tags": str(row['tags']).strip() if row['tags'] else '',
                        "views": views,
                        "likes": likes,
                        "dislikes": dislikes,
                        "comment_count": comment_count,
                        "comments_disabled": comm_disabled if comm_disabled is not None else False,
                        "ratings_disabled": ratings_disabled if ratings_disabled is not None else False,
                        "video_error_or_removed": err_removed if err_removed is not None else False,
                        "description": str(row['description']).strip() if row['description'] else '',
                        "source_system": row['source_system'],
                        "load_timestamp": datetime.now()
                    })
                    
                # Write valid rows to silver.videos
                if valid_rows:
                    valid_df = pd.DataFrame(valid_rows)
                    valid_df.to_sql(
                        name='videos', con=engine, schema='silver',
                        if_exists='append', index=False, method='multi'
                    )
                    total_valid += len(valid_rows)
                    
                # Write rejected rows to silver.rejected_records
                if rejected_rows:
                    rej_df = pd.DataFrame(rejected_rows)
                    rej_df.to_sql(
                        name='rejected_records', con=engine, schema='silver',
                        if_exists='append', index=False, method='multi'
                    )
                    total_rejected += len(rejected_rows)
                    
        log_audit(run_id, task_name, step_name, "COMPLETED", total_valid, 
                  f"Processed raw_youtube_videos. Valid: {total_valid}, Rejected: {total_rejected}")
        print(f"Processed videos. Valid: {total_valid}, Rejected: {total_rejected}")
        return total_valid, total_rejected
    except Exception as e:
        log_audit(run_id, task_name, step_name, "FAILED", 0, f"Error processing videos: {str(e)}")
        raise e

def process_categories(run_id, engine):
    task_name = "silver_validation"
    step_name = "validate_clean_categories"
    
    print("Validating and cleaning categories...")
    log_audit(run_id, task_name, step_name, "STARTED", 0, "Processing bronze.raw_youtube_categories")
    
    # Clean previous
    with engine.begin() as conn:
        conn.execute("TRUNCATE TABLE silver.categories;")
        
    try:
        df = pd.read_sql("SELECT * FROM bronze.raw_youtube_categories", con=engine)
        valid_rows = []
        rejected_rows = []
        
        for idx, row in df.iterrows():
            raw_record_json = json.dumps({k: v for k, v in row.to_dict().items() if k not in ['load_timestamp']})
            
            cat_id = parse_int(row['category_id'])
            cat_title = str(row['category_title']).strip() if row['category_title'] else ''
            
            if cat_id is None:
                rejected_rows.append({
                    "run_id": run_id, "table_name": "raw_youtube_categories",
                    "raw_record_json": raw_record_json, "rejection_reason": "INVALID_OR_MISSING_CATEGORY_ID"
                })
                continue
                
            if not cat_title:
                rejected_rows.append({
                    "run_id": run_id, "table_name": "raw_youtube_categories",
                    "raw_record_json": raw_record_json, "rejection_reason": "MISSING_CATEGORY_TITLE"
                })
                continue
                
            valid_rows.append({
                "category_id": cat_id,
                "category_title": cat_title,
                "source_system": row['source_system'],
                "load_timestamp": datetime.now()
            })
            
        if valid_rows:
            valid_df = pd.DataFrame(valid_rows)
            valid_df.to_sql(
                name='categories', con=engine, schema='silver',
                if_exists='append', index=False, method='multi'
            )
            
        if rejected_rows:
            rej_df = pd.DataFrame(rejected_rows)
            rej_df.to_sql(
                name='rejected_records', con=engine, schema='silver',
                if_exists='append', index=False, method='multi'
            )
            
        log_audit(run_id, task_name, step_name, "COMPLETED", len(valid_rows), 
                  f"Processed raw_youtube_categories. Valid: {len(valid_rows)}, Rejected: {len(rejected_rows)}")
        print(f"Processed categories. Valid: {len(valid_rows)}, Rejected: {len(rejected_rows)}")
        return len(valid_rows), len(rejected_rows)
    except Exception as e:
        log_audit(run_id, task_name, step_name, "FAILED", 0, f"Error processing categories: {str(e)}")
        raise e

def process_channels(run_id, engine):
    task_name = "silver_validation"
    step_name = "validate_clean_channels"
    
    print("Consolidating, validating and cleaning channels...")
    log_audit(run_id, task_name, step_name, "STARTED", 0, "Processing bronze.raw_channels and raw_channels_extra")
    
    # Clean previous
    with engine.begin() as conn:
        conn.execute("TRUNCATE TABLE silver.channels;")
        
    try:
        # Load raw_channels (XML source) and raw_channels_extra (DB source)
        df_xml = pd.read_sql("SELECT * FROM bronze.raw_channels", con=engine)
        df_db = pd.read_sql("SELECT * FROM bronze.raw_channels_extra", con=engine)
        
        # We will consolidate them on channel_id by joining them
        # Clean channel_id for keys
        df_xml['channel_id'] = df_xml['channel_id'].str.strip()
        df_db['channel_id'] = df_db['channel_id'].str.strip()
        
        # Merge on channel_id
        df_merged = pd.merge(df_xml, df_db, on='channel_id', how='outer', suffixes=('_xml', '_db'))
        
        valid_rows = []
        rejected_rows = []
        
        for idx, row in df_merged.iterrows():
            raw_record_json = json.dumps({k: str(v) for k, v in row.to_dict().items() if not pd.isna(v)})
            
            chan_id = row['channel_id']
            if not chan_id or chan_id == 'nan':
                rejected_rows.append({
                    "run_id": run_id, "table_name": "consolidated_channels",
                    "raw_record_json": raw_record_json, "rejection_reason": "MISSING_CHANNEL_ID"
                })
                continue
                
            # Casing and Title extraction: prefer xml, fallback to db title
            title = row['channel_title_xml'] if not pd.isna(row['channel_title_xml']) else row['channel_title_db']
            title = str(title).strip() if not pd.isna(title) else ''
            
            if not title:
                rejected_rows.append({
                    "run_id": run_id, "table_name": "consolidated_channels",
                    "raw_record_json": raw_record_json, "rejection_reason": "MISSING_CHANNEL_TITLE"
                })
                continue
                
            sub_count = parse_int(row['subscriber_count'])
            tot_vids = parse_int(row['total_videos'])
            tot_views = parse_int(row['total_views'])
            country = str(row['country']).strip() if not pd.isna(row['country']) else 'Unknown'
            
            valid_rows.append({
                "channel_id": chan_id,
                "channel_title": title,
                "subscriber_count": sub_count,
                "country": country,
                "total_videos": tot_vids,
                "total_views": tot_views,
                "source_system": "channels_consolidated",
                "load_timestamp": datetime.now()
            })
            
        if valid_rows:
            valid_df = pd.DataFrame(valid_rows)
            valid_df.to_sql(
                name='channels', con=engine, schema='silver',
                if_exists='append', index=False, method='multi'
            )
            
        if rejected_rows:
            rej_df = pd.DataFrame(rejected_rows)
            rej_df.to_sql(
                name='rejected_records', con=engine, schema='silver',
                if_exists='append', index=False, method='multi'
            )
            
        log_audit(run_id, task_name, step_name, "COMPLETED", len(valid_rows), 
                  f"Processed channels. Valid: {len(valid_rows)}, Rejected: {len(rejected_rows)}")
        print(f"Processed channels. Valid: {len(valid_rows)}, Rejected: {len(rejected_rows)}")
        return len(valid_rows), len(rejected_rows)
    except Exception as e:
        log_audit(run_id, task_name, step_name, "FAILED", 0, f"Error processing channels: {str(e)}")
        raise e

def run_pipeline(run_id):
    engine = get_db_engine()
    
    # Process each entity
    v_val, v_rej = process_videos(run_id, engine)
    c_val, c_rej = process_categories(run_id, engine)
    ch_val, ch_rej = process_channels(run_id, engine)
    
    total_val = v_val + c_val + ch_val
    total_rej = v_rej + c_rej + ch_rej
    
    # Update global run details
    from pipeline_config.config import complete_pipeline_run
    # We will complete this step but let the DAG handle final completion, 
    # however updating counts loaded is a good practice.
    print(f"Silver processing done. Total Valid: {total_val}, Total Rejected: {total_rej}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    run_pipeline(args.run_id)
