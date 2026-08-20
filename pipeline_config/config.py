import os
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime

# Check for single database connection URL (e.g., Neon or Render DATABASE_URL)
db_url_env = os.getenv("DATABASE_URL")
if db_url_env:
    CONN_STR_PSYCOPG = db_url_env
    if db_url_env.startswith("postgres://"):
        CONN_STR_SQLALCHEMY = db_url_env.replace("postgres://", "postgresql+psycopg2://", 1)
    elif db_url_env.startswith("postgresql://") and not db_url_env.startswith("postgresql+psycopg2://"):
        CONN_STR_SQLALCHEMY = db_url_env.replace("postgresql://", "postgresql+psycopg2://", 1)
    else:
        CONN_STR_SQLALCHEMY = db_url_env
else:
    # Database connection parameters
    DB_USER = os.getenv("POSTGRES_USER", "postgres")
    DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres_secure_pass")
    DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
    DB_PORT = os.getenv("POSTGRES_PORT", "5432")
    DB_NAME = os.getenv("POSTGRES_DB_DW", "data_warehouse")

    # Connection strings
    CONN_STR_PSYCOPG = f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}"
    CONN_STR_SQLALCHEMY = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Schema names
SCHEMAS = {
    "bronze": "bronze",
    "silver": "silver",
    "gold": "gold",
    "metadata": "metadata"
}

# Source configurations
DATA_DIR = "/opt/airflow/data"
YOUTUBE_DATASET_DIR = "/opt/airflow/YOUTUBE DATSET"
CSV_FILE_PATH = os.path.join(YOUTUBE_DATASET_DIR, "USvideos.csv")
JSON_FILE_PATH = os.path.join(YOUTUBE_DATASET_DIR, "US_category_id.json")
XML_FILE_PATH = os.path.join(DATA_DIR, "channels.xml")

# Processing configurations
CHUNK_SIZE = 10000  # Safe chunk size for large CSV file processing

def get_db_connection():
    """Returns a psycopg2 database connection."""
    return psycopg2.connect(CONN_STR_PSYCOPG)

def get_db_engine():
    """Returns a sqlalchemy engine."""
    return create_engine(CONN_STR_SQLALCHEMY)

def log_audit(run_id, task_name, step_name, status, affected_rows=0, log_message=None):
    """Helper to log execution details into metadata.audit_log."""
    query = """
        INSERT INTO metadata.audit_log (run_id, task_name, step_name, status, affected_rows, log_message, log_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, (run_id, task_name, step_name, status, affected_rows, log_message, datetime.now()))
        conn.commit()
    except Exception as e:
        print(f"Failed to log audit details: {e}")
    finally:
        conn.close()

def start_pipeline_run(run_id, pipeline_name, execution_date):
    """Helper to record the start of a pipeline run in metadata.pipeline_runs."""
    query = """
        INSERT INTO metadata.pipeline_runs (run_id, pipeline_name, execution_date, status, start_time)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (run_id) DO UPDATE 
        SET status = EXCLUDED.status, start_time = EXCLUDED.start_time;
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, (run_id, pipeline_name, execution_date, "RUNNING", datetime.now()))
        conn.commit()
    except Exception as e:
        print(f"Failed to start pipeline run: {e}")
    finally:
        conn.close()

def complete_pipeline_run(run_id, status, records_inserted=0, records_updated=0, records_rejected=0):
    """Helper to record the completion of a pipeline run."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # First fetch start_time to calculate duration
            cur.execute("SELECT start_time FROM metadata.pipeline_runs WHERE run_id = %s", (run_id,))
            row = cur.fetchone()
            if row:
                start_time = row[0]
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                
                query = """
                    UPDATE metadata.pipeline_runs 
                    SET status = %s, end_time = %s, duration_seconds = %s,
                        records_inserted = records_inserted + %s, 
                        records_updated = records_updated + %s,
                        records_rejected = records_rejected + %s
                    WHERE run_id = %s
                """
                cur.execute(query, (status, end_time, duration, records_inserted, records_updated, records_rejected, run_id))
        conn.commit()
    except Exception as e:
        print(f"Failed to complete pipeline run: {e}")
    finally:
        conn.close()
