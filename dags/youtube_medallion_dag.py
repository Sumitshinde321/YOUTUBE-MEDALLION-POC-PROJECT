import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Adjust Python path to load config
sys.path.append('/opt/airflow')
from pipeline_config.config import get_db_connection, start_pipeline_run, complete_pipeline_run, log_audit

# Define success and failure callbacks for the DAG
def on_dag_success(context):
    run_id = context['run_id']
    complete_pipeline_run(run_id, "SUCCESS")
    log_audit(run_id, "dag_lifecycle", "dag_success", "COMPLETED", 0, "DAG Run completed successfully.")

def on_dag_failure(context):
    run_id = context['run_id']
    complete_pipeline_run(run_id, "FAILED")
    log_audit(run_id, "dag_lifecycle", "dag_failure", "FAILED", 0, "DAG Run failed.")

# Python tasks in DAG
def initialize_schemas(run_id, **context):
    log_audit(run_id, "db_initialization", "run_sql", "STARTED", 0, "Executing DDL schema initialization")
    sql_path = "/opt/airflow/sql/init_schemas.sql"
    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"SQL file not found at {sql_path}")
        
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            with open(sql_path, 'r') as f:
                cur.execute(f.read())
        conn.commit()
        log_audit(run_id, "db_initialization", "run_sql", "COMPLETED", 0, "DDL Schema initialization successful")
    except Exception as e:
        conn.rollback()
        log_audit(run_id, "db_initialization", "run_sql", "FAILED", 0, f"Error initializing schemas: {str(e)}")
        raise e
    finally:
        conn.close()

def start_run(run_id, **context):
    # Airflow 2.0+ logical_date represents the execution date
    logical_date = context.get('logical_date', datetime.now())
    start_pipeline_run(run_id, "youtube_medallion_etl", logical_date)
    log_audit(run_id, "dag_lifecycle", "dag_start", "COMPLETED", 0, f"Started pipeline run at {logical_date}")

def inject_dirty_rows(run_id, **context):
    log_audit(run_id, "test_data_injection", "inject_dirty", "STARTED", 0, "Injecting test dirty records into Bronze raw_youtube_videos")
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # 1. Null Video ID
            cur.execute("""
                INSERT INTO bronze.raw_youtube_videos (video_id, trending_date, title, channel_title, category_id, publish_time, tags, views, likes, dislikes, comment_count, source_system, ingestion_date)
                VALUES (NULL, '17.14.11', 'Dirty Title Null ID', 'Dirty Channel', '22', '2017-11-13T17:13:01.000Z', 'tag', '100', '10', '1', '0', 'dirty_injector', CURRENT_DATE);
            """)
            # 2. Corrupt Date format
            cur.execute("""
                INSERT INTO bronze.raw_youtube_videos (video_id, trending_date, title, channel_title, category_id, publish_time, tags, views, likes, dislikes, comment_count, source_system, ingestion_date)
                VALUES ('dirty_vid_01', 'corrupted_date', 'Dirty Title Bad Date', 'Dirty Channel', '22', '2017-11-13T17:13:01.000Z', 'tag', '100', '10', '1', '0', 'dirty_injector', CURRENT_DATE);
            """)
            # 3. Bad views data type
            cur.execute("""
                INSERT INTO bronze.raw_youtube_videos (video_id, trending_date, title, channel_title, category_id, publish_time, tags, views, likes, dislikes, comment_count, source_system, ingestion_date)
                VALUES ('dirty_vid_02', '17.14.11', 'Dirty Title Bad Views', 'Dirty Channel', '22', '2017-11-13T17:13:01.000Z', 'tag', 'not_a_number', '10', '1', '0', 'dirty_injector', CURRENT_DATE);
            """)
            # 4. Bad Category ID type
            cur.execute("""
                INSERT INTO bronze.raw_youtube_videos (video_id, trending_date, title, channel_title, category_id, publish_time, tags, views, likes, dislikes, comment_count, source_system, ingestion_date)
                VALUES ('dirty_vid_03', '17.14.11', 'Dirty Title Bad Cat', 'Dirty Channel', 'not_int_cat', '2017-11-13T17:13:01.000Z', 'tag', '100', '10', '1', '0', 'dirty_injector', CURRENT_DATE);
            """)
        conn.commit()
        log_audit(run_id, "test_data_injection", "inject_dirty", "COMPLETED", 4, "Successfully injected 4 dirty records for testing validation flow")
        print("Successfully injected 4 dirty records for testing.")
    except Exception as e:
        conn.rollback()
        log_audit(run_id, "test_data_injection", "inject_dirty", "FAILED", 0, f"Error injecting dirty records: {str(e)}")
        raise e
    finally:
        conn.close()

# Default DAG arguments
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'youtube_medallion_etl_pipeline',
    default_args=default_args,
    description='PoC Medallion Architecture ETL Pipeline for YouTube Data (Bronze -> Silver -> Gold)',
    schedule_interval=None,  # Manual trigger for PoC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    on_success_callback=on_dag_success,
    on_failure_callback=on_dag_failure,
    tags=['youtube', 'medallion', 'etl'],
) as dag:

    # 1. Initialize schemas
    t_init_schemas = PythonOperator(
        task_id='initialize_schemas',
        python_callable=initialize_schemas,
        op_kwargs={'run_id': '{{ run_id }}'},
    )

    # 2. Record start run
    t_start_run = PythonOperator(
        task_id='start_pipeline_run',
        python_callable=start_run,
        op_kwargs={'run_id': '{{ run_id }}'},
    )

    # 3. Bronze Ingestion Tasks (Parallel)
    t_ingest_csv = BashOperator(
        task_id='ingest_csv_to_bronze',
        bash_command='python /opt/airflow/scripts/bronze/ingest_csv.py --run-id "{{ run_id }}"',
    )

    t_ingest_json = BashOperator(
        task_id='ingest_json_to_bronze',
        bash_command='python /opt/airflow/scripts/bronze/ingest_json.py --run-id "{{ run_id }}"',
    )

    t_ingest_xml = BashOperator(
        task_id='ingest_xml_to_bronze',
        bash_command='python /opt/airflow/scripts/bronze/ingest_xml.py --run-id "{{ run_id }}"',
    )

    t_ingest_db = BashOperator(
        task_id='ingest_db_to_bronze',
        bash_command='python /opt/airflow/scripts/bronze/ingest_db.py --run-id "{{ run_id }}"',
    )

    # 4. Inject Dirty Data for quality check validation testing
    t_inject_dirty = PythonOperator(
        task_id='inject_dirty_testing_data',
        python_callable=inject_dirty_rows,
        op_kwargs={'run_id': '{{ run_id }}'},
    )

    # 5. Silver Validation and Cleansing Task
    t_silver_clean = BashOperator(
        task_id='validate_and_clean_to_silver',
        bash_command='python /opt/airflow/scripts/silver/validate_and_clean.py --run-id "{{ run_id }}"',
    )

    # 6. SCD Processing Tasks (Silver -> Gold dimensions)
    t_scd_processing = BashOperator(
        task_id='process_scd_dimensions',
        bash_command='python /opt/airflow/scripts/silver/scd_handlers.py --run-id "{{ run_id }}"',
    )

    # 7. Gold Transformations and Marts Building Task
    t_gold_marts = BashOperator(
        task_id='build_gold_facts_and_marts',
        bash_command='python /opt/airflow/scripts/gold/build_marts.py --run-id "{{ run_id }}"',
    )

    # Task Dependencies
    t_init_schemas >> t_start_run
    t_start_run >> [t_ingest_csv, t_ingest_json, t_ingest_xml, t_ingest_db]
    [t_ingest_csv, t_ingest_json, t_ingest_xml, t_ingest_db] >> t_inject_dirty
    t_inject_dirty >> t_silver_clean
    t_silver_clean >> t_scd_processing
    t_scd_processing >> t_gold_marts
