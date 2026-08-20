# Production-Style Medallion Architecture Data Pipeline PoC
This project implements a complete, end-to-end data engineering batch ETL pipeline utilizing the Medallion Architecture (Bronze → Silver → Gold) to process and analyze YouTube video trending logs. It is orchestrated via **Apache Airflow** (LocalExecutor) and fully containerized with **Docker Compose**, using **PostgreSQL** as the underlying data warehouse.

---

## 1. Tech Stack & Architecture

- **Orchestration**: Apache Airflow 2.7.1 (using `LocalExecutor` for concurrent tasks)
- **Database**: PostgreSQL 15 (two databases: `airflow` for orchestration metadata, and `data_warehouse` with schemas for `bronze`, `silver`, `gold`, and `metadata`)
- **Processing**: Python 3.8 (pandas, sqlalchemy, psycopg2-binary, and standard library XML parsing)
- **Source Systems**: 
  - **CSV**: `USvideos.csv` containing video attributes and metrics (chunked reads)
  - **JSON**: `US_category_id.json` containing categories
  - **XML**: `channels.xml` (mocked and dynamically generated for channel details)
  - **PostgreSQL**: `external_source` schema (mocked for incremental DB loads)

### Flow Diagram

```mermaid
graph TD
    subgraph Source Layer
        CSV["CSV (USvideos.csv)"]
        JSON["JSON (US_category_id.json)"]
        XML["XML (channels.xml)"]
        PG_SRC["PG Source (Channel Seed Data)"]
    end

    subgraph ETL Orchestration (Airflow Tasks)
        CSV -->|Ingestion Chunked| B_Vid["bronze.raw_youtube_videos"]
        JSON -->|Ingestion Full| B_Cat["bronze.raw_youtube_categories"]
        XML -->|Ingestion XML| B_Chan["bronze.raw_channels"]
        PG_SRC -->|Ingestion Incremental| B_Chan_Extra["bronze.raw_channels_extra"]

        subgraph Validation Gate
            B_Vid & B_Cat & B_Chan & B_Chan_Extra --> V_Check{"Data Validation"}
            V_Check -->|Fail| R_Rec["silver.rejected_records"]
            V_Check -->|Pass| S_Clean["Silver Layer (Cleansing & Standardization)"]
        end

        subgraph Silver Layer
            S_Clean --> S_Vid["silver.videos"]
            S_Clean --> S_Cat["silver.categories"]
            S_Clean --> S_Chan["silver.channels"]
        end

        subgraph Gold Layer
            S_Vid --> G_DimVid["gold.dim_video (SCD2)"]
            S_Cat --> G_DimCat["gold.dim_category (SCD1)"]
            S_Chan --> G_DimChan["gold.dim_channel (SCD2)"]
            
            G_DimVid & G_DimCat & G_DimChan --> G_Fact["gold.fact_video_metrics"]
            G_Fact --> G_Mart1["gold.channel_performance_mart"]
            G_Fact --> G_Mart2["gold.trending_kpi_summary_mart"]
            G_Fact --> G_Mart3["gold.trending_trends_mart"]
        end
    end

    subgraph Metadata & Audit
        Airflow_DAG -->|Log Runs| M_Run["metadata.pipeline_runs"]
        Airflow_Tasks -->|Audit Steps| M_Audit["metadata.audit_log"]
    end
```

---

## 2. Medallion Layer Design

### A. Bronze Schema (`bronze`)
- **Goal**: Raw, immutable, schema-free ingestion of data. 
- **Tables**:
  - `raw_youtube_videos`: Houses chunked CSV load.
  - `raw_youtube_categories`: Houses parsed JSON load.
  - `raw_channels`: Houses parsed XML channel load.
  - `raw_channels_extra`: Houses incremental PostgreSQL source load.
- **Attributes**: Every table captures `source_system` and `load_timestamp`. No transformations or cleansing occur.

### B. Silver Schema (`silver`)
- **Goal**: Data validation, cleansing, standardization, and consolidation.
- **Validation Rules**:
  - Null check on primary keys (`video_id`, `category_id`, `channel_id`).
  - Data type validation (e.g., verification that metrics like `views` are valid integers).
  - Duplicate detection (retaining only the latest unique records per day and routing duplicates to rejected records).
  - Schema correctness and ISO date format conversion.
- **Error Routing**: Dirty/invalid rows are routed to the `silver.rejected_records` table alongside their JSON payload and the specific rejection reason.
- **Cleansing**: Trimming whitespace, standardizing boolean columns, converting `trending_date` from `YY.DD.MM` to standard date format `YYYY-MM-DD`.
- **Consolidation**: Joining XML channels and database stats into a unified `silver.channels` dataset.

### C. Gold Schema (`gold` - Star Schema)
- **Goal**: BI-ready, high-value, dimensional modeling.
- **Slowly Changing Dimensions**:
  - **SCD Type 1**: `dim_category` (overwrites names when changes occur).
  - **SCD Type 2**: `dim_video` and `dim_channel` (tracks historical changes to attributes like video titles, tags, or subscriber counts with columns `version_number`, `effective_start_date`, `effective_end_date`, and `is_current`).
- **Fact Table**: `fact_video_metrics` mapping metrics (`views`, `likes`, etc.) to active dimension keys.
- **Marts**:
  - `channel_performance_mart`: Channel KPIs (views, total likes, averages).
  - `trending_kpi_summary_mart`: Daily KPI performance leveraging ranking window functions to determine the top category.
  - `trending_trends_mart`: Analytics using window functions like cumulative sum (`SUM() OVER`) and day-over-day changes (`LAG`).

---

## 3. Metadata & Audit Framework
- **`metadata.pipeline_runs`**: Records pipeline name, execution date, start/end times, execution status (`RUNNING`, `SUCCESS`, `FAILED`), duration in seconds, and records modified (inserted/updated/rejected).
- **`metadata.audit_log`**: Detailed step-by-step logs for individual tasks detailing starting/finishing states, execution summaries, and affected row counts.

---

## 4. Setup & Running the Pipeline

### Prerequisites
- **Docker Desktop** installed with WSL2 backend.
- Local repository containing `YOUTUBE DATSET` containing `USvideos.csv` and `US_category_id.json`.

### Steps to Run

1. **Clone and Navigate to the Directory**:
   Ensure you are in the workspace root:
   ```powershell
   cd "c:\Users\hp\OneDrive\Desktop\YOUTUBE MEDALLION POC PROJECT"
   ```

2. **Spin Up the Containers**:
   Execute the following command to build and launch the containers:
   ```powershell
   docker compose up --build -d
   ```
   *Note: This starts PostgreSQL, initializes database schemas, and triggers the Airflow scheduler/webserver.*

3. **Verify running containers**:
   ```powershell
   docker compose ps
   ```

4. **Access the Airflow UI**:
   - Open your browser and go to `http://localhost:8080`
   - Log in using credentials: Username `admin` / Password `admin`

5. **Trigger the DAG**:
   - Unpause the DAG `youtube_medallion_etl_pipeline` and click **Trigger DAG**.
   - Watch the DAG run successfully through all stages.

---

## 5. Verification Queries

To inspect each layer and verify the data flow, log into the PostgreSQL container:
```powershell
docker exec -it youtube_medallion_poc_project-postgres-1 psql -U postgres -d data_warehouse
```

### Ingestion Verification (Bronze)
```sql
SELECT source_system, COUNT(*) FROM bronze.raw_youtube_videos GROUP BY source_system;
SELECT COUNT(*) FROM bronze.raw_youtube_categories;
SELECT COUNT(*) FROM bronze.raw_channels;
SELECT COUNT(*) FROM bronze.raw_channels_extra;
```

### Rejection & Quality Verification (Silver)
Verify the rejected records and see why they failed validation:
```sql
SELECT table_name, rejection_reason, count(*) 
FROM silver.rejected_records 
GROUP BY table_name, rejection_reason;
```
*Note: You will see records failed with `MISSING_VIDEO_ID`, `INVALID_DATE_FORMATS`, and `INVALID_METRIC_DATATYPES` injected by the test task.*

### SCD Type 2 Verification (Gold)
If a video title or tag updates, the pipeline expires the old version and inserts a new one. Verify this via:
```sql
SELECT video_id, title, version_number, effective_start_date, effective_end_date, is_current 
FROM gold.dim_video 
WHERE video_id IN ('2kyS6SvSYSE', '1ZAPwfrtAFY') 
ORDER BY video_id, version_number;
```

### Mart & Analytics Verification (Gold)
Verify the Daily KPI Summary and Day-over-Day View Trends calculated using window functions:
```sql
-- Day-over-day View change using LAG
SELECT trending_date, total_views, running_total_views, previous_day_views, pct_change_day_over_day 
FROM gold.trending_trends_mart 
ORDER BY trending_date 
LIMIT 10;
```

### Audit Log Verification (Metadata)
```sql
SELECT run_id, pipeline_name, status, duration_seconds FROM metadata.pipeline_runs;
SELECT task_name, step_name, status, affected_rows, log_message FROM metadata.audit_log ORDER BY log_id;
```
