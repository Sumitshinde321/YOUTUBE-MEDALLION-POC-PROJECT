-- Create schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS metadata;

-- ============================================================================
-- METADATA SCHEMA TABLES
-- ============================================================================

CREATE TABLE IF NOT EXISTS metadata.pipeline_runs (
    run_id VARCHAR(100) PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    execution_date TIMESTAMP NOT NULL,
    status VARCHAR(50) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    duration_seconds DECIMAL(10,2),
    records_inserted INT DEFAULT 0,
    records_updated INT DEFAULT 0,
    records_rejected INT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS metadata.audit_log (
    log_id SERIAL PRIMARY KEY,
    run_id VARCHAR(100) NOT NULL,
    task_name VARCHAR(100) NOT NULL,
    step_name VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL,
    affected_rows INT DEFAULT 0,
    log_message TEXT,
    log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- BRONZE SCHEMA TABLES (Immutable Raw Capture)
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze.raw_youtube_videos (
    video_id VARCHAR(255),
    trending_date VARCHAR(50),
    title TEXT,
    channel_title TEXT,
    category_id VARCHAR(50),
    publish_time VARCHAR(100),
    tags TEXT,
    views VARCHAR(50),
    likes VARCHAR(50),
    dislikes VARCHAR(50),
    comment_count VARCHAR(50),
    thumbnail_link TEXT,
    comments_disabled VARCHAR(50),
    ratings_disabled VARCHAR(50),
    video_error_or_removed VARCHAR(50),
    description TEXT,
    -- Metadata columns
    source_system VARCHAR(100) NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ingestion_date DATE DEFAULT CURRENT_DATE
);

CREATE TABLE IF NOT EXISTS bronze.raw_youtube_categories (
    category_id VARCHAR(50),
    category_title VARCHAR(255),
    -- Metadata columns
    source_system VARCHAR(100) NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.raw_channels (
    channel_id VARCHAR(255),
    channel_title VARCHAR(255),
    subscriber_count VARCHAR(50),
    country VARCHAR(100),
    last_updated VARCHAR(100),
    -- Metadata columns
    source_system VARCHAR(100) NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.raw_channels_extra (
    channel_id VARCHAR(255),
    channel_title VARCHAR(255),
    total_videos VARCHAR(50),
    total_views VARCHAR(50),
    last_updated VARCHAR(100),
    -- Metadata columns
    source_system VARCHAR(100) NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- SILVER SCHEMA TABLES (Cleansed & Standardized)
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver.rejected_records (
    rejected_id SERIAL PRIMARY KEY,
    run_id VARCHAR(100),
    table_name VARCHAR(100) NOT NULL,
    raw_record_json TEXT NOT NULL,
    rejection_reason VARCHAR(255) NOT NULL,
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.videos (
    video_id VARCHAR(50) NOT NULL,
    trending_date DATE NOT NULL,
    title TEXT,
    channel_title TEXT,
    category_id INT NOT NULL,
    publish_time TIMESTAMP,
    tags TEXT,
    views BIGINT,
    likes BIGINT,
    dislikes BIGINT,
    comment_count BIGINT,
    comments_disabled BOOLEAN,
    ratings_disabled BOOLEAN,
    video_error_or_removed BOOLEAN,
    description TEXT,
    source_system VARCHAR(100) NOT NULL,
    load_timestamp TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.categories (
    category_id INT PRIMARY KEY,
    category_title VARCHAR(255) NOT NULL,
    source_system VARCHAR(100) NOT NULL,
    load_timestamp TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.channels (
    channel_id VARCHAR(50) PRIMARY KEY,
    channel_title VARCHAR(255) NOT NULL,
    subscriber_count BIGINT,
    country VARCHAR(100),
    total_videos INT,
    total_views BIGINT,
    source_system VARCHAR(100) NOT NULL,
    load_timestamp TIMESTAMP NOT NULL
);

-- ============================================================================
-- GOLD SCHEMA TABLES (Star Schema & Business Marts)
-- ============================================================================

-- SCD Type 2 Dimension for Videos
CREATE TABLE IF NOT EXISTS gold.dim_video (
    dim_video_key SERIAL PRIMARY KEY,
    video_id VARCHAR(50) NOT NULL,
    title TEXT,
    channel_title TEXT,
    category_id INT,
    publish_time TIMESTAMP,
    tags TEXT,
    comments_disabled BOOLEAN,
    ratings_disabled BOOLEAN,
    video_error_or_removed BOOLEAN,
    description TEXT,
    -- SCD Type 2 columns
    version_number INT NOT NULL,
    effective_start_date TIMESTAMP NOT NULL,
    effective_end_date TIMESTAMP,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

-- Index for fast key lookups during SCD checks and queries
CREATE INDEX IF NOT EXISTS idx_dim_video_lookup ON gold.dim_video(video_id, is_current);

-- SCD Type 1 Dimension for Categories (Overwrites)
CREATE TABLE IF NOT EXISTS gold.dim_category (
    category_id INT PRIMARY KEY,
    category_title VARCHAR(255) NOT NULL,
    load_timestamp TIMESTAMP NOT NULL
);

-- SCD Type 2 Dimension for Channels
CREATE TABLE IF NOT EXISTS gold.dim_channel (
    dim_channel_key SERIAL PRIMARY KEY,
    channel_id VARCHAR(50) NOT NULL,
    channel_title VARCHAR(255),
    subscriber_count BIGINT,
    country VARCHAR(100),
    total_videos INT,
    total_views BIGINT,
    -- SCD Type 2 columns
    version_number INT NOT NULL,
    effective_start_date TIMESTAMP NOT NULL,
    effective_end_date TIMESTAMP,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_dim_channel_lookup ON gold.dim_channel(channel_id, is_current);

-- Transactional / Daily Snapshot Fact Table
CREATE TABLE IF NOT EXISTS gold.fact_video_metrics (
    fact_key SERIAL PRIMARY KEY,
    video_id VARCHAR(50) NOT NULL,
    trending_date DATE NOT NULL,
    dim_video_key INT NOT NULL,
    dim_category_id INT NOT NULL,
    dim_channel_key INT NOT NULL,
    views BIGINT NOT NULL,
    likes BIGINT NOT NULL,
    dislikes BIGINT NOT NULL,
    comment_count BIGINT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fact_video_date ON gold.fact_video_metrics(trending_date);

-- Channel Performance Mart (Aggregated View)
CREATE TABLE IF NOT EXISTS gold.channel_performance_mart (
    channel_title VARCHAR(255) PRIMARY KEY,
    total_trending_videos INT NOT NULL,
    total_views BIGINT NOT NULL,
    total_likes BIGINT NOT NULL,
    avg_likes_per_video DECIMAL(18,2),
    load_timestamp TIMESTAMP NOT NULL
);

-- Daily Trending KPI Summary Mart (Aggregation and Window functions)
CREATE TABLE IF NOT EXISTS gold.trending_kpi_summary_mart (
    trending_date DATE PRIMARY KEY,
    total_trending_videos INT NOT NULL,
    total_views BIGINT NOT NULL,
    total_likes BIGINT NOT NULL,
    most_trending_category VARCHAR(255),
    load_timestamp TIMESTAMP NOT NULL
);

-- Daily/Weekly Trends Mart utilizing Running Totals, Rank, and Lag/Lead Window Functions
CREATE TABLE IF NOT EXISTS gold.trending_trends_mart (
    trending_date DATE PRIMARY KEY,
    total_views BIGINT NOT NULL,
    running_total_views BIGINT NOT NULL,
    previous_day_views BIGINT,
    pct_change_day_over_day DECIMAL(10,2),
    load_timestamp TIMESTAMP NOT NULL
);
