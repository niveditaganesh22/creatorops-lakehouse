-- =========================================================
-- CreatorOps Lakehouse
-- Bronze Layer Definition
-- =========================================================

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.creatorops_events_raw (
    ingest_id STRING,
    source STRING,
    raw_json STRING,
    raw_schema_version INT,
    ingested_at TIMESTAMP,
    p_ingest_date DATE
)
USING DELTA
PARTITIONED BY (p_ingest_date)
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = true,
    delta.enableChangeDataFeed = false
);

-- Recommended maintenance (manual / scheduled)
-- OPTIMIZE bronze.creatorops_events_raw;
-- VACUUM bronze.creatorops_events_raw RETAIN 168 HOURS;