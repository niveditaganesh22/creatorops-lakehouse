-- =========================================================
-- CreatorOps Lakehouse
-- Silver Canonical Events Table
-- =========================================================

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.creatorops_events (

    -- Identity
    event_id STRING,
    event_type STRING,
    event_version INT,

    -- Time
    occurred_at TIMESTAMP,
    ingested_at TIMESTAMP,
    p_event_date DATE,

    -- Multi-tenancy
    tenant_id STRING,
    author_id STRING,
    plan STRING,

    -- Entities
    series_id STRING,
    story_id STRING,
    chapter_id STRING,
    scene_id STRING,

    -- Workflow
    stage STRING,

    -- Metrics
    revision_count INT,
    word_count INT,
    engagement_score DOUBLE,

    -- Metadata
    metadata_map MAP<STRING, STRING>,

    -- Producer traceability
    producer_service STRING,
    producer_env STRING,
    producer_region STRING,
    trace_id STRING,

    -- Derived / system fields
    event_hash STRING,
    is_late_event BOOLEAN

)
USING DELTA
PARTITIONED BY (p_event_date)
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = true,
    delta.enableChangeDataFeed = false
);

-- Suggested performance strategy (manual)
-- OPTIMIZE silver.creatorops_events ZORDER BY (story_id, author_id, event_type);