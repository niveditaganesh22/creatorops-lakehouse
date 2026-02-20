-- =========================================================
-- CreatorOps Lakehouse
-- Silver Rejects Table (Quarantine)
-- =========================================================

CREATE TABLE IF NOT EXISTS silver.creatorops_events_rejects (

    -- Raw context
    ingest_id STRING,
    raw_json STRING,

    -- Failure context
    reject_reason STRING,
    reject_stage STRING,        -- validation | parsing | enum_check | metric_check
    validation_errors STRING,   -- JSON string of error details

    -- Metadata
    rejected_at TIMESTAMP,
    p_reject_date DATE

)
USING DELTA
PARTITIONED BY (p_reject_date)
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = true,
    delta.enableChangeDataFeed = false
);