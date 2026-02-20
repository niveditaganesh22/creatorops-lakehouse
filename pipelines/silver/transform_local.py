import os
import hashlib
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType, MapType
)
from delta import configure_spark_with_delta_pip

DELTA_ROOT = os.path.join("out", "delta")
BRONZE_PATH = os.path.join(DELTA_ROOT, "bronze", "creatorops_events_raw")
SILVER_EVENTS_PATH = os.path.join(DELTA_ROOT, "silver", "creatorops_events")
SILVER_REJECTS_PATH = os.path.join(DELTA_ROOT, "silver", "creatorops_rejects")


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("creatorops-silver-transform-local")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.io.native.lib.available", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def sha256_str(col):
    return F.sha2(F.coalesce(col, F.lit("")), 256)


def main():
    spark = build_spark()

    bronze = spark.read.format("delta").load(BRONZE_PATH)

    # --- Canonical schema (matches your generated JSON structure) ---
    schema = StructType([
        StructField("eventId", StringType(), True),
        StructField("eventType", StringType(), True),
        StructField("eventVersion", IntegerType(), True),
        StructField("occurredAt", StringType(), True),  # parse to timestamp below
        StructField("stage", StringType(), True),

        StructField("tenant", StructType([
            StructField("tenantId", StringType(), True),
            StructField("authorId", StringType(), True),
            StructField("plan", StringType(), True),
        ]), True),

        StructField("entity", StructType([
            StructField("storyId", StringType(), True),
            StructField("seriesId", StringType(), True),
            StructField("chapterId", StringType(), True),
            StructField("sceneId", StringType(), True),
        ]), True),

        StructField("producer", StructType([
            StructField("service", StringType(), True),
            StructField("env", StringType(), True),
            StructField("region", StringType(), True),
            StructField("traceId", StringType(), True),
        ]), True),

        # metrics vary by event type; keep as map<string,string> for portability
        StructField("metrics", MapType(StringType(), StringType()), True),

        # metadata JSON-ish (persona etc). Keep as map too.
        StructField("metadata", MapType(StringType(), StringType()), True),
    ])

    parsed = (
        bronze
        .withColumn("parsed", F.from_json(F.col("raw_json"), schema))
        .withColumn("parse_ok", F.col("parsed").isNotNull())
        .withColumn("occurred_at", F.to_timestamp(F.col("parsed.occurredAt")))  # ISO-8601 Z parses fine
        .withColumn("p_event_date", F.to_date(F.col("occurred_at")))
    )

    # --- Contract checks (minimal but real) ---
    allowed_event_types = [
        "draft_created",
        "chapter_written",
        "scene_revised",
        "beta_feedback_received",
        "submission_sent",
        "editor_comment",
        "publish_scheduled",
        "publish_released",
        "reader_engagement",
        "reader_dropoff",
    ]

    required_ok = (
        (F.col("parsed.eventId").isNotNull()) &
        (F.col("parsed.eventType").isNotNull()) &
        (F.col("parsed.eventVersion").isNotNull()) &
        (F.col("occurred_at").isNotNull()) &
        (F.col("parsed.tenant.tenantId").isNotNull()) &
        (F.col("parsed.tenant.authorId").isNotNull()) &
        (F.col("parsed.entity.storyId").isNotNull())
    )

    domain_ok = (
        (F.col("parsed.eventVersion") >= F.lit(1)) &
        (F.col("parsed.eventType").isin(allowed_event_types))
    )

    # Late event flag: occurredAt older than 7 days compared to ingestion date
    is_late = F.datediff(F.col("p_ingest_date"), F.col("p_event_date")) > F.lit(7)

    # Event hash for idempotency
    event_hash = sha256_str(F.col("raw_json"))

    good = (
        parsed
        .where(F.col("parse_ok") & required_ok & domain_ok)
        .select(
            F.col("parsed.eventId").alias("event_id"),
            F.col("parsed.eventType").alias("event_type"),
            F.col("parsed.eventVersion").alias("event_version"),
            F.col("occurred_at"),
            F.col("p_event_date"),

            F.col("parsed.stage").alias("stage"),

            F.col("parsed.tenant.tenantId").alias("tenant_id"),
            F.col("parsed.tenant.authorId").alias("author_id"),
            F.col("parsed.tenant.plan").alias("plan"),

            F.col("parsed.entity.storyId").alias("story_id"),
            F.col("parsed.entity.seriesId").alias("series_id"),
            F.col("parsed.entity.chapterId").alias("chapter_id"),
            F.col("parsed.entity.sceneId").alias("scene_id"),

            F.col("parsed.producer.service").alias("producer_service"),
            F.col("parsed.producer.env").alias("producer_env"),
            F.col("parsed.producer.region").alias("producer_region"),
            F.col("parsed.producer.traceId").alias("trace_id"),

            F.col("parsed.metrics").alias("metrics"),
            F.col("parsed.metadata").alias("metadata"),

            event_hash.alias("event_hash"),
            is_late.alias("is_late_event"),

            # lineage from bronze
            F.col("ingest_id"),
            F.col("source"),
            F.col("raw_schema_version"),
            F.col("ingested_at"),
            F.col("p_ingest_date"),
            F.col("raw_json"),
        )
    )

    # Build reject reason (first match wins)
    reject_reason = (
        F.when(~F.col("parse_ok"), F.lit("JSON_PARSE_FAILED"))
         .when(F.col("parsed.eventId").isNull(), F.lit("MISSING_EVENT_ID"))
         .when(F.col("parsed.eventType").isNull(), F.lit("MISSING_EVENT_TYPE"))
         .when(~F.col("parsed.eventType").isin(allowed_event_types), F.lit("INVALID_EVENT_TYPE"))
         .when(F.col("parsed.eventVersion").isNull(), F.lit("MISSING_EVENT_VERSION"))
         .when(F.col("parsed.eventVersion") < F.lit(1), F.lit("INVALID_EVENT_VERSION"))
         .when(F.col("occurred_at").isNull(), F.lit("INVALID_OCCURRED_AT"))
         .when(F.col("parsed.tenant.tenantId").isNull(), F.lit("MISSING_TENANT_ID"))
         .when(F.col("parsed.tenant.authorId").isNull(), F.lit("MISSING_AUTHOR_ID"))
         .when(F.col("parsed.entity.storyId").isNull(), F.lit("MISSING_STORY_ID"))
         .otherwise(F.lit("CONTRACT_FAILED"))
    )

    bad = (
        parsed
        .where(~(F.col("parse_ok") & required_ok & domain_ok))
        .withColumn("reject_reason", reject_reason)
        .withColumn("event_hash", event_hash)
        .select(
            "event_hash",
            "reject_reason",
            "ingest_id",
            "source",
            "raw_schema_version",
            "ingested_at",
            "p_ingest_date",
            "raw_json",
        )
    )

    (
        good.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("p_event_date")
        .save(SILVER_EVENTS_PATH)
    )

    (
        bad.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("p_ingest_date")
        .save(SILVER_REJECTS_PATH)
    )

    print(f"✅ Silver events written to: {SILVER_EVENTS_PATH}  rows={good.count()}")
    print(f"✅ Silver rejects written to: {SILVER_REJECTS_PATH} rows={bad.count()}")

    spark.stop()


if __name__ == "__main__":
    main()