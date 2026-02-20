import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

DELTA_ROOT = os.path.join("out", "delta")
SILVER_EVENTS_PATH = os.path.join(DELTA_ROOT, "silver", "creatorops_events")

GOLD_VELOCITY_PATH = os.path.join(DELTA_ROOT, "gold", "kpi_writing_velocity_daily")
GOLD_CHURN_PATH = os.path.join(DELTA_ROOT, "gold", "kpi_revision_churn_daily")


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("creatorops-gold-kpis-local")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.io.native.lib.available", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def to_int_from_map(col_map, key):
    # map<string,string> -> int (null-safe)
    return F.col(col_map).getItem(key).cast("int")


def main():
    spark = build_spark()
    events = spark.read.format("delta").load(SILVER_EVENTS_PATH)

    # ----------------------------
    # KPI 1: Writing Velocity (daily)
    # ----------------------------
    writing = (
        events
        .filter(F.col("event_type") == F.lit("chapter_written"))
        .withColumn("word_count", to_int_from_map("metrics", "wordCount"))
        .groupBy("p_event_date", "tenant_id", "author_id", "story_id", "series_id")
        .agg(
            F.count("*").alias("chapters_written"),
            F.sum("word_count").alias("words_written"),
            F.avg("word_count").alias("avg_words_per_chapter"),
        )
        .withColumn("words_written", F.coalesce(F.col("words_written"), F.lit(0)))
    )

    (
        writing.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("p_event_date")
        .save(GOLD_VELOCITY_PATH)
    )

    # ----------------------------
    # KPI 2: Revision Churn (daily)
    # ----------------------------
    churn = (
        events
        .filter(F.col("event_type") == F.lit("scene_revised"))
        .withColumn("revision_count", to_int_from_map("metrics", "revisionCount"))
        .withColumn("revision_count", F.coalesce(F.col("revision_count"), F.lit(1)))
        .groupBy("p_event_date", "tenant_id", "author_id", "story_id", "series_id")
        .agg(
            F.count("*").alias("revision_events"),
            F.sum("revision_count").alias("revision_count_sum"),
        )
        .withColumn(
            "revision_churn_index",
            F.when(F.col("revision_events") == 0, F.lit(0.0))
             .otherwise(F.col("revision_count_sum") / F.col("revision_events"))
        )
    )

    (
        churn.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("p_event_date")
        .save(GOLD_CHURN_PATH)
    )

    print(f"✅ Gold writing velocity written: {GOLD_VELOCITY_PATH} rows={writing.count()}")
    print(f"✅ Gold revision churn written: {GOLD_CHURN_PATH} rows={churn.count()}")

    spark.stop()


if __name__ == "__main__":
    main()