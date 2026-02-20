import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

DELTA_ROOT = os.path.join("out", "delta")
SILVER_EVENTS_PATH = os.path.join(DELTA_ROOT, "silver", "creatorops_events")

GOLD_ENG_BANDS_PATH = os.path.join(DELTA_ROOT, "gold", "kpi_engagement_bands_daily")
GOLD_DROPOFF_PATH = os.path.join(DELTA_ROOT, "gold", "kpi_dropoff_rate_daily")


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("creatorops-gold-retention-local")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.io.native.lib.available", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def to_double_from_map(map_col, key):
    return F.col(map_col).getItem(key).cast("double")


def band_10(score_col):
    # 0-9 -> 0, 10-19 -> 10 ... 90-100 -> 90
    return (
        F.when(score_col.isNull(), F.lit(None))
         .otherwise(F.floor(F.least(F.greatest(score_col, F.lit(0.0)), F.lit(100.0)) / F.lit(10.0)) * F.lit(10.0))
    )


def main():
    spark = build_spark()
    ev = spark.read.format("delta").load(SILVER_EVENTS_PATH)

    engagement = (
        ev.filter(F.col("event_type") == F.lit("reader_engagement"))
          .withColumn("engagement_score", to_double_from_map("metrics", "engagementScore"))
          .withColumn("score_band", band_10(F.col("engagement_score")))
          .select(
              "p_event_date",
              "tenant_id", "author_id", "story_id", "series_id",
              "engagement_score", "score_band"
          )
    )

    dropoff = (
        ev.filter(F.col("event_type") == F.lit("reader_dropoff"))
          .select("p_event_date", "tenant_id", "author_id", "story_id", "series_id")
          .withColumn("dropoff_flag", F.lit(1))
    )

    # ----------------------------
    # Output 1: Engagement score bands (daily distribution)
    # ----------------------------
    eng_bands = (
        engagement.groupBy("p_event_date", "tenant_id", "story_id", "series_id", "score_band")
        .agg(
            F.count("*").alias("engagement_events"),
            F.avg("engagement_score").alias("avg_score_in_band")
        )
        .withColumn("as_of_date", F.current_date())
    )

    (
        eng_bands.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("p_event_date")
        .save(GOLD_ENG_BANDS_PATH)
    )

    # ----------------------------
    # Output 2: Dropoff rate (daily)
    # dropoff_rate = dropoffs / (engagements + dropoffs)
    # ----------------------------
    eng_counts = (
        engagement.groupBy("p_event_date", "tenant_id", "story_id", "series_id")
        .agg(F.count("*").alias("engagement_events"))
    )

    drop_counts = (
        dropoff.groupBy("p_event_date", "tenant_id", "story_id", "series_id")
        .agg(F.count("*").alias("dropoff_events"))
    )

    dropoff_rate = (
        eng_counts.join(drop_counts, on=["p_event_date", "tenant_id", "story_id", "series_id"], how="full")
        .na.fill(0, subset=["engagement_events", "dropoff_events"])
        .withColumn("total_events", F.col("engagement_events") + F.col("dropoff_events"))
        .withColumn(
            "dropoff_rate",
            F.when(F.col("total_events") == 0, F.lit(0.0))
             .otherwise(F.col("dropoff_events") / F.col("total_events"))
        )
        .withColumn("as_of_date", F.current_date())
    )

    (
        dropoff_rate.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("p_event_date")
        .save(GOLD_DROPOFF_PATH)
    )

    print(f"✅ Gold engagement bands written: {GOLD_ENG_BANDS_PATH} rows={eng_bands.count()}")
    print(f"✅ Gold dropoff rate written: {GOLD_DROPOFF_PATH} rows={dropoff_rate.count()}")

    spark.stop()


if __name__ == "__main__":
    main()