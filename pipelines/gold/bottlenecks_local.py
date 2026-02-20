import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip

DELTA_ROOT = os.path.join("out", "delta")
SILVER_EVENTS_PATH = os.path.join(DELTA_ROOT, "silver", "creatorops_events")
GOLD_BOTTLENECKS_PATH = os.path.join(DELTA_ROOT, "gold", "kpi_stage_bottlenecks")


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("creatorops-gold-bottlenecks-local")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.io.native.lib.available", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main():
    spark = build_spark()
    ev = spark.read.format("delta").load(SILVER_EVENTS_PATH)

    # Only keep events that represent "progress through pipeline"
    # (We include everything with a stage, then compute deltas)
    base = (
        ev.filter(F.col("stage").isNotNull())
          .select(
              "tenant_id", "author_id", "story_id", "series_id",
              "event_id", "event_type", "occurred_at", "stage",
              "p_event_date"
          )
    )

    w = Window.partitionBy("tenant_id", "story_id").orderBy(F.col("occurred_at").asc())

    transitions = (
        base
        .withColumn("next_occurred_at", F.lead("occurred_at").over(w))
        .withColumn("next_stage", F.lead("stage").over(w))
        .withColumn("duration_seconds", F.col("next_occurred_at").cast("long") - F.col("occurred_at").cast("long"))
        .filter(F.col("next_occurred_at").isNotNull())
        .filter(F.col("duration_seconds") >= 0)
        .withColumn("duration_hours", F.col("duration_seconds") / F.lit(3600.0))
        .withColumn("duration_days", F.col("duration_seconds") / F.lit(86400.0))
    )

    # Aggregate per stage (time spent in stage before moving to next stage)
    bottlenecks = (
        transitions
        .groupBy("tenant_id", "story_id", "series_id", "stage")
        .agg(
            F.count("*").alias("stage_hops"),
            F.avg("duration_hours").alias("avg_hours_in_stage"),
            F.expr("percentile_approx(duration_hours, 0.5)").alias("p50_hours_in_stage"),
            F.expr("percentile_approx(duration_hours, 0.9)").alias("p90_hours_in_stage"),
            F.max("duration_hours").alias("max_hours_in_stage"),
        )
        .withColumn("as_of_date", F.current_date())
    )

    (
        bottlenecks.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD_BOTTLENECKS_PATH)
    )

    print(f"✅ Gold bottlenecks written: {GOLD_BOTTLENECKS_PATH} rows={bottlenecks.count()}")

    spark.stop()


if __name__ == "__main__":
    main()