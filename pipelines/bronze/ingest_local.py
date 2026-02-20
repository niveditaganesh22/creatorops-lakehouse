import os
import uuid
import glob

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, to_date, lit
from delta import configure_spark_with_delta_pip


DELTA_ROOT = os.path.join("out", "delta")
BRONZE_PATH = os.path.join(DELTA_ROOT, "bronze", "creatorops_events_raw")
EVENTS_ROOT = os.path.join("out", "events")  # out/events/p_event_date=YYYY-MM-DD/events.ndjson


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("creatorops-bronze-ingest-local")
        # Delta config (pip-installed delta-spark)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Windows stability knobs
        .config("spark.hadoop.io.native.lib.available", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main() -> None:
    if not os.path.exists(EVENTS_ROOT):
        raise FileNotFoundError(f"Events root not found: {EVENTS_ROOT}. Run generator first.")

    # IMPORTANT: Avoid Hadoop globbing on Windows (it triggers NativeIO errors)
    files = glob.glob(os.path.join(EVENTS_ROOT, "**", "*.ndjson"), recursive=True)
    if not files:
        raise FileNotFoundError(f"No .ndjson files found under: {EVENTS_ROOT}")

    spark = build_spark()

    df = (
        spark.read
        .option("multiLine", "false")
        .json(files)  # pass explicit file list (no ** glob inside Spark)
    )

    bronze = (
        df.selectExpr("to_json(struct(*)) as raw_json")
        .withColumn("ingest_id", lit(str(uuid.uuid4())))
        .withColumn("source", lit("local_generator"))
        .withColumn("raw_schema_version", lit(1))
        .withColumn("ingested_at", current_timestamp())
        .withColumn("p_ingest_date", to_date(current_timestamp()))
        .select("ingest_id", "source", "raw_json", "raw_schema_version", "ingested_at", "p_ingest_date")
    )

    (
        bronze.write
        .format("delta")
        .mode("append")
        .partitionBy("p_ingest_date")
        .save(BRONZE_PATH)
    )

    print(f"✅ Bronze written to: {BRONZE_PATH}")
    print(f"Rows ingested: {bronze.count()}")

    spark.stop()


if __name__ == "__main__":
    main()