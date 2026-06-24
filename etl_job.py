"""
etl_job.py — PySpark ETL: raw JSON events → fact_user_daily Parquet
Usage:
    spark-submit etl_job.py \
        --input  "data/raw/*.json" \
        --output "data/output/fact_user_daily"

AWS EMR / Glue usage:
    Pass s3:// paths via --input / --output arguments.
"""

import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schema — enforced at read time; avoids costly schema inference on large data
# ---------------------------------------------------------------------------
PROPERTIES_SCHEMA = StructType([
    StructField("page",       StringType(),  True),
    StructField("session_id", StringType(),  True),
    StructField("product_id", StringType(),  True),
    StructField("quantity",   IntegerType(), True),
    StructField("query",      StringType(),  True),
])

EVENT_SCHEMA = StructType([
    StructField("event_id",   StringType(),  False),
    StructField("user_id",    StringType(),  True),
    StructField("event_type", StringType(),  False),
    StructField("timestamp",  StringType(),  False),
    StructField("revenue",    DoubleType(),  True),
    StructField("properties", PROPERTIES_SCHEMA, True),
])


def build_spark(app_name: str = "fact_user_daily_etl") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        # Tune for local mode; on EMR these are set by the cluster config
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def read_events(spark: SparkSession, input_path: str):
    """
    Read newline-delimited JSON files.
    DROPMALFORMED silently skips lines that don't parse (e.g. the broken
    line in events_2024-01-15_10.json).  Corrupt rows are logged via
    spark.sql.badRecordsPath if set.
    """
    logger.info(f"Reading events from: {input_path}")
    df = (
        spark.read
        .schema(EVENT_SCHEMA)
        .option("mode", "DROPMALFORMED")   # skip bad JSON lines gracefully
        .option("multiLine", "false")       # one JSON object per line
        .json(input_path)
    )
    raw_count = df.count()
    logger.info(f"Rows after read (pre-dedup): {raw_count}")
    return df


def clean(df):
    """
    1. Deduplicate on event_id — the same event can appear in multiple
       hourly files (evt_002 appears in both _09 and _10 files).
       We keep the first occurrence by earliest timestamp.
    2. Drop rows with null user_id — anonymous sessions cannot be
       attributed to a user and would pollute user-level aggregations.
    3. Cast timestamp string → TimestampType for reliable date extraction.
    """
    df = (
        df
        # Rank duplicates; keep the earliest record per event_id
        .withColumn(
            "_rank",
            F.row_number().over(
                __import__("pyspark.sql.window", fromlist=["Window"])
                .Window
                .partitionBy("event_id")
                .orderBy(F.col("timestamp").asc())
            )
        )
        .filter(F.col("_rank") == 1)
        .drop("_rank")
        # Drop anonymous events
        .filter(F.col("user_id").isNotNull())
        # Parse timestamp
        .withColumn("ts", F.to_timestamp("timestamp"))
        # Extract calendar date
        .withColumn("date", F.to_date("ts"))
    )

    clean_count = df.count()
    logger.info(f"Rows after cleaning: {clean_count}")
    return df


def aggregate(df):
    """
    Produce one row per (user_id, date).
    total_revenue: sum of revenue; null for non-purchase events is treated
    as 0 so that users who never purchased get 0.0 rather than null.
    """
    fact = (
        df
        .withColumn("revenue_safe", F.coalesce(F.col("revenue"), F.lit(0.0)))
        .groupBy("user_id", "date")
        .agg(
            F.count("*")                        .alias("event_count"),
            F.countDistinct("event_type")       .alias("distinct_event_types"),
            F.min("ts")                         .alias("first_event_ts"),
            F.max("ts")                         .alias("last_event_ts"),
            F.round(F.sum("revenue_safe"), 2)   .alias("total_revenue"),
        )
        .orderBy("date", "user_id")
    )
    return fact


def write_parquet(df, output_path: str):
    """
    Write partitioned Parquet.  Partitioning by date lets downstream
    queries (Athena, Redshift Spectrum, Spark) use partition pruning.
    """
    logger.info(f"Writing Parquet to: {output_path}")
    (
        df.write
        .mode("overwrite")
        .partitionBy("date")
        .parquet(output_path)
    )
    logger.info("Write complete.")


def main():
    parser = argparse.ArgumentParser(description="fact_user_daily ETL")
    parser.add_argument(
        "--input",
        default="data/raw/*.json",
        help="Glob path to input JSON files (local or s3://)"
    )
    parser.add_argument(
        "--output",
        default="data/output/fact_user_daily",
        help="Output path for Parquet (local or s3://)"
    )
    args = parser.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw   = read_events(spark, args.input)
    clean_df = clean(raw)
    fact  = aggregate(clean_df)

    logger.info("Preview of fact_user_daily:")
    fact.show(truncate=False)
    fact.printSchema()

    write_parquet(fact, args.output)
    spark.stop()


if __name__ == "__main__":
    main()
