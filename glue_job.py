"""
glue_job.py — AWS Glue 4.0 version of the same ETL (Spark 3.3 under the hood).

Deploy:
  1. Upload this file to S3.
  2. Create a Glue Job (Spark, Glue 4.0, Python 3) pointing to this script.
  3. Set job parameters:
       --INPUT_PATH   s3://your-bucket/raw/year=2024/month=01/day=15/*.json
       --OUTPUT_PATH  s3://your-bucket/processed/fact_user_daily

Glue handles the SparkContext / GlueContext setup automatically.
"""

import sys
import logging
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType
)
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Glue boilerplate ──────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ["JOB_NAME", "INPUT_PATH", "OUTPUT_PATH"])
sc   = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session
job  = Job(glue)
job.init(args["JOB_NAME"], args)

INPUT_PATH  = args["INPUT_PATH"]
OUTPUT_PATH = args["OUTPUT_PATH"]

# ── Schema ────────────────────────────────────────────────────────────────
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

# ── Read ──────────────────────────────────────────────────────────────────
logger.info(f"Reading from {INPUT_PATH}")
raw = (
    spark.read
    .schema(EVENT_SCHEMA)
    .option("mode", "DROPMALFORMED")
    .json(INPUT_PATH)
)

# ── Clean ─────────────────────────────────────────────────────────────────
window = Window.partitionBy("event_id").orderBy(F.col("timestamp").asc())

clean = (
    raw
    .withColumn("_rank", F.row_number().over(window))
    .filter(F.col("_rank") == 1).drop("_rank")
    .filter(F.col("user_id").isNotNull())
    .withColumn("ts",   F.to_timestamp("timestamp"))
    .withColumn("date", F.to_date("ts"))
)

# ── Aggregate ─────────────────────────────────────────────────────────────
fact = (
    clean
    .withColumn("revenue_safe", F.coalesce(F.col("revenue"), F.lit(0.0)))
    .groupBy("user_id", "date")
    .agg(
        F.count("*")                     .alias("event_count"),
        F.countDistinct("event_type")    .alias("distinct_event_types"),
        F.min("ts")                      .alias("first_event_ts"),
        F.max("ts")                      .alias("last_event_ts"),
        F.round(F.sum("revenue_safe"), 2).alias("total_revenue"),
    )
)

# ── Write ─────────────────────────────────────────────────────────────────
logger.info(f"Writing to {OUTPUT_PATH}")
(
    fact.write
    .mode("overwrite")
    .partitionBy("date")
    .parquet(OUTPUT_PATH)
)

job.commit()
logger.info("Glue job complete.")
