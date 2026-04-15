"""
Project 2 — Batch Ingestion PySpark Transformer
Kafka: batch-github-repos + batch-weather-history → PostgreSQL → S3 → Redshift
Features: dedup, feature engineering, DISTKEY/SORTKEY optimization
"""

import logging
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, BooleanType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_GITHUB = "batch-github-repos"
TOPIC_WEATHER = "batch-weather-history"

S3_BUCKET = "s3a://your-bucket/data-engineering-portfolio"
REDSHIFT_URL = "jdbc:redshift://your-cluster.redshift.amazonaws.com:5439/dev"
REDSHIFT_PROPS = {
    "user": "your_user",
    "password": "your_password",
    "driver": "com.amazon.redshift.jdbc42.Driver",
}

GITHUB_SCHEMA = StructType([
    StructField("repo_id", IntegerType()),
    StructField("name", StringType()),
    StructField("full_name", StringType()),
    StructField("language", StringType()),
    StructField("stars", IntegerType()),
    StructField("forks", IntegerType()),
    StructField("watchers", IntegerType()),
    StructField("created_at", StringType()),
    StructField("updated_at", StringType()),
    StructField("description", StringType()),
    StructField("ingested_at", TimestampType()),
])

WEATHER_SCHEMA = StructType([
    StructField("city", StringType()),
    StructField("date", StringType()),
    StructField("temp_max_c", DoubleType()),
    StructField("temp_min_c", DoubleType()),
    StructField("precipitation_mm", DoubleType()),
    StructField("wind_speed_max_kmh", DoubleType()),
    StructField("ingested_at", TimestampType()),
])


def transform_github(df):
    return (
        df
        .dropDuplicates(["repo_id"])
        .filter(F.col("stars") > 0)
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn(
            "repo_age_days",
            F.datediff(F.current_date(), F.to_date("created_at"))
        )
        .withColumn(
            "activity_score",
            F.round(
                F.col("stars") * 0.5 +
                F.col("forks") * 0.3 +
                F.col("watchers") * 0.2,
                2
            )
        )
        .withColumn(
            "stars_per_day",
            F.round(F.col("stars") / F.greatest(F.col("repo_age_days"), F.lit(1)), 4)
        )
        .withColumn("batch_date", F.current_date())
    )


def transform_weather(df):
    return (
        df
        .dropDuplicates(["city", "date"])
        .withColumn("date", F.to_date("date"))
        .withColumn(
            "temp_range_c",
            F.round(F.col("temp_max_c") - F.col("temp_min_c"), 2)
        )
        .withColumn(
            "is_rainy",
            F.when(F.col("precipitation_mm") > 1.0, True).otherwise(False)
        )
        .withColumn("batch_date", F.current_date())
    )


def read_kafka_batch(spark, topic, schema):
    return (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
        .select("d.*")
    )


def write_to_s3_parquet(df, path: str):
    df.write.mode("overwrite").parquet(path)
    logger.info(f"Written to S3: {path}")


def write_to_redshift(df, table: str):
    df.write.jdbc(url=REDSHIFT_URL, table=table, mode="append", properties=REDSHIFT_PROPS)
    logger.info(f"Loaded to Redshift: {table}")


def main():
    spark = (
        SparkSession.builder
        .appName("BatchIngestionTransformer")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # GitHub pipeline
    logger.info("Processing GitHub repos...")
    github_raw = read_kafka_batch(spark, TOPIC_GITHUB, GITHUB_SCHEMA)
    github_enriched = transform_github(github_raw)
    write_to_s3_parquet(github_enriched, f"{S3_BUCKET}/github/batch_date={F.current_date()}")
    write_to_redshift(github_enriched, "github_trending_repos")
    logger.info(f"GitHub: {github_enriched.count()} records loaded")

    # Weather pipeline
    logger.info("Processing weather history...")
    weather_raw = read_kafka_batch(spark, TOPIC_WEATHER, WEATHER_SCHEMA)
    weather_enriched = transform_weather(weather_raw)
    write_to_s3_parquet(weather_enriched, f"{S3_BUCKET}/weather/batch_date={F.current_date()}")
    write_to_redshift(weather_enriched, "weather_history_indonesia")
    logger.info(f"Weather: {weather_enriched.count()} records loaded")

    spark.stop()
    logger.info("Batch ingestion complete.")


if __name__ == "__main__":
    main()
