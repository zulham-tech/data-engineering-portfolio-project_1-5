"""
Project 5 — Crypto Lambda: Batch Layer Transformer
Kafka: crypto-ohlcv-batch → PySpark → Snowflake
Technical indicators: 7d MA, 30d MA, daily return %, golden cross signal
"""

import logging
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_OHLCV = "crypto-ohlcv-batch"

SNOWFLAKE_OPTIONS = {
    "sfURL": "your_account.snowflakecomputing.com",
    "sfDatabase": "CRYPTO_DW",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "SYSADMIN",
    "sfUser": "your_user",
    "sfPassword": "your_password",
}

OHLCV_SCHEMA = StructType([
    StructField("coin_id", StringType()),
    StructField("candle_date", StringType()),
    StructField("open_usd", DoubleType()),
    StructField("high_usd", DoubleType()),
    StructField("low_usd", DoubleType()),
    StructField("close_usd", DoubleType()),
    StructField("price_range_usd", DoubleType()),
    StructField("ingested_at", TimestampType()),
])


def compute_technical_indicators(df):
    window_coin = Window.partitionBy("coin_id").orderBy("candle_date")
    window_7d = window_coin.rowsBetween(-6, 0)
    window_30d = window_coin.rowsBetween(-29, 0)

    return (
        df
        .withColumn("ma_7d", F.round(F.avg("close_usd").over(window_7d), 6))
        .withColumn("ma_30d", F.round(F.avg("close_usd").over(window_30d), 6))
        .withColumn(
            "daily_return_pct",
            F.round(
                (F.col("close_usd") - F.lag("close_usd", 1).over(window_coin))
                / F.lag("close_usd", 1).over(window_coin) * 100, 4
            )
        )
        .withColumn(
            "golden_cross",
            F.when(
                (F.col("ma_7d") > F.col("ma_30d")) &
                (F.lag("ma_7d", 1).over(window_coin) <= F.lag("ma_30d", 1).over(window_coin)),
                True
            ).otherwise(False)
        )
        .withColumn("batch_date", F.current_date())
    )


def main():
    spark = (
        SparkSession.builder
        .appName("CryptoBatchLayer")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_OHLCV)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .select(F.from_json(F.col("value").cast("string"), OHLCV_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("candle_date", F.to_date("candle_date"))
        .dropDuplicates(["coin_id", "candle_date"])
        .filter(F.col("close_usd") > 0)
    )

    enriched = compute_technical_indicators(raw)

    (
        enriched.write
        .format("net.snowflake.spark.snowflake")
        .options(**SNOWFLAKE_OPTIONS)
        .option("dbtable", "CRYPTO_OHLCV_HISTORY")
        .mode("overwrite")
        .save()
    )

    logger.info(f"Loaded {enriched.count()} records to Snowflake.")
    spark.stop()


if __name__ == "__main__":
    main()
