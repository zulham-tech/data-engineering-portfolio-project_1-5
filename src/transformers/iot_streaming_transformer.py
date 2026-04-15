"""
Project 1 — IoT PySpark Structured Streaming Transformer
Kafka: iot-weather-sensors → PostgreSQL (operational) + BigQuery (analytics)
Features: 5-min tumbling window, z-score anomaly detection, data quality check
"""

import logging
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "iot-weather-sensors"
POSTGRES_URL = "jdbc:postgresql://localhost:5432/iot_db"
POSTGRES_PROPS = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}

SENSOR_SCHEMA = StructType([
    StructField("sensor_id", StringType()),
    StructField("city", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("temperature_c", DoubleType()),
    StructField("humidity_pct", DoubleType()),
    StructField("precipitation_mm", DoubleType()),
    StructField("wind_speed_kmh", DoubleType()),
    StructField("wind_direction_deg", DoubleType()),
    StructField("event_time", TimestampType()),
])

QUALITY_THRESHOLD = 0.95  # 95% completeness required


def write_batch(batch_df, batch_id: int):
    count = batch_df.count()
    if count == 0:
        logger.info(f"Batch {batch_id}: empty, skipping.")
        return

    # Data quality check
    non_null = batch_df.filter(F.col("temperature_c").isNotNull()).count()
    completeness = non_null / count
    if completeness < QUALITY_THRESHOLD:
        logger.warning(f"Batch {batch_id}: completeness {completeness:.2%} < threshold. Skipping.")
        return

    # Z-score anomaly detection (flag if temp > 2 std from mean)
    stats = batch_df.select(
        F.mean("temperature_c").alias("mean_temp"),
        F.stddev("temperature_c").alias("std_temp"),
    ).first()

    enriched = batch_df.withColumn(
        "is_anomaly",
        F.when(
            F.abs(F.col("temperature_c") - stats["mean_temp"]) > 2 * (stats["std_temp"] or 1),
            True
        ).otherwise(False)
    ).withColumn("batch_id", F.lit(batch_id))

    # Write raw readings to PostgreSQL
    (
        enriched.write.jdbc(
            url=POSTGRES_URL,
            table="iot_sensor_readings",
            mode="append",
            properties=POSTGRES_PROPS,
        )
    )

    # 5-min window aggregations
    agg_df = batch_df.groupBy(
        F.window("event_time", "5 minutes"),
        "city"
    ).agg(
        F.avg("temperature_c").alias("avg_temp_c"),
        F.min("temperature_c").alias("min_temp_c"),
        F.max("temperature_c").alias("max_temp_c"),
        F.avg("humidity_pct").alias("avg_humidity_pct"),
        F.sum("precipitation_mm").alias("total_precip_mm"),
        F.count("*").alias("reading_count"),
    ).withColumn("window_start", F.col("window.start")) \
     .withColumn("window_end", F.col("window.end")) \
     .drop("window")

    agg_df.write.jdbc(
        url=POSTGRES_URL,
        table="iot_sensor_aggregates_5min",
        mode="append",
        properties=POSTGRES_PROPS,
    )

    logger.info(f"Batch {batch_id}: {count} records written. Completeness: {completeness:.2%}")


def main():
    spark = (
        SparkSession.builder
        .appName("IoTStreamingTransformer")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints/iot")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
        .select(F.from_json(F.col("value").cast("string"), SENSOR_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withWatermark("event_time", "2 minutes")
        .filter(F.col("sensor_id").isNotNull())
    )

    query = (
        stream.writeStream
        .foreachBatch(write_batch)
        .trigger(processingTime="60 seconds")
        .option("checkpointLocation", "/tmp/checkpoints/iot")
        .start()
    )

    logger.info("IoT streaming started.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
