"""
Project 3 — Reddit Sentiment PySpark Streaming Transformer
Kafka: reddit-posts-stream → MongoDB (raw) + BigQuery (analytics)
Features: VADER + TextBlob ensemble NLP, foreachBatch upsert by post_id
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
TOPIC = "reddit-posts-stream"
MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "reddit_data"

POST_SCHEMA = StructType([
    StructField("post_id", StringType()),
    StructField("subreddit", StringType()),
    StructField("title", StringType()),
    StructField("body", StringType()),
    StructField("author", StringType()),
    StructField("score", IntegerType()),
    StructField("upvote_ratio", DoubleType()),
    StructField("num_comments", IntegerType()),
    StructField("url", StringType()),
    StructField("is_self", BooleanType()),
    StructField("flair", StringType()),
    StructField("created_utc", TimestampType()),
    StructField("ingested_at", TimestampType()),
])


def compute_sentiment(text: str) -> tuple[float, float, str]:
    """VADER + TextBlob ensemble sentiment."""
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    from textblob import TextBlob

    if not text or not text.strip():
        return 0.0, 0.0, "neutral"

    vader = SentimentIntensityAnalyzer()
    vader_score = vader.polarity_scores(text)["compound"]
    textblob_score = TextBlob(text).sentiment.polarity
    ensemble = (vader_score + textblob_score) / 2

    if ensemble >= 0.05:
        label = "positive"
    elif ensemble <= -0.05:
        label = "negative"
    else:
        label = "neutral"

    return round(vader_score, 4), round(textblob_score, 4), label


def write_batch(batch_df, batch_id: int):
    if batch_df.count() == 0:
        logger.info(f"Batch {batch_id}: empty, skipping.")
        return

    # Compute sentiment per row (collect to driver for NLP)
    rows = batch_df.collect()
    results = []
    for row in rows:
        text = f"{row['title']} {row['body'] or ''}"
        vader_s, tb_s, label = compute_sentiment(text)
        results.append({
            **row.asDict(),
            "vader_score": vader_s,
            "textblob_score": tb_s,
            "ensemble_score": round((vader_s + tb_s) / 2, 4),
            "sentiment_label": label,
            "batch_id": batch_id,
        })

    # Write to MongoDB
    from pymongo import MongoClient, UpdateOne
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    # Upsert raw posts
    raw_ops = [
        UpdateOne({"post_id": r["post_id"]}, {"$set": r}, upsert=True)
        for r in results
    ]
    db["raw_posts"].bulk_write(raw_ops)

    # Upsert sentiment results
    sentiment_ops = [
        UpdateOne(
            {"post_id": r["post_id"]},
            {"$set": {
                "post_id": r["post_id"],
                "subreddit": r["subreddit"],
                "vader_score": r["vader_score"],
                "textblob_score": r["textblob_score"],
                "ensemble_score": r["ensemble_score"],
                "sentiment_label": r["sentiment_label"],
                "score": r["score"],
                "created_utc": r["created_utc"],
            }},
            upsert=True
        )
        for r in results
    ]
    db["sentiment_results"].bulk_write(sentiment_ops)
    client.close()

    logger.info(f"Batch {batch_id}: {len(results)} posts processed → MongoDB")


def main():
    spark = (
        SparkSession.builder
        .appName("RedditSentimentStreaming")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints/reddit")
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
        .select(F.from_json(F.col("value").cast("string"), POST_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("created_utc", F.to_timestamp("created_utc"))
        .withWatermark("created_utc", "5 minutes")
        .filter(F.col("post_id").isNotNull())
    )

    query = (
        stream.writeStream
        .foreachBatch(write_batch)
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", "/tmp/checkpoints/reddit")
        .start()
    )

    logger.info("Reddit sentiment streaming started.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
