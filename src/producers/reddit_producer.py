"""
Project 3 — Reddit Sentiment Streaming Producer
Streams live posts from 5 subreddits → Kafka topic: reddit-posts-stream
Uses PRAW (Python Reddit API Wrapper) — free, no key for public subreddits
"""

import json
import logging
from datetime import datetime, timezone

import praw
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "reddit-posts-stream"

SUBREDDITS = ["technology", "datascience", "MachineLearning", "programming", "Python"]

# PRAW config — register app at https://www.reddit.com/prefs/apps
REDDIT_CONFIG = {
    "client_id": "your_client_id",
    "client_secret": "your_client_secret",
    "user_agent": "DataEngPortfolio/1.0 by u/your_username",
}


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        compression_type="gzip",
        retries=3,
        acks="all",
    )


def build_record(post) -> dict:
    return {
        "post_id": post.id,
        "subreddit": str(post.subreddit),
        "title": post.title[:500],
        "body": (post.selftext or "")[:1000],
        "author": str(post.author) if post.author else "[deleted]",
        "score": post.score,
        "upvote_ratio": post.upvote_ratio,
        "num_comments": post.num_comments,
        "url": post.url,
        "is_self": post.is_self,
        "flair": post.link_flair_text,
        "created_utc": datetime.fromtimestamp(post.created_utc, tz=timezone.utc).isoformat(),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source": "reddit_praw",
    }


def run():
    reddit = praw.Reddit(**REDDIT_CONFIG)
    producer = create_producer()

    subreddit_str = "+".join(SUBREDDITS)
    logger.info(f"Streaming from r/{subreddit_str}...")

    for post in reddit.subreddit(subreddit_str).stream.submissions(skip_existing=True):
        record = build_record(post)
        try:
            producer.send(TOPIC, key=record["post_id"], value=record)
            producer.flush()
            logger.info(f"[r/{record['subreddit']}] {record['title'][:60]}...")
        except KafkaError as e:
            logger.error(f"Kafka send error: {e}")


if __name__ == "__main__":
    run()
