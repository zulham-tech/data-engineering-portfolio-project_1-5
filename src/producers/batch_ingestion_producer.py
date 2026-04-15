"""
Project 2 — Batch Ingestion Producer
Sources: GitHub REST API + Open-Meteo Historical API
Targets: Kafka topics: batch-github-repos, batch-weather-history
"""

import json
import logging
from datetime import datetime, timezone, timedelta

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_GITHUB = "batch-github-repos"
TOPIC_WEATHER = "batch-weather-history"

GITHUB_API = "https://api.github.com/search/repositories"
WEATHER_ARCHIVE_API = "https://archive-api.open-meteo.com/v1/archive"

LANGUAGES = ["Python", "Go", "Rust", "TypeScript", "Java"]
TOP_N_PER_LANGUAGE = 30

CITIES = [
    {"city": "Jakarta",  "lat": -6.2,  "lon": 106.8},
    {"city": "Surabaya", "lat": -7.25, "lon": 112.75},
    {"city": "Medan",    "lat": 3.58,  "lon": 98.67},
    {"city": "Bandung",  "lat": -6.92, "lon": 107.61},
    {"city": "Makassar", "lat": -5.14, "lon": 119.41},
]


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        compression_type="gzip",
        retries=5,
        acks="all",
    )


def fetch_github_repos(language: str, top_n: int = TOP_N_PER_LANGUAGE) -> list:
    headers = {"Accept": "application/vnd.github.v3+json"}
    params = {
        "q": f"language:{language} stars:>100",
        "sort": "stars",
        "order": "desc",
        "per_page": top_n,
    }
    try:
        resp = requests.get(GITHUB_API, headers=headers, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json().get("items", [])
    except requests.RequestException as e:
        logger.error(f"GitHub API error [{language}]: {e}")
        return []


def fetch_weather_history(lat: float, lon: float, days: int = 90) -> dict:
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days)
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max",
        "timezone": "Asia/Jakarta",
    }
    try:
        resp = requests.get(WEATHER_ARCHIVE_API, params=params, timeout=20)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        logger.error(f"Weather archive error: {e}")
        return {}


def publish_github(producer: KafkaProducer):
    total = 0
    for lang in LANGUAGES:
        repos = fetch_github_repos(lang)
        for repo in repos:
            record = {
                "repo_id": repo["id"],
                "name": repo["name"],
                "full_name": repo["full_name"],
                "language": lang,
                "stars": repo.get("stargazers_count", 0),
                "forks": repo.get("forks_count", 0),
                "watchers": repo.get("watchers_count", 0),
                "created_at": repo.get("created_at"),
                "updated_at": repo.get("updated_at"),
                "description": (repo.get("description") or "")[:200],
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            }
            try:
                producer.send(TOPIC_GITHUB, key=str(record["repo_id"]), value=record)
                total += 1
            except KafkaError as e:
                logger.error(f"Kafka error [{repo['name']}]: {e}")
        logger.info(f"  {lang}: {len(repos)} repos queued")
    producer.flush()
    logger.info(f"GitHub batch complete. Total: {total} records")


def publish_weather(producer: KafkaProducer):
    total = 0
    for city_info in CITIES:
        data = fetch_weather_history(city_info["lat"], city_info["lon"])
        if not data or "daily" not in data:
            continue
        daily = data["daily"]
        dates = daily.get("time", [])
        for i, date in enumerate(dates):
            record = {
                "city": city_info["city"],
                "date": date,
                "temp_max_c": daily["temperature_2m_max"][i],
                "temp_min_c": daily["temperature_2m_min"][i],
                "precipitation_mm": daily["precipitation_sum"][i],
                "wind_speed_max_kmh": daily["wind_speed_10m_max"][i],
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            }
            try:
                producer.send(TOPIC_WEATHER, key=f"{city_info['city']}_{date}", value=record)
                total += 1
            except KafkaError as e:
                logger.error(f"Kafka error [{city_info['city']}_{date}]: {e}")
        logger.info(f"  {city_info['city']}: {len(dates)} days queued")
    producer.flush()
    logger.info(f"Weather batch complete. Total: {total} records")


def run():
    producer = create_producer()
    logger.info("=== Batch ingestion producer started ===")
    publish_github(producer)
    publish_weather(producer)
    logger.info("=== All batch data published to Kafka ===")


if __name__ == "__main__":
    run()
