"""
Project 4 — World Bank ETL Pipeline Loader
Fetches World Bank API → Kafka → PySpark → PostgreSQL staging → dbt → Snowflake
"""

import json
import logging
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "batch-worldbank-indicators"

WB_API = "https://api.worldbank.org/v2/country/{country}/indicator/{indicator}"

INDICATORS = {
    "NY.GDP.MKTP.CD": "gdp_usd",
    "NY.GDP.MKTP.KD.ZG": "gdp_growth_pct",
    "NY.GDP.PCAP.CD": "gdp_per_capita_usd",
    "FP.CPI.TOTL.ZG": "inflation_cpi_pct",
    "SL.UEM.TOTL.ZS": "unemployment_pct",
    "SP.POP.TOTL": "population",
    "IT.NET.USER.ZS": "internet_users_pct",
    "EN.ATM.CO2E.PC": "co2_per_capita",
}

# G20 + ASEAN countries
COUNTRIES = [
    "US", "CN", "JP", "DE", "GB", "FR", "IN", "IT", "BR", "CA",
    "RU", "KR", "AU", "MX", "ID", "TR", "SA", "AR", "ZA", "EU",
    "SG", "MY", "TH", "PH", "VN", "MM", "KH", "LA", "BN",
]
YEARS_BACK = 30


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        compression_type="gzip",
        retries=5,
        acks="all",
    )


def fetch_indicator(country: str, indicator: str) -> list:
    url = WB_API.format(country=country, indicator=indicator)
    params = {
        "format": "json",
        "per_page": YEARS_BACK + 5,
        "mrv": YEARS_BACK,
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if len(data) >= 2:
            return data[1] or []
        return []
    except requests.RequestException as e:
        logger.error(f"WB API error [{country}/{indicator}]: {e}")
        return []


def run():
    producer = create_producer()
    total = 0

    for country in COUNTRIES:
        for indicator_code, indicator_name in INDICATORS.items():
            records = fetch_indicator(country, indicator_code)
            for rec in records:
                if rec.get("value") is None:
                    continue
                payload = {
                    "country_code": country,
                    "country_name": rec.get("country", {}).get("value", country),
                    "indicator_code": indicator_code,
                    "indicator_name": indicator_name,
                    "year": int(rec.get("date", 0)),
                    "value": float(rec["value"]),
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                }
                try:
                    key = f"{country}_{indicator_code}_{payload['year']}"
                    producer.send(TOPIC, key=key, value=payload)
                    total += 1
                except KafkaError as e:
                    logger.error(f"Kafka error: {e}")

        logger.info(f"  {country}: indicators queued")

    producer.flush()
    logger.info(f"World Bank batch complete. Total records: {total}")


if __name__ == "__main__":
    run()
