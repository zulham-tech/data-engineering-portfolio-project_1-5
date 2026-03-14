# 🗄️ Data Engineering Portfolio Projects

> **Ahmad Zulham Hamdan** — Data Engineer | Big Data & Cloud  
> Stack: Kafka · PySpark · Airflow · PostgreSQL · BigQuery · Redshift · MongoDB · dbt · Snowflake · Cassandra

[![Python](https://img.shields.io/badge/Python-3.10-blue)](https://python.org)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.6-black)](https://kafka.apache.org)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange)](https://spark.apache.org)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-green)](https://airflow.apache.org)

---

## 📁 Repository Structure

```
data-engineering-portfolio/
│
├── project1_iot_streaming_pipeline.ipynb       ← Real-time IoT streaming
├── dag_project1_iot_streaming.py
├── architecture_project1_streaming.svg
│
├── project2_batch_ingestion_pipeline.ipynb     ← Batch ingestion (GitHub + Weather)
├── dag_project2_batch_ingestion.py
├── architecture_project2_batch.svg
│
├── project3_reddit_sentiment_streaming.ipynb   ← Reddit NLP sentiment (NoSQL)
├── dag_project3_reddit_sentiment.py
├── architecture_project3_reddit.svg
│
├── project4_worldbank_dbt_snowflake.ipynb      ← World Bank ETL + dbt
├── dag_project4_worldbank_snowflake.py
├── architecture_project4_worldbank.svg
│
├── project5_crypto_lambda_architecture.ipynb   ← Lambda Architecture (Advanced)
├── dag_project5_crypto_lambda.py
├── architecture_project5_lambda.svg
│
├── sql_queries_both_projects.txt               ← SQL: Projects 1 & 2
├── sql_queries_project3_reddit.sql
├── sql_queries_project4_worldbank.sql
├── sql_queries_project5_crypto.sql
│
├── docker-compose.yml                          ← Full local dev stack
└── README.md                                   ← This file
```

---

## Project 1 — Real-Time IoT Sensor Streaming Pipeline

> **Type:** Streaming | **Difficulty:** Intermediate  
> **Stack:** Open-Meteo API → Kafka → PySpark → PostgreSQL → BigQuery → Airflow

### Data Source
Open-Meteo API (`open-meteo.com`) — free, no API key required.  
5 virtual IoT sensor nodes: Jakarta, Surabaya, Medan, Bandung, Makassar. Polled every 60 seconds.

### Architecture
```
Open-Meteo API (free · no key)
    ↓ (5 sensor nodes · 60s interval)
Kafka [iot-weather-sensors · 5 partitions · GZIP · 7-day retention]
    ↓ (PySpark micro-batch)
PySpark Structured Streaming
    → watermark 2min → 5-min tumbling window → z-score anomaly detection
    ↓ (foreachBatch)
PostgreSQL (operational)          BigQuery (analytics)
    iot_sensor_readings                ← hourly Airflow sync ←
    iot_sensor_aggregates_5min         partitioned by date
```

### Key Features
- 5-minute tumbling window aggregations (avg, min, max temperature per city)
- Z-score anomaly detection against 7-day rolling baseline
- Airflow BranchPythonOperator: skip BigQuery sync when no new records
- Data quality check: completeness threshold > 95% before sync

### Metrics
| Metric | Value |
|---|---|
| Events per day | ~7,200 (5 sensors × 60s × 24h) |
| End-to-end latency | < 60 seconds |
| Kafka partitions | 5 (one per sensor city) |
| BigQuery sync | Every 1 hour via Airflow |

---

## Project 2 — Batch Data Ingestion Pipeline

> **Type:** Batch | **Difficulty:** Intermediate  
> **Stack:** GitHub API + Open-Meteo Historical → Kafka → PySpark → PostgreSQL → S3 → Redshift → Airflow

### Data Sources
- **GitHub REST API** (`api.github.com`) — free, 5,000 req/hr with token. Top 30 repos × 5 languages (Python, Go, Rust, TypeScript, Java) = ~750 records/day
- **Open-Meteo Historical API** (`archive-api.open-meteo.com`) — free, no key. 90-day daily weather for 5 Indonesian cities = 450 records/run

### Architecture
```
GitHub REST API          Open-Meteo Historical API
        ↓                           ↓
Kafka [batch-github-repos]   Kafka [batch-weather-history]
                ↓ (PySpark batch)
        PySpark Transform
    → dedup → enrich → feature engineering
                ↓
        PostgreSQL (staging)
                ↓
          S3 (Parquet)
                ↓
        Amazon Redshift
    DISTKEY · SORTKEY · AZ64 · ZSTD
```

### Feature Engineering
| Feature | Formula |
|---|---|
| `activity_score` | stars×0.5 + forks×0.3 + watchers×0.2 |
| `stars_per_day` | stars / repo_age_days |
| `temp_range_c` | temp_max - temp_min |
| `is_rainy` | precipitation_sum > 1mm |

### Metrics
| Metric | Value |
|---|---|
| GitHub records/day | ~750 (30 repos × 5 languages) |
| Weather records/run | 450 (90 days × 5 cities) |
| Full pipeline SLA | < 15 minutes/day |
| Schedule | Daily 01:00–03:00 WIB |

---

## Project 3 — Real-Time Reddit Sentiment Streaming Pipeline

> **Type:** Streaming | **Difficulty:** Intermediate  
> **Stack:** Reddit PRAW → Kafka → PySpark → MongoDB (NoSQL) → BigQuery → Airflow

### Data Source
Reddit API via PRAW (`reddit.com/dev/api`) — free, no API key for read-only public subreddits.  
Subreddits: `r/technology`, `r/datascience`, `r/MachineLearning`, `r/programming`, `r/Python`

### Architecture
```
Reddit PRAW (live stream)
    ↓ (~200–500 posts/hour)
Kafka [reddit-posts-stream · 5 partitions · GZIP]
    ↓ (30s micro-batch)
PySpark Structured Streaming
    → VADER + TextBlob ensemble NLP (sentiment UDFs)
    ↓ (foreachBatch upsert by post_id)
MongoDB (NoSQL Data Lake)          BigQuery (Analytics)
    raw_posts collection                ← hourly Airflow sync ←
    sentiment_results collection        partitioned by date
```

### Sentiment Model
VADER + TextBlob ensemble: `ensemble_score = (vader_compound + textblob_polarity) / 2`

| Label | Condition |
|---|---|
| positive | ensemble_score ≥ 0.05 |
| neutral | -0.05 < ensemble_score < 0.05 |
| negative | ensemble_score ≤ -0.05 |

### Why MongoDB?
Reddit posts have flexible, unstructured schemas — some have body text, some are link-only, flairs vary per subreddit. MongoDB's document model stores all variations without a fixed schema, making it ideal as a NoSQL data lake for social media data.

### Metrics
| Metric | Value |
|---|---|
| Throughput | ~200–500 posts/hour |
| End-to-end latency | < 60 seconds |
| MongoDB TTL | None (full archive) |
| BigQuery sync | Hourly, BranchOperator skip if empty |

---

## Project 4 — World Bank Economic Indicators ETL Pipeline

> **Type:** Batch | **Difficulty:** Intermediate  
> **Stack:** World Bank API → Kafka → PySpark → PostgreSQL → dbt → Snowflake → Airflow

### Data Source
World Bank Open Data API (`api.worldbank.org/v2`) — completely free, no API key.  
8 macroeconomic indicators × 30 countries (G20 + ASEAN) × 30+ years = ~36,000 records/run.

**Indicators:** GDP, GDP growth %, GDP per capita, Inflation (CPI), Unemployment rate, Population, Internet users %, CO2 emissions per capita

### Architecture
```
World Bank Open API (free · no key)
    ↓ (8 indicators × 30 countries × 30 years)
Kafka [batch-worldbank-indicators · GZIP]
    ↓ (PySpark batch)
PySpark Transform
    → pivot indicators → YoY growth (window) → income group classification
    ↓
PostgreSQL (staging)
    ↓
dbt Core (3-layer transformation)
    staging/   stg_worldbank_indicators   → type casting, cleaning (view)
    intermediate/ int_indicators_pivoted  → normalization, null handling
    marts/     mart_country_economy       → economic health composite score
               mart_indicator_trends      → 30-year rolling analytics
    ↓
Snowflake (WORLD_BANK_DW)
    RAW schema → STAGING schema → MARTS schema
    clustered by (country_code, year)
```

### dbt Models
| Model | Layer | Materialization | Description |
|---|---|---|---|
| `stg_worldbank_indicators` | staging | view | Type casting, null handling |
| `int_indicators_pivoted` | intermediate | ephemeral | Normalization |
| `mart_country_economy` | marts | table, clustered | Economic health score |
| `mart_indicator_trends` | marts | table, partitioned | 30-year trend analytics |

### Economic Health Score
```
health_score = gdp_growth_pct × 0.4
             - inflation_pct  × 0.3
             - unemployment_pct × 0.3
```

### Metrics
| Metric | Value |
|---|---|
| Records per run | ~36,000 |
| dbt models | 4 (1 view + 2 tables + 1 ephemeral) |
| Snowflake cost | < $0.10/run (XS warehouse, auto-suspend) |
| Schedule | Daily 03:00 WIB |

---

## Project 5 — Crypto Market Lambda Architecture Pipeline ⚡

> **Type:** Lambda Architecture (Streaming + Batch) | **Difficulty:** Advanced  
> **Stack:** CoinGecko API → Kafka → PySpark (Speed + Batch) → Cassandra → Snowflake → Airflow

### Data Source
CoinGecko API (`api.coingecko.com`) — free tier, no API key required.  
Speed layer: top 50 coins polled every 60s. Batch layer: top 20 coins × 90-day OHLCV candles.

### Lambda Architecture
```
CoinGecko API (free · no key · top 50 coins)
              │
    ┌─────────┴─────────┐
    │                   │
⚡ SPEED LAYER       📦 BATCH LAYER
  (every 60s)          (daily)
    │                   │
    ↓                   ↓
Kafka               Kafka
[prices-realtime]   [ohlcv-batch]
    │                   │
    ↓                   ↓
PySpark Streaming   PySpark Batch
volatility UDF      7d/30d MA · golden cross
    │                   │
    ↓                   ↓
Cassandra           Snowflake
(serving layer)     (analytical layer)
    │
    ↓
latest_price_view   → O(1) · < 5ms latency
price_time_series   → 30-day TTL auto-expire
```

### Why Lambda Architecture?
| Query | Layer | Latency |
|---|---|---|
| "What is BTC price RIGHT NOW?" | Cassandra `latest_price_view` | < 5ms |
| "Top 10 most volatile coins today" | Cassandra `price_time_series` | < 10ms |
| "Show 30-day MA + golden cross" | Snowflake `CRYPTO_OHLCV_HISTORY` | seconds |
| "BTC vs altcoin correlation matrix" | Snowflake analytical query | minutes |

### Cassandra Schema Design
```
price_time_series:
  PRIMARY KEY (coin_id, event_time)  → partition by coin, cluster by time DESC
  TTL = 2,592,000s (30 days)        → auto-expire, no cleanup job needed

latest_price_view:
  PRIMARY KEY (coin_id)              → O(1) single-row lookup per coin
```

### Airflow: 2 Separate DAGs
| DAG | Schedule | Tasks |
|---|---|---|
| `crypto_speed_layer` | Every 5 minutes | API check → fetch prices → Kafka → Spark → Cassandra |
| `crypto_batch_layer` | Daily 04:00 WIB | API check → OHLCV extract → Kafka → Spark → Snowflake |

### Metrics
| Metric | Value |
|---|---|
| Speed layer throughput | ~72,000 events/hour (50 coins × 60s) |
| Cassandra read latency | < 5ms |
| Batch OHLCV records | 20 coins × 90 candles = 1,800/run |
| Technical indicators | daily return %, 7d MA, 30d MA, golden cross |

---

## 🛠️ Full Tech Stack

| Technology | Projects | Role |
|---|---|---|
| **Apache Kafka** | All 5 | Message queue / event streaming |
| **PySpark** | All 5 | Batch & streaming data processing |
| **Apache Airflow** | All 5 | Pipeline orchestration & scheduling |
| **PostgreSQL** | 1, 2, 4 | Operational DB / staging layer |
| **Google BigQuery** | 1, 3 | Cloud analytical data warehouse |
| **Amazon Redshift** | 2 | Cloud analytical data warehouse |
| **MongoDB** | 3 | NoSQL data lake (unstructured docs) |
| **Snowflake** | 4, 5 | Cloud analytical data warehouse |
| **Apache Cassandra** | 5 | NoSQL serving layer (low-latency) |
| **dbt Core** | 4 | SQL transformation layer |
| **Amazon S3** | 2 | Object storage (Parquet staging) |
| **Docker Compose** | All 5 | Local development environment |

---

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- Docker Desktop
- Snowflake free trial (Projects 4, 5)
- Google Cloud account — BigQuery free tier (Projects 1, 3)
- AWS free tier — S3 + Redshift (Project 2)

### Run Locally

```bash
# 1. Clone repository
git clone https://github.com/zulhamva-tech/data-engineering-portfolio.git
cd data-engineering-portfolio

# 2. Start all services
docker compose up -d

# 3. Access services
# Airflow   → http://localhost:8080  (admin / admin)
# Kafka UI  → http://localhost:9090
# Jupyter   → http://localhost:8888  (token: datatengineer)
# PgAdmin   → http://localhost:5050  (admin@admin.com / admin)

# 4. Set Airflow Variables (UI → Admin → Variables):
#   KAFKA_BOOTSTRAP        = localhost:9092
#   GCP_PROJECT_ID         = your-gcp-project
#   BQ_DATASET             = iot_weather_streaming
#   GITHUB_TOKEN           = your-github-token
#   SNOWFLAKE_ACCOUNT      = your_account.ap-southeast-1
#   SNOWFLAKE_USER         = your_username
#   SNOWFLAKE_PASSWORD     = your_password
#   MONGO_URI              = mongodb://localhost:27017

# 5. Trigger any DAG from Airflow UI
```

### Python Dependencies

```bash
pip install praw kafka-python pyspark pymongo textblob vaderSentiment \
            google-cloud-bigquery snowflake-connector-python \
            cassandra-driver dbt-snowflake boto3 pandas numpy \
            requests sqlalchemy psycopg2-binary
```

---

## 📊 Project Comparison

| | Project 1 | Project 2 | Project 3 | Project 4 | Project 5 |
|---|---|---|---|---|---|
| **Type** | Streaming | Batch | Streaming | Batch | Lambda |
| **Source** | Open-Meteo | GitHub + Weather | Reddit PRAW | World Bank | CoinGecko |
| **NoSQL** | — | — | MongoDB | — | Cassandra |
| **Warehouse** | BigQuery | Redshift | BigQuery | Snowflake | Snowflake |
| **Transform** | PySpark | PySpark | PySpark + NLP | PySpark + dbt | PySpark × 2 |
| **Difficulty** | ⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |

---

## 👤 Author

**Ahmad Zulham Hamdan**  
Data Engineer | Big Data & Cloud  

📧 zulham.va@gmail.com  
🔗 [linkedin.com/in/ahmad-zulham-hamdan-665170279](https://linkedin.com/in/ahmad-zulham-hamdan-665170279)  
🐙 [github.com/zulhamva-tech](https://github.com/zulhamva-tech)

---

*Built with ❤️ using open-source tools and free public APIs*
