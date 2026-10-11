# Crypto Market Pipeline — Batch & Streaming

Solo follow-up to the NASA Log Analysis Pipeline (group project), rebuilt with the
same batch+streaming skeleton, plus the skills that project was missing:
cloud data warehouse, dbt, data quality testing, and CI/CD.

## Architecture

```
[CoinGecko API] --(python producer)--> [Kafka topic: crypto-prices]
                                              |
                                    [Spark Structured Streaming]
                                    windowed agg (5min, watermark 10min)
                                              |
                                     [PostgreSQL - cloud, RDS/Supabase]
                                              |
                                         [dbt models + tests]
                                              |
                                    [Streamlit dashboard]

Batch side:
[CoinGecko historical API] --> [Airflow DAG, retries + alerting] --> [Postgres]

CI: GitHub Actions runs `dbt test` on every push.
```

## Why this project (vs. NASA logs)

| Skill | NASA logs | This project |
|---|---|---|
| Ingestion | Static CSV | Live API + Kafka producer |
| Streaming | Spark windowed agg | Same pattern, cloud sink |
| Storage | Local Postgres | **Cloud** Postgres (RDS/Supabase) |
| Transformations | Raw SQL views | **dbt** models + docs |
| Quality | Manual `filter()` | **dbt tests** (not_null, unique, ranges) |
| Orchestration | Simple DAG | Airflow with **retries/alerting** |
| CI/CD | None | **GitHub Actions** running tests on push |
| Dashboard | Power BI | Streamlit (faster to spin up solo) |

## 8-Day Learning Plan

Each day has: a short concept explanation (ask your mentor if unclear), a `TODO`
in the code for you to fill in, and 2-3 checkpoint questions. Don't move to the
next day until you can answer them without looking things up.

- **Day 1** — Kafka producer + Docker Compose (`producer/producer.py`)
- **Day 2** — Spark Structured Streaming windowed aggregation (`spark_streaming/streaming_job.py`)
- **Day 3** — Cloud Postgres (RDS free tier or Supabase) + budget alert
- **Day 4** — dbt staging + mart models (`dbt_project/models/`)
- **Day 5** — dbt tests (`dbt_project/models/staging/schema.yml`)
- **Day 6** — Airflow DAG with retries/alerting (`airflow/dags/backfill_dag.py`)
- **Day 7** — GitHub Actions CI (`.github/workflows/ci.yml`)
- **Day 8** — Streamlit dashboard + architecture diagram + polish README

## Setup

```bash
docker compose up -d          # starts Kafka, Zookeeper, local Postgres (for dev)
py -3.10 -m venv venv
venv\Scripts\activate         # Windows; use `source venv/bin/activate` on Mac/Linux
pip install -r requirements.txt
python producer/producer.py   # Day 1
```

**Day 6 (Airflow) runs via Docker, not pip** — see `airflow/docker-compose.airflow.yml`.
No install needed; just `docker compose -f airflow/docker-compose.airflow.yml up`
once your main `docker compose up -d` (Kafka/Postgres) is already running.

## Status

- [ ] Day 1 — Producer working, prices visible in Kafka topic
- [ ] Day 2 — Streaming job writing windowed aggregates to Postgres
- [ ] Day 3 — Cloud Postgres connected, budget alert confirmed active
- [ ] Day 4 — dbt models run cleanly (`dbt run`)
- [ ] Day 5 — dbt tests pass (`dbt test`)
- [ ] Day 6 — Airflow DAG backfills historical data, survives a simulated failure
- [ ] Day 7 — CI badge green on GitHub
- [ ] Day 8 — Dashboard deployed, README has architecture diagram
