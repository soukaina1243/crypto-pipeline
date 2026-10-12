# Crypto Market Pipeline — Batch & Streaming

An end-to-end data pipeline: live crypto prices ingested via Kafka, aggregated
in real time with Spark Structured Streaming, stored in a cloud Postgres database
(Supabase), transformed and tested with dbt, orchestrated with Airflow, verified
on every push with CI, and visualized in a Streamlit dashboard.

## Architecture

```
[CoinGecko API] --(python producer)--> [Kafka topic: crypto-prices]
                                              |
                                    [Spark Structured Streaming]
                                 windowed agg (5min, watermark 10min)
                                              |
                                  [PostgreSQL - cloud, Supabase]
                                              |
                                  [dbt: staging model + tests]
                                              |
                                  [dbt: daily_coin_summary mart]
                                              |
                                    [Streamlit dashboard]

Batch side:
[CoinGecko historical API] --> [Airflow DAG, retries + alerting] --> [Postgres]

CI: GitHub Actions runs `dbt run` + `dbt test` on every push to main.
```

## What each piece does

| Component | Role |
|---|---|
| **Kafka producer** (`producer/producer.py`) | Polls CoinGecko every 15s for bitcoin/ethereum/solana prices, publishes to Kafka keyed by coin (preserves per-coin ordering). Fault-tolerant: a failed API call logs a warning and the loop continues. |
| **Spark Structured Streaming** (`spark_streaming/streaming_job.py`) | Reads the Kafka topic continuously, aggregates into 5-minute tumbling windows per coin (avg/min/max price) with a 10-minute watermark for late data, writes each micro-batch to Postgres via JDBC with fault-tolerant `foreachBatch`. |
| **Cloud Postgres** (Supabase) | Stores raw windowed aggregates. Accessed through Supabase's session connection pooler (port 5432) rather than the direct connection, for IPv4 compatibility. |
| **dbt** (`dbt_project/`) | Staging model (`stg_crypto_prices`) cleans and types the raw table. Mart model (`daily_coin_summary`) rolls up to daily per-coin statistics. 5 automated tests: `not_null`, `accepted_values`, `accepted_range`. |
| **Airflow** (`airflow/`, run via Docker) | A daily batch DAG (`crypto_daily_backfill`) backfills historical daily prices per coin into a separate table, with retries (3x) and an `on_failure_callback` alert. |
| **CI** (`.github/workflows/ci.yml`) | On every push: spins up a throwaway Postgres, installs dbt, runs `dbt deps` / `dbt run` / `dbt test` against seeded schema. |
| **Dashboard** (`dashboard/app.py`) | Streamlit app reading `daily_coin_summary`, showing daily average price and volatility per coin. |

## Setup

```bash
docker compose up -d          # starts Kafka, Zookeeper, local Postgres (dev only)
py -3.10 -m venv venv
venv\Scripts\activate         # Windows; `source venv/bin/activate` on Mac/Linux
pip install -r requirements.txt
```

Set credentials as environment variables rather than hardcoding them:
```bash
$env:SUPABASE_PASSWORD = "your-password-here"
```

Run the producer and streaming job in separate terminals:
```bash
python producer/producer.py
python spark_streaming/streaming_job.py
```

dbt:
```bash
cd dbt_project
dbt deps
dbt run
dbt test
```

Airflow (Docker, no pip install needed):
```bash
docker compose -f airflow/docker-compose.airflow.yml up
```
Open http://localhost:8080 — credentials are printed in the container logs on first startup.

Dashboard:
```bash
streamlit run dashboard/app.py
```

## Known limitations

- **Port 5432 is blocked on some networks** (observed on a home ISP/router during
  development). Supabase's pooler was unreachable over standard WiFi but worked
  over mobile hotspot — a real-world networking constraint, not a bug in the
  pipeline. A deployed version behind a normal cloud network wouldn't hit this.
- **Local Spark checkpoint state is flaky on Windows** for long-running
  Structured Streaming jobs (`HDFSBackedStateStoreProvider` occasionally fails
  to read its own delta files after extended runtime). This is a known
  Windows/Hadoop compatibility issue; running Spark on Linux, WSL2, or a real
  cluster avoids it.
- **`daily_coin_summary` is a static dbt table**, not automatically refreshed.
  In production this would be rebuilt on a schedule (e.g. a `dbt run` step in
  Airflow) rather than manually.
- **Data in the dashboard reflects development-session uptime**, not continuous
  operation — gaps between days correspond to when the pipeline was actually
  running during development, not missing/lost data.
- **Windows-specific setup friction**: this project surfaced several
  Windows-only issues (`winutils.exe`/Hadoop native libraries, PySpark launcher
  scripts breaking on paths with parentheses, venv launcher scripts breaking
  after a folder rename) that wouldn't occur on Linux/Mac. Documented here as
  a practical illustration of why many data engineering teams containerize
  everything or develop on Linux/WSL2.

## Possible next steps

- Schedule `dbt run` inside the Airflow DAG to keep the mart table fresh automatically
- Add Great Expectations or additional dbt tests for anomaly detection (e.g. price spike thresholds)
- Deploy the Streamlit dashboard publicly (Streamlit Community Cloud)
- Swap local Spark checkpointing for a more robust setup (Linux-based runner or managed Spark)

## Technologies

Python · Apache Kafka · Apache Spark (Structured Streaming) · PostgreSQL (Supabase)
· dbt · Apache Airflow · Docker · GitHub Actions · Streamlit · Plotly
