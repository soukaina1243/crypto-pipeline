

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

COINS = ["bitcoin", "ethereum", "solana"]


def fetch_and_load_historical(coin: str, **context):
    import requests
    import psycopg2
    from datetime import datetime

    # 1. Call CoinGecko's historical endpoint
    url = f"https://api.coingecko.com/api/v3/coins/{coin}/market_chart"
    params = {"vs_currency": "usd", "days": 1}
    response = requests.get(url, params=params, timeout=15)
    response.raise_for_status()
    data = response.json()

    prices = data["prices"]  # list of [timestamp_ms, price]

    # 2. Connect to Postgres (Supabase)
    conn = psycopg2.connect(
        host="aws-0-eu-central-1.pooler.supabase.com",
        port=5432,
        dbname="postgres",
        user="postgres.fjgwwsffebkdjrghtqkv",
        password="ur password",
        sslmode="require",
        gssencmode="disable",
        connect_timeout=15,
    )
    cur = conn.cursor()

    # 3. Make sure the table exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS crypto_daily_history (
            coin TEXT,
            price_time TIMESTAMP,
            price_usd DOUBLE PRECISION
        );
    """)

    # 4. Insert each price point
    for timestamp_ms, price in prices:
        price_time = datetime.utcfromtimestamp(timestamp_ms / 1000)
        cur.execute(
            "INSERT INTO crypto_daily_history (coin, price_time, price_usd) VALUES (%s, %s, %s)",
            (coin, price_time, price),
        )

    conn.commit()
    cur.close()
    conn.close()

    print(f"[OK] loaded {len(prices)} historical points for {coin}")

def alert_on_failure(context):

    task_instance = context.get("task_instance")
    print(f"[ALERT] Task {task_instance.task_id} failed on {context.get('execution_date')}")
    


default_args = {
    "owner": "soukaina",
    "retries": 3,                        
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="crypto_daily_backfill",
    default_args=default_args,
    description="Daily backfill of historical crypto prices",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["crypto", "backfill"],
) as dag:

    for coin in COINS:
        PythonOperator(
            task_id=f"backfill_{coin}",
            python_callable=fetch_and_load_historical,
            op_kwargs={"coin": coin},
        )
