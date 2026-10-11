"""
DAY 1 — Kafka Producer

CONCEPT:
A Kafka "producer" is anything that pushes messages onto a topic.
Here, your producer polls a public API every few seconds and publishes
each price tick as a JSON message — exactly like your NASA project's
KafkaLogProducer.scala, but pulling live data instead of replaying a CSV.

Your job: fill in the two TODOs below. Everything else (connection,
loop, error handling) is done for you so you can focus on the actual
producer logic.

Run with: python producer/producer.py
"""

import json
import time
import requests
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "crypto-prices"
COINS = ["bitcoin", "ethereum", "solana"]
POLL_INTERVAL_SECONDS = 15

# CoinGecko free public API, no key needed
API_URL = "https://api.coingecko.com/api/v3/simple/price"


def fetch_prices(coin_ids: list[str]) -> dict:
    params = {
        "ids": ",".join(coin_ids),       # coin_ids is a list, API wants "bitcoin,ethereum,solana"
        "vs_currencies": "usd",
        "x_cg_demo_api_key": "CG-jSmNRhxuX17b7hSF6JZ6opE6",
    }
    response = requests.get(API_URL, params=params)

    if response.status_code != 200:
        print(f"[WARN] CoinGecko returned {response.status_code}: {response.text}")
        return {}

    return response.json() 
    
def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def run():
    producer = build_producer()
    print(f"Producer started. Publishing to topic '{TOPIC}' every {POLL_INTERVAL_SECONDS}s.")

    while True:
        try:
            prices = fetch_prices(COINS)
            timestamp = int(time.time())

            for coin, data in prices.items():
                message = {
                    "coin": coin,
                    "price_usd": data["usd"],
                    "timestamp": timestamp,
                }
                
                # Send `message` to the Kafka topic `TOPIC`.
                # Use `coin` as the message key (bytes), like NASA used `host`
                # as the key — this keeps all ticks for the same coin on the
                # same partition, preserving order per coin.
                producer.send(TOPIC, key=coin.encode("utf-8"), value=message)
                # raise NotImplementedError("TODO 2: send message to Kafka")

            producer.flush()
            print(f"[{timestamp}] published {len(prices)} price ticks")

        except Exception as e:
            print(f"[WARN] cycle failed: {e}")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()
