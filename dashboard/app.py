"""
DAY 8 — Streamlit dashboard

Reads from the `daily_coin_summary` dbt mart and plots price + volatility
per coin. Deploy this on Streamlit Cloud (same as your real estate
project) for a live demo link on your CV.
"""

import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px

st.set_page_config(page_title="Crypto Pipeline Dashboard", page_icon="📈", layout="wide")

DB_CONFIG = {
    "host": "localhost",  # TODO: your cloud Postgres host
    "port": 5432,
    "dbname": "crypto_pipeline",
    "user": "crypto",
    "password": "crypto_dev_only",
}


@st.cache_data(ttl=300)
def load_summary() -> pd.DataFrame:
    """
    TODO:
    Connect with psycopg2 using DB_CONFIG, run:
      SELECT * FROM daily_coin_summary ORDER BY date;
    and return as a pandas DataFrame (pd.read_sql works well here).
    """
    raise NotImplementedError("TODO: implement load_summary()")


st.title("📈 Crypto Market Pipeline")
st.caption("Batch + streaming pipeline: Kafka → Spark → Postgres (cloud) → dbt → Streamlit")

df = load_summary()

coin = st.selectbox("Coin", df["coin"].unique())
filtered = df[df["coin"] == coin]

col1, col2 = st.columns(2)
with col1:
    fig = px.line(filtered, x="date", y="daily_avg_price", title=f"{coin} — daily average price")
    st.plotly_chart(fig, use_container_width=True)
with col2:
    fig2 = px.bar(filtered, x="date", y="volatility", title=f"{coin} — daily volatility")
    st.plotly_chart(fig2, use_container_width=True)
