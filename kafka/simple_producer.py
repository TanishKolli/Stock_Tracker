import time
import json
import uuid
from datetime import datetime

import yfinance as yf
from confluent_kafka import Producer
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import pandas as pd  # Needed for pd.isna

# ---------------- Kafka setup ---------------- #
bootstrap_servers = "localhost:9092"
kafka_topic = "stock-data"
producer = Producer({"bootstrap.servers": bootstrap_servers})

# ---------------- Cassandra setup ---------------- #
CASSANDRA_HOSTS = ['localhost']
KEYSPACE = "stockks"
TABLE = "aggregated_stock_data"

def create_cassandra_connection():
    cluster = Cluster(CASSANDRA_HOSTS)
    session = cluster.connect()
    return session

def create_keyspace_and_table(session):
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }};
    """)

    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE} (
            ticker text,
            ts timestamp,
            open double,
            high double,
            low double,
            close double,
            volume bigint,
            PRIMARY KEY ((ticker), ts)
        );
    """)

def write_to_cassandra(session, record):
    stmt = SimpleStatement(f"""
        INSERT INTO {KEYSPACE}.{TABLE} (ticker, ts, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """)
    session.execute(stmt, (
        record["ticker"],
        datetime.fromisoformat(record["timestamp"]),
        record["open"],
        record["high"],
        record["low"],
        record["close"],
        record["volume"]
    ))

# ---------------- Stock tickers ---------------- #
TICKERS = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "META"]

def fetch_last_candles(tickers):
    """Fetch the last 1-minute candle for multiple tickers using yfinance in batch."""
    hist = yf.download(
        tickers=tickers,
        period="1d",
        interval="1m",
        group_by="ticker",
        threads=True,
        auto_adjust=True
    )

    records = []
    for ticker in tickers:
        df = hist if len(tickers) == 1 else hist[ticker]
        if df.empty:
            continue

        last_row = df.iloc[-1]

        # Replace NaN with defaults
        record = {
            "id": uuid.uuid4().hex,
            "ticker": ticker,
            "timestamp": str(last_row.name),
            "open": float(last_row["Open"]) if not pd.isna(last_row["Open"]) else 0.0,
            "high": float(last_row["High"]) if not pd.isna(last_row["High"]) else 0.0,
            "low": float(last_row["Low"]) if not pd.isna(last_row["Low"]) else 0.0,
            "close": float(last_row["Close"]) if not pd.isna(last_row["Close"]) else 0.0,
            "volume": int(last_row["Volume"]) if not pd.isna(last_row["Volume"]) else 0,
        }

        records.append(record)
    return records

# ---------------- Main ---------------- #
if __name__ == "__main__":
    cassandra_session = create_cassandra_connection()
    create_keyspace_and_table(cassandra_session)

    try:
        while True:
            records = fetch_last_candles(TICKERS)

            for record in records:
                json_str = json.dumps(record)

                # Produce to Kafka
                producer.produce(kafka_topic, key=record["ticker"], value=json_str)
                print(f"Produced to Kafka: {record}")

                # Write directly to Cassandra
                write_to_cassandra(cassandra_session, record)
                print(f"Written to Cassandra: {record}")

            producer.flush()
            time.sleep(60)

    except (KeyboardInterrupt, SystemExit):
        producer.flush()
        print("Exiting...")
