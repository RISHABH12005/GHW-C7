import os
import sys
import psycopg
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    sys.exit("DATABASE_URL is missing. Configure it in your local .env file.")

SCHEMA = """
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS company (
    symbol TEXT PRIMARY KEY,
    name TEXT
);

CREATE TABLE IF NOT EXISTS stocks_real_time (
    time TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    bid DOUBLE PRECISION,
    ask DOUBLE PRECISION,
    day_volume DOUBLE PRECISION,
    event_type TEXT NOT NULL DEFAULT 'price',
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT create_hypertable(
    'stocks_real_time', by_range('time'),
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS stocks_real_time_symbol_time
ON stocks_real_time (symbol, time DESC);
"""

def main():
    with psycopg.connect(DATABASE_URL, connect_timeout=10) as conn:
        conn.execute(SCHEMA)
        conn.execute("INSERT INTO company (symbol, name) VALUES ('AAPL', 'Apple Inc.'), ('MSFT', 'Microsoft Corporation') ON CONFLICT (symbol) DO NOTHING;")
    print("Tiger Cloud schema is ready: company and stocks_real_time.")

if __name__ == "__main__":
    main()
