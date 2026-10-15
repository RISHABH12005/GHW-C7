# Real-Time Financial Data Ingestion into Tiger Cloud

A Python data pipeline for streaming financial price updates from Twelve Data into Tiger Cloud (PostgreSQL + TimescaleDB). The pipeline stores timestamped price/bid/ask/volume events in a hypertable, handles duplicate timestamps safely, and includes SQL for checking ingestion and aggregating ticks into OHLC candlesticks.

## Architecture

Twelve Data WebSocket → Python ingestion worker → Tiger Cloud `stocks_real_time` hypertable → SQL analytics / OHLC view.

## Requirements

- Python 3.10+
- A Tiger Cloud service with TimescaleDB enabled
- A Twelve Data account and API key with access to the symbols/WebSocket plan you use
- Tiger Cloud PostgreSQL connection string

## Configure secrets

Copy `.env.example` to `.env` and fill in your own values. Never commit `.env`, API keys, or database credentials.

```bash
python -m venv .venv
source .venv/bin/activate        # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
cp .env.example .env             # Windows: copy .env.example .env
```

Set:
- `TWELVEDATA_API_KEY` — your Twelve Data API key.
- `DATABASE_URL` — Tiger Cloud PostgreSQL connection URI, including SSL mode if required (for example `?sslmode=require`).
- `SYMBOLS` — comma-separated symbols supported by your Twelve Data plan, default `AAPL,MSFT`.
- `BATCH_SIZE` — in-memory batch flush size, default `25`.

## Run the pipeline

Initialize schema and hypertable:

```bash
python setup_db.py
```

Start ingesting data:

```bash
python ingest.py
```

Stop gracefully with Ctrl+C. Rows are written in small batches to reduce per-tick transaction overhead.

## Validate and query

Run the SQL in [sql/analytics.sql](sql/analytics.sql) in your Tiger Cloud SQL editor. It includes recent ticks, per-symbol summaries, and a minute-level OHLC view.

## Notes

- The Twelve Data WebSocket requires a valid API key and a subscription that permits the symbols and real-time access requested. Market data may only arrive during relevant trading sessions.
- This repository includes code for a real-time pipeline, but this repo itself does not contain a Twelve Data key or a live Tiger Cloud service URL.
- Use this for technical learning and data engineering demonstrations, not as financial advice.

## References

- [MLH challenge: Ingest real-time financial data into Tiger Cloud](https://www.mlh.com/events/global-hack-week-open-source-90/challenges)
- [Tiger Data WebSocket tutorial](https://www.tigerdata.com/docs/tutorials/financial-ingest-real-time/)
