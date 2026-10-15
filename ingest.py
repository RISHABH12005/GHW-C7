import json
import logging
import os
import signal
import threading
import time
from datetime import datetime, timezone

import psycopg
from dotenv import load_dotenv
from twelvedata import TDClient

load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("financial-ingest")

API_KEY = os.getenv("TWELVEDATA_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")
SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "AAPL,MSFT").split(",") if s.strip()]
BATCH_SIZE = max(1, int(os.getenv("BATCH_SIZE", "25")))
FLUSH_SECONDS = max(1, int(os.getenv("FLUSH_SECONDS", "5")))
stop_event = threading.Event()
buffer = []
last_flush = time.monotonic()
QUOTE_POLL_SECONDS = max(15, int(os.getenv("QUOTE_POLL_SECONDS", "30")))
last_quote_poll = 0.0

def shutdown(_signum, _frame):
    log.info("Shutdown requested.")
    stop_event.set()


def parse_time(value):
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    if isinstance(value, str):
        try:
            number = float(value)
            return datetime.fromtimestamp(number, tz=timezone.utc)
        except ValueError:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc)

def normalize_event(event):
    """Normalize common Twelve Data wrapper event shapes, ignoring heartbeats/status."""
    if hasattr(event, "as_json"):
        event = event.as_json()
    if isinstance(event, str):
        try:
            event = json.loads(event)
        except json.JSONDecodeError:
            log.debug("Ignoring non-JSON websocket message.")
            return None
    if not isinstance(event, dict):
        return None
    if event.get("event") in {"heartbeat", "subscribe-status", "error"}:
        if event.get("event") == "error" or event.get("status") == "error":
            log.warning("Twelve Data websocket message: %s", event)
        return None
    price = event.get("price")
    symbol = event.get("symbol")
    if price is None or not symbol:
        return None
    try:
        price = float(price)
        if price <= 0:
            return None
        bid = float(event["bid"]) if event.get("bid") not in (None, "") else None
        ask = float(event["ask"]) if event.get("ask") not in (None, "") else None
        volume = float(event["day_volume"]) if event.get("day_volume") not in (None, "") else None
    except (TypeError, ValueError):
        log.warning("Ignoring malformed price message: %s", event)
        return None
    return (parse_time(event.get("timestamp")), str(symbol).upper(), price, bid, ask, volume, str(event.get("event", "price")))

def flush_batch():
    global last_flush
    if not buffer:
        last_flush = time.monotonic()
        return
    rows = list(buffer)
    # Keep timestamp precision as given by provider. ON CONFLICT handles duplicate ticks.
    with psycopg.connect(DATABASE_URL, connect_timeout=10) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO stocks_real_time
                    (time, symbol, price, bid, ask, day_volume, event_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                rows,
            )
    del buffer[:len(rows)]
    last_flush = time.monotonic()
    log.info("Inserted batch of %d tick rows.", len(rows))

def on_event(event):
    log.info("WebSocket message received (type=%s).", type(event).__name__)
    row = normalize_event(event)
    if row:
        buffer.append(row)
        if len(buffer) >= BATCH_SIZE or time.monotonic() - last_flush >= FLUSH_SECONDS:
            try:
                flush_batch()
            except Exception:
                log.exception("Batch insert failed; buffered rows retained for retry.")
                time.sleep(2)

def poll_rest_quotes(client):
    """Fetch current quote snapshots as a fallback and diagnostic for quiet WebSocket feeds."""
    global last_quote_poll
    success_count = 0
    for symbol in SYMBOLS:
        try:
            response = client.quote(symbol=symbol).as_json()
            if isinstance(response, list):
                response = response[0] if response else {}
            if not isinstance(response, dict):
                log.warning("REST quote for %s returned an unexpected payload type: %s", symbol, type(response).__name__)
                continue
            raw_price = response.get("price") or response.get("close")
            if raw_price in (None, ""):
                log.warning("REST quote for %s did not include price/close; response keys: %s", symbol, sorted(response.keys()))
                continue
            price = float(raw_price)
            if price <= 0:
                log.warning("REST quote for %s returned non-positive price.", symbol)
                continue
            timestamp = response.get("timestamp") or response.get("datetime")
            row = (parse_time(timestamp), symbol, price, None, None,
                   float(response["volume"]) if response.get("volume") not in (None, "") else None,
                   "quote_snapshot")
            buffer.append(row)
            success_count += 1
        except Exception as exc:
            log.warning("REST quote failed for %s (%s): %s", symbol, type(exc).__name__, exc)
    if buffer:
        try:
            flush_batch()
        except Exception:
            log.exception("Could not persist REST quote snapshots.")
    last_quote_poll = time.monotonic()
    log.info("REST quote poll completed: %d/%d symbols stored.", success_count, len(SYMBOLS))


def main():
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    if not API_KEY:
        raise SystemExit("TWELVEDATA_API_KEY is missing. Add it to .env.")
    if not DATABASE_URL:
        raise SystemExit("DATABASE_URL is missing. Add your Tiger Cloud connection URI to .env.")
    if not SYMBOLS:
        raise SystemExit("SYMBOLS is empty. Configure at least one supported ticker.")
    # Ensure the target schema exists before opening the socket.
    with psycopg.connect(DATABASE_URL, connect_timeout=10) as conn:
        conn.execute("SELECT 1 FROM stocks_real_time LIMIT 0")
    client = TDClient(apikey=API_KEY)
    log.info("Connecting to Twelve Data WebSocket for: %s", ", ".join(SYMBOLS))
    ws = client.websocket(symbols=SYMBOLS, on_event=on_event)
    try:
        ws.connect()
        # Persist an initial real quote even if this account has limited WebSocket entitlements.
        poll_rest_quotes(client)
        while not stop_event.wait(1):
            try:
                if time.monotonic() - last_flush >= FLUSH_SECONDS:
                    flush_batch()
                if time.monotonic() - last_quote_poll >= QUOTE_POLL_SECONDS:
                    poll_rest_quotes(client)
                if hasattr(ws, "heartbeat"):
                    ws.heartbeat()
            except Exception:
                log.exception("Pipeline heartbeat/flush issue; continuing.")
                time.sleep(2)
    finally:
        try:
            flush_batch()
        except Exception:
            log.exception("Final batch flush failed.")
        log.info("Ingestion worker stopped.")

if __name__ == "__main__":
    main()
