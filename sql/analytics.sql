-- Confirm the hypertable is receiving events.
SELECT time, symbol, price, bid, ask, day_volume, received_at
FROM stocks_real_time
ORDER BY time DESC
LIMIT 50;

-- Latest value for each symbol.
SELECT DISTINCT ON (symbol)
       symbol, time, price, bid, ask, day_volume
FROM stocks_real_time
ORDER BY symbol, time DESC;

-- One-minute OHLC + volume observations.
SELECT time_bucket(INTERVAL '1 minute', time) AS bucket,
       symbol,
       first(price, time) AS open,
       max(price) AS high,
       min(price) AS low,
       last(price, time) AS close,
       max(day_volume) AS latest_reported_day_volume,
       count(*) AS ticks
FROM stocks_real_time
WHERE time > now() - INTERVAL '1 day'
GROUP BY bucket, symbol
ORDER BY bucket DESC, symbol;

-- Recent ingestion activity by symbol.
SELECT symbol, count(*) AS tick_count,
       min(time) AS first_tick, max(time) AS last_tick,
       max(received_at) AS last_received_at
FROM stocks_real_time
WHERE received_at > now() - INTERVAL '1 hour'
GROUP BY symbol
ORDER BY tick_count DESC;
