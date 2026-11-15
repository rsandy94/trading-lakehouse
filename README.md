# Trading Lakehouse

A streaming trade-execution data pipeline: **Kafka → Snowpipe Streaming (Apache Iceberg)** orchestrated with **Dagster + dbt**.

## What it does

1. A Python producer generates synthetic trade execution events (500 valid + 5 poison-pills for testing quarantine rules) and streams them to Kafka.
2. The Snowflake Kafka Connect sink (Snowpipe Streaming, Iceberg-enabled) lands the raw events into a Bronze Iceberg table.
3. Dagster orchestrates the downstream dbt transformations into the lakehouse layers.

## Components

| Path | Purpose |
|---|---|
| `docker-compose.yml` | Local Kafka (KRaft), Kafka Connect with the Snowflake connector, Kafka UI |
| `scripts/kafka_trade_producer.py` | Publishes trade execution events to the `trading.executions.v1` topic |
| `dagster_orchestrator/definitions.py` | Dagster assets/jobs |
| `connect/snowflake_iceberg_sink.json` | Kafka Connect sink connector config (sanitized template) |
| `requirements.txt` | Python environment (Dagster, dbt, confluent-kafka, Snowflake connector) |

## Quick start

```bash
# 1. Start the local streaming stack
docker compose up -d

# 2. Create the Python environment
python -m venv venv
venv\Scripts\pip install -r requirements.txt

# 3. Configure secrets
copy .env.example .env   # then fill in your Snowflake values

# 4. Stream events
venv\Scripts\python scripts\kafka_trade_producer.py

# 5. Register the sink connector (after filling in real values in the JSON)
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @connect/snowflake_iceberg_sink.json
```

## Running Dagster

The dbt project lives in a separate repo, [trading-lakehouse-dbt](https://github.com/rsandy94/trading-lakehouse-dbt). Clone it, set `DBT_PROJECT_DIR` in `.env` to that checkout, then start Dagster from this repo:

```bash
dagster dev -f dagster_orchestrator/definitions.py
```

- **Sensor:** `bronze_iceberg_stream_sensor` calls `SYSTEM$STREAM_HAS_DATA` on `BRONZE.TRADING.TRADE_EVENTS_STREAM` every 60 seconds and launches `trading_pipeline_job` when committed rows are waiting. It skips while a previous run is still in progress.
- **Quality check:** `check_trade_quarantine_threshold` runs after `fct_trade_executions`. It computes the pass rate of the latest batch (rows promoted to Gold vs. rows quarantined) and fails below `QUARANTINE_PASS_RATE_THRESHOLD_PCT` (default 98%).
- **Alerts:** if `ALERT_WEBHOOK_URL` is set to a Slack incoming webhook, a failed check posts the pass rate and quarantine count to it.

## Security note

The connector configs are committed as **sanitized templates** — the Snowflake private key, account identifier, service user, and role are placeholders. Keep real credentials in `.env` / `keys/` (both gitignored) or a secrets manager. Never commit private keys.
