import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "trading.executions.v1"

def delivery_report(err, msg):
    """Callback invoked once per message to confirm Kafka broker ack."""
    if err is not None:
        print(f"[ERROR] Message delivery failed: {err}")

def build_trade_events():
    tickers = ["AAPL", "NVDA", "MSFT", "AMZN", "META"]
    traders = ["TRD-1001", "TRD-1002", "TRD-1003"]
    events = []
    now = datetime.now(timezone.utc)

    # 1. Generate 500 Valid Market Execution Events
    # NOTE: Real partition/offset are assigned by the Kafka broker and by the
    # Snowflake connector (RECORD_METADATA) — never embed them in the payload.
    for i in range(1, 501):
        exec_time = (now - timedelta(minutes=random.randint(1, 120))).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        ingested_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        events.append({
            "TRADE_ID": f"EXE-{uuid.uuid4().hex[:8].upper()}",
            "TRADER_ID": random.choice(traders),
            "TICKER_SYMBOL": random.choice(tickers),
            "ORDER_SIDE": random.choice(["BUY", "SELL"]),
            "ORDER_STATUS": "EXECUTED",
            "QUANTITY": round(random.uniform(10, 500), 2),
            "EXECUTION_PRICE": round(random.uniform(120, 950), 2),
            "COMMISSION_FEE": 1.25,
            "EXECUTED_AT": exec_time,
            "_KAFKA_INGESTED_AT": ingested_time,
        })

    # 2. Inject 5 Poison-Pill Events to trigger the Silver Split-Gate Quarantine
    now_str = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    poison_events = [
        # Rule Failure 1: Negative execution price (-420.00)
        {
            "TRADE_ID": "EXE-BAD-001", "TRADER_ID": "TRD-1001", "TICKER_SYMBOL": "NVDA",
            "ORDER_SIDE": "BUY", "ORDER_STATUS": "EXECUTED", "QUANTITY": 50.0,
            "EXECUTION_PRICE": -450.00, "COMMISSION_FEE": 1.25, "EXECUTED_AT": now_str,
            "_KAFKA_INGESTED_AT": now_str,
        },
        # Rule Failure 2: Invalid ticker format (contains '$' and exceeds 5 chars)
        {
            "TRADE_ID": "EXE-BAD-002", "TRADER_ID": "TRD-1002", "TICKER_SYMBOL": "INVALID$SYM",
            "ORDER_SIDE": "SELL", "ORDER_STATUS": "EXECUTED", "QUANTITY": 10.0,
            "EXECUTION_PRICE": 185.00, "COMMISSION_FEE": 1.25, "EXECUTED_AT": now_str,
            "_KAFKA_INGESTED_AT": now_str,
        },
        # Rule Failure 3: Null mandatory trader_id key
        {
            "TRADE_ID": "EXE-BAD-003", "TRADER_ID": None, "TICKER_SYMBOL": "AAPL",
            "ORDER_SIDE": "BUY", "ORDER_STATUS": "EXECUTED", "QUANTITY": 25.0,
            "EXECUTION_PRICE": 210.50, "COMMISSION_FEE": 1.25, "EXECUTED_AT": now_str,
            "_KAFKA_INGESTED_AT": now_str,
        },
        # Rule Failure 4: Non-positive quantity (-15.0)
        {
            "TRADE_ID": "EXE-BAD-004", "TRADER_ID": "TRD-1003", "TICKER_SYMBOL": "MSFT",
            "ORDER_SIDE": "BUY", "ORDER_STATUS": "EXECUTED", "QUANTITY": -20.0,
            "EXECUTION_PRICE": 415.00, "COMMISSION_FEE": 1.25, "EXECUTED_AT": now_str,
            "_KAFKA_INGESTED_AT": now_str,
        },
        # Rule Failure 5: Timestamp out of bounds (Year 2015)
        {
            "TRADE_ID": "EXE-BAD-005", "TRADER_ID": "TRD-1001", "TICKER_SYMBOL": "AMZN",
            "ORDER_SIDE": "SELL", "ORDER_STATUS": "EXECUTED", "QUANTITY": 30.0,
            "EXECUTION_PRICE": 190.00, "COMMISSION_FEE": 1.25, "EXECUTED_AT": "2015-11-01 09:30:00.000",
            "_KAFKA_INGESTED_AT": now_str,
        },
    ]
    events.extend(poison_events)
    return events

def publish_stream():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    events = build_trade_events()

    print(f"Publishing {len(events)} trade execution events to Kafka topic '{KAFKA_TOPIC}'...")
    for idx, payload in enumerate(events, start=1):
        producer.produce(
            topic=KAFKA_TOPIC,
            key=payload["TRADE_ID"].encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
            callback=delivery_report,
        )
        if idx % 100 == 0:
            producer.poll(0.1)
            print(f"  -> Streamed {idx}/{len(events)} messages...")

    producer.flush()
    print("All 505 messages flushed to Kafka! Snowpipe Streaming will commit to S3 Iceberg within ~10 seconds.")

if __name__ == "__main__":
    publish_stream()