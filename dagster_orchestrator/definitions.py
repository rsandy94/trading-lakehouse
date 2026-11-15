import json
import os
import time
import urllib.request
from pathlib import Path

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetCheckExecutionContext,
    Definitions,
    DefaultSensorStatus,
    RunRequest,
    SensorEvaluationContext,
    DagsterRunStatus,
    RunsFilter,
    SkipReason,
    asset_check,
    define_asset_job,
    sensor,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets, get_asset_key_for_model

# Minimum share of a batch (Gold rows / (Gold + quarantine rows)) that must pass the
# Silver quality rules. Below this, the asset check fails and alerts on-call.
PASS_RATE_THRESHOLD_PCT = float(os.getenv("QUARANTINE_PASS_RATE_THRESHOLD_PCT", "98.0"))

# Optional Slack (or Slack-compatible) incoming webhook for check failures.
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL")

if not os.getenv("DBT_PROJECT_DIR"):
    raise RuntimeError(
        "Set DBT_PROJECT_DIR to your local trading-lakehouse-dbt checkout (see .env.example)."
    )
dbt_project_dir = Path(os.environ["DBT_PROJECT_DIR"]).resolve()
dbt_profiles_dir = Path(os.getenv("DBT_PROFILES_DIR", Path.home() / ".dbt")).resolve()

dbt_project = DbtProject(project_dir=dbt_project_dir, profiles_dir=dbt_profiles_dir)
dbt_project.prepare_if_dev()

@dbt_assets(manifest=dbt_project.manifest_path)
def trading_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

trading_pipeline_job = define_asset_job(
    name="trading_pipeline_job",
    selection="tag:trading_pipeline",
)

def get_snowflake_connection():
    auth_kwargs = {}
    if os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"):
        # Key-pair authentication (matches the dbt profiles.yml setup)
        with open(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"], "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=os.getenv("SNOWFLAKE_KEY_PASSWORD").encode()
                if os.getenv("SNOWFLAKE_KEY_PASSWORD")
                else None,
            )
        auth_kwargs["private_key"] = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    else:
        # Fallback: password authentication
        auth_kwargs["password"] = os.getenv("SNOWFLAKE_PASSWORD")

    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        role=os.getenv("SNOWFLAKE_ROLE"),  # least-privilege role from .env; no ACCOUNTADMIN default
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database="BRONZE",
        schema="TRADING",
        **auth_kwargs,
    )

@sensor(
    job=trading_pipeline_job,
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.RUNNING,
    description="Polls Snowflake Cloud Services metadata layer at zero warehouse compute cost.",
)
def bronze_iceberg_stream_sensor(context: SensorEvaluationContext):
    """
    Executes SYSTEM$STREAM_HAS_DATA(), which is answered entirely by Snowflake's
    Cloud Services metadata layer without waking up a Virtual Warehouse.
    """

    # Filter in the query: limit=5 across all jobs could miss an active run of this one.
    in_progress_runs = context.instance.get_runs(
        filters=RunsFilter(
            job_name="trading_pipeline_job",
            statuses=[DagsterRunStatus.QUEUED, DagsterRunStatus.STARTING, DagsterRunStatus.STARTED],
        ),
        limit=1,
    )
    if in_progress_runs:
        return SkipReason(
            f"Run {in_progress_runs[0].run_id[:8]} currently in progress. Skipping evaluation."
        )
    
    conn = get_snowflake_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT SYSTEM$STREAM_HAS_DATA('BRONZE.TRADING.TRADE_EVENTS_STREAM')"
            )
            has_new_iceberg_commits = cur.fetchone()[0]

        if has_new_iceberg_commits:
            now_epoch = str(int(time.time()))
            # Capture the cursor BEFORE updating it, otherwise the run key is
            # always fresh and never deduplicates repeat triggers.
            previous_cursor = context.cursor
            context.update_cursor(now_epoch)
            return RunRequest(
                run_key=f"iceberg_stream_commit_{previous_cursor or 'init'}",
                run_config={},
            )
        return SkipReason("No new Iceberg manifest commits detected in TRADE_EVENTS_STREAM.")
    finally:
        conn.close()

def send_alert(text: str) -> None:
    """Post to ALERT_WEBHOOK_URL if configured. Never raises: alerting must not break the check."""
    if not ALERT_WEBHOOK_URL:
        return
    request = urllib.request.Request(
        ALERT_WEBHOOK_URL,
        data=json.dumps({"text": text}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(request, timeout=10)
    except Exception as exc:  # noqa: BLE001
        print(f"Alert webhook failed: {exc}")


@asset_check(
    asset=get_asset_key_for_model([trading_dbt_assets], "fct_trade_executions"),
    description=(
        "Pass rate of the latest batch across the Silver split-gate quarantine and the Gold "
        f"fact table. Fails below {PASS_RATE_THRESHOLD_PCT}% and alerts on-call."
    ),
)
def check_trade_quarantine_threshold(context: AssetCheckExecutionContext) -> AssetCheckResult:
    conn = get_snowflake_connection()
    try:
        with conn.cursor() as cur:
            # The latest batch is everything stg_trade_events_cdc consumed in its last run,
            # identified by _stream_consumed_at (one value per dbt run). Counting by
            # calendar day instead would mix every batch since midnight.
            cur.execute("""
                WITH latest_batch AS (
                    SELECT MAX(_stream_consumed_at) AS consumed_at
                    FROM SILVER.STAGING.STG_TRADE_EVENTS_CDC
                ),
                batch_ids AS (
                    SELECT DISTINCT c.trade_id
                    FROM SILVER.STAGING.STG_TRADE_EVENTS_CDC c
                    JOIN latest_batch b ON c._stream_consumed_at = b.consumed_at
                ),
                batch_stats AS (
                    SELECT
                        (SELECT COUNT(*) FROM GOLD.CORE.FCT_TRADE_EXECUTIONS
                         WHERE trade_id IN (SELECT trade_id FROM batch_ids)) AS valid_count,
                        (SELECT COUNT(*) FROM SILVER.INTERMEDIATE.QUARANTINE_TRADE_EVENTS
                         WHERE trade_id IN (SELECT trade_id FROM batch_ids)) AS quarantine_count
                )
                SELECT
                    valid_count,
                    quarantine_count,
                    ROUND(DIV0(valid_count, valid_count + quarantine_count) * 100, 4) AS pass_rate_pct
                FROM batch_stats
            """)
            valid_cnt, quarantine_cnt, pass_rate = cur.fetchone()
    finally:
        conn.close()

    total = int(valid_cnt) + int(quarantine_cnt)
    if total == 0:
        return AssetCheckResult(
            passed=True,
            metadata={"note": "Latest batch has no rows to score."},
        )

    passed = float(pass_rate) >= PASS_RATE_THRESHOLD_PCT
    if not passed:
        send_alert(
            f":rotating_light: Trade quarantine check failed: {float(pass_rate):.2f}% of the latest "
            f"batch passed ({int(quarantine_cnt)} of {total} rows quarantined). "
            f"Threshold is {PASS_RATE_THRESHOLD_PCT}%. Triage SILVER.INTERMEDIATE.QUARANTINE_TRADE_EVENTS."
        )
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "valid_trades_promoted": int(valid_cnt),
            "quarantined_trades_isolated": int(quarantine_cnt),
            "batch_pass_score_pct": float(pass_rate),
            "sla_threshold_pct": PASS_RATE_THRESHOLD_PCT,
            "alert_sent": (not passed) and bool(ALERT_WEBHOOK_URL),
        },
    )


defs = Definitions(
    assets=[trading_dbt_assets],
    asset_checks=[check_trade_quarantine_threshold],
    jobs=[trading_pipeline_job],
    sensors=[bronze_iceberg_stream_sensor],
    resources={
        "dbt": DbtCliResource(
            project_dir=os.fspath(dbt_project_dir),
            profiles_dir=os.fspath(dbt_profiles_dir),
        ),
    },
)