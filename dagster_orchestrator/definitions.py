import os
from pathlib import Path
import snowflake.connector
from cryptography.hazmat.primitives import serialization
import time
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
    SkipReason,
    asset_check,
    define_asset_job,
    sensor,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets, get_asset_key_for_model

dbt_project_dir = Path(
    os.getenv(
        "DBT_PROJECT_DIR",
        r"C:\Users\sande\Documents\Trading Lakehouse dbt\trading_lakehouse_dbt",
    )
).resolve()
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
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
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

    in_progress_runs = [
        run for run in context.instance.get_runs(limit=5)
        if run.job_name == "trading_pipeline_job"
        and run.status in (DagsterRunStatus.QUEUED, DagsterRunStatus.STARTED, DagsterRunStatus.STARTING)
    ]
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
        return SkipReason("No new Iceberg manifest commits detected in BRONZE_TRADE_EVENTS_STREAM.")
    finally:
        conn.close()

@asset_check(
    asset=get_asset_key_for_model([trading_dbt_assets], "fct_trade_executions"),
    description="Computes batch Pass/Fail score across Silver Split-Gate Quarantine and Gold Fact table.",
)
def check_trade_quarantine_threshold(context: AssetCheckExecutionContext) -> AssetCheckResult:
    conn = get_snowflake_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                WITH batch_stats AS (
                    SELECT
                        (SELECT COUNT(*) FROM GOLD.CORE.FCT_TRADE_EXECUTIONS
                         WHERE ingested_date = CURRENT_DATE()) AS valid_count,
                        (SELECT COUNT(*) FROM SILVER.INTERMEDIATE.QUARANTINE_TRADE_EVENTS
                         WHERE quarantined_date = CURRENT_DATE()) AS quarantine_count
                )
                SELECT
                    valid_count,
                    quarantine_count,
                    ROUND(
                        DIV0(valid_count, (valid_count + quarantine_count)) * 100, 4
                    ) AS pass_rate_pct
                FROM batch_stats
            """)
            valid_cnt, quarantine_cnt, pass_rate = cur.fetchone()

        passed = float(pass_rate) >= 99.80
        return AssetCheckResult(
            passed=passed,
            severity=AssetCheckSeverity.ERROR if not passed else AssetCheckSeverity.WARN,
            metadata={
                "valid_trades_promoted": int(valid_cnt),
                "quarantined_trades_isolated": int(quarantine_cnt),
                "batch_pass_score_pct": float(pass_rate),
                "sla_threshold_pct": 98.00,
            },
        )
    finally:
        conn.close()

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