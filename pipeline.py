from pathlib import Path
from dagster import Definitions, AssetExecutionContext,DailyPartitionsDefinition, build_schedule_from_partitioned_job, RetryPolicy, asset, define_asset_job
from dagster_dbt import DbtCliResource, dbt_assets
from sqlalchemy import create_engine
from sqlalchemy.schema import CreateSchema
from datetime import datetime

import os
import kagglehub as kh
import pandas as pd
import json

POSTGRES_CONN = "postgresql://postgres:postgres@alt_payments_db:5432/analytics"

dbt_project_dir = Path(__file__).joinpath("..", "dbt_analytics").resolve()

dbt = DbtCliResource(
    project_dir=os.fspath(dbt_project_dir),
    profiles_dir=os.fspath(dbt_project_dir)
)


# If DAGSTER_DBT_PARSE_PROJECT_ON_LOAD is set, a manifest will be created at runtime.
if os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD"):
    dbt.cli(["deps"]).wait()
    dbt_parse_invocation = (
    dbt.cli(
        ["--quiet", "parse"],
        target_path=Path("target")
    ).wait()
    )
    dbt_manifest_path = dbt_parse_invocation.target_path.joinpath("manifest.json")
else:
    dbt_manifest_path = dbt_project_dir.joinpath("target", "manifest.json")

#asset #1
@asset(
    retry_policy=RetryPolicy(max_retries=2, delay=60),
    compute_kind="python"
)
def ingest_data(context: AssetExecutionContext) -> None:

    dataset_path = kh.dataset_download("anurag629/chinook-csv-dataset")
    context.log.info(f"Dataset downloaded to: {dataset_path}")

    engine = create_engine(POSTGRES_CONN)
    with engine.connect() as conn:
        conn.execute(CreateSchema("raw", if_not_exists=True))
        context.log.info("Schema 'raw' ensured.")
        conn.commit()

    for filename in os.listdir(dataset_path):
        if filename.endswith(".csv"):
            table_name = os.path.splitext(filename)[0].lower()
            file_path = os.path.join(dataset_path, filename)

            context.log.info(f"Processing file {filename} -> raw.{table_name}")
            
            try:
                df = pd.read_csv(file_path)
                df["IngestionTimestamp"] = pd.Timestamp.now()
                df["DataSource"] = 'kaggle'
                df.to_sql(
                    name=table_name,
                    schema="raw",
                    con=engine,
                    if_exists="append",
                    index=False
                )
                context.log.info(f"Loaded {len(df)} rows into raw.{table_name}")
            except Exception as e:
                context.log.error(f"Failed to load {filename}: {e}")

daily_partitions = DailyPartitionsDefinition(start_date="2025-07-06")

#asset #2
@dbt_assets(
    retry_policy=RetryPolicy(max_retries=2, delay=60), 
    manifest=dbt_manifest_path,
    partitions_def=daily_partitions,
)
def dbt_analytics_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    start, end = context.partition_time_window

    dbt_vars = {"start_date": start.isoformat(), "end_date": end.isoformat()}
    # Optional: check if full-refresh is requested
    full_refresh_flag = os.getenv("DBT_FULL_REFRESH", "false").lower() == "true"

    dbt_cmd = ["build", "--vars", json.dumps(dbt_vars)]
    if full_refresh_flag:
        dbt_cmd.append("--full-refresh")

    yield from dbt.cli( 
        dbt_cmd,
        context=context
    ).stream()

#job
daily_job = define_asset_job(
        name="daily_pipeline_job",
        selection=[ingest_data, dbt_analytics_assets],
        partitions_def=daily_partitions
)

#schedule
daily_schedule = build_schedule_from_partitioned_job(
    job=daily_job,
    hour_of_day=2,
    minute_of_hour=0
)

#definition
defs = Definitions(
    assets=[ingest_data, dbt_analytics_assets],
    schedules=[daily_schedule],
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
)


