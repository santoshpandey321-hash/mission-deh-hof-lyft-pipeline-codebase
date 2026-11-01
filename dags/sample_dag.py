from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Default args mirror the Step Function retry policy:
#   - 2 retries with a 30-second delay between attempts (BackoffRate applied
#     by Airflow's exponential retry_delay growth via retry_exponential_backoff)
# ---------------------------------------------------------------------------
default_args = {
    "owner": "deh-hof",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="mission_deh_hof_lyft_pipeline",
    description=(
        "DEH HOF Lyft Pipeline — runs NYCTLC ingestion, raw-to-curated, "
        "and curated-to-aggregated Glue jobs in sequence. "
        "Mirrors the pipeline-state-machine Step Function."
    ),
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # Triggered externally (Lambda on S3 file arrival)
    catchup=False,
    tags=["deh-hof", "lyft", "nyctlc", "glue", "pipeline"],
) as dag:

    # Step 1 — Ingestion
    # Glue Python Shell job: downloads NYCTLC HVFHV data to the raw layer.
    ingestion = GlueJobOperator(
        task_id="ingestion",
        job_name="mission-deh-hof-nyctlc-ingestion",
        aws_conn_id="aws_default",
        wait_for_completion=True,
    )

    # Step 2 — Raw → Curated
    # Glue Spark job: transforms raw HVFHV data into the curated layer.
    raw_to_curated = GlueJobOperator(
        task_id="raw_to_curated",
        job_name="mission-deh-hof-raw-to-curated-etl",
        aws_conn_id="aws_default",
        wait_for_completion=True,
    )

    # Step 3 — Curated → Aggregated
    # Glue Spark job: builds aggregate tables from the curated layer.
    curated_to_aggregated = GlueJobOperator(
        task_id="curated_to_aggregated",
        job_name="mission-deh-hof-curated-to-aggregated-etl",
        aws_conn_id="aws_default",
        wait_for_completion=True,
    )

    # Sequential pipeline — mirrors Ingestion → RawToCurated → CuratedToAggregated
    ingestion >> raw_to_curated >> curated_to_aggregated
