from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# ---------------------------------------------------------------------------
# Project paths
#
#   Financial Transaction Fraud Detection Platform/
#   ├── dbt/                                <- dbt project (dbt_project.yml lives here)
#   ├── orchestration/
#   │   └── airflow_venv/                   <- dbt-core installed here, same venv
#   │       └── bin/dbt
#   └── ...
#
# NOTE: this DAG currently has NO upstream dependency on the Neon -> Databricks
# sync. That sync is being built separately. Once it exists, the first task
# below (dbt_run_bronze) should either:
#   (a) start with an ExternalTaskSensor watching the sync DAG's completion, or
#   (b) be triggered directly via TriggerDagRunOperator at the end of the sync DAG
# Until then, this DAG assumes Databricks already has fresh-enough data whenever
# it runs — that assumption is on you to satisfy manually for now.
# ---------------------------------------------------------------------------

PROJECT_ROOT = "/mnt/e/Financial Transaction Fraud Detection Platform"
DBT_PROJECT_DIR = f"{PROJECT_ROOT}/FraudDetect"

# Same venv as the rest of the orchestration — assumes dbt-core (dbt-databricks
# adapter) is installed into it, e.g.:
#   airflow_venv/bin/pip install dbt-core dbt-databricks
DBT_BIN = f"{PROJECT_ROOT}/orchestration/airflow_venv/bin/dbt"

# If your profiles.yml lives outside the default ~/.dbt/, point at it explicitly.
# Safer to be explicit in an orchestrated context than rely on a user's home dir.
DBT_PROFILES_DIR = f"{PROJECT_ROOT}/FraudDetect"

default_args = {
    "owner": "fraud_pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Shared flags for every dbt invocation below
DBT_COMMON_FLAGS = f'--project-dir "{DBT_PROJECT_DIR}" --profiles-dir "{DBT_PROFILES_DIR}"'

with DAG(
    dag_id="dbt_medallion_daily",
    default_args=default_args,
    description="Build and test the full dbt medallion pipeline: bronze -> silver -> snapshot -> gold",
    schedule="0 8 * * *",          # after the Neon load DAG's 06:00 run, with a buffer
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["fraud-detection", "dbt", "bronze", "silver", "gold"],
) as dag:

    # dbt deps: installs any packages declared in packages.yml (dbt_utils, etc.)
    # Safe to run every time — no-op if there's nothing to install/update.
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f'"{DBT_BIN}" deps {DBT_COMMON_FLAGS}',
    )

    # dbt run, scoped to the bronze layer only. Assumes your bronze models live
    # under a models/bronze/ folder or are tagged `bronze` in dbt_project.yml —
    # adjust the --select value to match whichever convention your project uses.
    dbt_run_bronze = BashOperator(
        task_id="dbt_run_bronze",
        bash_command=f'"{DBT_BIN}" run --select bronze {DBT_COMMON_FLAGS}',
    )

    # dbt test, same scope. Runs schema tests (not_null, unique, relationships,
    # etc.) defined in your bronze models' .yml files. Fails the task (and DAG)
    # if any test fails — this is your main data-quality gate at this layer.
    dbt_test_bronze = BashOperator(
        task_id="dbt_test_bronze",
        bash_command=f'"{DBT_BIN}" test --select bronze {DBT_COMMON_FLAGS}',
    )

    # dbt run, scoped to the silver layer. Runs after bronze has been built
    # AND validated — if a bronze test fails, this task never fires, so silver
    # never builds on top of data you already know is bad.
    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=f'"{DBT_BIN}" run --select silver {DBT_COMMON_FLAGS}',
    )

    dbt_test_silver = BashOperator(
        task_id="dbt_test_silver",
        bash_command=f'"{DBT_BIN}" test --select silver {DBT_COMMON_FLAGS}',
    )

    # dbt snapshot, run against the silver layer per your ordering. This
    # captures SCD Type 2 history of silver-layer tables as of this run.
    # NOTE: snapshot definitions (.sql files under snapshots/) select their
    # own source tables independently of --select, so this only works
    # correctly if your snapshot .sql files point at silver models/tables.
    # If any snapshot targets bronze instead, split this into two tasks.
    dbt_snapshot_silver = BashOperator(
        task_id="dbt_snapshot_silver",
        bash_command=f'"{DBT_BIN}" snapshot {DBT_COMMON_FLAGS}',
    )

    # dbt run, scoped to gold. Builds on top of the just-snapshotted silver
    # layer.
    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=f'"{DBT_BIN}" run --select gold {DBT_COMMON_FLAGS}',
    )

    # Final gate: test everything (bronze + silver + gold + snapshots) in one
    # pass, catching any cross-layer relationship/referential tests that
    # --select bronze / --select silver alone wouldn't have caught, since
    # those tests may reference models across layers.
    dbt_test_all = BashOperator(
        task_id="dbt_test_all",
        bash_command=f'"{DBT_BIN}" test {DBT_COMMON_FLAGS}',
    )

    (
        dbt_deps
        >> dbt_run_bronze
        >> dbt_test_bronze
        >> dbt_run_silver
        >> dbt_test_silver
        >> dbt_snapshot_silver
        >> dbt_run_gold
        >> dbt_test_all
    )