import glob
import os
import random
import subprocess
import time
from datetime import datetime, timedelta

import psycopg2
import requests

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Project paths
#
#   Financial Transaction Fraud Detection Platform/
#   ├── FraudDetect/                        <- dbt project (dbt_project.yml lives here)
#   ├── DataGeneration/
#   │   ├── Transactiongeneration.py
#   │   └── raw_data/                       <- CSVs land here
#   └── orchestration/
#       └── airflow_venv/                   <- Python + dbt-core, one shared venv
#           ├── bin/python3
#           └── bin/dbt
#
# This single DAG replaces the three separate DAGs (fraud_pipeline_daily,
# neon_to_databricks_daily, dbt_medallion_daily) with one linear chain, so
# there's no cross-DAG triggering to keep track of — task order IS execution
# order:
#
#   generate_transactions -> load_to_neon -> trigger_dlt_pipeline
#     -> dbt_deps -> dbt_run_bronze -> dbt_test_bronze
#     -> dbt_run_silver -> dbt_test_silver -> dbt_snapshot_silver
#     -> dbt_run_gold -> dbt_test_all
#
# Each task only starts once the one before it succeeds, so a failure
# anywhere (e.g. a dbt test) stops everything downstream from running on
# bad or stale data.
# ---------------------------------------------------------------------------

PROJECT_ROOT = "/mnt/e/Financial Transaction Fraud Detection Platform"

# --- generation + load ---
GENERATOR_SCRIPT = f"{PROJECT_ROOT}/DataGeneration/Transactiongeneration.py"
CSV_OUTPUT_DIR = f"{PROJECT_ROOT}/DataGeneration/raw_data"
AIRFLOW_VENV_PYTHON = f"{PROJECT_ROOT}/orchestration/airflow_venv/bin/python3"

# --- dbt ---
DBT_PROJECT_DIR = f"{PROJECT_ROOT}/FraudDetect"
DBT_BIN = f"{PROJECT_ROOT}/orchestration/airflow_venv/bin/dbt"
DBT_PROFILES_DIR = f"{PROJECT_ROOT}/FraudDetect"
DBT_COMMON_FLAGS = f'--project-dir "{DBT_PROJECT_DIR}" --profiles-dir "{DBT_PROFILES_DIR}"'

# --- Databricks DLT ---
# Requires an Airflow connection named "databricks_default" (Host = workspace
# URL, Password = a Databricks PAT) and an Airflow Variable "dlt_pipeline_id"
# set to the DLT pipeline's ID.
DLT_PIPELINE_ID = Variable.get("dlt_pipeline_id")
POLL_INTERVAL_SECONDS = 30
MAX_WAIT_SECONDS = 60 * 30  # 30 minutes
SUCCESS_STATES = {"COMPLETED"}
FAILURE_STATES = {"FAILED", "CANCELED"}

default_args = {
    "owner": "fraud_pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# Task 1: Generate today's synthetic transactions
# ---------------------------------------------------------------------------
def _generate_transactions(**context):
    run_date = context["ds"]
    num_transactions = random.randint(5000, 50000)

    result = subprocess.run(
        [
            AIRFLOW_VENV_PYTHON, GENERATOR_SCRIPT,
            "--date", run_date,
            "--num-transactions", str(num_transactions),
            "--output-dir", CSV_OUTPUT_DIR,
        ],
        capture_output=True,
        text=True,
    )

    print(f"Generating {num_transactions} transactions for {run_date}")
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"generate_transactions.py failed for {run_date}")

    context["ti"].xcom_push(key="num_transactions", value=num_transactions)


# ---------------------------------------------------------------------------
# Task 2: Load any pending CSVs into the Neon landing table
# ---------------------------------------------------------------------------
def _load_to_neon(**context):
    conn_info = BaseHook.get_connection("neon_postgres")
    conn = psycopg2.connect(
        host=conn_info.host, port=conn_info.port or 5432,
        dbname=conn_info.schema, user=conn_info.login,
        password=conn_info.password, sslmode="require",
    )
    cur = conn.cursor()

    all_csvs = sorted(glob.glob(os.path.join(CSV_OUTPUT_DIR, "*.csv")))

    cur.execute("SELECT file_path FROM pipeline_load_log")
    already_loaded = {row[0] for row in cur.fetchall()}

    pending = [f for f in all_csvs if os.path.abspath(f) not in already_loaded]
    if not pending:
        print("Nothing pending — all files already loaded.")
        cur.close()
        conn.close()
        return

    for csv_path in pending:
        abs_path = os.path.abspath(csv_path)
        with open(csv_path, "r") as f:
            next(f)  # skip header
            row_count = sum(1 for _ in f)
            f.seek(0)
            next(f)
            try:
                cur.copy_expert(
                    """COPY raw_transactions_backup (
                        "TransactionID","UserID","MerchantID","DeviceID","TransactionAmount",
                        "TransactionDate","PaymentMethod","DeviceType","IP_Address",
                        "LocationLat","LocationLon","IP_Country","BillingCountry","IsFraud"
                    ) FROM STDIN WITH CSV""",
                    f,
                )
                cur.execute(
                    "INSERT INTO pipeline_load_log (file_path, rows_loaded) VALUES (%s, %s)",
                    (abs_path, row_count),
                )
                conn.commit()  # commit per file: a failure on file N doesn't undo 1..N-1
                print(f"Loaded {csv_path} ({row_count} rows)")
            except Exception as e:
                conn.rollback()
                print(f"Failed on {csv_path}: {e}")
                raise

    cur.close()
    conn.close()


# ---------------------------------------------------------------------------
# Task 3: Trigger the Databricks DLT pipeline (Neon -> Databricks) and wait
# ---------------------------------------------------------------------------
def _databricks_headers():
    conn = BaseHook.get_connection("databricks_default")
    token = conn.password
    if not token:
        raise AirflowException(
            "databricks_default connection has no token set in the Password field."
        )
    return {"Authorization": f"Bearer {token}"}, conn.host.rstrip("/")


def _trigger_and_wait_dlt(**context):
    headers, host = _databricks_headers()

    start_resp = requests.post(
        f"{host}/api/2.0/pipelines/{DLT_PIPELINE_ID}/updates",
        headers=headers,
        json={"full_refresh": False},
        timeout=30,
    )
    start_resp.raise_for_status()
    update_id = start_resp.json()["update_id"]
    print(f"Triggered DLT pipeline {DLT_PIPELINE_ID}, update_id={update_id}")

    elapsed = 0
    while elapsed < MAX_WAIT_SECONDS:
        status_resp = requests.get(
            f"{host}/api/2.0/pipelines/{DLT_PIPELINE_ID}/updates/{update_id}",
            headers=headers,
            timeout=30,
        )
        status_resp.raise_for_status()
        state = status_resp.json()["update"]["state"]
        print(f"DLT update {update_id} state: {state} (elapsed {elapsed}s)")

        if state in SUCCESS_STATES:
            print("DLT pipeline update completed successfully.")
            return
        if state in FAILURE_STATES:
            raise AirflowException(
                f"DLT pipeline update {update_id} ended in state {state}. "
                f"Check the pipeline's event log in the Databricks UI for details."
            )

        time.sleep(POLL_INTERVAL_SECONDS)
        elapsed += POLL_INTERVAL_SECONDS

    raise AirflowException(
        f"DLT pipeline update {update_id} did not reach a terminal state within "
        f"{MAX_WAIT_SECONDS}s. It may still be running in Databricks — check "
        f"there before re-running this task, to avoid overlapping updates."
    )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="fraud_pipeline_full_daily",
    default_args=default_args,
    description=(
        "Full daily fraud detection pipeline in one chain: generate -> load "
        "to Neon -> sync Neon to Databricks via DLT -> dbt bronze -> silver "
        "-> snapshot -> gold -> test all"
    ),
    schedule="0 6 * * *",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["fraud-detection", "dbt", "databricks"],
) as dag:

    generate_transactions = PythonOperator(
        task_id="generate_transactions",
        python_callable=_generate_transactions,
    )

    load_to_neon = PythonOperator(
        task_id="load_to_neon",
        python_callable=_load_to_neon,
    )

    trigger_dlt_pipeline = PythonOperator(
        task_id="trigger_dlt_pipeline",
        python_callable=_trigger_and_wait_dlt,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f'"{DBT_BIN}" deps {DBT_COMMON_FLAGS}',
    )

    dbt_run_bronze = BashOperator(
        task_id="dbt_run_bronze",
        bash_command=f'"{DBT_BIN}" run --select bronze {DBT_COMMON_FLAGS}',
    )

    dbt_test_bronze = BashOperator(
        task_id="dbt_test_bronze",
        bash_command=f'"{DBT_BIN}" test --select bronze {DBT_COMMON_FLAGS}',
    )

    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=f'"{DBT_BIN}" run --select silver {DBT_COMMON_FLAGS}',
    )

    dbt_test_silver = BashOperator(
        task_id="dbt_test_silver",
        bash_command=f'"{DBT_BIN}" test --select silver {DBT_COMMON_FLAGS}',
    )

    dbt_snapshot_silver = BashOperator(
        task_id="dbt_snapshot_silver",
        bash_command=f'"{DBT_BIN}" snapshot {DBT_COMMON_FLAGS}',
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=f'"{DBT_BIN}" run --select gold {DBT_COMMON_FLAGS}',
    )

    dbt_test_all = BashOperator(
        task_id="dbt_test_all",
        bash_command=f'"{DBT_BIN}" test {DBT_COMMON_FLAGS}',
    )

    (
        generate_transactions
        >> load_to_neon
        >> trigger_dlt_pipeline
        >> dbt_deps
        >> dbt_run_bronze
        >> dbt_test_bronze
        >> dbt_run_silver
        >> dbt_test_silver
        >> dbt_snapshot_silver
        >> dbt_run_gold
        >> dbt_test_all
    )