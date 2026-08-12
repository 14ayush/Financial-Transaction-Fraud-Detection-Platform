from datetime import datetime, timedelta
import random
import subprocess

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ---------------------------------------------------------------------------
# Project paths
#
#   Financial Transaction Fraud Detection Platform/
#   ├── dbt/
#   ├── generator/
#   │   └── generate_transactions.py
#   ├── data/                              <- CSVs land here (no date= subfolder)
#   └── orchestration/
#       └── airflow_home/
#           └── dags/
#               └── fraud_pipeline_dag.py   <- this file
# ---------------------------------------------------------------------------

PROJECT_ROOT = "/mnt/e/Financial Transaction Fraud Detection Platform"
GENERATOR_SCRIPT = f"{PROJECT_ROOT}/DataGeneration/Transactiongeneration.py"
CSV_OUTPUT_DIR = f"{PROJECT_ROOT}/raw_data"

# the Python interpreter INSIDE airflow_venv — generate_transactions.py has
# no external dependencies (pure stdlib), so any Python works, but pointing
# at the venv's own interpreter keeps things consistent
AIRFLOW_VENV_PYTHON = f"{PROJECT_ROOT}/orchestration/airflow_venv/bin/python3"

default_args = {
    "owner": "fraud_pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# Task 1: Generate today's synthetic transactions
# ---------------------------------------------------------------------------
def _generate_transactions(**context):
    """
    Calls generate_transactions.py as a subprocess, since it's a standalone
    script living in a different directory (generator/), not something we
    import as a module. Daily volume is randomized between 5,000 and 50,000
    to simulate realistic day-to-day fluctuation.
    """
    run_date = context["ds"]  # Airflow's logical date, format YYYY-MM-DD
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
# Task 2: Load today's CSV into the Neon landing table
# ---------------------------------------------------------------------------
def _load_to_neon(**context):
    """
    Streams today's CSV into raw_landing.transactions via Postgres COPY.
    Column order below must match the CSV's FIELDNAMES order exactly,
    since COPY matches columns by position, not by name.
    """
    run_date = context["ds"]
    csv_path = f"{CSV_OUTPUT_DIR}/transactions_{run_date}.csv"

    hook = PostgresHook(postgres_conn_id="neon_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    with open(csv_path, "r") as f:
        next(f)  # skip header row
        cur.copy_expert(
            """
            COPY raw_landing.transactions (
                transaction_id, user_id, merchant_id, device_id,
                transaction_amount, transaction_date, payment_method,
                device_type, ip_address, location_lat, location_lon,
                ip_country, billing_country, is_fraud
            ) FROM STDIN WITH CSV
            """,
            f,
        )
    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {csv_path} into raw_landing.transactions")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="fraud_pipeline_daily",
    default_args=default_args,
    description="Daily fraud detection pipeline: generate -> load -> transform -> score",
    schedule="0 6 * * *",          # every day at 06:00
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["fraud-detection"],
) as dag:

    generate_transactions = PythonOperator(
        task_id="generate_transactions",
        python_callable=_generate_transactions,
    )

    load_to_neon = PythonOperator(
        task_id="load_to_neon",
        python_callable=_load_to_neon,
    )

    # more tasks (jdbc_pull_to_databricks, dbt_snapshot, dbt_run, dbt_test)
    # get appended below as we build them out

    generate_transactions >> load_to_neon