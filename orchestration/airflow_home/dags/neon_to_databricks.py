import time
from datetime import datetime, timedelta

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException

# ---------------------------------------------------------------------------
# Requires an Airflow connection named "databricks_default":
#   Conn type : Databricks (or Generic/HTTP is fine too, we only use host+password)
#   Host      : https://<your-workspace>.cloud.databricks.com
#   Password  : a Databricks personal access token (PAT) with permission
#               to run this pipeline
#
# Requires an Airflow Variable named "dlt_pipeline_id" set to the DLT
# pipeline's ID (found in the DLT pipeline's settings page in Databricks UI).
# Using a Variable rather than hardcoding here means you can point at a
# different pipeline (e.g. a dev one) without touching code.
# ---------------------------------------------------------------------------

from airflow.models import Variable

DLT_PIPELINE_ID = Variable.get("dlt_pipeline_id")

# How long to wait for the DLT update to finish before giving up.
# DLT updates on a fresh/append-only Delta table are usually fast, but leave
# headroom — this is a full daily batch, not a tiny incremental trickle.
POLL_INTERVAL_SECONDS = 30
MAX_WAIT_SECONDS = 60 * 30  # 30 minutes

# Terminal states per the Databricks Pipelines API. Anything not in
# TERMINAL_STATES means "still running, keep polling."
SUCCESS_STATES = {"COMPLETED"}
FAILURE_STATES = {"FAILED", "CANCELED"}
TERMINAL_STATES = SUCCESS_STATES | FAILURE_STATES

default_args = {
    "owner": "fraud_pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


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

    # 1. Start a pipeline update (incremental, not full_refresh — we want it
    #    to pick up only what's new since last run, matching the "no data
    #    loss, no reload" philosophy of the rest of this pipeline).
    start_resp = requests.post(
        f"{host}/api/2.0/pipelines/{DLT_PIPELINE_ID}/updates",
        headers=headers,
        json={"full_refresh": False},
        timeout=30,
    )
    start_resp.raise_for_status()
    update_id = start_resp.json()["update_id"]
    print(f"Triggered DLT pipeline {DLT_PIPELINE_ID}, update_id={update_id}")

    # 2. Poll the specific update until it reaches a terminal state.
    #    Polling the update (not just "latest pipeline state") avoids a race
    #    where a stale previous update's state gets read as this run's result.
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


with DAG(
    dag_id="neon_to_databricks_daily",
    default_args=default_args,
    description="Trigger the DLT pipeline that pulls Neon data into Databricks via UC connection, then hand off to dbt",
    schedule=None,   # triggered by fraud_pipeline_daily, not run on its own cron
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["fraud-detection", "databricks", "dlt"],
) as dag:

    trigger_dlt = PythonOperator(
        task_id="trigger_dlt_pipeline",
        python_callable=_trigger_and_wait_dlt,
    )

    # Hands off to the dbt medallion DAG only once Databricks genuinely has
    # the new data — this is what closes the race-condition gap flagged
    # earlier. dbt_medallion_daily should have schedule=None too, so it can
    # ONLY be started this way (see note in that DAG file).
    trigger_dbt_medallion = TriggerDagRunOperator(
        task_id="trigger_dbt_medallion",
        trigger_dag_id="dbt_medallion_daily",
        wait_for_completion=True,
        poke_interval=30,
    )

    trigger_dlt >> trigger_dbt_medallion