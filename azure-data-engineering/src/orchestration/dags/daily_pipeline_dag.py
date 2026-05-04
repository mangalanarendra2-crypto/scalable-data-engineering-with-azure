"""
src/orchestration/dags/daily_pipeline_dag.py
---------------------------------------------
Apache Airflow DAG: Daily Data Engineering Pipeline

Pipeline stages:
  1. Health checks (ADLS connectivity, API availability)
  2. Batch ingestion (CRM, ERP API sources)
  3. Bronze → Silver transformation (Databricks)
  4. Silver → Gold aggregation (Databricks)
  5. Synapse sync (external table refresh)
  6. Data quality report
  7. Slack / email alerts on failure

Schedule: Daily at 02:00 UTC
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.microsoft.azure.operators.adls import ADLSDeleteOperator
from airflow.providers.microsoft.azure.operators.data_factory import (
    AzureDataFactoryRunPipelineOperator,
)
from airflow.providers.microsoft.azure.sensors.wasb import WasbPrefixSensor
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule


# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------

DEFAULT_ARGS = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": ["data-alerts@yourorg.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=4),
}


# ---------------------------------------------------------------------------
# Helper functions (PythonOperator callables)
# ---------------------------------------------------------------------------

def check_adls_connectivity(**context: Any) -> bool:
    """Verify ADLS Gen2 is reachable and bronze container exists."""
    import logging
    from azure.identity import DefaultAzureCredential
    from azure.storage.filedatalake import DataLakeServiceClient

    account = Variable.get("ADLS_ACCOUNT_NAME")
    try:
        credential = DefaultAzureCredential()
        client = DataLakeServiceClient(
            f"https://{account}.dfs.core.windows.net", credential=credential
        )
        client.get_file_system_client("bronze").get_file_system_properties()
        logging.info("ADLS connectivity check passed.")
        return True
    except Exception as e:
        raise RuntimeError(f"ADLS connectivity check failed: {e}") from e


def run_batch_ingestion(source: str, entity: str, **context: Any) -> dict:
    """Trigger batch ingestion for a source/entity pair."""
    import subprocess, json, sys

    process_date = context["ds"]  # YYYY-MM-DD from Airflow execution date
    result = subprocess.run(
        [
            sys.executable, "-m", "src.ingestion.batch_ingestor",
            "--source", source,
            "--entity", entity,
            "--since", f"{process_date}T00:00:00",
        ],
        capture_output=True, text=True, check=True
    )
    return json.loads(result.stdout)


def trigger_databricks_job(job_id: int, parameters: dict, **context: Any) -> str:
    """Trigger a Databricks job and wait for completion."""
    import time
    import requests

    host = Variable.get("DATABRICKS_HOST")
    token = Variable.get("DATABRICKS_TOKEN")
    headers = {"Authorization": f"Bearer {token}"}
    base_url = f"https://{host}.azuredatabricks.net/api/2.1/jobs"

    # Submit run
    resp = requests.post(
        f"{base_url}/run-now",
        headers=headers,
        json={"job_id": job_id, "notebook_params": parameters},
        timeout=30,
    )
    resp.raise_for_status()
    run_id = resp.json()["run_id"]

    # Poll for completion
    max_wait = 7200  # 2 hours
    poll_interval = 30
    elapsed = 0
    while elapsed < max_wait:
        time.sleep(poll_interval)
        elapsed += poll_interval
        status_resp = requests.get(
            f"{base_url}/runs/get", headers=headers, params={"run_id": run_id}
        )
        state = status_resp.json()["state"]
        life_cycle = state.get("life_cycle_state")
        result_state = state.get("result_state", "")

        if life_cycle == "TERMINATED":
            if result_state == "SUCCESS":
                return f"Databricks run {run_id} succeeded."
            else:
                raise RuntimeError(f"Databricks run {run_id} failed: {state}")
        elif life_cycle in ("INTERNAL_ERROR", "SKIPPED"):
            raise RuntimeError(f"Databricks run {run_id} error: {state}")

    raise TimeoutError(f"Databricks run {run_id} timed out after {max_wait}s")


def sync_synapse(**context: Any) -> None:
    """Refresh Azure Synapse external tables to reflect Gold Delta changes."""
    import pyodbc

    server = Variable.get("SYNAPSE_SQL_ENDPOINT")
    database = Variable.get("SYNAPSE_DATABASE", default_var="DataWarehouse")
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Authentication=ActiveDirectoryMsi;"
    )

    tables = [
        "dw.fact_transactions",
        "dw.fact_sessions",
        "mart.daily_revenue",
        "mart.customer_lifetime",
    ]

    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        for table in tables:
            cursor.execute(f"ALTER EXTERNAL TABLE {table} REBUILD;")
            conn.commit()
            print(f"Rebuilt external table: {table}")


def generate_dq_report(**context: Any) -> None:
    """Generate and post a data quality report."""
    import json
    execution_date = context["ds"]
    # Placeholder: in production, fetch DQ metrics from Azure Monitor / Delta history
    report = {
        "date": execution_date,
        "entities": ["transactions", "clickstream"],
        "status": "PASS",
        "summary": "All DQ checks passed.",
    }
    print(json.dumps(report, indent=2))


def on_pipeline_failure(context: Any) -> None:
    """Callback on task failure - posts alert to Slack."""
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["ds"]
    message = (
        f":red_circle: *Pipeline Failed*\n"
        f"DAG: `{dag_id}`\n"
        f"Task: `{task_id}`\n"
        f"Date: `{execution_date}`"
    )
    # Post to Slack webhook (configure SLACK_WEBHOOK_URL variable in Airflow)
    try:
        import requests
        webhook = Variable.get("SLACK_WEBHOOK_URL", default_var="")
        if webhook:
            requests.post(webhook, json={"text": message}, timeout=10)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="daily_data_engineering_pipeline",
    description="End-to-end daily data engineering pipeline on Azure",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",   # 02:00 UTC daily
    catchup=False,
    max_active_runs=1,
    tags=["data-engineering", "azure", "production"],
    on_failure_callback=on_pipeline_failure,
) as dag:

    # ------------------------------------------------------------------
    # Stage 0: Health checks
    # ------------------------------------------------------------------
    start = EmptyOperator(task_id="start")

    health_check = PythonOperator(
        task_id="health_check_adls",
        python_callable=check_adls_connectivity,
    )

    # ------------------------------------------------------------------
    # Stage 1: Batch Ingestion
    # ------------------------------------------------------------------
    ingest_crm_orders = PythonOperator(
        task_id="ingest_crm_orders",
        python_callable=run_batch_ingestion,
        op_kwargs={"source": "crm", "entity": "orders"},
    )

    ingest_crm_customers = PythonOperator(
        task_id="ingest_crm_customers",
        python_callable=run_batch_ingestion,
        op_kwargs={"source": "crm", "entity": "customers"},
    )

    ingest_erp_products = PythonOperator(
        task_id="ingest_erp_products",
        python_callable=run_batch_ingestion,
        op_kwargs={"source": "erp", "entity": "products"},
    )

    ingestion_complete = EmptyOperator(task_id="ingestion_complete")

    # ------------------------------------------------------------------
    # Stage 2: Bronze → Silver (Databricks)
    # ------------------------------------------------------------------
    silver_transactions = PythonOperator(
        task_id="silver_transactions",
        python_callable=trigger_databricks_job,
        op_kwargs={
            "job_id": "{{ var.value.DATABRICKS_JOB_BRONZE_SILVER }}",
            "parameters": {"entity": "transactions", "date": "{{ ds }}"},
        },
    )

    silver_clickstream = PythonOperator(
        task_id="silver_clickstream",
        python_callable=trigger_databricks_job,
        op_kwargs={
            "job_id": "{{ var.value.DATABRICKS_JOB_BRONZE_SILVER }}",
            "parameters": {"entity": "clickstream", "date": "{{ ds }}"},
        },
    )

    silver_complete = EmptyOperator(task_id="silver_complete")

    # ------------------------------------------------------------------
    # Stage 3: Silver → Gold (Databricks)
    # ------------------------------------------------------------------
    gold_job = PythonOperator(
        task_id="silver_to_gold",
        python_callable=trigger_databricks_job,
        op_kwargs={
            "job_id": "{{ var.value.DATABRICKS_JOB_SILVER_GOLD }}",
            "parameters": {"date": "{{ ds }}"},
        },
    )

    # ------------------------------------------------------------------
    # Stage 4: Synapse sync
    # ------------------------------------------------------------------
    synapse_sync = PythonOperator(
        task_id="synapse_sync",
        python_callable=sync_synapse,
    )

    # ------------------------------------------------------------------
    # Stage 5: DQ Report & completion
    # ------------------------------------------------------------------
    dq_report = PythonOperator(
        task_id="dq_report",
        python_callable=generate_dq_report,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ------------------------------------------------------------------
    # Task dependencies
    # ------------------------------------------------------------------
    start >> health_check

    health_check >> [ingest_crm_orders, ingest_crm_customers, ingest_erp_products]
    [ingest_crm_orders, ingest_crm_customers, ingest_erp_products] >> ingestion_complete

    ingestion_complete >> [silver_transactions, silver_clickstream]
    [silver_transactions, silver_clickstream] >> silver_complete

    silver_complete >> gold_job >> synapse_sync >> dq_report >> end
