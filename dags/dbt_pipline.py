from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pendulum
import os
import requests

local_tz = pendulum.timezone("Asia/Bangkok")
pipeline_env = os.getenv("PIPELINE_ENV", "dev").strip().lower()
default_dbt_target = "prd" if pipeline_env == "prd" else "dev"

load_command = "python /opt/airflow/scripts/synthetic_pipeline/load_to_bronze.py"
dbt_target = os.getenv("DBT_TARGET", default_dbt_target).strip() or default_dbt_target

if pipeline_env == "prd":
    load_command = "python /opt/airflow/scripts/synthetic_pipeline/load_to_bronze_bigquery.py"

default_args = {
    "owner": "datateam",
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}

def notify_success(context):
    dag_run = context.get("dag_run")
    form_ids = []
    if dag_run and dag_run.conf:
        form_ids = dag_run.conf.get("form_ids", []) or []

    # ถ้าไม่มี form_ids (เช่น schedule ปกติ) ก็ไม่ทำอะไร
    if not form_ids:
        return

    callback_url = os.environ.get(
        "SYNC_CALLBACK_URL",
        "http://host.docker.internal:5001/api/admin/sync-validator/trigger-price/complete"
    )
    requests.post(
        callback_url,
        json={"status": "success", "form_ids": form_ids},
        timeout=10
    )

with DAG(
    dag_id="synthetic_pricing_pipeline",
    schedule_interval="0 7 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    tags=["model-pipeline", "elt", "dbt"],
    max_active_runs=1,
    catchup=False,
    default_args=default_args,
    on_success_callback=notify_success,   # << เพิ่มบรรทัดนี้
) as dag:

    start = EmptyOperator(task_id="start")

    load_bronze = BashOperator(
        task_id="load_to_bronze",
        bash_command=load_command,
    )

    run_dbt = BashOperator(
        task_id="dbt_build",
        bash_command=f"""
        docker exec \
        -w /usr/app/synthetic_pricing_pipeline \
        dbt \
        dbt build --target {dbt_target} --select +mart_contract_pricing
        """,
    )

    end = EmptyOperator(task_id="end")

    start >> load_bronze >> run_dbt >> end
