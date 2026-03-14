"""
pf_duckdb_daily - Build local DuckDB from JSON + SQL transformers.
"""
import json
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

LIVE_BANKS = [
    "santander", "bancoestado", "bice", "scotiabank", "bci",
    "bancochile", "cencosud", "falabella", "internacional", "ripley", "itau"
]

DEFAULT_JSON_DIR = "/opt/airflow/dags/json"
DEFAULT_SQL_DIR = "/opt/airflow/dags/sql"
DEFAULT_DUCKDB_PATH = "/opt/airflow/dags/datitos_nam.duckdb"

def _sync_raw_json(**context):
    from portfolio.libs.minio_client import get_raw_exact

    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    pf_dt = conf.get("pf_dt", context["ds"])
    pf_run_id = conf.get("pf_run_id", context["run_id"])

    json_dir = os.environ.get("PF_JSON_DIR", DEFAULT_JSON_DIR)
    os.makedirs(json_dir, exist_ok=True)

    synced = []
    skipped = []
    for bank in LIVE_BANKS:
        try:
            data, _ = get_raw_exact(bank=bank, dt=pf_dt, run_id=pf_run_id)
        except FileNotFoundError:
            data = []
            skipped.append(bank)
            print(f"⚠️ {bank}: raw.json not found in MinIO (scraper may have failed), writing empty JSON")
        path = os.path.join(json_dir, f"{bank}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Wrote {bank} JSON -> {path} ({len(data)} records)")
        if data:
            synced.append(bank)

    if skipped:
        print(f"⚠️ Banks skipped (no data): {skipped}")

    return {"json_dir": json_dir, "banks": synced, "skipped": skipped}


def _build_duckdb(**context):
    from portfolio.libs.duckdb_loader import build_duckdb

    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    pf_dt = conf.get("pf_dt", context["ds"])
    pf_run_id = conf.get("pf_run_id", context["run_id"])

    json_dir = os.environ.get("PF_JSON_DIR", DEFAULT_JSON_DIR)
    sql_dir = os.environ.get("PF_SQL_DIR", DEFAULT_SQL_DIR)
    db_path = os.environ.get("PF_DUCKDB_PATH", DEFAULT_DUCKDB_PATH)

    print(f"Building DuckDB at: {db_path}")
    print(f"SQL dir: {sql_dir}")
    print(f"JSON dir: {json_dir}")

    build_duckdb(
        db_path=db_path, sql_dir=sql_dir, json_base_path=json_dir,
        snapshot_date=pf_dt, run_id=pf_run_id,
    )
    return {"db_path": db_path}


default_args = {
    "owner": "portfolio",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="pf_duckdb_daily",
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule=None,
    catchup=False,
    tags=["portfolio", "pf"],
    doc_md="Build DuckDB using SQL transformers on JSON synced from MinIO.",
) as dag:

    start = EmptyOperator(task_id="start")

    sync_json = PythonOperator(
        task_id="sync_raw_json",
        python_callable=_sync_raw_json,
    )

    build_db = PythonOperator(
        task_id="build_duckdb",
        python_callable=_build_duckdb,
    )

    end = EmptyOperator(task_id="end")

    start >> sync_json >> build_db >> end
