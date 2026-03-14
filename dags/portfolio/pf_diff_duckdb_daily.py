"""
pf_diff_duckdb_daily - Day-over-day diff on DuckDB fact_offers.

Compares current fact_offers vs the previous offers_snapshot.
Writes results to diff_offers table + uploads JSON summary to MinIO.
Runs after pf_duckdb_daily.
"""
import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


DEFAULT_DUCKDB_PATH = "/opt/airflow/dags/datitos_nam.duckdb"


def _compute_diff(**context):
    from portfolio.libs.duckdb_diff import compute_duckdb_diff

    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    pf_dt = conf.get("pf_dt", context["ds"])
    pf_run_id = conf.get("pf_run_id", context["run_id"])
    db_path = os.environ.get("PF_DUCKDB_PATH", DEFAULT_DUCKDB_PATH)

    print(f"--- DIFF DUCKDB | dt={pf_dt} | run_id={pf_run_id} ---")

    summary = compute_duckdb_diff(
        diff_date=pf_dt,
        run_id=pf_run_id,
        db_path=db_path,
    )
    return summary


def _upload_diff_report(**context):
    from portfolio.libs.minio_client import get_client, DIFF_BUCKET
    import io

    ti = context["ti"]
    summary = ti.xcom_pull(task_ids="compute_diff")
    if not summary:
        print("⚠️ No diff summary to upload")
        return

    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    pf_dt = conf.get("pf_dt", context["ds"])
    pf_run_id = conf.get("pf_run_id", context["run_id"])

    client = get_client()
    if not client.bucket_exists(DIFF_BUCKET):
        client.make_bucket(DIFF_BUCKET)

    key = f"duckdb/dt={pf_dt}/run_id={pf_run_id}/diff_summary.json"
    payload = json.dumps(summary, ensure_ascii=False, indent=2).encode("utf-8")
    client.put_object(
        DIFF_BUCKET, key, io.BytesIO(payload),
        length=len(payload), content_type="application/json",
    )
    print(f"✅ Uploaded {key} ({len(payload)} bytes)")
    return {"key": key}


def _audit_log(**context):
    ti = context["ti"]
    summary = ti.xcom_pull(task_ids="compute_diff")

    print("=== DIFF DUCKDB AUDIT ===")
    if not summary:
        print("No diff summary available")
        return

    counts = summary.get("counts", {})
    print(f"  Date:     {summary.get('diff_date')}")
    print(f"  Prev:     {summary.get('prev_snapshot_date', 'N/A')}")
    print(f"  Status:   {summary.get('status')}")
    print(f"  Added:    {counts.get('added', 0)}")
    print(f"  Removed:  {counts.get('removed', 0)}")
    print(f"  Changed:  {counts.get('changed', 0)}")
    print(f"  Current:  {counts.get('current_total', 0)}")
    print(f"  Previous: {counts.get('previous_total', 0)}")


default_args = {
    "owner": "portfolio",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="pf_diff_duckdb_daily",
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule=None,
    catchup=False,
    tags=["portfolio", "pf", "duckdb"],
    doc_md="Day-over-day diff on DuckDB fact_offers using persistent snapshots.",
) as dag:

    start = EmptyOperator(task_id="start")

    compute_diff = PythonOperator(
        task_id="compute_diff",
        python_callable=_compute_diff,
    )

    upload_report = PythonOperator(
        task_id="upload_diff_report",
        python_callable=_upload_diff_report,
    )

    audit = PythonOperator(
        task_id="audit_log",
        python_callable=_audit_log,
        trigger_rule="all_done",
    )

    end = EmptyOperator(task_id="end")

    start >> compute_diff >> upload_report >> audit >> end
