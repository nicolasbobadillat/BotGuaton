"""
pf_orchestrator_duckdb - MinIO -> SQL transformers -> DuckDB.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "portfolio",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="pf_orchestrator_duckdb",
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule="@daily",
    catchup=False,
    tags=["portfolio", "pf", "duckdb"],
    doc_md="Scrape -> DuckDB using SQL transformers.",
) as dag:

    trigger_scrape = TriggerDagRunOperator(
        task_id="trigger_scrape",
        trigger_dag_id="pf_scrape_daily",
        wait_for_completion=True,
        conf={
            "pf_dt": "{{ ds }}",
            "pf_run_id": "{{ run_id }}",
        },
    )

    trigger_duckdb = TriggerDagRunOperator(
        task_id="trigger_duckdb",
        trigger_dag_id="pf_duckdb_daily",
        wait_for_completion=True,
        conf={
            "pf_dt": "{{ ds }}",
            "pf_run_id": "{{ run_id }}",
        },
    )

    trigger_diff_duckdb = TriggerDagRunOperator(
        task_id="trigger_diff_duckdb",
        trigger_dag_id="pf_diff_duckdb_daily",
        wait_for_completion=True,
        conf={
            "pf_dt": "{{ ds }}",
            "pf_run_id": "{{ run_id }}",
        },
    )

    def _notify_failure(**context):
        from portfolio.libs.alerts import send_alert
        dag_run = context.get("dag_run")
        failed_tasks = [ti.task_id for ti in dag_run.get_task_instances() if ti.state == "failed"]

        send_alert(
            message=f"Pipeline failure in {dag_run.dag_id}",
            level="ERROR",
            context={
                "run_id": dag_run.run_id,
                "ds": context["ds"],
                "failed_tasks": failed_tasks,
            },
        )

    def _generate_ops_report(**context):
        from portfolio.libs.minio_client import upload_ops_report
        dag_run = context.get("dag_run")
        start_time = dag_run.start_date
        end_time = datetime.now(start_time.tzinfo) if start_time and start_time.tzinfo else datetime.now()
        duration_sec = (end_time - start_time).total_seconds() if start_time else 0

        report = {
            "dag_id": dag_run.dag_id,
            "run_id": dag_run.run_id,
            "ds": context["ds"],
            "start_time": start_time.isoformat() if start_time else None,
            "end_time": end_time.isoformat(),
            "duration_sec": duration_sec,
            "state": dag_run.state,
            "counts_per_stage": {
                ti.task_id: ti.state for ti in dag_run.get_task_instances()
                if ti.task_id.startswith("trigger_")
            },
        }
        upload_ops_report(report, context["ds"], dag_run.run_id)

    notify_failure = PythonOperator(
        task_id="notify_failure",
        python_callable=_notify_failure,
        trigger_rule="one_failed",
    )

    generate_ops_report = PythonOperator(
        task_id="generate_ops_report",
        python_callable=_generate_ops_report,
        trigger_rule="all_done",
    )

    trigger_scrape >> trigger_duckdb >> trigger_diff_duckdb

    for t in [trigger_scrape, trigger_duckdb, trigger_diff_duckdb]:
        t >> notify_failure

    trigger_diff_duckdb >> generate_ops_report
