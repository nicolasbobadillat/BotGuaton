"""
pf_scrape_daily – Daily scraping pipeline.

Phase 3 pilot: Santander is the only bank with real scraping logic.
Other banks remain as stubs (EmptyOperator) until later phases.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import os

# Banks with real scraping implemented
LIVE_BANKS = ["santander", "bancoestado", "bice", "scotiabank", "bci", "bancochile", "cencosud", "falabella", "internacional", "ripley", "itau"]

# Banks still pending implementation
STUB_BANKS = []

# M4: Configurable Empty Payload Policy & Timeout
ALLOW_EMPTY_PARSED = os.environ.get("PF_SCRAPE_ALLOW_EMPTY", "")
SCRAPE_ALLOW_EMPTY = {b.strip() for b in ALLOW_EMPTY_PARSED.split(",") if b.strip()}
TIMEOUT_MIN = int(os.environ.get("PF_SCRAPE_TIMEOUT_MIN", "25"))

default_args = {
    "owner": "portfolio",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------
def _scrape_and_upload(bank: str, **context):
    """Scrape a bank and upload raw JSON to MinIO."""
    from portfolio.libs.scraper_adapter import (
        scrape_santander, scrape_ripley, scrape_itau
    )
    import portfolio.libs.scraper_adapter as scraper_adapter
    from portfolio.libs.minio_client import upload_raw_json

    # Select scraper
    scrapers = {
        "santander": scrape_santander,
        "bancoestado": scraper_adapter.scrape_bancoestado,
        "bice": scraper_adapter.scrape_bice,
        "scotiabank": scraper_adapter.scrape_scotiabank,
        "bci": scraper_adapter.scrape_bci,
        "bancochile": scraper_adapter.scrape_bancochile,
        "cencosud": scraper_adapter.scrape_cencosud,
        "falabella": scraper_adapter.scrape_falabella,
        "internacional": scraper_adapter.scrape_internacional,
        "ripley": scrape_ripley,
        "itau": scrape_itau,
    }
    scraper_fn = scrapers[bank]

    # Resolve context
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    pf_dt = conf.get("pf_dt", context["ds"])
    pf_run_id = conf.get("pf_run_id", context["run_id"])

    # Run scraper
    print(f"--- SCRAPING {bank} | dt={pf_dt} | run_id={pf_run_id} ---")
    data = scraper_fn()
    count = len(data)
    print(f"Scraped {count} records from {bank}")

    # M4: Empty payload logic
    is_empty = (count == 0)
    is_allowed = bank in SCRAPE_ALLOW_EMPTY

    if is_empty:
        if is_allowed:
            print(f"WARNING: No records found for {bank}. Continuing because bank is in ALLOW_EMPTY.")
        else:
            raise RuntimeError(
                f"Scraper for {bank} returned 0 records and is NOT in ALLOW_EMPTY list. "
                "Failing task to prevent silent empty data success."
            )

    # Upload to MinIO
    metadata = upload_raw_json(
        bank=bank,
        data=data,
        dt=pf_dt,
        run_id=pf_run_id,
        is_empty=is_empty,
        is_allowed=is_allowed,
    )

    print(f"--- Upload metadata ---")
    for k, v in metadata.items():
        print(f"  {k}: {v}")

    return metadata

def _audit_log(**context):
    print("=== AUDIT LOG ===")
    print(f"Run ID: {context['run_id']}")
    ti = context['ti']
    for bank in LIVE_BANKS:
        stats = ti.xcom_pull(task_ids=f"scrape_{bank}")
        if stats:
            print(f"[{bank}] Status: SUCCESS")
            print(f"  Record Count: {stats.get('record_count', 'N/A')}")
            print(f"  Raw Key: {stats.get('raw_key', 'N/A')}")
        else:
            print(f"[{bank}] Status: FAILED OR PENDING")

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="pf_scrape_daily",
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=16,
    tags=["portfolio", "pf"],
    doc_md="Scrape all banks and store raw JSON snapshots in MinIO.",
) as dag:

    start = EmptyOperator(task_id="start")
    
    audit = PythonOperator(
        task_id="scrape_audit",
        python_callable=_audit_log,
        trigger_rule="all_done",
    )
    
    end   = EmptyOperator(task_id="end")

    # Live tasks (real scraping + MinIO upload)
    prev_task = start
    for bank in LIVE_BANKS:
        task = PythonOperator(
            task_id=f"scrape_{bank}",
            python_callable=_scrape_and_upload,
            op_kwargs={"bank": bank},
            execution_timeout=timedelta(minutes=TIMEOUT_MIN),
        )
        prev_task >> task
        prev_task = task
        
    prev_task >> audit >> end

    # Stub tasks (placeholder until implementation)
    # Stub tasks just hook up in parallel since they do nothing
    for bank in STUB_BANKS:
        task = EmptyOperator(task_id=f"scrape_{bank}")
        start >> task >> audit >> end
