import os
import sys
import pandas as pd
from datetime import datetime, timezone
import json

# Add dags to path
sys.path.insert(0, "/opt/airflow/dags")
from portfolio.libs.minio_client import upload_curated_parquet, get_latest_qa_report
from portfolio.pf_qa_daily import _qa_bank
from portfolio.pf_publish_daily import _publish_bank

def test_empty_publish_block():
    print("--- 1. Simulating Empty Curated File for Santander ---")
    empty_df = pd.DataFrame(columns=["bank", "title", "discount_text", "location", "description", "discount_cap", "expiration_date", "url", "image_url", "scraped_at", "category", "cuisine", "card_type", "discount_cap_is_unlimited", "discount_pct", "valid_days"])
    ds = "2026-03-08"
    run_id = "test_empty_guardrail"
    
    import io as _io
    buf = _io.BytesIO()
    empty_df.to_parquet(buf, index=False, engine="pyarrow")
    parquet_bytes = buf.getvalue()
    
    upload_curated_parquet(bank="santander", parquet_bytes=parquet_bytes, dt=ds, run_id=run_id, record_count=0, input_raw_key="dummy_raw_key.json")
    print("Uploaded 0-record Parquet.")

    print("\n--- 2. Running QA (Expecting M1 = FAIL) ---")
    os.environ["ALLOW_EMPTY_PUBLISH"] = "false"
    try:
        _qa_bank(bank="santander", ds=ds, run_id=run_id)
    except RuntimeError as e:
        print(f"EXPECTED ERROR CAUGHT: {e}")
    
    # Check what QA saved
    qa_report, _ = get_latest_qa_report(bank="santander", dt=ds)
    print(f"QA passed flag saved to MinIO: {qa_report.get('passed')}")

    print("\n--- 3. Running Publish with Failed QA (Expecting Block) ---")
    try:
        _publish_bank(bank="santander", ds=ds, run_id=run_id)
        print("ERROR: Publish succeeded when it should have blocked!")
    except RuntimeError as e:
        print(f"EXPECTED PUBLISH BLOCK CAUGHT: {e}")

if __name__ == "__main__":
    test_empty_publish_block()
