import sys
import os
sys.path.insert(0, '/opt/airflow/dags')

from portfolio.pf_scrape_daily import _scrape_and_upload
from portfolio.pf_etl_daily import _etl_bank
from portfolio.pf_qa_daily import _qa_bank
from portfolio.pf_publish_daily import _publish_bank

def run_ola_b():
    banks = ["falabella", "internacional"]
    # Use 2026-03-08 since that's what we want for parity checks
    ds = "2026-03-08" 
    run_id = "manual_python_bypass"
    
    for bank in banks:
        print(f"\n=======================")
        print(f"    RUNNING {bank.upper()}    ")
        print(f"=======================\n")
        try:
            print(f"1. SCRAPING {bank}...")
            _scrape_and_upload(bank=bank, ds=ds, run_id=run_id)
            
            print(f"2. ETL {bank}...")
            _etl_bank(bank=bank, ds=ds, run_id=run_id)
            
            print(f"3. QA {bank}...")
            _qa_bank(bank=bank, ds=ds, run_id=run_id)
            
            print(f"4. PUBLISH {bank}...")
            _publish_bank(bank=bank, ds=ds, run_id=run_id)
            
            print(f">>> {bank} COMPLETED <<<")
        except Exception as e:
            print(f">>> ERROR in {bank}: {e} <<<")

if __name__ == "__main__":
    run_ola_b()
