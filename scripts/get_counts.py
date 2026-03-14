import sys
sys.path.insert(0, '/opt/airflow/dags')
from portfolio.libs.minio_client import get_latest_qa_report, get_latest_curated

ALL_BANKS = ["bancochile", "bancoestado", "bci", "bice", "santander", "scotiabank", "cencosud", "falabella", "internacional", "ripley", "itau"]

print("--- OLA B PIPELINE VERIFICATION ---")
for bank in ALL_BANKS:
    found = False
    for test_dt in ["2026-03-09", "2026-03-08", "2026-03-07"]:
        try:
            report, _ = get_latest_qa_report(bank=bank, dt=test_dt)
            print(f"[{test_dt}] BANK: {bank.ljust(15)} | CURATED_COUNT: {str(report.get('record_count')).ljust(4)} | QA_PASSED: {report.get('passed')}")
            found = True
            break
        except Exception:
            pass
    if not found:
        print(f"BANK: {bank.ljust(15)} | ERROR: No data found in recent dates.")

