import sys
import json
sys.path.insert(0, '/opt/airflow/dags')
from portfolio.libs.minio_client import _get_client

client = _get_client()

ALL_BANKS = [
    "bancochile", "bancoestado", "bci", "bice", "cencosud", 
    "falabella", "internacional", "ripley", "itau", "santander", "scotiabank"
]

def get_qa_summary(bank, dt):
    try:
        # Find latest QA report for that dt
        objs = list(client.list_objects("nam-pf-qa", prefix=f"bank={bank}/dt={dt}/", recursive=True))
        reports = [o.object_name for o in objs if o.object_name.endswith("qa_report.json")]
        if not reports: return None
        
        latest_report = sorted(reports)[-1]
        resp = client.get_object("nam-pf-qa", latest_report)
        data = json.loads(resp.read().decode('utf-8'))
        resp.close()
        resp.release_conn()
        
        checks = data.get("checks", {})
        summary = []
        for m in ["M1_volume", "M2_location_coverage", "M3_valid_days", "M4_card_type", 
                  "M5_discount_range", "M6_unlimited_consistency", "M7_expiration_coverage"]:
            c = checks.get(m, {})
            status = c.get("status", "N/A")
            summary.append(f"{m}:{status}")
        
        return {
            "passed": data.get("passed"),
            "count": data.get("record_count"),
            "summary": " | ".join(summary),
            "key": latest_report,
            "run_id": data.get("dag_run_id")
        }
    except Exception as e:
        return {"error": str(e)}

# Priority on the clean 2026-03-07 run for parity evidence
check_dts = ["2026-03-07", "2026-03-08", "2026-03-09"]

results = []
for bank in ALL_BANKS:
    info = None
    for dt in check_dts:
        info = get_qa_summary(bank, dt)
        if info and not info.get("error"):
            break
    results.append((bank, info))

print("================ FINAL QA VERIFICATION ================\n")
print("| Banco | Passed | Count | M1-M7 Summary | Run ID |")
print("|---|---|---|---|---|")
for bank, info in results:
    if info and not info.get("error"):
        print(f"| {bank.ljust(13)} | {str(info['passed']).ljust(6)} | {str(info['count']).ljust(5)} | {info['summary']} | {info['run_id']} |")
    else:
        print(f"| {bank.ljust(13)} | ERROR  | N/A   | {info.get('error') if info else 'Not found'} | N/A |")
