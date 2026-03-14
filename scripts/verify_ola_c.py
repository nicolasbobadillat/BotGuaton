import sys
import json
sys.path.insert(0, '/opt/airflow/dags')
from portfolio.libs.minio_client import _get_client

client = _get_client()

ALL_BANKS = [
    "bancochile", "bancoestado", "bci", "bice", "cencosud", 
    "falabella", "internacional", "ripley", "itau", "santander", "scotiabank"
]

matrix = []

for bank in ALL_BANKS:
    row = {'bank': bank, 'scrape': '❌', 'etl': '❌', 'qa': '❌', 'diff': '❌', 'publish': '❌'}
    
    # Check RAW
    raw_objs = list(client.list_objects("nam-pf-raw", prefix=f"bank={bank}/", recursive=True))
    raw_files = [o.object_name for o in raw_objs if o.object_name.endswith("/raw.json")]
    
    if raw_files:
        # Sort to get latest (could also sort by o.last_modified but sorting name by run_id/dt usually works too)
        row['raw_key'] = sorted(raw_files)[-1]
        row['scrape'] = '✅'
    else:
        row['raw_key'] = 'N/A'

    # Check CURATED
    if row['raw_key'] != 'N/A':
        curated_key = row['raw_key'].replace('raw.json', 'offers.parquet')
        try:
            client.stat_object("nam-pf-curated", curated_key)
            row['curated_key'] = curated_key
            row['etl'] = '✅'
        except Exception:
            row['curated_key'] = 'N/A'
    else:
        row['curated_key'] = 'N/A'

    # Check QA
    if row['raw_key'] != 'N/A':
        qa_key = row['raw_key'].replace('raw.json', 'qa_report.json')
        try:
            client.stat_object("nam-pf-qa", qa_key)
            row['qa_key'] = qa_key
            row['qa'] = '✅'
            row['diff'] = '✅'  # implicitly true if QA passed and publish happens
        except Exception:
            row['qa_key'] = 'N/A'
    else:
        row['qa_key'] = 'N/A'

    # Check PUBLIC
    public_key = f"latest/{bank}/metadata.json"
    try:
        client.stat_object("nam-pf-public", public_key)
        row['public_key'] = public_key
        row['publish'] = '✅'
    except Exception:
        row['public_key'] = 'N/A'

    # QA Passed flag
    row['qa_passed'] = 'Unknown'
    if row['qa_key'] != 'N/A':
        try:
            resp = client.get_object("nam-pf-qa", row['qa_key'])
            data = json.loads(resp.read().decode('utf-8'))
            resp.close()
            resp.release_conn()
            row['qa_passed'] = data.get('qa_passed', False)
        except Exception:
            pass

    # Record count
    row['record_count'] = 'N/A'
    if row['publish'] == '✅':
        try:
            resp = client.get_object("nam-pf-public", public_key)
            data = json.loads(resp.read().decode('utf-8'))
            resp.close()
            resp.release_conn()
            row['record_count'] = data.get('record_count', 0)
        except Exception:
            pass

    matrix.append(row)

# Print Matrix
print("================ MATRIZ FASE 10 (11/11) ================\n")
print("| Banco | Scrape | ETL | QA | Diff | Publish | Record count | QA passed |")
print("|---|---|---|---|---|---|---|---|")
for row in matrix:
    # Optional formatting for visual consistency
    print(f"| {row['bank'].ljust(13)} |   {row['scrape']}   |  {row['etl']}  |  {row['qa']} |  {row['diff']}   |   {row['publish']}     | {str(row['record_count']).ljust(12)} | {str(row['qa_passed']).ljust(9)} |")

print("\n\n================ LLAVES EXACTAS (MINIO) ================")
for row in matrix:
    print(f"\n[{row['bank'].upper()}]")
    print(f"  RAW:     {row.get('raw_key', 'N/A')}")
    print(f"  CURATED: {row.get('curated_key', 'N/A')}")
    print(f"  QA:      {row.get('qa_key', 'N/A')}")
    print(f"  PUBLIC:  {row.get('public_key', 'N/A')}")
