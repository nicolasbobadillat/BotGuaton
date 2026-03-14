# DEV Handoff - Quick Start

1. `Copy-Item .env.example .env`
2. `docker compose -p datitos_nam_pf --env-file .env up -d --build`
3. `docker compose -p datitos_nam_pf --env-file .env run --rm pf-airflow-init`
4. `docker compose -p datitos_nam_pf --env-file .env up -d pf-airflow-webserver pf-airflow-scheduler pf-airflow-triggerer`
5. Airflow UI: `http://localhost:8092`
6. MinIO UI: `http://localhost:9101`

## Primer TODO del dev
- Parametrizar rutas absolutas en `dags/sql/transformers/*.sql`.
- Crear DAGs `pf_*` (scrape, etl, qa, diff, publish).

7. Si hay rutas absolutas heredadas, ejecutar: powershell scripts/patch_sql_paths.ps1
