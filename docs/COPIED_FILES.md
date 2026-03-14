# Copia Selectiva Realizada

## Incluido
- `dags/` con scrapers, runners y SQL ETL.
- `infra/airflow.Dockerfile` y `infra/requirements-airflow.txt`.
- `README_SOURCE.md` y `ONBOARDING_SOURCE.md`.
- `docker-compose.yml` aislado para portfolio.

## Excluido (a propósito)
- Bases de datos (`*.duckdb`).
- `tmp/`, `sandbox/`, `__pycache__/`.
- Artefactos de debug (`*.png`, `*.html`, `*.log`, `out.txt`).
- JSON crudos históricos (`dags/json/`).
- Bot/API legacy no necesarios para stack portfolio.

## Nota técnica clave
Los SQL de `dags/sql/transformers/` mantienen rutas absolutas heredadas al repo original.
El primer bloque de trabajo del dev es parametrizar esas rutas para el repo `datitos_nam_portfolio`.
