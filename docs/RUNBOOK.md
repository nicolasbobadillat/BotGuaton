# RUNBOOK - Datitos NAM Portfolio

Este documento describe cómo operar, detener, reiniciar y solucionar problemas comunes del pipeline de datos de Datitos NAM Portfolio.

## 1. Gestión del Stack (Docker Compose)

El proyecto completo está dockerizado y aislado bajo el prefijo `pf-`.

**Iniciar todo el stack (modo detached):**
```bash
docker compose up -d
```
*(Nota: Airflow puede tardar unos 30-60 segundos tras levantar los contenedores para que la UI esté disponible).*

**Detener el stack y remover contenedores:**
```bash
docker compose down
```

**Reiniciar un servicio específico (ej. scheduler):**
```bash
docker restart pf_airflow_scheduler
```

**Ver logs de un servicio (ej. webserver):**
```bash
docker logs -f pf_airflow_webserver
```

---

## 2. Acceso a Interfaces (UI)

*   **Airflow Web UI:** `http://localhost:8092` (admin / admin)
*   **MinIO Console:** `http://localhost:9001` (minioadmin / minioadmin). Buckets: *nam-pf-raw, nam-pf-curated, nam-pf-qa, nam-pf-diff, nam-pf-public*.
*   **Selenium Grid:** `http://localhost:4444` (Para ver las sesiones activas de scraping).
*   **App MVP (Streamlit):** `http://localhost:8501` (Si está levantada manualmente).

---

## 3. Ejecución Manual de Pipelines (DAGs)

Los DAGs están programados de forma diaria (`@daily`), pero se pueden ejecutar manualmente:

**Vía Web UI:**
1. Ve a `http://localhost:8092`.
2. Enciende el switch (Unpause) del DAG si está apagado.
3. Haz clic en el botón de "Play" (Trigger DAG).

**Vía Terminal (Docker Exec):**
Probar un DAG para una fecha específica sin afectar la base de metadatos oficial (ideal para debugear):
```bash
docker exec -e MINIO_ENDPOINT=pf-minio:9000 pf_airflow_scheduler airflow dags test pf_scrape_daily 2026-03-08
```
*(Reemplaza con `pf_duckdb_daily`, `pf_diff_duckdb_daily`, etc.).*

---

## 3b. Pipeline DuckDB + Diff (Flujo Activo)

### Flujo completo (orquestado por `pf_orchestrator_duckdb`)

```
pf_orchestrator_duckdb
  ├─ trigger_scrape ──→ pf_scrape_daily       (Selenium → JSON → MinIO)
  ├─ trigger_duckdb ──→ pf_duckdb_daily       (JSON → snapshot → transformers → DuckDB)
  ├─ trigger_diff   ──→ pf_diff_duckdb_daily  (diff snapshot vs fact_offers → diff_offers + MinIO JSON)
  └─ generate_ops_report                       (reporte operacional)
```

### DAGs involucrados

| DAG | Descripción | schedule |
|---|---|---|
| `pf_orchestrator_duckdb` | Orquestador activo. Ejecuta todo en cadena. | `@daily` |
| `pf_scrape_daily` | Scraping vía Selenium. Resultado en MinIO `nam-pf-raw`. | `None` (trigger) |
| `pf_duckdb_daily` | Descarga JSON de MinIO, snapshot `fact_offers` → `offers_snapshot`, rebuild con transformers SQL. | `None` (trigger) |
| `pf_diff_duckdb_daily` | Compara `fact_offers` actual vs snapshot anterior. Persiste en `diff_offers` + JSON a MinIO `nam-pf-diff`. | `None` (trigger) |

### Tablas DuckDB para diff

- **`offers_snapshot`** (`snapshot_date, offer_id` PK) — Estado diario de `fact_offers`.
- **`diff_offers`** (`diff_date, run_id, offer_id, diff_type` PK) — Diffs: `added`, `removed`, `changed`.

### Comportamiento definido

| Escenario | Comportamiento |
|---|---|
| **Rerun mismo día** | El snapshot se reemplaza (DELETE + INSERT). |
| **Múltiples runs/día** | Cada `run_id` conserva su propio diff (PK compuesta). |
| **fact_offers vacío** | Guardrail: retorna `status=empty_current`, no genera diffs falsos. |
| **Sin snapshot previo** | Retorna `status=no_previous_snapshot`, primera corrida sin diff. |

### Reporte JSON en MinIO

- **Bucket:** `nam-pf-diff`
- **Path:** `duckdb/dt=<fecha>/run_id=<id>/diff_summary.json`
- **Contenido:** `{counts: {added, removed, changed, current_total, previous_total}, status}`

---

## 4. Validar el `latest` Publicado

Para estar seguro de qué hay en producción:
1. Entra a la consola MinIO (`localhost:9001`).
2. Ve al bucket `nam-pf-public/latest/santander/`.
3. Descarga `metadata.json` y verifica `published_at`, `record_count` y `qa_passed`.
4. (Alternativa rápida): Abre la **App Streamlit** en `http://localhost:8501` y revisa los KPIs superiores.

---

## 5. Troubleshooting (Solución de Problemas)

### A. Airflow arroja errores de importación (Import Errors)
*Error:* En la UI aparece una barra roja "Broken DAG".
*Solución:* Validar importaciones ejecutando:
`docker exec pf_airflow_scheduler airflow dags list-import-errors`
Normalmente es un error de sintaxis en `dags/portfolio/` o dependencias faltantes.

### B. El Scraper falla conectando a Selenium
*Error:* `urllib3.exceptions.MaxRetryError: HTTPConnectionPool(host='pf-selenium', ...)`
*Solución:* El contenedor de Selenium Grid podría estar caído o saturado.
`docker restart pf_selenium`
Si el problema persiste de inmediato, asegúrate que las variables de entorno de Airflow tengan `SELENIUM_GRID_URL="http://pf-selenium:4444/wd/hub"`.

### C. El Task de Publish Falla silenciosamente (o Runtime Error)
*Error:* QA no pasó y el log dice "QA gate FAILED".
*Solución:* Publicar fue bloqueado intencionalmente. Revisa el archivo `qa_report.json` en el bucket `nam-pf-qa` para la fecha afectada, o entra a la tarea `qa_santander` en el DAG `pf_qa_daily` y lee los logs correspondientes a los checks M1-M7 que fallaron.

### D. La App Streamlit no inicializa
*Error:* "Network datitos_nam_pf_default not found" al crear el contenedor docker para Streamlit.
*Solución:* Asegúrate de estar usando el nombre correcto de la red puente que generó `docker-compose`. Puedes usar `docker network ls` para ver la red actual. La variable `MINIO_ENDPOINT="pf-minio:9000"` debe poder resolverse dentro de dicha red.

### E. Faltan bancos en DuckDB ("Skipping ... because json is empty")
*Síntoma:* El log del step `build_duckdb` o `sync_raw_json` muestra advertencias tipo `⚠️ itau: raw.json not found` o `⏩ Skipping itau.sql`.
*Solución:* Esto es una funcionalidad de tolerancia a fallos. Significa que el scraper de ese banco falló (ej. Cloudflare bot block). El pipeline continúa construyendo DuckDB con los bancos restantes para no detener el sistema. En la próxima ejecución donde el scraper funcione, los datos se recuperarán solos y el *diff* los marcará como "Added".

---

## 6. Observabilidad y Alertas

### A. Alertas Automáticas (Telegram/Log)
El pipeline envía alertas automáticas si falla cualquier etapa del orquestador. El canal se configura vía:
- `PF_ALERT_CHANNEL=telegram`
- `PF_TELEGRAM_BOT_TOKEN=...`
- `PF_TELEGRAM_CHAT_ID=...`

Si no se configuran, las alertas caen en los logs de Airflow con el prefijo `[ERROR] ❌ Pipeline failure`.

### B. Reporte Operativo (`ops_report.json`)
Al final de cada corrida, el orquestador genera un reporte en MinIO:
- **Bucket:** `nam-pf-qa`
- **Path:** `ops/dt=<fecha>/run_id=<id>/ops_report.json`

Este JSON contiene tiempos de inicio/fin, estado final y auditoría básica de la corrida.

### C. Diff Report
Al final de cada corrida DuckDB, el diff genera un JSON resumen en MinIO:
- **Bucket:** `nam-pf-diff`
- **Path:** `duckdb/dt=<fecha>/run_id=<id>/diff_summary.json`
