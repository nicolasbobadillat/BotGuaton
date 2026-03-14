# RELEASE CHECKLIST - Datitos NAM Portfolio

Este documento detalla los pasos críticos para poner en producción el pipeline y validar su correcto funcionamiento.

## 1. Pre-Release Checks (Infraestructura)
- [ ] **Docker Containers**: Ejecutar `.\scripts\smoke_check.ps1` y asegurar que todos los servicios responden.
- [ ] **Environment**: Verificar `PF_ALERT_CHANNEL` y credenciales de Telegram si aplica.
- [ ] **Airflow Code**: Ejecutar `docker exec pf_airflow_scheduler airflow dags list-import-errors` y asegurar que está vacío.
- [ ] **MinIO Buckets**: Verificar que los 5 buckets (`raw`, `curated`, `qa`, `diff`, `public`) existen.

## 2. Release Steps (Manual Trigger)
- [ ] **Orquestador**: Activar y disparar manualmente el DAG `pf_orchestrator_daily`.
- [ ] **Monitoring**: Seguir la ejecución en `localhost:8092`. Asegurar que cada etapa (`trigger_scrape` -> `trigger_publish`) termine en **SUCCESS**.
- [ ] **OPS Report**: Verificar que existe un archivo `ops/dt=<fecha>/run_id=<id>/ops_report.json` en el bucket `nam-pf-qa`.

## 3. Post-Release Checks (Datos & Externos)
- [ ] **Publicación**: Verificar que el bucket `nam-pf-public/latest/<bank>/` tiene archivos actualizados.
- [ ] **Web MVP**: Abrir `http://localhost:8501` y confirmar que el selector de bancos muestra los datos recientemente publicados.
- [ ] **QA Results**: Confirmar que `qa_passed` es `true` en el dashboard para los bancos productivos.

## 4. Go/No-Go Criteria
- **GO**: Todas las etapas del orquestador son verdes y los datos son visibles en la Web App con KPIs correctos.
- **NO-GO**: Fallo persistente en el Scraper, QA falla sistemáticamente por volumen cero, o Web App no carga datos obligatorios.

## 5. Rollback Plan
- En caso de corrupción crítica de datos en `latest/`:
  1. Identificar el `run_id` estable previo.
  2. Usar comandos manuales de `mc cp` para restaurar `curated/<bank>/dt=<prev_dt>/run_id=<prev_id>/` hacia `latest/<bank>/`.
  3. Desactivar el orquestador programado hasta resolver el root cause.
