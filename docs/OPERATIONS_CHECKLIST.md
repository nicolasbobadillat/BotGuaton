# OPERATIONS CHECKLIST

Usa este checklist para validaciones manuales periódicas del pipeline de Datitos NAM Portfolio.

## Revisión Diaria (Daily)

- [ ] **Estabilidad del Pipeline:** Ingresar a Airflow (`localhost:8092`) y validar que la última ejecución diaria (DagRuns) de los 5 DAGs se completaron exitosamente (`state=success`).
- [ ] **Monitoreo de QA:** Revisar si hubo alguna caída en los checks M1-M7 durante la fase de QA. (Visible en los logs de `pf_qa_daily` -> task de auditoría).
- [ ] **Volumen de Ofertas:** Verificar en la UI (App Streamlit) que el número de ofertas de Santander se mantiene estable comparado con días previos, o revisar el módulo *Daily Changes Summary*.
- [ ] **Errores de DAGs:** Validar que la interfaz web de Airflow no reporta import errors (bandas rojas en la cabecera).

## Revisión Semanal (Weekly)

- [ ] **Limpieza de Espacio (Logs/Data):** Validar cuánto espacio en disco consume el bucket de RAW (`nam-pf-raw`). *Acción sugerida futura: implementar lifecycle rules de MinIO para borrar .json raw después de 30 días.*
- [ ] **Salud de Contenedores:** Correr `scripts/smoke_check.ps1` localmente para confirmar que todos los servicios y puertos esenciales están operando y escuchando correctamente.
- [ ] **Cambios de Estructura de Target (Scraping):** Entrar a portal de Santander manualmente y a la App Streamlit. Comparar un par de promociones al azar para certificar que el HTML parseado por el scraper sigue siendo vigente y los localizadores del XPath no han quedado obsoletos de manera silenciosa (donde por ejemplo, se capturen cap_discounts nulos porque la clase del modal cambió).
