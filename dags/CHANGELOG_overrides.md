# Changelog: SQL Overrides & Manual Patches

## [2026-03-13] - DuckDB Pipeline Robustness & Diff Architecture

### Added
- **Instagram Tracking**: Añadida la columna `instagram` a `dim_restaurants` en `schema_v2.sql`. Los primeros 6 restaurantes fueron poblados manualmente a través de updates transaccionales en `zz_01_user_knowledge.sql`.
- **Pipeline Fault Tolerance**: El `build_duckdb` en `duckdb_loader.py` ahora ignora dinámicamente cualquier transformador SQL si su correspondiente archivo JSON está vacío (`[]`). Esto evita errores `Binder Error` en cascada y permite que el ETL continúe procesando los bancos restantes incluso si uno o más scrapers fallan.
- **MinIO Fault Tolerance**: `sync_raw_json` modificado para escribir arreglos vacíos `[]` y omitir los errores tipo `FileNotFoundError` cuando un banco no existe en el storage remoto para una ejecución dada.
- **Empty Diff Guardrail**: `duckdb_diff.py` ahora implementa un guardrail "empty_current". Si `fact_offers` está vacía (cero datos scrapeados), el sistema se salta la lectura en lugar de marcar artificialmente todas las ofertas históricas como `removed`.

### Changed
- **Diff Constraints**: Reemplazo de PRIMARY KEY único (`diff_id`) por llave PRIMARY compuesta `(diff_date, run_id, offer_id, diff_type)` en `diff_offers`. 
- **Diff Scope Validation**: Ahora las instrucciones `DELETE` en el paso de diferencias son acotadas mediante `(diff_date, run_id)` en lugar de solo por `diff_date`, asegurando que reruns dentro del mismo día generen historiales separados sin pisar ejecuciones previas.
- **Snapshot Logic**: Añadido un `DELETE` en `offers_snapshot` previo a la copia para permitir que re-ejecuciones de DAG actúen con semánticas Replace en vez de ignorarlas por la constraint de uniqueness.

### Fixed
- **Banco de Chile Transformer**: Eliminada la referencia directa a `title` en el archivo principal a favor de validaciones previas por su dependencia explícita que bloqueaba los `Binder`.
- **Bice Transformer**: Solucionados los errores de inferencia `discount_cap` y `expiration_date`. Ambas variables se leen ahora exclusivamente empleando fallbacks robustos de RegEx inspeccionando la columna string `description`.
- **Foreign Key Violation**: Añadido un `DELETE FROM fact_offers` intermedio dentro del proceso de `duckdb_loader.py` (posterior al snapshot) para liberar los constraints en cadena que impedían el refresco completo de la tabla categórica `dim_restaurants`.
## [2026-03-02] - Consolidación de Itaú, Categorías y Cadenas

### Added
- **San Pedro de la Paz & Chicureo**: Agregado `Chicureo` a la tabla `dim_locations` en `schema_v2.sql` para permitir match con BICE. Eliminada la exclusión de `sped` (San Pedro) en `itau.sql`.
- **Global Exclusions**: Agregadas exclusiones globales para `Penguin`, `Perry Ellis` y `Trial` en `00_restaurants.sql` e `itau.sql` para evitar parseo como restaurantes.

### Changed
- **Itaú Card Logic (Legend vs Black)**: Rediseño completo de la lógica de tarjetas en `itau.sql` usando `GROUP BY` y `string_agg()`. Ahora detecta correctamente cuando existen ambas ofertas y consolida en `itau_combined`, evitando pérdida de datos por deduplicación.
- **Itaú Regex Parsing**: Cambiado `ILIKE` por regex contextual (ej. `(?i)Tarjetas\s+Legend`) para diferenciar ofertas activas de exclusiones (ej. "excluye tarjetas black").
- **Heladería Larrs**: Actualizado el regex en `00_restaurants.sql` para matchear tanto `HELADERIA LARRS` como simplemente `LARRS` como tipo de cocina *Helado*.

### Fixed
- **La Pasta de la Nonna (Itaú)**: Corregido el mapeo de ubicación. Ahora resuelve correctamente a Concepción (`conp`) y San Pedro de la Paz (`sped`) en lugar de caer al fallback de Estación Central.
- **Al Pesto (BICE)**: Corregido bug donde la oferta de los Lunes no se cargaba por falta de `Chicureo` en las locaciones de referencia.
- **Cencosud Burger King**: Eliminado Burger King completamente de Cencosud mediante un override de limpieza en `zz_02_overrides.sql`.
- **Santander Just Burger**: Forzado el `location_id` a `all` mediante override para aplicar la oferta de cadena a todos los locales.
- **Dominga Bistro**: Re-añadido el `DELETE` en `zz_02_overrides.sql` para Banco de Chile, dado que ya no tiene descuento en marzo.

## [2026-02-28] - Itaú Discount Caps & Location Precision

### Added
- **Itaú Discount Caps**: Robust extraction of discount caps for Itaú using regex `(?i)(?:tope|hasta|maximo)[^$]*?\$([\d\.]+)`.
- **Multi-Field Extraction**: The Itaú transformer now scans both `conditions` and `description` fields to catch caps (e.g., **Marley Coffee**).
- **Location Precision**: Added explicit mapping for **Alonso de Córdova** to **Vitacura** (`vita`) in `itau.sql`.

### Fixed
- **Itaú Column Shadowing**: Resolved binder errors and missing caps caused by output columns shadowing input data in `itau.sql`.
- **Override Preservation**: Updated `zz_02_overrides.sql` to include `discount_cap` and `discount_cap_is_unlimited` in the **La Maestranza** expansion logic, preventing data loss.
- **Muu Grill & Rocoto**: Verified and reinforced location overrides for Falabella in `zz_02_overrides.sql`.

## [2026-02-24] - Nivelación de Días y Categorías Globales

### Changed
- **Banco Estado**: Refactor de lógica en `bancoestado.sql` para no filtrar antojos con formato de día anómalo, forzando correctamente Juan Maestro, Doggis, Dominos y Barrio Chicken a **Martes**.
- **Categorización Global**: **Starbucks** añadido a la lista global de *Antojos* en `00_restaurants.sql`.
- **Mamut**: Eliminado override obsoleto en `zz_02_overrides.sql` que lo forzaba a *Restaurante*, normalizando su categoría a *Antojo*.
- **Leonidas Chocolate (Itaú)**: Implementado fallback en `itau.sql` para extraer días truncados; validado **Miércoles**.
- **Marley Coffee (Itaú y Ripley)**: Forzado a **Jueves** (corrigiendo truncamiento de descripciones y errores de origen de datos).
- **Just Burger (Ripley)**: Resuelta regresión (JSON retornaba arreglo vacío), fijando días en **Martes**.

## [2026-02-23] - Antojo Expansion & Definitive Fixes

### Added
- **Antojo Expansion**: Added "Barrio Chicken", "La Fête Chocolat", "Heladería Larrs", "China 365", and "Buffet Express" to the global categorization rules.
- **Improved Chain Detection**: These new restaurants are now automatically marked as `is_chain = TRUE`.
- **McDonald's Consistency**: Simplified rules to map all variants (App McDonalds, McDonald's, etc.) as **Antojo** across all banks.

### Changed
- **Lorenzo Lounge-Bar (Itaú)**: Recategorized from 'Antojo' to **'Restaurante'**. Fix implemented at the root (`00_restaurants.sql`) and reinforced in `zz_02_overrides.sql` to bypass incorrect source classification.
- **Leonidas Chocolate (Itaú)**: Forced valid days to **Miércoles** via override.
- **Marley Coffee (Itaú)**: Forced valid days to **Jueves** via override.
- **Philia (Ripley)**: Updated valid days from 'Todos los días' to **Jueves a Domingo** to match real conditions.
- **Prioritization**: Re-ordered transformer logic so that our global Antojo/Online lists take precedence over the categories provided by the scrapers' raw JSON data.

### Fixed
- **Database Locks**: Implemented a more robust wait and termination strategy for DuckDB connections during pipeline runs.
