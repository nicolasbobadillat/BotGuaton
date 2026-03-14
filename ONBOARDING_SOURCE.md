# 🚀 Guía de Onboarding para Desarrolladores: Datitos Nam

¡Bienvenido al equipo de Datitos Nam! Esta guía está diseñada para que puedas entender rápidamente cómo funciona el proyecto, dónde están las partes clave y qué hace cada componente.

## 🗺️ Arquitectura del Proyecto

Este proyecto recolecta (Scraping), transforma (ETL) y guarda descuentos de bancos en una base de datos DuckDB. Operamos en un flujo lineal diario.

El directorio de trabajo principal es `dags/`.

### 1. Extracción (Scraping & JSON)
Los scripts recolectan la página web de cada banco y guardan el HTML/API crudo en archivos JSON temporales. Estos no se tocan.
- **Scrapers:** `dags/*_scraper.py` (ej. `bci_scraper.py`, `itau_scraper.py`)
- **Datos Crudos:** Se almacenan en `dags/json/*.json`.

### 2. Transformadores (SQL ETL)
El núcleo de la inteligencia del proyecto. Leen los JSONs y los limpian usando consultas SQL de "Extract, Transform, Load".
- **Directorio:** `dags/sql/transformers/`
- **Generales:**
  - `00_restaurants.sql`: Modifica nombres globalmente, clasifica franquicias (Antojo, Helado, etc.) a base de REGEX en todo el pipeline. Se asegura que la "McDonald's" del Chile sea la misma "McDonalds" del BCI.
  - `zz_01_user_knowledge.sql`: Inserta locaciones conocidas en duro que no son evidentes (ej. Parques, Edificios).
  - `zz_02_overrides.sql`: La "Libreta del Jefe". Reglas forzadas (UPDATES y DELETES) ejecutadas sobre el producto final `fact_offers` para arreglar errores persistentes en los bancos que son muy complejos para un Regex manual. (ej. *Forzar Just Burger a ser locación global en Santander*).
  - `zz_03_normalize_days.sql`: Limpia la columna de días para dejar formatos uniformes.
- **Específicos por Banco:** `dags/sql/transformers/<banco>.sql` (ej. `itau.sql`, `bice.sql`). Cada uno extrae, limpia, maneja fechas, topes y ubica coordenadas. Tienen sus propias CTEs y lógica y son ejecutados dinámicamente.

### 3. Base de Datos y Esquemas
DuckDB se estructura en formato estrella, con tablas dimensionales (`dim_restaurants`, `dim_locations`, `dim_card_types`) y hechos (`fact_offers`).
- **Esquema:** `dags/sql/schema_v2.sql` - Define la estructura y puebla `dim_locations` y `dim_card_types`. Si necesitas que una comuna o mall empiece a hacer match, debes agregarlo acá.
- **Base de Datos:** `datitos_nam.duckdb` (en la raíz).
- **Ejecución Central:** `dags/run_v2.py`. Orquesta el schema, las referencias, y todos los transformadores en el orden correcto. Corre este archivo para recompilar todo.

## 🛠️ Herramientas de Trabajo y Debugging

Gran parte de nuestro día a día es buscar inconsistencias y arreglarlas en los Transformadores.
1. **DBeaver / DuckDB:** Conéctate a `datitos_nam.duckdb` como solo lectura para explorar. *Asegúrate de cerrar DBeaver o matar su proceso (PID) al correr `run_v2.py`, ya que bloquea el archivo duckdb exclusivimante.*
2. **Scripts en /tmp/:** Es común crear mini scripts temporales `debug_xxx.py` que hacen un SELECT a los `.json` o al `.duckdb` mediante la librería de Python `duckdb` para verificar qué está pasando en un paso específico del CTE (ej. *¿Por qué "Al Pesto" no hace JOIN con `dim_locations`? Ah, le faltaba la comuna "Chicureo" en `schema_v2.sql`*).

## 📌 Top Reglas de Negocio a Recordar

1. **Locaciones de Cadenas Globales (Antojos):** Si un restaurante ingresa al bucket de *Antojo*, *Online*, etc. (lista en `00_restaurants.sql`), siempre tendrá `location_id = 'all'`. Esto evita miles de puntos repetidos para "Doggis".
2. **Duplicados:** El motor colapsa ofertas con el mismo (Banco, Local, Descuento, Días, Tarjeta). 
3. **Regex de Exclusión:** Cuando un banco dice "Excluye Tarjetas Black", usamos Regex complejas en `<banco>.sql` para no asignar el `card_type_id` de esa u otra. 

¡Mucho éxito arreglando la Matrix de descuentos!
