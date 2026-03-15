# Datitos Nam: BotGuaton

Este proyecto es el motor de datos que alimenta a la cuenta de Instagram **@BotGuaton**, un sistema automatizado para encontrar descuentos y promociones en restaurantes y comida en Chile.
https://www.instagram.com/botguaton/
---

## 1. El problema

Esto nace de una necesidad real, con amigos nos gusta salir a comer pero siempre sale caro, entonces siempre era tedioso buscar a qué local ir página por página. Por esto nació mi idea de centralizar los datos.

### ¿Por qué Instagram y no una app?
Porque Instagram es más rápido de revisar y la difusión es mucho más fácil. Actualmente, el producto final son las **láminas** (imágenes con el resumen diario). 

A futuro, la idea es que **BotGuaton** te responda automáticamente si le preguntas algo como *"descuento Falabella la Reina"* y te dé las opciones, pero aún no empiezo a trabajar en eso. En esta primera instancia, lo fundamental para mí es **validar y tener la data lo más limpia posible**, ya que un error puede significar que un usuario se equivoque y pague de más.

---

## 2. El Proceso (El embudo de datos)

Todo el proyecto se resume en una cadena de 4 pasos:

1. **Los Recolectores (Scrapers en Python):** Tengo un script programado para cada banco. Abren Chrome invisiblemente, hacen click en "Ver más promociones" y copian el título, descuento y días. Esta info cruda la tiran en archivos de texto (`.json`).
2. **El Diccionario Traductor (ETL - Transformers):** Cada banco escribe las cosas a su pinta (unos escriben "Macdonald", otros "McDonald's"). El ELT lee los `.json`, corrige la ortografía, limpia la gramática, estandariza los nombres y detecta si un antojo al paso o restaurante.
3. **El Eliminador de Duplicados:** A veces una cadena tiene 40 locales y el banco las sube como 40 promociones separadas. La máquina detecta esto y las fusiona en una sola oferta que dice *Aplica en Todas las Sucursales* para no ensuciarnos la visibilidad.
4. **DuckDB:** Toda esta masa limpia se empaqueta en un archivo ultraligero  `datitos_nam.duckdb`. Esta base de datos es el núcleo del proyecto y el producto final consumible.

---

## 3. Mejoras y Auditoría de Datos

Para asegurar que BotGuaton publique información 100% confiable antes de que llegue a Instagram, agregué estas mejoras técnicas:
*   **Motor de Diferencias Diarias**: Compara automáticamente la base de datos de hoy contra la de ayer para detectar de forma exacta qué ofertas son nuevas, cuáles se eliminaron y cuáles cambiaron de condiciones (porcentaje o días).
*   **Visualización de Control**: Una interfaz en Streamlit para auditar visualmente estos cambios y alertar sobre datos extraños (por ejemplo, si un descuento dice ser mayor al 50%).

---

## 4. Reglas Intocables del Proyecto

1. **Los Antojos no tienen calle**: Locales como "Doggis" o "Tarragona" son *"Antojos"*. Nunca les asignamos una dirección o ciudad fija. Su ubicación por defecto es siempre `location_id = 'all'` (Todas las sucursales). 
2. **Cero Tolerancia a Duplicados Identicos**: Si una oferta tiene exactamente el mismo banco, porcentaje, día y local, la base de datos fusionará y eliminará la copias.
3. **El Override manda**: Lo que se ponga en el archivo `zz_02_overrides.sql` es la última palabra. 

---

## 🚀 Cómo instalar

1. **Desplegar Contenedores**:
   ```bash
   docker-compose up -d
   ```
2. **Ajustar Rutas** (si usas rutas absolutas en entornos aislados):
   ```powershell
   powershell scripts/patch_sql_paths.ps1
   ```
3. **Consultar**:
   Una vez que Airflow procesa el hilo conductor, puedes correr la página de Streamlit para revisar el catálogo de forma visual.

