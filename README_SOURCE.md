# Datitos Nam: Encuentra tu Descuento Bacan 🍔💳

¡Hola! Si acabas de llegar a este proyecto y no entiendes nada de bases de datos o programación, estás en el lugar correcto. Esta es la guía rápida y sin palabras raras para entender qué hacemos aquí.

---

## 🧐 1. ¿Qué problema resolvemos?

Imagina esto: Tienes una tarjeta del Banco Santander, otra del BCI y una de Falabella. Sabes que existen descuentos en cientos de restaurantes... pero para encontrarlos tienes que entrar a 3 páginas web distintas, pelear con buscadores malos, leer la "letra chica" en PDF, y para cuando encuentras que había un 40% en hamburguesas, el restaurante ya cerró.

**Datitos Nam soluciona esto.**

Construimos un grupo de "robots" (scrapers) que religiosamente todos los días van solos a las páginas web de **11 bancos distintos** en Chile, leen todas las condiciones, porcentajes y locales, y meten toda esa información limpia y masticada en un solo gran archivo maestro (nuestra base de datos). 

Gracias a esto, podemos responder a la velocidad de un click preguntas como:
* *"¿Dónde como barato hoy en la comuna de Las Condes?"*
* *"¿Quién tiene oferta en Papa John's los martes?"*

---

## ⚙️ 2. ¿Cómo funciona la fábrica? (El embudo de datos)

Todo el proyecto se resume en una cadena de ensamblaje de 4 pasos:

1. **Los Recolectores (Scrapers en Python):** Tenemos un script programado para cada banco. Como si fueran humanos súper rápidos, abren Chrome invisiblemente, hacen click en "Ver más promociones" y copian el título, descuento y días. Esta basura cruda la tiran en archivos de texto (`.json`).
2. **El Diccionario Traductor (ETL - Transformers):** Cada banco escribe las cosas a su pinta (unos escriben "Macdonald", otros "McDonald's", otros "Mc Donalds"). Nuestro ordenador lee los `.json`, corrige la ortografía, limpia la gramática, estandariza los nombres y detecta si es un delivery ("Online"), un antojo al paso ("Antojo") o restaurante propiamente tal.
3. **El Eliminador de Duplicados:** A veces una cadena (como Burger King) tiene 40 locales y el banco las sube como 40 promociones separadas. La máquina detecta esto y las fusiona en una sola "mega oferta" que dice *Aplica en Todas las Sucursales* para no ensuciarnos la visibilidad.
4. **El Estante Final (DuckDB):** Toda esta masa limpia y brillante se empaqueta en un archivo ultraligero llamado `datitos_nam.duckdb`. Este archivo es la Biblia de nuestro proyecto, el producto final consumible.

---

## 🗺️ 3. El Mapa del Tesoro (Para Programadores o Entusiastas)

Si necesitas meter mano a los archivos, aquí está la brújula para que no te pierdas en las carpetas. Todo ocurre dentro de la carpeta `/dags/`:

### El Corazón Operativo:
- 🚀 `dags/run_v2.py`: **El Botón Rojo.** Si haces correr este archivo, tomas todos los JSON crudos y armas la base de datos limpia de cero. Es el director de la orquesta.
- 🗄️ `datitos_nam.duckdb`: El producto final empaquetado y listo para ser consultado.

### Los Archivos de Extracción (Scrapers):
- 🕷️ `dags/*_scraper.py` (ej: `bci_scraper.py`, `itau_scraper.py`): Los archivos independientes que extraen info de cada banco.
- 📦 `dags/json/`: La bandeja de entrada. Aquí aterrizan los archivos feos e intermedios recién escrapeados.

### Las Reglas del Juego (SQL Transformers):
Dentro de la carpeta `dags/sql/transformers/` viven los filtros de limpieza:
- 📖 `00_restaurants.sql`: El gran diccionario. Aquí le enseñamos al programa a catalogar. *Ej: "Si el local se llama 'PedidosYa', bórralo" o "Si se llama 'Fundo Sofruco', asúmelo como compra Online".*
- 🩹 `zz_02_overrides.sql`: La "Libreta del Jefe". Los bancos son mañosos y se equivocan, publican ofertas en fechas que no son. Aquí ponemos reglas a la fuerza (ej: *Marley Coffee en Itaú SIEMPRE es los jueves, sin importar lo que diga su web*).
- 🔄 `<banco>.sql`: Archivos individuales para traducir las rarezas de cada banco específico antes de unificar.

---

## 🧠 4. Reglas Intocables del Proyecto

Si vas a agregar cosas nuevas o arreglar algo malo de los bancos, recuerda que Nam opera bajo estos mandamientos:

1. **Los Antojos no tienen calle:** Locales como "Doggis" o "Tarragona" son *"Antojos"*. Nunca les asignamos una dirección o ciudad fija. Su ubicación por defecto es siempre `location_id = 'all'` (Todas las sucursales). 
2. **Cero Tolerancia a Duplicados Identicos:** Si una oferta tiene exactamente el mismo banco, porcentaje, día y local, la base de datos fusionará y eliminará la copias.
3. **El Override manda:** Lo que se ponga en el archivo `zz_02_overrides.sql` es la última palabra. 

---

### ¿Quieres encender la fábrica?
Si ya instalaste los prerrequisitos básicos en tu computador y quieres reconstruir toda la base de datos para ver la data fresquita:

Abre tu terminal y corre:
```bash
python dags/run_v2.py
```
*(Si no tira errores en rojo, felicitaciones, tienes en tus manos todos los descuentos de comida de Chile actualizados al día de hoy).*

---

## 🚀 Actualizaciones Recientes (Marzo 2026)

- **Consolidación de Itaú, Categorías y Cadenas (Marzo 02)**:
  - **Itaú Cards Logic**: Refactor completo del algoritmo de tarjetas para diferenciar "Legend" vs "Black" correctamente. Implementación de `GROUP BY` y regex contextual para evitar que exclusiones (ej. "excluye black") filtren mal la tarjeta. Asignación correcta a `itau_combined`.
  - **Corrección Cadenas y Categorías**: Agregado `LARRS` como Helado. Just Burger y Burger King forzados a `location_id: all` en Santander y Cencosud respectivamente.
  - **Precisión Geográfica Extrema**: Se mejoró el mapeo a nivel comuna. "La Pasta de la Nonna" mapea exactamente a Concepción y San Pedro de la Paz gracias a excepciones controladas pre-fallback. "Al Pesto" ahora se asocia a *Chicureo* tras agregarlo a las locaciones oficiales en `schema_v2.sql`.
  - **Filtros Globales**: Brands the ropa excluidas (Penguin, Perry Ellis, Trial) en el nivel global `00_restaurants.sql`.
  - **Overrides Específicos**: Dominga Bistro eliminada de Banco de Chile al caducar su descuento en marzo.

## 🚀 Actualizaciones Anteriores (Febrero 2026)

Para mantener la calidad de la base de datos, hemos aplicado los siguientes parches de limpieza:

- **Evolución Itaú y Precisión Geográfica (Febrero 28)**:
  - **Itaú Discount Caps**: Extracción masiva de topes de descuento (ej: **$50.000** en **La Maestranza**, **$15.000** en **Marley Coffee**) mediante búsqueda en múltiples campos y regex mejorada.
  - **Fix de Sombreado**: Corregido bug técnico en el transformador de Itaú que ocultaba información de los topes durante la limpieza.
  - **Mapeo de Locaciones**: **Prima Bar** (Alonso de Córdova) ahora mapea correctamente a **Vitacura**.
  - **Preservación de Topes**: Ajuste en los overrides globales para no perder los topes de descuento al expandir sucursales de cadenas.
- **Correcciones de Días y Categorías (Febrero 24)**:
  - **Categorización Global**: Starbucks y Mamut estandarizados como *Antojo*.
  - **Banco Estado**: Restaurados los antojos de los martes (Juan Maestro, Doggis, Dominos Pizza, Barrio Chicken) solucionando bug de filtrado por días.
  - **Ajustes Manuales**: Marley Coffee (Jueves), Leonidas (Miércoles) y Just Burger (Martes) corregidos para compensar datos incompletos o erróneos en portales de Itaú y Ripley.
- **Detección Automática Mejorada**:
  - **Carl's Jr**: Ahora clasificado siempre como *Antojo* y *Cadena*.
  - **CAV** y **La Vinoteca Tienda**: Clasificados como *Online* para evitar confusiones con restaurantes físicos.
  - **Duran Bar de Carne**: Validado como categoría *Restaurante*.
- **Correcciones Geográficas (Overrides)**:
  - **Rocoto**: Movido de Valdivia a **Ñuñoa**.
  - **Muu Grill**: Ubicación específica en **Isidora 3000**.
  - **Consolidación de Malls**: Corrección masiva de locales en *Costanera Center, Vitacura, Las Condes y Lo Barnechea*.
- **Ajustes de Calendario**:
  - **Vapiano**: Expansión manual de días válidos (Lu, Mi, Vi, Sá, Do) para asegurar visibilidad total.
- **Limpieza de Ruido**:
  - **SkinFit**: Eliminación permanente automática de la base de datos (rubro no gastronómico).
- **Aportes de Usuario**:
  - **La Santoria**: Agregada oferta manual de Banco Itaú (40%, Providencia, Dom-Mié).
  - **Badass (Parque Arauco)**: Insertada manualmente oferta de **40% CMR** válida **Todos los días**.
- **Manual Fixes & Overrides**:
  - **Lorenzo Lounge-Bar**: Recategorized to **Restaurante** (definitive fix in the root transformer).
  - **Leonidas (Itaú)**: Forced to **Miércoles**.
  - **Marley Coffee (Itaú)**: Forced to **Jueves**.
  - **Philia (Ripley)**: Corrected to **Jueves a Domingo**.
- **Expansion of Antojo & Chains**:
  - Added **Barrio Chicken**, **La Fête**, **Larrs**, **China 365**, **Buffet Express** and **App McDonald's** to the global list of fast-food chains.
- **Robustez Extrema en Falabella (V 2.1.0)**:
  - **Generación de Slugs Inteligente**: El scraper ahora prueba múltiples variantes de URL para cada nombre, ignorando centros comerciales o corrigiendo errores de redacción del banco.
  - **Manejo de Ampersands (`&`)**: Fix para locales como "Sushi & Burger Home" probando variantes con "y" y con eliminación del símbolo.
  - **Forzado de Cabrera**: Implementado override directo para "La Cabrera al Paso", garantizando su captura al 100%.

---

## 📜 5. Bitácoras de Cambio (Changelogs)

Para ver el detalle técnico de qué hemos arreglado en cada robot, revisa los archivos individuales en la carpeta `dags/`:

- [Changelog Falabella](file:///C:/Users/Nico/Documents/py/datitos_nam/dags/CHANGELOG_falabella.md): Arreglo de selectores, Phase 2 fuzzy y Phase 3 forced visits.
- [Changelog Banco de Chile](file:///C:/Users/Nico/Documents/py/datitos_nam/dags/CHANGELOG_bancochile.md): Arreglo del "Bug de los Domingos" y extracción de porcentajes.
- [Changelog BCI & Itaú](file:///C:/Users/Nico/Documents/py/datitos_nam/dags/CHANGELOG_bci_itau.md): Mapeo autónomo de locaciones y tipos de tarjeta.
- [Changelog Scotiabank & Cencosud](file:///C:/Users/Nico/Documents/py/datitos_nam/dags/CHANGELOG_scotia_cenco.md): Extracción múltiple de locales.
- [Changelog Overrides](file:///C:/Users/Nico/Documents/py/datitos_nam/dags/CHANGELOG_overrides.md): Correcciones manuales de días, categorías y antojos.
