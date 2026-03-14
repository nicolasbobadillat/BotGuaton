-- Limpiar basura de ejecuciones previas o sources corruptos (primero fact_offers por FK)
DELETE FROM fact_offers 
WHERE restaurant_id IN (
    SELECT restaurant_id FROM dim_restaurants 
    WHERE name ILIKE '%RESTOFANS%' 
       OR name ILIKE '%PedidosYa%' 
       OR name ILIKE '%La Pesca de los Mekis%' 
       OR name ILIKE '%Sacacorchos%'
       OR name ILIKE '%la Ruta del Sabor%'
       OR name ILIKE '%Jackie Guiloff%'
       OR name ILIKE '%Penguin%'
       OR name ILIKE '%Perry Ellis%'
       OR name ILIKE '%Trial%'
);

DELETE FROM dim_restaurants 
WHERE name ILIKE '%RESTOFANS%' 
   OR name ILIKE '%PedidosYa%' 
   OR name ILIKE '%La Pesca de los Mekis%' 
   OR name ILIKE '%Sacacorchos%'
   OR name ILIKE '%la Ruta del Sabor%'
   OR name ILIKE '%Jackie Guiloff%'
   OR name ILIKE '%Penguin%'
   OR name ILIKE '%Perry Ellis%'
   OR name ILIKE '%Trial%';

-- Crear vista temporal con nombres normalizados de todos los sources
CREATE OR REPLACE TEMP VIEW all_titles AS
SELECT DISTINCT 
    -- Limpiar el nombre: quitar "Disfruta de tu beneficio en" y " - Descuento"
    COALESCE(NULLIF(regexp_replace(
        regexp_replace(
            regexp_replace(title, '^Disfruta de tu beneficio en ', '', 'i'),
            '\s*-\s*Descuento.*| - \d+% dcto', '', 'i'
        ),
        '^Restaurant(e)?\s+|\s+Restaurant(e)?$|^Pasteleria\s+|\s+Pasteleria$|^Heladeria\s+|\s+Heladeria$', '', 'i'
    ), ''), title) as title,
    upper(trim(regexp_replace(strip_accents(regexp_replace(regexp_replace(regexp_replace(title, '^Disfruta de tu beneficio en ', '', 'i'), '\s*-\s*Descuento.*| - \d+% dcto', '', 'i'), '^Restaurant(e)?\s+|\s+Restaurant(e)?$|^Pasteleria\s+|\s+Pasteleria$|^Heladeria\s+|\s+Heladeria$', '', 'i')), '[^a-zA-Z0-9]', '', 'g'))) as normalized_name,
    md5(upper(trim(regexp_replace(strip_accents(regexp_replace(regexp_replace(regexp_replace(title, '^Disfruta de tu beneficio en ', '', 'i'), '\s*-\s*Descuento.*| - \d+% dcto', '', 'i'), '^Restaurant(e)?\s+|\s+Restaurant(e)?$|^Pasteleria\s+|\s+Pasteleria$|^Heladeria\s+|\s+Heladeria$', '', 'i')), '[^a-zA-Z0-9]', '', 'g')))) as restaurant_id,
    -- Detectar si es cadena
    CASE 
        WHEN regexp_matches(upper(strip_accents(title)), '(MCDONALD|KFC|JUAN MAESTRO|CHINA WOK|WENDY|DOMINO|DUNKIN|SUSHI BLUES|VOLKA|YOGEN FRUZ|MELT|BURGER KING|DOGGIS|PAPA JOHN|PIZZA HUT|SUBWAY|TARRAGONA|LITTLE CAESAR|STARBUCKS|JUAN VALDEZ|LOVDO|SAVORY|BOZZO|FORK|PEDRO JUAN|BOOST|POLLO STOP|FUKU MOCHI|BARRIO CHICKEN|EL TALLER|MR PRETZELS|CHICKEN FACTORY|CHINA365|CHINA 365|JUST BURGER|TOMMY BEANS|PIZZA PIZZA|TACO BELL|VELVET BAKERY|BURGERBEEF|COPPELIA|MAMUT|CAFE FIKA|PHILIA|MARLEY COFFEE|BLACK\s*DROP|BLACK CHICKEN|CASTANO|HELLO BEER|HELLO WINE|LEONIDAS|TERE YOUNG|PIZKA|CAVA MORANDE|THE WILD FOODS|MARIBERICO|CAV|CARL.?S JR|LA FETE|HELADERIA LARRS|LARRS|BUFFET EXPRESS|FACTORY NINE|HAPPY BOX|DAI!|BIANCO\s*LATTE|BIANCOLATTE)') 
             AND title NOT ILIKE '%Cafeteria%Fete%'
        THEN TRUE 
        ELSE FALSE 
    END as is_chain,

    CASE 
        WHEN regexp_matches(upper(strip_accents(title)), '(MAGNOLIA BAR|MAGOLIA BAR|DURAN BAR DE CARNES|LORENZO LOUNGE)') THEN 'Restaurante'
        WHEN regexp_matches(upper(strip_accents(title)), '(AQUALITY|ECOOK|PARAMICAFE|DESCORCHA|YOYO TEA|YOYOTEA|FUNDO SOFRUCO|CAVA MORANDE|PIZKA|HELLO BEER|HELLOBEER|SACACORCHOS|HELLO WINE|HELLOWINE|CAV|LA VINOTECA TIENDA|CARNES PREMIUM|BOKA SUSHI)') THEN 'Online'
        WHEN regexp_matches(upper(strip_accents(title)), '(MAMUT|COPPELIA|BURGER KING|PAPA JOHNS|PHILIA|MARLEY COFFEE|JUST BURGER|BARRIO CHICKEN|CAFE FIKA|CASTANO|BLACK DROP|BLACK\s*DROP|BLACK CHICKEN|TERE YOUNG|LA CAV|CARL.?S JR|LA FETE|LARRS|BUFFET EXPRESS|FACTORY NINE|AM DULCERIA)') THEN 'Antojo'
        WHEN regexp_matches(upper(strip_accents(title)), '(MCDONALD|KFC|JUAN MAESTRO|CHINA WOK|WENDY|DOMINO|DUNKIN|SUSHI BLUES|VOLKA|YOGEN FRUZ|MELT|BURGER KING|DOGGIS|PAPA JOHN|PIZZA HUT|SUBWAY|TARRAGONA|LITTLE CAESAR|LE VICE|BOZZO|PEDRO JUAN|JUAN VALDEZ|BOOST|POLLO STOP|FUKU MOCHI|BARRIO CHICKEN|EL TALLER|MR PRETZELS|CHICKEN FACTORY|LOVDO|CHINA365|CHINA 365|JUST BURGER|TOMMY BEANS|PIZZA PIZZA|TACO BELL|VELVET BAKERY|BURGERBEEF|COPPELIA|MAMUT|CAFE FIKA|PHILIA|MARLEY COFFEE|BLACK\s*DROP|BLACK CHICKEN|CASTANO|LEONIDAS|TERE YOUNG|THE WILD FOODS|MARIBERICO|CAV|CARL.?S JR|LA FETE|HELADERIA LARRS|LARRS|BUFFET EXPRESS|FACTORY NINE|HAPPY BOX|DAI!|STARBUCKS|PIWEN|BIANCO\s*LATTE|BIANCOLATTE|AM DULCERIA|FORK)') 
             AND title NOT ILIKE '%Cafeteria%Fete%'
        THEN 'Antojo'
        WHEN category IS NOT NULL THEN category
        ELSE 'Restaurante'
    END as category

FROM (
    -- Subquery with all sources
    SELECT * FROM (
        SELECT json_extract_string(row_to_json(t), '$.title') as title, NULL as category FROM read_json_auto('{{JSON_BASE_PATH}}/bci.json', columns={'title': 'VARCHAR'}) t
        UNION
        SELECT CASE WHEN json_extract_string(row_to_json(t), '$.title') = '1213' THEN 'Doce Trece' ELSE json_extract_string(row_to_json(t), '$.title') END as title, NULL as category FROM read_json_auto('{{JSON_BASE_PATH}}/santander.json', columns={'title': 'VARCHAR'}) t WHERE json_extract_string(row_to_json(t), '$.title') IS NOT NULL
        UNION
        SELECT json_extract_string(row_to_json(t), '$.title') as title, NULL as category FROM read_json_auto('{{JSON_BASE_PATH}}/bancochile.json', columns={'title': 'VARCHAR'}) t
        UNION
        SELECT json_extract_string(row_to_json(t), '$.title') as title, CAST(json_extract_string(row_to_json(t), '$.category') AS VARCHAR) as category FROM read_json_auto('{{JSON_BASE_PATH}}/scotiabank.json', columns={'title': 'VARCHAR', 'category': 'VARCHAR'}) t
        UNION
        -- Falabella: excluir paquetes "Especial Verano/Invierno"
        SELECT json_extract_string(row_to_json(t), '$.title') as title, CAST(json_extract_string(row_to_json(t), '$.category') AS VARCHAR) as category FROM read_json_auto('{{JSON_BASE_PATH}}/falabella.json', columns={'title': 'VARCHAR', 'category': 'VARCHAR'}) t
        WHERE json_extract_string(row_to_json(t), '$.title') NOT ILIKE '%Especial Verano%' AND json_extract_string(row_to_json(t), '$.title') NOT ILIKE '%Especial Invierno%' AND json_extract_string(row_to_json(t), '$.title') NOT ILIKE '%Experiencia Verano%'
        UNION
        -- Ripley (Use category from JSON: Restaurante vs Antojo)
        SELECT json_extract_string(row_to_json(t), '$.title') as title, CAST(json_extract_string(row_to_json(t), '$.category') AS VARCHAR) as category FROM read_json_auto('{{JSON_BASE_PATH}}/ripley.json', columns={'title': 'VARCHAR', 'category': 'VARCHAR'}) t
        UNION
        -- BancoEstado
        SELECT json_extract_string(row_to_json(t), '$.title') as title, NULL as category FROM read_json_auto('{{JSON_BASE_PATH}}/bancoestado.json', columns={'title': 'VARCHAR'}) t
        UNION
        -- Cencosud
        SELECT json_extract_string(row_to_json(t), '$.title') as title, NULL as category FROM read_json_auto('{{JSON_BASE_PATH}}/cencosud.json', columns={'title': 'VARCHAR'}) t
        UNION
        -- BICE: extraer nombre de la línea 3 de description, o usar 'title' si la regex falla. Normalización.
        SELECT 
           CASE 
               WHEN regexp_extract(json_extract_string(row_to_json(t), '$.description'), '\n[^\n]+\n([^\n]+)\n', 1) ILIKE '%HELADOS EL TALLER CHILE%' THEN 'El Taller'
               ELSE regexp_extract(json_extract_string(row_to_json(t), '$.description'), '\n[^\n]+\n([^\n]+)\n', 1) 
           END as title, CAST(json_extract_string(row_to_json(t), '$.category') AS VARCHAR) as category
        FROM read_json_auto('{{JSON_BASE_PATH}}/bice.json', columns={'title': 'VARCHAR', 'description': 'VARCHAR', 'category': 'VARCHAR'}) t
        WHERE lower(json_extract_string(row_to_json(t), '$.title')) != 'visa'
          AND regexp_extract(json_extract_string(row_to_json(t), '$.description'), '\n[^\n]+\n([^\n]+)\n', 1) IS NOT NULL
          AND regexp_extract(json_extract_string(row_to_json(t), '$.description'), '\n[^\n]+\n([^\n]+)\n', 1) != ''
        UNION
        -- Itau (Use json parsing to avoid schema errors if category is missing in old scrapes)
        SELECT title, TRY_CAST(json_extract_string(row_to_json(t), '$.category') AS VARCHAR) as category 
        FROM read_json_auto('{{JSON_BASE_PATH}}/itau.json', columns={'title': 'VARCHAR', 'category': 'VARCHAR'}) t
        UNION
        -- Internacional
        -- Internacional: Limpiar sufijos de fecha/día para que el restaurant_id sea consistente
        SELECT 
            regexp_replace(json_extract_string(row_to_json(t), '$.title'), '\s*(lunes|nov\.|enero|diciembre|dic|noviembre|sabados|martes|miercoles).*$', '', 'i') as title, NULL as category
        FROM read_json_auto('{{JSON_BASE_PATH}}/internacional.json', columns={'title': 'VARCHAR'}) t
        WHERE json_extract_string(row_to_json(t), '$.title') NOT ILIKE '%Sabados Domani%'
          AND json_extract_string(row_to_json(t), '$.title') NOT ILIKE '%Pesca de los Mekis%'
          AND json_extract_string(row_to_json(t), '$.title') NOT ILIKE '%RESTOFANS%'
          AND json_extract_string(row_to_json(t), '$.title') NOT ILIKE '%PedidosYa%'
          AND json_extract_string(row_to_json(t), '$.title') NOT ILIKE '%Penguin%'
          AND json_extract_string(row_to_json(t), '$.title') NOT ILIKE '%Perry Ellis%'
          AND json_extract_string(row_to_json(t), '$.title') NOT ILIKE '%Trial%'
    ) all_sources
    WHERE title NOT ILIKE '%RESTOFANS%'
      AND title NOT ILIKE '%PedidosYa%'
      AND title NOT ILIKE '%Sacacorchos%'
      AND title NOT ILIKE '%La Pesca de los Mekis%'
      AND title NOT ILIKE '%Penguin%'
      AND title NOT ILIKE '%Perry Ellis%'
      AND title NOT ILIKE '%Trial%'
);

INSERT INTO dim_restaurants (restaurant_id, name, normalized_name, category, is_chain, cuisine_type)
SELECT 
    restaurant_id,
    first(title) as name,
    normalized_name,
    first(category) as category,
    bool_or(is_chain) as is_chain,
    -- Clasificación de Cuisine Type para Antojos
    CASE 
        WHEN first(category) != 'Antojo' THEN NULL
        WHEN regexp_matches(upper(strip_accents(first(title))), '(MCDONALD|BURGER KING|WENDY|BURGERBEEF|CARLS JR|JUST BURGER|WENDYS)') THEN 'Burger'
        WHEN regexp_matches(upper(strip_accents(first(title))), '(PAPA JOHN|DOMINO''S|PIZZA HUT|MELT|LOVDO|LITTLE CAESAR|PIZZA PIZZA|SICILY PIZZERIA|HAPPY BOX|HAPPY BOX PIZZA)') THEN 'Pizza'
        -- Dominó (con tilde) -> Completos. Domino (sin nada o con 's) -> Pizza
        WHEN first(title) ILIKE '%Dominó%' THEN 'Completos'
        WHEN first(title) ILIKE '%Domino%' THEN 'Pizza' -- Fallback 
        WHEN regexp_matches(upper(strip_accents(first(title))), '(JUAN MAESTRO|SUBWAY|FUENTE SUIZA|LA SANGUCHERA del BARRIO|DANES|PEDRO JUAN)') THEN 'Sandwich'
        WHEN regexp_matches(upper(strip_accents(first(title))), '(DOGGIS)') THEN 'Completos'
        WHEN regexp_matches(upper(strip_accents(first(title))), '(SUSHI BLUES|CRUNCHY ROLLS|SAKURA|NIKARA SUSHI)') THEN 'Sushi'
        WHEN regexp_matches(upper(strip_accents(first(title))), '(KFC|POLLO STOP|BARRIO CHICKEN|BARRIO CHICK''EN|CHICKEN FACTORY|BLACK CHICKEN|TARRAGONA)') THEN 'Pollo'
        WHEN regexp_matches(upper(strip_accents(first(title))), '(STARBUCKS|JUAN VALDEZ|MARLEY COFFEE|BLACK\s*DROP|CAFE FIKA|PHILIA)') THEN 'Cafe'
        WHEN regexp_matches(upper(strip_accents(first(title))), '(DUNKIN)') THEN 'Donas'
        WHEN regexp_matches(upper(strip_accents(first(title))), '(VELVET)') THEN 'Galleta'
        WHEN regexp_matches(upper(strip_accents(first(title))), '(MR PRETZEL|MR\. PRETZELS)') THEN 'Pretzel'
        WHEN regexp_matches(upper(strip_accents(first(title))), '(COPPELIA|SIENNA BAKERY|AMELIE|DULCERIA|DULCES ISSA|FRUTILLAR|PASTELERIA|BARQUILLERIA|FACTORY NINE|TERE YOUNG)') THEN 'Pasteleria'
        WHEN regexp_matches(upper(strip_accents(first(title))), '(LA FETE|LEONIDAS|BOZZO|LE VICE|CAKAO|VARSOVIENNE|CARAMEL)') THEN 'Chocolate'
        WHEN regexp_matches(upper(strip_accents(first(title))), '(CASTANO)') THEN 'Panaderia'
        WHEN regexp_matches(upper(strip_accents(first(title))), '(YOGEN FRUZ|SAVORY|HELADERIA LARRS|LARRS|EL TALLER|FUKU MOCHI|BIANCO\s*LATTE|BIANCOLATTE|POGA HELADERIA|OGGI GELATO|DILUSSO|PATAGONIA SCHOKOLAND|CIAO AMORE|DAI!)') THEN 'Helado'
        WHEN regexp_matches(upper(strip_accents(first(title))), '(BOOST)') THEN 'Jugo'
        WHEN regexp_matches(upper(strip_accents(first(title))), '(CHINA WOK|CHINA 365|CHINA365|TAKE A WOK|PF CHANG|P\.F\. CHANG|UDON)') THEN 'Asiática/China'
        WHEN regexp_matches(upper(strip_accents(first(title))), '(TACO BELL|TOMMY BEANS|JALISCO|EKEKO)') THEN 'Mexicana'
        WHEN regexp_matches(upper(strip_accents(first(title))), '(MAMUT|BUFFET EXPRESS|FORK|MUU GRILL|TANTA|BARRA CHALACA|KECHUA|SABOR Y AROMA)') THEN 'General/Grill'
        WHEN regexp_matches(upper(strip_accents(first(title))), '(PIWEN)') THEN 'Frutos Secos'
        ELSE NULL
    END as cuisine_type
FROM all_titles
GROUP BY restaurant_id, normalized_name
    ON CONFLICT (restaurant_id) DO UPDATE SET
        is_chain = EXCLUDED.is_chain,
        category = EXCLUDED.category,
        cuisine_type = EXCLUDED.cuisine_type;



