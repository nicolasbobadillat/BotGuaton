DELETE FROM fact_offers WHERE bank_id = 'bce';
DELETE FROM fact_offers WHERE bank_id = 'bice';

INSERT INTO fact_offers (offer_id, restaurant_id, bank_id, card_type_id, location_id, valid_days, discount_pct, discount_cap, discount_cap_is_unlimited, conditions, valid_month, source_url, image_url, address, scraped_at, expiration_date)
WITH raw_bice AS (
    SELECT 
        *,
        -- Match 00_restaurants.sql extraction logic exactly
        CASE 
            WHEN regexp_extract(description, '\n[^\n]+\n([^\n]+)\n', 1) ILIKE '%HELADOS EL TALLER CHILE%' THEN 'El Taller'
            ELSE regexp_extract(description, '\n[^\n]+\n([^\n]+)\n', 1) 
        END as clean_title,
        -- Extraer la l�nea de encabezado de d�as (l�nea 4) para evitar leak de descripciones largas
        regexp_extract(description, '\n[^\n]+\n[^\n]+\n([^\n]+)\n', 1) as day_line
    FROM read_json_auto('{{JSON_BASE_PATH}}/bice.json')
),
exploded_bice AS (
    SELECT 
        *,
        unnest(
            CASE 
                WHEN day_line ILIKE '%todos los d_as%' OR day_line ILIKE '%lunes a domingo%' THEN ['Lunes', 'Martes', 'Mi�rcoles', 'Jueves', 'Viernes', 'S�bado', 'Domingo']
                WHEN day_line ILIKE '%lunes a viernes%' THEN ['Lunes', 'Martes', 'Mi�rcoles', 'Jueves', 'Viernes']
                WHEN day_line ILIKE '%lunes a s_bado%' OR day_line ILIKE '%lunes a sabado%' THEN ['Lunes', 'Martes', 'Mi�rcoles', 'Jueves', 'Viernes', 'S�bado']
                WHEN day_line ILIKE '%domingo a mi_rcoles%' OR day_line ILIKE '%domingo a miercoles%' THEN ['Domingo', 'Lunes', 'Martes', 'Mi�rcoles']
                WHEN day_line ILIKE '%domingo a jueves%' THEN ['Domingo', 'Lunes', 'Martes', 'Mi�rcoles', 'Jueves']
                WHEN day_line ILIKE '%mi_rcoles a s_bado%' OR day_line ILIKE '%miercoles a sabado%' THEN ['Mi�rcoles', 'Jueves', 'Viernes', 'S�bado']
                WHEN len(regexp_extract_all(lower(description), '(lunes|martes|mi[e�]rcoles|jueves|viernes|s[a�]bado|domingo)')) > 0 
                THEN apply(regexp_extract_all(lower(description), '(lunes|martes|mi[e�]rcoles|jueves|viernes|s[a�]bado|domingo)'), x -> CASE WHEN x = 'lunes' THEN 'Lunes' WHEN x = 'martes' THEN 'Martes' WHEN x IN ('miercoles', 'mi�rcoles') THEN 'Mi�rcoles' WHEN x = 'jueves' THEN 'Jueves' WHEN x = 'viernes' THEN 'Viernes' WHEN x IN ('sabado', 's�bado') THEN 'S�bado' WHEN x = 'domingo' THEN 'Domingo' ELSE x END)
                ELSE ['Consultar']
            END
        ) as unnested_day
    FROM raw_bice
),
processed_bice AS (
    SELECT 
        *,
        -- discount_cap: from JSON field with regex fallback
        CASE
            WHEN strip_accents(description) ILIKE '%sin tope%' 
                 OR strip_accents(description) ILIKE '%sin topes%' 
                 OR strip_accents(description) ILIKE '%sin tope maximo%' 
            THEN NULL
            ELSE CAST(NULLIF(regexp_replace(
                    regexp_extract(strip_accents(description), '(?i)(?:tope|hasta|maximo)[^$]*?\$([\d\.]+)', 1),
                    '\.', '', 'g'
                ), '') AS INTEGER)
        END as calculated_cap,
        CAST(NULLIF(regexp_extract(discount_text, '(\d+)', 1), '') AS INTEGER) as calculated_discount_pct,
        -- Robust parsing for expiration_date
        CASE 
            WHEN description IS NOT NULL AND description ILIKE '% de %' THEN
                    (
                        WITH date_parts AS (
                            SELECT 
                                regexp_extract(lower(description), '(\d{1,2})\s+de\s+([a-z]+)\s+(?:de\s+)?(\d{4})', 1) as d,
                                regexp_extract(lower(description), '(\d{1,2})\s+de\s+([a-z]+)\s+(?:de\s+)?(\d{4})', 2) as m_name,
                                regexp_extract(lower(description), '(\d{1,2})\s+de\s+([a-z]+)\s+(?:de\s+)?(\d{4})', 3) as y
                        )
                        SELECT TRY_CAST(y || '-' || 
                            CASE 
                                WHEN m_name LIKE 'enero%' THEN '01'
                                WHEN m_name LIKE 'febrero%' THEN '02'
                                WHEN m_name LIKE 'marzo%' THEN '03'
                                WHEN m_name LIKE 'abril%' THEN '04'
                                WHEN m_name LIKE 'mayo%' THEN '05'
                                WHEN m_name LIKE 'junio%' THEN '06'
                                WHEN m_name LIKE 'julio%' THEN '07'
                                WHEN m_name LIKE 'agosto%' THEN '08'
                                WHEN m_name LIKE 'septiembre%' THEN '09'
                                WHEN m_name LIKE 'octubre%' THEN '10'
                                WHEN m_name LIKE 'noviembre%' THEN '11'
                                WHEN m_name LIKE 'diciembre%' THEN '12'
                                ELSE '01'
                            END || '-' || LPAD(d, 2, '0') AS DATE)
                        FROM date_parts
                        WHERE y != '' AND d != ''
                    )
                WHEN description IS NOT NULL AND description ILIKE '%/%/%' THEN
                    TRY_CAST(
                        regexp_extract(description, '(\d{2}/\d{2}/\d{4})', 1) AS DATE
                    )
                ELSE NULL
            END as calculated_expiration_date
    FROM exploded_bice
)
SELECT 
    md5(concat('bice', clean_title, discount_text, dl.location_id, unnested_day)) as offer_id,
    md5(upper(trim(regexp_replace(strip_accents(regexp_replace(regexp_replace(regexp_replace(clean_title, '^Disfruta de tu beneficio en ', '', 'i'), '\s*-\s*Descuento.*| - \d+% dcto', '', 'i'), '^Restaurant(e)?\s+|\s+Restaurant(e)?$|^Pasteleria\s+|\s+Pasteleria$|^Heladeria\s+|\s+Heladeria$', '', 'i')), '[^a-zA-Z0-9]', '', 'g')))) as restaurant_id,
    'bice' as bank_id,
    CASE 
        WHEN description ILIKE '%Signature%' OR description ILIKE '%Infinite%' THEN 'bice_premium'
        WHEN description ILIKE '%Limitless%' THEN 'bice_limitless'
        WHEN description ILIKE '%Banca Joven%' OR description ILIKE '%GO BICE%' THEN 'bice_banca_joven'
        ELSE 'bice_credito'
    END as card_type_id,
    dl.location_id,
    CASE WHEN unnested_day IN ('Consultar', 'Consultar condiciones') THEN NULL ELSE unnested_day END as valid_days,
    calculated_discount_pct as discount_pct,
    calculated_cap as discount_cap,
    CASE
        WHEN calculated_cap IS NOT NULL THEN FALSE -- Numeric cap found -> NOT unlimited
        WHEN strip_accents(description) ILIKE '%sin tope%' OR strip_accents(description) ILIKE '%sin topes%' OR strip_accents(description) ILIKE '%sin tope maximo%' THEN TRUE
        ELSE NULL
    END as discount_cap_is_unlimited,
    description as conditions,
    strftime(current_timestamp, '%B') as valid_month,
    url as source_url,
    image_url,
    dl.commune as address,
    scraped_at,
    calculated_expiration_date as expiration_date
FROM processed_bice
JOIN dim_locations dl ON (
    processed_bice.location ILIKE '%' || dl.commune || '%' OR 
    (processed_bice.description ILIKE '%' || dl.commune || '%' AND length(dl.commune) > 5) OR
    (strip_accents(processed_bice.description) ILIKE '%' || strip_accents(dl.commune) || '%' AND length(dl.commune) > 5)
)
WHERE clean_title IS NOT NULL AND clean_title != ''
  AND clean_title NOT ILIKE '%Riva A�a�%' AND clean_title NOT ILIKE '%RESTOFANS%' AND clean_title NOT ILIKE '%PedidosYa%' AND clean_title NOT ILIKE '%La Pesca de los Mekis%' AND clean_title NOT ILIKE '%Sacacorchos%'
  AND calculated_discount_pct > 0
QUALIFY row_number() OVER (PARTITION BY clean_title, discount_pct, dl.location_id, unnested_day, card_type_id ORDER BY scraped_at DESC) = 1
ON CONFLICT (offer_id) DO NOTHING;



