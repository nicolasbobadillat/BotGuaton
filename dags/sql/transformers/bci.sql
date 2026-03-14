-- =============================================================================
-- 2. POBLAR fact_offers - BCI
-- =============================================================================
-- Transformer robusto para BCI con detección automática de locaciones y tipos de tarjeta
-- Versión mejorada con fallbacks para evitar pérdida de registros (Don Carlos, Ocean Pacific, etc.)

DELETE FROM fact_offers WHERE bank_id = 'bci';

INSERT INTO fact_offers (offer_id, restaurant_id, bank_id, card_type_id, location_id, valid_days, discount_pct, discount_cap, discount_cap_is_unlimited, expiration_date, conditions, valid_month, source_url, image_url, address, scraped_at)
WITH processed_data AS (
    SELECT 
        clean_title,
        unnested_day,
        discount_pct,
        calculated_cap,
        -- Expiration Date parsing (Robust: handles YYYY-MM-DD and Spanish text)
        COALESCE(
            TRY_CAST(expiration_date AS DATE),
            CASE 
                WHEN expiration_date IS NOT NULL AND expiration_date ILIKE '% de %' THEN
                    (
                        WITH date_parts AS (
                            SELECT 
                                regexp_extract(lower(expiration_date), '(\d{1,2}) de ([a-z]+) (?:de )?(\d{4})', 1) as d,
                                regexp_extract(lower(expiration_date), '(\d{1,2}) de ([a-z]+) (?:de )?(\d{4})', 2) as m_name,
                                regexp_extract(lower(expiration_date), '(\d{1,2}) de ([a-z]+) (?:de )?(\d{4})', 3) as y
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
                ELSE NULL
            END
        ) as calculated_exp_date,
        conditions,
        url,
        image_url,
        location,
        scraped_at,
        card_type,
        title,
        discount_text,
        expiration_date
    FROM (
        SELECT 
            clean_title,
            unnested_day,
            COALESCE(
                CAST(NULLIF(regexp_extract(discount_text, '(\d+)', 1), '') AS INTEGER),
                CAST(NULLIF(regexp_extract(conditions, '(\d+)%\s*de\s*dcto', 1), '') AS INTEGER),
                CAST(NULLIF(regexp_extract(conditions, '(\d+)%\s*dcto', 1), '') AS INTEGER),
                0
            ) as discount_pct,
            -- discount_cap: from JSON field with regex fallback
            COALESCE(
                NULLIF(CAST(NULLIF(regexp_replace(discount_cap, '\D', '', 'g'), '') AS INTEGER), 0),
                CASE
                    WHEN strip_accents(conditions) ILIKE '%sin tope%' 
                        OR strip_accents(conditions) ILIKE '%sin topes%' 
                        OR strip_accents(conditions) ILIKE '%sin tope maximo%' 
                    THEN NULL
                    ELSE CAST(
                        NULLIF(
                            regexp_replace(
                                regexp_extract(
                                    strip_accents(conditions), 
                                    '(?i)(?:tope|hasta|maximo)[^\d]*?(\d{1,3}(?:\.\d{3})*|\d+)', 
                                    1
                                ),
                                '\D', '', 'g'
                            ), 
                            ''
                        ) AS INTEGER
                    )
                END
            ) as calculated_cap,
            conditions,
            url,
            image_url,
            location,
            scraped_at,
            card_type,
            title,
            discount_text,
            expiration_date
        FROM (
            SELECT 
                clean_title,
                unnest(
                    CASE 
                        WHEN lower(raw_days) ILIKE '%todos los d_as%' THEN ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
                        WHEN lower(raw_days) ILIKE '%lunes a viernes%' THEN ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes']
                        WHEN lower(raw_days) ILIKE '%s_bado y domingo%' THEN ['Sábado', 'Domingo']
                        WHEN raw_days ILIKE '%,%' OR raw_days ILIKE '% y %' THEN 
                            apply(
                                string_split(regexp_replace(raw_days, ' y ', ', ', 'g'), ','),
                                x -> CASE 
                                    WHEN lower(trim(x)) = 'lunes' THEN 'Lunes'
                                    WHEN lower(trim(x)) = 'martes' THEN 'Martes'
                                    WHEN lower(trim(x)) IN ('miercoles', 'mi_rcoles') THEN 'Miércoles'
                                    WHEN lower(trim(x)) = 'jueves' THEN 'Jueves'
                                    WHEN lower(trim(x)) = 'viernes' THEN 'Viernes'
                                    WHEN lower(trim(x)) IN ('sabado', 's_bado') THEN 'Sábado'
                                    WHEN lower(trim(x)) = 'domingo' THEN 'Domingo'
                                    ELSE trim(x)
                                END
                            )
                        WHEN lower(raw_days) IN ('lunes', 'martes', 'miércoles', 'miercoles', 'jueves', 'viernes', 'sábado', 'sabado', 'domingo') 
                        THEN [upper(left(raw_days, 1)) || lower(substring(raw_days, 2))]
                        ELSE [raw_days]
                    END
                ) as unnested_day,
                discount_text,
                discount_cap,
                conditions,
                url,
                image_url,
                location,
                scraped_at,
                card_type,
                title,
                expiration_date
            FROM (
                SELECT 
                    -- Limpiar título
                    regexp_replace(title, '\s*-\s*Descuento.*| - \d+% dcto', '', 'i') as clean_title,
                    -- Normalizar días base
                    coalesce(nullif(recurrence, ''), 'Todos los días') as raw_days,
                    discount_text,
                    discount_cap,
                    conditions,
                    url,
                    image_url,
                    location,
                    scraped_at,
                    card_type,
                    title,
                    expiration_date
                FROM (
                    SELECT 
                        json_extract_string(row_to_json(t), '$.title') as title,
                        json_extract_string(row_to_json(t), '$.recurrence') as recurrence,
                        json_extract_string(row_to_json(t), '$.discount_text') as discount_text,
                        json_extract_string(row_to_json(t), '$.discount_cap') as discount_cap,
                        json_extract_string(row_to_json(t), '$.conditions') as conditions,
                        json_extract_string(row_to_json(t), '$.url') as url,
                        json_extract_string(row_to_json(t), '$.image_url') as image_url,
                        json_extract_string(row_to_json(t), '$.location') as location,
                        json_extract_string(row_to_json(t), '$.scraped_at') as scraped_at,
                        json_extract_string(row_to_json(t), '$.card_type') as card_type,
                        json_extract_string(row_to_json(t), '$.expiration_date') as expiration_date
                    FROM read_json_auto('{{JSON_BASE_PATH}}/bci.json', sample_size=-1) t
                )
            ) inner_sub
        ) sub
    ) sub_with_unnest
)
SELECT 
    md5(concat('bci', clean_title, discount_text, COALESCE(dl.location_id, 'stgo'), unnested_day)) as offer_id,
    md5(upper(trim(regexp_replace(strip_accents(regexp_replace(regexp_replace(regexp_replace(title, '^Disfruta de tu beneficio en ', '', 'i'), '\s*-\s*Descuento.*| - \d+% dcto', '', 'i'), '^Restaurant(e)?\s+|\s+Restaurant(e)?$|^Pasteleria\s+|\s+Pasteleria$|^Heladeria\s+|\s+Heladeria$', '', 'i')), '[^a-zA-Z0-9]', '', 'g')))) as restaurant_id,
    'bci' as bank_id,
    CASE 
        -- Priorizar tipos específicos detectados en condiciones o por el scraper
        WHEN card_type = 'bci_premium' OR conditions ILIKE '%Signature%' OR conditions ILIKE '%Infinite%' OR conditions ILIKE '%Mastercard Black%' THEN 'bci_combined'
        WHEN card_type = 'bci_credito_debito' OR conditions ILIKE '%débito%' OR conditions ILIKE '%debito%' THEN 'bci_credito_debito'
        ELSE 'bci_credito'
    END as card_type_id,
    COALESCE(dl.location_id, 'stgo') as location_id,
    unnested_day as valid_days,
    discount_pct,
    calculated_cap as discount_cap,
    CASE
        WHEN calculated_cap IS NOT NULL THEN FALSE -- Numeric cap found -> NOT unlimited
        WHEN strip_accents(conditions) ILIKE '%sin tope%' 
             OR strip_accents(conditions) ILIKE '%sin topes%' 
             OR strip_accents(conditions) ILIKE '%sin tope maximo%' 
        THEN TRUE
        ELSE NULL
    END as discount_cap_is_unlimited,
    calculated_exp_date as expiration_date,
    conditions,
    strftime(current_timestamp, '%B') as valid_month,
    url as source_url,
    image_url,
    COALESCE(NULLIF(location, 'Ver sitio'), dl.commune, 'Santiago') as address,
    scraped_at
FROM processed_data sub
LEFT JOIN dim_locations dl ON (
    -- Únete por la locación explícita (si no es genérica)
    (sub.location NOT IN ('Ver sitio', 'En locales', 'Presencial') AND lower(strip_accents(sub.location)) ILIKE '%' || lower(strip_accents(dl.commune)) || '%') OR
    -- O por el slug de la URL (normalizando para que "las-condes" coincida con "las condes")
    (regexp_replace(lower(sub.url), '[^a-z]', '', 'g') ILIKE '%' || regexp_replace(lower(strip_accents(dl.commune)), '[^a-z]', '', 'g') || '%' AND length(dl.commune) > 4) OR
    -- Especial para casos como 'all'
    (dl.location_id = 'all' AND regexp_matches(upper(sub.title), '(MCDONALD|KFC|JUAN MAESTRO|CHINA WOK|WENDY|DOMINO|DUNKIN|SUSHI BLUES|VOLKA|YOGEN FRUZ|MELT|BURGER KING|DOGGIS|PAPA JOHN|PIZZA HUT|SUBWAY|TARRAGONA|LITTLE CAESAR|LE VICE|STARBUCKS|CARL|TOMMY|TACO BELL|VELVET BAKERY|BURGERBEEF|HAPPY BOX)'))
)
WHERE (sub.discount_pct > 0 OR sub.conditions ILIKE '%dcto%')
  AND sub.title NOT ILIKE '%ecook%'
-- Evitar duplicados si el join por URL y location coinciden accidentalmente, pero permitir múltiples ubicaciones si se detectaron
QUALIFY row_number() OVER (PARTITION BY clean_title, discount_pct, COALESCE(dl.location_id, 'stgo'), unnested_day, card_type_id ORDER BY scraped_at DESC) = 1
ON CONFLICT (offer_id) DO NOTHING;



