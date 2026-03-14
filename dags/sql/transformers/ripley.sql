-- =============================================================================
-- 7. POBLAR fact_offers - RIPLEY
-- =============================================================================

DELETE FROM fact_offers WHERE bank_id = 'ripley';

INSERT INTO fact_offers (offer_id, restaurant_id, bank_id, card_type_id, location_id, valid_days, discount_pct, discount_cap, discount_cap_is_unlimited, expiration_date, conditions, valid_month, source_url, image_url, address, scraped_at)
WITH raw_data AS (
    SELECT * FROM read_json_auto('{{JSON_BASE_PATH}}/ripley.json', columns={
        'bank': 'VARCHAR',
        'title': 'VARCHAR',
        'discount_text': 'VARCHAR',
        'active_days': 'VARCHAR[]',
        'recurrence': 'VARCHAR',
        'location': 'VARCHAR',
        'conditions': 'VARCHAR',
        'discount_cap': 'INTEGER',
        'expiration_date': 'VARCHAR',
        'description': 'VARCHAR',
        'image_url': 'VARCHAR',
        'url': 'VARCHAR',
        'scraped_at': 'VARCHAR',
        'category': 'VARCHAR'
    })
    WHERE title NOT ILIKE '%RESTOFANS%'
      AND title NOT ILIKE '%PedidosYa%'
      AND title NOT ILIKE '%Sacacorchos%'
      AND title NOT ILIKE '%La Pesca de los Mekis%'
),
base_data AS (
    SELECT 
        title,
        discount_text,
        active_days,
        location as raw_location,
        regexp_extract(location, '\(([^)]+)\)', 1) as extracted_commune,
        conditions,
        discount_cap as json_discount_cap,
        expiration_date as json_expiration_date,
        category,
        recurrence,
        description,
        url,
        image_url,
        scraped_at,
        CASE
            WHEN upper(conditions) LIKE '%EXCLUSIVO BLACK%' OR upper(conditions) LIKE '%MASTERCARD BLACK%' THEN 'ripley_black'
            ELSE 'ripley_credito'
        END as calculated_card_type,
        CAST(NULLIF(regexp_extract(discount_text, '(\d+)', 1), '') AS INTEGER) as calculated_discount_pct
    FROM raw_data
    WHERE CAST(NULLIF(regexp_extract(discount_text, '(\d+)', 1), '') AS INTEGER) > 0
),
-- Expand multi-commune locations (e.g. "R.M. (La Florida / Ñuñoa / Providencia)") into separate rows
location_expanded AS (
    SELECT 
        bd.*,
        COALESCE(
            CASE 
                WHEN bd.raw_location ILIKE '%Todas las sucursales%' OR bd.raw_location ILIKE '%Todo Chile%' THEN 'all'
                WHEN regexp_matches(upper(strip_accents(bd.title)), '(MCDONALD|KFC|JUAN MAESTRO|CHINA WOK|WENDY|DOMINO|DUNKIN|SUSHI BLUES|VOLKA|YOGEN FRUZ|MELT|BURGER KING|DOGGIS|PAPA JOHN|PIZZA HUT|SUBWAY|TARRAGONA|LITTLE CAESAR|LE VICE|COPPELIA)') THEN 'all'
                WHEN bd.extracted_commune LIKE '%/%' THEN
                    (SELECT dl.location_id FROM dim_locations dl 
                     WHERE strip_accents(lower(trim(split_commune))) = strip_accents(lower(dl.commune)) 
                     LIMIT 1)
                WHEN (SELECT dl.location_id FROM dim_locations dl WHERE strip_accents(lower(trim(bd.extracted_commune))) = strip_accents(lower(dl.commune)) LIMIT 1) IS NOT NULL 
                THEN (SELECT dl.location_id FROM dim_locations dl WHERE strip_accents(lower(trim(bd.extracted_commune))) = strip_accents(lower(dl.commune)) LIMIT 1)
                ELSE 'stgo' 
            END,
            'stgo'
        ) as calculated_location_id
    FROM (
        SELECT base_data.*, 
               CASE 
                   WHEN extracted_commune LIKE '%/%' 
                   THEN unnest(string_split(extracted_commune, '/'))
                   ELSE extracted_commune 
               END as split_commune
        FROM base_data
    ) bd
),
exploded_data AS (
    SELECT 
        *,
        unnest(
            CASE 
                WHEN active_days IS NOT NULL AND len(active_days) > 0 THEN 
                    CASE
                        WHEN len(active_days) = 1 AND upper(active_days[1]) LIKE '%LUNES A DOMINGO%' THEN ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
                        WHEN len(active_days) = 1 AND upper(active_days[1]) LIKE '%TODOS LOS D%' THEN ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
                        WHEN len(active_days) = 1 AND upper(active_days[1]) LIKE '%LUNES A VIERNES%' THEN ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes']
                        WHEN len(active_days) = 1 AND upper(active_days[1]) LIKE '%LUNES A JUEVES%' THEN ['Lunes', 'Martes', 'Miércoles', 'Jueves']
                        WHEN len(active_days) = 1 AND upper(active_days[1]) LIKE '%JUEVES A DOMINGO%' THEN ['Jueves', 'Viernes', 'Sábado', 'Domingo']
                        WHEN len(active_days) = 1 AND upper(active_days[1]) LIKE '%VIERNES A DOMINGO%' THEN ['Viernes', 'Sábado', 'Domingo']
                        WHEN len(active_days) = 1 AND (upper(active_days[1]) LIKE '%MIERCOLES A DOMINGO%' OR upper(active_days[1]) LIKE '%MI_RCOLES A DOMINGO%') THEN ['Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
                        ELSE active_days
                    END
                WHEN recurrence IS NOT NULL AND trim(recurrence) != '' THEN
                    CASE
                        WHEN lower(recurrence) ILIKE '%todos los d_as%' THEN ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
                        WHEN lower(recurrence) ILIKE '%lunes a viernes%' THEN ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes']
                        WHEN lower(recurrence) ILIKE '%lunes a jueves%' THEN ['Lunes', 'Martes', 'Miércoles', 'Jueves']
                        WHEN len(regexp_extract_all(lower(recurrence), '(lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bado|domingo)')) > 0 
                        THEN apply(regexp_extract_all(lower(recurrence), '(lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bado|domingo)'), x -> CASE WHEN x = 'lunes' THEN 'Lunes' WHEN x = 'martes' THEN 'Martes' WHEN x IN ('miercoles', 'miércoles') THEN 'Miércoles' WHEN x = 'jueves' THEN 'Jueves' WHEN x = 'viernes' THEN 'Viernes' WHEN x IN ('sabado', 'sábado') THEN 'Sábado' WHEN x = 'domingo' THEN 'Domingo' ELSE x END)
                        ELSE ['Todos los días']
                    END
                ELSE ['Todos los días']
            END
        ) as day_unnested
    FROM location_expanded
),
processed_data AS (
    SELECT 
        *,
        -- Robust discount cap logic
        CASE
            WHEN strip_accents(conditions) ILIKE '%sin tope%' 
                 OR strip_accents(conditions) ILIKE '%sin topes%' 
                 OR strip_accents(conditions) ILIKE '%sin tope maximo%' 
            THEN NULL
            ELSE COALESCE(
                json_discount_cap,
                CAST(NULLIF(regexp_replace(
                    regexp_extract(strip_accents(conditions), '(?i)(?:tope|hasta|maximo)[^$]*?\$([\d\.]+)', 1),
                    '\.', '', 'g'
                ), '') AS INTEGER)
            )
        END as calculated_cap
    FROM exploded_data
)
SELECT 
    md5(concat('ripley', title, discount_text, COALESCE(day_unnested, ''), calculated_location_id)) as offer_id,
    md5(upper(trim(regexp_replace(strip_accents(regexp_replace(title, '^Restaurant(e)?\s+|\s+Restaurant(e)?$', '', 'i')), '[^a-zA-Z0-9]', '', 'g')))) as restaurant_id,
    'ripley' as bank_id,
    calculated_card_type as card_type_id,
    calculated_location_id as location_id,
    CASE 
        WHEN lower(day_unnested) = 'lunes' THEN 'Lunes'
        WHEN lower(day_unnested) = 'martes' THEN 'Martes'
        WHEN lower(day_unnested) IN ('miércoles', 'miercoles') THEN 'Miércoles'
        WHEN lower(day_unnested) = 'jueves' THEN 'Jueves'
        WHEN lower(day_unnested) = 'viernes' THEN 'Viernes'
        WHEN lower(day_unnested) IN ('sábado', 'sabado') THEN 'Sábado'
        WHEN lower(day_unnested) = 'domingo' THEN 'Domingo'
        WHEN lower(day_unnested) LIKE '%todos los%' THEN 'Todos los días'
        ELSE day_unnested
    END as valid_days,
    calculated_discount_pct as discount_pct,
    calculated_cap as discount_cap,
    CASE
        WHEN calculated_cap IS NOT NULL THEN FALSE -- Numeric cap found -> NOT unlimited
        WHEN strip_accents(conditions) ILIKE '%sin tope%' OR strip_accents(conditions) ILIKE '%sin topes%' OR strip_accents(conditions) ILIKE '%sin tope maximo%' THEN TRUE
        ELSE NULL
    END as discount_cap_is_unlimited,
    TRY_CAST(json_expiration_date AS DATE) as expiration_date,
    conditions,
    strftime(current_timestamp, '%B') as valid_month,
    url as source_url,
    image_url,
    raw_location as address,
    scraped_at
FROM processed_data
ON CONFLICT (offer_id) DO NOTHING;

