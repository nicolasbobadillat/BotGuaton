DELETE FROM fact_offers WHERE bank_id = 'cencosud';

INSERT INTO fact_offers (offer_id, restaurant_id, bank_id, card_type_id, location_id, valid_days, discount_pct, discount_cap, discount_cap_is_unlimited, conditions, valid_month, source_url, image_url, address, expiration_date, scraped_at)
WITH raw_data AS (
    SELECT 
        json_extract_string(row_to_json(t), '$.title') as title,
        json_extract_string(row_to_json(t), '$.discount_text') as discount_text,
        unnest(string_split(json_extract_string(row_to_json(t), '$.location'), ' - ')) as location_split,
        json_extract_string(row_to_json(t), '$.recurrence') as recurrence,
        json_extract_string(row_to_json(t), '$.conditions') as conditions,
        json_extract_string(row_to_json(t), '$.description') as description,
        json_extract_string(row_to_json(t), '$.url') as url,
        json_extract_string(row_to_json(t), '$.image_url') as image_url,
        json_extract_string(row_to_json(t), '$.scraped_at') as scraped_at
    FROM read_json_auto('{{JSON_BASE_PATH}}/cencosud.json') t
    WHERE CAST(NULLIF(regexp_extract(json_extract_string(row_to_json(t), '$.discount_text'), '(\d+)', 1), '') AS INTEGER) > 0
      AND json_extract_string(row_to_json(t), '$.title') NOT ILIKE '%ecook%'
      AND json_extract_string(row_to_json(t), '$.title') NOT ILIKE '%RESTOFANS%'
      AND json_extract_string(row_to_json(t), '$.title') NOT ILIKE '%PedidosYa%'
      AND json_extract_string(row_to_json(t), '$.title') NOT ILIKE '%Sacacorchos%'
      AND json_extract_string(row_to_json(t), '$.title') NOT ILIKE '%La Pesca de los Mekis%'
),
exploded_data AS (
    SELECT 
        r.*,
        unnest(
            CASE 
                WHEN lower(recurrence) ILIKE '%todos los d_as%' THEN ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
                WHEN lower(recurrence) ILIKE '%lunes a viernes%' THEN ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes']
                WHEN lower(recurrence) ILIKE '%lunes a jueves%' THEN ['Lunes', 'Martes', 'Miércoles', 'Jueves']
                WHEN lower(recurrence) ILIKE '%jueves y viernes%' THEN ['Jueves', 'Viernes']
                WHEN lower(recurrence) ILIKE '%lunes y mi_rcoles%' THEN ['Lunes', 'Miércoles']
                WHEN len(regexp_extract_all(lower(coalesce(recurrence, '') || ' ' || coalesce(conditions, '')), '(lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bado|domingo)')) > 0 
                THEN apply(
                    regexp_extract_all(lower(coalesce(recurrence, '') || ' ' || coalesce(conditions, '')), '(lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bado|domingo)'),
                    x -> CASE 
                        WHEN x = 'lunes' THEN 'Lunes'
                        WHEN x = 'martes' THEN 'Martes'
                        WHEN x IN ('miercoles', 'miércoles') THEN 'Miércoles'
                        WHEN x = 'jueves' THEN 'Jueves'
                        WHEN x = 'viernes' THEN 'Viernes'
                        WHEN x IN ('sabado', 'sábado') THEN 'Sábado'
                        WHEN x = 'domingo' THEN 'Domingo'
                        ELSE x
                    END
                )
                ELSE ['Consultar condiciones']
            END
        ) as unnested_day
    FROM raw_data r
),
processed_data AS (
    SELECT 
        *,
        -- discount_cap: regex on conditions
        CASE
            WHEN strip_accents(conditions) ILIKE '%sin tope%' 
                 OR strip_accents(conditions) ILIKE '%sin topes%' 
                 OR strip_accents(conditions) ILIKE '%sin tope maximo%' 
            THEN NULL
            ELSE CAST(NULLIF(regexp_replace(
                regexp_extract(strip_accents(conditions), '(?i)(?:tope|hasta|maximo)[^$]*?\$([\d\.]+)', 1),
                '\.', '', 'g'
            ), '') AS INTEGER)
        END as calculated_cap,
        -- Extract month and year for expiration_date
        CASE 
            WHEN lower(conditions) LIKE '%enero%' THEN '01'
            WHEN lower(conditions) LIKE '%febrero%' THEN '02'
            WHEN lower(conditions) LIKE '%marzo%' THEN '03'
            WHEN lower(conditions) LIKE '%abril%' THEN '04'
            WHEN lower(conditions) LIKE '%mayo%' THEN '05'
            WHEN lower(conditions) LIKE '%junio%' THEN '06'
            WHEN lower(conditions) LIKE '%julio%' THEN '07'
            WHEN lower(conditions) LIKE '%agosto%' THEN '08'
            WHEN lower(conditions) LIKE '%septiembre%' THEN '09'
            WHEN lower(conditions) LIKE '%octubre%' THEN '10'
            WHEN lower(conditions) LIKE '%noviembre%' THEN '11'
            WHEN lower(conditions) LIKE '%diciembre%' THEN '12'
            ELSE NULL
        END as m_num,
        regexp_extract(conditions, '202\d', 0) as y_val
    FROM exploded_data
)
SELECT 
    md5(concat('cencosud', title, discount_text, trim(location_split), unnested_day)) as offer_id,
    md5(upper(trim(regexp_replace(strip_accents(regexp_replace(regexp_replace(regexp_replace(title, '^Disfruta de tu beneficio en ', '', 'i'), '\s*-\s*Descuento.*| - \d+% dcto', '', 'i'), '^Restaurant(e)?\s+|\s+Restaurant(e)?$|^Pasteleria\s+|\s+Pasteleria$|^Heladeria\s+|\s+Heladeria$', '', 'i')), '[^a-zA-Z0-9]', '', 'g')))) as restaurant_id,
    'cencosud' as bank_id,
    'cencosud_scotiabank' as card_type_id,
    COALESCE(
        CASE 
            WHEN regexp_matches(upper(strip_accents(title)), '(MCDONALD|KFC|JUAN MAESTRO|CHINA WOK|WENDY|DOMINO|DUNKIN|SUSHI BLUES|VOLKA|YOGEN FRUZ|MELT|BURGER KING|DOGGIS|PAPA JOHN|PIZZA HUT|SUBWAY|TARRAGONA|LITTLE CAESAR|LE VICE)') THEN 'all'
            WHEN location_split ILIKE '%Costanera%' THEN 'cost'
            WHEN location_split ILIKE '%Parque Arauco%' THEN 'parq'
            WHEN location_split ILIKE '%Alto Las Condes%' THEN 'alto'
            WHEN location_split ILIKE '%Mirador del Alto%' THEN 'mira'
            WHEN location_split ILIKE '%Mall Plaza%' THEN 'stgo'
            ELSE (SELECT dl.location_id FROM dim_locations dl WHERE location_split ILIKE '%' || dl.commune || '%' ORDER BY length(dl.commune) DESC LIMIT 1)
        END,
        'stgo'
    ) as location_id,
    CASE WHEN unnested_day IN ('Consultar', 'Consultar condiciones') THEN NULL ELSE unnested_day END as valid_days,
    CAST(NULLIF(regexp_extract(discount_text, '(\d+)', 1), '') AS INTEGER) as discount_pct,
    calculated_cap as discount_cap,
    CASE
        WHEN calculated_cap IS NOT NULL THEN FALSE -- Numeric cap found -> NOT unlimited
        WHEN strip_accents(conditions) ILIKE '%sin tope%' 
             OR strip_accents(conditions) ILIKE '%sin topes%' 
             OR strip_accents(conditions) ILIKE '%sin tope maximo%' 
        THEN TRUE
        ELSE NULL
    END as discount_cap_is_unlimited,
    conditions,
    strftime(current_timestamp, '%B') as valid_month,
    url as source_url,
    image_url,
    location_split as address,
    CASE 
        WHEN m_num IS NOT NULL THEN last_day(CAST(COALESCE(NULLIF(y_val, ''), '2026') || '-' || m_num || '-01' AS DATE))
        ELSE NULL
    END as expiration_date,
    scraped_at
FROM processed_data
ON CONFLICT (offer_id) DO NOTHING;



