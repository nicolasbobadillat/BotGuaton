DELETE FROM fact_offers WHERE bank_id = 'bancoestado';

INSERT INTO fact_offers (offer_id, restaurant_id, bank_id, card_type_id, location_id, valid_days, discount_pct, discount_cap, discount_cap_is_unlimited, conditions, valid_month, expiration_date, source_url, image_url, address, scraped_at)
WITH raw_be AS (
    SELECT 
        json_extract_string(row_to_json(t), '$.bank') as bank,
        json_extract_string(row_to_json(t), '$.title') as title,
        json_extract_string(row_to_json(t), '$.discount_text') as discount_text,
        json_extract_string(row_to_json(t), '$.validity') as validity,
        json_extract_string(row_to_json(t), '$.location') as location,
        json_extract_string(row_to_json(t), '$.scraped_at') as scraped_at,
        json_extract_string(row_to_json(t), '$.discount_cap') as discount_cap,
        json_extract_string(row_to_json(t), '$.expiration_date') as json_expiration_date
    FROM read_json_auto('{{JSON_BASE_PATH}}/bancoestado.json') t
),
exploded_be AS (
    SELECT 
        bank, title, discount_text, validity, scraped_at, discount_cap, json_expiration_date,
        unnest(
            CASE 
                WHEN lower(strip_accents(validity)) = 'todos los dias' THEN ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
                WHEN lower(strip_accents(validity)) IN ('lunes', 'martes', 'miercoles', 'jueves', 'viernes', 'sabado', 'domingo') 
                THEN [upper(left(validity, 1)) || lower(substring(validity, 2))]
                ELSE ['Todos los días']
            END
        ) as unnested_day,
        location
    FROM raw_be
),
exploded_locations AS (
    SELECT 
        bank, title, discount_text, validity, scraped_at, discount_cap, json_expiration_date, unnested_day,
        unnest(string_split(location, '|')) as extracted_location
    FROM exploded_be
),
processed_be AS (
    SELECT 
        *,
        TRY_CAST(discount_cap AS INTEGER) as calculated_cap,
        CAST(NULLIF(regexp_extract(discount_text, '(\d+)', 1), '') AS INTEGER) as calculated_discount_pct
    FROM exploded_locations
)
SELECT 
    md5(concat('bancoestado', title, discount_text, extracted_location, unnested_day)) as offer_id,
    md5(upper(trim(regexp_replace(strip_accents(regexp_replace(regexp_replace(regexp_replace(title, '^Disfruta de tu beneficio en ', '', 'i'), '\s*-\s*Descuento.*| - \d+% dcto', '', 'i'), '^Restaurant(e)?\s+|\s+Restaurant(e)?$|^Pasteleria\s+|\s+Pasteleria$|^Heladeria\s+|\s+Heladeria$', '', 'i')), '[^a-zA-Z0-9]', '', 'g')))) as restaurant_id,
    'bancoestado' as bank_id,
    'bancoestado_general' as card_type_id,
    COALESCE(
        CASE 
            WHEN regexp_matches(upper(strip_accents(title)), '(MCDONALD|KFC|JUAN MAESTRO|CHINA WOK|WENDY|DOMINO|DUNKIN|SUSHI BLUES|VOLKA|YOGEN FRUZ|MELT|BURGER KING|DOGGIS|PAPA JOHN|PIZZA HUT|SUBWAY|TARRAGONA|LITTLE CAESAR|LE VICE)') THEN 'all'
            WHEN extracted_location ILIKE '%Metropolitana%' THEN 'stgo'
            WHEN extracted_location ILIKE '%Valparaíso%' THEN 'valp'
            ELSE (SELECT dl.location_id FROM dim_locations dl WHERE strip_accents(extracted_location) ILIKE '%' || strip_accents(dl.commune) || '%' ORDER BY length(dl.commune) DESC LIMIT 1)
        END,
        'stgo'
    ) as location_id,
    CASE WHEN title ILIKE '%Juan Maestro%' OR title ILIKE '%Doggis%' OR title ILIKE '%Dominos Pizza%' OR title ILIKE '%Barrio Chicken%' THEN 'Martes' ELSE unnested_day END as valid_days,
    calculated_discount_pct as discount_pct,
    calculated_cap as discount_cap,
    CASE WHEN calculated_cap IS NOT NULL THEN FALSE ELSE NULL END as discount_cap_is_unlimited,
    NULL as conditions,
    strftime(current_timestamp, '%B') as valid_month,
    CASE 
        WHEN json_expiration_date IS NULL OR json_expiration_date = '' THEN NULL
        ELSE 
           TRY_CAST(
            regexp_extract(json_expiration_date, '(\d{4})', 1) || '-' ||
            CASE 
                WHEN lower(json_expiration_date) ILIKE '%enero%' THEN '01'
                WHEN lower(json_expiration_date) ILIKE '%febrero%' THEN '02'
                WHEN lower(json_expiration_date) ILIKE '%marzo%' THEN '03'
                WHEN lower(json_expiration_date) ILIKE '%abril%' THEN '04'
                WHEN lower(json_expiration_date) ILIKE '%mayo%' THEN '05'
                WHEN lower(json_expiration_date) ILIKE '%junio%' THEN '06'
                WHEN lower(json_expiration_date) ILIKE '%julio%' THEN '07'
                WHEN lower(json_expiration_date) ILIKE '%agosto%' THEN '08'
                WHEN lower(json_expiration_date) ILIKE '%septiembre%' THEN '09'
                WHEN lower(json_expiration_date) ILIKE '%octubre%' THEN '10'
                WHEN lower(json_expiration_date) ILIKE '%noviembre%' THEN '11'
                WHEN lower(json_expiration_date) ILIKE '%diciembre%' THEN '12'
                ELSE '01'
            END || '-' ||
            LPAD(regexp_extract(json_expiration_date, '(\d{1,2})', 1), 2, '0')
           AS DATE)
    END as expiration_date,
    NULL as source_url,
    NULL as image_url,
    trim(extracted_location) as address,
    scraped_at
FROM processed_be
WHERE calculated_discount_pct > 0
  AND title NOT ILIKE '%ecook%'
  AND title NOT ILIKE '%Papa John%'
ON CONFLICT (offer_id) DO NOTHING;



