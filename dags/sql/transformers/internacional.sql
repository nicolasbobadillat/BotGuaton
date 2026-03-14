DELETE FROM fact_offers WHERE bank_id = 'internacional';

INSERT INTO fact_offers (offer_id, restaurant_id, bank_id, card_type_id, location_id, valid_days, discount_pct, discount_cap, discount_cap_is_unlimited, conditions, expiration_date, valid_month, source_url, image_url, address, scraped_at)
WITH raw_int AS (
    SELECT 
        title,
        regexp_replace(regexp_replace(title, '(?i)^\s*s[aá]bados?\s+', ''), '(?i)\s+(lunes|nov\.|enero|diciembre|dic|noviembre|s[aá]bados?|martes|mi[eé]rcoles|s[aá]bado|jueves|viernes|domingo).*$', '') as clean_title,
        discount_text,
        discount_cap,
        conditions,
        scraped_at,
        expiration_date as raw_expiration_date,
        recurrence,
        COALESCE(
            location,
            regexp_extract(conditions, ',?\s*en\s+[^,]+,\s*([^\.]+(?:Vitacura|Las Condes|Providencia|Lo Barnechea|Ñuñoa|Reñaca|Concepción)[^\.]*)', 1),
            regexp_extract(conditions, 'ubicado en\s+([^\.]+)', 1),
            regexp_extract(conditions, 'dirección:\s*([^\.]+)', 1)
        ) as extracted_location
    FROM read_json_auto('{{JSON_BASE_PATH}}/internacional.json', columns={'title': 'VARCHAR', 'discount_text': 'VARCHAR', 'expiration_date': 'VARCHAR', 'recurrence': 'VARCHAR', 'discount_cap': 'VARCHAR', 'location': 'VARCHAR', 'conditions': 'VARCHAR', 'scraped_at': 'VARCHAR'})
    WHERE title NOT ILIKE '%ecook%'
      AND title NOT ILIKE '%RESTOFANS%'
      AND title NOT ILIKE '%PedidosYa%'
      AND title NOT ILIKE '%Sacacorchos%'
      AND title NOT ILIKE '%La Pesca de los Mekis%'
),
exploded_int AS (
    SELECT 
        *,
        unnest(
            CASE 
                WHEN conditions ILIKE '%lunes a s_bado%' OR conditions ILIKE '%lunes a sabado%' THEN ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado']
                WHEN conditions ILIKE '%lunes, martes y mi_rcoles%' THEN ['Lunes', 'Martes', 'Miércoles']
                WHEN len(regexp_extract_all(lower(conditions), '(lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bado|domingo)')) > 0 
                THEN apply(regexp_extract_all(lower(conditions), '(lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bado|domingo)'), x -> CASE WHEN x = 'lunes' THEN 'Lunes' WHEN x = 'martes' THEN 'Martes' WHEN x IN ('miercoles', 'miércoles') THEN 'Miércoles' WHEN x = 'jueves' THEN 'Jueves' WHEN x = 'viernes' THEN 'Viernes' WHEN x IN ('sabado', 'sábado') THEN 'Sábado' WHEN x = 'domingo' THEN 'Domingo' ELSE x END)
                -- Fallback to recurrence
                WHEN recurrence IS NOT NULL AND trim(recurrence) != '' THEN
                    CASE
                        WHEN lower(recurrence) ILIKE '%todos los d_as%' THEN ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
                        WHEN lower(recurrence) ILIKE '%lunes a s_bado%' THEN ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado']
                        WHEN len(regexp_extract_all(lower(recurrence), '(lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bado|domingo)')) > 0 
                        THEN apply(regexp_extract_all(lower(recurrence), '(lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bado|domingo)'), x -> CASE WHEN x = 'lunes' THEN 'Lunes' WHEN x = 'martes' THEN 'Martes' WHEN x IN ('miercoles', 'miércoles') THEN 'Miércoles' WHEN x = 'jueves' THEN 'Jueves' WHEN x = 'viernes' THEN 'Viernes' WHEN x IN ('sabado', 'sábado') THEN 'Sábado' WHEN x = 'domingo' THEN 'Domingo' ELSE x END)
                        ELSE ['Consultar']
                    END
                ELSE ['Consultar']
            END
        ) as unnested_day
    FROM raw_int
),
processed_int AS (
    SELECT 
        *,
        -- discount_cap: Prioritize JSON discount_cap, fallback to regex on conditions
        COALESCE(
            CAST(NULLIF(regexp_replace(CAST(discount_cap AS VARCHAR), '\D', '', 'g'), '') AS INTEGER),
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
        CAST(NULLIF(regexp_extract(discount_text, '(\d+)', 1), '') AS INTEGER) as calculated_discount_pct
    FROM exploded_int
)
SELECT 
    md5(concat('internacional', clean_title, discount_text, COALESCE(extracted_location, 'stgo'), unnested_day)) as offer_id,
    md5(upper(trim(regexp_replace(strip_accents(regexp_replace(regexp_replace(regexp_replace(clean_title, '^Disfruta de tu beneficio en ', '', 'i'), '\s*-\s*Descuento.*| - \d+% dcto', '', 'i'), '^Restaurant(e)?\s+|\s+Restaurant(e)?$|^Pasteleria\s+|\s+Pasteleria$|^Heladeria\s+|\s+Heladeria$', '', 'i')), '[^a-zA-Z0-9]', '', 'g')))) as restaurant_id,
    'internacional' as bank_id,
    'internacional_credito' as card_type_id,
    COALESCE(
        CASE 
            WHEN conditions ILIKE '%Vitacura%' THEN 'vita'
            WHEN conditions ILIKE '%Las Condes%' THEN 'lcon'
            WHEN conditions ILIKE '%Providencia%' THEN 'prov'
            WHEN conditions ILIKE '%Lo Barnechea%' THEN 'lbar'
            WHEN conditions ILIKE '%Ñuñoa%' THEN 'nuno'
            WHEN conditions ILIKE '%Reñaca%' THEN 'rena'
            WHEN clean_title ILIKE '%Domani%' THEN 'prov'
            WHEN clean_title ILIKE '%Ramblas%' THEN 'prov'
            WHEN clean_title ILIKE '%Vicenzo%' THEN 'lcon'
            ELSE 'stgo'
        END
    ) as location_id,
    unnested_day as valid_days,
    calculated_discount_pct as discount_pct,
    calculated_cap as discount_cap,
    CASE
        WHEN calculated_cap IS NOT NULL THEN FALSE -- Numeric cap found -> NOT unlimited
        WHEN strip_accents(conditions) ILIKE '%sin tope%' OR strip_accents(conditions) ILIKE '%sin topes%' OR strip_accents(conditions) ILIKE '%sin tope maximo%' THEN TRUE
        ELSE NULL
    END as discount_cap_is_unlimited,
    conditions,
    TRY_CAST(raw_expiration_date AS DATE) as expiration_date,
    strftime(current_timestamp, '%B') as valid_month,
    NULL as source_url,
    NULL as image_url,
    extracted_location as address,
    scraped_at
FROM processed_int
WHERE calculated_discount_pct > 0
ON CONFLICT (offer_id) DO NOTHING;



