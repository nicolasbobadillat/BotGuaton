-- =============================================================================
-- 11. POBLAR fact_offers - ITAU
-- =============================================================================

DELETE FROM fact_offers WHERE bank_id = 'itau';

INSERT INTO fact_offers (offer_id, restaurant_id, bank_id, card_type_id, location_id, valid_days, discount_pct, discount_cap, discount_cap_is_unlimited, conditions, valid_month, source_url, image_url, address, expiration_date, scraped_at)
WITH raw_itau AS (
    SELECT 
        json_extract_string(row_to_json(t), '$.title') as title,
        json_extract_string(row_to_json(t), '$.discount_text') as discount_text,
        json_extract_string(row_to_json(t), '$.conditions') as raw_conditions,
        json_extract_string(row_to_json(t), '$.description') as raw_description,
        json_extract_string(row_to_json(t), '$.url') as url,
        json_extract_string(row_to_json(t), '$.image_url') as image_url,
        json_extract_string(row_to_json(t), '$.scraped_at') as scraped_at,
        json_extract_string(row_to_json(t), '$.scraped_at') as scraped_at,
        json_extract_string(row_to_json(t), '$.location') as raw_location,
        json_extract_string(row_to_json(t), '$.commune') as raw_commune,
        COALESCE(json_extract_string(row_to_json(t), '$.recurrence'), '') as recurrence,
        json_extract_string(row_to_json(t), '$.expiration_date') as expiration_date
    FROM read_json_auto('{{JSON_BASE_PATH}}/itau.json') t
),
exploded_itau AS (
    SELECT 
        *,
        unnest(
            CASE 
                WHEN lower(title) ILIKE '%Philia%' THEN ['Jueves', 'Viernes', 'Sábado', 'Domingo']
                WHEN lower(title) ILIKE '%Leonidas%' THEN ['Miércoles']
                WHEN lower(title) ILIKE '%Marley Coffee%' THEN ['Jueves']
                WHEN lower(recurrence) ILIKE '%todos los d_as%' THEN ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
                WHEN lower(recurrence) ILIKE '%lunes a viernes%' THEN ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes']
                WHEN lower(recurrence) ILIKE '%lunes a s_bado%' THEN ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado']
                WHEN lower(recurrence) ILIKE '%domingo a mi_rcoles%' THEN ['Domingo', 'Lunes', 'Martes', 'Miércoles']
                WHEN len(regexp_extract_all(lower(COALESCE(recurrence, '')), '(lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bado|domingo)')) > 0 
                THEN regexp_extract_all(lower(COALESCE(recurrence, '')), '(lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bado|domingo)')
                WHEN len(regexp_extract_all(lower(COALESCE(raw_description, '')), '(lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bado|domingo)')) > 0 
                THEN regexp_extract_all(lower(COALESCE(raw_description, '')), '(lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bado|domingo)')
                ELSE ['Todos los días']
            END
        ) as valid_days_raw
    FROM raw_itau
),
processed_itau AS (
    SELECT 
        *,
        md5(upper(trim(regexp_replace(strip_accents(regexp_replace(regexp_replace(regexp_replace(title, '^Disfruta de tu beneficio en ', '', 'i'), '\s*-\s*Descuento.*| - \d+% dcto', '', 'i'), '^Restaurant(e)?\s+|\s+Restaurant(e)?$|^Pasteleria\s+|\s+Pasteleria$|^Heladeria\s+|\s+Heladeria$', '', 'i')), '[^a-zA-Z0-9]', '', 'g')))) as restaurant_id,
        COALESCE(
            CASE 
                WHEN regexp_matches(upper(strip_accents(title)), '(MCDONALD|KFC|JUAN MAESTRO|CHINA WOK|WENDY|DOMINO|DUNKIN|SUSHI BLUES|VOLKA|YOGEN FRUZ|MELT|BURGER KING|DOGGIS|PAPA JOHN|PIZZA HUT|SUBWAY|TARRAGONA|LITTLE CAESAR|LE VICE)') THEN 'all'
                WHEN raw_location ILIKE '%Manuel Montt%' OR raw_commune ILIKE '%Providencia%' THEN 'prov'
                WHEN strip_accents(raw_location) ILIKE '%Alonso de Cordova%' THEN 'vita'
                WHEN raw_location ILIKE '%La Serena%' THEN 'sere'
                WHEN raw_location ILIKE '%Coquimbo%' THEN 'coqu'
                WHEN raw_location ILIKE '%Viña del Mar%' THEN 'vina'
                WHEN raw_location ILIKE '%Valparaíso%' THEN 'valp'
                WHEN raw_location ILIKE '%Maitencillo%' THEN 'mait'
                WHEN raw_location ILIKE '%Zapallar%' THEN 'zapa'
                WHEN raw_location ILIKE '%Puchuncaví%' THEN 'puch'
                WHEN raw_location ILIKE '%Algarrobo%' THEN 'algr'
                WHEN raw_location ILIKE '%Concón%' THEN 'conc'
                WHEN raw_location ILIKE '%Reñaca%' THEN 'rena'
                WHEN raw_location ILIKE '%Santo Domingo%' THEN 'sdom'
                WHEN raw_location ILIKE '%Concepción%' THEN 'conp'
                WHEN raw_location ILIKE '%Temuco%' THEN 'temu'
                WHEN raw_location ILIKE '%Rancagua%' THEN 'ranc'
                WHEN raw_location ILIKE '%Machalí%' THEN 'mach'
                WHEN raw_location ILIKE '%Curicó%' THEN 'curi'
                WHEN raw_location ILIKE '%Rengo%' THEN 'reng'
                WHEN raw_location ILIKE '%Talca%' THEN 'talm'
                WHEN raw_location ILIKE '%Chillán%' THEN 'chil'
                WHEN raw_location ILIKE '%Puerto Varas%' THEN 'pvar'
                WHEN raw_location ILIKE '%Puerto Montt%' THEN 'pmon'
                WHEN raw_location ILIKE '%Valdivia%' AND raw_location NOT ILIKE '%Pedro de Valdivia%' THEN 'vald'
                WHEN raw_location ILIKE '%Osorno%' THEN 'osor'
                WHEN raw_location ILIKE '%Pucón%' THEN 'puco'
                WHEN raw_location ILIKE '%Villarrica%' THEN 'vill'
                WHEN raw_location ILIKE '%Isidora Goyenechea%' THEN 'lcon'
                WHEN raw_location ILIKE '%Jorge Washington%' THEN 'nuno'
                WHEN raw_location ILIKE '%Kennedy%' THEN 'lcon'
                WHEN raw_location ILIKE '%Cachagua%' THEN 'cach'
                WHEN raw_location ILIKE '%Providencia%' OR raw_commune ILIKE '%Providencia%' THEN 'prov'
                WHEN raw_location ILIKE '%Las Condes%' OR raw_commune ILIKE '%Las Condes%' THEN 'lcon'
                WHEN raw_location ILIKE '%Vitacura%' OR raw_commune ILIKE '%Vitacura%' THEN 'vita'
                WHEN raw_location ILIKE '%Ñuñoa%' OR raw_location ILIKE '%Nunoa%' OR raw_commune ILIKE '%Nunoa%' OR raw_commune ILIKE '%Ñuñoa%' THEN 'nuno'
                WHEN raw_location ILIKE '%La Reina%' OR raw_commune ILIKE '%La Reina%' THEN 'lare'
                WHEN raw_location ILIKE '%Lo Barnechea%' OR raw_commune ILIKE '%Lo Barnechea%' THEN 'lbar'
                WHEN raw_location ILIKE '%Peñalolén%' OR raw_commune ILIKE '%Peñalolén%' THEN 'pena'
                WHEN raw_location ILIKE '%La Florida%' OR raw_location ILIKE '%Florida%' OR raw_commune ILIKE '%Florida%' THEN 'flor'
                WHEN raw_location ILIKE '%Maipú%' OR raw_location ILIKE '%Maipu%' OR raw_commune ILIKE '%Maipu%' THEN 'maip'
                WHEN raw_location ILIKE '%San Miguel%' OR raw_commune ILIKE '%San Miguel%' THEN 'smig'
                WHEN raw_location ILIKE '%Macul%' OR raw_commune ILIKE '%Macul%' THEN 'macu'
                WHEN raw_location ILIKE '%Huechuraba%' OR raw_commune ILIKE '%Huechuraba%' THEN 'huec'
                WHEN raw_location ILIKE '%Estación Central%' OR raw_commune ILIKE '%Estación Central%' THEN 'estc'
                WHEN raw_location ILIKE '%Recoleta%' OR raw_commune ILIKE '%Recoleta%' THEN 'reco'
                WHEN raw_location ILIKE '%Pudahuel%' OR raw_commune ILIKE '%Pudahuel%' THEN 'puda'
                WHEN raw_location ILIKE '%Santiago Centro%' OR raw_commune ILIKE '%Santiago%' THEN 'stgo'
                WHEN raw_commune ILIKE '%San Pedro de la Paz%' THEN 'sped'
                WHEN raw_commune ILIKE '%Concepcion%' OR raw_commune ILIKE '%Concepción%' THEN 'conp'
                WHEN raw_commune IS NOT NULL AND raw_commune != '' THEN (
                    SELECT dl.location_id FROM dim_locations dl 
                    WHERE strip_accents(raw_commune) ILIKE '%' || strip_accents(dl.commune) || '%' 
                    AND dl.commune NOT ILIKE '%Mall%' AND dl.commune NOT ILIKE '%Mallplaza%'
                    AND dl.location_id NOT IN ('all', 'isid', 'open', 'vivo', 'brio', 'cost', 'parq', 'alto', 'mfce')
                    ORDER BY length(dl.commune) DESC LIMIT 1
                )
                ELSE (
                    SELECT dl.location_id FROM dim_locations dl 
                    WHERE raw_location ILIKE '%' || dl.commune || '%' 
                    AND dl.commune NOT ILIKE '%Mall%' AND dl.commune NOT ILIKE '%Mallplaza%'
                    AND dl.location_id NOT IN ('all', 'isid', 'open', 'vivo', 'brio', 'cost', 'parq', 'alto', 'mfce')
                    ORDER BY length(dl.commune) DESC LIMIT 1
                )
            END,
            'stgo'
        ) as location_id,
        strftime(current_timestamp, '%B') as valid_month,
        CASE
            WHEN strip_accents(COALESCE(raw_conditions, '') || ' ' || COALESCE(raw_description, '')) ILIKE '%sin tope%' 
                 OR strip_accents(COALESCE(raw_conditions, '') || ' ' || COALESCE(raw_description, '')) ILIKE '%sin topes%' 
                 OR strip_accents(COALESCE(raw_conditions, '') || ' ' || COALESCE(raw_description, '')) ILIKE '%sin tope maximo%' 
            THEN NULL
            ELSE CAST(NULLIF(regexp_replace(
                regexp_extract(strip_accents(COALESCE(raw_conditions, '') || ' ' || COALESCE(raw_description, '')), '(?i)(?:tope|hasta|maximo)[^$]*?\$([\d\.]+)', 1),
                '\.', '', 'g'
            ), '') AS INTEGER)
        END as calculated_cap,
        CASE 
            WHEN lower(valid_days_raw) = 'lunes' THEN 'Lunes'
            WHEN lower(valid_days_raw) = 'martes' THEN 'Martes'
            WHEN lower(valid_days_raw) IN ('miercoles', 'miércoles') THEN 'Miércoles'
            WHEN lower(valid_days_raw) = 'jueves' THEN 'Jueves'
            WHEN lower(valid_days_raw) = 'viernes' THEN 'Viernes'
            WHEN lower(valid_days_raw) IN ('sabado', 'sábado') THEN 'Sábado'
            WHEN lower(valid_days_raw) = 'domingo' THEN 'Domingo'
            ELSE valid_days_raw 
        END as unnested_day
    FROM exploded_itau
),
grouped_itau AS (
    SELECT 
        restaurant_id,
        title,
        raw_location as location,
        location_id,
        unnested_day as valid_days,
        calculated_cap as discount_cap,
        CAST(NULLIF(regexp_extract(discount_text, '(\d+)', 1), '') AS INTEGER) as discount_pct,
        -- Aggregate conditions to look for both Legend and Black within the same group
        string_agg(raw_conditions || ' ' || raw_description, ' | ') as combined_conditions,
        -- Pick any valid auxiliary data for the group
        MAX(scraped_at) as scraped_at,
        MAX(url) as url,
        MAX(image_url) as image_url,
        MAX(valid_month) as valid_month,
        MAX(expiration_date) as expiration_date,
        CASE
            WHEN calculated_cap IS NOT NULL THEN FALSE -- Numeric cap found -> NOT unlimited
            WHEN strip_accents(string_agg(raw_conditions || ' ' || raw_description, ' | ')) ILIKE '%sin tope%' 
                 OR strip_accents(string_agg(raw_conditions || ' ' || raw_description, ' | ')) ILIKE '%sin topes%' 
                 OR strip_accents(string_agg(raw_conditions || ' ' || raw_description, ' | ')) ILIKE '%sin tope maximo%' 
            THEN TRUE
            ELSE NULL
        END as discount_cap_is_unlimited
    FROM processed_itau
    GROUP BY 
        restaurant_id,
        title,
        raw_location,
        location_id,
        unnested_day,
        calculated_cap,
        CAST(NULLIF(regexp_extract(discount_text, '(\d+)', 1), '') AS INTEGER)
)
SELECT 
    md5(concat('itau', title, discount_pct, valid_days, location)) as offer_id,
    restaurant_id,
    'itau' as bank_id,
    -- Card type detection: look for positive mentions of Legend/Black
    -- Pattern: "Tarjetas Legend, Black y Signature" = both positive
    -- Pattern: "Tarjetas Legend... (excluye ...black...)" = Legend only (Black is excluded)
    -- We check if Black/Legend appear in "titulares de Tarjetas [list]" (positive context)
    CASE 
        WHEN regexp_matches(combined_conditions, '(?i)Tarjetas\s+(Legend[,\s]+Black|Black[,\s]+Legend)')
          OR (regexp_matches(combined_conditions, '(?i)Tarjetas\s+Legend') AND regexp_matches(combined_conditions, '(?i)Tarjetas\s+Black'))
        THEN 'itau_combined'
        WHEN regexp_matches(combined_conditions, '(?i)Tarjetas\s+Legend')
        THEN 'itau_legend'
        WHEN regexp_matches(combined_conditions, '(?i)Tarjetas\s+Black')
        THEN 'itau_black'
        ELSE 'itau_general'
    END as card_type_id,
    location_id,
    valid_days,
    discount_pct,
    discount_cap,
    discount_cap_is_unlimited,
    combined_conditions as conditions,
    valid_month,
    url as source_url,
    image_url,
    location as address,
    -- Robust parsing for expiration_date
    COALESCE(
        TRY_CAST(expiration_date AS DATE),
        CASE 
            WHEN combined_conditions IS NOT NULL AND combined_conditions ILIKE '% de %' THEN
                (
                    WITH date_parts AS (
                        SELECT 
                            regexp_extract(lower(combined_conditions), '(\d{1,2})\s+de\s+([a-z]+)\s+(?:de\s+)?(\d{4})', 1) as d,
                            regexp_extract(lower(combined_conditions), '(\d{1,2})\s+de\s+([a-z]+)\s+(?:de\s+)?(\d{4})', 2) as m_name,
                            regexp_extract(lower(combined_conditions), '(\d{1,2})\s+de\s+([a-z]+)\s+(?:de\s+)?(\d{4})', 3) as y
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
    ) as expiration_date,
    scraped_at
FROM grouped_itau p
WHERE title NOT ILIKE '%Penguin%'
  AND title NOT ILIKE '%Perry Ellis%'
  AND title NOT ILIKE '%Trial%'
ON CONFLICT (offer_id) DO NOTHING;



