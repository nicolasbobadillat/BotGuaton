DELETE FROM fact_offers WHERE bank_id = 'falabella';

WITH base_falabella AS (
    SELECT * FROM read_json_auto('{{JSON_BASE_PATH}}/falabella.json', columns={
        'title': 'VARCHAR',
        'active_days': 'VARCHAR[]',
        'discount_pct': 'VARCHAR',
        'raw_text': 'VARCHAR',
        'url': 'VARCHAR',
        'card_types': 'VARCHAR[]',
        'address': 'VARCHAR',
        'steps': 'VARCHAR',
        'location_details': 'STRUCT(location VARCHAR, days VARCHAR)[]',
        'location': 'VARCHAR',
        'validity': 'VARCHAR',
        'category': 'VARCHAR'
    })
),
exploded_falabella AS (
    -- 1. Multi-Location Offers (Structured location_details)
    SELECT 
        title, active_days, discount_pct, raw_text, url, card_types, address, steps, validity,
        CAST(loc_struct['location'] as VARCHAR) as location,  
        CASE WHEN regexp_matches(lower(CAST(loc_struct['days'] as VARCHAR)), 'lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bado|domingo|todos los|d[íi]a|fin de semana') THEN CAST(loc_struct['days'] as VARCHAR) ELSE NULL END as day_code_raw, 
        unnest(CASE WHEN regexp_matches(lower(CAST(loc_struct['days'] as VARCHAR)), 'lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bado|domingo|todos los|d[íi]a|fin de semana') THEN [NULL] WHEN len(active_days)=0 THEN CAST([NULL] AS VARCHAR[]) ELSE active_days END) as day_code_fallback,
        true as is_multi
    FROM (SELECT *, unnest(location_details) as loc_struct FROM base_falabella WHERE location_details IS NOT NULL AND len(location_details) > 0 AND CAST(location_details[1]['location'] as VARCHAR) NOT ILIKE '%Excluye%') t1
    UNION ALL
    -- 2. Multi-Location Offers (Split Address by " y ")
    SELECT title, active_days, discount_pct, raw_text, url, card_types, address, steps, validity, trim(split_loc) as location, NULL as day_code_raw, unnest(CASE WHEN len(active_days)=0 THEN CAST([NULL] AS VARCHAR[]) ELSE active_days END) as day_code_fallback, true as is_multi
    FROM (SELECT *, unnest(string_split(regexp_replace(address, '(?i) y ', ' | '), ' | ')) as split_loc FROM base_falabella WHERE address LIKE '% y %' OR address LIKE '% / %') t2
    UNION ALL
    -- 3. Multi-Location Offers (Raw Text Extraction)
    SELECT title, active_days, discount_pct, raw_text, url, card_types, address, steps, validity, trim(split_loc) as location, NULL as day_code_raw, unnest(CASE WHEN len(active_days)=0 THEN CAST([NULL] AS VARCHAR[]) ELSE active_days END) as day_code_fallback, true as is_multi
    FROM (SELECT *, unnest(regexp_extract_all(regexp_extract(raw_text, '(?is)Locales:(.*?)Condiciones:', 1), '[^\\n\\r.]+', 0)) as split_loc FROM base_falabella WHERE (location_details IS NULL OR len(location_details) = 0 OR CAST(location_details[1]['location'] as VARCHAR) ILIKE '%Excluye%') AND raw_text ILIKE '%Locales:%' AND address NOT LIKE '% y %') t3
    UNION ALL
    -- 4. Single Location Offers
    SELECT title, active_days, discount_pct, raw_text, url, card_types, address, steps, validity, location, NULL as day_code_raw, unnest(CASE WHEN len(active_days)=0 THEN CAST([NULL] AS VARCHAR[]) ELSE active_days END) as day_code_fallback, false as is_multi
    FROM base_falabella WHERE (location_details IS NULL OR len(location_details) = 0 OR CAST(location_details[1]['location'] as VARCHAR) ILIKE '%Excluye%') AND raw_text NOT ILIKE '%Locales:%' AND (address NOT LIKE '% y %' AND address NOT LIKE '% / %')
),
processed_falabella AS (
    SELECT 
        *,
        md5(upper(trim(regexp_replace(strip_accents(regexp_replace(regexp_replace(regexp_replace(title, '^Disfruta de tu beneficio en ', '', 'i'), '\s*-\s*Descuento.*| - \d+% dcto', '', 'i'), '^Restaurant(e)?\s+|\s+Restaurant(e)?$|^Pasteleria\s+|\s+Pasteleria$|^Heladeria\s+|\s+Heladeria$', '', 'i')), '[^a-zA-Z0-9]', '', 'g')))) as calculated_restaurant_id,
        CAST(NULLIF(regexp_extract(discount_pct, '(\d+)', 1), '') AS INTEGER) as calculated_discount_pct,
        -- discount_cap: robust regex on raw_text
        CASE
            WHEN strip_accents(raw_text) ILIKE '%sin tope%' 
                 OR strip_accents(raw_text) ILIKE '%sin topes%' 
                 OR strip_accents(raw_text) ILIKE '%sin tope maximo%' 
            THEN NULL
            ELSE CAST(NULLIF(regexp_replace(
                regexp_extract(strip_accents(raw_text), '(?i)(?:tope|hasta|maximo)[^$]*?\$([\d\.]+)', 1),
                '\.', '', 'g'
            ), '') AS INTEGER)
        END as calculated_cap,
        -- Parsing expiration_date from Spanish text (e.g., "28 de febrero de 2026")
        CASE 
            WHEN regexp_matches(validity, '(?i)\d+\s+de\s+[a-z]+\s+de\s+\d+') THEN
                CAST(
                    regexp_extract(validity, '(?i)(\d+)\s+de\s+([a-z]+)\s+de\s+(\d+)', 3) || '-' ||
                    CASE lower(strip_accents(regexp_extract(validity, '(?i)(\d+)\s+de\s+([a-z]+)\s+de\s+(\d+)', 2)))
                        WHEN 'enero' THEN '01'
                        WHEN 'febrero' THEN '02'
                        WHEN 'marzo' THEN '03'
                        WHEN 'abril' THEN '04'
                        WHEN 'mayo' THEN '05'
                        WHEN 'junio' THEN '06'
                        WHEN 'julio' THEN '07'
                        WHEN 'agosto' THEN '08'
                        WHEN 'septiembre' THEN '09'
                        WHEN 'octubre' THEN '10'
                        WHEN 'noviembre' THEN '11'
                        WHEN 'diciembre' THEN '12'
                    END || '-' ||
                    LPAD(regexp_extract(validity, '(?i)(\d+)\s+de\s+([a-z]+)\s+de\s+(\d+)', 1), 2, '0')
                AS DATE)
            ELSE NULL
        END as calculated_expiration_date
    FROM exploded_falabella
)
INSERT INTO fact_offers (offer_id, restaurant_id, bank_id, card_type_id, location_id, valid_days, discount_pct, discount_cap, discount_cap_is_unlimited, expiration_date, conditions, valid_month, source_url, image_url, address, scraped_at)
SELECT 
    md5(concat('falabella', title, coalesce(discount_pct, ''), coalesce(cast(card_types as varchar), ''), coalesce(day_code_fallback, ''), coalesce(location, ''), coalesce(address, ''))) as offer_id,
    calculated_restaurant_id,
    'falabella' as bank_id,
    CASE WHEN cast(card_types as varchar) LIKE '%CMR Mastercard,%' THEN 'falabella_cmr' WHEN cast(card_types as varchar) LIKE '%Elite%' THEN 'falabella_elite' WHEN cast(card_types as varchar) LIKE '%Banco Falabella%' THEN 'falabella_cmr' WHEN title ILIKE '%Mamut%' THEN 'falabella_cmr' ELSE 'falabella_elite' END as card_type_id,
    COALESCE(CASE WHEN is_multi THEN (SELECT dl.location_id FROM dim_locations dl WHERE strip_accents(coalesce(location, '')) ILIKE '%' || strip_accents(dl.commune) || '%' OR strip_accents(dl.commune) ILIKE '%' || strip_accents(coalesce(location, '')) || '%' ORDER BY length(dl.commune) DESC, CASE WHEN dl.location_id = 'brio' THEN 0 ELSE 1 END ASC LIMIT 1) END, CASE WHEN regexp_matches(upper(strip_accents(title)), '(MCDONALD|KFC|JUAN MAESTRO|CHINA WOK|WENDY|DOMINO|DUNKIN|SUSHI BLUES|VOLKA|YOGEN FRUZ|MELT|BURGER KING|DOGGIS|PAPA JOHN|PIZZA HUT|SUBWAY|TARRAGONA|LITTLE CAESAR|LE VICE|MAMUT)') THEN 'all' WHEN regexp_matches(upper(strip_accents(title)), 'VINA') THEN 'vina' WHEN regexp_matches(upper(strip_accents(title)), 'VALPARAISO') THEN 'valp' WHEN regexp_matches(upper(strip_accents(title)), 'OSORNO') THEN 'osor' WHEN regexp_matches(upper(strip_accents(title)), 'RANCAGUA') THEN 'ranc' WHEN regexp_matches(upper(strip_accents(title)), 'CONCON') THEN 'conc' WHEN regexp_matches(upper(strip_accents(title)), 'TEMUCO') THEN 'temu' WHEN regexp_matches(upper(strip_accents(title)), 'ANTOFAGASTA') THEN 'anto' END, (SELECT dl.location_id FROM dim_locations dl WHERE strip_accents(coalesce(address, '') || ' ' || coalesce(steps, '') || ' ' || coalesce(title, '')) ILIKE '%' || strip_accents(dl.commune) || '%' OR strip_accents(dl.commune) ILIKE '%' || strip_accents(coalesce(address, '') || ' ' || coalesce(steps, '') || ' ' || coalesce(title, '')) || '%' ORDER BY length(dl.commune) DESC, CASE WHEN dl.location_id = 'brio' THEN 0 ELSE 1 END ASC LIMIT 1), CASE WHEN location ILIKE '%Metropolitana%' THEN 'stgo' WHEN location ILIKE '%Valparaíso%' THEN 'valp' WHEN location ILIKE '%Antofagasta%' THEN 'anto' WHEN location ILIKE '%Concepción%' OR location ILIKE '%Biobío%' THEN 'conp' WHEN location ILIKE '%Araucanía%' THEN 'temu' WHEN location ILIKE '%Los Lagos%' THEN 'pmon' WHEN location ILIKE '%O''Higgins%' THEN 'ranc' ELSE 'stgo' END) as location_id,
    CASE WHEN day_code_fallback = 'LU' THEN 'Lunes' WHEN day_code_fallback = 'MA' THEN 'Martes' WHEN day_code_fallback = 'MI' THEN 'Miércoles' WHEN day_code_fallback = 'JU' THEN 'Jueves' WHEN day_code_fallback = 'VI' THEN 'Viernes' WHEN day_code_fallback = 'SA' THEN 'Sábado' WHEN day_code_fallback = 'DO' THEN 'Domingo' WHEN day_code_raw IS NOT NULL AND day_code_raw != '' THEN day_code_raw WHEN len(active_days)>0 THEN active_days[1] ELSE 'Todos los días' END as valid_days,
    calculated_discount_pct as discount_pct,
    calculated_cap as discount_cap,
    CASE WHEN strip_accents(raw_text) ILIKE '%sin tope%' OR strip_accents(raw_text) ILIKE '%sin topes%' OR strip_accents(raw_text) ILIKE '%sin tope maximo%' THEN TRUE WHEN calculated_cap IS NOT NULL THEN FALSE ELSE NULL END as discount_cap_is_unlimited,
    calculated_expiration_date as expiration_date,
    split_part(raw_text, 'Conoce todos nuestros beneficios aquí', 1) as conditions,
    strftime(current_timestamp, '%B') as valid_month,
    url as source_url,
    NULL as image_url,
    CASE WHEN title ILIKE '%Mamut%' THEN 'Todos los locales' ELSE address END as address,
    current_timestamp as scraped_at
FROM processed_falabella
WHERE calculated_discount_pct > 0
  AND title NOT ILIKE '%Especial Verano%' AND title NOT ILIKE '%Especial Invierno%' AND title NOT ILIKE '%Experiencia Verano%'
  AND title NOT ILIKE '%ecook%'
  AND title NOT ILIKE '%Paga tu pasaje%'
ON CONFLICT (offer_id) DO NOTHING;



