-- =============================================================================
-- 10. POBLAR fact_offers - BANCO DE CHILE
-- =============================================================================

-- BCH locations: "Address, Region - Commune" (commune after last dash)
-- Add missing communes that BCH uses
INSERT INTO dim_locations (location_id, commune, region) VALUES
    ('ldeh', 'La Dehesa', 'Metropolitana'),
    ('lher', 'La Herradura', 'Coquimbo')
ON CONFLICT DO NOTHING;

DELETE FROM fact_offers WHERE bank_id = 'bancochile';

INSERT INTO fact_offers (offer_id, restaurant_id, bank_id, card_type_id, location_id, valid_days, discount_pct, discount_cap, discount_cap_is_unlimited, expiration_date, conditions, valid_month, source_url, image_url, address, scraped_at)
WITH raw_bch AS (
    SELECT 
        *,
        COALESCE(NULLIF(regexp_replace(regexp_replace(title, 'Ari Nikkei Lo Barnechea', 'Ari Nikkei', 'i'), 'Domingo Bistro exclusivo.*', 'Dominga Bistro', 'i'), ''), title) as clean_title,
        -- Extract commune: text after the last " - " in the location field
        trim(regexp_extract(location, '.*-\s*(.+)$', 1)) as bch_commune
    FROM read_json_auto('{{JSON_BASE_PATH}}/bancochile.json')
),
exploded_bch AS (
    SELECT 
        *,
        unnest(
            CASE 
                WHEN active_days IS NOT NULL AND len(active_days) > 0 THEN active_days 
                WHEN lower(recurrence) ILIKE '%todos los d_as%' THEN ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
                WHEN lower(recurrence) ILIKE '%lunes a viernes%' THEN ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes']
                ELSE 
                    CASE 
                        WHEN len(regexp_extract_all(lower(recurrence), '(lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bado|domingo)')) > 0 
                        THEN regexp_extract_all(lower(recurrence), '(lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bado|domingo)')
                        ELSE CAST([NULL] AS VARCHAR[])
                    END
            END
        ) as day_exploded
    FROM raw_bch
),
processed_bch AS (
    SELECT 
        *,
        -- discount_cap: prefer JSON field, fallback to regex on conditions
        CASE
            WHEN strip_accents(conditions) ILIKE '%sin tope%' 
                 OR strip_accents(conditions) ILIKE '%sin topes%' 
                 OR strip_accents(conditions) ILIKE '%sin tope maximo%' 
            THEN NULL
            ELSE COALESCE(
                TRY_CAST(discount_cap AS INTEGER),
                CAST(NULLIF(regexp_replace(
                    regexp_extract(strip_accents(conditions), '(?i)(?:tope|hasta|maximo)[^$]*?\$([\d\.]+)', 1),
                    '\.', '', 'g'
                ), '') AS INTEGER)
            )
        END as calculated_cap,
        COALESCE(
            CASE 
                WHEN clean_title ILIKE '%Entre Rios%' THEN 'reng'
                WHEN clean_title ILIKE '%Golf Los Lirios%' THEN 'requ'
                WHEN clean_title ILIKE '%Dominga%' THEN 'vald'
                WHEN clean_title ILIKE '%Anima%' THEN 'prov'
                WHEN clean_title ILIKE '%Pristino%' THEN 'vita'
                WHEN regexp_matches(upper(strip_accents(title)), '(MCDONALD|KFC|JUAN MAESTRO|CHINA WOK|WENDY|DOMINO|DUNKIN|SUSHI BLUES|VOLKA|YOGEN FRUZ|MELT|BURGER KING|DOGGIS|PAPA JOHN|PIZZA HUT|SUBWAY|TARRAGONA|LITTLE CAESAR|LE VICE|STARBUCKS|CARL|TOMMY|TACO BELL|VELVET BAKERY|BURGERBEEF|HAPPY BOX)') THEN 'all'
                WHEN (SELECT dl.location_id FROM dim_locations dl WHERE strip_accents(lower(trim(bch_commune))) = strip_accents(lower(dl.commune)) LIMIT 1) IS NOT NULL
                THEN (SELECT dl.location_id FROM dim_locations dl WHERE strip_accents(lower(trim(bch_commune))) = strip_accents(lower(dl.commune)) LIMIT 1)
                ELSE (SELECT dl.location_id FROM dim_locations dl WHERE strip_accents(lower(location)) ILIKE concat('%', strip_accents(lower(dl.commune)), '%') LIMIT 1)
            END,
            'stgo'
        ) as calculated_location_id,
        COALESCE(
            CAST(NULLIF(regexp_extract(discount_text, '(\d+)', 1), '') AS INTEGER),
            CAST(NULLIF(regexp_extract(recurrence, '(\d+)%', 1), '') AS INTEGER),
            CASE 
                WHEN conditions ILIKE '%Visa Infinite%' THEN 50
                WHEN conditions ILIKE '%Visa Signature%' OR conditions ILIKE '%Visa Platinum%' THEN 40
                ELSE 0
            END
        ) as calculated_discount_pct
    FROM exploded_bch
)
SELECT 
    md5(concat('bancochile', clean_title, discount_text, conditions, location, day_exploded)) as offer_id,
    md5(upper(trim(regexp_replace(strip_accents(regexp_replace(regexp_replace(regexp_replace(title, '^Disfruta de tu beneficio en ', '', 'i'), '\s*-\s*Descuento.*| - \d+% dcto', '', 'i'), '^Restaurant(e)?\s+|\s+Restaurant(e)?$|^Pasteleria\s+|\s+Pasteleria$|^Heladeria\s+|\s+Heladeria$', '', 'i')), '[^a-zA-Z0-9]', '', 'g')))) as restaurant_id,
    'bancochile' as bank_id,
    CASE 
        WHEN conditions ILIKE '%Visa Infinite%' THEN 'bancochile_infinite'
        WHEN conditions ILIKE '%Visa Signature%' OR conditions ILIKE '%Visa Platinum%' THEN 'bancochile_visa'
        ELSE 'bancochile_general'
    END as card_type_id,
    calculated_location_id as location_id,
    COALESCE(
        CASE 
            WHEN clean_title ILIKE '%Cocoa%' THEN 'Jueves'
            WHEN clean_title ILIKE '%Dominga%' THEN 'Lunes'
            WHEN lower(day_exploded) = 'lunes' THEN 'Lunes'
            WHEN lower(day_exploded) = 'martes' THEN 'Martes'
            WHEN lower(day_exploded) IN ('miercoles', 'miércoles') THEN 'Miércoles'
            WHEN lower(day_exploded) = 'jueves' THEN 'Jueves'
            WHEN lower(day_exploded) = 'viernes' THEN 'Viernes'
            WHEN lower(day_exploded) IN ('sabado', 'sábado') THEN 'Sábado'
            WHEN lower(day_exploded) = 'domingo' THEN 'Domingo'
            ELSE day_exploded 
        END, 
        'Todos los días'
    ) as valid_days,
    calculated_discount_pct as discount_pct,
    calculated_cap as discount_cap,
    CASE
        WHEN calculated_cap IS NOT NULL THEN FALSE -- Numeric cap found -> NOT unlimited
        WHEN strip_accents(conditions) ILIKE '%sin tope%' 
             OR strip_accents(conditions) ILIKE '%sin topes%' 
             OR strip_accents(conditions) ILIKE '%sin tope maximo%' 
        THEN TRUE
        ELSE NULL
    END as discount_cap_is_unlimited,
    expiration_date::DATE as expiration_date,
    conditions,
    strftime(current_timestamp, '%B') as valid_month,
    url as source_url,
    NULL as image_url,
    location as address,
    scraped_at
FROM processed_bch
WHERE calculated_discount_pct > 0
  AND title NOT ILIKE '%ecook%'
ON CONFLICT (offer_id) DO NOTHING;



