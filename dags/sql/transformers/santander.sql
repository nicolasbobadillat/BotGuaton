DELETE FROM fact_offers WHERE bank_id = 'santander';

INSERT INTO fact_offers (offer_id, restaurant_id, bank_id, card_type_id, location_id, valid_days, discount_pct, discount_cap, discount_cap_is_unlimited, conditions, valid_month, source_url, image_url, address, expiration_date, scraped_at)

WITH raw_data AS (
    SELECT 
        title, 
        discount_text,
        discount_pct as json_discount_pct,
        conditions, 
        description, 
        location,
        recurrence,
        card_type,
        discount_cap as json_discount_cap,
        discount_cap_is_unlimited as json_cap_unlimited,
        expiration_date,
        url,
        image_url,
        scraped_at,
        current_timestamp as now
    FROM read_json_auto('{{JSON_BASE_PATH}}/santander.json')
    WHERE title IS NOT NULL 
      AND title NOT ILIKE '%ecook%'
),
base_data AS (
    SELECT 
        *,
        -- discount_pct: prefer from JSON (scraper already extracted from Bajada externa)
        COALESCE(
            json_discount_pct,
            CAST(NULLIF(regexp_extract(discount_text, '(\d+)', 1), '') AS INTEGER)
        ) as calculated_discount_pct,
        -- card_type_id mapping
        CASE
            WHEN card_type = 'amex' THEN 'santander_amex'
            WHEN card_type = 'limited' THEN 'santander_limited'
            WHEN card_type = 'credito_debito' THEN 'santander_credito_debito'
            WHEN description ILIKE '%WorldMember%' OR description ILIKE '%Limited%' THEN 'santander_limited'
            WHEN description ILIKE '%American Express%' OR description ILIKE '%Amex%' THEN 'santander_amex'
            WHEN description ILIKE '%D_bito%' OR description ILIKE '%Debito%' THEN 'santander_credito_debito'
            ELSE 'santander_general'
        END as calculated_card_type
    FROM raw_data
    WHERE COALESCE(
        json_discount_pct,
        CAST(NULLIF(regexp_extract(discount_text, '(\d+)', 1), '') AS INTEGER)
    ) > 0
),
-- Location matching:
-- 1. Split comma-separated location field → join each commune token against dim_locations (proper multi-commune expansion)
-- 2. Fallback: description LIMIT 1 (avoid cartesian)
-- 3. Chain detection → 'all'
-- 4. Fallback → 'stgo'
location_split AS (
    SELECT r.*, trim(unnest(string_split(r.location, ','))) as loc_token
    FROM base_data r
    WHERE r.location IS NOT NULL AND r.location != '' AND r.location != 'Varios'
),
matches_location AS (
    SELECT ls.*, dl.location_id
    FROM location_split ls
    JOIN dim_locations dl ON strip_accents(lower(ls.loc_token)) = strip_accents(lower(dl.commune))
),
-- Rows that didn't match via location field (no location, or no matches)
unmatched AS (
    SELECT r.*
    FROM base_data r
    WHERE r.title NOT IN (SELECT DISTINCT title FROM matches_location)
),
matches_fallback AS (
    SELECT u.*,
        COALESCE(
            -- description-based match (LIMIT 1 to avoid cartesian)
            (SELECT dl.location_id FROM dim_locations dl
             WHERE u.description ILIKE '%' || dl.commune || '%'
             ORDER BY length(dl.commune) DESC LIMIT 1),
            -- Chain detection → 'all'
            CASE WHEN regexp_matches(upper(strip_accents(u.title)), '(MCDONALD|KFC|JUAN MAESTRO|CHINA WOK|WENDY|DOMINO|DUNKIN|SUSHI BLUES|VOLKA|YOGEN FRUZ|MELT|BURGER KING|DOGGIS|PAPA JOHN|PIZZA HUT|SUBWAY|TARRAGONA|LITTLE CAESAR|LE VICE)') THEN 'all' END,
            -- Fallback
            'stgo'
        ) as location_id
    FROM unmatched u
),
combined AS (
    SELECT title, discount_text, json_discount_pct, conditions, description, location,
           recurrence, card_type, json_discount_cap, json_cap_unlimited, expiration_date,
           url, image_url, scraped_at, now, calculated_discount_pct, calculated_card_type,
           location_id
    FROM matches_location
    UNION ALL
    SELECT * FROM matches_fallback
),
processed_data AS (
    SELECT 
        *,
        -- expiration_date logic: 'Hasta el 31 de marzo de 2026' -> '2026-03-31'
        CASE 
            WHEN expiration_date IS NULL OR expiration_date = '' THEN NULL
            ELSE 
               TRY_CAST(
                regexp_extract(expiration_date, '(\d{4})', 1) || '-' ||
                CASE 
                    WHEN lower(expiration_date) ILIKE '%enero%' THEN '01'
                    WHEN lower(expiration_date) ILIKE '%febrero%' THEN '02'
                    WHEN lower(expiration_date) ILIKE '%marzo%' THEN '03'
                    WHEN lower(expiration_date) ILIKE '%abril%' THEN '04'
                    WHEN lower(expiration_date) ILIKE '%mayo%' THEN '05'
                    WHEN lower(expiration_date) ILIKE '%junio%' THEN '06'
                    WHEN lower(expiration_date) ILIKE '%julio%' THEN '07'
                    WHEN lower(expiration_date) ILIKE '%agosto%' THEN '08'
                    WHEN lower(expiration_date) ILIKE '%septiembre%' THEN '09'
                    WHEN lower(expiration_date) ILIKE '%octubre%' THEN '10'
                    WHEN lower(expiration_date) ILIKE '%noviembre%' THEN '11'
                    WHEN lower(expiration_date) ILIKE '%diciembre%' THEN '12'
                    ELSE '01'
                END || '-' ||
                LPAD(regexp_extract(expiration_date, '(\d{1,2})', 1), 2, '0')
               AS DATE)
        END as parsed_expiration_date
    FROM combined
)
SELECT 
    md5(concat('santander', title, coalesce(discount_text, ''), coalesce(cast(calculated_card_type as varchar), ''), location_id, recurrence)) as offer_id,
    md5(upper(trim(regexp_replace(strip_accents(regexp_replace(regexp_replace(regexp_replace(CASE WHEN title = '1213' THEN 'Doce Trece' ELSE title END, '^Disfruta de tu beneficio en ', '', 'i'), '\s*-\s*Descuento.*| - \d+% dcto', '', 'i'), '^Restaurant(e)?\s+|\s+Restaurant(e)?$|^Pasteleria\s+|\s+Pasteleria$|^Heladeria\s+|\s+Heladeria$', '', 'i')), '[^a-zA-Z0-9]', '', 'g')))) as restaurant_id,
    'santander' as bank_id,
    calculated_card_type as card_type_id,
    location_id,
    recurrence as valid_days,
    calculated_discount_pct as discount_pct,
    json_discount_cap as discount_cap,
    json_cap_unlimited as discount_cap_is_unlimited,
    discount_text as conditions,
    strftime(current_timestamp, '%B') as valid_month,
    url as source_url,
    image_url,
    location as address,
    parsed_expiration_date as expiration_date,
    now as scraped_at
FROM processed_data
ON CONFLICT (offer_id) DO NOTHING;



