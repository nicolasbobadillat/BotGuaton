-- =============================================================================
-- 5. POBLAR fact_offers - SCOTIABANK
-- =============================================================================

-- Add communes that Scotiabank uses but are missing from schema
INSERT INTO dim_locations (location_id, commune, region) VALUES
    ('lran', 'Lago Ranco', 'Los Ríos'),
    ('land', 'Los Andes', 'Valparaíso'),
    ('frut', 'Frutillar', 'Los Lagos'),
    ('pich', 'Pichilemu', 'O''Higgins'),
    ('sfel', 'San Felipe', 'Valparaíso'),
    ('mait', 'Maitencillo', 'Valparaíso'),
    ('quil', 'Quillota', 'Valparaíso'),
    ('futr', 'Futrono', 'Los Ríos'),
    ('pefl', 'Peñaflor', 'Metropolitana'),
    ('algr', 'Algarrobo', 'Valparaíso'),
    ('scru', 'Santa Cruz', 'O''Higgins'),
    ('cach', 'Cachagua', 'Valparaíso')
ON CONFLICT DO NOTHING;

-- Add Scotiabank Premium card type
INSERT INTO dim_card_types (card_type_id, bank_id, card_name, tier) VALUES
    ('scotiabank_premium', 'scotiabank', 'Signature/Infinite/Wealth', 3)
ON CONFLICT DO NOTHING;

DELETE FROM fact_offers WHERE bank_id = 'scotiabank';

INSERT INTO fact_offers (offer_id, restaurant_id, bank_id, card_type_id, location_id, valid_days, discount_pct, discount_cap, discount_cap_is_unlimited, conditions, valid_month, source_url, image_url, address, scraped_at, expiration_date)
WITH raw_scotia AS (
    SELECT *,
        unnest(string_split(location, ' | ')) as unnested_location, 
        -- Extract commune from the unnested location
        regexp_replace(
            trim(regexp_extract(split_part(unnest(string_split(location, ' | ')), chr(10), 1), ',\s*([^,]+)$', 1)),
            '\.\s*$', '', 'g'
        ) as scotia_commune
    FROM read_json_auto('{{JSON_BASE_PATH}}/scotiabank.json', columns={
        'title': 'VARCHAR',
        'discount_text': 'VARCHAR',
        'description': 'VARCHAR',
        'location': 'VARCHAR',
        'url': 'VARCHAR',
        'conditions': 'VARCHAR',
        'scraped_at': 'VARCHAR',
        'category': 'VARCHAR',
        'discount_cap': 'VARCHAR',
        'image_url': 'VARCHAR',
        'expiration_date': 'VARCHAR'
    })
),
exploded_scotia AS (
    SELECT 
        *,
        unnest(
            CASE 
                WHEN lower(description) ILIKE '%todos los d_as%' OR lower(description) ILIKE '%todos los día%' OR lower(description) ILIKE '%lunes a domingo%' THEN ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
                WHEN lower(description) ILIKE '%lunes a viernes%' THEN ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes']
                WHEN lower(description) ILIKE '%lunes a s_bado%' THEN ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado']
                WHEN lower(description) ILIKE '%lunes a jueves%' THEN ['Lunes', 'Martes', 'Miércoles', 'Jueves']
                WHEN lower(description) ILIKE '%martes a s_bado%' THEN ['Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado']
                WHEN lower(description) ILIKE '%domingo a mi_rcoles%' THEN ['Domingo', 'Lunes', 'Martes', 'Miércoles']
                WHEN len(regexp_extract_all(lower(description), '(lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bados?|domingos?)')) > 0 
                THEN apply(
                    regexp_extract_all(lower(description), '(lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bados?|domingos?)'),
                    x -> CASE 
                        WHEN x = 'lunes' THEN 'Lunes'
                        WHEN x = 'martes' THEN 'Martes'
                        WHEN x IN ('miercoles', 'miércoles') THEN 'Miércoles'
                        WHEN x = 'jueves' THEN 'Jueves'
                        WHEN x = 'viernes' THEN 'Viernes'
                        WHEN x IN ('sabado', 'sábado', 'sabados', 'sábados') THEN 'Sábado'
                        WHEN x IN ('domingo', 'domingos') THEN 'Domingo'
                        ELSE x
                    END
                )
                ELSE ['Todos los días']
            END
        ) as unnested_day
    FROM raw_scotia
),
processed_scotia AS (
    SELECT 
        *,
        md5(upper(trim(regexp_replace(strip_accents(regexp_replace(regexp_replace(regexp_replace(title, '^Disfruta de tu beneficio en ', '', 'i'), '\s*-\s*Descuento.*| - \d+% dcto', '', 'i'), '^Restaurant(e)?\s+|\s+Restaurant(e)?$|^Pasteleria\s+|\s+Pasteleria$|^Heladeria\s+|\s+Heladeria$', '', 'i')), '[^a-zA-Z0-9]', '', 'g')))) as calculated_restaurant_id,
        -- discount_cap: from JSON field with regex fallback
        CASE
            WHEN strip_accents(description) ILIKE '%sin tope%' 
                 OR strip_accents(description) ILIKE '%sin topes%' 
                 OR strip_accents(description) ILIKE '%sin tope maximo%' 
            THEN NULL
            ELSE COALESCE(
                TRY_CAST(discount_cap AS INTEGER),
                CAST(NULLIF(regexp_replace(
                    regexp_extract(strip_accents(description), '(?i)(?:tope|hasta|maximo)[^$]*?\$([\d\.]+)', 1),
                    '\.', '', 'g'
                ), '') AS INTEGER)
            )
        END as calculated_cap,
        COALESCE(
            CASE 
                WHEN regexp_matches(upper(strip_accents(title)), '(MCDONALD|KFC|JUAN MAESTRO|CHINA WOK|WENDY|DOMINO|DUNKIN|SUSHI BLUES|VOLKA|YOGEN FRUZ|MELT|BURGER KING|DOGGIS|PAPA JOHN|PIZZA HUT|SUBWAY|TARRAGONA|LITTLE CAESAR|LE VICE)') THEN 'all'
                WHEN (SELECT dl.location_id FROM dim_locations dl WHERE strip_accents(lower(trim(scotia_commune))) = strip_accents(lower(dl.commune)) LIMIT 1) IS NOT NULL
                THEN (SELECT dl.location_id FROM dim_locations dl WHERE strip_accents(lower(trim(scotia_commune))) = strip_accents(lower(dl.commune)) LIMIT 1)
                WHEN lower(trim(scotia_commune)) = 'santiago centro' THEN 'stgo'
                ELSE (SELECT dl.location_id FROM dim_locations dl WHERE strip_accents(lower(unnested_location)) ILIKE concat('%', strip_accents(lower(dl.commune)), '%') LIMIT 1)
            END,
            'stgo'
        ) as calculated_location_id,
        CASE 
            WHEN discount_text = 'Ver detalle' THEN CAST(NULLIF(regexp_extract(description, '(\d+)%', 1), '') AS INTEGER)
            ELSE CAST(NULLIF(regexp_extract(discount_text, '(\d+)', 1), '') AS INTEGER)
        END as calculated_discount_pct,
        -- Robust parsing for expiration_date
        COALESCE(
            TRY_CAST(expiration_date AS DATE),
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
                ELSE NULL
            END
        ) as calculated_expiration_date
    FROM exploded_scotia
)
SELECT 
    md5(concat('scotiabank', title, discount_text, description, trim(unnested_location), COALESCE(unnested_day, ''))) as offer_id,
    calculated_restaurant_id as restaurant_id,
    'scotiabank' as bank_id,
    CASE 
        WHEN description ILIKE '%Signature%' OR description ILIKE '%Infinite%' OR description ILIKE '%Singular%' OR description ILIKE '%Wealth%' THEN 'scotiabank_premium'
        ELSE 'scotiabank_general'
    END as card_type_id,
    calculated_location_id as location_id,
    unnested_day as valid_days,
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
    unnested_location as address,
    scraped_at,
    calculated_expiration_date as expiration_date
FROM processed_scotia
WHERE COALESCE(calculated_discount_pct, 0) > 0
  AND title NOT ILIKE '%ecook%'
  AND length(trim(title)) > 2
ON CONFLICT (offer_id) DO NOTHING;


