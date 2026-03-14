-- =============================================================================
-- 13. GLOBAL NORMALIZER: valid_days
-- =============================================================================
-- Normalizes the valid_days field to a strict set of values and removes
-- garbage data (like addresses, floors, or full conditions) that slipped through
-- the initial scraper extraction or earlier transformers.

UPDATE fact_offers
SET valid_days = CASE
    -- 1. Exact day string normalization (accents and casing)
    WHEN lower(valid_days) IN ('lunes', 'todos los lunes') THEN 'Lunes'
    WHEN lower(valid_days) IN ('martes', 'todos los martes') THEN 'Martes'
    WHEN lower(valid_days) IN ('miercoles', 'miércoles', 'todos los miercoles', 'todos los miércoles') THEN 'Miércoles'
    WHEN lower(valid_days) IN ('jueves', 'todos los jueves') THEN 'Jueves'
    WHEN lower(valid_days) IN ('viernes', 'todos los viernes') THEN 'Viernes'
    WHEN lower(valid_days) IN ('sabado', 'sábado', 'todos los sabado', 'todos los sábado', 'todos los sabados', 'todos los sábados') THEN 'Sábado'
    WHEN lower(valid_days) IN ('domingo', 'todos los domingo', 'todos los domingos') THEN 'Domingo'

    -- 2. "Todos los días" variations
    WHEN lower(valid_days) LIKE '%todos los d%' OR lower(valid_days) LIKE '%lunes a domingo%' THEN 'Todos los días'

    -- 3. Common ranges (Case Insensitive)
    WHEN lower(valid_days) LIKE '%lunes a jueves y domingo%' THEN 'Lunes a Jueves y Domingo'
    WHEN lower(valid_days) LIKE '%lunes a jueves%' THEN 'Lunes a Jueves'
    WHEN lower(valid_days) LIKE '%lunes a mi_rcoles%' THEN 'Lunes a Miércoles'
    WHEN lower(valid_days) LIKE '%lunes a viernes%' THEN 'Lunes a Viernes'
    WHEN lower(valid_days) LIKE '%martes a s_bado%' THEN 'Martes a Sábado'
    WHEN lower(valid_days) LIKE '%domingo a mi_rcoles%' THEN 'Domingo a Miércoles'
    WHEN lower(valid_days) LIKE '%mi_rcoles a s_bado%' THEN 'Miércoles a Sábado'

    -- 4. Specific combinations (Pairs and Lists)
    WHEN lower(valid_days) LIKE '%s_bado y domingo%' THEN 'Sábado y Domingo'
    WHEN lower(valid_days) LIKE '%viernes y s_bado%' THEN 'Viernes y Sábado'
    WHEN lower(valid_days) LIKE '%lunes y martes%' THEN 'Lunes y Martes'
    WHEN lower(valid_days) LIKE '%lunes y jueves%' THEN 'Lunes y Jueves'
    WHEN lower(valid_days) LIKE '%lunes y mi_rcoles%' THEN 'Lunes y Miércoles'
    WHEN lower(valid_days) LIKE '%martes, viernes y s_bado%' THEN 'Martes, Viernes y Sábado'
    WHEN lower(valid_days) LIKE '%mi_rcoles, jueves y s_bado%' THEN 'Miércoles, Jueves y Sábado'
    
    -- 5. Dash separated (Vi-Sá-Do)
    WHEN lower(valid_days) LIKE '%vi-s_-%' THEN 'Viernes a Domingo'
    WHEN lower(valid_days) LIKE '%ju-mi-vi%' THEN 'Jueves, Miércoles y Viernes'

    -- 6. Garbage protection (Addresses and complex strings NOT matching days above)
    -- We skip digits if it looks like a time (19:00), but otherwise complex strings with numbers are usually garbage.
    WHEN regexp_matches(lower(valid_days), '\d') AND NOT regexp_matches(lower(valid_days), '\d{2}:\d{2}') THEN 'Consultar'
    WHEN regexp_matches(lower(valid_days), '(av\.|calle|n°|nro|piso|local|mall|plaza|sucursal)') THEN 'Consultar'

    -- 7. Capitalize first letter of anything else that survived the filters
    ELSE upper(substring(valid_days, 1, 1)) || lower(substring(valid_days, 2))
END;
