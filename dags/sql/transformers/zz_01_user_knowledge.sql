-- =============================================================================
-- SCRAPER PATCHES: Datos que están en las webs pero los scrapers no extraen bien
-- Los scrapers visitan las páginas pero fallan al extraer location_details,
-- días específicos por sucursal, o datos de páginas de detalle.
-- Este archivo compensa esas limitaciones hasta que se arreglen los scrapers.
-- Se ejecuta DESPUÉS de todos los transformers de banco.
-- =============================================================================

-- 0. Limpiar inserciones manuales anteriores (evita datos stale)
DELETE FROM fact_offers WHERE conditions = 'Manual Insert';

-- 1. Asegurar que existen las ubicaciones necesarias
INSERT INTO dim_locations (location_id, commune, region) VALUES
    ('mpno', 'Mall Plaza Norte', 'Metropolitana'),
    ('mpto', 'Mall Plaza Tobalaba', 'Metropolitana'),
    ('mmai', 'Mall Plaza Maipu', 'Metropolitana'),
    ('isid', 'Isidora 3000', 'Metropolitana'),
    ('vimp', 'Vivo Imperio', 'Metropolitana'),
    ('pdeh', 'Portal La Dehesa', 'Metropolitana'),
    ('pbel', 'Patio Bellavista', 'Metropolitana'),
    ('coch', 'Cochoa', 'Valparaíso')
ON CONFLICT DO NOTHING;

-- 2. Asegurar que existen los tipos de tarjeta
INSERT INTO dim_card_types (card_type_id, bank_id, card_name, tier)
VALUES ('falabella_cmr', 'falabella', 'CMR', 1)
ON CONFLICT DO NOTHING;

-- 3. Asegurar que los restaurantes existen en dim_restaurants
INSERT INTO dim_restaurants (restaurant_id, name, normalized_name, category) VALUES
    (md5('TIGRE BRAVO'), 'Tigre Bravo', 'TIGRE BRAVO', 'Restaurante'),
    (md5('VAPIANO'), 'Vapiano', 'VAPIANO', 'Restaurante'),
    (md5('LA PIAZZA'), 'La Piazza', 'LA PIAZZA', 'Restaurante'),
    (md5('PISTACHO'), 'Pistacho', 'PISTACHO', 'Restaurante'),
    (md5('LA CAPERUCITA Y EL LOBO'), 'La Caperucita y el Lobo', 'LA CAPERUCITA Y EL LOBO', 'Restaurante'),
    (md5('CAOBA BAR'), 'Caoba Bar', 'CAOBA BAR', 'Restaurante'),
    (md5('SILOS'), 'Silos', 'SILOS', 'Restaurante'),
    (md5('DOMINGA BISTRO'), 'Dominga Bistro', 'DOMINGA BISTRO', 'Restaurante'),
    (md5('LA SANTORIA'), 'La Santoria', 'LA SANTORIA', 'Restaurante'),
    (md5('MONACO SPORT BAR'), 'Monaco Sport Bar', 'MONACO SPORT BAR', 'Restaurante'),
    (md5('MUU GRILL'), 'Muu grill', 'MUU GRILL', 'Restaurante'),
    (md5('THE TEARAPY HOUSE'), 'The Tearapy House', 'THE TEARAPY HOUSE', 'Restaurante')
ON CONFLICT DO NOTHING;

-- 4. Insertar ofertas del usuario
--    Estas ofertas complementan lo que el scraper captura (días/ubicaciones extra)
-- INSERT INTO fact_offers (offer_id, restaurant_id, bank_id, card_type_id, location_id, valid_days, discount_pct, address, conditions, valid_month, scraped_at)
--    -- End of manual offers
--
-- ON CONFLICT (offer_id) DO UPDATE SET
--     valid_days = excluded.valid_days,
--     address = excluded.address,
--     bank_id = excluded.bank_id,
--     card_type_id = excluded.card_type_id,
--     location_id = excluded.location_id,
--     discount_pct = excluded.discount_pct;

-- 5. Set custom category overrides

-- 6. Instagram handles (poblado manualmente, automatizable después)
UPDATE dim_restaurants SET instagram = '@domingabistro' WHERE normalized_name = 'DOMINGA BISTRO';
UPDATE dim_restaurants SET instagram = '@tigrebravocl' WHERE normalized_name = 'TIGRE BRAVO';
UPDATE dim_restaurants SET instagram = '@vapianocl' WHERE normalized_name = 'VAPIANO';
UPDATE dim_restaurants SET instagram = '@muugrillcl' WHERE normalized_name = 'MUU GRILL';
UPDATE dim_restaurants SET instagram = '@caobabarcl' WHERE normalized_name = 'CAOBA BAR';
UPDATE dim_restaurants SET instagram = '@pistachocl' WHERE normalized_name = 'PISTACHO';
