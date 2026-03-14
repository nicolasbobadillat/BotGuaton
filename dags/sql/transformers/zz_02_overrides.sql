-- =============================================================================
-- OVERRIDES: Correcciones a datos del scraper
-- Arregla locations, card types, días, y limpia ofertas expiradas/duplicadas.
-- Se ejecuta DESPUÉS de zz_01 (scraper patches).
-- =============================================================================

-- =============================================================================
-- BICE
-- =============================================================================

-- =============================================================================
-- TEMPORARY MONTHLY OVERRIDES (Self-Cleaning)
-- =============================================================================

-- Santander: Ciros (Expires explicitly on 2026-03-31, but only applies during March 2026)
-- If the current date is in April 2026 or later, this override naturally stops executing.
UPDATE fact_offers 
SET expiration_date = '2026-03-31'
WHERE bank_id = 'santander' 
  AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Ciros%')
  AND CURRENT_DATE <= DATE '2026-03-31';

-- Eliminar entradas genéricas 'Consultar'
DELETE FROM fact_offers
WHERE bank_id = 'bice' AND valid_days = 'Consultar'
AND restaurant_id IN (
    SELECT restaurant_id FROM dim_restaurants
    WHERE name ILIKE '%Aurea%' OR name ILIKE '%Pescados Capitales%' OR name ILIKE '%Maadaam%'
);

-- Carnivo -> Temuco
UPDATE fact_offers SET location_id = 'temu'
WHERE bank_id = 'bci' AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Carnivo%');

-- Don Carlos -> Las Condes
UPDATE fact_offers SET location_id = 'lcon'
WHERE bank_id = 'bci' AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Don Carlos%');

-- Casa Vitalis -> Vitacura (JSON has "Ver sitio" as location, falls to stgo)
UPDATE fact_offers SET location_id = 'vita'
WHERE bank_id = 'bci' AND location_id = 'stgo'
AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Vitalis%');

-- =============================================================================
-- CENCOSUD
-- =============================================================================

-- La Pergola Restobar -> Peñalolén (JSON says "Peñalolen" without accent, no match)
UPDATE fact_offers SET location_id = 'pena'
WHERE bank_id = 'cencosud' AND location_id = 'stgo'
AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Pergola%');

-- =============================================================================
-- FALABELLA
-- =============================================================================

-- Sushi & Burger Home -> Viña del Mar + Reñaca (JSON has "Región de Valparaíso", falls to stgo)
-- Step 1: Update existing row to Viña del Mar
UPDATE fact_offers SET location_id = 'vina'
WHERE bank_id = 'falabella' AND location_id = 'stgo'
AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Sushi%Burger Home%');

-- Step 2: Insert duplicate for Reñaca
INSERT INTO fact_offers (offer_id, restaurant_id, bank_id, card_type_id, location_id, valid_days, discount_pct, discount_cap, discount_cap_is_unlimited, conditions, valid_month, source_url, image_url, address, expiration_date, scraped_at)
SELECT md5(offer_id || '_rena'), restaurant_id, bank_id, card_type_id,
       'rena', valid_days, discount_pct, discount_cap,
       discount_cap_is_unlimited, conditions, valid_month, source_url,
       image_url, address, expiration_date, scraped_at
FROM fact_offers
WHERE bank_id = 'falabella' AND location_id = 'vina'
AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Sushi%Burger Home%');

-- =============================================================================
-- BANCO DE CHILE — All location matching now handled by transformer
-- (commune extracted from after last dash in location field)
-- =============================================================================

-- Dominga Bistro: ya no tiene descuento en marzo, eliminar de Banco de Chile
DELETE FROM fact_offers
WHERE bank_id = 'bancochile'
AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Dominga Bistro%');

-- SANTANDER: Eliminar Piano Gourmet (User request)
DELETE FROM fact_offers
WHERE bank_id = 'santander'
AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Piano Gourmet%');

-- =============================================================================
-- SCOTIABANK
-- =============================================================================
-- [REMOVED] Hotel Antofagasta -> Antofagasta (Offer no longer exists)
-- UPDATE fact_offers SET location_id = 'anto'
-- WHERE bank_id = 'scotiabank' AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Hotel Antofagasta%');

-- Prístino -> solo Martes
UPDATE fact_offers SET valid_days = 'Martes'
WHERE bank_id = 'scotiabank' AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Prístino%');

-- Pasta Basta -> 50% (Scotiabank API says 30% in title but 50% in conditions)
UPDATE fact_offers SET discount_pct = 50
WHERE bank_id = 'scotiabank' AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Pasta Basta%');


-- =============================================================================
-- ITAÚ
-- =============================================================================
-- La Pastelería del Cerro (Itaú): no es cadena puede confundir
UPDATE dim_restaurants SET is_chain = FALSE WHERE name ILIKE '%Pastelería del Cerro%' OR name ILIKE '%Pasteleria del Cerro%';

-- [REMOVED] Monaco -> Puchuncaví (Handled by Scraper)
-- UPDATE fact_offers SET location_id = 'puch'
-- WHERE restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE normalized_name ILIKE '%MONACO%');

-- [REMOVED] Fuente Germana & Amura -> La Serena (Handled by Scraper)
-- UPDATE fact_offers SET location_id = 'sere'
-- WHERE bank_id = 'itau' AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Fuente Germana%' OR name ILIKE '%Amura%');

-- [REMOVED] Augusta Bar -> Concepción (Handled by Scraper)
-- UPDATE fact_offers SET location_id = 'conp'
-- WHERE bank_id = 'itau' AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Augusta Bar%');

-- [REMOVED] Boreal Bar -> Rancagua (Handled by Scraper)
-- UPDATE fact_offers SET location_id = 'ranc'
-- WHERE bank_id = 'itau' AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Boreal Bar%');

-- Los Volcanes -> Puerto Varas
UPDATE fact_offers SET location_id = 'pvar'
WHERE bank_id = 'itau' AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Los Volcanes%');

-- The Ñaños -> Temuco
UPDATE fact_offers SET location_id = 'temu'
WHERE bank_id = 'itau' AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%The Ñaños%');

-- Ágape -> Viña del Mar
UPDATE fact_offers SET location_id = 'vina'
WHERE bank_id = 'itau' AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Ágape%');

-- =============================================================================
-- CENCOSUD
-- =============================================================================
-- Mirador del Alto -> location mira
UPDATE fact_offers SET location_id = 'mira'
WHERE bank_id = 'cencosud' AND address ILIKE '%Mirador del Alto%';

-- [REMOVED] Silos -> Futrono (User requested removal)
-- UPDATE fact_offers SET location_id = 'futr'
-- WHERE bank_id = 'cencosud' AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Silos%');

-- [REMOVED] Caoba Bar (Cencosud): Scraper correctly captures Miércoles

-- =============================================================================
-- SANTANDER
-- =============================================================================
-- Cuk, Mundo del Vino, Curacaribs -> Online
UPDATE dim_restaurants SET category = 'Online' WHERE name ILIKE '%Cuk%';
UPDATE dim_restaurants SET category = 'Online' WHERE name ILIKE '%Mundo del Vino%';
UPDATE dim_restaurants SET category = 'Online' WHERE name ILIKE '%Curacaribs%';

-- KA -> Cochoa (Handled by Loader)
-- UPDATE fact_offers SET location_id = 'coch'
-- WHERE bank_id = 'santander' AND location_id = 'valp'
-- AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%KA%');

-- Capri, Sushi Nikkei -> Crédito/Débito
UPDATE fact_offers SET card_type_id = 'santander_credito_debito'
WHERE bank_id = 'santander' AND card_type_id = 'santander_general'
AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Capri%' OR name ILIKE '%Sushi Nikkei%');

-- Sushi Nikkei -> Eliminar Santiago genérico
DELETE FROM fact_offers
WHERE bank_id = 'santander' AND location_id = 'stgo'
AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Sushi Nikkei%');

-- Barrio Chicken: Rename + Antojo
UPDATE dim_restaurants SET name = 'Barrio Chicken', category = 'Antojo' WHERE name = 'Barrio Chick''en';

-- =============================================================================
-- RIPLEY
-- =============================================================================
-- Demencia -> Eliminar Lunes
DELETE FROM fact_offers
WHERE bank_id = 'ripley' AND valid_days = 'LUNES'
AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Demencia%');

-- Jardín Mallinkrodt -> Eliminar Martes
DELETE FROM fact_offers
WHERE bank_id = 'ripley' AND valid_days = 'MARTES'
AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Jardín Mallinkrodt%');

-- La Sociedad -> Restaurante
UPDATE dim_restaurants SET category = 'Restaurante' WHERE name ILIKE '%La Sociedad%';

-- Burger King Wallet -> Eliminar (User request)
DELETE FROM fact_offers 
WHERE bank_id = 'ripley' 
AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Burger King Wallet%');

-- =============================================================================
-- FALABELLA: Correcciones de Location
-- =============================================================================


-- Sicily -> Vitacura (Handled by Loader)
-- UPDATE fact_offers SET location_id = 'vita'
-- WHERE bank_id = 'falabella' AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Sicily%');





-- Jeró Bistro -> Puchuncaví
UPDATE fact_offers SET location_id = 'puch'
WHERE bank_id = 'falabella' AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Jeró Bistro%');

-- Vendetta: Bellavista -> Patio Bellavista
UPDATE fact_offers SET location_id = 'pbel'
WHERE location_id = 'bell' AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Vendetta%');

-- Tigre Bravo: Maipú -> Mall Plaza Maipú (Handled by Loader)
-- UPDATE fact_offers SET location_id = 'mmai'
-- WHERE bank_id = 'falabella' AND location_id = 'maip'
-- AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Tigre Bravo%');

-- Danés -> Las Condes + CMR (Handled by Loader)
-- UPDATE fact_offers SET location_id = 'lcon', card_type_id = 'falabella_cmr'
-- WHERE bank_id = 'falabella' AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Danés%');



-- MOOi -> Costanera Center (Handled by Loader)
-- UPDATE fact_offers SET location_id = 'cost'
-- WHERE bank_id = 'falabella' AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Mooi%');

-- Mamut -> Todos los locales
UPDATE fact_offers SET location_id = 'all', address = 'Todos los locales'
WHERE restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Mamut%');

-- Mesón del Marinero (Falabella) -> Open Kennedy (User reported incorrectly as Parque Arauco)
UPDATE fact_offers SET location_id = 'open'
WHERE bank_id = 'falabella' AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Meson del Marinero%');

-- Sakura (Falabella) -> Santiago / R. Metropolitana (Generic override)
UPDATE fact_offers SET location_id = 'stgo'
WHERE bank_id = 'falabella' AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Sakura%');

-- =============================================================================
-- FALABELLA: Correcciones de Card Type
-- =============================================================================
-- [REMOVED] Barra Chalaca & El Japonés -> CMR (Handled by Loader Logic)
-- UPDATE fact_offers SET card_type_id = 'falabella_cmr'
-- WHERE bank_id = 'falabella' AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Barra Chalaca%' OR name ILIKE '%El Japones%');

-- [REMOVED] Milá -> CMR (Handled by Loader Logic)
-- UPDATE fact_offers SET card_type_id = 'falabella_cmr'
-- WHERE bank_id = 'falabella' AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Milá%' OR name ILIKE '%Mila%');

-- [REMOVED] Mamma Mia -> CMR (Handled by Loader Logic)
-- UPDATE fact_offers SET card_type_id = 'falabella_cmr'
-- WHERE bank_id = 'falabella' AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Mamma mia%' OR name ILIKE '%Mama Mia%');

-- Frida Kahlo, El Bodegón, Mamut: Elite -> CMR
UPDATE fact_offers SET card_type_id = 'falabella_cmr'
WHERE bank_id = 'falabella' AND card_type_id = 'falabella_elite'
AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Frida Kahlo%' OR name ILIKE '%El Bodegon%' OR name ILIKE '%Mamut%');

-- Frida Kahlo (Falabella): User requested valid days to explicitly be Lunes a Jueves + Domingo
UPDATE fact_offers SET valid_days = 'Lunes a Jueves y Domingo'
WHERE bank_id = 'falabella' 
AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Frida Kahlo%');

-- Muu Grill (Falabella) -> Isidora 3000 (User request: change from Las Condes)
UPDATE fact_offers SET location_id = 'isid', address = 'Isidora Goyenechea 3000, Las Condes'
WHERE bank_id = 'falabella' AND (location_id = 'lcon' OR location_id = 'alto')
AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Muu Grill%');

-- Rocoto (Falabella) -> Ñuñoa (User request: change from Valdivia, likely mis-matched from Pedro de Valdivia street)
UPDATE fact_offers SET location_id = 'nuno', address = 'Pedro de Valdivia 2573, Ñuñoa'
WHERE bank_id = 'falabella' AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Rocoto%')
AND location_id = 'vald';

-- [REMOVED] Ekeko -> CMR (Handled by Loader Logic)
-- UPDATE fact_offers SET card_type_id = 'falabella_cmr'
-- WHERE bank_id = 'falabella' AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Ekeko%');

-- Bianco latte -> Antojo
UPDATE dim_restaurants SET category = 'Antojo' WHERE name ILIKE '%Bianco latte%';

-- =============================================================================
-- FALABELLA: Eliminar días incorrectos
-- =============================================================================
-- Vapiano: Eliminar Santiago genérico (Handled by all whitelist)
-- DELETE FROM fact_offers
-- WHERE bank_id = 'falabella' AND location_id = 'stgo'
-- AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Vapiano%');

-- Tigre Bravo: Eliminar Santiago genérico
DELETE FROM fact_offers
WHERE bank_id = 'falabella' AND location_id = 'stgo'
AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Tigre Bravo%');

-- Marola -> Pupuya
UPDATE fact_offers SET location_id = 'pupu', address = 'Playa de Pupuya'
WHERE bank_id = 'falabella' AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Marola%');

-- Vapiano -> Expandir a Las Condes y Providencia
CREATE OR REPLACE TEMP TABLE _vapiano_ref AS
SELECT restaurant_id, bank_id, card_type_id, valid_days, discount_pct, discount_cap, discount_cap_is_unlimited, expiration_date, conditions
FROM fact_offers
WHERE bank_id = 'falabella'
AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Vapiano%');

DELETE FROM fact_offers
WHERE bank_id = 'falabella'
AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Vapiano%');

INSERT INTO fact_offers (offer_id, restaurant_id, bank_id, card_type_id, location_id, valid_days, discount_pct, discount_cap, discount_cap_is_unlimited, expiration_date, conditions, address)
SELECT
    md5(concat('falabella_vapiano_', v.addr, '_', r.valid_days)),
    r.restaurant_id, 'falabella', r.card_type_id, v.loc_id,
    r.valid_days, r.discount_pct, r.discount_cap, r.discount_cap_is_unlimited, r.expiration_date, r.conditions, v.addr
FROM _vapiano_ref r
CROSS JOIN (VALUES
    ('Avenida Presidente Kennedy 5413, Las Condes', 'lcon'),
    ('Avenida Providencia 1984, Providencia', 'prov')
) AS v(addr, loc_id)
ON CONFLICT DO NOTHING;

DROP TABLE IF EXISTS _vapiano_ref;

-- Mamma Mia: Eliminar Lunes
DELETE FROM fact_offers
WHERE bank_id = 'falabella' AND valid_days = 'Lunes'
AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Mamma mia%' OR name ILIKE '%Mama Mia%');

-- Bar Santiago: Eliminar Martes
DELETE FROM fact_offers
WHERE bank_id = 'falabella' AND valid_days = 'Martes'
AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Bar Santiago%');

-- Boost y Fuku Mochi: Eliminar Miércoles (para que calcen en una sola lámina)
DELETE FROM fact_offers
WHERE bank_id = 'falabella' AND valid_days = 'Miércoles'
AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Boost%' OR name ILIKE '%Fuku Mochi%');

-- [REMOVED] Dagan, Caperucita, Martina Lounge, Pistacho: overrides no longer needed (scraper data is correct)
-- [REMOVED] Caperucita: Jueves delete no longer needed

-- Take a Wok: Eliminar Lun-Vie en Vivo Imperio
DELETE FROM fact_offers
WHERE bank_id = 'falabella' AND location_id = 'vimp'
AND valid_days IN ('Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes')
AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Take a Wok%');

-- [REMOVED] Tanta: Eliminar Lunes en Isidora 3000 (Handled by JSON extraction)
-- DELETE FROM fact_offers
-- WHERE bank_id = 'falabella' AND location_id = 'isid' AND valid_days = 'Lunes'
-- AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Tanta%');

-- =============================================================================
-- FALABELLA: Eliminar ofertas y ubicaciones incorrectas
-- =============================================================================
-- Barceloneta, Casa Barroso, Mar Blanco, Libre Salvaje, Tropera, Marola, Sublime
DELETE FROM fact_offers
WHERE bank_id = 'falabella'
AND restaurant_id IN (
    SELECT restaurant_id FROM dim_restaurants
    WHERE name ILIKE '%Casa Barroso%'
       OR name ILIKE '%Mar Blanco%' OR name ILIKE '%Libre Salvaje%'
       OR name ILIKE '%Sublime%' -- User request: not a valid discount
       OR name ILIKE '%Citadelle%' -- User request: not a valid discount
       OR name = 'Dominga Bistró' -- User request: Duplicate/Garbage locations (instructions parsed as locs)
);

-- Happy Box: Set Category to Antojo
UPDATE dim_restaurants SET category = 'Antojo' WHERE name ILIKE '%Happy Box%';

-- [REMOVED] La Piazza Open Kennedy Override: Scraper handles correctly
-- DELETE FROM fact_offers
-- WHERE bank_id = 'falabella' AND location_id = 'open'
-- AND valid_days IN ('Lunes', 'Martes')
-- AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%La Piazza%');

-- [REMOVED] Barra Chalaca Mercado Bulnes (Handled by JSON extraction)
-- DELETE FROM fact_offers
-- WHERE bank_id = 'falabella' AND location_id = 'mbul'
-- AND valid_days IN ('Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes')
-- AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Barra Chalaca%');

-- [REMOVED] Jalisco Open Kennedy (Handled by JSON extraction)
-- DELETE FROM fact_offers
-- WHERE bank_id = 'falabella' AND location_id = 'open'
-- AND valid_days IN ('Lunes', 'Martes')
-- AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Jalisco%');

-- [REMOVED] Sanguchera del Barrio Mercado Bulnes (Handled by JSON extraction)
-- DELETE FROM fact_offers
-- WHERE bank_id = 'falabella' AND location_id = 'mbul'
-- AND valid_days IN ('Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes')
-- AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Sanguchera del Barrio%');

-- =============================================================================
-- FALABELLA: Expansiones multi-location
-- =============================================================================

-- [REMOVED] Kento Gourmet (Handled by Loader Mapping)
-- UPDATE fact_offers SET location_id = 'alto' ...

-- [REMOVED] La Maestranza (Handled by Loader Expansion)
-- UPDATE fact_offers SET location_id = 'lbar' ...

-- INSERT INTO fact_offers ... (Handled by Loader)

-- =============================================================================
-- INTERNACIONAL
-- =============================================================================

-- Danubio Marzo -> Danubio Azul (User request, Scraper caught 'Marzo' from promo text)
UPDATE dim_restaurants SET name = 'Danubio Azul' WHERE name ILIKE '%Danubio Marzo%';

-- Barbazul (Internacional): Expandir de 1 ubicación a 10 sucursales
CREATE OR REPLACE TEMP TABLE _barbazul_ref AS
SELECT restaurant_id, card_type_id, valid_days, discount_pct, discount_cap, discount_cap_is_unlimited, conditions, expiration_date
FROM fact_offers
WHERE bank_id = 'internacional'
AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name = 'Barbazul');

DELETE FROM fact_offers
WHERE bank_id = 'internacional'
AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name = 'Barbazul');

INSERT INTO fact_offers (offer_id, restaurant_id, bank_id, card_type_id, location_id, valid_days, discount_pct, discount_cap, discount_cap_is_unlimited, conditions, address, expiration_date)
SELECT
    md5(concat('internacional_barbazul_', v.addr, '_', r.valid_days)),
    r.restaurant_id, 'internacional', r.card_type_id, v.loc_id,
    r.valid_days, r.discount_pct, r.discount_cap, r.discount_cap_is_unlimited, r.conditions, v.addr, r.expiration_date
FROM _barbazul_ref r
CROSS JOIN (VALUES
    ('Av Vitacura #9257', 'vita'),
    ('Av Apoquindo #7741', 'lcon'),
    ('Av Tobalaba 175', 'prov'),
    ('Jorge Washington #58', 'nuno'),
    ('Av Italia #1034', 'prov'),
    ('General Holley 2285', 'prov'),
    ('Av Apoquindo #7581', 'lcon'),
    ('Av Tobalaba #1155', 'prov'),
    ('Central #14408', 'rena'),
    ('Calle Constitución 241', 'prov')
) AS v(addr, loc_id)
ON CONFLICT DO NOTHING;

DROP TABLE IF EXISTS _barbazul_ref;

-- La Maestranza (Itaú): Expandir a Vitacura y Las Condes
CREATE OR REPLACE TEMP TABLE _maestranza_ref AS
SELECT restaurant_id, bank_id, card_type_id, valid_days, discount_pct, discount_cap, discount_cap_is_unlimited, conditions, source_url, image_url, expiration_date
FROM fact_offers
WHERE bank_id = 'itau'
AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Maestranza%');

DELETE FROM fact_offers
WHERE bank_id = 'itau'
AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Maestranza%');

INSERT INTO fact_offers (offer_id, restaurant_id, bank_id, card_type_id, location_id, valid_days, discount_pct, discount_cap, discount_cap_is_unlimited, conditions, address, source_url, image_url, expiration_date)
SELECT
    md5(concat('itau_maestranza_', v.addr, '_', r.valid_days)),
    r.restaurant_id, 'itau', r.card_type_id, v.loc_id,
    r.valid_days, r.discount_pct, r.discount_cap, r.discount_cap_is_unlimited, r.conditions, v.addr, r.source_url, r.image_url, r.expiration_date
FROM _maestranza_ref r
CROSS JOIN (VALUES
    ('Presidente Kennedy N°9001, local 3265, Las Condes', 'lcon'),
    ('Vitacura 3708, Vitacura', 'vita')
) AS v(addr, loc_id)
ON CONFLICT (offer_id) DO NOTHING;

DROP TABLE IF EXISTS _maestranza_ref;

-- =============================================================================
-- FALABELLA: Ofertas expiradas (API Contentful sirve datos viejos)
-- =============================================================================
DELETE FROM fact_offers
WHERE bank_id = 'falabella'
AND restaurant_id IN (
    SELECT restaurant_id FROM dim_restaurants
    WHERE name ILIKE '%Dagan%' OR name ILIKE '%Mirador Gourmet%'
       OR name ILIKE '%VOLKA%' -- OR name ILIKE '%Casaluz%' (Valid Elite)
       OR name ILIKE '%Pastelería Amelie%'
       OR name ILIKE '%Ari Nikkei%'
       OR name ILIKE '%WENDYS%' OR name ILIKE '%CHINA WOK%'
       OR name ILIKE '%Holy Moly%'
       OR name ILIKE '%The Top Brunch%'
);

-- Mamut: categorizado como Antojo globalmente en 00_restaurants.sql

-- =============================================================================
-- GLOBAL OVERRIDES: Marcas que siempre deben ser Online / All locations
-- =============================================================================

-- Cava Morandé: Siempre Online en todos los bancos (User request)
UPDATE dim_restaurants SET category = 'Online' WHERE name ILIKE '%Cava Morand%';
UPDATE fact_offers SET location_id = 'all', address = 'Online' 
WHERE restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Cava Morand%');

-- Anima Cocktail Lab -> Restaurante (Falabella JSON incorrectly marks as Antojo)
UPDATE dim_restaurants SET category = 'Restaurante' WHERE name ILIKE '%Anima cocktail%';

-- La Parrilla del Guaton Jerez -> Guaton Jerez (User request)
UPDATE dim_restaurants SET name = 'Guaton Jerez' WHERE name ILIKE '%Parrilla del Guaton Jerez%';

-- =============================================================================
-- SANTANDER
-- =============================================================================

-- Just Burger: Es cadena, debe aplicar a todos los locales (User request)
UPDATE fact_offers 
SET location_id = 'all', address = 'Todos los locales'
WHERE bank_id = 'santander' 
AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Just Burger%');

-- =============================================================================
-- RIPLEY
-- =============================================================================

-- Just Burger: JSON extraction results in 'Todos los días' (missing recurrence) but promotion is only valid on Tuesdays (user request).
UPDATE fact_offers
SET valid_days = 'Martes'
WHERE bank_id = 'ripley' AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Just Burger%');

-- =============================================================================
-- CENCOSUD
-- =============================================================================

-- Burger King: Eliminar oferta (User request)
DELETE FROM fact_offers
WHERE bank_id = 'cencosud'
AND restaurant_id IN (SELECT restaurant_id FROM dim_restaurants WHERE name ILIKE '%Burger King%');

-- La Sociedad -> Restaurante (Restored/Confirmed)
UPDATE dim_restaurants SET category = 'Restaurante' WHERE name ILIKE '%La Sociedad%';
