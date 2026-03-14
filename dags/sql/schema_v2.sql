-- =============================================================================
-- SCHEMA V2: Descuentos Bancarios - Modelo Normalizado
-- Motor: DuckDB
-- =============================================================================

-- 0. CONFIGURACIÓN
INSTALL httpfs; LOAD httpfs;
INSTALL json; LOAD json;

-- 1. LIMPIAR TABLAS (OPCIONAL/COMENTADO PARA PERSISTENCIA)
-- DROP TABLE IF EXISTS fact_offers;
-- DROP TABLE IF EXISTS dim_card_types;
-- DROP TABLE IF EXISTS dim_banks;
-- DROP TABLE IF EXISTS dim_locations;
-- DROP TABLE IF EXISTS dim_restaurants;

-- =============================================================================
-- 2. DIMENSIONES
-- =============================================================================

-- 2.1 Restaurantes
CREATE TABLE IF NOT EXISTS dim_restaurants (
    restaurant_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    normalized_name VARCHAR,
    category VARCHAR DEFAULT 'Restaurante',
    is_chain BOOLEAN DEFAULT FALSE,
    cuisine_type VARCHAR,
    instagram VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Asegurar que las columnas existan si la tabla ya fue creada previamente
ALTER TABLE dim_restaurants ADD COLUMN IF NOT EXISTS cuisine_type VARCHAR;
ALTER TABLE dim_restaurants ADD COLUMN IF NOT EXISTS instagram VARCHAR;

-- 2.2 Ubicaciones
CREATE TABLE IF NOT EXISTS dim_locations (
    location_id VARCHAR PRIMARY KEY,
    commune VARCHAR NOT NULL,
    region VARCHAR
);

-- Datos de referencia: Comunas de Chile
INSERT INTO dim_locations VALUES
-- Metropolitana
('stgo', 'Santiago', 'Metropolitana'),
('prov', 'Providencia', 'Metropolitana'),
('lcon', 'Las Condes', 'Metropolitana'),
('vita', 'Vitacura', 'Metropolitana'),
('nuno', 'Ñuñoa', 'Metropolitana'),
('lbar', 'Lo Barnechea', 'Metropolitana'),
('lare', 'La Reina', 'Metropolitana'),
('maip', 'Maipú', 'Metropolitana'),
('flor', 'Florida', 'Metropolitana'),
('macu', 'Macul', 'Metropolitana'),
('pena', 'Peñalolén', 'Metropolitana'),
('estc', 'Estación Central', 'Metropolitana'),
('reco', 'Recoleta', 'Metropolitana'),
('smig', 'San Miguel', 'Metropolitana'),
('huec', 'Huechuraba', 'Metropolitana'),
('cerr', 'Cerrillos', 'Metropolitana'),
('qnor', 'Quinta Normal', 'Metropolitana'),
('puda', 'Pudahuel', 'Metropolitana'),
('bell', 'Bellavista', 'Metropolitana'),
('coli', 'Colina', 'Metropolitana'),
('chic', 'Chicureo', 'Metropolitana'),
('lamp', 'Lampa', 'Metropolitana'),
('cist', 'La Cisterna', 'Metropolitana'),
('sber', 'San Bernardo', 'Metropolitana'),
('buin', 'Buin', 'Metropolitana'),
-- Valparaíso
('valp', 'Valparaíso', 'Valparaíso'),
('vina', 'Viña del Mar', 'Valparaíso'),
('conc', 'Concón', 'Valparaíso'),
('rena', 'Reñaca', 'Valparaíso'),
('mait', 'Maitencillo', 'Valparaíso'),
('alga', 'Algarrobo', 'Valparaíso'),
('zapa', 'Zapallar', 'Valparaíso'),
('quil', 'Quillota', 'Valparaíso'),
('qlpe', 'Quilpué', 'Valparaíso'),
('curm', 'Curauma', 'Valparaíso'),
('sfli', 'San Felipe', 'Valparaíso'),
('land', 'Los Andes', 'Valparaíso'),
('olmu', 'Olmué', 'Valparaíso'),
-- Biobío
('conp', 'Concepción', 'Biobío'),
('talc', 'Talcahuano', 'Biobío'),
('lang', 'Los Ángeles', 'Biobío'),
('sped', 'San Pedro de la Paz', 'Biobío'),
('hual', 'Hualpén', 'Biobío'),
('cane', 'Cañete', 'Biobío'),
-- Otras regiones
('anto', 'Antofagasta', 'Antofagasta'),
('cala', 'Calama', 'Antofagasta'),
('sere', 'La Serena', 'Coquimbo'),
('coqu', 'Coquimbo', 'Coquimbo'),
('oval', 'Ovalle', 'Coquimbo'),
('temu', 'Temuco', 'Araucanía'),
('puco', 'Pucón', 'Araucanía'),
('vill', 'Villarrica', 'Araucanía'),
('pmon', 'Puerto Montt', 'Los Lagos'),
('pvar', 'Puerto Varas', 'Los Lagos'),
('cast', 'Castro', 'Los Lagos'),
('ancu', 'Ancud', 'Los Lagos'),
('osor', 'Osorno', 'Los Lagos'),
('vald', 'Valdivia', 'Los Ríos'),
('talm', 'Talca', 'Maule'),
('curi', 'Curicó', 'Maule'),
('pell', 'Pelluhue', 'Maule'),
('aric', 'Arica', 'Arica y Parinacota'),
('iqui', 'Iquique', 'Tarapacá'),
('pich', 'Pichilemu', 'O''Higgins'),
('ranc', 'Rancagua', 'O''Higgins'),
('mach', 'Machalí', 'O''Higgins'),
('reng', 'Rengo', 'O''Higgins'),
('requ', 'Requínoa', 'O''Higgins'),
('chil', 'Chillán', 'Ñuble'),
('copa', 'Copiapó', 'Atacama'),
('coyh', 'Coyhaique', 'Aysén'),
('puch', 'Puchuncaví', 'Valparaíso'),
('cach', 'Cachagua', 'Valparaíso'),
('lran', 'Lago Ranco', 'Los Ríos'),
('mata', 'Matanzas', 'O''Higgins'),
('pnat', 'Puerto Natales', 'Magallanes'),
('futr', 'Futrono', 'Los Ríos'),
('casa', 'Casablanca', 'Valparaíso'),
('pare', 'Punta Arenas', 'Magallanes'),
-- Malls (como ubicaciones especiales)
('parq', 'Parque Arauco', 'Metropolitana'),
('cost', 'Costanera Center', 'Metropolitana'),
('alto', 'Alto Las Condes', 'Metropolitana'),
('mpla', 'Mall Plaza Egaña', 'Metropolitana'),
('moes', 'Mallplaza Oeste', 'Metropolitana'),
('mves', 'Mallplaza Vespucio', 'Metropolitana'),
('mtob', 'Mallplaza Tobalaba', 'Metropolitana'),
('mdom', 'Mallplaza Los Dominicos', 'Metropolitana'),
('open', 'Open Kennedy', 'Metropolitana'),
('vivo', 'Vivo Panorámico', 'Metropolitana'),
('mbul', 'Mercado Bulnes', 'Metropolitana'),
('brio', 'BordeRío', 'Metropolitana'),
('mvin', 'Mall Marina Viña', 'Valparaíso'),
('mmar', 'Mall Marina', 'Valparaíso'),
-- Variantes con espacio para Falabella
('mpve', 'Mall Plaza Vespucio', 'Metropolitana'),
('mpoe', 'Mall Plaza Oeste', 'Metropolitana'),
('mpto', 'Mall Plaza Tobalaba', 'Metropolitana'),
('mpdo', 'Mall Plaza Los Dominicos', 'Metropolitana'),
('mpno', 'Mall Plaza Norte', 'Metropolitana'),
('mpla_v2', 'Mall Plaza Egaña', 'Metropolitana'), -- Redundante pero seguro
-- Missing Tanta Locations
('isid', 'Isidora 3000', 'Metropolitana'),
('mfce', 'Mall Florida Center', 'Metropolitana'),
('mpla_tight', 'Mallplaza Egaña', 'Metropolitana'),
('mpno_tight', 'Mallplaza Norte', 'Metropolitana'),
('mpoe_tight', 'Mallplaza Oeste', 'Metropolitana'),
('mpto_tight', 'Mallplaza Tobalaba', 'Metropolitana'),
('mpdo_tight', 'Mallplaza Los Dominicos', 'Metropolitana'),
    ('mves_tight', 'Mallplaza Vespucio', 'Metropolitana'),
    ('mira', 'Mirador del Alto', 'Metropolitana'),
    ('pupu', 'Pupuya', 'O''Higgins'),
    -- Valor especial para cadenas
    ('all', 'Todas las Sucursales', NULL)
ON CONFLICT (location_id) DO NOTHING;

-- 2.3 Bancos
CREATE TABLE IF NOT EXISTS dim_banks (
    bank_id VARCHAR PRIMARY KEY,
    bank_name VARCHAR NOT NULL
);

INSERT INTO dim_banks VALUES
('bci', 'BCI'),
('santander', 'Santander'),
('bancochile', 'Banco de Chile'),
('scotiabank', 'Scotiabank'),
('falabella', 'Banco Falabella'),
('ripley', 'Banco Ripley'),
('bancoestado', 'Banco Estado'),
('cencosud', 'Cencosud'),
('bice', 'Banco BICE'),
('itau', 'Banco Itaú'),
('internacional', 'Banco Internacional')
ON CONFLICT (bank_id) DO NOTHING;

-- 2.4 Tipos de Tarjeta
CREATE TABLE IF NOT EXISTS dim_card_types (
    card_type_id VARCHAR PRIMARY KEY,
    bank_id VARCHAR REFERENCES dim_banks(bank_id),
    card_name VARCHAR NOT NULL,
    tier INTEGER DEFAULT 1
);

INSERT INTO dim_card_types VALUES
-- BCI
('bci_general', 'bci', 'General', 1),
('bci_visa_signature', 'bci', 'Visa Signature', 2),
('bci_visa_infinite', 'bci', 'Visa Infinite', 3),
('bci_mastercard_black', 'bci', 'Mastercard Black', 3),
('bci_credito_debito', 'bci', 'Crédito y Débito', 1),
('bci_credito', 'bci', 'Crédito', 1),
('bci_combined', 'bci', 'Infinite/Black/Sign.', 2),
('bci_premium', 'bci', 'Black/Sign./Infinite', 3),
-- Santander
('santander_general', 'santander', 'General', 1),
('santander_credito_debito', 'santander', 'Crédito/Débito', 1),
('santander_amex', 'santander', 'American Express', 2),
('santander_limited', 'santander', 'WorldMember Limited', 3),
-- Banco de Chile
('bancochile_general', 'bancochile', 'General', 1),
('bancochile_visa', 'bancochile', 'Visa', 1),
('bancochile_infinite', 'bancochile', 'Visa Infinite', 2),
-- Scotiabank
('scotiabank_general', 'scotiabank', 'General', 1),
-- Falabella
('falabella_cmr', 'falabella', 'CMR', 1),
('falabella_elite', 'falabella', 'Elite', 2),
('falabella_general', 'falabella', 'CMR Falabella', 1),
-- Ripley
('ripley_credito', 'ripley', 'Tarjeta Ripley', 1),
('ripley_gold', 'ripley', 'Gold', 2),
('ripley_black', 'ripley', 'Black', 3),
-- BancoEstado
('bancoestado_visa', 'bancoestado', 'Crédito Visa', 1),
('bancoestado_general', 'bancoestado', 'Crédito', 1),
-- Cencosud
('cencosud_scotiabank', 'cencosud', 'Scotiabank Cencosud', 1),
-- BICE
('bice_credito', 'bice', 'Crédito', 1),
('bice_limitless', 'bice', 'Limitless', 1),
('bice_banca_joven', 'bice', 'Banca Joven', 1),
('bice_premium', 'bice', 'Black/Sign./Limitless', 2),
-- Itau
('itau_general', 'itau', 'General', 1),
('itau_legend', 'itau', 'Legend', 2),
('itau_black', 'itau', 'Black', 3),
('itau_combined', 'itau', 'Legend/Black', 2),
-- Internacional
('internacional_credito', 'internacional', 'Crédito', 1)
ON CONFLICT (card_type_id) DO UPDATE SET card_name = EXCLUDED.card_name;

-- =============================================================================
-- 3. TABLA DE HECHOS
-- =============================================================================

CREATE TABLE IF NOT EXISTS fact_offers (
    offer_id VARCHAR PRIMARY KEY,
    restaurant_id VARCHAR REFERENCES dim_restaurants(restaurant_id),
    bank_id VARCHAR REFERENCES dim_banks(bank_id),
    card_type_id VARCHAR REFERENCES dim_card_types(card_type_id),
    location_id VARCHAR REFERENCES dim_locations(location_id),
    valid_days VARCHAR,
    discount_pct INTEGER,
    conditions VARCHAR,
    valid_month VARCHAR,
    source_url VARCHAR,
    image_url VARCHAR,
    address VARCHAR,
    discount_cap INTEGER,
    discount_cap_is_unlimited BOOLEAN,
    expiration_date DATE,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Backwards compat: add columns if table already existed
ALTER TABLE fact_offers ADD COLUMN IF NOT EXISTS discount_cap INTEGER;
ALTER TABLE fact_offers ADD COLUMN IF NOT EXISTS discount_cap_is_unlimited BOOLEAN;
ALTER TABLE fact_offers ADD COLUMN IF NOT EXISTS expiration_date DATE;

-- =============================================================================
-- 4. SNAPSHOT TABLE (persistent, for day-over-day diff)
-- =============================================================================

CREATE TABLE IF NOT EXISTS offers_snapshot (
    snapshot_date DATE NOT NULL,
    run_id VARCHAR NOT NULL,
    offer_id VARCHAR NOT NULL,
    restaurant_id VARCHAR,
    bank_id VARCHAR,
    card_type_id VARCHAR,
    location_id VARCHAR,
    valid_days VARCHAR,
    discount_pct INTEGER,
    discount_cap INTEGER,
    PRIMARY KEY (snapshot_date, offer_id)
);

-- =============================================================================
-- 5. DIFF TABLE (day-over-day changes)
-- =============================================================================

CREATE TABLE IF NOT EXISTS diff_offers (
    diff_date DATE NOT NULL,
    run_id VARCHAR NOT NULL,
    diff_type VARCHAR NOT NULL,
    offer_id VARCHAR NOT NULL,
    restaurant_id VARCHAR,
    bank_id VARCHAR,
    card_type_id VARCHAR,
    location_id VARCHAR,
    valid_days VARCHAR,
    discount_pct INTEGER,
    discount_cap INTEGER,
    prev_discount_pct INTEGER,
    prev_valid_days VARCHAR,
    prev_discount_cap INTEGER,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (diff_date, run_id, offer_id, diff_type)
);
