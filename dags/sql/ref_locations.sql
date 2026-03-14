-- =============================================================================
-- TABLA DE REFERENCIA: LOCACIONES POR REGIÓN
-- =============================================================================
-- Esta tabla mapea comunas/locales a regiones para filtrado dinámico
-- Fuente: Comunas oficiales de Chile (INE)
-- =============================================================================

DROP TABLE IF EXISTS ref_locations;

CREATE TABLE ref_locations (
    location_id INTEGER PRIMARY KEY,
    location_name VARCHAR NOT NULL,
    location_type VARCHAR NOT NULL,  -- 'comuna', 'mall', 'sector'
    region_code VARCHAR NOT NULL,    -- 'RM', 'V', 'VIII', etc.
    region_name VARCHAR NOT NULL
);

-- =============================================================================
-- REGIÓN METROPOLITANA (52 comunas)
-- =============================================================================
INSERT INTO ref_locations (location_id, location_name, location_type, region_code, region_name) VALUES
-- Provincia de Santiago
(1, 'Santiago', 'comuna', 'RM', 'Región Metropolitana'),
(2, 'Cerrillos', 'comuna', 'RM', 'Región Metropolitana'),
(3, 'Cerro Navia', 'comuna', 'RM', 'Región Metropolitana'),
(4, 'Conchalí', 'comuna', 'RM', 'Región Metropolitana'),
(5, 'El Bosque', 'comuna', 'RM', 'Región Metropolitana'),
(6, 'Estación Central', 'comuna', 'RM', 'Región Metropolitana'),
(7, 'Huechuraba', 'comuna', 'RM', 'Región Metropolitana'),
(8, 'Independencia', 'comuna', 'RM', 'Región Metropolitana'),
(9, 'La Cisterna', 'comuna', 'RM', 'Región Metropolitana'),
(10, 'La Florida', 'comuna', 'RM', 'Región Metropolitana'),
(11, 'La Granja', 'comuna', 'RM', 'Región Metropolitana'),
(12, 'La Pintana', 'comuna', 'RM', 'Región Metropolitana'),
(13, 'La Reina', 'comuna', 'RM', 'Región Metropolitana'),
(14, 'Las Condes', 'comuna', 'RM', 'Región Metropolitana'),
(15, 'Lo Barnechea', 'comuna', 'RM', 'Región Metropolitana'),
(16, 'Lo Espejo', 'comuna', 'RM', 'Región Metropolitana'),
(17, 'Lo Prado', 'comuna', 'RM', 'Región Metropolitana'),
(18, 'Macul', 'comuna', 'RM', 'Región Metropolitana'),
(19, 'Maipú', 'comuna', 'RM', 'Región Metropolitana'),
(20, 'Ñuñoa', 'comuna', 'RM', 'Región Metropolitana'),
(21, 'Pedro Aguirre Cerda', 'comuna', 'RM', 'Región Metropolitana'),
(22, 'Peñalolén', 'comuna', 'RM', 'Región Metropolitana'),
(23, 'Providencia', 'comuna', 'RM', 'Región Metropolitana'),
(24, 'Pudahuel', 'comuna', 'RM', 'Región Metropolitana'),
(25, 'Quilicura', 'comuna', 'RM', 'Región Metropolitana'),
(26, 'Quinta Normal', 'comuna', 'RM', 'Región Metropolitana'),
(27, 'Recoleta', 'comuna', 'RM', 'Región Metropolitana'),
(28, 'Renca', 'comuna', 'RM', 'Región Metropolitana'),
(29, 'San Joaquín', 'comuna', 'RM', 'Región Metropolitana'),
(30, 'San Miguel', 'comuna', 'RM', 'Región Metropolitana'),
(31, 'San Ramón', 'comuna', 'RM', 'Región Metropolitana'),
(32, 'Vitacura', 'comuna', 'RM', 'Región Metropolitana'),
-- Provincia de Cordillera
(33, 'Puente Alto', 'comuna', 'RM', 'Región Metropolitana'),
(34, 'Pirque', 'comuna', 'RM', 'Región Metropolitana'),
(35, 'San José de Maipo', 'comuna', 'RM', 'Región Metropolitana'),
-- Provincia de Chacabuco
(36, 'Colina', 'comuna', 'RM', 'Región Metropolitana'),
(37, 'Lampa', 'comuna', 'RM', 'Región Metropolitana'),
(38, 'Tiltil', 'comuna', 'RM', 'Región Metropolitana'),
-- Provincia de Maipo
(39, 'San Bernardo', 'comuna', 'RM', 'Región Metropolitana'),
(40, 'Buin', 'comuna', 'RM', 'Región Metropolitana'),
(41, 'Calera de Tango', 'comuna', 'RM', 'Región Metropolitana'),
(42, 'Paine', 'comuna', 'RM', 'Región Metropolitana'),
-- Provincia de Melipilla
(43, 'Melipilla', 'comuna', 'RM', 'Región Metropolitana'),
(44, 'Alhué', 'comuna', 'RM', 'Región Metropolitana'),
(45, 'Curacaví', 'comuna', 'RM', 'Región Metropolitana'),
(46, 'María Pinto', 'comuna', 'RM', 'Región Metropolitana'),
(47, 'San Pedro', 'comuna', 'RM', 'Región Metropolitana'),
-- Provincia de Talagante
(48, 'Talagante', 'comuna', 'RM', 'Región Metropolitana'),
(49, 'El Monte', 'comuna', 'RM', 'Región Metropolitana'),
(50, 'Isla de Maipo', 'comuna', 'RM', 'Región Metropolitana'),
(51, 'Padre Hurtado', 'comuna', 'RM', 'Región Metropolitana'),
(52, 'Peñaflor', 'comuna', 'RM', 'Región Metropolitana');

-- =============================================================================
-- MALLS/SECTORES CONOCIDOS DE RM (agregados para mejor matching)
-- =============================================================================
INSERT INTO ref_locations (location_id, location_name, location_type, region_code, region_name) VALUES
(100, 'Costanera Center', 'mall', 'RM', 'Región Metropolitana'),
(101, 'Parque Arauco', 'mall', 'RM', 'Región Metropolitana'),
(102, 'Alto Las Condes', 'mall', 'RM', 'Región Metropolitana'),
(103, 'Portal La Dehesa', 'mall', 'RM', 'Región Metropolitana'),
(104, 'Mallplaza Vespucio', 'mall', 'RM', 'Región Metropolitana'),
(105, 'Mallplaza Egaña', 'mall', 'RM', 'Región Metropolitana'),
(106, 'Mallplaza Norte', 'mall', 'RM', 'Región Metropolitana'),
(107, 'Mallplaza Oeste', 'mall', 'RM', 'Región Metropolitana'),
(108, 'Mallplaza Tobalaba', 'mall', 'RM', 'Región Metropolitana'),
(109, 'Mallplaza Los Dominicos', 'mall', 'RM', 'Región Metropolitana'),
(110, 'Plaza Norte', 'mall', 'RM', 'Región Metropolitana'),
(111, 'Vivo Imperio', 'mall', 'RM', 'Región Metropolitana'),
(112, 'Vivo Panorámico', 'mall', 'RM', 'Región Metropolitana'),
(113, 'Open Kennedy', 'mall', 'RM', 'Región Metropolitana'),
(114, 'Patio Bellavista', 'sector', 'RM', 'Región Metropolitana'),
(115, 'Mercado Bulnes', 'sector', 'RM', 'Región Metropolitana'),
(116, 'Isidora Goyenechea', 'sector', 'RM', 'Región Metropolitana'),
(117, 'Isidora 3000', 'sector', 'RM', 'Región Metropolitana'),
(118, 'BordeRío', 'sector', 'RM', 'Región Metropolitana'),
(119, 'Borde Rio', 'sector', 'RM', 'Región Metropolitana'),
(126, 'Mirador del Alto', 'sector', 'RM', 'Región Metropolitana'),
-- Variantes Mall Plaza (con espacio)
(120, 'Mall Plaza Vespucio', 'mall', 'RM', 'Región Metropolitana'),
(121, 'Mall Plaza Egaña', 'mall', 'RM', 'Región Metropolitana'),
(122, 'Mall Plaza Norte', 'mall', 'RM', 'Región Metropolitana'),
(123, 'Mall Plaza Oeste', 'mall', 'RM', 'Región Metropolitana'),
(124, 'Mall Plaza Tobalaba', 'mall', 'RM', 'Región Metropolitana'),
(125, 'Mall Plaza Los Dominicos', 'mall', 'RM', 'Región Metropolitana');

-- =============================================================================
-- REGIÓN DE VALPARAÍSO (principales comunas)
-- =============================================================================
INSERT INTO ref_locations (location_id, location_name, location_type, region_code, region_name) VALUES
(200, 'Valparaíso', 'comuna', 'V', 'Región de Valparaíso'),
(201, 'Viña del Mar', 'comuna', 'V', 'Región de Valparaíso'),
(202, 'Viña', 'comuna', 'V', 'Región de Valparaíso'),
(203, 'Concón', 'comuna', 'V', 'Región de Valparaíso'),
(204, 'Quilpué', 'comuna', 'V', 'Región de Valparaíso'),
(205, 'Villa Alemana', 'comuna', 'V', 'Región de Valparaíso'),
(206, 'San Antonio', 'comuna', 'V', 'Región de Valparaíso'),
(207, 'Mall Marina', 'mall', 'V', 'Región de Valparaíso');

-- =============================================================================
-- OTRAS REGIONES (principales ciudades)
-- =============================================================================
INSERT INTO ref_locations (location_id, location_name, location_type, region_code, region_name) VALUES
(300, 'Antofagasta', 'comuna', 'II', 'Región de Antofagasta'),
(301, 'La Serena', 'comuna', 'IV', 'Región de Coquimbo'),
(302, 'Coquimbo', 'comuna', 'IV', 'Región de Coquimbo'),
(303, 'Rancagua', 'comuna', 'VI', 'Región de O''Higgins'),
(304, 'Talca', 'comuna', 'VII', 'Región del Maule'),
(305, 'Concepción', 'comuna', 'VIII', 'Región del Biobío'),
(306, 'Temuco', 'comuna', 'IX', 'Región de La Araucanía'),
(307, 'Valdivia', 'comuna', 'XIV', 'Región de Los Ríos'),
(308, 'Puerto Montt', 'comuna', 'X', 'Región de Los Lagos'),
(309, 'Puerto Varas', 'comuna', 'X', 'Región de Los Lagos'),
(310, 'Iquique', 'comuna', 'I', 'Región de Tarapacá'),
(311, 'Punta Arenas', 'comuna', 'XII', 'Región de Magallanes'),
(312, 'Lago Ranco', 'comuna', 'XIV', 'Región de Los Ríos'),
(313, 'Matanzas', 'comuna', 'VI', 'Región de O''Higgins'),
(314, 'Casablanca', 'comuna', 'V', 'Región de Valparaíso'),
(315, 'Osorno', 'comuna', 'X', 'Región de Los Lagos'),
(316, 'Zapallar', 'comuna', 'V', 'Región de Valparaíso'),
(317, 'Maitencillo', 'sector', 'V', 'Región de Valparaíso'),
(318, 'Cachagua', 'sector', 'V', 'Región de Valparaíso'),
(319, 'Papudo', 'comuna', 'V', 'Región de Valparaíso'),
(320, 'Santo Domingo', 'comuna', 'V', 'Región de Valparaíso'),
(321, 'Algarrobo', 'comuna', 'V', 'Región de Valparaíso'),
(322, 'Pucón', 'comuna', 'IX', 'Región de La Araucanía'),
(323, 'Villarrica', 'comuna', 'IX', 'Región de La Araucanía');
