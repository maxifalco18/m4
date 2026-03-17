-- =============================================================================
-- DDL: Olist Brazilian E-commerce Dataset — PostgreSQL / Supabase
-- =============================================================================
-- Fuente original: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
--
-- Esquema diseñado para:
--   1. Carga inicial (Full Refresh) desde CSVs de Kaggle
--   2. Sincronización incremental (CDC cursor) via Airbyte con campos de timestamp
--   3. Consultas de solo lectura desde el usuario readonly_user
--
-- Ejecutar en orden (respetar dependencias de FK):
--   1. Este archivo (DDL)
--   2. seed_data.py (carga masiva desde CSVs)
--
-- Convenciones:
--   - Nombres en snake_case (PEP-8 compatible con PySpark)
--   - Timestamps en UTC
--   - Campo updated_at en tablas con CDC habilitado
-- =============================================================================

-- ─── Eliminar tablas existentes (orden inverso de FK) ────────────────────────
DROP TABLE IF EXISTS olist_order_reviews CASCADE;
DROP TABLE IF EXISTS olist_order_payments CASCADE;
DROP TABLE IF EXISTS olist_order_items CASCADE;
DROP TABLE IF EXISTS olist_orders CASCADE;
DROP TABLE IF EXISTS olist_products CASCADE;
DROP TABLE IF EXISTS olist_sellers CASCADE;
DROP TABLE IF EXISTS olist_customers CASCADE;
DROP TABLE IF EXISTS olist_geolocation CASCADE;
DROP TABLE IF EXISTS olist_product_category_name_translation CASCADE;


-- ─── 1. GEOLOCATION ─────────────────────────────────────────────────────────
-- Carga: Full Refresh (no tiene campo de timestamp)
-- Nota: No tiene PK real — zip_code_prefix puede repetirse con distintas lat/lon.
--       Se usa como tabla de referencia geográfica.
CREATE TABLE olist_geolocation (
    geolocation_zip_code_prefix   VARCHAR(10)   NOT NULL,
    geolocation_lat               DOUBLE PRECISION NOT NULL,
    geolocation_lng               DOUBLE PRECISION NOT NULL,
    geolocation_city              VARCHAR(100),
    geolocation_state             VARCHAR(5)
);

CREATE INDEX idx_geolocation_zip ON olist_geolocation (geolocation_zip_code_prefix);
CREATE INDEX idx_geolocation_state ON olist_geolocation (geolocation_state);

COMMENT ON TABLE olist_geolocation IS 'Coordenadas geográficas por código postal brasileño. Full Refresh en Airbyte.';


-- ─── 2. CUSTOMERS ───────────────────────────────────────────────────────────
-- Carga: Full Refresh (unique_id no cambia; sin campo updated_at nativo).
--        Se agrega updated_at para habilitar CDC desde Airbyte.
CREATE TABLE olist_customers (
    customer_id                   VARCHAR(50)   PRIMARY KEY,
    customer_unique_id            VARCHAR(50)   NOT NULL,
    customer_zip_code_prefix      VARCHAR(10),
    customer_city                 VARCHAR(100),
    customer_state                VARCHAR(5),
    updated_at                    TIMESTAMP     DEFAULT NOW()
);

CREATE INDEX idx_customers_unique_id ON olist_customers (customer_unique_id);
CREATE INDEX idx_customers_state ON olist_customers (customer_state);
CREATE INDEX idx_customers_updated_at ON olist_customers (updated_at);

COMMENT ON TABLE olist_customers IS 'Clientes del marketplace. CDC: cursor en updated_at.';


-- ─── 3. SELLERS ─────────────────────────────────────────────────────────────
-- Carga: Full Refresh (catálogo de vendedores estable)
CREATE TABLE olist_sellers (
    seller_id                     VARCHAR(50)   PRIMARY KEY,
    seller_zip_code_prefix        VARCHAR(10),
    seller_city                   VARCHAR(100),
    seller_state                  VARCHAR(5),
    updated_at                    TIMESTAMP     DEFAULT NOW()
);

CREATE INDEX idx_sellers_state ON olist_sellers (seller_state);

COMMENT ON TABLE olist_sellers IS 'Vendedores del marketplace Olist. Full Refresh en Airbyte.';


-- ─── 4. PRODUCT CATEGORY TRANSLATION ────────────────────────────────────────
-- Carga: Full Refresh (lookup table pequeña, ~70 filas)
CREATE TABLE olist_product_category_name_translation (
    product_category_name         VARCHAR(100)  PRIMARY KEY,
    product_category_name_english VARCHAR(100)
);

COMMENT ON TABLE olist_product_category_name_translation IS 'Traducción de categorías del portugués al inglés. Full Refresh.';


-- ─── 5. PRODUCTS ────────────────────────────────────────────────────────────
-- Carga: Full Refresh (catálogo de productos)
CREATE TABLE olist_products (
    product_id                    VARCHAR(50)   PRIMARY KEY,
    product_category_name         VARCHAR(100),
    product_name_length           INTEGER,
    product_description_length    INTEGER,
    product_photos_qty            INTEGER,
    product_weight_g              INTEGER,
    product_length_cm             INTEGER,
    product_height_cm             INTEGER,
    product_width_cm              INTEGER,
    updated_at                    TIMESTAMP     DEFAULT NOW()
);

CREATE INDEX idx_products_category ON olist_products (product_category_name);

COMMENT ON TABLE olist_products IS 'Catálogo de productos. Full Refresh en Airbyte.';


-- ─── 6. ORDERS (tabla principal de hechos) ──────────────────────────────────
-- Carga: Incremental Append con cursor en order_purchase_timestamp
-- Esta tabla es el centro del modelo estrella.
CREATE TABLE olist_orders (
    order_id                       VARCHAR(50)   PRIMARY KEY,
    customer_id                    VARCHAR(50)   NOT NULL REFERENCES olist_customers(customer_id),
    order_status                   VARCHAR(30)   NOT NULL,
    order_purchase_timestamp       TIMESTAMP     NOT NULL,
    order_approved_at              TIMESTAMP,
    order_delivered_carrier_date   TIMESTAMP,
    order_delivered_customer_date  TIMESTAMP,
    order_estimated_delivery_date  TIMESTAMP,
    updated_at                     TIMESTAMP     DEFAULT NOW()
);

CREATE INDEX idx_orders_customer ON olist_orders (customer_id);
CREATE INDEX idx_orders_status ON olist_orders (order_status);
CREATE INDEX idx_orders_purchase_ts ON olist_orders (order_purchase_timestamp);
CREATE INDEX idx_orders_updated_at ON olist_orders (updated_at);

COMMENT ON TABLE olist_orders IS 'Órdenes de compra. CDC: cursor en order_purchase_timestamp. Tabla central del modelo estrella.';


-- ─── 7. ORDER ITEMS ─────────────────────────────────────────────────────────
-- Carga: Incremental Append con cursor en shipping_limit_date
-- Granularidad: una fila por ítem dentro de una orden.
CREATE TABLE olist_order_items (
    order_id                       VARCHAR(50)   NOT NULL REFERENCES olist_orders(order_id),
    order_item_id                  INTEGER       NOT NULL,
    product_id                     VARCHAR(50)   NOT NULL REFERENCES olist_products(product_id),
    seller_id                      VARCHAR(50)   NOT NULL REFERENCES olist_sellers(seller_id),
    shipping_limit_date            TIMESTAMP,
    price                          NUMERIC(10,2) NOT NULL,
    freight_value                  NUMERIC(10,2),
    updated_at                     TIMESTAMP     DEFAULT NOW(),
    PRIMARY KEY (order_id, order_item_id)
);

CREATE INDEX idx_order_items_product ON olist_order_items (product_id);
CREATE INDEX idx_order_items_seller ON olist_order_items (seller_id);
CREATE INDEX idx_order_items_updated_at ON olist_order_items (updated_at);

COMMENT ON TABLE olist_order_items IS 'Ítems por orden (granularidad fina). CDC: cursor en shipping_limit_date.';


-- ─── 8. ORDER PAYMENTS ─────────────────────────────────────────────────────
-- Carga: Incremental (ligada a orders — usa el order_id como referencia)
CREATE TABLE olist_order_payments (
    order_id                       VARCHAR(50)   NOT NULL REFERENCES olist_orders(order_id),
    payment_sequential             INTEGER       NOT NULL,
    payment_type                   VARCHAR(30)   NOT NULL,
    payment_installments           INTEGER,
    payment_value                  NUMERIC(10,2) NOT NULL,
    updated_at                     TIMESTAMP     DEFAULT NOW(),
    PRIMARY KEY (order_id, payment_sequential)
);

CREATE INDEX idx_payments_type ON olist_order_payments (payment_type);
CREATE INDEX idx_payments_updated_at ON olist_order_payments (updated_at);

COMMENT ON TABLE olist_order_payments IS 'Pagos por orden (múltiples métodos posibles). CDC: cursor en updated_at.';


-- ─── 9. ORDER REVIEWS ──────────────────────────────────────────────────────
-- Carga: Incremental Append con cursor en review_creation_date
CREATE TABLE olist_order_reviews (
    review_id                      VARCHAR(50)   NOT NULL,
    order_id                       VARCHAR(50)   NOT NULL REFERENCES olist_orders(order_id),
    review_score                   INTEGER       NOT NULL CHECK (review_score BETWEEN 1 AND 5),
    review_comment_title           TEXT,
    review_comment_message         TEXT,
    review_creation_date           TIMESTAMP,
    review_answer_timestamp        TIMESTAMP,
    updated_at                     TIMESTAMP     DEFAULT NOW(),
    PRIMARY KEY (review_id, order_id)
);

CREATE INDEX idx_reviews_score ON olist_order_reviews (review_score);
CREATE INDEX idx_reviews_creation ON olist_order_reviews (review_creation_date);
CREATE INDEX idx_reviews_updated_at ON olist_order_reviews (updated_at);

COMMENT ON TABLE olist_order_reviews IS 'Reseñas de clientes (1-5 estrellas). CDC: cursor en review_creation_date.';


-- =============================================================================
-- USUARIO DE SOLO LECTURA (para Airbyte)
-- =============================================================================
-- En Supabase, crear este usuario desde el panel SQL Editor.
-- La contraseña se almacena en .env (no en código).

-- Crear el role (si no existe)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'readonly_user') THEN
        CREATE ROLE readonly_user WITH LOGIN PASSWORD 'CHANGEME_USE_ENV_VAR';
    END IF;
END
$$;

-- Otorgar permisos de solo lectura
GRANT CONNECT ON DATABASE postgres TO readonly_user;
GRANT USAGE ON SCHEMA public TO readonly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly_user;

-- Asegurar que tablas futuras también sean legibles
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO readonly_user;

COMMENT ON ROLE readonly_user IS 'Usuario de solo lectura para Airbyte. Sin INSERT/UPDATE/DELETE.';
