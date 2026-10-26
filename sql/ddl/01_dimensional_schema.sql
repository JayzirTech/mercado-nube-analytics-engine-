-- =============================================================================
-- ESQUEMA DIMENSIONAL ANALÍTICO (DATA WAREHOUSE) - MERCADO NUBE
-- Dialecto: PostgreSQL 14+ / Amazon Redshift (Serverless y Provisionado)
-- =============================================================================
-- Este script define el modelo dimensional en estrella (Star Schema) requerido
-- por la Sección 6 del simulacro técnico de Mercado Nube.
--
-- Principios de Diseño:
-- 1. Trazabilidad completa con columnas de auditoría: source, batch_id, ingested_at, record_hash.
-- 2. Manejo de catálogo con Slowly Changing Dimensions Tipo 2 (SCD2).
-- 3. Manejo de clientes anónimos con surrogate key ('GUEST_UNKNOWN').
-- 4. Soporte para consultas analíticas de ventas brutas, netas, cancelaciones y devoluciones.
-- =============================================================================

-- Esquema lógico para analítica
CREATE SCHEMA IF NOT EXISTS dwh_analytics;

-- =============================================================================
-- TABLA 1: DIMENSIÓN FECHA (dim_date)
-- Granularidad: Un registro por cada día calendario.
-- =============================================================================
CREATE TABLE IF NOT EXISTS dwh_analytics.dim_date (
    date_id             DATE PRIMARY KEY,
    year                INT NOT NULL,
    month               INT NOT NULL,
    day                 INT NOT NULL,
    quarter             INT NOT NULL,
    day_of_week         INT NOT NULL,
    day_name            VARCHAR(20) NOT NULL,
    month_name          VARCHAR(20) NOT NULL,
    is_weekend          BOOLEAN NOT NULL
);

-- =============================================================================
-- TABLA 2: DIMENSIÓN PAÍS Y MONEDA (dim_country_currency)
-- Granularidad: Un registro por cada país operativo.
-- =============================================================================
CREATE TABLE IF NOT EXISTS dwh_analytics.dim_country_currency (
    country_code        VARCHAR(10) PRIMARY KEY, -- 'CO', 'MX', 'PE', 'UK'
    country_name        VARCHAR(100) NOT NULL,
    currency_code       VARCHAR(10) NOT NULL,    -- 'COP', 'MXN', 'PEN', 'GBP'
    latest_rate_to_usd  NUMERIC(18, 6) NOT NULL,
    updated_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- TABLA 3: DIMENSIÓN CLIENTE (dim_customer)
-- Granularidad: Un registro por cada cliente identificado + 1 fila especial para invitados.
-- =============================================================================
CREATE TABLE IF NOT EXISTS dwh_analytics.dim_customer (
    customer_sk         VARCHAR(64) PRIMARY KEY, -- Surrogate Key (SHA-256 de customer_id)
    customer_id         VARCHAR(50) NOT NULL,    -- Clave natural de origen
    customer_name       VARCHAR(150) DEFAULT 'Cliente Registrado',
    country_code        VARCHAR(10) DEFAULT 'UNKNOWN',
    is_guest            BOOLEAN NOT NULL DEFAULT FALSE,
    source              VARCHAR(50) NOT NULL,
    ingested_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Índice para búsquedas rápidas por clave natural
CREATE INDEX IF NOT EXISTS idx_dim_customer_id ON dwh_analytics.dim_customer (customer_id);

-- =============================================================================
-- TABLA 4: DIMENSIÓN PRODUCTO CON HISTORIAL (dim_product - SCD Tipo 2)
-- Granularidad: Una fila por cada versión de un producto en el tiempo.
-- Resuelve la Pregunta 8.7 (Trazabilidad de cambios de catálogo).
-- =============================================================================
CREATE TABLE IF NOT EXISTS dwh_analytics.dim_product (
    product_sk          VARCHAR(64) PRIMARY KEY, -- Surrogate Key único por versión (hash(sku + valid_from))
    sku                 VARCHAR(50) NOT NULL,    -- Clave de producto de negocio
    name                VARCHAR(255) NOT NULL,   -- Nombre en esta versión
    category            VARCHAR(100) NOT NULL,   -- Categoría en esta versión
    list_price          NUMERIC(12, 2) NOT NULL, -- Precio de lista
    valid_from          TIMESTAMP WITH TIME ZONE NOT NULL, -- Inicio de vigencia
    valid_to            TIMESTAMP WITH TIME ZONE,          -- Fin de vigencia (NULL o 9999-12-31 si activo)
    is_current          BOOLEAN NOT NULL DEFAULT TRUE,     -- TRUE si es la versión activa actual
    catalog_version     VARCHAR(20) DEFAULT '1.0.0',
    source              VARCHAR(50) NOT NULL,
    batch_id            VARCHAR(100) NOT NULL,
    ingested_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Índices para búsquedas de vigencia temporal y productos actuales
CREATE INDEX IF NOT EXISTS idx_dim_product_sku ON dwh_analytics.dim_product (sku);
CREATE INDEX IF NOT EXISTS idx_dim_product_current ON dwh_analytics.dim_product (sku, is_current);

-- =============================================================================
-- TABLA 5: HECHOS - PEDIDOS CONSOLIDADOS (fact_orders)
-- Granularidad: Un registro por cada orden o factura.
-- Idempotencia: Controlada por order_id único.
-- =============================================================================
CREATE TABLE IF NOT EXISTS dwh_analytics.fact_orders (
    order_id            VARCHAR(50) PRIMARY KEY,
    order_date          DATE NOT NULL REFERENCES dwh_analytics.dim_date (date_id),
    customer_sk         VARCHAR(64) NOT NULL REFERENCES dwh_analytics.dim_customer (customer_sk),
    country_code        VARCHAR(10) NOT NULL REFERENCES dwh_analytics.dim_country_currency (country_code),
    currency_code       VARCHAR(10) NOT NULL,
    order_status        VARCHAR(30) NOT NULL, -- 'CREATED', 'PAID', 'CANCELLED', 'REFUNDED'
    gross_amount_local  NUMERIC(16, 2) NOT NULL DEFAULT 0.00,
    gross_amount_usd    NUMERIC(16, 2) NOT NULL DEFAULT 0.00,
    discount_amount_usd NUMERIC(16, 2) NOT NULL DEFAULT 0.00,
    net_amount_usd      NUMERIC(16, 2) NOT NULL DEFAULT 0.00,
    created_at          TIMESTAMP WITH TIME ZONE,
    paid_at             TIMESTAMP WITH TIME ZONE,
    time_to_paid_seconds INT,                 -- Métrica para responder Pregunta 8.4
    is_late_arrival     BOOLEAN DEFAULT FALSE,-- Indicador de evento procesado con > 24h lag
    is_guest_order      BOOLEAN DEFAULT FALSE,
    source              VARCHAR(50) NOT NULL,
    batch_id            VARCHAR(100) NOT NULL,
    record_hash         VARCHAR(64) NOT NULL,
    ingested_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_orders_date ON dwh_analytics.fact_orders (order_date);
CREATE INDEX IF NOT EXISTS idx_fact_orders_status ON dwh_analytics.fact_orders (order_status);

-- =============================================================================
-- TABLA 6: HECHOS - DETALLE DE LÍNEAS DE PEDIDO (fact_order_items)
-- Granularidad: Un registro por cada línea de producto dentro de una factura/orden.
-- =============================================================================
CREATE TABLE IF NOT EXISTS dwh_analytics.fact_order_items (
    item_id             VARCHAR(64) PRIMARY KEY, -- Surrogate Key hash(order_id + sku + line_number)
    order_id            VARCHAR(50) NOT NULL REFERENCES dwh_analytics.fact_orders (order_id) ON DELETE CASCADE,
    product_sk          VARCHAR(64) NOT NULL REFERENCES dwh_analytics.dim_product (product_sk),
    sku                 VARCHAR(50) NOT NULL,
    quantity            INT NOT NULL,            -- Positivo en ventas, negativo si devolución en CSV
    unit_price_local    NUMERIC(14, 2) NOT NULL,
    line_total_local    NUMERIC(16, 2) NOT NULL,
    line_total_usd      NUMERIC(16, 2) NOT NULL,
    is_cancelled        BOOLEAN NOT NULL DEFAULT FALSE,
    is_refunded         BOOLEAN NOT NULL DEFAULT FALSE,
    source              VARCHAR(50) NOT NULL,
    batch_id            VARCHAR(100) NOT NULL,
    record_hash         VARCHAR(64) NOT NULL,
    ingested_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_order_items_sku ON dwh_analytics.fact_order_items (sku);

-- =============================================================================
-- TABLA 7: HECHOS - REGISTRO DE EVENTOS INDIVIDUALES (fact_order_events)
-- Granularidad: Un registro por cada evento de cambio de estado.
-- =============================================================================
CREATE TABLE IF NOT EXISTS dwh_analytics.fact_order_events (
    event_id            VARCHAR(64) PRIMARY KEY,
    order_id            VARCHAR(50) NOT NULL,
    event_type          VARCHAR(30) NOT NULL,
    event_time          TIMESTAMP WITH TIME ZONE NOT NULL,
    country_code        VARCHAR(10) NOT NULL,
    currency_code       VARCHAR(10) NOT NULL,
    is_late_arrival     BOOLEAN NOT NULL DEFAULT FALSE,
    source              VARCHAR(50) NOT NULL,
    batch_id            VARCHAR(100) NOT NULL,
    record_hash         VARCHAR(64) NOT NULL,
    ingested_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_events_order_id ON dwh_analytics.fact_order_events (order_id);
CREATE INDEX IF NOT EXISTS idx_events_time ON dwh_analytics.fact_order_events (event_time);

-- =============================================================================
-- TABLA 8: TABLA DE CUARENTENA / AUDITORÍA DE RECHAZOS (quarantine_records)
-- Granularidad: Un registro por cada anomalía o dato corrupto detectado.
-- =============================================================================
CREATE TABLE IF NOT EXISTS dwh_analytics.quarantine_records (
    quarantine_id       BIGSERIAL PRIMARY KEY,
    source              VARCHAR(50) NOT NULL,
    record_identifier   VARCHAR(100) NOT NULL,
    rejection_reason    VARCHAR(100) NOT NULL,
    raw_payload         TEXT NOT NULL,
    batch_id            VARCHAR(100) NOT NULL,
    processed_at        TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_quarantine_reason ON dwh_analytics.quarantine_records (rejection_reason);
CREATE INDEX IF NOT EXISTS idx_quarantine_batch ON dwh_analytics.quarantine_records (batch_id);
