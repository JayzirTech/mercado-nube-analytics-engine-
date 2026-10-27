-- =============================================================================
-- CONSULTAS ANALÍTICAS DE NEGOCIO (MERCADO NUBE)
-- Dialecto: PostgreSQL / Amazon Redshift
-- =============================================================================
-- Este archivo responde a las 7 preguntas de negocio formuladas en la Sección 8
-- del simulacro de prueba técnica. Cada consulta incluye comentarios explicativos,
-- lógica de cálculo y justificación de métricas financieras.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- PREGUNTA 1:
-- ¿Cómo evolucionan por mes las ventas brutas, devoluciones y ventas netas
-- por país y moneda normalizada (USD)?
-- -----------------------------------------------------------------------------
-- Explicación:
-- - Venta Bruta: Pedidos pagados o creados sin cancelar.
-- - Devoluciones: Pedidos en estado REFUNDED o líneas devueltas.
-- - Venta Neta: Venta Bruta - Devoluciones.
-- -----------------------------------------------------------------------------
SELECT
    d.year,
    d.month,
    d.month_name,
    o.country_code,
    c.country_name,
    -- Moneda de reporte unificada (USD)
    ROUND(SUM(CASE WHEN o.order_status IN ('PAID', 'CREATED') THEN o.gross_amount_usd ELSE 0 END), 2) AS gross_sales_usd,
    ROUND(SUM(CASE WHEN o.order_status = 'REFUNDED' THEN o.gross_amount_usd ELSE 0 END), 2) AS returns_usd,
    ROUND(SUM(
        CASE 
            WHEN o.order_status = 'PAID' THEN o.gross_amount_usd
            WHEN o.order_status = 'REFUNDED' THEN -o.gross_amount_usd
            ELSE 0 
        END
    ), 2) AS net_sales_usd,
    COUNT(DISTINCT o.order_id) AS total_orders
FROM dwh_analytics.fact_orders o
JOIN dwh_analytics.dim_date d ON o.order_date = d.date_id
JOIN dwh_analytics.dim_country_currency c ON o.country_code = c.country_code
GROUP BY d.year, d.month, d.month_name, o.country_code, c.country_name
ORDER BY d.year DESC, d.month DESC, net_sales_usd DESC;


-- -----------------------------------------------------------------------------
-- PREGUNTA 2:
-- ¿Qué categorías generan más ingresos y cuáles tienen mayor proporción de devoluciones?
-- -----------------------------------------------------------------------------
-- Explicación:
-- - Se agrupa por categoría del producto usando la dimensión dim_product.
-- - Ingresos Totales: Suma de line_total_usd de ítems no cancelados ni devueltos.
-- - Proporción de Devoluciones: (Total Devuelto / Total Bruto) * 100.
-- -----------------------------------------------------------------------------
WITH category_metrics AS (
    SELECT
        p.category,
        ROUND(SUM(CASE WHEN NOT i.is_refunded AND NOT i.is_cancelled THEN i.line_total_usd ELSE 0 END), 2) AS net_revenue_usd,
        ROUND(SUM(CASE WHEN i.is_refunded THEN i.line_total_usd ELSE 0 END), 2) AS refund_amount_usd,
        ROUND(SUM(i.line_total_usd), 2) AS gross_revenue_usd,
        COUNT(i.item_id) AS total_items_sold
    FROM dwh_analytics.fact_order_items i
    JOIN dwh_analytics.dim_product p ON i.sku = p.sku AND p.is_current = TRUE
    GROUP BY p.category
)
SELECT
    category,
    net_revenue_usd,
    refund_amount_usd,
    gross_revenue_usd,
    total_items_sold,
    CASE 
        WHEN gross_revenue_usd > 0 
        THEN ROUND((refund_amount_usd / gross_revenue_usd) * 100.0, 2)
        ELSE 0.00 
    END AS return_rate_percent
FROM category_metrics
ORDER BY net_revenue_usd DESC;


-- -----------------------------------------------------------------------------
-- PREGUNTA 3:
-- ¿Cuáles son los 10 SKU con mayor venta neta y los 10 con más devoluciones,
-- incluyendo volumen y tasa?
-- -----------------------------------------------------------------------------
-- Explicación:
-- - Top 10 ventas netas ordenado por net_sales_usd DESC.
-- - Top 10 devoluciones ordenado por return_volume DESC y return_rate_percent DESC.
-- -----------------------------------------------------------------------------
-- 3.A: Top 10 SKUs por Venta Neta
SELECT
    i.sku,
    MAX(p.name) AS product_name,
    MAX(p.category) AS category,
    SUM(CASE WHEN NOT i.is_refunded AND NOT i.is_cancelled THEN i.quantity ELSE 0 END) AS units_sold,
    ROUND(SUM(CASE WHEN NOT i.is_refunded AND NOT i.is_cancelled THEN i.line_total_usd ELSE 0 END), 2) AS net_sales_usd
FROM dwh_analytics.fact_order_items i
JOIN dwh_analytics.dim_product p ON i.sku = p.sku AND p.is_current = TRUE
GROUP BY i.sku
ORDER BY net_sales_usd DESC
LIMIT 10;

-- 3.B: Top 10 SKUs con Más Devoluciones
SELECT
    i.sku,
    MAX(p.name) AS product_name,
    SUM(CASE WHEN i.is_refunded THEN ABS(i.quantity) ELSE 0 END) AS return_volume,
    SUM(ABS(i.quantity)) AS total_volume_handled,
    ROUND(SUM(CASE WHEN i.is_refunded THEN i.line_total_usd ELSE 0 END), 2) AS return_amount_usd,
    CASE 
        WHEN SUM(ABS(i.quantity)) > 0 
        THEN ROUND((SUM(CASE WHEN i.is_refunded THEN ABS(i.quantity) ELSE 0 END)::NUMERIC / SUM(ABS(i.quantity))) * 100.0, 2)
        ELSE 0.00 
    END AS return_rate_percent
FROM dwh_analytics.fact_order_items i
JOIN dwh_analytics.dim_product p ON i.sku = p.sku AND p.is_current = TRUE
GROUP BY i.sku
HAVING SUM(CASE WHEN i.is_refunded THEN ABS(i.quantity) ELSE 0 END) > 0
ORDER BY return_volume DESC, return_rate_percent DESC
LIMIT 10;


-- -----------------------------------------------------------------------------
-- PREGUNTA 4:
-- ¿Cuánto tarda un pedido desde creación hasta pago y qué proporción termina cancelada?
-- -----------------------------------------------------------------------------
-- Explicación:
-- - time_to_paid_seconds: Registrado en fact_orders a partir de la máquina de estados.
-- - Proporción cancelada: Órdenes en estado 'CANCELLED' sobre total de órdenes.
-- -----------------------------------------------------------------------------
SELECT
    ROUND(AVG(time_to_paid_seconds) / 60.0, 2) AS avg_minutes_to_paid,
    ROUND(AVG(time_to_paid_seconds) / 3600.0, 2) AS avg_hours_to_paid,
    COUNT(order_id) AS total_orders,
    SUM(CASE WHEN order_status = 'CANCELLED' THEN 1 ELSE 0 END) AS total_cancelled_orders,
    ROUND(
        (SUM(CASE WHEN order_status = 'CANCELLED' THEN 1 ELSE 0 END)::NUMERIC / COUNT(order_id)) * 100.0, 
        2
    ) AS cancellation_rate_percent
FROM dwh_analytics.fact_orders;


-- -----------------------------------------------------------------------------
-- PREGUNTA 5:
-- ¿Qué proporción de eventos llega tarde más de 24 horas y cómo se reprocesa?
-- -----------------------------------------------------------------------------
-- Explicación:
-- - is_late_arrival = TRUE identifica eventos recibidos con lag > 24h.
-- - Reprocesamiento: Se re-ejecuta la máquina de estados sobre el order_id
--   actualizando el estado consolidado en fact_orders sin crear registros duplicados.
-- -----------------------------------------------------------------------------
SELECT
    COUNT(event_id) AS total_events,
    SUM(CASE WHEN is_late_arrival THEN 1 ELSE 0 END) AS late_events_count,
    ROUND(
        (SUM(CASE WHEN is_late_arrival THEN 1 ELSE 0 END)::NUMERIC / COUNT(event_id)) * 100.0,
        2
    ) AS late_events_proportion_percent
FROM dwh_analytics.fact_order_events;


-- -----------------------------------------------------------------------------
-- PREGUNTA 6:
-- ¿Qué porcentaje de registros se rechaza por fuente y motivo?
-- ¿Cómo cambió durante los últimos siete lotes?
-- -----------------------------------------------------------------------------
-- Explicación:
-- - Consulta la tabla dwh_analytics.quarantine_records.
-- - Muestra la distribución por fuente y motivo de rechazo en los últimos 7 batches.
-- -----------------------------------------------------------------------------
WITH last_7_batches AS (
    SELECT DISTINCT batch_id
    FROM dwh_analytics.quarantine_records
    ORDER BY batch_id DESC
    LIMIT 7
)
SELECT
    q.batch_id,
    q.source,
    q.rejection_reason,
    COUNT(q.quarantine_id) AS rejected_count,
    ROUND(
        COUNT(q.quarantine_id)::NUMERIC / SUM(COUNT(q.quarantine_id)) OVER(PARTITION BY q.batch_id) * 100.0,
        2
    ) AS batch_rejection_percentage
FROM dwh_analytics.quarantine_records q
JOIN last_7_batches b ON q.batch_id = b.batch_id
GROUP BY q.batch_id, q.source, q.rejection_reason
ORDER BY q.batch_id DESC, rejected_count DESC;


-- -----------------------------------------------------------------------------
-- PREGUNTA 7:
-- ¿Qué productos tienen atributos de catálogo modificados durante el periodo
-- y cómo se conserva el historial?
-- -----------------------------------------------------------------------------
-- Explicación:
-- - Gracias a SCD Tipo 2 en dim_product, un SKU con más de 1 registro refleja cambios.
-- - Muestra la versión anterior (is_current=FALSE) vs la actual (is_current=TRUE),
--   los atributos que cambiaron (precio, categoría o nombre) y las fechas de vigencia.
-- -----------------------------------------------------------------------------
SELECT
    p.sku,
    p.name AS current_name,
    p.category AS current_category,
    p.list_price AS current_price,
    p.valid_from AS current_valid_from,
    COUNT(p.product_sk) OVER(PARTITION BY p.sku) AS total_versions_count,
    p.is_current
FROM dwh_analytics.dim_product p
WHERE p.sku IN (
    -- Filtrar solo aquellos SKUs que tienen más de una versión en el historial
    SELECT sku
    FROM dwh_analytics.dim_product
    GROUP BY sku
    HAVING COUNT(product_sk) > 1
)
ORDER BY p.sku, p.valid_from ASC;
