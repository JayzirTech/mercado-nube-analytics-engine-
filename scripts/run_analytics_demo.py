"""
Demostrador Analítico Interactivo de Mercado Nube
==================================================
Ejecuta las 7 preguntas de negocio del simulacro técnico sobre los datos
curados y de cuarentena producidos por el Lakehouse Pipeline.
Muestra las tablas formateadas en consola y verifica la reconciliación de datos.
"""

from __future__ import annotations

import glob
from datetime import date, datetime, timedelta
from pathlib import Path
import duckdb


def build_analytics_engine() -> duckdb.DuckDBPyConnection:
    """
    Inicializa una sesión analítica en DuckDB, crea el esquema dwh_analytics,
    crea las dimensiones auxiliares (dim_date, dim_country_currency)
    y carga las tablas de hechos y dimensiones a partir de los Parquet curados.
    """
    con = duckdb.connect()
    base_dir = Path(__file__).resolve().parents[1]
    curated_dir = base_dir / "data" / "lakehouse" / "curated"
    quarantine_dir = base_dir / "data" / "lakehouse" / "quarantine"

    con.execute("CREATE SCHEMA IF NOT EXISTS dwh_analytics;")

    # 1. Dimensión Fecha (dim_date)
    con.execute("""
        CREATE TABLE dwh_analytics.dim_date AS
        WITH date_series AS (
            SELECT CAST('2025-01-01' AS DATE) + INTERVAL (n) DAY AS d
            FROM range(0, 730) t(n)
        )
        SELECT 
            d AS date_id,
            EXTRACT(YEAR FROM d)::INT AS year,
            EXTRACT(MONTH FROM d)::INT AS month,
            EXTRACT(DAY FROM d)::INT AS day,
            EXTRACT(QUARTER FROM d)::INT AS quarter,
            EXTRACT(ISODOW FROM d)::INT AS day_of_week,
            STRFTIME(d, '%A') AS day_name,
            STRFTIME(d, '%B') AS month_name,
            (EXTRACT(ISODOW FROM d) IN (6, 7)) AS is_weekend
        FROM date_series;
    """)

    # 2. Dimensión País y Moneda (dim_country_currency)
    con.execute("""
        CREATE TABLE dwh_analytics.dim_country_currency (
            country_code VARCHAR PRIMARY KEY,
            country_name VARCHAR,
            currency_code VARCHAR,
            latest_rate_to_usd NUMERIC
        );
        INSERT INTO dwh_analytics.dim_country_currency VALUES
            ('CO', 'Colombia', 'COP', 0.00025),
            ('MX', 'México', 'MXN', 0.05714),
            ('PE', 'Perú', 'PEN', 0.26667),
            ('UK', 'Reino Unido', 'GBP', 1.27000),
            ('UNKNOWN', 'Desconocido', 'USD', 1.00000);
    """)

    # 3. Dimensión Producto (dim_product - SCD Tipo 2)
    prod_files = list(curated_dir.glob("dim_product/*.parquet"))
    if prod_files:
        latest_prod = max(prod_files, key=lambda p: p.stat().st_mtime)
        con.execute(f"CREATE TABLE dwh_analytics.dim_product AS SELECT * FROM read_parquet('{latest_prod.as_posix()}');")
    else:
        con.execute("CREATE TABLE dwh_analytics.dim_product (product_sk VARCHAR, sku VARCHAR, name VARCHAR, category VARCHAR, list_price NUMERIC, valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN);")

    # 4. Dimensión Cliente (dim_customer)
    con.execute("""
        CREATE TABLE dwh_analytics.dim_customer (
            customer_sk VARCHAR PRIMARY KEY,
            customer_id VARCHAR,
            customer_name VARCHAR,
            country_code VARCHAR,
            is_guest BOOLEAN,
            source VARCHAR,
            ingested_at TIMESTAMP
        );
        INSERT INTO dwh_analytics.dim_customer VALUES 
            ('guest_sk', 'GUEST_UNKNOWN', 'Invitado / No Registrado', 'UNKNOWN', TRUE, 'SYSTEM', CURRENT_TIMESTAMP);
    """)

    # 5. Hechos: fact_orders
    order_files = list(curated_dir.glob("fact_orders/*.parquet"))
    if order_files:
        latest_orders = max(order_files, key=lambda p: p.stat().st_mtime)
        con.execute(f"CREATE TABLE dwh_analytics.fact_orders AS SELECT * FROM read_parquet('{latest_orders.as_posix()}');")
    else:
        con.execute("CREATE TABLE dwh_analytics.fact_orders (order_id VARCHAR PRIMARY KEY, order_date DATE, customer_sk VARCHAR, country_code VARCHAR, currency_code VARCHAR, order_status VARCHAR, gross_amount_local NUMERIC, gross_amount_usd NUMERIC, discount_amount_usd NUMERIC, net_amount_usd NUMERIC, time_to_paid_seconds INT, is_late_arrival BOOLEAN);")

    # 6. Hechos: fact_order_items
    item_files = list(curated_dir.glob("fact_order_items/*.parquet"))
    if item_files:
        latest_items = max(item_files, key=lambda p: p.stat().st_mtime)
        con.execute(f"CREATE TABLE dwh_analytics.fact_order_items AS SELECT * FROM read_parquet('{latest_items.as_posix()}');")
    else:
        con.execute("CREATE TABLE dwh_analytics.fact_order_items (item_id VARCHAR PRIMARY KEY, order_id VARCHAR, product_sk VARCHAR, sku VARCHAR, quantity INT, unit_price_local NUMERIC, line_total_local NUMERIC, line_total_usd NUMERIC, is_cancelled BOOLEAN, is_refunded BOOLEAN);")

    # 7. Hechos: fact_order_events
    event_files = list(curated_dir.glob("fact_order_events/*.parquet"))
    if event_files:
        latest_events = max(event_files, key=lambda p: p.stat().st_mtime)
        con.execute(f"CREATE TABLE dwh_analytics.fact_order_events AS SELECT * FROM read_parquet('{latest_events.as_posix()}');")
    else:
        con.execute("CREATE TABLE dwh_analytics.fact_order_events (event_id VARCHAR PRIMARY KEY, order_id VARCHAR, event_type VARCHAR, event_time TIMESTAMP, country_code VARCHAR, currency_code VARCHAR, is_late_arrival BOOLEAN);")

    # 8. Cuarentena: quarantine_records
    con.execute("""
        CREATE TABLE dwh_analytics.quarantine_records (
            quarantine_id BIGINT,
            source VARCHAR,
            record_identifier VARCHAR,
            rejection_reason VARCHAR,
            raw_payload VARCHAR,
            batch_id VARCHAR,
            processed_at TIMESTAMP
        );
    """)

    # Leer archivos JSONL de cuarentena si existen
    quarantine_files = list(quarantine_dir.glob("*/*.jsonl"))
    for qf in quarantine_files:
        con.execute(f"""
            INSERT INTO dwh_analytics.quarantine_records (source, record_identifier, rejection_reason, raw_payload, batch_id, processed_at)
            SELECT source, record_identifier, rejection_reason, raw_payload, batch_id, CAST(processed_at AS TIMESTAMP)
            FROM read_json_auto('{qf.as_posix()}');
        """)

    return con


def run_demonstration():
    print("=" * 80)
    print("  PLATAFORMA ANALÍTICA DE MERCADO NUBE - DEMOSTRACIÓN EN VIVO")
    print("=" * 80)
    con = build_analytics_engine()

    # Conteos generales reconciliables
    total_orders = con.execute("SELECT count(*) FROM dwh_analytics.fact_orders").fetchone()[0]
    total_items = con.execute("SELECT count(*) FROM dwh_analytics.fact_order_items").fetchone()[0]
    total_events = con.execute("SELECT count(*) FROM dwh_analytics.fact_order_events").fetchone()[0]
    total_quarantine = con.execute("SELECT count(*) FROM dwh_analytics.quarantine_records").fetchone()[0]

    print(f"\n[ESTADO DEL ALMACÉN ANALÍTICO]:")
    print(f"  • fact_orders:        {total_orders:,} registros de pedidos")
    print(f"  • fact_order_items:   {total_items:,} líneas de producto")
    print(f"  • fact_order_events:  {total_events:,} eventos trazados")
    print(f"  • quarantine_records: {total_quarantine:,} anomalías aisladas en Dead-Letter")
    print("-" * 80)

    # -------------------------------------------------------------------------
    # PREGUNTA 1
    # -------------------------------------------------------------------------
    print("\n[PREGUNTA 1]: Ventas Brutas, Devoluciones y Ventas Netas por País (USD)")
    p1 = con.execute("""
        SELECT
            d.year,
            d.month_name,
            o.country_code,
            c.country_name,
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
        ORDER BY net_sales_usd DESC
        LIMIT 6;
    """).df()
    print(p1.to_string(index=False))

    # -------------------------------------------------------------------------
    # PREGUNTA 2
    # -------------------------------------------------------------------------
    print("\n" + "-" * 80)
    print("[PREGUNTA 2]: Categorías con Mayor Ingreso y Proporción de Devoluciones")
    p2 = con.execute("""
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
            ROUND((refund_amount_usd / NULLIF(gross_revenue_usd, 0)) * 100.0, 2) AS return_rate_percent
        FROM category_metrics
        ORDER BY net_revenue_usd DESC;
    """).df()
    print(p2.to_string(index=False))

    # -------------------------------------------------------------------------
    # PREGUNTA 3
    # -------------------------------------------------------------------------
    print("\n" + "-" * 80)
    print("[PREGUNTA 3A]: Top SKUs por Venta Neta")
    p3a = con.execute("""
        SELECT
            i.sku,
            MAX(p.name) AS product_name,
            SUM(CASE WHEN NOT i.is_refunded AND NOT i.is_cancelled THEN i.quantity ELSE 0 END) AS units_sold,
            ROUND(SUM(CASE WHEN NOT i.is_refunded AND NOT i.is_cancelled THEN i.line_total_usd ELSE 0 END), 2) AS net_sales_usd
        FROM dwh_analytics.fact_order_items i
        JOIN dwh_analytics.dim_product p ON i.sku = p.sku AND p.is_current = TRUE
        GROUP BY i.sku
        ORDER BY net_sales_usd DESC
        LIMIT 5;
    """).df()
    print(p3a.to_string(index=False))

    print("\n[PREGUNTA 3B]: Top SKUs con Mayor Volumen y Tasa de Devoluciones")
    p3b = con.execute("""
        SELECT
            i.sku,
            MAX(p.name) AS product_name,
            SUM(CASE WHEN i.is_refunded THEN ABS(i.quantity) ELSE 0 END) AS return_volume,
            ROUND(SUM(CASE WHEN i.is_refunded THEN i.line_total_usd ELSE 0 END), 2) AS return_amount_usd,
            ROUND((SUM(CASE WHEN i.is_refunded THEN ABS(i.quantity) ELSE 0 END)::NUMERIC / NULLIF(SUM(ABS(i.quantity)), 0)) * 100.0, 2) AS return_rate_percent
        FROM dwh_analytics.fact_order_items i
        JOIN dwh_analytics.dim_product p ON i.sku = p.sku AND p.is_current = TRUE
        GROUP BY i.sku
        HAVING SUM(CASE WHEN i.is_refunded THEN ABS(i.quantity) ELSE 0 END) > 0
        ORDER BY return_volume DESC
        LIMIT 5;
    """).df()
    print(p3b.to_string(index=False))

    # -------------------------------------------------------------------------
    # PREGUNTA 4
    # -------------------------------------------------------------------------
    print("\n" + "-" * 80)
    print("[PREGUNTA 4]: Tiempo Promedio Creación a Pago y Tasa de Cancelación")
    p4 = con.execute("""
        SELECT
            ROUND(AVG(time_to_paid_seconds) / 60.0, 2) AS avg_minutes_to_paid,
            ROUND(AVG(time_to_paid_seconds) / 3600.0, 2) AS avg_hours_to_paid,
            COUNT(order_id) AS total_orders,
            SUM(CASE WHEN order_status = 'CANCELLED' THEN 1 ELSE 0 END) AS total_cancelled_orders,
            ROUND((SUM(CASE WHEN order_status = 'CANCELLED' THEN 1 ELSE 0 END)::NUMERIC / COUNT(order_id)) * 100.0, 2) AS cancellation_rate_percent
        FROM dwh_analytics.fact_orders;
    """).df()
    print(p4.to_string(index=False))

    # -------------------------------------------------------------------------
    # PREGUNTA 5
    # -------------------------------------------------------------------------
    print("\n" + "-" * 80)
    print("[PREGUNTA 5]: Proporción de Eventos con Retraso > 24 Horas (Late Arrivals)")
    p5 = con.execute("""
        SELECT
            COUNT(event_id) AS total_events,
            SUM(CASE WHEN is_late_arrival THEN 1 ELSE 0 END) AS late_events_count,
            ROUND((SUM(CASE WHEN is_late_arrival THEN 1 ELSE 0 END)::NUMERIC / COUNT(event_id)) * 100.0, 2) AS late_events_percent
        FROM dwh_analytics.fact_order_events;
    """).df()
    print(p5.to_string(index=False))

    # -------------------------------------------------------------------------
    # PREGUNTA 6
    # -------------------------------------------------------------------------
    print("\n" + "-" * 80)
    print("[PREGUNTA 6]: Registros Rechazados por Fuente y Motivo en Cuarentena")
    p6 = con.execute("""
        SELECT
            source,
            rejection_reason,
            COUNT(*) AS rejected_count,
            ROUND((COUNT(*)::NUMERIC / (SELECT COUNT(*) FROM dwh_analytics.quarantine_records)) * 100.0, 2) AS percentage_of_total_quarantine
        FROM dwh_analytics.quarantine_records
        GROUP BY source, rejection_reason
        ORDER BY rejected_count DESC;
    """).df()
    print(p6.to_string(index=False))

    # -------------------------------------------------------------------------
    # PREGUNTA 7
    # -------------------------------------------------------------------------
    print("\n" + "-" * 80)
    print("[PREGUNTA 7]: Historial de Catálogo con SCD Tipo 2 (Atributos Modificados)")
    p7 = con.execute("""
        SELECT
            sku,
            name,
            category,
            list_price,
            SUBSTR(valid_from, 1, 10) AS valid_from,
            COALESCE(SUBSTR(valid_to, 1, 10), 'VIGENTE') AS valid_to,
            is_current
        FROM dwh_analytics.dim_product
        WHERE sku IN (
            SELECT sku FROM dwh_analytics.dim_product GROUP BY sku HAVING COUNT(product_sk) > 1
        )
        ORDER BY sku, valid_from ASC;
    """).df()
    print(p7.to_string(index=False))

    print("\n" + "=" * 80)
    print("  DEMOSTRACIÓN FINALIZADA CON ÉXITO: 7 PREGUNTAS DE NEGOCIO RESPONDIDAS")
    print("=" * 80)


if __name__ == "__main__":
    run_demonstration()
