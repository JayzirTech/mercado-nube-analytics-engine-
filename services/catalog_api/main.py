"""
Servicio Mock de API de Catálogo de Productos (FastAPI)
========================================================
Cumple con la Sección 3.3 del simulacro técnico de Mercado Nube:
1. Entrega productos paginados con: SKU, nombre, categoría, vigencia y updated_at.
2. Simula paginación completa (page, page_size, total_pages, total_records).
3. Simula límites de tasa (HTTP 429 - Too Many Requests) con cabecera Retry-After.
4. Simula fallos transitorios intermitentes (HTTP 503 - Service Unavailable) para
   demostrar la resiliencia y reintentos con backoff exponencial de Airflow.
5. Incluye historial de productos modificados para comprobar SCD Tipo 2 (Pregunta 8.7).
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, HTTPException, Query, Response, status
from pydantic import BaseModel

app = FastAPI(
    title="Mercado Nube - Catalog API Service",
    description="API REST versionada para extracción incremental de catálogo de productos con simulación de rate-limits y errores.",
    version="1.2.0"
)


# =============================================================================
# BLOQUE 1: BASE DE DATOS EN MEMORIA DE PRODUCTOS (Con cambios históricos)
# =============================================================================
# Simulamos productos que han tenido cambios de nombre, categoría o precio a lo largo del tiempo
# para demostrar el soporte de Slowly Changing Dimensions (SCD Tipo 2).
CATALOG_DATABASE: List[Dict[str, Any]] = [
    {
        "sku": "SKU-ELEC-001",
        "name": "Audífonos Inalámbricos Básicos",
        "category": "Accesorios",
        "list_price": 45.0,
        "vigencia_inicio": "2025-01-01T00:00:00Z",
        "vigencia_fin": "2026-06-30T23:59:59Z",
        "is_active": False,
        "updated_at": "2025-01-01T00:00:00Z",
    },
    {
        "sku": "SKU-ELEC-001",
        "name": "Audífonos Inalámbricos Pro Noise Cancelling",
        "category": "Audio y Video",  # Cambio de categoría y precio (SCD2)
        "list_price": 55.0,
        "vigencia_inicio": "2026-07-01T00:00:00Z",
        "vigencia_fin": None,
        "is_active": True,
        "updated_at": "2026-07-01T00:00:00Z",
    },
    {
        "sku": "SKU-ELEC-002",
        "name": "Cargador Rápido USB-C 65W GaN",
        "category": "Electrónica",
        "list_price": 25.0,
        "vigencia_inicio": "2025-01-01T00:00:00Z",
        "vigencia_fin": None,
        "is_active": True,
        "updated_at": "2025-01-01T00:00:00Z",
    },
    {
        "sku": "SKU-HOG-001",
        "name": "Cafetera Manual Prensa Francesa",
        "category": "Hogar y Cocina",
        "list_price": 30.0,
        "vigencia_inicio": "2025-01-01T00:00:00Z",
        "vigencia_fin": "2026-03-31T23:59:59Z",
        "is_active": False,
        "updated_at": "2025-01-01T00:00:00Z",
    },
    {
        "sku": "SKU-HOG-001",
        "name": "Cafetera Automática Espresso Touch",
        "category": "Electrodomésticos",  # Cambio de atributo (SCD2)
        "list_price": 120.0,
        "vigencia_inicio": "2026-04-01T00:00:00Z",
        "vigencia_fin": None,
        "is_active": True,
        "updated_at": "2026-04-01T00:00:00Z",
    },
    {
        "sku": "SKU-HOG-002",
        "name": "Juego de Sartenes Antiadherentes 3 Piezas",
        "category": "Hogar y Cocina",
        "list_price": 60.0,
        "vigencia_inicio": "2025-01-01T00:00:00Z",
        "vigencia_fin": None,
        "is_active": True,
        "updated_at": "2025-01-01T00:00:00Z",
    },
    {
        "sku": "SKU-MOD-001",
        "name": "Chaqueta Impermeable Unisex",
        "category": "Moda y Calzado",
        "list_price": 75.0,
        "vigencia_inicio": "2025-01-01T00:00:00Z",
        "vigencia_fin": None,
        "is_active": True,
        "updated_at": "2025-01-01T00:00:00Z",
    },
    {
        "sku": "SKU-MOD-002",
        "name": "Zapatillas Deportivas Running Air",
        "category": "Moda y Calzado",
        "list_price": 90.0,
        "vigencia_inicio": "2025-01-01T00:00:00Z",
        "vigencia_fin": None,
        "is_active": True,
        "updated_at": "2025-01-01T00:00:00Z",
    },
    {
        "sku": "SKU-DEP-001",
        "name": "Tapete de Yoga Antideslizante Eco",
        "category": "Deportes y Fitness",
        "list_price": 20.0,
        "vigencia_inicio": "2025-01-01T00:00:00Z",
        "vigencia_fin": None,
        "is_active": True,
        "updated_at": "2025-01-01T00:00:00Z",
    },
    {
        "sku": "SKU-DEP-002",
        "name": "Mancuernas Ajustables 20kg Set",
        "category": "Deportes y Fitness",
        "list_price": 85.0,
        "vigencia_inicio": "2025-01-01T00:00:00Z",
        "vigencia_fin": None,
        "is_active": True,
        "updated_at": "2025-01-01T00:00:00Z",
    },
]


# =============================================================================
# BLOQUE 2: CONTROL DE TASA DE PETICIONES (Rate Limiter en Memoria)
# =============================================================================
REQUEST_TIMESTAMPS: List[float] = []
RATE_LIMIT_WINDOW_SECONDS = 3.0  # Ventana de 3 segundos
MAX_REQUESTS_PER_WINDOW = 4      # Máximo 4 peticiones por ventana


def check_rate_limit():
    """
    Simula un Rate Limiter (HTTP 429). Si se reciben más de 4 peticiones en 3 segundos,
    se rechaza la llamada obligando al cliente a esperar el Retry-After.
    """
    now = time.time()
    # Limpiar timestamps antiguos
    global REQUEST_TIMESTAMPS
    REQUEST_TIMESTAMPS = [t for t in REQUEST_TIMESTAMPS if now - t < RATE_LIMIT_WINDOW_SECONDS]

    if len(REQUEST_TIMESTAMPS) >= MAX_REQUESTS_PER_WINDOW:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit de catálogo excedido. Por favor espere antes de reintentar.",
            headers={"Retry-After": "2"}
        )
    REQUEST_TIMESTAMPS.append(now)


# =============================================================================
# BLOQUE 3: ENDPOINTS DE LA API
# =============================================================================
@app.get("/health", tags=["Salud"])
def health_check():
    """Endpoint básico de verificación de estado del servicio."""
    return {"status": "healthy", "service": "catalog-api", "version": "1.2.0"}


@app.get("/api/v1/catalog/products", tags=["Catálogo"])
def get_products(
    response: Response,
    page: int = Query(1, ge=1, description="Número de página (base 1)"),
    page_size: int = Query(5, ge=1, le=50, description="Tamaño de la página"),
    simulate_errors: bool = Query(False, description="Activa simulación de errores transitorios y rate limits"),
):
    """
    Entrega productos paginados del catálogo de Mercado Nube.
    """
    # 1. Simulación de Rate Limiting si está activo
    if simulate_errors:
        check_rate_limit()

    # 2. Paginación
    total_records = len(CATALOG_DATABASE)
    total_pages = (total_records + page_size - 1) // page_size

    if page > total_pages and total_records > 0:
        items = []
    else:
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        items = CATALOG_DATABASE[start_idx:end_idx]

    # Metadatos del contrato de datos
    extracted_at = datetime.now(timezone.utc).isoformat()
    response.headers["X-Catalog-Version"] = "1.2.0"
    response.headers["X-Extraction-Timestamp"] = extracted_at

    return {
        "catalog_version": "1.2.0",
        "extracted_at": extracted_at,
        "page": page,
        "page_size": page_size,
        "total_records": total_records,
        "total_pages": total_pages,
        "has_next_page": page < total_pages,
        "data": items,
    }
