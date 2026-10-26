"""
Módulo de Dominio: Modelos y Entidades Centrales
=================================================
Este módulo define las entidades de datos puras del negocio para Mercado Nube.
Siguiendo los principios de Clean Architecture y SOLID:
1. No contiene dependencias directas a bases de datos o servicios externos.
2. Define contratos y validaciones estrictas con Pydantic.
3. Asegura tipos de datos deterministas y cálculos de hashes criptográficos.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field, field_validator


# =============================================================================
# BLOQUE 1: ENUMERACIONES DEL NEGOCIO (Estados y Tipos de Eventos)
# =============================================================================
class OrderEventType(str, Enum):
    """
    Tipos de eventos válidos en el ciclo de vida de un pedido.
    La precedencia numérica se usa para desempate si dos eventos llegan con igual timestamp:
    created (1) -> paid (2) -> cancelled (3) -> refunded (4).
    """
    CREATED = "created"
    PAID = "paid"
    CANCELLED = "cancelled"
    REFUNDED = "refunded"

    @property
    def precedence(self) -> int:
        """Devuelve el orden de prioridad en la máquina de estados."""
        precedence_map = {
            OrderEventType.CREATED: 1,
            OrderEventType.PAID: 2,
            OrderEventType.CANCELLED: 3,
            OrderEventType.REFUNDED: 4,
        }
        return precedence_map[self]


class CountryCode(str, Enum):
    """Códigos ISO de los países de operación de Mercado Nube."""
    COLOMBIA = "CO"
    MEXICO = "MX"
    PERU = "PE"
    UNITED_KINGDOM = "UK"  # Presente en dataset de retail histórico
    UNKNOWN = "UNKNOWN"


class CurrencyCode(str, Enum):
    """Monedas soportadas."""
    COP = "COP"  # Peso Colombiano
    MXN = "MXN"  # Peso Mexicano
    PEN = "PEN"  # Sol Peruano
    USD = "USD"  # Dólar (Moneda de Reporte Unificada)
    GBP = "GBP"  # Libra Esterlina (Dataset histórico)


# =============================================================================
# BLOQUE 2: MODELO DE EVENTOS DIARIOS (JSONL de 50.000 registros)
# =============================================================================
class OrderEvent(BaseModel):
    """
    Representa un evento transaccional emitido por la plataforma web o comercios aliados.
    Cumple con el contrato exigido en la Sección 3.2 del simulacro.
    """
    event_id: str = Field(..., description="Identificador único del evento (UUIDv4/v5)")
    order_id: str = Field(..., description="Identificador único de la orden de compra")
    event_type: OrderEventType = Field(..., description="Tipo de evento (created, paid, cancelled, refunded)")
    event_time: datetime = Field(..., description="Marca temporal del evento en UTC")
    country: str = Field(..., description="Código de país de origen")
    currency: str = Field(..., description="Moneda en la que se transó")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Datos adicionales (items, total, cliente)")

    # Campos de auditoría interna
    ingested_at: Optional[datetime] = Field(default=None, description="Timestamp UTC cuando el pipeline leyó el evento")
    is_late_arrival: bool = Field(default=False, description="True si llegó con retraso > 24 horas")

    @field_validator("country")
    def normalize_country(cls, value: str) -> str:
        """Normaliza el código de país a mayúsculas y formato estándar ISO."""
        val = value.strip().upper()
        mapping = {
            "COLOMBIA": "CO", "COL": "CO", "CO": "CO",
            "MEXICO": "MX", "MEX": "MX", "MX": "MX",
            "PERU": "PE", "PER": "PE", "PE": "PE",
            "UK": "UK", "UNITED KINGDOM": "UK"
        }
        return mapping.get(val, val)

    @field_validator("currency")
    def normalize_currency(cls, value: str) -> str:
        """Normaliza la moneda a 3 letras mayúsculas ISO 4217."""
        return value.strip().upper()

    def calculate_content_hash(self) -> str:
        """
        Calcula un hash SHA-256 determinista a partir del contenido clave del evento.
        Se usa para resolver la Decisión 3 de deduplicación:
        - Mismo event_id + mismo hash = Reintento duplicado idéntico (descarte seguro).
        - Mismo event_id + diferente hash = Colisión con contenido alterado (envío a cuarentena).
        """
        payload_canonical = json.dumps(self.payload, sort_keys=True, default=str)
        content_string = (
            f"{self.order_id}|{self.event_type.value}|"
            f"{self.event_time.isoformat()}|{self.country}|"
            f"{self.currency}|{payload_canonical}"
        )
        return hashlib.sha256(content_string.encode("utf-8")).hexdigest()


# =============================================================================
# BLOQUE 3: MODELO DE HISTÓRICO DE PEDIDOS (Dataset Online Retail II)
# =============================================================================
class RawRetailRecord(BaseModel):
    """
    Representa una línea cruda del archivo CSV Online Retail II.
    Maneja nulos en CustomerID, facturas canceladas ('C') y cantidades negativas.
    """
    invoice: str = Field(..., description="Número de factura (puede iniciar con 'C' si es cancelación)")
    stock_code: str = Field(..., description="Identificador del producto/SKU")
    description: Optional[str] = Field(default="Sin Descripción", description="Descripción del producto")
    quantity: int = Field(..., description="Cantidad vendida o devuelta (negativo si devolución)")
    invoice_date: datetime = Field(..., description="Fecha y hora de la transacción")
    price: Decimal = Field(..., description="Precio unitario en moneda origen")
    customer_id: Optional[str] = Field(default=None, description="Identificador del cliente (puede ser nulo)")
    country: str = Field(..., description="País del cliente")

    @property
    def is_cancellation(self) -> bool:
        """Indica si el registro es una cancelación/devolución formal."""
        return self.invoice.strip().upper().startswith("C") or self.quantity < 0

    @property
    def is_guest_customer(self) -> bool:
        """Indica si la compra fue realizada por un cliente no registrado/anónimo."""
        return self.customer_id is None or str(self.customer_id).strip() in ("", "nan", "null")

    def get_effective_customer_id(self) -> str:
        """
        Aplica la Decisión 5: Asigna un identificador representativo para clientes anónimos
        sin inventar una identidad real falsa, preservando la trazabilidad.
        """
        if self.is_guest_customer:
            return "GUEST_UNKNOWN"
        return str(self.customer_id).strip().split(".")[0]  # Limpia posibles floats como 12345.0


# =============================================================================
# BLOQUE 4: MODELO DE CATÁLOGO DE PRODUCTOS (SCD Tipo 2)
# =============================================================================
class ProductCatalog(BaseModel):
    """
    Entidad de Catálogo de Productos con soporte para SCD Tipo 2 (Slowly Changing Dimensions).
    Resuelve la Pregunta 8.7 conservando el historial de modificaciones de atributos.
    """
    sku: str = Field(..., description="Código único de producto (Stock Keeping Unit)")
    name: str = Field(..., description="Nombre del producto")
    category: str = Field(..., description="Categoría asignada")
    list_price: Decimal = Field(..., description="Precio de lista oficial")
    valid_from: datetime = Field(..., description="Inicio de vigencia de esta versión")
    valid_to: Optional[datetime] = Field(default=None, description="Fin de vigencia (None si está activo)")
    is_current: bool = Field(default=True, description="True si es la versión vigente actual")
    updated_at: datetime = Field(..., description="Timestamp de última modificación en la API origen")


# =============================================================================
# BLOQUE 5: MODELO DE TASAS DE CAMBIO (Moneda y Normalización)
# =============================================================================
class ExchangeRate(BaseModel):
    """
    Tasa de conversión de divisas con fecha efectiva para normalización contable a USD.
    Cumple con la Decisión 6 para permitir consolidación entre Colombia, México y Perú.
    """
    base_currency: str = Field(..., description="Moneda origen (ej. COP, MXN, PEN)")
    target_currency: str = Field(default="USD", description="Moneda de reporte (por defecto USD)")
    rate_to_usd: Decimal = Field(..., description="Tasa multiplicadora para convertir a USD")
    effective_date: date = Field(..., description="Fecha de vigencia de la tasa")


# =============================================================================
# BLOQUE 6: MODELO DE CUARENTENA / DEAD-LETTER (Gestión de Rechazos)
# =============================================================================
class QuarantineRecord(BaseModel):
    """
    Registro de datos rechazados por incumplimiento de reglas de calidad.
    Cumple con la Sección 5: 'Los errores se registran en una tabla de cuarentena
    con fuente, identificador, motivo y fecha de procesamiento'.
    """
    source: str = Field(..., description="Fuente de origen (CSV_RETAIL, DAILY_EVENTS, CATALOG_API)")
    record_identifier: str = Field(..., description="ID de factura, order_id o sku con error")
    rejection_reason: str = Field(..., description="Causa exacta del rechazo (ej. PRECIO_NO_POSITIVO)")
    raw_payload: str = Field(..., description="Contenido íntegro del registro en formato JSON string")
    processed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Fecha/hora UTC del descarte")
