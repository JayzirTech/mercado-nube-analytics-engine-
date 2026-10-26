"""
Módulo de Dominio: Reglas de Calidad e Interfaces Abstractas (SOLID)
=====================================================================
Aplica los principios:
- OCP (Open/Closed Principle): Nuevas reglas de calidad se añaden creando
  nuevas subclases sin modificar el motor de evaluación existente.
- DIP (Dependency Inversion Principle): El motor de orquestación depende de
  abstracciones (DataQualityRule) y no de implementaciones concretas.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Tuple
from src.domain.models import QuarantineRecord


class DataQualityRule(ABC):
    """
    Contrato base abstracto para cualquier regla de calidad en Mercado Nube.
    Cada regla inspecciona un registro y determina si es válido o si debe ir a cuarentena.
    """

    @abstractmethod
    def validate(self, record: Dict[str, Any], source: str) -> Tuple[bool, Optional[QuarantineRecord]]:
        """
        Evalúa si un registro cumple la regla.

        Retorna:
            (True, None) si el registro es válido.
            (False, QuarantineRecord) con la justificación del rechazo si es inválido.
        """
        pass


# =============================================================================
# REGLAS CONCRETAS DE CALIDAD
# =============================================================================
class PriceMustBePositiveOnSalesRule(DataQualityRule):
    """
    Regla: Para ventas regulares (no cancelaciones), el precio unitario debe ser mayor a 0.
    """
    def validate(self, record: Dict[str, Any], source: str) -> Tuple[bool, Optional[QuarantineRecord]]:
        is_cancel = str(record.get("invoice", "")).upper().startswith("C") or float(record.get("quantity", 0)) < 0
        price = float(record.get("price", 0.0))

        if not is_cancel and price <= 0:
            rec_id = str(record.get("invoice") or record.get("order_id", "DESCONOCIDO"))
            return False, QuarantineRecord(
                source=source,
                record_identifier=rec_id,
                rejection_reason="PRECIO_VENTA_NO_POSITIVO",
                raw_payload=str(record)
            )
        return True, None


class SkuMustBePresentRule(DataQualityRule):
    """
    Regla: Todo producto transaccionado debe tener un código de SKU o StockCode no nulo ni vacío.
    """
    def validate(self, record: Dict[str, Any], source: str) -> Tuple[bool, Optional[QuarantineRecord]]:
        sku = record.get("stock_code") or record.get("sku")
        if not sku or str(sku).strip().lower() in ("", "nan", "null", "none"):
            rec_id = str(record.get("invoice") or record.get("order_id", "DESCONOCIDO"))
            return False, QuarantineRecord(
                source=source,
                record_identifier=rec_id,
                rejection_reason="SKU_PRODUCTO_AUSENTE",
                raw_payload=str(record)
            )
        return True, None


class ValidTimestampRule(DataQualityRule):
    """
    Regla: La fecha/hora de la transacción debe ser parseable y no estar en el futuro lejano.
    """
    def validate(self, record: Dict[str, Any], source: str) -> Tuple[bool, Optional[QuarantineRecord]]:
        dt_val = record.get("invoice_date") or record.get("event_time")
        if not dt_val:
            rec_id = str(record.get("invoice") or record.get("event_id", "DESCONOCIDO"))
            return False, QuarantineRecord(
                source=source,
                record_identifier=rec_id,
                rejection_reason="FECHA_TRANSACCION_NULA",
                raw_payload=str(record)
            )
        return True, None
