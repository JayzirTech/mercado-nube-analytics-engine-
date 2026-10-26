"""
Máquina de Estados de Pedidos y Tratamiento de Llegadas Tardías (Late Arrivals)
==============================================================================
Cumple con la Sección 5 y Decisión 4 del simulacro de Mercado Nube:
1. Deriva el estado actual del pedido mediante orden temporal explícito y desempate:
   created (1) -> paid (2) -> cancelled (3) -> refunded (4).
2. Reprocesa eventos tardíos (> 24h) recalculando el estado final de la orden
   sin duplicar filas en el destino analítico.
3. Calcula métricas temporales críticas como el tiempo de creación a pago (Pregunta 8.4).
4. Documentado por bloques con principios SOLID.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from src.domain.models import OrderEventType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("OrderStateMachine")


class OrderStateMachine:
    """
    Motor determinista para calcular el estado consolidado de pedidos a partir de eventos.
    """

    PRECEDENCE_MAP = {
        "created": 1,
        "paid": 2,
        "cancelled": 3,
        "refunded": 4,
    }

    # =========================================================================
    # BLOQUE 1: ORDENAMIENTO TEMPORAL Y DESEMPATE DETERMINISTA
    # =========================================================================
    @classmethod
    def sort_order_events(cls, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Ordena los eventos de un pedido aplicando la regla explícita:
        1. Marca de tiempo del evento (`event_time`).
        2. En caso de igualdad exacta de microsegundos, desempata por precedencia de estado.
        """
        def sort_key(ev: Dict[str, Any]):
            event_type = str(ev.get("event_type", "created")).lower()
            precedence = cls.PRECEDENCE_MAP.get(event_type, 0)
            time_str = ev.get("event_time", "")
            try:
                dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
            except Exception:
                dt = datetime.min.replace(tzinfo=timezone.utc)
            return (dt, precedence)

        return sorted(events, key=sort_key)

    # =========================================================================
    # BLOQUE 2: DERIVACIÓN DEL ESTADO FINAL CONSOLIDADO
    # =========================================================================
    @classmethod
    def derive_order_state(
        cls,
        order_id: str,
        events: List[Dict[str, Any]],
        current_processing_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Calcula el estado actual de la orden recorriendo su secuencia de eventos ordenada.

        Retorna un diccionario representativo para la tabla de hechos `fact_orders`:
        - order_id
        - current_status
        - created_at
        - paid_at
        - time_to_paid_seconds (Pregunta 8.4)
        - is_late_processed (True si alguno de los eventos llegó con más de 24h de atraso)
        """
        sorted_events = cls.sort_order_events(events)
        proc_time = current_processing_time or datetime.now(timezone.utc)

        current_status = "UNKNOWN"
        created_at: Optional[datetime] = None
        paid_at: Optional[datetime] = None
        country = "UNKNOWN"
        currency = "UNKNOWN"
        total_local = 0.0
        customer_id = None
        is_guest = False
        items: List[Dict[str, Any]] = []
        has_late_arrival = False

        for ev in sorted_events:
            ev_type = str(ev.get("event_type", "")).lower()
            ev_time_str = ev.get("event_time", "")
            try:
                ev_time = datetime.fromisoformat(ev_time_str.replace("Z", "+00:00"))
            except Exception:
                ev_time = proc_time

            # Detección de evento tardío (> 24 horas de desfase respecto a la ingestión)
            delay_seconds = (proc_time - ev_time).total_seconds()
            if delay_seconds > 86400:  # 24 horas = 86.400 segundos
                has_late_arrival = True

            country = ev.get("country", country)
            currency = ev.get("currency", currency)
            payload = ev.get("payload", {})

            if ev_type == "created":
                current_status = "CREATED"
                created_at = ev_time
                total_local = float(payload.get("total_local", 0.0))
                customer_id = payload.get("customer_id")
                is_guest = payload.get("is_guest", False)
                items = payload.get("items", [])

            elif ev_type == "paid":
                current_status = "PAID"
                paid_at = ev_time

            elif ev_type == "cancelled":
                current_status = "CANCELLED"

            elif ev_type == "refunded":
                current_status = "REFUNDED"

        # Cálculo del tiempo de conversión (created -> paid) en segundos
        time_to_paid_seconds: Optional[int] = None
        if created_at and paid_at and paid_at >= created_at:
            time_to_paid_seconds = int((paid_at - created_at).total_seconds())

        return {
            "order_id": order_id,
            "order_status": current_status,
            "country_code": country,
            "currency_code": currency,
            "customer_id": customer_id if not is_guest else "GUEST_UNKNOWN",
            "is_guest": is_guest,
            "created_at": created_at.isoformat() if created_at else None,
            "paid_at": paid_at.isoformat() if paid_at else None,
            "time_to_paid_seconds": time_to_paid_seconds,
            "gross_amount_local": total_local,
            "is_late_processed": has_late_arrival,
            "items": items,
        }
