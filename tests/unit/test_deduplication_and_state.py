"""
Pruebas Unitarias: Deduplicación y Máquina de Estados
=====================================================
Verifica la deduplicación determinista, manejo de colisiones
y ordenamiento de máquina de estados de pedidos.
"""

from datetime import datetime, timezone
import pytest
from src.application.deduplication import DeduplicationEngine
from src.application.state_machine import OrderStateMachine


def test_deduplication_drops_identical_retries_and_catches_collisions():
    engine = DeduplicationEngine()

    batch = [
        {"event_id": "EVT-1", "order_id": "ORD-1", "event_type": "created", "total": 100.0},
        # Reintento idéntico (mismo event_id y mismo contenido)
        {"event_id": "EVT-1", "order_id": "ORD-1", "event_type": "created", "total": 100.0},
        # Colisión (mismo event_id pero payload alterado)
        {"event_id": "EVT-1", "order_id": "ORD-1", "event_type": "created", "total": 999.0},
        # Nuevo evento legítimo
        {"event_id": "EVT-2", "order_id": "ORD-1", "event_type": "paid", "total": 100.0},
    ]

    uniques, collisions, metrics = engine.deduplicate(batch, natural_key_field="event_id", source_name="EVENTS")

    assert len(uniques) == 2  # EVT-1 legítimo y EVT-2
    assert len(collisions) == 1  # La colisión de contenido
    assert metrics["identical_duplicates_dropped"] == 1
    assert metrics["content_collisions_quarantined"] == 1


def test_order_state_machine_derives_paid_and_calculates_time():
    events = [
        {
            "event_id": "E1",
            "order_id": "ORD-100",
            "event_type": "created",
            "event_time": "2026-10-26T10:00:00+00:00",
            "country": "CO",
            "currency": "COP",
            "payload": {"total_local": 400000.0, "is_guest": True}
        },
        {
            "event_id": "E2",
            "order_id": "ORD-100",
            "event_type": "paid",
            "event_time": "2026-10-26T10:15:00+00:00",  # 15 minutos después (900 seg)
            "country": "CO",
            "currency": "COP",
            "payload": {"amount_paid": 400000.0}
        }
    ]

    state = OrderStateMachine.derive_order_state("ORD-100", events)

    assert state["order_status"] == "PAID"
    assert state["time_to_paid_seconds"] == 900
    assert state["customer_id"] == "GUEST_UNKNOWN"
    assert state["is_guest"] is True
