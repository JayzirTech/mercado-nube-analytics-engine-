"""
Pruebas Unitarias: Reglas de Calidad y Cuarentena
==================================================
Valida que el motor de calidad desvíe correctamente datos anómalos
y apruebe los datos válidos sin alterar el estado.
"""

from decimal import Decimal
import pytest
from src.application.quality_engine import QualityEngine
from src.domain.rules import PriceMustBePositiveOnSalesRule, SkuMustBePresentRule, ValidTimestampRule


def test_price_must_be_positive_rule_rejects_zero_or_negative_price():
    rule = PriceMustBePositiveOnSalesRule()

    # Venta regular con precio 0 debe fallar
    bad_record = {"invoice": "1001", "quantity": 2, "price": 0.0}
    is_valid, rejection = rule.validate(bad_record, "TEST_SOURCE")
    assert not is_valid
    assert rejection is not None
    assert rejection.rejection_reason == "PRECIO_VENTA_NO_POSITIVO"

    # Devolución ('C') con cantidad negativa y precio 0 no se considera venta regular
    cancel_record = {"invoice": "C1001", "quantity": -2, "price": 10.0}
    is_valid, rejection = rule.validate(cancel_record, "TEST_SOURCE")
    assert is_valid
    assert rejection is None


def test_sku_must_be_present_rule():
    rule = SkuMustBePresentRule()

    # SKU vacío o ausente
    bad_record = {"invoice": "1002", "stock_code": ""}
    is_valid, rejection = rule.validate(bad_record, "TEST_SOURCE")
    assert not is_valid
    assert rejection.rejection_reason == "SKU_PRODUCTO_AUSENTE"

    # SKU válido
    good_record = {"invoice": "1003", "stock_code": "SKU-ELEC-001"}
    is_valid, rejection = rule.validate(good_record, "TEST_SOURCE")
    assert is_valid
    assert rejection is None


def test_quality_engine_batch_evaluation():
    engine = QualityEngine()
    batch = [
        {"invoice": "101", "stock_code": "SKU-1", "quantity": 1, "price": 20.0, "invoice_date": "2026-10-26T10:00:00Z"},
        {"invoice": "102", "stock_code": "", "quantity": 1, "price": 15.0, "invoice_date": "2026-10-26T10:00:00Z"},      # Falla SKU
        {"invoice": "103", "stock_code": "SKU-2", "quantity": 1, "price": -5.0, "invoice_date": "2026-10-26T10:00:00Z"},  # Falla Precio
    ]

    valid, rejected, metrics = engine.evaluate_batch(batch, "TEST_BATCH")
    assert len(valid) == 1
    assert len(rejected) == 2
    assert metrics["total_read"] == 3
    assert metrics["total_valid"] == 1
    assert metrics["total_rejected"] == 2
    assert metrics["rejection_rate_percent"] == 66.67
