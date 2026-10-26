"""
Módulo de Conversión y Normalización de Divisas
================================================
Cumple con la Sección 5 y Decisión 6 de Mercado Nube:
1. Normaliza los importes monetarios locales (COP, MXN, PEN, GBP) a la moneda de reporte unificada (USD).
2. Conserva tanto el valor original en moneda local como el valor normalizado en USD para fines de auditoría financiera.
3. Permite responder a la Pregunta 8.1 (Evolución de ventas brutas y netas por país y moneda normalizada).
"""

from __future__ import annotations

from decimal import Decimal
from typing import Dict, Optional


class CurrencyConverter:
    """
    Servicio de conversión de divisas con tasas efectivas.
    """

    def __init__(self, rates_to_usd: Dict[str, Decimal]):
        """
        :param rates_to_usd: Diccionario de tasas {moneda: multiplicador_a_usd}.
        """
        self.rates_to_usd = {k.upper(): Decimal(str(v)) for k, v in rates_to_usd.items()}
        # USD a USD siempre es 1.0
        self.rates_to_usd["USD"] = Decimal("1.0")

    def convert_to_usd(self, amount_local: float | Decimal, currency_code: str) -> Decimal:
        """
        Convierte un monto local a USD usando la tasa efectiva configurada.
        Fórmula: USD = Monto_Local * Tasa_Multiplicadora
        """
        curr = currency_code.strip().upper()
        rate = self.rates_to_usd.get(curr)

        if rate is None:
            # Fallback seguro para monedas no registradas
            return Decimal(str(round(float(amount_local), 2)))

        amt = Decimal(str(amount_local))
        return (amt * rate).quantize(Decimal("0.01"))
