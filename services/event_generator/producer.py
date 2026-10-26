"""
Servicio Generador de Eventos Sintéticos de Pedidos (50.000 Registros JSONL)
=============================================================================
Cumple con la Sección 3.2 del simulacro técnico de Mercado Nube:
1. Generación reproducible y determinista mediante semilla fija (seed=42).
2. Genera mínimo 50.000 eventos en formato JSONL.
3. Incluye los 4 tipos de evento del ciclo de vida: created, paid, cancelled, refunded.
4. Inyecta anomalías controladas:
   - Eventos fuera de orden temporal (out-of-order).
   - Reintentos de red duplicados (idéntico event_id).
   - Eventos tardíos con desfase superior a 24 horas (late arrivals).
5. Documentado exhaustivamente por bloques de código para facilitar su sustentación.
"""

from __future__ import annotations

import argparse
import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List


# =============================================================================
# BLOQUE 1: CONFIGURACIÓN Y CATÁLOGOS BASE PARA SIMULACIÓN
# =============================================================================
COUNTRIES_CURRENCIES = [
    ("CO", "COP", 4000.0),    # Colombia: Pesos (~4.000 COP/USD)
    ("MX", "MXN", 17.5),      # México: Pesos (~17.5 MXN/USD)
    ("PE", "PEN", 3.75),      # Perú: Soles (~3.75 PEN/USD)
]

SAMPLE_SKUS = [
    ("SKU-ELEC-001", "Audífonos Inalámbricos Pro", "Electrónica", 50.0),
    ("SKU-ELEC-002", "Cargador Rápido USB-C 65W", "Electrónica", 25.0),
    ("SKU-HOG-001", "Cafetera Automática Espresso", "Hogar", 120.0),
    ("SKU-HOG-002", "Juego de Sartenes Antiadherentes", "Hogar", 60.0),
    ("SKU-MOD-001", "Chaqueta Impermeable Unisex", "Moda", 75.0),
    ("SKU-MOD-002", "Zapatillas Deportivas Running", "Moda", 90.0),
    ("SKU-DEP-001", "Tapete de Yoga Antideslizante", "Deportes", 20.0),
    ("SKU-DEP-002", "Mancuernas Ajustables 20kg", "Deportes", 85.0),
]


# =============================================================================
# BLOQUE 2: CLASE PRINCIPAL DEL GENERADOR (Sintético & Determinista)
# =============================================================================
class OrderEventGenerator:
    """
    Generador de eventos sintéticos para Mercado Nube.
    Garantiza reproducibilidad utilizando random.Random con semilla fija.
    """

    def __init__(
        self,
        seed: int = 42,
        base_date: datetime | None = None,
        duplicate_rate: float = 0.03,     # 3% de reintentos duplicados
        late_arrival_rate: float = 0.05,  # 5% de eventos con retraso > 24 horas
        out_of_order_rate: float = 0.08   # 8% de eventos intercambiados de orden
    ):
        """
        Inicializa los parámetros del generador determinista.
        """
        self.seed = seed
        self.rng = random.Random(seed)
        self.base_date = base_date or datetime(2026, 10, 20, 0, 0, 0, tzinfo=timezone.utc)
        self.duplicate_rate = duplicate_rate
        self.late_arrival_rate = late_arrival_rate
        self.out_of_order_rate = out_of_order_rate

    # =========================================================================
    # BLOQUE 3: CONSTRUCCIÓN DEL CICLO DE VIDA DE UN PEDIDO
    # =========================================================================
    def _generate_order_lifecycle(self, order_index: int) -> List[Dict[str, Any]]:
        """
        Genera la secuencia realista de eventos para una única orden:
        - Siempre nace con 'created'.
        - Un 85% pasa a 'paid'.
        - Del 15% restante, un 10% pasa a 'cancelled'.
        - De los pedidos 'paid', un 5% posterior pasa a 'refunded'.
        """
        # Determinar país y moneda
        country, currency, rate = self.rng.choice(COUNTRIES_CURRENCIES)

        # Generar identificadores deterministas
        order_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, f"order_{self.seed}_{order_index}")
        order_id = f"ORD-{country}-{order_uuid.hex[:8].upper()}"

        # Cliente (un 15% de clientes anónimos/invitados sin customer_id)
        is_guest = self.rng.random() < 0.15
        customer_id = None if is_guest else f"CUST-{self.rng.randint(1000, 9999)}"

        # Fecha base de la orden (repartida en 7 días)
        day_offset = self.rng.uniform(0, 6)
        order_created_at = self.base_date + timedelta(days=day_offset, minutes=self.rng.randint(0, 1440))

        # Items de la orden
        num_items = self.rng.randint(1, 4)
        selected_products = self.rng.sample(SAMPLE_SKUS, num_items)
        items = []
        total_usd = 0.0

        for sku, name, cat, price_usd in selected_products:
            qty = self.rng.randint(1, 3)
            price_local = round(price_usd * rate, 2)
            items.append({
                "sku": sku,
                "name": name,
                "category": cat,
                "quantity": qty,
                "unit_price_local": price_local,
                "unit_price_usd": price_usd,
            })
            total_usd += (price_usd * qty)

        total_local = round(total_usd * rate, 2)

        # 1. Evento CREATED
        events = []
        created_event_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{order_id}_created"))
        events.append({
            "event_id": created_event_id,
            "order_id": order_id,
            "event_type": "created",
            "event_time": order_created_at.isoformat(),
            "country": country,
            "currency": currency,
            "payload": {
                "customer_id": customer_id,
                "is_guest": is_guest,
                "total_local": total_local,
                "items": items
            }
        })

        # Decisión del ciclo de vida posterior
        prob = self.rng.random()
        if prob < 0.85:
            # Pasa a PAID (entre 5 minutos y 4 horas después)
            paid_delay_minutes = self.rng.randint(5, 240)
            paid_time = order_created_at + timedelta(minutes=paid_delay_minutes)
            paid_event_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{order_id}_paid"))

            events.append({
                "event_id": paid_event_id,
                "order_id": order_id,
                "event_type": "paid",
                "event_time": paid_time.isoformat(),
                "country": country,
                "currency": currency,
                "payload": {
                    "payment_method": self.rng.choice(["CREDIT_CARD", "PSE", "OXXO", "YAPE"]),
                    "amount_paid": total_local,
                    "time_to_paid_seconds": paid_delay_minutes * 60
                }
            })

            # Un 5% de los pagados termina en REFUNDED días después
            if self.rng.random() < 0.05:
                refund_delay_hours = self.rng.randint(24, 96)
                refund_time = paid_time + timedelta(hours=refund_delay_hours)
                refund_event_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{order_id}_refunded"))

                events.append({
                    "event_id": refund_event_id,
                    "order_id": order_id,
                    "event_type": "refunded",
                    "event_time": refund_time.isoformat(),
                    "country": country,
                    "currency": currency,
                    "payload": {
                        "refund_reason": self.rng.choice(["PRODUCT_DEFECT", "LATE_DELIVERY", "BUYER_REMORSE"]),
                        "amount_refunded": total_local
                    }
                })

        else:
            # Un 10% se CANCELA antes de pagar (entre 10 y 60 minutos)
            cancelled_delay = self.rng.randint(10, 60)
            cancelled_time = order_created_at + timedelta(minutes=cancelled_delay)
            cancelled_event_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{order_id}_cancelled"))

            events.append({
                "event_id": cancelled_event_id,
                "order_id": order_id,
                "event_type": "cancelled",
                "event_time": cancelled_time.isoformat(),
                "country": country,
                "currency": currency,
                "payload": {
                    "cancellation_reason": self.rng.choice(["USER_ABANDONED", "PAYMENT_TIMEOUT", "OUT_OF_STOCK"])
                }
            })

        return events

    # =========================================================================
    # BLOQUE 4: INYECCIÓN DE CASOS BORDE (Duplicados, Tardíos y Desorden)
    # =========================================================================
    def generate_dataset(self, target_count: int = 50000) -> List[Dict[str, Any]]:
        """
        Genera al menos target_count registros con anomalías distribuidas.
        """
        all_events: List[Dict[str, Any]] = []
        order_idx = 0

        # 1. Generar órdenes hasta alcanzar el volumen objetivo
        while len(all_events) < target_count:
            order_events = self._generate_order_lifecycle(order_idx)
            all_events.extend(order_events)
            order_idx += 1

        # Truncar al tamaño exacto solicitado
        all_events = all_events[:target_count]

        # 2. Inyectar eventos duplicados (reintentos idénticos)
        num_duplicates = int(target_count * self.duplicate_rate)
        duplicates = self.rng.sample(all_events, num_duplicates)
        all_events.extend(duplicates)

        # 3. Inyectar eventos tardíos (> 24 horas de retraso)
        for event in all_events:
            if self.rng.random() < self.late_arrival_rate:
                # Simular llegada tardía retrasando el event_time más de 24 horas respecto al procesamiento
                dt = datetime.fromisoformat(event["event_time"])
                dt_late = dt - timedelta(hours=self.rng.randint(25, 120))
                event["event_time"] = dt_late.isoformat()
                event["_simulated_late"] = True

        # 4. Inyectar desorden temporal (out-of-order)
        # Intercambiar parejas de eventos cercanos para simular asincronía en redes de microservicios
        num_swaps = int(len(all_events) * self.out_of_order_rate)
        for _ in range(num_swaps):
            i = self.rng.randint(0, len(all_events) - 2)
            all_events[i], all_events[i + 1] = all_events[i + 1], all_events[i]

        return all_events

    # =========================================================================
    # BLOQUE 5: ESCRITURA EN ARCHIVO JSONL
    # =========================================================================
    def save_to_jsonl(self, output_path: Path, target_count: int = 50000) -> Path:
        """
        Guarda los eventos en el formato JSONL estándar requerido por el data lake.
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)
        events = self.generate_dataset(target_count)

        with open(output_path, "w", encoding="utf-8") as f:
            for event in events:
                f.write(json.dumps(event, ensure_ascii=False) + "\n")

        return output_path


# =============================================================================
# BLOQUE 6: PUNTO DE ENTRADA CLI (Ejecución Independiente)
# =============================================================================
def main():
    parser = argparse.ArgumentParser(description="Generador determinista de eventos para Mercado Nube")
    parser.add_argument("--count", type=int, default=50000, help="Cantidad de eventos a generar (min 50.000)")
    parser.add_argument("--seed", type=int, default=42, help="Semilla determinista")
    parser.add_argument(
        "--output",
        type=str,
        default="data/lakehouse/raw/daily_events/events_50k.jsonl",
        help="Ruta destino del archivo JSONL"
    )

    args = parser.parse_args()
    generator = OrderEventGenerator(seed=args.seed)
    out_file = Path(args.output)
    print(f"[PRODUCER] Generando {args.count} eventos sintéticos deterministas con seed={args.seed}...")
    saved_file = generator.save_to_jsonl(out_file, target_count=args.count)
    print(f"[PRODUCER] Archivo generado exitosamente en: {saved_file}")


if __name__ == "__main__":
    main()
