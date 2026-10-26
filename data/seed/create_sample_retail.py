"""
Generador de Muestra del Dataset Histórico 'Online Retail II'
=============================================================
Cumple con la Sección 3.1 del simulacro técnico de Mercado Nube:
1. Emula la estructura fiel del dataset 'Online Retail II' (Kaggle/UCI).
2. Contiene facturas normales y facturas canceladas (con prefijo 'C').
3. Contiene cantidades negativas y devoluciones.
4. Contiene valores nulos en CustomerID (clientes anónimos/invitados).
5. Contiene registros repetidos (duplicados exactos para probar deduplicación).
6. Contiene anomalías para probar la tabla de cuarentena (precios <= 0, SKUs vacíos).
7. Documentado por bloques para fácil explicación.
"""

from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path


def generate_retail_sample(
    output_path: Path,
    num_records: int = 5000,
    seed: int = 42
) -> Path:
    """
    Genera un archivo CSV sintético representativo de Online Retail II.
    """
    rng = random.Random(seed)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    base_date = datetime(2025, 1, 1, 8, 0, 0, tzinfo=timezone.utc)

    # Catálogo de productos para el histórico
    products = [
        ("SKU-ELEC-001", "Audífonos Inalámbricos Básicos", 45.0),
        ("SKU-ELEC-002", "Cargador Rápido USB-C 65W GaN", 25.0),
        ("SKU-HOG-001", "Cafetera Manual Prensa Francesa", 30.0),
        ("SKU-HOG-002", "Juego de Sartenes Antiadherentes 3 Piezas", 60.0),
        ("SKU-MOD-001", "Chaqueta Impermeable Unisex", 75.0),
        ("SKU-MOD-002", "Zapatillas Deportivas Running Air", 90.0),
        ("SKU-DEP-001", "Tapete de Yoga Antideslizante Eco", 20.0),
        ("SKU-DEP-002", "Mancuernas Ajustables 20kg Set", 85.0),
    ]

    countries = ["Colombia", "Mexico", "Peru", "United Kingdom"]

    records = []
    invoice_seq = 489000

    while len(records) < num_records:
        invoice_seq += 1
        is_cancel = rng.random() < 0.08  # 8% de cancelaciones
        invoice_no = f"C{invoice_seq}" if is_cancel else str(invoice_seq)

        country = rng.choice(countries)
        # 20% de clientes anónimos (CustomerID nulo)
        customer_id = "" if rng.random() < 0.20 else str(rng.randint(12000, 18000))

        # Fecha de la factura
        invoice_date = (base_date + timedelta(days=rng.randint(0, 180), minutes=rng.randint(0, 1440))).strftime("%Y-%m-%d %H:%M:%S")

        # Líneas de la factura (1 a 3 productos)
        num_items = rng.randint(1, 3)
        for _ in range(num_items):
            sku, desc, price = rng.choice(products)

            if is_cancel:
                qty = -rng.randint(1, 5)  # Cantidad negativa en cancelaciones
            else:
                qty = rng.randint(1, 10)

            # Inyección de anomalías leves para probar la Cuarentena
            r = rng.random()
            if r < 0.01:
                # Anomalía 1: Precio menor o igual a cero en venta
                price = 0.0
            elif r < 0.015:
                # Anomalía 2: SKU vacío
                sku = ""

            records.append({
                "Invoice": invoice_no,
                "StockCode": sku,
                "Description": desc,
                "Quantity": qty,
                "InvoiceDate": invoice_date,
                "Price": f"{price:.2f}",
                "Customer ID": customer_id,
                "Country": country,
            })

    # Inyectar duplicados exactos (3% de registros repetidos)
    num_dupes = int(num_records * 0.03)
    duplicates = [dict(rec) for rec in rng.sample(records, num_dupes)]
    records.extend(duplicates)

    # Escribir en CSV
    fieldnames = ["Invoice", "StockCode", "Description", "Quantity", "InvoiceDate", "Price", "Customer ID", "Country"]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    return output_path


if __name__ == "__main__":
    out = Path("data/lakehouse/raw/retail_history/online_retail_sample.csv")
    generate_retail_sample(out, num_records=10000, seed=42)
    print(f"Sample Retail generado exitosamente en: {out}")
