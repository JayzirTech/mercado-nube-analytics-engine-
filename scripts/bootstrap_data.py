"""
Script de Inicialización y Verificación Automática de Datos (Bootstrap)
========================================================================
Se ejecuta automáticamente al hacer 'docker compose up -d'.
Garantiza que el proyecto funcione en 1 solo comando sin pasos manuales:
1. Verifica si existe el archivo CSV histórico 'online_retail_sample.csv'.
   - Si no existe: lo descarga automáticamente de la web (UCI / Kaggle) y lo convierte a CSV.
   - Si ya existe: omite la descarga y continúa en menos de 1 segundo.
2. Verifica si existe el dataset de 50.000 eventos diarios 'events_50k.jsonl'.
   - Si no existe: lo genera de forma determinista con semilla fija (seed=42).
   - Si ya existe: omite la generación.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# Asegurar que el path del proyecto esté disponible
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [BOOTSTRAP] %(message)s")
logger = logging.getLogger("BootstrapData")


def ensure_retail_dataset():
    """Verifica y descarga si es necesario el dataset histórico real."""
    retail_dir = PROJECT_ROOT / "data" / "lakehouse" / "raw" / "retail_history"
    csv_path = retail_dir / "online_retail_sample.csv"

    if csv_path.exists() and csv_path.stat().st_size > 1_000_000:
        logger.info(f"Dataset histórico CSV ya existe ({csv_path.stat().st_size:,} bytes). Omitiendo descarga.")
        return csv_path

    logger.info("Dataset histórico CSV NO encontrado. Iniciando descarga automática desde la web...")
    from scripts.download_retail_data import prepare_real_retail_data
    return prepare_real_retail_data(base_dir=PROJECT_ROOT, max_records=50000)


def ensure_daily_events():
    """Verifica y genera si es necesario el archivo de 50.000 eventos sintéticos."""
    events_dir = PROJECT_ROOT / "data" / "lakehouse" / "raw" / "daily_events"
    jsonl_path = events_dir / "events_50k.jsonl"

    if jsonl_path.exists() and jsonl_path.stat().st_size > 10_000_000:
        logger.info(f"Dataset de eventos JSONL ya existe ({jsonl_path.stat().st_size:,} bytes). Omitiendo generación.")
        return jsonl_path

    logger.info("Dataset de eventos JSONL NO encontrado. Generando 50.000 registros deterministas (seed=42)...")
    from services.event_generator.producer import OrderEventGenerator
    generator = OrderEventGenerator(seed=42)
    return generator.save_to_jsonl(jsonl_path, target_count=50000)


def main():
    logger.info("Iniciando verificación automática del entorno de datos de Mercado Nube...")
    ensure_retail_dataset()
    ensure_daily_events()
    logger.info("Verificación completada: Todos los datos operacionales están listos para Airflow.")


if __name__ == "__main__":
    main()
