"""
Script de Inicialización y Ejecución Automática (Bootstrap & Lakehouse Runner)
=============================================================================
Se ejecuta automáticamente dentro del contenedor 'data_init' al hacer:
    docker compose up   O   docker compose up -d

Comportamiento:
1. NO elimina ningún archivo existente: preserva el estado del Lakehouse.
2. Comprueba si existe el archivo CSV histórico 'online_retail_sample.csv'.
   - Si no existe: lo descarga automáticamente de la web (UCI / Kaggle) y lo extrae.
   - Si ya existe: lo valida y continúa sin re-descargar.
3. Comprueba si existe el dataset de 50.000 eventos diarios 'events_50k.jsonl'.
   - Si no existe: lo genera de forma determinista con semilla fija (seed=42).
   - Si ya existe: lo valida y continúa sin re-generar.
4. Verifica la capa Curated del Lakehouse:
   - Si no existe: ejecuta el pipeline de procesamiento (Raw -> Cleaned -> Curated -> Cuarentena).
   - Si ya existe: conserva los datos existentes y avanza rápidamente.
5. Ejecuta las 7 consultas analíticas de negocio en SQL y muestra los resultados por terminal.
6. Muestra los enlaces de Airflow UI, Catalog API y PostgreSQL DWH.
"""

from __future__ import annotations

import logging
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Asegurar codificación UTF-8 en consolas
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

# Colores y formatos ANSI para terminal
BOLD = "\033[1m"
CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RESET = "\033[0m"


def log_header(title: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"\n{BOLD}{CYAN}[{timestamp}] ==========================================================================={RESET}")
    print(f"{BOLD}{CYAN}[{timestamp}] >>> {title}{RESET}")
    print(f"{BOLD}{CYAN}[{timestamp}] ==========================================================================={RESET}", flush=True)


def log_info(msg: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"{BLUE}[{timestamp}] [*] {msg}{RESET}", flush=True)


def log_success(msg: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"{GREEN}[{timestamp}] [OK] {msg}{RESET}", flush=True)


def log_warning(msg: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"{YELLOW}[{timestamp}] [!] {msg}{RESET}", flush=True)


def ensure_retail_dataset() -> Path:
    """Verifica y descarga si es necesario el dataset histórico real."""
    log_header("PASO 1: VERIFICACIÓN DE DATOS HISTÓRICOS REALES (WEB)")
    retail_dir = PROJECT_ROOT / "data" / "lakehouse" / "raw" / "retail_history"
    csv_path = retail_dir / "online_retail_sample.csv"

    if csv_path.exists() and csv_path.stat().st_size > 1_000_000:
        log_success(f"Dataset histórico CSV existente ({csv_path.stat().st_size:,} bytes). Conservando archivo.")
        return csv_path

    log_warning("Dataset histórico CSV NO encontrado. Iniciando descarga automática desde la web (UCI / Kaggle)...")
    from scripts.download_retail_data import prepare_real_retail_data
    csv_file = prepare_real_retail_data(base_dir=PROJECT_ROOT, max_records=50000)
    log_success(f"Descarga y extracción completada: {csv_file.name} ({csv_file.stat().st_size:,} bytes).")
    return csv_file


def ensure_daily_events() -> Path:
    """Verifica y genera si es necesario el archivo de 50.000 eventos sintéticos."""
    log_header("PASO 2: VERIFICACIÓN DE EVENTOS DIARIOS SINTÉTICOS")
    events_dir = PROJECT_ROOT / "data" / "lakehouse" / "raw" / "daily_events"
    jsonl_path = events_dir / "events_50k.jsonl"

    if jsonl_path.exists() and jsonl_path.stat().st_size > 10_000_000:
        log_success(f"Dataset de eventos JSONL existente ({jsonl_path.stat().st_size:,} bytes). Conservando archivo.")
        return jsonl_path

    log_info("Dataset de eventos JSONL NO encontrado. Generando 50.000 registros deterministas (seed=42)...")
    from services.event_generator.producer import OrderEventGenerator
    generator = OrderEventGenerator(seed=42)
    path = generator.save_to_jsonl(jsonl_path, target_count=50000)
    log_success(f"Generación de 50.000 eventos completada exitosamente ({path.stat().st_size:,} bytes).")
    return path


def run_lakehouse_pipeline():
    """Ejecuta el pipeline de procesamiento Lakehouse si no ha sido generado."""
    log_header("PASO 3: ESTADO DEL LAKEHOUSE (CLEANED, CURATED & CUARENTENA)")
    curated_dir = PROJECT_ROOT / "data" / "lakehouse" / "curated" / "fact_orders"
    has_curated_data = curated_dir.exists() and any(curated_dir.glob("*.parquet"))

    if has_curated_data:
        log_success("Capa Curated (fact_orders Parquet) existente en disco. Conservando datos sin reescribir.")
        return

    from src.infrastructure.processing.lakehouse_pipeline import LakehousePipeline

    pipeline = LakehousePipeline(base_dir=PROJECT_ROOT)
    batch_id = f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    log_info(f"Capa Curated no encontrada. Iniciando transformaciones columnares en Parquet (Snappy) | Batch: {batch_id}")
    metrics = pipeline.run_full_pipeline(batch_id=batch_id)

    retail_m = metrics["retail_metrics"]
    events_m = metrics["events_metrics"]
    catalog_m = metrics["catalog_metrics"]

    log_success("Capa Cleaned y Curated generada exitosamente en Parquet:")
    log_info(f"  • Catálogo (SCD2): {catalog_m['total_records']} versiones históricas procesadas.")
    log_info(f"  • Retail Histórico: {retail_m['total_read']:,} leídos | {retail_m['published']:,} limpios | {retail_m['rejected']:,} a cuarentena.")
    log_info(f"  • Eventos Diarios: {events_m['total_read']:,} leídos | {events_m['orders_published']:,} pedidos curados | {events_m['duplicates_dropped']:,} duplicados descartados.")

    # Auditoría de cuarentena
    quarantine_dir = PROJECT_ROOT / "data" / "lakehouse" / "quarantine"
    quarantine_files = list(quarantine_dir.glob("*/*.jsonl"))
    total_q_records = 0
    for qf in quarantine_files:
        with open(qf, "r", encoding="utf-8") as f:
            total_q_records += sum(1 for line in f if line.strip())

    log_success(f"Cuarentena (Dead-Letter) auditada: {total_q_records:,} registros aislados con motivo rastreable.")


def run_analytics_queries():
    """Ejecuta las 7 consultas de negocio en SQL y muestra las tablas en la terminal."""
    log_header("PASO 4: EJECUCIÓN DE LAS 7 CONSULTAS ANALÍTICAS DE NEGOCIO (SQL)")
    from scripts.run_analytics_demo import run_demonstration
    run_demonstration()


def main():
    start_time = time.time()
    print(f"\n{BOLD}{GREEN}")
    print("*" * 75)
    print("   MERCADO NUBE DATA PLATFORM — INICIALIZACIÓN AUTOMÁTICA")
    print("   Cohorte 6 Avanzado AWS | Docker Compose Startup")
    print("*" * 75)
    print(f"{RESET}", flush=True)

    # 1. Comprobación de datos raw (descarga o generación sólo si faltan)
    ensure_retail_dataset()
    ensure_daily_events()

    # 2. Procesamiento Lakehouse (sólo si no está ya curado)
    run_lakehouse_pipeline()

    # 3. Demostración Analítica
    run_analytics_queries()

    # 4. Resumen final de servicios
    elapsed = time.time() - start_time
    log_header("RESUMEN DE INICIALIZACIÓN COMPLETADA")
    print(f"{GREEN}{BOLD}✔ Comprobación completada en {elapsed:.2f} segundos.{RESET}")
    print(f"{BOLD}Servicios locales activos:{RESET}")
    print(f"  • {BOLD}Apache Airflow UI:{RESET}     http://localhost:8080 (admin / admin)")
    print(f"  • {BOLD}API Mock Catálogo:{RESET}     http://localhost:8000/docs")
    print(f"  • {BOLD}PostgreSQL DWH:{RESET}        localhost:5433 (dwh_admin / mercado_nube_dwh)")
    print(f"{GREEN}==========================================================================={RESET}\n", flush=True)


if __name__ == "__main__":
    main()
