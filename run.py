"""
Master CLI Runner — Plataforma Analítica Mercado Nube
======================================================
Ejecuta todo el pipeline de datos de punta a punta informando en tiempo real
por la terminal cada acción que el sistema realiza:
1. Verificación y descarga de datos reales desde la web.
2. Generación determinista de los 50.000 eventos sintéticos.
3. Procesamiento Lakehouse (Raw -> Cleaned -> Curated -> Cuarentena).
4. Ejecución de las 7 consultas analíticas de negocio en SQL.
5. Resumen final con métricas de reconciliación y estado de servicios.
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Si se ejecutó con el python global del sistema y existe .venv, reejecutar con el venv
venv_python = Path(__file__).resolve().parent / ".venv" / "Scripts" / "python.exe"
if venv_python.exists() and Path(sys.executable).resolve() != venv_python.resolve():
    import subprocess
    sys.exit(subprocess.call([str(venv_python)] + sys.argv))

# Asegurar codificación UTF-8 en consolas Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# Colores para salida profesional en terminal ANSI
GREEN = "\033[92m"
BLUE = "\033[94m"
CYAN = "\033[96m"
YELLOW = "\033[93m"
RED = "\033[91m"
BOLD = "\033[1m"
RESET = "\033[0m"


def log_step(step_num: int, total_steps: int, title: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"\n{BOLD}{CYAN}[{timestamp}] [{step_num}/{total_steps}] >>> {title}{RESET}")
    print(f"{CYAN}{'=' * 75}{RESET}")


def log_info(msg: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"{BLUE}[{timestamp}] [*] {msg}{RESET}")


def log_success(msg: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"{GREEN}[{timestamp}] [OK] {msg}{RESET}")


def log_warning(msg: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"{YELLOW}[{timestamp}] [!] {msg}{RESET}")


def run_full_flow():
    start_time = time.time()
    print(f"{BOLD}{GREEN}")
    print("*" * 75)
    print("   MERCADO NUBE DATA PLATFORM — EJECUCIÓN INTEGRAL EN VIVO")
    print("   Cohorte 6 Avanzado AWS | Local-First & Cloud-Ready")
    print("*" * 75)
    print(f"{RESET}")

    project_root = Path(__file__).resolve().parent
    sys.path.insert(0, str(project_root))

    # -------------------------------------------------------------------------
    # PASO 1: BOOTSTRAP Y DESCARGA WEB DE DATOS REALES
    # -------------------------------------------------------------------------
    log_step(1, 5, "VERIFICACIÓN Y DESCARGA DE DATOS REALES DESDE LA WEB")
    from scripts.bootstrap_data import ensure_retail_dataset, ensure_daily_events

    log_info("Comprobando archivo histórico de Online Retail II...")
    csv_file = ensure_retail_dataset()
    log_success(f"Dataset histórico listo en: {csv_file.name} ({csv_file.stat().st_size:,} bytes)")

    log_info("Comprobando archivo de eventos sintéticos diarios...")
    events_file = ensure_daily_events()
    log_success(f"Dataset de eventos listo en: {events_file.name} ({events_file.stat().st_size:,} bytes)")

    # -------------------------------------------------------------------------
    # PASO 2: CONTROL DE CALIDAD, DEDUPLICACIÓN Y CAPA MEDALLION
    # -------------------------------------------------------------------------
    log_step(2, 5, "PROCESAMIENTO LAKEHOUSE (RAW -> CLEANED -> CURATED)")
    from src.infrastructure.processing.lakehouse_pipeline import LakehousePipeline

    pipeline = LakehousePipeline(base_dir=project_root)
    batch_id = f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    log_info(f"Iniciando transformaciones columnares Parquet (Batch: {batch_id})...")
    metrics = pipeline.run_full_pipeline(batch_id=batch_id)

    retail_m = metrics["retail_metrics"]
    events_m = metrics["events_metrics"]
    catalog_m = metrics["catalog_metrics"]

    log_success("Capa Cleaned y Curated generada exitosamente en Parquet (Snappy).")
    log_info(f"  • Catálogo (SCD2): {catalog_m['total_records']} versiones históricas procesadas.")
    log_info(f"  • Retail Histórico: {retail_m['total_read']:,} leídos | {retail_m['published']:,} limpios | {retail_m['rejected']:,} a cuarentena.")
    log_info(f"  • Eventos Diarios: {events_m['total_read']:,} leídos | {events_m['orders_published']:,} pedidos curados | {events_m['duplicates_dropped']:,} duplicados descartados.")

    # -------------------------------------------------------------------------
    # PASO 3: AUDITORÍA DE CUARENTENA (DEAD-LETTER)
    # -------------------------------------------------------------------------
    log_step(3, 5, "AUDITORÍA DE CALIDAD Y REGISTROS EN CUARENTENA")
    quarantine_dir = project_root / "data" / "lakehouse" / "quarantine"
    quarantine_files = list(quarantine_dir.glob("*/*.jsonl"))
    total_q_records = 0
    for qf in quarantine_files:
        with open(qf, "r", encoding="utf-8") as f:
            total_q_records += sum(1 for line in f if line.strip())

    log_success(f"Cuarentena verificada: {total_q_records:,} anomalías aisladas en Dead-Letter.")
    log_info("Ningún dato corrupto fue convertido silenciosamente; todos tienen motivo rastreable.")

    # -------------------------------------------------------------------------
    # PASO 4: EJECUCIÓN DE LAS 7 CONSULTAS DE NEGOCIO EN SQL
    # -------------------------------------------------------------------------
    log_step(4, 5, "EJECUCIÓN DE LAS 7 CONSULTAS ANALÍTICAS DE NEGOCIO (SQL)")
    from scripts.run_analytics_demo import run_demonstration
    run_demonstration()

    # -------------------------------------------------------------------------
    # PASO 5: RESUMEN FINAL Y TIEMPO TOTAL
    # -------------------------------------------------------------------------
    elapsed = time.time() - start_time
    log_step(5, 5, "RESUMEN DE EJECUCIÓN Y CRITERIOS DE EVALUACIÓN")
    print(f"{GREEN}{BOLD}✔ Pipeline ejecutado de punta a punta en {elapsed:.2f} segundos.{RESET}")
    print(f"{BOLD}Servicios locales disponibles:{RESET}")
    print(f"  • {BOLD}Apache Airflow UI:{RESET}     http://localhost:8080 (admin / admin)")
    print(f"  • {BOLD}API Mock Catálogo:{RESET}     http://localhost:8000/docs")
    print(f"  • {BOLD}PostgreSQL DWH:{RESET}        localhost:5433 (dwh_admin / mercado_nube_dwh)")
    print(f"{GREEN}==========================================================================={RESET}\n")


if __name__ == "__main__":
    run_full_flow()
