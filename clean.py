"""
Script de Limpieza Total para Pruebas desde Cero (Clean Slate)
==============================================================
Elimina todos los datos y archivos generados durante las ejecuciones:
1. Capa Raw descargada de la web: online_retail_II.xlsx, zip y CSV muestra.
2. Capa Raw sintética generada: events_50k.jsonl.
3. Capa Cleaned: archivos Parquet intermedios.
4. Capa Curated: tablas del modelo dimensional (fact y dim Parquet).
5. Capa Cuarentena: logs de dead-letter con anomalías.
6. Caches temporales de Python (__pycache__, .pytest_cache).

Preserva intacto:
- Código fuente y configuración.
- Archivo semilla requerido por la rúbrica: data/seed/exchange_rates_seed.csv.

Uso:
    python clean.py
"""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

# Configurar salida UTF-8 para consolas de Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent

# Estilos ANSI para terminal
BOLD = "\033[1m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RED = "\033[91m"
RESET = "\033[0m"


def delete_contents(folder_path: Path, preserve_folder: bool = True) -> int:
    """Elimina el contenido de una carpeta sin borrar la carpeta base si preserve_folder=True."""
    if not folder_path.exists():
        return 0

    count = 0
    for item in folder_path.iterdir():
        try:
            if item.is_dir():
                shutil.rmtree(item, ignore_errors=True)
                count += 1
            elif item.is_file():
                item.unlink(missing_ok=True)
                count += 1
        except Exception as e:
            print(f"{YELLOW}[!] No se pudo eliminar {item.name}: {e}{RESET}")

    if not preserve_folder and folder_path.exists():
        shutil.rmtree(folder_path, ignore_errors=True)

    return count


def main():
    print(f"\n{BOLD}{CYAN}==========================================================================={RESET}")
    print(f"{BOLD}{CYAN}>>> MERCADO NUBE — LIMPIEZA TOTAL PARA PRUEBAS DESDE CERO{RESET}")
    print(f"{BOLD}{CYAN}==========================================================================={RESET}")

    data_dir = PROJECT_ROOT / "data" / "lakehouse"
    total_deleted = 0

    # 1. Capa Raw - Retail Histórico (Descargas web)
    retail_raw = data_dir / "raw" / "retail_history"
    deleted_retail = delete_contents(retail_raw)
    total_deleted += deleted_retail
    print(f"[*] Retail Histórico descargado (ZIP, XLSX, CSV): {deleted_retail} archivo(s) eliminados.")

    # 2. Capa Raw - Eventos Diarios (Sintéticos 50k)
    events_raw = data_dir / "raw" / "daily_events"
    deleted_events = delete_contents(events_raw)
    total_deleted += deleted_events
    print(f"[*] Eventos Diarios generados (JSONL 50k):        {deleted_events} archivo(s) eliminados.")

    # 3. Capa Cleaned
    cleaned_dir = data_dir / "cleaned"
    deleted_cleaned = delete_contents(cleaned_dir)
    total_deleted += deleted_cleaned
    print(f"[*] Capa Cleaned (Parquet intermedios):           {deleted_cleaned} elemento(s) eliminados.")

    # 4. Capa Curated
    curated_dir = data_dir / "curated"
    deleted_curated = delete_contents(curated_dir)
    total_deleted += deleted_curated
    print(f"[*] Capa Curated (Modelo estrella Parquet):       {deleted_curated} elemento(s) eliminados.")

    # 5. Capa Cuarentena
    quarantine_dir = data_dir / "quarantine"
    deleted_q = delete_contents(quarantine_dir)
    total_deleted += deleted_q
    print(f"[*] Capa Cuarentena (Dead-Letter anomalías):      {deleted_q} elemento(s) eliminados.")

    # 6. Caches temporales de Python
    deleted_cache = 0
    for cache_dir in PROJECT_ROOT.rglob("__pycache__"):
        if ".venv" not in str(cache_dir):
            shutil.rmtree(cache_dir, ignore_errors=True)
            deleted_cache += 1
    pytest_cache = PROJECT_ROOT / ".pytest_cache"
    if pytest_cache.exists():
        shutil.rmtree(pytest_cache, ignore_errors=True)
        deleted_cache += 1
    logs_dir = PROJECT_ROOT / "logs"
    if logs_dir.exists():
        deleted_cache += delete_contents(logs_dir)

    print(f"[*] Caches temporales de Python:                  {deleted_cache} elemento(s) eliminados.")

    # 7. Verificación de archivo semilla protegido
    seed_file = PROJECT_ROOT / "data" / "seed" / "exchange_rates_seed.csv"
    if seed_file.exists():
        print(f"{GREEN}[OK] Archivo semilla protegido (Rúbrica Sección 5): data/seed/exchange_rates_seed.csv PRESERVADO.{RESET}")
    else:
        print(f"{YELLOW}[!] Advertencia: data/seed/exchange_rates_seed.csv no encontrado.{RESET}")

    print(f"\n{BOLD}{GREEN}✔ Limpieza completada con éxito ({total_deleted + deleted_cache} elementos eliminados).{RESET}")
    print(f"{BOLD}Tu entorno está en estado 100% virgen (Clean Slate).{RESET}")
    print(f"\n{BOLD}Para probar el proyecto completo desde cero, ejecuta:{RESET}")
    print(f"  {CYAN}docker compose up{RESET}     (o {CYAN}docker compose up -d{RESET})\n")


if __name__ == "__main__":
    main()
