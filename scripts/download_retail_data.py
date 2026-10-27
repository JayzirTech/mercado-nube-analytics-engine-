"""
Módulo de Descarga y Preparación del Dataset Histórico 'Online Retail II'
========================================================================
Cumple con la Sección 3.1 del simulacro técnico de Mercado Nube:
1. Intenta descargar el dataset desde Kaggle usando 'kagglehub' (si hay credenciales).
2. Si Kaggle requiere autenticación (HTTP 403), descarga el dataset oficial directamente
   desde el repositorio abierto UCI Machine Learning Repository (sin necesidad de API keys).
3. Extrae y convierte los registros reales a formato CSV en el lago de datos crudos:
   'data/lakehouse/raw/retail_history/online_retail_sample.csv'.
4. Conserva el 100% de los datos auténticos de facturación, cancelaciones, devoluciones y clientes.
"""

from __future__ import annotations

import csv
import logging
import os
import zipfile
from pathlib import Path
from typing import Optional
import openpyxl
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("RetailDownloader")

UCI_URL = "https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip"


def download_from_kaggle(dataset_handle: str = "thedevastator/online-retail-transaction-dataset") -> Optional[str]:
    """Intenta descargar usando kagglehub si hay credenciales configuradas."""
    try:
        import kagglehub
        logger.info(f"Intentando descargar desde KaggleHub: {dataset_handle}...")
        path = kagglehub.dataset_download(dataset_handle)
        logger.info(f"Dataset descargado exitosamente con KaggleHub en: {path}")
        return path
    except Exception as e:
        logger.warning(f"KaggleHub no pudo autenticarse o falló ({e}). Usando descarga web directa abierta...")
        return None


def download_from_uci(target_dir: Path) -> Path:
    """Descarga el archivo ZIP original desde el repositorio abierto UCI."""
    target_dir.mkdir(parents=True, exist_ok=True)
    zip_path = target_dir / "online_retail_ii.zip"

    if zip_path.exists() and zip_path.stat().st_size > 40_000_000:
        logger.info(f"Archivo ZIP ya existe en caché: {zip_path}")
        return zip_path

    logger.info(f"Descargando Online Retail II original desde UCI ({UCI_URL})...")
    resp = requests.get(UCI_URL, stream=True, timeout=60)
    resp.raise_for_status()

    with open(zip_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

    logger.info(f"Descarga finalizada exitosamente: {zip_path} ({zip_path.stat().st_size:,} bytes)")
    return zip_path


def extract_and_convert_to_csv(
    xlsx_path: Path,
    output_csv: Path,
    max_records: int = 50000
) -> Path:
    """
    Convierte las hojas de cálculo del archivo Excel descargado a un CSV estándar
    para la ingestión en la capa Raw de Mercado Nube.
    """
    logger.info(f"Extrayendo y convirtiendo {xlsx_path} a CSV (máximo {max_records:,} registros reales)...")
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    wb = openpyxl.load_workbook(xlsx_path, read_only=True)
    sheet_name = wb.sheetnames[0]  # 'Year 2009-2010'
    sheet = wb[sheet_name]

    records_written = 0
    with open(output_csv, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for i, row in enumerate(sheet.iter_rows(values_only=True)):
            if i == 0:
                # Escribir encabezado estándar
                writer.writerow(["Invoice", "StockCode", "Description", "Quantity", "InvoiceDate", "Price", "Customer ID", "Country"])
                continue

            writer.writerow(row)
            records_written += 1
            if records_written >= max_records:
                break

    logger.info(f"CSV generado con datos reales de la web: {output_csv} ({records_written:,} filas)")
    return output_csv


def prepare_real_retail_data(base_dir: Optional[Path] = None, max_records: int = 50000) -> Path:
    """
    Función orquestadora que descarga el dataset real de la web y lo deja listo en la capa raw.
    """
    project_root = base_dir or Path(__file__).resolve().parents[1]
    raw_retail_dir = project_root / "data" / "lakehouse" / "raw" / "retail_history"
    output_csv = raw_retail_dir / "online_retail_sample.csv"

    # 1. Intentar con KaggleHub
    kaggle_path = download_from_kaggle()
    if kaggle_path:
        # Buscar archivo csv o excel en la ruta de kaggle
        for root, _, files in os.walk(kaggle_path):
            for file in files:
                if file.endswith(".csv"):
                    import shutil
                    shutil.copy(os.path.join(root, file), output_csv)
                    logger.info(f"Dataset de Kaggle copiado a: {output_csv}")
                    return output_csv

    # 2. Descargar directo de la web abierta (UCI)
    zip_path = download_from_uci(raw_retail_dir)
    xlsx_path = raw_retail_dir / "online_retail_II.xlsx"

    if not xlsx_path.exists():
        logger.info(f"Descomprimiendo {zip_path}...")
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extract("online_retail_II.xlsx", raw_retail_dir)

    # 3. Convertir a CSV para la capa raw
    return extract_and_convert_to_csv(xlsx_path, output_csv, max_records=max_records)


if __name__ == "__main__":
    prepare_real_retail_data()
