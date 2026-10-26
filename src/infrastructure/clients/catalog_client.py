"""
Cliente HTTP para la API de Catálogo con Reintentos y Espera Incremental
========================================================================
Cumple con la Sección 3.3 y 7 del simulacro técnico de Mercado Nube:
1. Extrae productos paginados de la API de Catálogo.
2. Maneja límites de tasa (HTTP 429) respetando la cabecera 'Retry-After'.
3. Implementa reintentos con backoff exponencial para errores transitorios (HTTP 500/502/503/504).
4. Registra metadatos de versión y timestamp de extracción.
5. Documentado por bloques para fácil sustentación.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional
import requests
from src.domain.exceptions import RateLimitExceededException, TransientApiException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("CatalogClient")


class CatalogApiClient:
    """
    Cliente robusto para interactuar con la API REST de Catálogo.
    Diseñado para tolerar fallos transitorios e intermitencias de red.
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        max_retries: int = 4,
        backoff_factor: float = 1.5,
        timeout_seconds: int = 10,
    ):
        """
        Inicializa la configuración del cliente.
        :param base_url: URL base del microservicio de catálogo.
        :param max_retries: Número máximo de reintentos antes de fallar.
        :param backoff_factor: Factor multiplicador para el tiempo de espera exponencial.
        :param timeout_seconds: Timeout para llamadas HTTP.
        """
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()

    # =========================================================================
    # BLOQUE 1: PETICIÓN HTTP CON BACKOFF EXPONENCIAL
    # =========================================================================
    def _execute_with_retry(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Ejecuta una petición GET con política de reintentos:
        - Si recibe 429: Lee cabecera Retry-After y duerme el tiempo indicado.
        - Si recibe 5xx o Timeout: Aplica espera exponencial (base * backoff ^ intento).
        """
        last_exception = None

        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(f"Llamando a {url} (Intento {attempt}/{self.max_retries})...")
                response = self.session.get(url, params=params, timeout=self.timeout_seconds)

                # Caso 1: Éxito (HTTP 200)
                if response.status_code == 200:
                    return response.json()

                # Caso 2: Límite de tasa excedido (HTTP 429)
                elif response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 2))
                    logger.warning(
                        f"HTTP 429: Rate limit alcanzado. Esperando {retry_after}s antes de reintentar..."
                    )
                    time.sleep(retry_after)
                    continue

                # Caso 3: Errores transitorios del servidor (5xx)
                elif response.status_code in (500, 502, 503, 504):
                    wait_time = self.backoff_factor ** attempt
                    logger.warning(
                        f"HTTP {response.status_code}: Error transitorio. Esperando {wait_time:.1f}s..."
                    )
                    time.sleep(wait_time)
                    continue

                # Caso 4: Error no reintentable del cliente (4xx excepto 429)
                else:
                    response.raise_for_status()

            except (requests.ConnectionError, requests.Timeout) as err:
                last_exception = err
                wait_time = self.backoff_factor ** attempt
                logger.warning(f"Error de red/timeout: {err}. Reintentando en {wait_time:.1f}s...")
                time.sleep(wait_time)

        raise TransientApiException(
            f"Fallo al consultar la API de Catálogo tras {self.max_retries} intentos: {last_exception}"
        )

    # =========================================================================
    # BLOQUE 2: EXTRACCIÓN PAGINADA COMPLETA
    # =========================================================================
    def fetch_all_products(self, page_size: int = 10, simulate_errors: bool = False) -> Dict[str, Any]:
        """
        Descarga todo el catálogo de productos recorriendo todas las páginas.
        Retorna los registros junto con metadatos de auditoría (versión del catálogo y timestamp).
        """
        all_items: List[Dict[str, Any]] = []
        page = 1
        catalog_version = "UNKNOWN"
        extracted_at = ""

        while True:
            url = f"{self.base_url}/api/v1/catalog/products"
            params = {
                "page": page,
                "page_size": page_size,
                "simulate_errors": simulate_errors
            }
            body = self._execute_with_retry(url, params=params)

            catalog_version = body.get("catalog_version", "1.0.0")
            extracted_at = body.get("extracted_at", "")
            items = body.get("data", [])

            if not items:
                break

            all_items.extend(items)

            if not body.get("has_next_page", False):
                break

            page += 1

        logger.info(
            f"Extracción finalizada exitosamente: {len(all_items)} productos extraídos (Versión {catalog_version})."
        )
        return {
            "catalog_version": catalog_version,
            "extracted_at": extracted_at,
            "total_extracted": len(all_items),
            "products": all_items
        }
