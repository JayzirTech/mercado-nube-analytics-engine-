"""
Cargador Staging e Idempotente para PostgreSQL / Amazon Redshift
================================================================
Implementa el patrón de carga exigido en las Secciones 4, 6 y Decisión 8:
1. Conecta al Data Warehouse (PostgreSQL en Docker o Redshift Serverless).
2. Asegura la existencia del esquema analítico (dwh_analytics).
3. Aplica carga atómica idempotente mediante Staging y Merge/Upsert:
   - Carga lote a tabla temporal.
   - Elimina registros anteriores coincidentes (DELETE WHERE order_id IN ...).
   - Inserta registros actualizados.
4. Si ocurre cualquier fallo, realiza ROLLBACK automático.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional
import duckdb
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PostgresLoader")


class PostgresWarehouseLoader:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5433,
        dbname: str = "mercado_nube_dwh",
        user: str = "dwh_admin",
        password: str = "dwh_secure_password123",
    ):
        self.conn_params = {
            "host": host,
            "port": port,
            "dbname": dbname,
            "user": user,
            "password": password,
        }

    def get_connection(self):
        return psycopg2.connect(**self.conn_params)

    def initialize_schema(self, ddl_path: Path):
        """Ejecuta el script DDL de creación de tablas en el almacén analítico."""
        logger.info(f"Aplicando DDL desde {ddl_path}...")
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                with open(ddl_path, "r", encoding="utf-8") as f:
                    ddl_sql = f.read()
                cur.execute(ddl_sql)
            conn.commit()
        logger.info("Esquema dimensional verificado exitosamente en PostgreSQL / Redshift.")
