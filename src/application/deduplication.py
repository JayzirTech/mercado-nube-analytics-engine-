"""
Motor de Deduplicación Determinista (Idempotencia y Hash SHA-256)
=================================================================
Cumple con la Sección 5 y Decisión 3 del simulacro de Mercado Nube:
1. Deduplica de forma matemática y determinista usando Clave Natural + Hash SHA-256.
2. Si un ID repetido tiene el mismo hash: se trata de un reintento de red idéntico
   (se descarta silenciosamente para preservar la idempotencia).
3. Si un ID repetido tiene un hash diferente: se detecta una colisión de contenido
   (conflicto de integridad) y se envía a la tabla de cuarentena.
4. Documentado por bloques con principios SOLID.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Dict, List, Tuple
from src.domain.models import QuarantineRecord

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DeduplicationEngine")


class DeduplicationEngine:
    """
    Motor para deduplicación determinista de lotes de eventos o transacciones.
    """

    @staticmethod
    def compute_record_hash(record: Dict[str, Any], ignore_keys: List[str] | None = None) -> str:
        """
        Calcula un hash SHA-256 canónico ignorando claves variables no pertenecientes al negocio
        (como timestamps de ingestión o identificadores de lote).
        """
        keys_to_skip = set(ignore_keys or ["ingested_at", "batch_id", "_simulated_late"])
        filtered_dict = {k: v for k, v in record.items() if k not in keys_to_skip}
        canonical_str = json.dumps(filtered_dict, sort_keys=True, default=str)
        return hashlib.sha256(canonical_str.encode("utf-8")).hexdigest()

    # =========================================================================
    # BLOQUE 1: PROCESAMIENTO DEDUPLICADOR
    # =========================================================================
    def deduplicate(
        self,
        records: List[Dict[str, Any]],
        natural_key_field: str,
        source_name: str
    ) -> Tuple[List[Dict[str, Any]], List[QuarantineRecord], Dict[str, int]]:
        """
        Procesa una lista de registros eliminando duplicados idénticos y aislando colisiones.

        :param records: Lote de registros crudos.
        :param natural_key_field: Nombre del campo clave (ej. 'event_id', 'invoice').
        :param source_name: Nombre de la fuente para auditoría.
        :return: (registros_únicos, colisiones_cuarentena, métricas)
        """
        seen_keys: Dict[str, str] = {}  # {natural_key: record_hash}
        unique_records: List[Dict[str, Any]] = []
        collisions: List[QuarantineRecord] = []
        identical_duplicates_count = 0

        for rec in records:
            natural_key = str(rec.get(natural_key_field, "")).strip()

            if not natural_key:
                # Si no tiene clave, no se puede verificar duplicación, pasa directo o a calidad
                unique_records.append(rec)
                continue

            rec_hash = self.compute_record_hash(rec)
            # Añadir hash de auditoría al propio registro
            rec["record_hash"] = rec_hash

            if natural_key not in seen_keys:
                # Caso A: Registro nuevo, se registra y se conserva
                seen_keys[natural_key] = rec_hash
                unique_records.append(rec)

            else:
                existing_hash = seen_keys[natural_key]
                if existing_hash == rec_hash:
                    # Caso B: Reintento idéntico de red (Deduplicación silenciosa)
                    identical_duplicates_count += 1
                else:
                    # Caso C: Mismo ID con contenido alterado (Colisión de integridad)
                    logger.warning(
                        f"Colisión detectada para clave '{natural_key}' con contenido divergente."
                    )
                    collisions.append(
                        QuarantineRecord(
                            source=source_name,
                            record_identifier=natural_key,
                            rejection_reason="COLISION_CLAVE_CONTENIDO_DISTINTO",
                            raw_payload=str(rec)
                        )
                    )

        metrics = {
            "total_input": len(records),
            "unique_output": len(unique_records),
            "identical_duplicates_dropped": identical_duplicates_count,
            "content_collisions_quarantined": len(collisions),
        }

        logger.info(
            f"[{source_name}] Deduplicación finalizada: {len(unique_records)} únicos, "
            f"{identical_duplicates_count} reintentos descartados, {len(collisions)} colisiones en cuarentena."
        )

        return unique_records, collisions, metrics
