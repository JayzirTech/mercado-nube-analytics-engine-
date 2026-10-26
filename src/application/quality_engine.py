"""
Motor de Calidad de Datos y Cuarentena (Dead-Letter)
===================================================
Cumple con la Sección 5 y 10 del simulacro de Mercado Nube:
1. Aplica reglas de validación sin convertir silenciosamente valores inválidos en válidos.
2. Separa registros limpios de los registros corruptos.
3. Desvía los datos rechazados a la capa de cuarentena con:
   - Fuente (source)
   - Identificador (record_id)
   - Motivo exacto del rechazo (rejection_reason)
   - Timestamp de procesamiento (processed_at)
4. Emite métricas por lote (filas leídas, válidas, rechazadas y tasa de rechazo).
5. Documentado bloque a bloque con arquitectura limpia (SOLID).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple
from src.domain.models import QuarantineRecord
from src.domain.rules import (
    DataQualityRule,
    PriceMustBePositiveOnSalesRule,
    SkuMustBePresentRule,
    ValidTimestampRule,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("QualityEngine")


class QualityEngine:
    """
    Motor central de calidad de datos. Orquesta la ejecución de reglas
    y el desvío a cuarentena.
    """

    def __init__(self, rules: List[DataQualityRule] | None = None):
        """
        Inicializa el motor con las reglas de negocio (Principio de Inversión de Dependencias).
        """
        self.rules: List[DataQualityRule] = rules or [
            PriceMustBePositiveOnSalesRule(),
            SkuMustBePresentRule(),
            ValidTimestampRule(),
        ]

    # =========================================================================
    # BLOQUE 1: EVALUACIÓN DE UN LOTE DE REGISTROS
    # =========================================================================
    def evaluate_batch(
        self,
        records: List[Dict[str, Any]],
        source_name: str
    ) -> Tuple[List[Dict[str, Any]], List[QuarantineRecord], Dict[str, Any]]:
        """
        Evalúa un lote de registros aplicando la lista de reglas configuradas.

        Retorna:
            - valid_records: Registros aprobados para la capa cleaned.
            - quarantine_records: Registros rechazados con su motivo.
            - metrics: Métricas de observabilidad del lote.
        """
        valid_records: List[Dict[str, Any]] = []
        quarantine_records: List[QuarantineRecord] = []

        for record in records:
            record_is_valid = True

            for rule in self.rules:
                is_valid, rejection = rule.validate(record, source=source_name)
                if not is_valid and rejection is not None:
                    quarantine_records.append(rejection)
                    record_is_valid = False
                    break  # Un solo motivo de rechazo suficiente para desviar a cuarentena

            if record_is_valid:
                valid_records.append(record)

        total_read = len(records)
        total_valid = len(valid_records)
        total_rejected = len(quarantine_records)
        rejection_rate = (total_rejected / total_read * 100.0) if total_read > 0 else 0.0

        metrics = {
            "source": source_name,
            "total_read": total_read,
            "total_valid": total_valid,
            "total_rejected": total_rejected,
            "rejection_rate_percent": round(rejection_rate, 2),
            "evaluated_at": datetime.now(timezone.utc).isoformat(),
        }

        logger.info(
            f"[{source_name}] Calidad evaluada: {total_valid} válidos, "
            f"{total_rejected} rechazados ({rejection_rate:.2f}% tasa de rechazo)."
        )

        return valid_records, quarantine_records, metrics

    # =========================================================================
    # BLOQUE 2: PERSISTENCIA EN CAPA DE CUARENTENA (Dead-Letter)
    # =========================================================================
    def save_quarantine(
        self,
        quarantine_records: List[QuarantineRecord],
        output_dir: Path,
        batch_id: str
    ) -> Path:
        """
        Escribe los registros rechazados en la capa de cuarentena en formato JSONL.
        Cumple con el criterio de que ningún dato rechazado desaparece silenciosamente.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        quarantine_file = output_dir / f"quarantine_{batch_id}_{date_str}.jsonl"

        with open(quarantine_file, mode="a", encoding="utf-8") as f:
            for rec in quarantine_records:
                payload_dict = {
                    "source": rec.source,
                    "record_identifier": rec.record_identifier,
                    "rejection_reason": rec.rejection_reason,
                    "raw_payload": rec.raw_payload,
                    "processed_at": rec.processed_at.isoformat(),
                    "batch_id": batch_id,
                }
                f.write(json.dumps(payload_dict, ensure_ascii=False) + "\n")

        logger.info(f"Guardados {len(quarantine_records)} registros en cuarentena: {quarantine_file}")
        return quarantine_file
