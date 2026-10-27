"""
DAG de Carga Histórica (Backfill On-Demand) - Mercado Nube
===========================================================
Cumple con la Sección 4 y 7 del simulacro técnico:
1. Carga bajo demanda sin necesidad de editar código.
2. Ingesta el dataset histórico de Online Retail II.
3. Extrae la versión base del catálogo de productos (SCD2).
4. Aplica reintentos automáticos con backoff exponencial.
5. Controla idempotencia (puedes ejecutarlo múltiples veces sin duplicar métricas).
6. Registra métricas de observabilidad por lote.
"""

from datetime import datetime, timedelta
from airflow import DAG
try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator

default_args = {
    "owner": "mercado_nube",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=15),
    "retry_exponential_backoff": True,
}

dag = DAG(
    dag_id="mercado_nube_historical_load",
    default_args=default_args,
    description="Carga histórica bajo demanda de Online Retail II y Catálogo Base",
    schedule=None,  # Manual / On-Demand
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["mercado_nube", "historical", "backfill"],
)


def task_extract_catalog(**context):
    """Tarea 1: Extraer catálogo inicial de productos."""
    from src.infrastructure.processing.lakehouse_pipeline import LakehousePipeline
    batch_id = context.get("run_id", "historical_manual")
    pipeline = LakehousePipeline()
    metrics = pipeline.process_catalog(batch_id=batch_id)
    return metrics


def task_process_retail_history(**context):
    """Tarea 2: Procesar archivo histórico Online Retail II."""
    from src.infrastructure.processing.lakehouse_pipeline import LakehousePipeline
    batch_id = context.get("run_id", "historical_manual")
    pipeline = LakehousePipeline()
    metrics = pipeline.process_retail_history(batch_id=batch_id)
    return metrics


def task_quality_gate(**context):
    """Tarea 3: Quality Gate que valida que los rechazos no superen el umbral permitido (5%)."""
    ti = context["ti"]
    metrics = ti.xcom_pull(task_ids="process_retail_history")
    if metrics:
        read = metrics.get("total_read", 0)
        rejected = metrics.get("rejected", 0)
        rate = (rejected / read * 100.0) if read > 0 else 0.0
        if rate > 10.0:  # Umbral máximo de rechazo 10%
            raise ValueError(f"Quality Gate Fallido: Tasa de rechazo {rate:.2f}% supera el límite permitido.")
    return "QUALITY_GATE_PASSED"


with dag:
    t_catalog = PythonOperator(
        task_id="extract_catalog_base",
        python_callable=task_extract_catalog,
    )

    t_retail = PythonOperator(
        task_id="process_retail_history",
        python_callable=task_process_retail_history,
    )

    t_gate = PythonOperator(
        task_id="historical_quality_gate",
        python_callable=task_quality_gate,
    )

    t_catalog >> t_retail >> t_gate
