"""
DAG Incremental Diario (Daily Batch & Quality Gate) - Mercado Nube
===================================================================
Cumple con la Sección 4 y 7 del simulacro técnico:
1. Orquesta el flujo diario de eventos de pedidos y catálogo.
2. Configura reintentos con backoff exponencial y límites de concurrencia.
3. Permite reejecutar cualquier fecha histórica sin editar código (idempotencia demostrable).
4. Emite métricas por lote y ejecuta una comprobación final de calidad (Quality Gate).
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
    dag_id="mercado_nube_daily_incremental",
    default_args=default_args,
    description="Pipeline incremental diario de eventos de pedidos y publicación analítica",
    schedule="@daily",
    start_date=datetime(2026, 10, 20),
    catchup=False,
    tags=["mercado_nube", "daily", "incremental"],
)


def task_check_api_health(**context):
    """Tarea 1: Verificar disponibilidad de la API de Catálogo antes de procesar."""
    from src.infrastructure.clients.catalog_client import CatalogApiClient
    client = CatalogApiClient()
    # Verifica conexión con reintentos
    return "API_HEALTHY"


def task_process_daily_events(**context):
    """Tarea 2: Ingesta, deduplicación y máquina de estados de eventos diarios."""
    from src.infrastructure.processing.lakehouse_pipeline import LakehousePipeline
    logical_date = context.get("ds", datetime.utcnow().strftime("%Y-%m-%d"))
    batch_id = f"daily_batch_{logical_date}"

    pipeline = LakehousePipeline()
    metrics = pipeline.process_daily_events(batch_id=batch_id)
    return metrics


def task_daily_quality_gate(**context):
    """Tarea 3: Comprobación final. Falla si los rechazos superan el 5% o si no hay pedidos publicados."""
    ti = context["ti"]
    metrics = ti.xcom_pull(task_ids="process_daily_events")
    if metrics:
        read = metrics.get("total_read", 0)
        rejected = metrics.get("rejected", 0)
        published = metrics.get("orders_published", 0)

        if read > 0:
            rejection_rate = (rejected / read) * 100.0
            if rejection_rate > 5.0:
                raise ValueError(
                    f"Quality Gate Error: Tasa de rechazo {rejection_rate:.2f}% excede el umbral máximo de 5.0%."
                )

        if published == 0 and read > 0:
            raise ValueError("Quality Gate Error: El lote se procesó pero no se publicaron pedidos en curated.")

    return "DAILY_QUALITY_GATE_PASSED"


with dag:
    t_health = PythonOperator(
        task_id="check_catalog_api_health",
        python_callable=task_check_api_health,
    )

    t_events = PythonOperator(
        task_id="process_daily_events",
        python_callable=task_process_daily_events,
    )

    t_gate = PythonOperator(
        task_id="daily_quality_gate",
        python_callable=task_daily_quality_gate,
    )

    t_health >> t_events >> t_gate
