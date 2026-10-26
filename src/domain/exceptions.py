"""
Módulo de Dominio: Excepciones de Negocio y Calidad
====================================================
Define las excepciones tipadas para clasificar errores de calidad de datos,
fallos transitorios de red y colisiones de integridad.
"""


class MercadoNubeException(Exception):
    """Excepción base para el dominio de Mercado Nube."""
    pass


# =============================================================================
# BLOQUE 1: EXCEPCIONES DE CALIDAD DE DATOS (Envío a Cuarentena)
# =============================================================================
class DataQualityViolation(MercadoNubeException):
    """
    Excepción base para violaciones de calidad que envían un registro a cuarentena
    en lugar de detener abruptamente el pipeline de datos.
    """
    def __init__(self, reason: str, identifier: str, raw_data: str):
        self.reason = reason
        self.identifier = identifier
        self.raw_data = raw_data
        super().__init__(f"Calidad violada [{reason}] para registro ID: {identifier}")


class InvalidPriceException(DataQualityViolation):
    """Lanzada cuando una venta regular tiene precio unitario menor o igual a cero."""
    pass


class MissingSkuException(DataQualityViolation):
    """Lanzada cuando un ítem o pedido carece de identificador de producto/SKU."""
    pass


class InvalidTimestampException(DataQualityViolation):
    """Lanzada cuando una fecha no se puede parsear o está corrupta."""
    pass


class DuplicateKeyCollisionException(DataQualityViolation):
    """Lanzada cuando se detecta el mismo event_id con datos de carga útiles diferentes."""
    pass


# =============================================================================
# BLOQUE 2: EXCEPCIONES OPERACIONALES (Reintentos con Backoff)
# =============================================================================
class TransientApiException(MercadoNubeException):
    """
    Lanzada ante errores transitorios de APIs externas (HTTP 500, 502, 503, 504 o Timeout)
    que ameritan un reintento automático con backoff exponencial.
    """
    pass


class RateLimitExceededException(TransientApiException):
    """
    Lanzada cuando la API de Catálogo responde con HTTP 429 (Too Many Requests),
    requiriendo una pausa incremental antes del reintento.
    """
    pass
