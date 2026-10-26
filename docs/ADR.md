# Registro de Decisiones de Arquitectura (ADR)
## Plataforma de Datos Analítica — Caso: Mercado Nube

* **Fecha:** 26 de Octubre de 2026
* **Autor:** Jayzir Martinez (Data Engineer)
* **Estado:** Aprobado / Implementado
* **Contexto:** Prueba Técnica de Desempeño — Cohorte 6 Avanzado AWS

---

## 1. Decisión de Motor de Cómputo para Transformaciones

### Contexto
El requerimiento solicita procesar un archivo histórico (~10.000 filas) y eventos diarios sintéticos (~50.000 a 500.000 filas), aplicando controles de calidad, deduplicación y normalización hacia capas Medallion en Amazon S3. Se pedía justificar entre AWS Glue (Spark), AWS Lambda, ECS/Fargate o contenedor local.

### Alternativas Evaluadas
1. **AWS Glue / EMR (Spark):**
   * *Descartado:* Los clusters de Glue tienen un tiempo de arranque en frío (*cold start*) de 2 a 3 minutos y cobran un mínimo de 2 DPUs por trabajo. Para volúmenes menores a 10 GB, la sobrecarga de coordinación de red entre nodos y el costo son desproporcionados.
2. **AWS Lambda:**
   * *Descartado:* Aunque económico, tiene límites duros de 15 minutos de ejecución y memoria máxima de 10 GB. Si el backfill histórico crece o se reprocesan semanas completas, Lambda falla por timeout.
3. **ECS/Fargate con Contenedor Python + DuckDB (Seleccionada):**
   * **Justificación:** DuckDB es un motor columnar vectorizado en memoria embebido. Procesa 50.000 a 1.000.000 de registros en menos de 3 segundos, soporta consultas SQL estándar y escribe directamente en formato Parquet comprimido con Snappy.
   * **Portabilidad 100%:** Se empaqueta en una imagen Docker ligera (~150 MB) que corre de manera idéntica en el Docker Compose local y como una tarea Serverless en AWS ECS/Fargate o dentro del contenedor de Apache Airflow (MWAA).
   * **Costo:** Cero costo de idle; solo se pagan los segundos exactos de cómputo en Fargate.

---

## 2. Decisión de Deduplicación y Clave de Evento

### Contexto
La red de eventos opera de manera asíncrona, provocando reintentos de red y anomalías. Se requería una estrategia determinista para distinguir reintentos legítimos de conflictos de integridad.

### Definición Técnica
* **Clave Natural del Evento:** `event_id` (UUID único asignado en el origen).
* **Hash Criptográfico de Contenido:** 
  $$\text{record\_hash} = \text{SHA-256}(\text{order\_id} \parallel \text{event\_type} \parallel \text{event\_time} \parallel \text{canonical\_json}(\text{payload}))$$

### Reglas de Decisión:
1. **Reintento Idéntico:** Si llega un `event_id` ya visto con **idéntico `record_hash`**, se clasifica como un duplicado de red y se **descarta silenciosamente**. Esto garantiza que reejecutar el pipeline 10 veces produzca el mismo resultado.
2. **Conflicto de Contenido:** Si llega un `event_id` repetido con un **`record_hash` diferente**, representa una alteración de datos o corrupción. El registro no se procesa en el lago; se envía de inmediato a la **capa de cuarentena** con el motivo `COLISION_CLAVE_CONTENIDO_DISTINTO`.

---

## 3. Decisión de Máquina de Estados y Manejo de Eventos Tardíos (*Late Arrivals*)

### Contexto
Un pedido pasa por múltiples estados (`created` $\rightarrow$ `paid` $\rightarrow$ `cancelled` $\rightarrow$ `refunded`). Los microservicios pueden entregar eventos desordenados o con retrasos superiores a 24 horas.

### Definición Técnica
1. **Regla de Ordenamiento y Desempate:**
   * Orden primario: `event_time` en UTC.
   * Criterio de desempate determinista (si dos eventos tienen el mismo microsegundo exacto): Precedencia fija de negocio:
     $$\text{created} \ (1) \rightarrow \text{paid} \ (2) \rightarrow \text{cancelled} \ (3) \rightarrow \text{refunded} \ (4)$$
2. **Estrategia para Llegadas Tardías (> 24 horas):**
   * Los eventos tardíos no se rechazan. Se marcan con la bandera `is_late_arrival = TRUE`.
   * En lugar de procesar la orden como una fila nueva, se activa un **reprocesamiento incremental por clave de orden (`order_id`)**. La máquina de estados re-evalúa el histórico completo de eventos de ese `order_id` y actualiza la fila correspondiente en `fact_orders` mediante una operación de **Upsert Atómico**.
   * **Resultado:** Las métricas financieras y el estado consolidado se corrigen automáticamente sin inflar el conteo de pedidos.

---

## 4. Decisión sobre Clientes Anónimos / Nulos

### Contexto
El dataset histórico de retail y los pedidos de comercios aliados contienen transacciones sin identificador de cliente (`customer_id` nulo o vacío). No se deben descartar las ventas, pero tampoco inventar identidades personales falsas.

### Definición Técnica
* En la dimensión `dim_customer` se crea un registro surrogate reservado:
  * `customer_sk = hash("GUEST_UNKNOWN")`
  * `customer_id = "GUEST_UNKNOWN"`
  * `customer_name = "Cliente Invitado / No Registrado"`
  * `is_guest = TRUE`
* En `fact_orders`, los pedidos sin cliente apuntan a este surrogate key y registran `is_guest_order = TRUE`.
* **Beneficio:** No se violan las claves foráneas de integridad referencial y se preserva el 100% de la facturación contable.

---

## 5. Decisión de Moneda y Tasas de Cambio

### Contexto
Mercado Nube opera en Colombia (COP), México (MXN) y Perú (PEN), además del dataset histórico en Libras (GBP). La consolidación ejecutiva requiere una moneda base homogénea.

### Definición Técnica
* **Moneda de Reporte Unificada:** **USD** (Dólar estadounidense).
* **Mecanismo de Tasas con Fallback:**
  * El pipeline intenta consultar en tiempo real las tasas oficiales del Banco Central Europeo mediante la API abierta **Frankfurter** (`https://api.frankfurter.dev/v2/rates`).
  * Si la llamada falla por corte de red o la API no está disponible, el cliente conmuta de inmediato a una **tabla semilla demostrativa local (`exchange_rates_seed.csv`)**.
  * En las tablas de hechos (`fact_orders` y `fact_order_items`), se conservan tanto el importe original en moneda local (`gross_amount_local`) como el valor normalizado (`gross_amount_usd`), permitiendo auditorías en ambas direcciones.

---

## 6. Decisión de Granularidad del Modelo Dimensional

### Definición de Tablas:
1. `fact_orders`: Granularidad de **un registro por cada pedido completo o factura**. Contiene métricas agregadas (total USD, tiempo de conversión a pago, estado final).
2. `fact_order_items`: Granularidad de **un registro por cada línea de producto dentro del pedido**. Permite análisis detallados de categorías, unidades vendidas y devoluciones a nivel de SKU.
3. `fact_order_events`: Granularidad de **un registro por cada evento atómico de cambio de estado**. Preserva la línea de tiempo completa para auditoría y observabilidad.
4. `dim_product` (SCD Tipo 2): Almacena múltiples versiones del mismo SKU identificadas por `valid_from`, `valid_to` e `is_current`. Si un producto cambia de categoría o precio, se conserva el registro histórico y se abre uno nuevo.

---

## 7. Decisión de Idempotencia y Tolerancia a Fallos

### Definición Técnica
* **En Almacenamiento S3:**
  * Las tareas escriben las particiones de fecha con estrategia de sobrescritura atómica (`overwrite` en la ruta `year=YYYY/month=MM/day=DD/`).
* **En el Almacén Analítico (PostgreSQL / Redshift):**
  * Se aplica el patrón **Staging Table + Merge Atómico** dentro de una transacción `BEGIN ... COMMIT`:
    ```sql
    DELETE FROM dwh_analytics.fact_orders 
    WHERE order_id IN (SELECT order_id FROM staging_orders);
    
    INSERT INTO dwh_analytics.fact_orders 
    SELECT * FROM staging_orders;
    ```
  * Si el proceso falla a mitad de camino, la transacción hace `ROLLBACK`. Si el DAG se reejecuta 5 veces para la misma fecha, el almacén termina con exactamente los mismos datos sin filas duplicadas.

---

## 8. Decisión de Seguridad y Permisos IAM

### Definición Técnica
* **Cero Credenciales en Código:** No existen llaves `AWS_ACCESS_KEY_ID` ni contraseñas quemadas en el repositorio Git.
* **IAM Roles Específicos:** Se crearon roles separados:
  * `pipeline_execution_role`: Permisos exclusivos sobre el bucket S3 del lago (`s3:GetObject`, `s3:PutObject`), CloudWatch Logs y Redshift.
* **Cifrado Obligatorio:** Todos los datos en reposo en S3 están cifrados con **AES-256 (SSE-S3)** y bloqueado el acceso público al 100%.
