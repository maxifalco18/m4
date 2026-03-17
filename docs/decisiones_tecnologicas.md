# Decisiones Tecnológicas — Justificación del Stack

## AWS S3 como almacenamiento base

**Alternativas consideradas**: Azure Data Lake Storage Gen2, Google Cloud Storage, HDFS on-premise.

**Por qué S3**:
- Free Tier generoso (5 GB/mes, 20.000 GET requests)
- Ecosistema AWS: integración nativa con Glue, Lake Formation, Athena, EMR
- SDK boto3 de Python maduro y bien documentado
- Durabilidad 99.999999999% (11 nueves)
- Object store: escala infinita, sin gestión de nodos

**Consideración adicional**: S3 no es un filesystem — es un object store con prefijos. Esto simplifica la escalabilidad pero requiere comprender las diferencias de comportamiento (ej: `list_objects()` puede ser costoso con milones de objetos; se mitiga con particionamiento cuidadoso).

---

## AWS Lake Formation + Glue Data Catalog

**Alternativas consideradas**: Apache Atlas (open source), Databricks Unity Catalog, AWS Glue solo.

**Por qué Lake Formation**:
- Control de acceso a nivel de tabla y columna (column-level security)
- Integración nativa con IAM, S3, Glue, Athena
- Free Tier sin costo adicional más allá de los servicios subyacentes
- Soporte de Row-Level Security para auditoría futura

**Desventaja principal**: curva de configuración inicial más alta que solo usar políticas S3. Se mitiga con el script `setup_lakeformation.py`.

---

## Airbyte Cloud (Free Tier)

**Alternativas consideradas**: Apache NiFi, AWS Glue ETL Jobs, Fivetran, Stitch, scripts Python ad-hoc.

**Por qué Airbyte**:
- 300+ conectores pre-construidos (incluye HTTP genérico y PostgreSQL)
- Manejo automático de CDC (Change Data Capture) para sincronización incremental
- Escribe en S3 en formato Parquet — no requiere conversión
- Free Tier para proyectos de desarrollo/educación
- Alternativa open source (`airbyte-ce`) deployable en EC2 si se agota el Free Tier

**Desventaja principal**: limitación de sincronizaciones manuales en Free Tier (no scheduling automático). Mitigación: el DAG de Airflow dispara la sync via API de Airbyte.

---

## Apache Spark (PySpark)

**Alternativas consideradas**: Apache Flink, Pandas (single-node), dbt + Redshift, AWS Glue ETL.

**Por qué Spark**:
- Estándar de la industria para procesamiento distribuido
- Una sola API para batch y streaming (Structured Streaming)
- spark-submit permite el mismo código en local, EC2, EMR o Databricks
- Optimizaciones avanzadas: broadcast joins, partition pruning, cache
- Sin cambios de código al escalar horizontalmente

**Configuración recomendada según entorno**:

| Entorno | Costo | Cuándo usarlo |
|---------|-------|---------------|
| EC2 t2.large (spark-submit local) | ~$0.09/h | Desarrollo y pruebas |
| Amazon EMR (cluster temporal) | ~$0.15/h + EC2 | Volúmenes > 10 GB en producción |
| Databricks Community Edition | Gratis | Alternativa educativa |

---

## Apache Airflow (Docker Compose en EC2)

**Alternativas consideradas**: AWS Step Functions, Prefect, Dagster, cron jobs.

**Por qué Airflow**:
- DAGs como código Python: versionables en Git
- UI web para monitoreo, re-ejecución y debug
- Operadores nativos para Spark, Airbyte, S3
- Comunidad enorme, documentación extensa
- Backend compatible con el Free Tier de EC2 (LocalExecutor en t2.micro)

**Limitación del Free Tier**: en t2.micro, Airflow puede ser lento con DAGs complejos. Solución: usar `LocalExecutor` (no CeleryExecutor) y limitar la paralelización.

---

## Formato Parquet (y Delta Lake para Gold)

**Alternativas consideradas**: CSV, JSON, Avro, ORC, Iceberg.

**Por qué Parquet**:
- Columnar: lecturas analíticas ~10x más rápidas que CSV
- Compresión Snappy: ~70% menos espacio — crítico para Free Tier de S3
- Schema embebido: Spark infiere tipos automáticamente
- Compatible con todo: Athena, Redshift Spectrum, Pandas, Spark

**Por qué Delta Lake en Gold**:
- ACID transactions: múltiples writers (batch + streaming) sin corrupción
- Time travel: auditar versiones anteriores del KPI
- Schema evolution: agregar columnas sin reescribir la tabla
- Z-ordering: optimización de lectura por columnas frecuentes (ej: `fecha`, `region`)

---

## GitHub Actions (CI/CD)

**Alternativas consideradas**: Jenkins, GitLab CI, CircleCI, AWS CodePipeline.

**Por qué GitHub Actions**:
- Plan gratuito: 2.000 minutos/mes (más que suficiente para este proyecto)
- Integrado con el repositorio — no requiere infraestructura adicional
- Amplio marketplace de actions pre-construidas
- Deploy directo a EC2 via SSH sin servicios adicionales

---

## Apache Kafka (Streaming)

**Alternativas consideradas**: Amazon Kinesis, Apache Pulsar, RabbitMQ.

**Por qué Kafka**:
- Estándar de la industria para event streaming
- Docker Compose en EC2: deployment simple y reproducible
- Integración nativa con Spark Structured Streaming
- Offset management robusto para garantizar exactly-once semántica
- Kinesis sería más simple pero tiene costo fuera del Free Tier

**Desventaja principal**: Kafka en una sola instancia EC2 no tiene alta disponibilidad. Para este proyecto educativo es aceptable; en producción se requiere un cluster multi-broker.
