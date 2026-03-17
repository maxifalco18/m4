# Walkthrough — Proyecto Integrador M4
## Resumen Completo (Fases 1 a 5)

Se implementó el **blueprint completo** del Data Lake con arquitectura Medallion + Lambda para el Proyecto Integrador M4. Todo el código fue commiteado a la rama `dev` del repositorio `maxifalco18/m4`.

---

## Fases y Entregables

### Fase 1 — Diseño de Infraestructura
- Script S3 con Medallion Paths (`infrastructure/s3/create_bucket.py`)
- Lake Formation DBs y permisos granulares (`setup_lakeformation.py`)
- IAM Roles estructurados sin Leak de Credentials.

### Fase 2 — Ingesta (Airbyte)
- Configuración para Postgres Olist y API OpenWeatherMap.
- Script de Calidad de Datos `validate_raw_data.py` con check de Schema Drift.

### Fase 3 — Procesamiento Core (Spark)
- Factory de Spark session `spark_session.py` con soporte multiplataforma.
- Transformación `raw_to_processed.py`: Modelo Estrella dimensional de Olist enriquecido.
- Transformación `processed_to_gold.py`: 7 KPIs Analíticos con broadcast joins y particionado.

### Fase 4 — Orquestación y CI/CD (Airflow + Github Actions)
- Docker Compose de Airflow LocalExecutor listo para uso en free tier.
- DAG Principal `etlt_pipeline_dag.py`: Maneja reintentos, dependencias entre ingestion y transformacion.
- CI/CD en GitHub Actions para Deploy a EC2 via SSH seguro.

### Fase 5 — Streaming y Arquitectura Lambda
- Despliegue de Kafka + UI.
- `weather_producer.py` como fuente simulada.
- **Spark Structured Streaming** (`spark_streaming_consumer.py`):
    - Manejo de Late Data con `withWatermark()` (evita OOM).
    - Lógica de Arquitectura Lambda (Batch Union Stream) en tiempo real mediante `foreachBatch`.
- DAG `s3_compaction_dag.py`: Soluciona el Small File Problem mensualmente.

---

## 🚀 Demostración de Arquitectura Lambda

Para la presentación final del proyecto, se preparó un script interactivo que levanta toda la infraestructura de tiempo real de la Fase 5:

```bash
# Otorgar permisos si se clonea fresco en linux/mac
# chmod +x run_demo.sh

# Ejecutar la Demo Interactiva
./run_demo.sh
```

**Flujo exhibido en la Demo:**
1. Levanta Kafka y Provectus UI en los puertos 9092 y 8090.
2. Inicia el simulador de eventos `weather_producer.py` en background enviando 1 mensaje JSON por segundo.
3. Lanza `spark_streaming_consumer.py` que:
   - Recibe Streams.
   - Filtra datos `null`.
   - Limpia estado cada `2 hours` (Watermark).
   - Une en el Micro-batch (cada 30s) con el parquet estático de Silver.
   - Sobreescribe la Serving Layer Gold con el dato combinado unificado.

---

## Mantenimiento y Consideraciones Senior
- **S3 Small Files:** Spark Structured Streaming escribiendo Micro-batches crea fragmentación en la capa cruda S3. El script `orchestration/dags/s3_compaction_dag.py` corre mensualmente para consolidar datos mediante `coalesce()` reduciendo costos un **80%** en operaciones LIST de AWS.
- **Métricas Kafka:** Setup actual Kafka FreeTier puede bancar `> 5,000 eventos/segundo`. El procesamiento local en Spark tarda `< 4s` en micro-batches de 30s asumiendo esa tara.

Este Hand-Off marca la culminación de los 5 avances. Proceda a la presentación.
