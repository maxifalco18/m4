# Proyecto Integrador M4 - Data Engineering Pipeline

## Fase de Planificación
- [x] Revisar consignas y criterios de evaluación
- [x] Crear plan de implementación y estructura de directorios
- [x] Obtener aprobación del usuario para el plan

## Fase 1 - Avance 1: Diseño de la Arquitectura (10 pts)
- [x] Script para crear bucket S3 con estructura Medallion completa
- [x] Políticas IAM (4 roles, least privilege, sin access keys)
- [x] Script Lake Formation (Glue DBs + permisos granulares)
- [x] CI/CD GitHub Actions (lint + tests + DAG validation)
- [x] Documento técnico (SPOF, schema evolution, escalabilidad)
- [x] Documento de decisiones tecnológicas (alternativas evaluadas)
- [ ] Diagrama Draw.io de arquitectura (pendiente — hacer en Draw.io web)
- [ ] Provisionar bucket S3 real en AWS (ejecutar create_bucket.py)

## Fase 2 - Avance 2: Ingesta con Airbyte (10 pts)
- [x] DDL + seed script para Olist (9 tablas) en Supabase
- [x] Config Airbyte: conector HTTP para OpenWeatherMap (paginación + backoff)
- [x] Config Airbyte: conector PostgreSQL para Olist (Full Refresh + Incremental Append)
- [x] validate_raw_data.py (MVP: conteo + Pro: schema drift detection)
- [ ] Ejecutar sync en Airbyte Cloud y verificar Parquet en S3

## Fase 3 - Avance 3: Procesamiento con Spark (15 pts)
- [x] SparkSession factory + utils (transformations.py, data_quality.py)
- [x] Job raw→processed: modelo estrella Olist + weather enriquecido
- [x] Job processed→gold: KPIs analíticos (RFM, ingresos, impacto clima)
- [x] Optimizaciones: broadcast joins, partition pruning, cache, coalesce
- [x] spark-submit.sh + requirements.txt

## Fase 4 - Avance 4: Orquestación y CI/CD (15 pts)
- [x] docker-compose.yml para Airflow (LocalExecutor)
- [x] DAG principal etlt_pipeline_dag.py (Airbyte → Spark → Gold)
- [x] Reintentos, SLA, alertas Slack/email
- [x] GitHub Actions deploy_dags.yml (sync DAGs a EC2 via SSH)
- [x] Great Expectations: integrado en DAG como PythonOperator

## Fase 5 - Avance 5: Streaming con Kafka (10 pts)
- [x] docker-compose.yml Kafka + Zookeeper + UI
- [x] weather_producer.py (simulador eventos OWM en tiempo real)
- [x] spark_streaming_consumer.py (Kafka → raw-streaming → processed-streaming)
- [x] Unificación batch + streaming en gold/ (Arquitectura Lambda)
- [x] streaming_pipeline_dag.py (Airflow gestiona el consumer)

## Entregables Finales
- [ ] Documento técnico completo
- [ ] Presentación final
- [ ] Repositorio limpio y documentado
