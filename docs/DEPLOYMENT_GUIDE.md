# 🚀 Guía de Deployment Paso a Paso — Proyecto Integrador M4

> Cada paso está vinculado a la consigna que satisface y al criterio de evaluación correspondiente.

---

## Pre-requisitos

| Herramienta | Versión | Para qué |
|-------------|---------|----------|
| Python | 3.11+ | Scripts de infra, Spark, tests |
| AWS CLI | v2 | Interacción con S3, IAM, Lake Formation |
| Docker + Compose | v24+ | Airflow, Kafka, Spark |
| Git | v2+ | Versionado, CI/CD |
| Java JDK | 11+ | PySpark local |
| Cuenta AWS | Free Tier | S3, IAM, Lake Formation |
| Airbyte Cloud | Free Tier | Ingesta de datos |

```bash
# Verificar pre-requisitos
python --version && aws --version && docker --version && java -version
```

---

## FASE 1: Arquitectura del Data Lake (Avance 1 — 10 pts)

### 📋 Consignas que cubre
- ✅ Configuración del bucket S3 con estructura en capas
- ✅ Lake Formation para gobernanza y catalogación
- ✅ Documento técnico del pipeline ETLT
- ✅ Diagrama detallado de arquitectura
- ✅ Justificación del stack tecnológico

### Paso 1.1: Configurar variables de entorno
```bash
cd proyecto_integrador_m4
cp .env.example .env
# Editar .env con tus valores reales:
#   AWS_REGION, AWS_ACCOUNT_ID, S3_BUCKET_NAME
#   LAKEFORMATION_ADMIN_ROLE_ARN
```

### Paso 1.2: Crear bucket S3 con estructura de capas
```bash
pip install boto3 python-dotenv
python infrastructure/s3/create_bucket.py
```
**Qué hace**: Crea el bucket `pi-m4-datalake-maxi` con prefijos `raw/`, `processed/`, `gold/`, `raw-streaming/`. Habilita versionado, cifrado SSE-S3 y bloquea acceso público.

**📸 Screenshot**: AWS Console → S3 → Bucket → mostrar estructura de carpetas.

### Paso 1.3: Configurar Lake Formation
```bash
python infrastructure/lake_formation/setup_lakeformation.py
```
**Qué hace**: Registra el bucket como ubicación del Data Lake, crea 4 databases en Glue Catalog (`raw_ecommerce`, `raw_weather`, `processed`, `gold`) y otorga permisos granulares vía Lake Formation.

**📸 Screenshot**: AWS Console → Lake Formation → Databases → mostrar las 4 databases.

### Paso 1.4: Verificar documentación
Los siguientes archivos ya existen y satisfacen las consignas:
- `docs/documento_tecnico.md` → Documento técnico completo del pipeline ETLT
- `docs/decisiones_tecnologicas.md` → Justificación del stack
- `docs/arquitectura_pipeline.md` → Diagramas Mermaid (Batch + Streaming + Seguridad)

---

## FASE 2: Ingesta con Airbyte (Avance 2 — 10 pts)

### 📋 Consignas que cubre
- ✅ Conector API pública (OpenWeatherMap → S3)
- ✅ Conector PostgreSQL (Olist E-commerce → S3)
- ✅ Validación de datos en S3

### Paso 2.1: Cargar datos iniciales en PostgreSQL
```bash
# Agregar credenciales reales al .env:
#   POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
pip install psycopg2-binary
python ingestion/scripts/seed_data.py
```
**Qué hace**: Carga los CSV de `0. Data/` a las tablas PostgreSQL en Supabase (9 tablas de Olist).

### Paso 2.2: Configurar Airbyte Cloud
1. Ir a [cloud.airbyte.com](https://cloud.airbyte.com)
2. Crear **Source PostgreSQL**: usar credenciales del `.env`, modo `Full Refresh` + `Incremental`
3. Crear **Source HTTP** (OpenWeatherMap): usar `OPENWEATHER_API_KEY`
4. Crear **Destination S3**: bucket `pi-m4-datalake-maxi`, prefijo `raw/`, formato Parquet
5. Crear las 2 **Connections** (PostgreSQL→S3, WeatherAPI→S3)

**Configuraciones JSON de referencia**: `ingestion/airbyte/connections/*.json`

### Paso 2.3: Ejecutar sincronización manual
En Airbyte Cloud → Connections → "Sync now" en cada connection.

**📸 Screenshots**:
- Connection PostgreSQL → S3 con estado "Succeeded"
- Connection Weather API → S3 con estado "Succeeded"

### Paso 2.4: Validar datos en S3
```bash
python ingestion/scripts/validate_raw_data.py
```
**Qué hace**: Verifica que los archivos Parquet existen en `raw/ecommerce/` y `raw/weather_api/`, chequea schemas, detecta Schema Drift y reporta estadísticas.

**📸 Screenshot**: AWS Console → S3 → `raw/ecommerce/` mostrando las 9 carpetas de tablas.

---

## FASE 3: Procesamiento con Spark (Avance 3 — 15 pts)

### 📋 Consignas que cubre
- ✅ Scripts PySpark para transformaciones de negocio
- ✅ Combinar datos estructurados con no estructurados (clima)
- ✅ Optimización: particionamiento, caché, control de shuffle

### Paso 3.1: Ejecutar transformación Raw → Processed (Silver)
```bash
# Modo local (laptop)
export SPARK_MODE=local
bash processing/spark-submit.sh raw_to_processed local

# Modo EC2
bash processing/spark-submit.sh raw_to_processed ec2
```
**Qué hace**: Lee 9 tablas de `raw/`, las limpia, deduplica, normaliza strings, y crea un **modelo estrella** con 5 dimensiones + 4 facts + `weather_enriched`. Escribe en `processed/`.

### Paso 3.2: Ejecutar transformación Processed → Gold (KPIs)
```bash
bash processing/spark-submit.sh processed_to_gold local
```
**Qué hace**: Calcula **7 KPIs** analíticos que responden las preguntas de negocio:

| KPI | Pregunta de Negocio | Archivo en Gold |
|-----|---------------------|-----------------|
| Top Productos por Categoría | ¿Productos más vendidos? | `gold/kpi_top_products/` |
| Segmentación RFM | ¿Clientes con mayor frecuencia? | `gold/kpi_customer_rfm/` |
| Ingresos por Región | ¿Regiones con mayores ingresos? | `gold/kpi_revenue_region/` |
| Nuevos vs Recurrentes | ¿Proporción nuevos vs recurrentes? | `gold/kpi_new_vs_returning/` |
| Precio vs Volumen | ¿Relación precio-volumen? | `gold/kpi_price_volume/` |
| Impacto del Clima | ¿Cómo impacta el clima? | `gold/kpi_weather_impact/` |
| Métodos de Pago | ¿Desempeño por método de pago? | `gold/kpi_payment_methods/` |

**📸 Screenshot**: AWS Console → S3 → `gold/` mostrando los 7 prefijos de KPIs.

---

## FASE 4: Orquestación con Airflow (Avance 4 — 10 pts)

### 📋 Consignas que cubre
- ✅ EC2 + Airflow con Docker Compose
- ✅ DAG que orquesta Airbyte + Spark
- ✅ Dependencias, reintentos y notificaciones
- ✅ CI/CD con GitHub Actions (Extra Credit)

### Paso 4.1: Levantar Airflow localmente
```bash
cd orchestration
docker compose up -d --build
# Esperar ~60 segundos a que Airflow inicialice
docker compose logs -f airflow-webserver
```
**Acceder a**: http://localhost:8080 (usuario: `admin`, contraseña: `admin`)

### Paso 4.2: Configurar variables de Airflow
```powershell
# Desde PowerShell (Windows)
.\orchestration\scripts\setup_airflow_vars.ps1
```
Esto setea: `airbyte_api_key`, `airbyte_connection_ecommerce`, `airbyte_connection_weather`.

### Paso 4.3: Activar y ejecutar el DAG
1. Abrir Airflow UI → DAGs → `etlt_pipeline_v1` → Toggle ON
2. Click "Trigger DAG" para ejecución manual
3. Observar el flujo: **Ingesta** → **Validación** → **Raw→Processed** → **Processed→Gold**

**📸 Screenshots**:
- Vista Graph del DAG con todos los tasks en verde
- Vista Grid mostrando ejecuciones exitosas

### Paso 4.4: Verificar CI/CD (Extra Credit)
```bash
git push origin main
```
- GitHub Actions ejecuta: `CI Pipeline` (lint + tests + DAG validation)
- Si hay cambios en `orchestration/dags/`, ejecuta: `Deploy DAGs to EC2`

**📸 Screenshot**: GitHub → Actions → Workflows con checks en verde.

---

## FASE 5: Streaming con Kafka + Spark (Avance 5 — 10 pts)

### 📋 Consignas que cubre
- ✅ Kafka en Docker Compose con topics
- ✅ Consumidor Spark Structured Streaming → raw-streaming
- ✅ Enriquecimiento con datos batch (processed)
- ✅ Capa Gold unificada (Lambda Architecture)

### Paso 5.1: Levantar Kafka
```bash
cd streaming
docker compose up -d
# Esperar 10 segundos
bash scripts/create_topics.sh
cd ..
```
**Acceder a Kafka UI**: http://localhost:8090

### Paso 5.2: Ejecutar la demo completa
```bash
bash run_demo.sh
```
**Qué hace en secuencia**:
1. Levanta Kafka (si no está corriendo)
2. Inicia el **Productor** (`weather_producer.py`) → envía 1 evento JSON/segundo al topic `weather_events`
3. Inicia el **Consumidor** Spark Structured Streaming (`spark_streaming_consumer.py`):
   - Lee de Kafka en micro-batches de 30 segundos
   - Aplica Watermark de 2 horas para datos tardíos
   - Escribe en `raw-streaming/` (append)
   - **Unifica** streaming + batch en `gold/lambda_weather_unified/` (Lambda Architecture)
4. `Ctrl+C` para finalizar la demo

**📸 Screenshots**:
- Kafka UI → Topic `weather_events` con mensajes
- Terminal mostrando micro-batches procesados
- AWS S3 → `gold/lambda_weather_unified/` con archivos

### Paso 5.3: Verificar el DAG Watchdog
En Airflow UI → DAGs → `streaming_pipeline_watchdog` → Toggle ON.
Este DAG corre cada 5 minutos y reinicia el consumidor si se cayó.

---

## Verificación Final End-to-End

```bash
# 1. Tests unitarios (13 tests)
pytest quality/tests/ -v
# Esperado: 13 passed

# 2. Verificar estructura S3
aws s3 ls s3://pi-m4-datalake-maxi/ --recursive | head -50

# 3. Verificar KPIs en Gold
aws s3 ls s3://pi-m4-datalake-maxi/gold/
# Esperado: 7+ prefijos de KPIs

# 4. Verificar Airflow DAGs
docker exec -it airflow-scheduler airflow dags list
# Esperado: etlt_pipeline_v1, streaming_pipeline_watchdog, s3_maintenance_compaction
```

---

## Checklist de Screenshots para la Entrega

| # | Servicio | Qué capturar | Nombre archivo |
|---|----------|-------------|----------------|
| 1 | S3 | Estructura `raw/ecommerce/` (9 tablas) | `s3_raw_structure.png` |
| 2 | S3 | Carpeta `gold/` (7 KPIs) | `s3_gold_kpis.png` |
| 3 | Airbyte | Connection sync "Succeeded" | `airbyte_sync_ok.png` |
| 4 | Airflow | DAG Graph view (tasks verdes) | `airflow_dag_graph.png` |
| 5 | Airflow | Grid view (múltiples runs) | `airflow_dag_runs.png` |
| 6 | Kafka UI | Topic con mensajes | `kafka_topic_messages.png` |
| 7 | GitHub | CI Pipeline passing | `github_ci_passing.png` |
| 8 | Lake Formation | Databases en Glue Catalog | `lakeformation_databases.png` |

Guardar en `docs/screenshots/` y referenciar en el README principal.
