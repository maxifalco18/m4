# Plan de Implementación - Proyecto Integrador M4

Pipeline ETLT escalable sobre Data Lake en AWS con arquitectura Medallion.

---

## 1. Estructura de Directorios del Repositorio

```
proyecto_integrador_m4/
├── README.md                          # Descripción general del proyecto
├── .gitignore                         # Archivos a excluir del repo
├── .env.example                       # Variables de entorno de ejemplo (sin secrets)
│
├── docs/                              # Documentación técnica
│   ├── documento_tecnico.md           # Documento técnico (4-6 pág)
│   ├── diagrama_arquitectura.drawio   # Diagrama principal (Draw.io)
│   ├── diagrama_arquitectura.png      # Export PNG para README
│   └── decisiones_tecnologicas.md     # Justificación del stack
│
├── infrastructure/                    # IaC y configuración de AWS
│   ├── s3/
│   │   └── create_bucket.py           # Script para crear bucket + estructura
│   ├── iam/
│   │   └── policies.json              # Políticas IAM
│   └── lake_formation/
│       └── setup_lakeformation.py     # Configuración Lake Formation
│
├── ingestion/                         # Avance 2 - Airbyte
│   ├── airbyte/
│   │   ├── connections/
│   │   │   ├── api_weather_config.json     # Config conector API
│   │   │   └── postgres_dvdrental_config.json  # Config conector PG
│   │   └── README.md                 # Guía de configuración Airbyte
│   └── scripts/
│       └── validate_raw_data.py       # Script validación post-ingesta
│
├── processing/                        # Avance 3 - PySpark
│   ├── jobs/
│   │   ├── __init__.py
│   │   ├── raw_to_processed.py        # Job principal: raw → processed
│   │   ├── processed_to_gold.py       # Job: processed → gold
│   │   └── utils/
│   │       ├── __init__.py
│   │       ├── spark_session.py       # Factory de SparkSession
│   │       ├── data_quality.py        # Funciones de calidad de datos
│   │       └── transformations.py     # Transformaciones reutilizables
│   ├── spark-submit.sh                # Script para enviar jobs
│   └── requirements.txt              # Dependencias Python
│
├── orchestration/                     # Avance 4 - Airflow
│   ├── docker-compose.yml             # Airflow en Docker
│   ├── Dockerfile                     # Imagen custom de Airflow
│   ├── dags/
│   │   ├── etlt_pipeline_dag.py       # DAG principal
│   │   └── utils/
│   │       └── slack_alerts.py        # Callbacks de alertas
│   ├── plugins/                       # Plugins Airflow (si aplica)
│   └── config/
│       └── airflow.cfg                # Configuración custom
│
├── streaming/                         # Avance 5 - Kafka
│   ├── docker-compose.yml             # Kafka + Zookeeper
│   ├── producer/
│   │   ├── weather_producer.py        # Productor simulado
│   │   └── requirements.txt
│   ├── consumer/
│   │   ├── spark_streaming_consumer.py  # Structured Streaming
│   │   └── requirements.txt
│   └── scripts/
│       └── create_topics.sh           # Crear topics en Kafka
│
├── quality/                           # Calidad de datos
│   ├── great_expectations/
│   │   ├── great_expectations.yml
│   │   └── expectations/
│   │       ├── raw_weather_suite.json
│   │       └── raw_dvdrental_suite.json
│   └── tests/
│       ├── test_transformations.py    # Tests unitarios
│       └── test_data_quality.py       # Tests de calidad
│
├── .github/                           # CI/CD
│   └── workflows/
│       ├── ci.yml                     # Lint + Tests en cada push
│       └── deploy_dags.yml            # Deploy DAGs a EC2
│
└── consignas_proyecto/                # Material original (ya existe)
    ├── consignas.txt
    ├── criterios_evaluacion.xlsx
    └── DE B2C _ Presentación del PI_M4.pptx.txt
```

---

## 2. Stack Tecnológico

| Componente | Herramienta | Justificación |
|---|---|---|
| **Almacenamiento** | AWS S3 (Free Tier) | Escalable, económico, integra con todo AWS |
| **Gobernanza** | AWS Lake Formation + IAM | Permisos granulares, catálogo centralizado |
| **Ingesta Batch** | Airbyte Cloud (Free Tier) | Conectores pre-construidos, low-code |
| **Procesamiento** | Apache Spark (PySpark) | Procesamiento distribuido, batch + streaming |
| **Orquestación** | Apache Airflow (Docker) | DAGs como código, monitoreo, reintentos |
| **Streaming** | Apache Kafka + Spark SS | Eventos en tiempo real + enriquecimiento |
| **CI/CD** | GitHub Actions | Gratuito, integrado con el repo |
| **Calidad** | Great Expectations + pytest | Validación de esquema, nulos, rangos |
| **Formato** | Apache Parquet / Delta Lake | Columnar, compresión, schema evolution |
| **Diagrama** | Draw.io | Gratuito, exporta a múltiples formatos |

---

## 3. Fuentes de Datos

### 3.1 API Pública - OpenWeatherMap

- **Endpoint**: [OpenWeatherMap History API](https://openweathermap.org/history) o similar gratuita
- **Conector Airbyte**: HTTP genérico (declarative)
- **Datos ya descargados**: `Patagonia_-41.json` con datos horarios meteorológicos (temp, humedad, viento, presión, nubes, lluvia, etc.)
- **Frecuencia**: Ejecución manual 1x/día (Free Tier)
- **Formato destino**: Parquet en `s3://bucket/raw/weather/YYYY/MM/DD/`

### 3.2 Base de Datos PostgreSQL Pública

- **Opción recomendada**: Base de datos `dvdrental` o `Pagila` desplegada en un servicio PostgreSQL gratuito (ElephantSQL, Neon, Supabase o Railway)
- **Tablas relevantes**: `rental`, `customer`, `film`, `payment`, `store`, `inventory`, `category`
- **Modo**: Full Refresh inicial → Incremental (CDC con campo `last_update`)
- **Formato destino**: Parquet en `s3://bucket/raw/dvdrental/{tabla}/`

> [!IMPORTANT]
> **Decisión requerida**: Para la base PostgreSQL, la opción más común y documentada es **`dvdrental`/Pagila** (alquiler de películas). Si preferís un dataset más alineado con ventas (consistente con las preguntas de negocio), podemos usar una base de e-commerce pública. ¿Tenés alguna preferencia o la decide el bootcamp?

---

## 4. Estructura del Data Lake (S3)

```
s3://pi-m4-datalake-{tu-id}/
├── raw/                          # Datos crudos (immutable)
│   ├── weather/                  # API OpenWeatherMap
│   │   └── YYYY/MM/DD/           # Particionado por fecha
│   └── dvdrental/                # PostgreSQL
│       ├── rental/
│       ├── customer/
│       ├── payment/
│       └── .../
│
├── raw-streaming/                # Streaming crudo
│   └── weather_events/
│       └── YYYY/MM/DD/HH/
│
├── processed/                    # Datos limpiados + transformados
│   ├── dim_customers/
│   ├── dim_films/
│   ├── dim_stores/
│   ├── fact_rentals/
│   ├── fact_payments/
│   └── weather_enriched/
│
├── processed-streaming/          # Streaming enriquecido
│   └── weather_enriched_rt/
│
└── gold/                         # Capa analítica final
    ├── kpi_ventas_por_categoria/
    ├── kpi_clientes_frecuentes/
    ├── kpi_ingresos_por_tienda/
    ├── kpi_weather_impact/
    └── unified_analytics/        # Batch + Streaming unificado
```

---

## 5. Plan de Implementación Paso a Paso

### Fase 1: Diseño de la Arquitectura (Avance 1 — 10 pts)

| # | Tarea | Archivo/Recurso |
|---|---|---|
| 1.1 | Crear `.gitignore` y `README.md` base | Raíz del repositorio |
| 1.2 | Script Python para crear bucket S3 + carpetas | `infrastructure/s3/create_bucket.py` |
| 1.3 | Definir políticas IAM en JSON | `infrastructure/iam/policies.json` |
| 1.4 | Script para configurar Lake Formation | `infrastructure/lake_formation/setup_lakeformation.py` |
| 1.5 | Crear diagrama de arquitectura en Draw.io | `docs/diagrama_arquitectura.drawio` |
| 1.6 | Redactar documento técnico (4-6 pág) | `docs/documento_tecnico.md` |
| 1.7 | Documento de decisiones tecnológicas | `docs/decisiones_tecnologicas.md` |

---

### Fase 2: Ingesta con Airbyte (Avance 2 — 10 pts)

| # | Tarea | Archivo/Recurso |
|---|---|---|
| 2.1 | Crear cuenta Airbyte Cloud (Free Tier) | Manual (screenshots en docs) |
| 2.2 | Configurar Source: API Weather (HTTP) | `ingestion/airbyte/connections/api_weather_config.json` |
| 2.3 | Configurar Source: PostgreSQL dvdrental | `ingestion/airbyte/connections/postgres_dvdrental_config.json` |
| 2.4 | Configurar Destination: S3 (Parquet) | Airbyte Cloud UI |
| 2.5 | Ejecutar sync manual y validar | `ingestion/scripts/validate_raw_data.py` |
| 2.6 | Documentar proceso | `ingestion/airbyte/README.md` |

---

### Fase 3: Procesamiento con Spark (Avance 3 — 15 pts)

| # | Tarea | Archivo/Recurso |
|---|---|---|
| 3.1 | Factory de SparkSession reutilizable | `processing/jobs/utils/spark_session.py` |
| 3.2 | Job `raw_to_processed.py` con: joins, limpieza, tipado, modelo dimensional | `processing/jobs/raw_to_processed.py` |
| 3.3 | Job `processed_to_gold.py` con: agregaciones por preguntas de negocio | `processing/jobs/processed_to_gold.py` |
| 3.4 | Optimizaciones: `repartition()`, `.cache()`, broadcast joins, `coalesce()` | Dentro de cada job |
| 3.5 | Script `spark-submit.sh` listo para EC2/EMR | `processing/spark-submit.sh` |
| 3.6 | Tests unitarios de transformaciones | `quality/tests/test_transformations.py` |

---

### Fase 4: Orquestación y CI/CD (Avance 4 — 15 pts)

| # | Tarea | Archivo/Recurso |
|---|---|---|
| 4.1 | `docker-compose.yml` de Airflow (CeleryExecutor o LocalExecutor) | `orchestration/docker-compose.yml` |
| 4.2 | Dockerfile custom con `boto3`, `pyspark` | `orchestration/Dockerfile` |
| 4.3 | DAG `etlt_pipeline_dag.py` con: `TriggerDagRunOperator` o `AirbyteOperator` → `SparkSubmitOperator` → mover a gold | `orchestration/dags/etlt_pipeline_dag.py` |
| 4.4 | Alertas Slack/Email en callbacks | `orchestration/dags/utils/slack_alerts.py` |
| 4.5 | GitHub Actions CI: lint + pytest en cada push | `.github/workflows/ci.yml` |
| 4.6 | GitHub Actions Deploy: sync DAGs a EC2 | `.github/workflows/deploy_dags.yml` |
| 4.7 | Integrar Great Expectations como task del DAG | `quality/great_expectations/` |

---

### Fase 5: Streaming con Kafka (Avance 5 — 10 pts)

| # | Tarea | Archivo/Recurso |
|---|---|---|
| 5.1 | `docker-compose.yml` para Kafka + Zookeeper | `streaming/docker-compose.yml` |
| 5.2 | Script para crear topics | `streaming/scripts/create_topics.sh` |
| 5.3 | `weather_producer.py` simulando eventos weather en tiempo real | `streaming/producer/weather_producer.py` |
| 5.4 | `spark_streaming_consumer.py`: lee Kafka → escribe `raw-streaming/` | `streaming/consumer/spark_streaming_consumer.py` |
| 5.5 | Enriquecimiento con datos batch → `processed-streaming/` | Dentro del consumer |
| 5.6 | Unificación batch + streaming → `gold/` (Lambda Architecture) | Dentro del consumer o job separado |
| 5.7 | Actualizar diagrama de arquitectura | `docs/diagrama_arquitectura.drawio` |

---

## 6. Convenciones de Git

| Aspecto | Convención |
|---|---|
| **Branches** | `main` (producción), `dev` (desarrollo), `feature/{avance}-{descripcion}` |
| **Commits** | Conventional Commits: `feat:`, `fix:`, `docs:`, `chore:`, `refactor:` |
| **PRs** | Uno por avance, con descripción y checklist |
| **Tags** | `v1.0-avance1`, `v2.0-avance2`, etc. |

---

## 7. Verificación

### Automatizada
- `pytest quality/tests/` — Tests unitarios de transformaciones
- `flake8 . --max-line-length=120` — Lint de código Python
- GitHub Actions ejecuta ambos en cada push

### Manual (por Avance)
1. **Avance 1**: Verificar bucket S3 existe con estructura correcta vía AWS Console
2. **Avance 2**: Verificar archivos Parquet en `raw/` tras sync de Airbyte
3. **Avance 3**: Ejecutar `spark-submit` y verificar datos en `processed/` y `gold/`
4. **Avance 4**: Trigger manual del DAG en Airflow y verificar ejecución completa
5. **Avance 5**: Enviar mensajes al topic Kafka y verificar datos en `raw-streaming/` → `gold/`
