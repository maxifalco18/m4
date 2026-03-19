# Documento Técnico — Pipeline ETLT Escalable sobre Data Lake en AWS
### Arquitectura Medallion + Lambda · AWS S3 · Spark · Airflow · Kafka

**Proyecto Integrador M4 — Data Engineering Bootcamp (ByHENRY)**  
**Autor:** Maximiliano Falco | **Fecha:** Marzo 2026 | **Versión:** Final  
**Cubre:** Avances 1–5 + CI/CD + Calidad de Datos

---

## 1. Introducción y Contexto

La organización requiere migrar su infraestructura de datos a la nube para gestionar fuentes de información heterogéneas y crecientes. El desafío central es diseñar un pipeline de tipo **ETLT** (Extract, Transform, Load, Transform) que sea escalable, costo-eficiente y que permita responder a preguntas de negocio concretas sobre comportamiento de clientes, performance de ventas y factores externos como el clima.

El presente documento describe la arquitectura técnica implementada, justifica las decisiones de diseño adoptadas en cada avance, documenta los resultados del pipeline y demuestra cómo el sistema responde a las preguntas de negocio planteadas.

---

## 2. Arquitectura General: Medallion + Lambda

El sistema implementa una **Arquitectura Medallion** en tres capas (Bronze, Silver, Gold) sobre AWS S3, extendida con los principios de la **Arquitectura Lambda** para soportar procesamiento batch y streaming en forma unificada.

### 2.1 Diagrama de Flujo de Datos
> 📐 Ver el diagrama interactivo de la arquitectura en [arquitectura_pipeline.md](./arquitectura_pipeline.md)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  FUENTES DE DATOS                                                           │
│                                                                             │
│  [OpenWeatherMap API]              [PostgreSQL E-commerce — Supabase/Neon]  │
│  Clima horario (JSON, REST)        Órdenes, Clientes, Productos, Pagos      │
└──────────────┬──────────────────────────────────────────────────┬───────────┘
               │                  AIRBYTE CLOUD                   │
               │         (Ingesta batch — manual diaria)          │
               ▼                                                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  🟤 CAPA RAW — Bronze (Inmutable)                                           │
│  s3://bucket/raw/weather_api/YYYY/MM/DD/   s3://bucket/raw/ecommerce/*/    │
│  Formato: Parquet (Airbyte output) | Versionado: habilitado en S3           │
│  Gobernanza: Lake Formation — DB: raw_weather, raw_ecommerce               │
└──────────────────────────────────────────────────┬──────────────────────────┘
                                                   │
                                        ┌──────────┴──────────┐
                                        │   APACHE SPARK      │
                                        │   (EC2 / EMR)       │
                                        │   spark-submit      │
                                        └──────────┬──────────┘
                                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  ⚪ CAPA PROCESSED — Silver (Modelo Dimensional)                            │
│  dim_customers / dim_products / dim_sellers / dim_date / dim_geolocation   │
│  fact_orders / fact_order_items / fact_payments / fact_reviews              │
│  weather_enriched                                                           │
│  Formato: Parquet (Snappy) | Schema: Estrella | DB: processed              │
└──────────────────────────────────────────────────┬──────────────────────────┘
                                                   │
                                        ┌──────────┴──────────┐
                                        │   APACHE SPARK      │
                                        │   (Agregaciones)    │
                                        └──────────┬──────────┘
                                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  🟡 CAPA GOLD (KPIs Analíticos — Batch + Streaming Unificado)               │
│  kpi_top_products / kpi_customer_rfm / kpi_revenue_by_region               │
│  kpi_weather_sales_impact / unified_analytics                              │
│  Formato: Parquet o Delta Lake | Optimizado para consumo analítico          │
└─────────────────────────────────────────────────────────────────────────────┘
          ▲  ▲
          │  │  ARQUITECTURA LAMBDA — Unificación Batch + Real-time
          │  │
 ┌────────┴──┴───────────────────────────────────────────────────────────────┐
 │  CAPA STREAMING                                                            │
 │                                                                            │
 │  [OpenWeatherMap API] ──► weather_producer.py                             │
 │                                    │                                       │
 │                                    ▼                                       │
 │         APACHE KAFKA (Docker Compose en EC2)                               │
 │         Topic: weather-events-rt   (3 particiones, 24h retención)          │
 │                                    │                                       │
 │                                    ▼                                       │
 │  Spark Structured Streaming consumer (foreachBatch + watermark 2h)        │
 │          │                          │                                       │
 │          ▼                          ▼                                       │
 │  raw-streaming/           processed-streaming/                             │
 │  (Parquet append)         (enriquecido con processed/)                     │
 │                                    │                                       │
 │                                    ▼                                       │
 │         UNION con Batch View  ──► gold/lambda_weather_unified/             │
 │         (Serving Layer — Batch ∪ Real-time)                                │
 │                                                                            │
 │  Checkpoints: s3://bucket/_checkpoints/streaming_*  (fault-tolerant)      │
 └────────────────────────────────────────────────────────────────────────────┘
 
      Orquestación: Apache Airflow (EC2 + Docker) | Monitoreo: DAG + Alertas Slack
      Streaming DAG: streaming_pipeline_dag.py (opcional, documenta automatización)
      Gobernanza:   AWS Lake Formation + IAM Roles (sin access keys en texto plano)
      CI/CD:        GitHub Actions (lint + tests + deploy DAGs automático a EC2)
```

### 2.2 Propósito de Cada Capa

| Capa | Nombre | Descripción | Inmutabilidad |
|------|--------|-------------|---------------|
| **Raw** | Bronze | Datos tal como llegan de la fuente. Sin transformación. Verdad histórica del sistema. | ✅ Inmutable |
| **Processed** | Silver | Datos limpiados, tipados y modelados (esquema estrella). Elimina duplicados y nulos críticos. | ❌ Puede re-generarse |
| **Gold** | Gold | KPIs y agregados listos para análisis. Optimizados para lectura por analistas/científicos. | ❌ Puede re-generarse |
| **Streaming** | Lambda | Eventos en tiempo real que se unifican con los datos batch en la capa Gold. | ❌ Continuamente actualizado |

---

## 3. Fuentes de Datos

### 3.1 API Pública — OpenWeatherMap

| Campo | Detalle |
|-------|---------|
| **API** | OpenWeatherMap History (hourly) |
| **Datos** | Temperatura, humedad, presión, viento, nubosidad, precipitación |
| **Granularidad** | Horaria, por ciudad/coordenada |
| **Frecuencia** | Manual 1x/día (límite Free Tier de Airbyte) |
| **Relevancia** | Permite responder: ¿el clima impacta en las ventas? (cross-analysis con órdenes) |
| **Sample** | `Patagonia_-41.json` — datos horarios del 01/08/2024 al 31/12/2024 |

### 3.2 Base de Datos Relacional — E-commerce (Supabase/Neon)

| Campo | Detalle |
|-------|---------|
| **Sistema** | PostgreSQL 15 (Supabase o Neon — cloud native, acceso seguro) |
| **Dataset** | Dataset tipo Olist/Retail con tablas de ventas, clientes, productos, geografía |
| **Tablas** | `orders`, `customers`, `products`, `order_items`, `sellers`, `payments`, `reviews`, `geolocation`, `categories` |
| **Carga inicial** | Full Refresh (carga completa) |
| **Carga incremental** | CDC con campo `updated_at` (Airbyte incremental sync) |
| **Seguridad** | Usuario `readonly_user` — sin permisos de escritura |

---

## 4. Decisiones de Diseño

### 4.1 Particionamiento en S3

Los datos en la capa raw se particionan por `YYYY/MM/DD/` para:
- **Eficiencia en queries**: Spark solo lee las particiones necesarias (partition pruning)
- **Schema Evolution**: cada sync de Airbyte escribe en su propia partición de fecha sin romper datos anteriores
- **Lifecycle policies**: las particiones más antiguas migran automáticamente a STANDARD_IA → GLACIER

### 4.2 Formato Parquet vs. CSV

Se eligió **Parquet** como formato estándar porque:
- Columnar: las queries analíticas leen solo las columnas necesarias (10-100x más rápido)
- Compresión Snappy: ~70% menos espacio que CSV, con compresión/descompresión rápida
- Typed schema: previene errores de tipo en lecturas de Spark
- Compatible con Athena, Redshift Spectrum, y cualquier herramienta del ecosistema AWS

Para la capa Gold se evaluó **Delta Lake** por su soporte de ACID transactions y schema evolution automática, especialmente útil en la unificación Lambda de datos batch + streaming.

### 4.3 Puntos de Falla (SPOF) y Mitigaciones

| Componente | SPOF | Mitigación |
|------------|------|------------|
| **S3** | Falla del servicio AWS | S3 tiene SLA 99.99%. Versionado habilitado para recuperación |
| **Airbyte Cloud** | Sincronización fallida | Validación post-sync (`validate_raw_data.py`) + alerta en Airflow |
| **EC2 (Airflow)** | Instancia termina | Docker Compose con restart policy. Logs persistidos en S3 |
| **EC2 (Spark)** | OOM/Timeout en job | Retries en spark-submit + checkpoints de Spark SS en S3 |
| **Kafka** | Broker caído | Réplica mínima de particiones. Spark SS con checkpoints para retoma |
| **Glue Catalog** | Metadata desincronizada | Crawlers periódicos o escritura con `enableHiveSupport()` en Spark |

### 4.4 Escalabilidad Horizontal

El diseño soporta escalar sin cambios arquitecturales:
- **Más datos**: S3 es prácticamente ilimitado. Solo cambia el costo de almacenamiento
- **Más fuentes**: Agregar conectores en Airbyte + nuevas carpetas en raw/
- **Más procesamiento**: Migrar de EC2 (+spark-submit) a EMR o Databricks sin cambiar el código PySpark
- **Más consumers de Gold**: Athena, QuickSight, Redshift Spectrum leen Parquet directamente de S3

---

## 5. Gobernanza y Seguridad

### 5.1 IAM Roles (sin Access Keys en texto plano)

```
LakeFormationAdmin  → Administra catálogo y permisos de Lake Formation
AirbyteS3Role      → Solo escribe en raw/ (External ID para mayor seguridad)
SparkProcessingRole → Lee raw/, escribe processed/ y gold/
AirflowEC2Role     → Orquesta y escribe en gold/; puede asumir SparkProcessingRole
```

### 5.2 Cifrado

- **En tránsito**: TLS obligatorio (bucket policy deniega requests sin HTTPS)
- **En reposo**: SSE-S3 (AES-256) habilitado como default en todo el bucket

### 5.3 Control de Acceso a Datos (Lake Formation)

Lake Formation implementa **data mesh** de facto: un único catálogo central (Glue) con permisos a nivel de base de datos y tabla. Los analistas de negocio pueden acceder únicamente a la base de datos `gold`, sin visibilidad de raw o processed.

---

## 6. Estimación de Costos (AWS Free Tier)

| Servicio | Uso Estimado | Costo Free Tier |
|----------|-------------|-----------------|
| S3 | < 5 GB (datos de prueba) | Gratis (5 GB/mes) |
| EC2 t2.micro (Airflow) | < 750 h/mes | Gratis (750 h/mes) |
| EC2 t2.micro (Kafka) | < 750 h/mes | Gratis (750 h/mes) |
| Glue Data Catalog | < 1M objetos | Gratis (1M objetos/mes) |
| Lake Formation | Sin costo propio | Gratis |
| Airbyte Cloud | < syncs/mes | Free Tier |

> ⚠️ **Nota**: Detener las instancias EC2 cuando no se estén usando. El Free Tier cubre 750h/mes *por cuenta*, no por instancia. Si hay 2 instancias simultáneas se consume el doble.

---

## 7. Resultados del Pipeline — KPIs Implementados

Los jobs de Spark (`raw_to_processed.py` + `processed_to_gold.py`) producen **7 KPIs analíticos** en la capa Gold que responden directamente las preguntas de negocio del proyecto:

| KPI (capa Gold) | Pregunta de negocio que responde | Dimensiones | Métrica clave |
|-----------------|----------------------------------|-------------|---------------|
| `kpi_top_products_by_category` | ¿Cuáles son los productos más vendidos por categoría? | Categoría, Mes/Año | Revenue total + unidades |
| `kpi_customer_rfm` | ¿Qué clientes presentan mayor frecuencia y ticket? | Segmento RFM | Score R + F + M por cliente |
| `kpi_revenue_by_region` | ¿Qué regiones generan más ingresos? | Estado, Trimestre | Revenue + estacionalidad |
| `kpi_new_vs_returning_customers` | ¿Qué proporción son clientes nuevos vs. recurrentes? | Mes/Año, Tipo cliente | % órdenes por tipo |
| `kpi_price_volume_correlation` | ¿Qué relación existe entre precio y volumen? | Bucket de precio, Categoría | Unidades por bracket |
| `kpi_weather_sales_impact` | ¿Cómo impacta el clima en las ventas? | Temperatura, Lluvia, Día | Órdenes + Revenue por condición |
| `kpi_payment_methods` | ¿Cómo varía el desempeño por método de pago? | Método, Cuotas | Revenue + cuotas promedio |

Además, la **Arquitectura Lambda** (Avance 5) unifica los datos batch históricos con los eventos de clima en tiempo real de Kafka en `gold/lambda_weather_unified/`, permitiendo a los analistas consumir una capa Gold siempre actualizada.

---

## 8. Conclusiones

El sistema implementado cumple con los objetivos del proyecto integrador en sus cinco dimensiones principales:

1. **Arquitectura escalable**: El stack AWS S3 + Spark + Airflow + Kafka soporta horizontalmente mayores volúmenes sin cambios de código. La migración de EC2 a EMR o Databricks es directa.

2. **Gobernanza y seguridad**: IAM Roles con mínimo privilegio, cifrado SSE-S3 en reposo, TLS obligatorio en tránsito, y Lake Formation para control de acceso granular por capa.

3. **Calidad garantizada**: Validación post-ingesta (`validate_raw_data.py`), tests unitarios de PySpark (pytest), linting CI (flake8), y validación de DAGs en cada push.

4. **Automatización completa**: El pipeline diario se ejecuta sin intervención manual — Airflow orquesta Airbyte + Spark, GitHub Actions despliega los DAGs automáticamente ante cada cambio en `main`.

5. **Integración real-time**: La Arquitectura Lambda unifica datos históricos (batch) y datos en tiempo real (Kafka/Spark SS) en una capa Gold unificada, completando el ciclo de procesamiento moderno.

El diseño está preparado para extenderse con nuevas fuentes de datos (Airbyte conectores adicionales), nuevas preguntas de negocio (nuevos KPIs en `processed_to_gold.py`), y mayor escala de procesamiento (EMR, MSK, Glue) sin cambios arquitecturales sustanciales.
