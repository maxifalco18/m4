# 📸 Evidencia Visual del Pipeline

Esta carpeta contiene capturas de pantalla que validan la ejecución real del pipeline.

---

## Checklist de Evidencias a Capturar

### 1. Airbyte Cloud — Syncs Exitosos

**Dónde**: https://cloud.airbyte.com → Connections

| Screenshot | Qué mostrar | Nombre sugerido |
|------------|-------------|-----------------|
| `01_airbyte_ecommerce_sync.png` | Connection `postgres_olist → S3` con estado **Succeeded** + timestamp | Subir aquí |
| `02_airbyte_weather_sync.png` | Connection `openweathermap_api → S3` con estado **Succeeded** | Subir aquí |
| `03_airbyte_sync_logs.png` | Logs detallados de un sync exitoso (rows synced + files written) | Subir aquí |

> **Cómo capturar**: Airbyte Cloud → tu workspace → Connections → clic en la connection → "Job History" → screenshot del último sync exitoso

---

### 2. AWS S3 — Datos Aterrizados en Raw Layer

**Dónde**: https://s3.console.aws.amazon.com → tu bucket → raw/

| Screenshot | Qué mostrar | Nombre sugerido |
|------------|-------------|-----------------|
| `04_s3_raw_ecommerce_structure.png` | Vista del prefijo `raw/ecommerce/` mostrando las 9 tablas | Subir aquí |
| `05_s3_raw_weather_files.png` | Vista de `raw/weather_api/YYYY/MM/DD/` con archivos `.parquet` | Subir aquí |
| `06_s3_gold_kpis.png` | Vista de `gold/` mostrando los 7 prefijos de KPIs | Subir aquí |

> **Cómo capturar**: AWS Console → S3 → bucket → navegar al prefijo → screenshot mostrando la lista de objetos con sus tamaños y fechas

---

### 3. Airflow Web UI — DAG Ejecutado

**Dónde**: http://`<EC2-IP>`:8080 (o localhost:8080 en local)

| Screenshot | Qué mostrar | Nombre sugerido |
|------------|-------------|-----------------|
| `07_airflow_dag_graph.png` | Vista **Graph** del DAG `etlt_pipeline_v1` con todos los tasks en verde (succeeded) | Subir aquí |
| `08_airflow_dag_runs.png` | Vista **Grid** mostrando múltiples ejecuciones exitosas | Subir aquí |
| `09_airflow_task_logs.png` | Logs de uno de los tasks de Spark (duración + output) | Subir aquí |

> **Cómo capturar**: Airflow UI → DAGs → `etlt_pipeline_v1` → clic en la última run → screenshot

---

### 4. Kafka UI — Topics con Mensajes

**Dónde**: http://localhost:8090 (o `<EC2-IP>`:8090)

| Screenshot | Qué mostrar | Nombre sugerido |
|------------|-------------|-----------------|
| `10_kafka_topic_weather.png` | Topic `weather-events-rt` con mensajes producidos (offset, timestamp, payload) | Subir aquí |
| `11_kafka_consumer_group.png` | Consumer group `spark-streaming-consumer` con lag = 0 (mensajes consumidos) | Subir aquí |

> **Cómo capturar**: Kafka UI (Provectus) → Topics → `weather-events-rt` → Messages → screenshot

---

### 5. GitHub Actions — CI/CD Pasando

**Dónde**: https://github.com/maxifalco18/m4/actions

| Screenshot | Qué mostrar | Nombre sugerido |
|------------|-------------|-----------------|
| `12_github_ci_passing.png` | Workflow `CI Pipeline` con todos los checks en verde (lint + tests + DAG validation) | Subir aquí |
| `13_github_deploy_dags.png` | Workflow `Deploy DAGs to EC2` con estado **Success** | Subir aquí |

---

## Una vez que tengas las capturas

1. Guardalas en esta carpeta: `docs/screenshots/`
2. Referencia las más importantes en el `README.md` principal del proyecto
3. Incluí las más visuales en la presentación final (slides)

---

---

## Estado Actual

> [!NOTE]
> Las capturas se generarán durante la ejecución del pipeline en la demo final.
> Para reproducir, ejecutar los servicios según el `README.md` principal del proyecto.
