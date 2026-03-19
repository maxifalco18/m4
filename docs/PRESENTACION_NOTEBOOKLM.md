# Pipeline ETLT Escalable sobre Data Lake en AWS — Presentación Final
## Proyecto Integrador M4 · Data Engineering Bootcamp (ByHENRY)
### Autor: Maximiliano Falco · Marzo 2026

---

## 1. El Problema

Una organización en crecimiento necesita migrar su infraestructura de datos a la nube. Los desafíos específicos son:

- **Fuentes heterogéneas**: Datos transaccionales en PostgreSQL (e-commerce Olist con 9 tablas y más de 100 mil órdenes) y datos meteorológicos de una API REST (OpenWeatherMap).
- **Escala creciente**: Volúmenes que superan la capacidad de procesamiento local.
- **Decisiones basadas en datos**: La empresa necesita KPIs actualizados para responder preguntas concretas de negocio.
- **Tiempo real**: Se requiere visibilidad inmediata de eventos (como cambios climáticos) que impactan las ventas.

### Preguntas de Negocio Clave

Estas son las preguntas de negocio que el pipeline debe responder:

1. ¿Cuáles son los productos más vendidos por categoría y cómo varía su demanda a lo largo del tiempo?
2. ¿Qué clientes presentan mayor frecuencia de compra y cuál es su ticket promedio mensual?
3. ¿Qué regiones generan los mayores ingresos y cómo se comportan frente a la estacionalidad?
4. ¿Qué proporción de ventas proviene de clientes nuevos frente a clientes recurrentes?
5. ¿Qué relación existe entre el precio promedio y el volumen de ventas en cada categoría?
6. ¿Cómo impactan las condiciones climáticas en el volumen de ventas?
7. ¿Cómo varía el desempeño de ventas en función del método de pago utilizado?

---

## 2. La Solución: Arquitectura Medallion + Lambda

Se implementó un pipeline de tipo ETLT (Extract, Transform, Load, Transform) sobre una arquitectura de Data Lake Moderno en AWS. La solución combina dos patrones arquitectónicos:

### Arquitectura Medallion (Batch)

La data fluye a través de tres capas claramente definidas:

- **Bronze (Raw)**: Datos crudos e inmutables tal como llegan desde las fuentes, en formato Parquet con compresión Snappy. Se almacenan en el prefijo `raw/` del bucket S3. Nunca se modifican, lo que permite reprocesamiento completo.
- **Silver (Processed)**: Datos limpiados, deduplicados y estructurados en un modelo dimensional de estrella. Se almacenan en `processed/`. Incluye 5 tablas de dimensiones (customers, products, sellers, date, geolocation) y 4 tablas de hechos (orders, order_items, payments, reviews), más una tabla de clima enriquecida.
- **Gold**: KPIs de negocio precalculados y listos para consumo analítico. Se almacenan en `gold/`. Cada KPI es una carpeta independiente en formato Parquet optimizado.

### Arquitectura Lambda (Streaming)

Para el procesamiento en tiempo real, se implementó una Arquitectura Lambda que unifica dos vistas:

- **Batch View**: Los datos históricos procesados por Spark en la capa Silver/Gold (ejecución programada, una vez al día).
- **Real-time View**: Los datos de streaming procesados por Spark Structured Streaming (micro-batches cada 30 segundos).
- **Serving Layer (Unified Gold)**: La unión de ambas vistas en una capa Gold unificada (`gold/lambda_weather_unified/`), donde los analistas pueden consumir datos de batch y streaming de manera transparente.

---

## 3. Stack Tecnológico y Justificación

Cada herramienta fue seleccionada con criterios de escalabilidad, costo (Free Tier de AWS) y facilidad de integración:

### Almacenamiento: AWS S3
Se eligió S3 por su durabilidad (99.999999999%), escalabilidad ilimitada y costo mínimo. Se activó el cifrado SSE-S3 para datos en reposo y se bloqueó el acceso público con `PUT_PUBLIC_ACCESS_BLOCK`. El bucket policy fuerza HTTPS (Deny HTTP) para proteger los datos en tránsito. Se usa Parquet como formato columnar, que reduce el espacio de almacenamiento entre un 60% y un 80% comparado con CSV y permite lecturas selectivas por columna.

### Gobernanza: AWS Lake Formation + IAM
Lake Formation centraliza la gobernanza de datos. Se crearon 4 databases en el Glue Data Catalog (raw_ecommerce, raw_weather, processed, gold). Los permisos se asignan con el principio de privilegio mínimo: Airbyte solo puede escribir en raw, Spark puede leer raw y escribir en processed y gold, Airflow solo gestiona la orquestación. Cada rol IAM tiene exactamente los permisos que necesita y ni uno más.

### Ingesta: Airbyte Cloud
Se eligió Airbyte por su capacidad de configurar conectores sin código. Se configuraron dos conectores: PostgreSQL (con modo incremental para CDC) y HTTP genérico para OpenWeatherMap. Airbyte escribe directamente en S3 en formato Parquet, eliminando la necesidad de un ETL intermedio. Se documentaron todas las conexiones con archivos JSON de configuración.

### Procesamiento: Apache Spark (PySpark)
Spark es el estándar para procesamiento distribuido de datos. Se implementaron dos jobs principales:
- `raw_to_processed.py`: Crea el modelo estrella con optimizaciones como Adaptive Query Execution (AQE), Broadcast Joins explícitos para tablas pequeñas (eliminando shuffles costosos) y Partition Pruning.
- `processed_to_gold.py`: Calcula 7 KPIs con caché estratégico y reutilización de DataFrames.
El script `spark-submit.sh` permite ejecutar en tres modos: local (laptop), EC2 (single node) y EMR (cluster YARN).

### Orquestación: Apache Airflow
Airflow se despliega en Docker Compose sobre EC2 con un DAG principal (`etlt_pipeline_v1`) que orquesta todo el pipeline ETLT. Usa TaskGroups para organizar las tareas lógicamente (Ingesta, Validación, Procesamiento). Incluye SLAs, 3 reintentos con backoff exponencial y notificaciones a Slack tanto en éxito como en fallo.

### Streaming: Apache Kafka + Spark Structured Streaming
Kafka actúa como broker de mensajes en tiempo real. Un productor simula eventos meteorológicos y los publica en el topic `weather_events`. Un consumidor Spark Structured Streaming lee de Kafka, aplica transformaciones y escribe en S3. Se implementaron Watermarking (2 horas) para manejar datos tardíos y Checkpointing en S3 para fault tolerance.

### CI/CD: GitHub Actions
Se implementaron dos workflows:
- `CI Pipeline`: En cada push a main y dev, ejecuta linting (flake8), tests unitarios (pytest) y validación de sintaxis de DAGs.
- `Deploy DAGs to EC2`: Cuando cambian archivos en `orchestration/dags/`, sincroniza los DAGs via rsync SSH y verifica que Airflow los parsee correctamente.

### Calidad de Datos: Great Expectations + pytest
Great Expectations valida los datos de la capa raw (nulos, tipos, rangos válidos). Se definieron expectation suites y checkpoints que se ejecutan como tareas dentro del DAG de Airflow, antes de las transformaciones de Spark. Adicionalmente, pytest valida las funciones de transformación con 13 tests unitarios.

---

## 4. Los 7 KPIs Analíticos

Cada KPI responde directamente a una pregunta de negocio definida en las consignas:

### KPI 1: Top Productos por Categoría
Identifica los 10 productos más vendidos dentro de cada categoría, con métricas de revenue total y cantidad de unidades. Responde a: "¿Cuáles son los productos más vendidos por categoría?"

### KPI 2: Segmentación RFM de Clientes (Avanzado)
Clasifica a cada cliente según tres dimensiones: Recency (cuándo fue su última compra), Frequency (cuántas veces compró), y Monetary (cuánto gastó en total). Cada dimensión se puntúa de 1 a 5 usando percentiles. Los clientes se segmentan en grupos como "Champions", "Loyal Customers", "At Risk", etc. Responde a: "¿Qué clientes presentan mayor frecuencia de compra?"

### KPI 3: Ingresos por Región
Agrega el revenue total por estado brasileño y lo enriquece con datos geográficos. Permite identificar las regiones con mayor y menor contribución al negocio. Responde a: "¿Qué regiones generan los mayores ingresos?"

### KPI 4: Clientes Nuevos vs Recurrentes
Clasifica cada cliente como "nuevo" (una sola compra) o "recurrente" (más de una), y calcula la proporción de cada grupo sobre el total de ventas. Responde a: "¿Qué proporción de ventas proviene de clientes nuevos frente a recurrentes?"

### KPI 5: Correlación Precio-Volumen
Analiza la relación entre el precio promedio de los productos en cada categoría y el volumen total vendido. Permite identificar si los productos más caros se venden menos. Responde a: "¿Qué relación existe entre el precio promedio y el volumen de ventas?"

### KPI 6: Impacto del Clima en Ventas (Cross-domain)
Este es un KPI avanzado que cruza datos de dos fuentes completamente distintas: las ventas del e-commerce (PostgreSQL) con los datos meteorológicos (OpenWeatherMap API). Analiza si las condiciones climáticas (temperatura, humedad, velocidad del viento) correlacionan con cambios en el volumen de ventas. Responde a: "¿Cómo impactan las condiciones climáticas en las ventas?"

### KPI 7: Análisis de Métodos de Pago
Desglosa el revenue y la cantidad de transacciones por cada método de pago (tarjeta de crédito, boleto, voucher, débito). Identifica cuál método genera más ingresos y cuál es el más popular. Responde a: "¿Cómo varía el desempeño de ventas en función del método de pago?"

---

## 5. Optimizaciones de Spark (Nivel Senior)

El código de Spark no solo funciona, sino que está optimizado para producción:

### Adaptive Query Execution (AQE)
Habilitado globalmente. Spark ajusta automáticamente el plan de ejecución en runtime basándose en estadísticas reales. Esto incluye la coalescencia automática de particiones pequeñas post-shuffle, la optimización de joins skew y la selección dinámica del tipo de join más eficiente.

### Broadcast Joins Explícitos
Las tablas de dimensiones pequeñas (como `dim_date`, `dim_sellers`, `dim_geolocation`) se marcan explícitamente con `F.broadcast()` para que Spark las distribuya a todos los nodos en lugar de hacer un shuffle completo. Esto elimina el paso más costoso de un join distribuido. El umbral de broadcast se configuró en 50 MB.

### Partition Pruning
Los datos en Gold se particionan por campos relevantes (como `category`, `customer_state`, `payment_type`), lo que permite que Spark solo lea las particiones necesarias en queries posteriores, reduciendo drásticamente el I/O.

### Control de Shuffles
Cada transformación en `transformations.py` documenta qué tipo de operación de shuffle genera (narrow vs wide). Las funciones utilitarias están diseñadas para minimizar los shuffles: la deduplicación usa sort-based dedup sobre la clave primaria, y las normalizaciones de strings son transformaciones narrow (sin shuffle).

---

## 6. Seguridad y Gobernanza

La seguridad se implementó en múltiples capas:

### Capa IAM
Se crearon roles específicos con privilegio mínimo: AirbyteS3Role (solo PUT en raw), SparkProcessingRole (lectura en raw, escritura en processed y gold), AirflowEC2Role (gestión de jobs), LakeFormationAdmin (gobernanza). Ningún componente tiene más permisos de los que necesita.

### Capa S3
El bucket policy fuerza HTTPS para todas las conexiones (Deny HTTP), bloquea el acceso público con PUT_PUBLIC_ACCESS_BLOCK, y aplica cifrado SSE-S3 para datos en reposo. Las credenciales se manejan vía IAM Roles, nunca hardcodeadas en el código.

### Capa Lake Formation
Centraliza la catalogación de datos en el Glue Data Catalog y aplica permisos a nivel de database y tabla, complementando a IAM con control de acceso fino sobre los datos del Lake.

### Manejo de Secretos
El archivo `.env` contiene placeholders (CHANGEME) y NO se commitea al repositorio. Las credenciales sensibles se inyectan como variables de entorno o como GitHub Secrets para CI/CD.

---

## 7. Calidad de Datos

La calidad se asegura en dos niveles:

### Great Expectations (Framework)
Se configuró una expectation suite `raw_ecommerce_orders` que valida la capa raw antes de que Spark la procese. Las validaciones incluyen: columnas no nulas en campos críticos (order_id, customer_id), tipos de datos correctos y valores dentro de rangos esperados. El checkpoint se ejecuta como tarea del DAG, bloqueando el procesamiento si la validación falla.

### Tests Unitarios con pytest
13 tests que corren en CI y localmente, verificando:
- Funciones reales de transformación: `drop_duplicates_by_key`, `normalize_string_columns`
- Utilidades de calidad de datos: `check_nulls`, `check_unique`, `check_range`, `check_allowed_values`
- Deduplicación, filtrado de nulos, renombrado de columnas, rangos válidos de temperatura

### Validación de Schema Drift
El script `validate_raw_data.py` compara el schema actual de los datos con el esperado, detectando automáticamente columnas nuevas, eliminadas o con cambios de tipo. Esto protege contra cambios no anunciados en las fuentes de datos.

---

## 8. Streaming y Arquitectura Lambda

La implementación de streaming es uno de los diferenciadores del proyecto:

### Productor (weather_producer.py)
Simula un flujo de datos en tiempo real leyendo registros históricos de clima y publicándolos como eventos JSON al topic de Kafka `weather_events`, con un delay configurable entre mensajes. Cada mensaje contiene timestamp, temperatura, humedad, velocidad del viento y nombre de ciudad.

### Kafka
Desplegado con Docker Compose (Zookeeper + Kafka Broker + Kafka UI). El topic `weather_events` tiene 3 particiones y retención de 24 horas. Kafka UI en el puerto 8090 permite monitorear los mensajes en tiempo real.

### Consumidor (spark_streaming_consumer.py)
Spark Structured Streaming lee de Kafka en micro-batches de 30 segundos. Cada micro-batch se procesa así:
1. Los datos JSON se parsean con el schema esperado (`WEATHER_SCHEMA`).
2. Se aplica un Watermark de 2 horas para tolerar datos tardíos.
3. Los datos crudos se escriben en `raw-streaming/` (append mode).
4. La función `process_micro_batch` implementa la unificación Lambda: lee la vista batch estática de `processed/weather_enriched`, le hace UNION con los datos del micro-batch actual, y escribe el resultado en `gold/lambda_weather_unified/`.
5. Checkpointing en S3 garantiza fault tolerance (si el consumidor se cae, retoma desde el último checkpoint).

### Watchdog (streaming_pipeline_dag.py)
Un DAG de Airflow que corre cada 5 minutos y verifica si el consumidor Spark está corriendo. Si detecta que se cayó, lo reinicia automáticamente con spark-submit.

---

## 9. CI/CD con GitHub Actions

Se implementaron dos pipelines de GitHub Actions:

### CI Pipeline (ci.yml)
Se ejecuta en cada push y pull request a las ramas main y dev:
1. Lint con flake8 (PEP-8, líneas de hasta 120 caracteres).
2. Tests unitarios con pytest y reporte de cobertura.
3. Validación de sintaxis de DAGs de Airflow (importa cada .py y verifica que no explote).
4. Upload de cobertura a Codecov.

### Deploy DAGs (deploy_dags.yml)
Se ejecuta solo en pushes a main que modifican archivos en `orchestration/dags/`:
1. Valida la sintaxis de los DAGs antes de deployar.
2. Sincroniza los DAGs a la instancia EC2 via rsync (SSH).
3. Verifica que Airflow en el servidor pueda parsear los DAGs nuevos.
4. Notifica el resultado (éxito o fallo) a un canal de Slack.

---

## 10. Estructura del Repositorio

El proyecto está organizado por componentes funcionales:

- `docs/`: Documentación técnica, decisiones tecnológicas, diagramas de arquitectura y screenshots.
- `infrastructure/`: Scripts Python para provisionar AWS (create_bucket.py, setup_lakeformation.py, iam_roles.json, bucket_policy.json).
- `ingestion/`: Configuraciones JSON de Airbyte, scripts de seed de datos y validación (validate_raw_data.py, seed_data.py).
- `processing/`: Jobs de PySpark (raw_to_processed.py, processed_to_gold.py) con utilidades (spark_session.py, transformations.py, data_quality.py) y el script spark-submit.sh multi-entorno.
- `orchestration/`: DAGs de Airflow (etlt_pipeline_dag.py, streaming_pipeline_dag.py, s3_compaction_dag.py), Docker Compose para Airflow, y utilidades (slack_alerts.py).
- `streaming/`: Productor (weather_producer.py), consumidor (spark_streaming_consumer.py), Docker Compose para Kafka y script de creación de topics.
- `quality/`: Configuración de Great Expectations (suites, checkpoints) y tests unitarios de pytest.
- `.github/workflows/`: Pipelines de CI/CD.

---

## 11. Conclusiones

Este proyecto implementa un pipeline de datos de nivel producción que:

1. **Responde 7 preguntas de negocio** concretas con KPIs precalculados y listos para consumo.
2. **Unifica batch y streaming** con una Arquitectura Lambda que permite tomar decisiones tanto con datos históricos como en tiempo real.
3. **Está optimizado para producción** con técnicas avanzadas de Spark (AQE, Broadcast Joins, Partition Pruning).
4. **Es seguro** con IAM Roles de privilegio mínimo, cifrado en reposo y tránsito, y sin credenciales hardcodeadas.
5. **Es automatizado** con Airflow para orquestación, GitHub Actions para CI/CD y un watchdog para el proceso de streaming.
6. **Tiene calidad asegurada** con Great Expectations en el pipeline y 13 tests unitarios que validan el código.
7. **Es escalable**: El mismo código funciona en local, EC2 o EMR sin modificaciones, gracias al script spark-submit.sh multi-entorno.

El resultado es una arquitectura moderna, robusta y lista para escalar, que demuestra competencias de Ingeniería de Datos a nivel Senior.
