"""
processing/jobs/utils/transformations.py
==========================================
Funciones de transformación de PySpark reutilizables entre jobs.

Cada función está documentada con:
- Propósito
- Técnica de optimización utilizada (si aplica)
- Tipo de shuffle que genera (o cómo lo evita)
"""

import logging
from typing import List, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType

logger = logging.getLogger(__name__)


# ─── Limpieza General ────────────────────────────────────────────────────────

def drop_duplicates_by_key(df: DataFrame, pk_cols: List[str]) -> DataFrame:
    """
    Elimina duplicados usando la clave primaria dada.

    Optimización: usa .dropDuplicates() (más eficiente que GROUP BY para dedup)
    porque no genera un agregado completo — Spark usa el hash del registro.
    """
    before = df.count()
    result = df.dropDuplicates(pk_cols)
    after = result.count()
    if before != after:
        logger.warning(f"⚠️  Dedup: {before - after:,} filas duplicadas eliminadas.")
    return result


def cast_timestamp_columns(df: DataFrame, cols: List[str]) -> DataFrame:
    """
    Convierte columnas de string → TimestampType (formato ISO 8601).

    Airbyte exporta los timestamps como strings ('2017-10-02T10:56:33.000Z').
    Esta función los convierte al tipo correcto para operaciones de fecha.

    No genera shuffle — es una projection.
    """
    for col in cols:
        if col in df.columns:
            df = df.withColumn(
                col,
                F.to_timestamp(F.col(col), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").cast(TimestampType())
            )
    return df


def normalize_string_columns(df: DataFrame, cols: List[str]) -> DataFrame:
    """
    Normaliza columnas de texto: trim, lowercase, reemplaza strings vacíos con null.

    No genera shuffle — es una serie de proyecciones.
    """
    for col in cols:
        if col in df.columns:
            df = df.withColumn(
                col,
                F.when(F.trim(F.col(col)) == "", None).otherwise(F.lower(F.trim(F.col(col))))
            )
    return df


def add_surrogate_key(df: DataFrame, id_col_name: str = "sk_id") -> DataFrame:
    """
    Agrega una clave surrogada numérica usando monotonically_increasing_id().

    Útil para dimensiones en el modelo estrella donde se necesita
    una clave entera secuencial.

    Nota: monotonically_increasing_id() no es consecutivo (hay huecos),
    pero sí es único. Para IDs estrictamente consecutivos usar row_number(Window)
    en su lugar, con el costo de un shuffle adicional.
    """
    return df.withColumn(id_col_name, F.monotonically_increasing_id())


# ─── Transformaciones de Fecha ────────────────────────────────────────────────

def build_date_dimension(start_date: str = "2016-01-01",
                         end_date: str = "2018-12-31") -> DataFrame:
    """
    Genera la dimensión Date completa con atributos útiles para análisis.

    Se ejecuta UNA SOLA VEZ y se escribe en processed/dim_date/.
    Esta dimensión se usa para todos los joins de fecha en los jobs de Gold.

    Optimización: se genera como un DataFrame pequeño (~1000 filas para 3 años)
    y se broadcastea automáticamente por Spark en los joins posteriores.
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()

    return spark.sql(f"""
        SELECT
            date_format(date_seq, 'yyyyMMdd')       AS date_key,
            date_seq                                 AS full_date,
            year(date_seq)                           AS year,
            quarter(date_seq)                        AS quarter,
            month(date_seq)                          AS month,
            weekofyear(date_seq)                     AS week_of_year,
            dayofmonth(date_seq)                     AS day_of_month,
            dayofweek(date_seq)                      AS day_of_week,
            date_format(date_seq, 'EEEE')            AS day_name,
            date_format(date_seq, 'MMMM')            AS month_name,
            CASE WHEN dayofweek(date_seq) IN (1, 7)
                 THEN true ELSE false END             AS is_weekend,
            CASE WHEN month(date_seq) IN (12, 1, 2)
                 THEN 'Summer'   -- Hemisferio sur: Diciembre-Febrero = verano
                 WHEN month(date_seq) IN (3, 4, 5)  THEN 'Autumn'
                 WHEN month(date_seq) IN (6, 7, 8)  THEN 'Winter'
                 ELSE 'Spring' END                    AS season_southern
        FROM (
            SELECT explode(sequence(
                to_date('{start_date}'),
                to_date('{end_date}'),
                interval 1 day
            )) AS date_seq
        )
    """)


# ─── Joins Optimizados ───────────────────────────────────────────────────────

def broadcast_join(large_df: DataFrame, small_df: DataFrame,
                   join_col: str, join_type: str = "left") -> DataFrame:
    """
    Ejecuta un join con broadcast hint explícito en la tabla pequeña.

    Cuándo usar broadcast join:
    - Tabla pequeña < 50 MB (umbral configurable en SparkSession)
    - Dimensiones: sellers (~3K filas), categ_translation (~71 filas)

    Por qué evita shuffle:
    - La tabla pequeña se serializa y envía a TODOS los nodos.
    - Cada nodo puede hacer el join localmente sin mover datos de la tabla grande.
    - Evita el Sort-Merge Join (que requiere ordenar + shuffle ambos lados).

    Diferencia vs AQE auto-broadcast:
    - AQE solo puede hacer auto-broadcast si ya conoce el tamaño en runtime.
    - F.broadcast() fuerza el broadcast incluso si el planner no está seguro.
    """
    return large_df.join(F.broadcast(small_df), on=join_col, how=join_type)


def repartition_for_write(df: DataFrame, partition_cols: List[str],
                          target_file_size_mb: int = 128) -> DataFrame:
    """
    Reparticiona el DataFrame antes de escribir a S3 para crear archivos
    de tamaño óptimo (128 MB ≈ bloque HDFS estándar).

    Cuándo repartir vs coalesce:
    - repartition(): crea SHUFFLE — usar cuando queremos redistribuir datos
      de forma balanceada entre particiones (por ej., por columna de fecha).
    - coalesce(): NO genera shuffle — solo combina particiones existentes.
      Usar al final para reducir el número de archivos pequeños.

    Estrategia:
    - Para tablas de hechos grandes: repartition(by date col) para que los
      lectores puedan hacer partition pruning en Athena/Spark.
    - Para dimensiones pequeñas: coalesce(1) para tener un solo archivo.
    """
    logger.info(f"Reparticionando por: {partition_cols}")
    return df.repartition(*[F.col(c) for c in partition_cols])


# ─── Transformaciones Específicas de Olist ──────────────────────────────────

def parse_weather_json(df: DataFrame) -> DataFrame:
    """
    Aplana y transforma el JSON de OpenWeatherMap desde la capa raw.

    El JSON de Airbyte/OWM tiene una estructura semi-anidada:
    {
      "dt": 1234567890,
      "dt_iso": "2017-10-02 01:00:00 +0000 UTC",
      "temp": 8.16,
      "feels_like": 4.3,
      "wind": {"speed": 3.09, "deg": 120},
      "rain": {"1h": 0.5},
      "weather": [{"id": 800, "main": "Clear", "description": "clear sky"}]
    }

    Optimización: getItem() y getField() en structs son O(1) — no genera shuffle.
    """
    return df.select(
        F.col("dt").cast(IntegerType()).alias("unix_timestamp"),
        F.to_timestamp(F.col("dt_iso"), "yyyy-MM-dd HH:mm:ss Z").alias("datetime_utc"),
        F.col("city_name"),
        F.col("lat").cast(DoubleType()),
        F.col("lon").cast(DoubleType()),
        F.col("temp").cast(DoubleType()).alias("temperature_celsius"),
        F.col("feels_like").cast(DoubleType()).alias("feels_like_celsius"),
        F.col("temp_min").cast(DoubleType()).alias("temp_min_celsius"),
        F.col("temp_max").cast(DoubleType()).alias("temp_max_celsius"),
        F.col("pressure").cast(DoubleType()).alias("pressure_hpa"),
        F.col("humidity").cast(IntegerType()).alias("humidity_pct"),
        # Extrae velocidad del viento (puede ser null si no hay datos)
        F.when(F.col("wind_speed").isNotNull(), F.col("wind_speed").cast(DoubleType()))
         .otherwise(None).alias("wind_speed_ms"),
        # Extrae lluvia de la última hora (campo opcional)
        F.coalesce(F.col("rain_1h").cast(DoubleType()), F.lit(0.0)).alias("rain_1h_mm"),
        # Descripción del clima
        F.col("weather_main").alias("weather_category"),
        F.col("weather_description").alias("weather_description"),
        # Columnas de partición
        F.year(F.to_timestamp(F.col("dt_iso"), "yyyy-MM-dd HH:mm:ss Z")).alias("year"),
        F.month(F.to_timestamp(F.col("dt_iso"), "yyyy-MM-dd HH:mm:ss Z")).alias("month"),
        F.dayofmonth(F.to_timestamp(F.col("dt_iso"), "yyyy-MM-dd HH:mm:ss Z")).alias("day"),
    )


def compute_rfm_scores(fact_orders: DataFrame, fact_payments: DataFrame,
                       reference_date: str = "2018-10-17") -> DataFrame:
    """
    Calcula el análisis RFM (Recency, Frequency, Monetary) por cliente.

    RFM es una técnica de segmentación de clientes ampliamente usada en retail:
    - Recency:   ¿Cuándo fue la última compra? (días desde la referencia)
    - Frequency: ¿Cuántas compras hicieron?
    - Monetary:  ¿Cuánto gastaron en total?

    Optimización:
    - fact_orders y fact_payments se leen desde caché (son usados por otros KPIs).
    - El join payments→orders se hace por order_id (shuffle inevitable pero solo 1 shuffle).
    - El Window para el rank no genera shuffle adicional al del groupBy previo.

    Shuffle generado: 1 (groupBy customer_id) — el mínimo posible para esta agregación.
    """
    ref = F.to_date(F.lit(reference_date))

    # Valor monetario por orden
    order_value = fact_payments.groupBy("order_id").agg(
        F.sum("payment_value").alias("order_total")
    )

    # RFM base
    rfm_base = (
        fact_orders
        .filter(F.col("order_status") == "delivered")  # Solo órdenes entregadas
        .join(order_value, on="order_id", how="left")
        .groupBy("customer_id")
        .agg(
            F.datediff(ref, F.max("order_purchase_timestamp")).alias("recency_days"),
            F.count("order_id").alias("frequency"),
            F.round(F.sum(F.coalesce(F.col("order_total"), F.lit(0))), 2).alias("monetary_brl"),
        )
    )

    # Quintil scoring (1-5) usando ntile sobre Window
    # ntile() genera UN solo shuffle (necesario para calcular percentiles).
    w_recency = Window.orderBy(F.col("recency_days").asc())   # Menos días = mejor
    w_freq = Window.orderBy(F.col("frequency").desc())         # Más frecuencia = mejor
    w_monetary = Window.orderBy(F.col("monetary_brl").desc())  # Más gasto = mejor

    return (
        rfm_base
        .withColumn("r_score", F.ntile(5).over(w_recency))
        .withColumn("f_score", F.ntile(5).over(w_freq))
        .withColumn("m_score", F.ntile(5).over(w_monetary))
        .withColumn("rfm_score", F.col("r_score") + F.col("f_score") + F.col("m_score"))
        .withColumn(
            "customer_segment",
            F.when(F.col("rfm_score") >= 13, "Champions")
             .when(F.col("rfm_score") >= 10, "Loyal Customers")
             .when(F.col("rfm_score") >= 7, "At Risk")
             .when(F.col("rfm_score") >= 4, "Needs Attention")
             .otherwise("Lost")
        )
    )
