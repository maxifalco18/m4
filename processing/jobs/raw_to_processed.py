"""
processing/jobs/raw_to_processed.py
======================================
Job Spark: Capa Raw (Bronze) → Capa Processed (Silver)

Lee los datos crudos de Airbyte en S3 (Parquet) y produce el modelo
dimensional (esquema estrella) en la capa processed/.

Modelo resultante:
  Dimensiones: dim_customers, dim_products, dim_sellers,
               dim_geolocation, dim_date
  Hechos:      fact_orders, fact_order_items, fact_payments, fact_reviews
  Enriquecido: weather_enriched (JSON de OWM aplanado)

Estrategia de optimización:
  - Tablas pequeñas (sellers, category_translation) → broadcast join
  - Tablas grandes (orders, items) → partition pruning por fecha
  - Columnas de repartición alineadas con los patrones de consulta en Gold
  - .cache() en DataFrames usados en múltiples joins en el mismo job
  - AQE habilitado (en SparkSession factory): maneja skew automáticamente

Uso:
    python processing/jobs/raw_to_processed.py
    spark-submit --py-files processing/jobs/utils.zip processing/jobs/raw_to_processed.py
"""

import logging
import os
import sys
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

# Importar utilidades del proyecto
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
from jobs.utils.spark_session import get_spark_session
from jobs.utils.transformations import (
    broadcast_join,
    build_date_dimension,
    cast_timestamp_columns,
    drop_duplicates_by_key,
    normalize_string_columns,
    parse_weather_json,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger("raw_to_processed")

# ─── Configuración ────────────────────────────────────────────────────────────
BUCKET = os.getenv("S3_BUCKET_NAME", "pi-m4-datalake")
RAW_BASE = f"s3a://{BUCKET}/raw"
PROCESSED_BASE = f"s3a://{BUCKET}/processed"

TIMESTAMP_COLS_ORDERS = [
    "order_purchase_timestamp", "order_approved_at",
    "order_delivered_carrier_date", "order_delivered_customer_date",
    "order_estimated_delivery_date",
]


# ─── Funciones de lectura ─────────────────────────────────────────────────────

def read_raw_table(spark, source: str, table: str) -> DataFrame:
    """
    Lee una tabla de la capa raw con path auto-resuelto.

    Usa el wildcard de Spark para fusionar las particiones de fecha
    de múltiples syncs de Airbyte en un solo DataFrame.

    Partition pruning: si se pasa un filtro de fecha ANTES del read,
    Spark solo lee las subcarpetas de fecha necesarias — mejora el
    performance en lecturas incrementales del DAG de Airflow.
    """
    path = f"{RAW_BASE}/{source}/{table}/"
    logger.info(f"  ← Leyendo: {path}")
    return spark.read.parquet(path)


# ─── Dimensiones ────────────────────────────────────────────────────────────--

def build_dim_customers(raw_customers: DataFrame) -> DataFrame:
    """
    dim_customers: Dimensión de clientes (SCD Tipo 1 — solo última versión).

    No hay historial de cambios en Olist — si un cliente cambia su zip_code,
    solo nos interesa el valor actual. SCD Tipo 2 se implementaría con
    Delta Lake (Avance 5 - optional).
    """
    logger.info("Construyendo dim_customers...")
    df = (
        raw_customers
        .select(
            F.col("customer_id"),
            F.col("customer_unique_id"),
            F.col("customer_zip_code_prefix").cast(StringType()),
            F.col("customer_city"),
            F.col("customer_state"),
        )
        .filter(F.col("customer_id").isNotNull())
    )
    df = drop_duplicates_by_key(df, ["customer_id"])
    df = normalize_string_columns(df, ["customer_city", "customer_state"])
    return df


def build_dim_products(raw_products: DataFrame, raw_category_translation: DataFrame) -> DataFrame:
    """
    dim_products: Dimensión de productos con categoría en inglés.

    Usa broadcast join para unir con la tabla de traducción (~71 filas).
    Broadcast elimina el shuffle — la tabla de traducción se envía a todos los nodos.
    """
    logger.info("Construyendo dim_products...")
    df = (
        raw_products
        .select(
            F.col("product_id"),
            F.col("product_category_name"),
            F.col("product_name_length").cast(IntegerType()),
            F.col("product_description_length").cast(IntegerType()),
            F.col("product_photos_qty").cast(IntegerType()),
            F.col("product_weight_g").cast(IntegerType()),
            F.col("product_length_cm").cast(IntegerType()),
            F.col("product_height_cm").cast(IntegerType()),
            F.col("product_width_cm").cast(IntegerType()),
        )
        .filter(F.col("product_id").isNotNull())
    )

    category_en = raw_category_translation.select(
        "product_category_name",
        F.col("product_category_name_english").alias("product_category_name_en"),
    )

    # Broadcast join: category_translation es pequeño (~71 filas, < 10 KB)
    df = broadcast_join(df, category_en, "product_category_name", "left")
    return drop_duplicates_by_key(df, ["product_id"])


def build_dim_sellers(raw_sellers: DataFrame) -> DataFrame:
    """dim_sellers: Dimensión de vendedores."""
    logger.info("Construyendo dim_sellers...")
    df = (
        raw_sellers
        .select("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state")
        .filter(F.col("seller_id").isNotNull())
    )
    return drop_duplicates_by_key(df, ["seller_id"])


def build_dim_geolocation(raw_geo: DataFrame) -> DataFrame:
    """
    dim_geolocation: Dimensión geográfica por zip code.

    La tabla original tiene múltiples lat/lon por zip_code_prefix.
    Tomamos el centroide (promedio) para tener una única fila por zip.

    Shuffle generado: 1 (groupBy zip_code_prefix) — inevitable para este agregado.
    """
    logger.info("Construyendo dim_geolocation (agregando centroide por zip)...")
    return (
        raw_geo
        .filter(F.col("geolocation_zip_code_prefix").isNotNull())
        .groupBy("geolocation_zip_code_prefix", "geolocation_city", "geolocation_state")
        .agg(
            F.round(F.avg("geolocation_lat"), 6).alias("lat_centroid"),
            F.round(F.avg("geolocation_lng"), 6).alias("lon_centroid"),
        )
        .withColumnRenamed("geolocation_zip_code_prefix", "zip_code_prefix")
        .withColumnRenamed("geolocation_city", "city")
        .withColumnRenamed("geolocation_state", "state")
    )


# ─── Tablas de Hechos ─────────────────────────────────────────────────────────

def build_fact_orders(raw_orders: DataFrame) -> DataFrame:
    """
    fact_orders: Tabla de hechos de órdenes.

    Columnas de partición: year, month — para partition pruning en Gold.
    """
    logger.info("Construyendo fact_orders...")
    df = (
        raw_orders
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
    )
    df = drop_duplicates_by_key(df, ["order_id"])
    df = cast_timestamp_columns(df, TIMESTAMP_COLS_ORDERS)

    # Agregar columnas de partición y métricas de tiempo de entrega
    df = (
        df
        .withColumn("year",  F.year("order_purchase_timestamp"))
        .withColumn("month", F.month("order_purchase_timestamp"))
        .withColumn(
            "delivery_days",
            F.when(
                F.col("order_delivered_customer_date").isNotNull() &
                F.col("order_purchase_timestamp").isNotNull(),
                F.datediff("order_delivered_customer_date", "order_purchase_timestamp")
            ).otherwise(None)
        )
        .withColumn(
            "estimated_vs_actual_days",
            F.when(
                F.col("order_delivered_customer_date").isNotNull() &
                F.col("order_estimated_delivery_date").isNotNull(),
                F.datediff("order_delivered_customer_date", "order_estimated_delivery_date")
            ).otherwise(None)
        )
    )
    return df


def build_fact_order_items(raw_items: DataFrame) -> DataFrame:
    """fact_order_items: granularidad más fina — un registro por producto por orden."""
    logger.info("Construyendo fact_order_items...")
    df = (
        raw_items
        .select(
            "order_id", "order_item_id", "product_id", "seller_id",
            "shipping_limit_date",
            F.col("price").cast(DoubleType()),
            F.col("freight_value").cast(DoubleType()),
        )
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("price") >= 0)
    )
    return df.withColumn(
        "total_item_value",
        F.round(F.col("price") + F.coalesce(F.col("freight_value"), F.lit(0.0)), 2)
    )


def build_fact_payments(raw_payments: DataFrame) -> DataFrame:
    """fact_payments: pagos por orden (puede haber múltiples métodos por orden)."""
    logger.info("Construyendo fact_payments...")
    return (
        raw_payments
        .select(
            "order_id", "payment_sequential",
            F.col("payment_type"),
            F.col("payment_installments").cast(IntegerType()),
            F.col("payment_value").cast(DoubleType()),
        )
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("payment_value") >= 0)
    )


def build_fact_reviews(raw_reviews: DataFrame) -> DataFrame:
    """fact_reviews: reseñas de clientes (1-5 estrellas)."""
    logger.info("Construyendo fact_reviews...")
    df = (
        raw_reviews
        .select(
            "review_id", "order_id",
            F.col("review_score").cast(IntegerType()),
            "review_comment_title", "review_comment_message",
            "review_creation_date",
        )
        .filter(F.col("review_id").isNotNull())
        .filter(F.col("review_score").between(1, 5))
    )
    return drop_duplicates_by_key(df, ["review_id", "order_id"])


def build_weather_enriched(raw_weather: DataFrame) -> DataFrame:
    """
    weather_enriched: datos meteorológicos aplanados desde el JSON de OWM.

    Combina datos estructurados (Olist) con no estructurados (JSON de clima).
    Particionado por year/month para permitir joins eficientes en gold/
    (kpi_weather_sales_impact).
    """
    logger.info("Construyendo weather_enriched...")
    df = parse_weather_json(raw_weather)
    return (
        df
        .filter(F.col("datetime_utc").isNotNull())
        .dropDuplicates(["unix_timestamp"])
    )


# ─── Escritura a S3 ──────────────────────────────────────────────────────────

def write_processed(df: DataFrame, table_name: str,
                    partition_cols: list = None, num_files: int = None) -> None:
    """
    Escribe el DataFrame en la capa processed/ de S3 en formato Parquet (Snappy).

    Estrategia de particionamiento:
    - Tablas de hechos grandes (fact_orders, fact_items): .partitionBy(year, month)
      → Permite partition pruning en los jobs de Gold y en Athena.
    - Dimensiones pequeñas (dim_customers, dim_products): coalesce(1)
      → Un solo archivo; no valen las particiones para datasets < 10 MB.
    """
    path = f"{PROCESSED_BASE}/{table_name}/"
    logger.info(f"  → Escribiendo: {path} (partición: {partition_cols})")

    writer = df.write.mode("overwrite").format("parquet")

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    elif num_files:
        df = df.coalesce(num_files)

    writer.save(path)
    logger.info(f"  ✅ Escrito: {table_name}")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    logger.info("\n" + "=" * 60)
    logger.info("JOB: raw_to_processed")
    logger.info(f"Inicio: {datetime.utcnow().isoformat()}")
    logger.info("=" * 60)

    spark = get_spark_session("pi-m4-raw-to-processed")

    # ── Lectura de Raw (Bronze) ───────────────────────────────────────────
    logger.info("\n[1/3] Leyendo datos de la capa raw...")

    # Tablas Olist
    raw_orders    = read_raw_table(spark, "ecommerce", "olist_orders")
    raw_items     = read_raw_table(spark, "ecommerce", "olist_order_items")
    raw_payments  = read_raw_table(spark, "ecommerce", "olist_order_payments")
    raw_reviews   = read_raw_table(spark, "ecommerce", "olist_order_reviews")
    raw_customers = read_raw_table(spark, "ecommerce", "olist_customers")
    raw_products  = read_raw_table(spark, "ecommerce", "olist_products")
    raw_sellers   = read_raw_table(spark, "ecommerce", "olist_sellers")
    raw_geo       = read_raw_table(spark, "ecommerce", "olist_geolocation")
    raw_category  = read_raw_table(spark, "ecommerce", "olist_product_category_name_translation")

    # Clima (API o upload manual desde Patagonia_-41.json)
    raw_weather = read_raw_table(spark, "weather_api", "weather_current")

    # ── Cache de tablas usadas múltiples veces ────────────────────────────
    # Cachear evita re-leer desde S3 (I/O costoso) en cada materialización.
    # Solo hacemos cache de DataFrames que se usen en MÁS DE UN join/acción.
    raw_customers.cache()
    raw_products.cache()

    # ── Construcción del Modelo Dimensional ──────────────────────────────
    logger.info("\n[2/3] Construyendo modelo dimensional...")

    dim_date        = build_date_dimension("2016-01-01", "2018-12-31")
    dim_customers   = build_dim_customers(raw_customers)
    dim_products    = build_dim_products(raw_products, raw_category)
    dim_sellers     = build_dim_sellers(raw_sellers)
    dim_geolocation = build_dim_geolocation(raw_geo)
    fact_orders     = build_fact_orders(raw_orders)
    fact_items      = build_fact_order_items(raw_items)
    fact_payments   = build_fact_payments(raw_payments)
    fact_reviews    = build_fact_reviews(raw_reviews)
    weather         = build_weather_enriched(raw_weather)

    # Liberar caché después de usarlo
    raw_customers.unpersist()
    raw_products.unpersist()

    # ── Escritura en Processed (Silver) ───────────────────────────────────
    logger.info("\n[3/3] Escribiendo capa processed/...")

    # Dimensiones: sin partición (datasets pequeños → 1 archivo)
    write_processed(dim_date,        "dim_date",        num_files=1)
    write_processed(dim_customers,   "dim_customers",   num_files=4)
    write_processed(dim_products,    "dim_products",    num_files=2)
    write_processed(dim_sellers,     "dim_sellers",     num_files=1)
    write_processed(dim_geolocation, "dim_geolocation", num_files=4)

    # Hechos: particionados por año/mes para partition pruning en Gold
    write_processed(fact_orders,   "fact_orders",      partition_cols=["year", "month"])
    write_processed(fact_items,    "fact_order_items", partition_cols=None, num_files=8)
    write_processed(fact_payments, "fact_payments",    num_files=8)
    write_processed(fact_reviews,  "fact_reviews",     num_files=4)

    # Clima: particionado por año/mes (joins con fact_orders en Gold)
    write_processed(weather, "weather_enriched", partition_cols=["year", "month"])

    logger.info("\n🎉 Job raw_to_processed completado.")
    spark.stop()


if __name__ == "__main__":
    main()
