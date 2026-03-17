"""
streaming/consumer/spark_streaming_consumer.py
==============================================
Consumidor de Spark Structured Streaming + Arquitectura Lambda.

Flujo del consumidor (Micro-batch cada 30 segundos):
  1. Lee eventos JSON desde Kafka (topic: weather_events).
  2. Parsea y valida el schema de los eventos en tiempo real.
  3. RAW-STREAMING: Escribe el log crudo en S3 (Append Parquet).
  4. PROCESSED-STREAMING: Filtra datos anómalos y formatea.
  5. GOLD (Arquitectura Lambda):
     - Lee el histórico BATCH desde s3://bucket/processed/weather_enriched 
     - Une (Union) los datos BATCH estáticos con los últimos del STREAMING.
     - Guarda la vista unificada sobreescribiendo s3://bucket/gold/lambda_weather_unified/

Esta unificación ilustra el corazón de la Arquitectura Lambda:
    Serving Layer = Batch View ∪ Real-time View

Uso:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
                 streaming/consumer/spark_streaming_consumer.py
"""

import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# Importar utilidades
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
from processing.jobs.utils.spark_session import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("spark_streaming")

# Configuraciones S3 / Kafka
BUCKET = os.getenv("S3_BUCKET_NAME", "pi-m4-datalake")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPIC = "weather_events"

# Rutas Checkpoints (Vital para tolerancia a fallos en Structured Streaming)
CHK_RAW = f"s3a://{BUCKET}/_checkpoints/streaming_raw"
CHK_UNIFIED = f"s3a://{BUCKET}/_checkpoints/streaming_unified"

# Rutas Destino
PATH_RAW_STREAMING = f"s3a://{BUCKET}/raw-streaming/weather_events"
PATH_BATCH_PROCESSED = f"s3a://{BUCKET}/processed/weather_enriched"
PATH_GOLD_LAMBDA = f"s3a://{BUCKET}/gold/lambda_weather_unified"

# Schema esperado del JSON de Kafka
WEATHER_SCHEMA = StructType([
    StructField("dt", IntegerType(), True),
    StructField("dt_iso", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("temp", DoubleType(), True),
    StructField("humidity", IntegerType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("weather_main", StringType(), True),
    StructField("weather_description", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True)
])


def process_micro_batch(micro_batch_df, batch_id):
    """
    ForeachBatch function para implementar Arquitectura Lambda.
    Se ejecuta una vez por cada micro-batch (ej: cada 30 segundos).
    """
    logger.info(f"⚡ Procesando Micro-Batch ID: {batch_id} - Filas: {micro_batch_df.count()}")
    
    if micro_batch_df.isEmpty():
        return

    # 1. Transformar datos del Streaming (Capa Processed)
    stream_processed = (
        micro_batch_df
        .filter(F.col("temp").isNotNull())  # Filtro de calidad de datos
        .select(
            F.col("dt").alias("unix_timestamp"),
            F.to_timestamp(F.col("dt_iso"), "yyyy-MM-dd HH:mm:ss Z").alias("datetime_utc"),
            F.col("city_name"),
            F.col("lat"),
            F.col("lon"),
            F.col("temp").alias("temperature_celsius"),
            F.col("humidity").alias("humidity_pct"),
            F.col("wind_speed").alias("wind_speed_ms"),
            F.col("weather_main").alias("weather_category"),
            F.year(F.to_timestamp(F.col("dt_iso"), "yyyy-MM-dd HH:mm:ss Z")).alias("year"),
            F.month(F.to_timestamp(F.col("dt_iso"), "yyyy-MM-dd HH:mm:ss Z")).alias("month")
        )
    )

    spark = micro_batch_df.sparkSession

    # 2. Arquitectura Lambda: Leer Vista Batch Estática
    # Fallback si el directiorio batch no existe aún
    try:
        batch_view = spark.read.parquet(PATH_BATCH_PROCESSED)
        
        # Seleccionar solo las columnas comunes para hacer el UNION
        common_cols = [c.name for c in stream_processed.schema.fields]
        batch_view = batch_view.select(*common_cols)
        
        # 3. Unificar: Batch View ∪ Real-time View
        lambda_unified = batch_view.unionByName(stream_processed).dropDuplicates(["unix_timestamp"])
        logger.info(f"  → Unified View: haciendo merge de Batch con datos en tiempo real.")
    except Exception as e:
        logger.warning(f"  ⚠️ No se encontró la capa Batch Processed. Escribiendo solo Real-time. ({e})")
        lambda_unified = stream_processed.dropDuplicates(["unix_timestamp"])

    # 4. Escribir vista unificada a Gold (Overwrite)
    # Nota: Overwrite en S3 no es transaccional sin Delta Lake. En prd usaríamos Delta.
    (lambda_unified
     .write
     .mode("overwrite")
     .partitionBy("year", "month")
     .parquet(PATH_GOLD_LAMBDA))
    
    logger.info(f"  ✅ Micro-batch {batch_id} escrito en Capa Gold Lambda: {PATH_GOLD_LAMBDA}")


def main():
    logger.info("\n" + "=" * 60)
    logger.info("⚡ INICIANDO SPARK LIBRERÍA ESTRUCTURADA - ARQUITECTURA LAMBDA")
    logger.info("=" * 60)

    # El spark-submit.sh debe pasar el package de kafka
    spark = get_spark_session("pi-m4-streaming-consumer")

    # 1. Leer stream desde Kafka
    # ==========================
    logger.info(f"🔌 Conectando a Kafka: {KAFKA_BROKER}, topic: {TOPIC}")
    
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")  # En prod usar 'earliest' en la primera corrida
        .option("failOnDataLoss", "false")
        .load()
    )

    # Kafka payload está en BINARY "value". Hay que castearlo y parsear JSON.
    parsed_stream = (
        kafka_df
        .selectExpr("CAST(value AS STRING) as json_payload")
        .select(F.from_json("json_payload", WEATHER_SCHEMA).alias("data"))
        .select("data.*")
    )

    # 2. RAW-STREAMING LAYER (Append-only)
    # ====================================
    # Se guarda todo el evento crudo como backup particionado por hora de llegada.
    logger.info(f"📝 Configurando stream hacia RAW-STREAMING...")
    
    raw_stream_query = (
        parsed_stream
        .withColumn("ingestion_year", F.year(F.current_timestamp()))
        .withColumn("ingestion_month", F.month(F.current_timestamp()))
        .withColumn("ingestion_day", F.dayofmonth(F.current_timestamp()))
        .writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", PATH_RAW_STREAMING)
        .option("checkpointLocation", CHK_RAW)
        .trigger(processingTime="30 seconds")
        .start()
    )

    # 3. LAMBDA UNIFIED LAYER via ForeachBatch
    # ========================================
    logger.info(f"🔄 Configurando stream hacia GOLD LAMBDA (ForeachBatch)...")
    
    unified_stream_query = (
        parsed_stream
        .writeStream
        .foreachBatch(process_micro_batch)
        .option("checkpointLocation", CHK_UNIFIED)
        .trigger(processingTime="30 seconds")
        .start()
    )

    logger.info("🚀 Streams en ejecución. Esperando finalización (Ctrl+C para detener).")
    
    try:
        # Esperar a que terminen (correrán para siempre hasta error o manual stop)
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("\n🛑 Deteniendo streams por acción del usuario...")
        for q in spark.streams.active:
            q.stop()
        logger.info("✅ Spark Streaming terminado.")


if __name__ == "__main__":
    main()
