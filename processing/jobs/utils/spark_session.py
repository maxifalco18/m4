"""
processing/jobs/utils/spark_session.py
========================================
Factory de SparkSession centralizada y reutilizable.

Soporta tres modos de ejecución sin cambiar el código de los jobs:
  - local:    desarrollo en laptop (spark.master=local[*])
  - ec2:      spark-submit en instancia EC2 única
  - emr:      cluster EMR (spark.master configurado externamente)

Configuraciones de optimización incluidas:
  - Adaptive Query Execution (AQE): Spark 3.x ajusta el plan dinámicamente
  - Broadcast threshold: tablas < 50 MB se broadcastean automáticamente
  - Snappy compression: formato columnar eficiente para S3
  - Hive metastore: permite interactuar con el Glue Data Catalog

Uso:
    from processing.jobs.utils.spark_session import get_spark_session
    spark = get_spark_session("my_job_name")
"""

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)


def get_spark_session(app_name: str, mode: Optional[str] = None):
    """
    Crea y retorna una SparkSession configurada para el entorno de ejecución.

    Args:
        app_name: Nombre de la aplicación Spark (aparece en la UI y en los logs).
        mode: 'local', 'ec2', 'emr'. Si es None, se lee de SPARK_MODE env var.
              Default final: 'local'.

    Returns:
        SparkSession configurada y lista para usar.
    """
    from pyspark.sql import SparkSession

    # Determinar el modo de ejecución
    exec_mode = mode or os.getenv("SPARK_MODE", "local")
    aws_region = os.getenv("AWS_REGION", "us-east-1")
    bucket_name = os.getenv("S3_BUCKET_NAME", "")

    logger.info(f"Iniciando SparkSession: app='{app_name}', mode='{exec_mode}'")

    builder = (
        SparkSession.builder
        .appName(app_name)

        # ── Adaptive Query Execution (AQE) ────────────────────────────
        # AQE permite a Spark re-optimizar el plan de ejecución en runtime:
        # - Coalesce automático de particiones pequeñas post-shuffle
        # - Conversión de Sort-Merge Join a Broadcast Join cuando detecta
        #   que una tabla es pequeña
        # - Skew join handling: divide particiones grandes automáticamente
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")

        # ── Broadcast Join Threshold ──────────────────────────────────
        # Tablas < 50 MB se envían completas a todos los workers.
        # Evita el shuffle más costoso en joins con dimensiones pequeñas.
        # Cubre: product_category_translation (~70 filas), sellers (~3K)
        .config("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))  # 50 MB

        # ── Formato de Output: Parquet con Snappy y Dynamic Overwrites ─
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.parquet.mergeSchema", "false")  # Performance: no merge schemas en lectura
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") # CRITICO: Solo sobreescribe las particiones modificadas, no toda la tabla

        # ── Optimizaciones de Shuffle ─────────────────────────────────
        # 200 particiones es el default de Spark — apropiado para desarrollo.
        # En EMR con datasets grandes, ajustar a num_cores * num_executors * 3.
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "200"))

        # ── Hive Metastore / Glue Data Catalog ───────────────────────
        # Con enableHiveSupport() + las configs de Glue, las tablas escritas
        # con .saveAsTable() quedan registradas automáticamente en el catálogo.
        .config("spark.sql.catalogImplementation", "hive")
        .config(
            "spark.hadoop.hive.metastore.client.factory.class",
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        )

        # ── UTC como timezone por defecto ─────────────────────────────
        # Previene bugs de timezone en columnas TIMESTAMP al leer desde S3.
        .config("spark.sql.session.timeZone", "UTC")
    )

    # ── Configuración por entorno ──────────────────────────────────────
    if exec_mode == "local":
        builder = (
            builder
            .master("local[*]")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.shuffle.partitions", "8")  # Menos particiones en local
            .config("spark.ui.enabled", "false")          # Desactivar UI en local
        )
        # En local, simular acceso a S3 via AWS CLI profile
        builder = _configure_s3_local(builder, aws_region)

    elif exec_mode == "ec2":
        # EC2 single node: spark-submit con todos los recursos de la instancia
        builder = (
            builder
            .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "3g"))
            .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "3g"))
        )
        builder = _configure_s3_ec2(builder, aws_region)

    elif exec_mode == "emr":
        # EMR gestiona su propia configuración de recursos.
        # Solo pasamos las configs específicas de negocio.
        builder = _configure_s3_emr(builder, aws_region)

    try:
        spark = builder.enableHiveSupport().getOrCreate()
    except Exception:
        # Fallback si Hive no está disponible (en local sin metastore)
        logger.warning("⚠️  Hive no disponible — usando SparkSession sin metastore.")
        spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"✅ SparkSession iniciada. Spark {spark.version}, modo '{exec_mode}'.")
    return spark


def _configure_s3_local(builder, region: str):
    """S3 access desde local (usa el IAM Role de la CLI configurada vía `aws configure`)."""
    return (
        builder
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{region}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        # Fast upload: usa multipart para archivos grandes
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
        .config("spark.hadoop.fs.s3a.multipart.size", "104857600")  # 100 MB partes
    )


def _configure_s3_ec2(builder, region: str):
    """S3 access desde EC2 usando IAM Instance Profile (sin credenciales hardcodeadas)."""
    return (
        builder
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{region}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
    )


def _configure_s3_emr(builder, region: str):
    """En EMR, las credenciales las maneja el cluster automáticamente."""
    return (
        builder
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider")
    )
