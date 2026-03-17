"""
infrastructure/s3/create_bucket.py
====================================
Script de inicialización del Data Lake en AWS S3.

Crea el bucket principal y define la estructura de carpetas Medallion
(raw, processed, gold) más las capas de streaming para la Arquitectura Lambda.

SEGURIDAD: Este script utiliza el IAM Role del entorno de ejecución.
           No se requieren ni se aceptan AWS_ACCESS_KEY en texto plano.
           Configurar la sesión con: `aws configure sso` o IAM Instance Profile.

Uso:
    python infrastructure/s3/create_bucket.py
    python infrastructure/s3/create_bucket.py --dry-run
    python infrastructure/s3/create_bucket.py --bucket-name mi-bucket --region us-east-1
"""

import argparse
import json
import logging
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

# --------------------------------------------------------------------------- #
# Configuración de logging
# --------------------------------------------------------------------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("s3_setup")

# --------------------------------------------------------------------------- #
# Definición de la estructura Medallion + Lambda en S3
# Cada prefijo representa una "carpeta virtual" (S3 usa prefijos, no directorios).
# Los comentarios explican el propósito de cada capa para documentación.
# --------------------------------------------------------------------------- #
DATALAKE_STRUCTURE = {
    # ── CAPA RAW (Bronze) ──────────────────────────────────────────────────
    # Datos crudos e inmutables tal como llegan de las fuentes.
    # Inmutabilidad garantizada: nunca se modifican, solo se agregan nuevos archivos.
    # Soporta schema evolution: cada sync de Airbyte escribe en subcarpetas con fecha,
    # permitiendo comparar esquemas entre ejecuciones sin romper las anteriores.
    "raw/weather_api/": "# Datos de OpenWeatherMap (API REST). Particionado: YYYY/MM/DD/",
    "raw/ecommerce/orders/": "# Tabla orders del E-commerce (PostgreSQL/Supabase)",
    "raw/ecommerce/customers/": "# Tabla customers del E-commerce",
    "raw/ecommerce/products/": "# Tabla products del E-commerce",
    "raw/ecommerce/order_items/": "# Tabla order_items del E-commerce",
    "raw/ecommerce/sellers/": "# Tabla sellers del E-commerce",
    "raw/ecommerce/geolocation/": "# Tabla geolocation del E-commerce",
    "raw/ecommerce/reviews/": "# Tabla reviews del E-commerce",
    "raw/ecommerce/payments/": "# Tabla order_payments del E-commerce",
    "raw/ecommerce/categories/": "# Tabla product_category_name_translation",

    # ── CAPA RAW-STREAMING ─────────────────────────────────────────────────
    # Eventos de Kafka escritos por Spark Structured Streaming.
    # Particionado por hora para micro-batches eficientes.
    "raw-streaming/weather_events/": "# Eventos de clima en tiempo real desde Kafka. Part: YYYY/MM/DD/HH/",

    # ── CAPA PROCESSED (Silver) ────────────────────────────────────────────
    # Datos limpiados, tipados, normalizados y modelados (esquema estrella).
    # Formato: Parquet con compresión Snappy.
    # Schema evolution: se usa Delta Lake en producción para manejar cambios de esquema.
    "processed/dim_customers/": "# Dimensión clientes (SCD Tipo 1)",
    "processed/dim_products/": "# Dimensión productos con categoría",
    "processed/dim_sellers/": "# Dimensión vendedores con geolocalización",
    "processed/dim_geolocation/": "# Dimensión geográfica (Estado, Ciudad, CEP)",
    "processed/dim_date/": "# Dimensión fecha (calendario completo)",
    "processed/fact_orders/": "# Tabla de hechos: órdenes de compra",
    "processed/fact_order_items/": "# Tabla de hechos: ítems por orden (granularidad fina)",
    "processed/fact_payments/": "# Tabla de hechos: pagos (método, cuotas, valor)",
    "processed/fact_reviews/": "# Tabla de hechos: reseñas de clientes",
    "processed/weather_enriched/": "# Datos meteorológicos enriquecidos y aplanados",

    # ── CAPA PROCESSED-STREAMING ───────────────────────────────────────────
    # Datos de streaming enriquecidos con información de la capa processed (batch).
    # Join stream-batch: weather en tiempo real + contexto de órdenes históricas.
    "processed-streaming/weather_enriched_rt/": "# Eventos de clima enriquecidos con contexto de ventas",

    # ── CAPA GOLD (Gold) ───────────────────────────────────────────────────
    # KPIs y agregaciones listas para consumo por analistas y científicos de datos.
    # Optimizadas para lectura: particionamiento por dimensión analítica.
    # Esta capa unifica datos batch y streaming (Arquitectura Lambda).
    "gold/kpi_top_products_by_category/": "# Productos más vendidos por categoría y período",
    "gold/kpi_customer_rfm/": "# Análisis RFM (Recency, Frequency, Monetary) de clientes",
    "gold/kpi_revenue_by_region/": "# Ingresos por estado/ciudad (estacionalidad)",
    "gold/kpi_new_vs_returning_customers/": "# Proporción clientes nuevos vs. recurrentes",
    "gold/kpi_price_volume_correlation/": "# Relación precio promedio vs. volumen de ventas",
    "gold/kpi_weather_sales_impact/": "# Impacto del clima en el volumen de ventas",
    "gold/kpi_payment_methods/": "# Desempeño por método y canal de pago",
    "gold/unified_analytics/": "# Vista unificada Batch + Streaming (Arquitectura Lambda final)",

    # ── ZONA DE LOGS / AUDITORÍA ───────────────────────────────────────────
    # Registros de ejecución de DAGs, jobs Spark y pipelines de calidad.
    # Separados de los datos para facilitar rotación y archivado.
    "_logs/airflow/": "# Logs de ejecución de Airflow DAGs",
    "_logs/spark/": "# Logs de Spark submit jobs",
    "_logs/great_expectations/": "# Reportes de validación de calidad de datos",
    "_checkpoints/spark-streaming/": "# Checkpoints de Spark Structured Streaming (fault-tolerance)",
}

# Configuración de versionado y lifecycle para el bucket
BUCKET_VERSIONING_CONFIG = {
    "Status": "Enabled"  # Protección contra borrado accidental
}

BUCKET_LIFECYCLE_CONFIG = {
    "Rules": [
        {
            "ID": "raw-retention-policy",
            "Status": "Enabled",
            "Filter": {"Prefix": "raw/"},
            "Transitions": [
                # Después de 30 días, mover a almacenamiento infrequente (más barato)
                {"Days": 30, "StorageClass": "STANDARD_IA"},
                # Después de 90 días, mover a Glacier para archivado a largo plazo
                {"Days": 90, "StorageClass": "GLACIER"},
            ],
        },
        {
            "ID": "logs-retention-policy",
            "Status": "Enabled",
            "Filter": {"Prefix": "_logs/"},
            "Expiration": {"Days": 90},  # Eliminar logs después de 90 días
        },
    ]
}


def parse_args() -> argparse.Namespace:
    """Parsea los argumentos de línea de comandos."""
    parser = argparse.ArgumentParser(
        description="Inicializa el Data Lake en AWS S3 con arquitectura Medallion.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--bucket-name",
        default=None,
        help="Nombre del bucket S3. Si no se especifica, se lee de la variable de entorno S3_BUCKET_NAME.",
    )
    parser.add_argument(
        "--region",
        default="us-east-1",
        help="Región AWS donde crear el bucket (default: us-east-1).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Muestra qué se crearía sin ejecutar ninguna acción en AWS.",
    )
    return parser.parse_args()


def get_bucket_name(args: argparse.Namespace) -> str:
    """Obtiene el nombre del bucket del argumento o variable de entorno."""
    import os
    bucket_name = args.bucket_name or os.getenv("S3_BUCKET_NAME")
    if not bucket_name:
        logger.error(
            "No se especificó el nombre del bucket. "
            "Usar --bucket-name o definir S3_BUCKET_NAME en el entorno."
        )
        sys.exit(1)
    return bucket_name


def create_bucket(s3_client, bucket_name: str, region: str) -> bool:
    """
    Crea el bucket S3 con la configuración recomendada.

    Configuración de seguridad:
    - Block all public access: habilitado (no hay datos públicos)
    - Bucket versioning: habilitado (protección contra borrado accidental)
    - Encryption: SSE-S3 por defecto (transparente, sin costo adicional)

    Returns:
        bool: True si se creó exitosamente, False si ya existía.
    """
    try:
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        logger.info(f"✅ Bucket '{bucket_name}' creado en región '{region}'.")

        # Bloquear TODO acceso público (best practice de seguridad)
        s3_client.put_public_access_block(
            Bucket=bucket_name,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True,
            },
        )
        logger.info("✅ Acceso público bloqueado correctamente.")

        # Habilitar cifrado por defecto (SSE-S3)
        s3_client.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {
                            "SSEAlgorithm": "AES256"
                        }
                    }
                ]
            },
        )
        logger.info("✅ Cifrado SSE-S3 habilitado.")
        return True

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            logger.warning(f"⚠️  El bucket '{bucket_name}' ya existe. Continuando...")
            return False
        raise


def enable_versioning(s3_client, bucket_name: str) -> None:
    """Habilita el versionado del bucket para protección contra borrado accidental."""
    s3_client.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration=BUCKET_VERSIONING_CONFIG,
    )
    logger.info("✅ Versionado habilitado.")


def apply_lifecycle_policy(s3_client, bucket_name: str) -> None:
    """
    Aplica política de lifecycle para optimizar costos.

    Estrategia de costos:
    - raw/: STANDARD → STANDARD_IA (30d) → GLACIER (90d)
      Justificación: los datos raw se consultan poco después de procesados.
    - _logs/: eliminación después de 90 días.
    """
    s3_client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name,
        LifecycleConfiguration=BUCKET_LIFECYCLE_CONFIG,
    )
    logger.info("✅ Política de lifecycle aplicada (optimización de costos).")


def create_folder_structure(s3_client, bucket_name: str, dry_run: bool) -> None:
    """
    Crea la estructura de carpetas del Data Lake.

    En S3, las "carpetas" son prefijos vacíos (objetos de 0 bytes con clave terminada en '/').
    Esto es una convención visual; S3 es un object store flat, no un filesystem jerárquico.

    El archivo README.txt dentro de cada carpeta documenta su propósito,
    lo que facilita la gobernanza y el onboarding de nuevos ingenieros.
    """
    logger.info(f"\n{'─'*60}")
    logger.info(f"Creando estructura de carpetas en: s3://{bucket_name}/")
    logger.info(f"{'─'*60}")

    for prefix, description in DATALAKE_STRUCTURE.items():
        readme_key = f"{prefix}README.txt"
        content = (
            f"Carpeta: s3://{bucket_name}/{prefix}\n"
            f"Propósito: {description.lstrip('# ')}\n"
            f"Creado por: infrastructure/s3/create_bucket.py\n"
            f"Arquitectura: Medallion Data Lake (Bronze/Silver/Gold) + Lambda\n"
        )

        if dry_run:
            logger.info(f"  [DRY-RUN] s3://{bucket_name}/{readme_key}")
        else:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=readme_key,
                Body=content.encode("utf-8"),
                ContentType="text/plain",
            )
            logger.info(f"  ✅ s3://{bucket_name}/{prefix}")

    logger.info(f"{'─'*60}")
    total = len(DATALAKE_STRUCTURE)
    logger.info(f"{'[DRY-RUN] ' if dry_run else ''}Estructura completada: {total} carpetas.")


def print_structure_summary(bucket_name: str) -> None:
    """Imprime un resumen visual de la arquitectura Medallion creada."""
    print("\n" + "=" * 70)
    print(f"  DATA LAKE — s3://{bucket_name}/")
    print("=" * 70)
    layers = {
        "🟤 RAW (Bronze)": [k for k in DATALAKE_STRUCTURE if k.startswith("raw/")],
        "🔴 RAW-STREAMING": [k for k in DATALAKE_STRUCTURE if k.startswith("raw-streaming/")],
        "⚪ PROCESSED (Silver)": [k for k in DATALAKE_STRUCTURE if k.startswith("processed/")],
        "🔵 PROCESSED-STREAMING": [k for k in DATALAKE_STRUCTURE if k.startswith("processed-streaming/")],
        "🟡 GOLD": [k for k in DATALAKE_STRUCTURE if k.startswith("gold/")],
        "📋 LOGS/CHECKPOINTS": [k for k in DATALAKE_STRUCTURE if k.startswith("_")],
    }
    for layer_name, prefixes in layers.items():
        if prefixes:
            print(f"\n  {layer_name}")
            for p in prefixes:
                print(f"    └── {p}")
    print("=" * 70 + "\n")


def main() -> None:
    """Punto de entrada principal del script."""
    args = parse_args()
    bucket_name = get_bucket_name(args)
    region = args.region
    dry_run = args.dry_run

    if dry_run:
        logger.info("🔍 MODO DRY-RUN: no se realizarán cambios en AWS.")

    print_structure_summary(bucket_name)

    if dry_run:
        create_folder_structure(None, bucket_name, dry_run=True)
        logger.info("✅ Dry-run completado. Revisá la estructura y ejecutá sin --dry-run.")
        return

    # Crear cliente S3 usando las credenciales del entorno (IAM Role / SSO)
    # IMPORTANTE: nunca pasar access_key y secret_key directamente aquí.
    session = boto3.Session(region_name=region)
    s3_client = session.client("s3")

    try:
        create_bucket(s3_client, bucket_name, region)
        enable_versioning(s3_client, bucket_name)
        apply_lifecycle_policy(s3_client, bucket_name)
        create_folder_structure(s3_client, bucket_name, dry_run=False)

        logger.info("\n🎉 Data Lake inicializado correctamente.")
        logger.info(f"   Bucket: s3://{bucket_name}")
        logger.info(f"   Región: {region}")
        logger.info("   Siguiente paso: python infrastructure/lake_formation/setup_lakeformation.py")

    except ClientError as e:
        logger.error(f"❌ Error de AWS: {e.response['Error']['Message']}")
        logger.error("   Verificá que el IAM Role tenga permisos s3:CreateBucket, s3:PutBucketPolicy, etc.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Error inesperado: {e}")
        raise


if __name__ == "__main__":
    main()
