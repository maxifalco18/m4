"""
infrastructure/lake_formation/setup_lakeformation.py
======================================================
Script de configuración de AWS Lake Formation para el Data Lake.

Lake Formation provee:
- Catálogo de datos centralizado (via AWS Glue Data Catalog)
- Control de acceso fino a nivel de base de datos, tabla y columna
- Auditoría de acceso a los datos
- Registro del bucket S3 como "Data Lake location"

IMPORTANTE: Este script requiere que el usuario que lo ejecuta tenga
permisos de Administrador en Lake Formation ('DataLakeAdmin').
Asignar el rol de admin manualmente en la consola de Lake Formation antes
de ejecutar por primera vez.

Uso:
    python infrastructure/lake_formation/setup_lakeformation.py
    python infrastructure/lake_formation/setup_lakeformation.py --dry-run
"""

import argparse
import logging
import os
import sys

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("lake_formation_setup")


# --------------------------------------------------------------------------- #
# Definición de bases de datos del Catálogo de Glue
# Una base de datos por capa Medallion para organización lógica.
# --------------------------------------------------------------------------- #
GLUE_DATABASES = [
    {
        "name": "raw_ecommerce",
        "description": "Capa Bronze: datos crudos del E-commerce provenientes de Airbyte (PostgreSQL/Supabase).",
        "location_uri": "s3://PI_BUCKET_NAME/raw/ecommerce/",
    },
    {
        "name": "raw_weather",
        "description": "Capa Bronze: datos crudos meteorológicos de OpenWeatherMap via Airbyte.",
        "location_uri": "s3://PI_BUCKET_NAME/raw/weather_api/",
    },
    {
        "name": "processed",
        "description": "Capa Silver: modelo dimensional limpio (esquema estrella). Dimensiones y Facts.",
        "location_uri": "s3://PI_BUCKET_NAME/processed/",
    },
    {
        "name": "gold",
        "description": "Capa Gold: KPIs y agregados analíticos listos para consumo. Unifica batch + streaming.",
        "location_uri": "s3://PI_BUCKET_NAME/gold/",
    },
    {
        "name": "streaming",
        "description": "Capas de streaming (raw-streaming, processed-streaming) de la Arquitectura Lambda.",
        "location_uri": "s3://PI_BUCKET_NAME/raw-streaming/",
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Configura AWS Lake Formation con bases de datos y permisos para el Data Lake.",
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Solo muestra qué se configuraría, sin ejecutar cambios.")
    parser.add_argument("--region", default="us-east-1", help="Región AWS.")
    return parser.parse_args()


def get_config() -> dict:
    """Obtiene la configuración desde variables de entorno."""
    bucket_name = os.getenv("S3_BUCKET_NAME")
    account_id = os.getenv("AWS_ACCOUNT_ID")
    lf_admin_role_arn = os.getenv("LAKEFORMATION_ADMIN_ROLE_ARN")

    if not all([bucket_name, account_id]):
        logger.error(
            "Variables de entorno requeridas: S3_BUCKET_NAME, AWS_ACCOUNT_ID. "
            "Copiar .env.example a .env y completar los valores."
        )
        sys.exit(1)

    return {
        "bucket_name": bucket_name,
        "account_id": account_id,
        "lf_admin_role_arn": lf_admin_role_arn,
    }


def register_data_lake_location(lf_client, bucket_name: str, role_arn: str, dry_run: bool) -> None:
    """
    Registra el bucket S3 como una ubicación de Data Lake en Lake Formation.

    Esto es necesario para que Lake Formation pueda gestionar permisos
    a nivel de paths dentro del bucket, yendo más allá de las políticas de S3.

    Sin este registro, Lake Formation no puede hacer fine-grained access control
    sobre los datos en S3.
    """
    location_arn = f"arn:aws:s3:::{bucket_name}"

    if dry_run:
        logger.info(f"[DRY-RUN] Registraría ubicación del Data Lake: {location_arn}")
        return

    try:
        lf_client.register_resource(
            ResourceArn=location_arn,
            UseServiceLinkedRole=False,
            RoleArn=role_arn,
        )
        logger.info(f"✅ Bucket registrado como Data Lake location: {location_arn}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "AlreadyExistsException":
            logger.warning(f"⚠️  El bucket ya estaba registrado como Data Lake location.")
        else:
            raise


def create_glue_databases(glue_client, bucket_name: str, dry_run: bool) -> None:
    """
    Crea las bases de datos en el AWS Glue Data Catalog.

    Cada base de datos corresponde a una capa de la arquitectura Medallion.
    Los crawlers de Glue (o las escrituras de Spark con metastore) poblarán
    las tablas dentro de estas bases de datos automáticamente.

    Estrategia de naming:
    - Una base de datos por capa facilita el control de acceso granular en Lake Formation.
    - Analistas de negocio pueden tener acceso solo a 'gold'.
    - Ingenieros de datos tienen acceso a todas las capas.
    """
    logger.info("\n--- Creando bases de datos en Glue Data Catalog ---")
    for db_config in GLUE_DATABASES:
        db_name = db_config["name"]
        location = db_config["location_uri"].replace("PI_BUCKET_NAME", bucket_name)

        if dry_run:
            logger.info(f"  [DRY-RUN] Crearía DB: '{db_name}' → {location}")
            continue

        try:
            glue_client.create_database(
                DatabaseInput={
                    "Name": db_name,
                    "Description": db_config["description"],
                    "LocationUri": location,
                    "Parameters": {
                        "classification": "parquet",
                        "layer": db_name.split("_")[0] if "_" in db_name else db_name,
                        "project": "pi-m4-data-engineering",
                        "managed_by": "lake_formation_setup.py",
                    },
                }
            )
            logger.info(f"  ✅ Base de datos '{db_name}' creada.")
        except ClientError as e:
            if e.response["Error"]["Code"] == "AlreadyExistsException":
                logger.warning(f"  ⚠️  La base de datos '{db_name}' ya existe. Omitiendo.")
            else:
                raise


def grant_lake_formation_permissions(lf_client, account_id: str, bucket_name: str, dry_run: bool) -> None:
    """
    Configura permisos Lake Formation siguiendo el principio de mínimo privilegio.

    Esquema de permisos:
    ┌──────────────────────┬────────────────────────────────────────────────┐
    │ Rol IAM              │ Acceso en Lake Formation                       │
    ├──────────────────────┼────────────────────────────────────────────────┤
    │ SparkProcessingRole  │ ALL en raw_* / DESCRIBE+SELECT en todas        │
    │                      │ ALL en processed, gold, streaming              │
    ├──────────────────────┼────────────────────────────────────────────────┤
    │ AirflowEC2Role       │ DESCRIBE+SELECT en todas                       │
    │                      │ ALL en gold                                    │
    ├──────────────────────┼────────────────────────────────────────────────┤
    │ AirbyteS3Role        │ Solo acceso S3 gestionado por bucket policy    │
    │                      │ (no requiere permisos Lake Formation)          │
    └──────────────────────┴────────────────────────────────────────────────┘
    """
    permissions_config = [
        # Spark puede leer todas las capas y escribir en processed + gold
        {
            "Principal": {"DataLakePrincipalIdentifier": f"arn:aws:iam::{account_id}:role/SparkProcessingRole"},
            "Resource": {"Database": {"Name": "raw_ecommerce"}},
            "Permissions": ["ALL"],
            "PermissionsWithGrantOption": [],
        },
        {
            "Principal": {"DataLakePrincipalIdentifier": f"arn:aws:iam::{account_id}:role/SparkProcessingRole"},
            "Resource": {"Database": {"Name": "raw_weather"}},
            "Permissions": ["ALL"],
            "PermissionsWithGrantOption": [],
        },
        {
            "Principal": {"DataLakePrincipalIdentifier": f"arn:aws:iam::{account_id}:role/SparkProcessingRole"},
            "Resource": {"Database": {"Name": "processed"}},
            "Permissions": ["ALL"],
            "PermissionsWithGrantOption": [],
        },
        {
            "Principal": {"DataLakePrincipalIdentifier": f"arn:aws:iam::{account_id}:role/SparkProcessingRole"},
            "Resource": {"Database": {"Name": "gold"}},
            "Permissions": ["ALL"],
            "PermissionsWithGrantOption": [],
        },
        # Airflow puede leer todo y escribir solo en gold (para mover ficheros finales)
        {
            "Principal": {"DataLakePrincipalIdentifier": f"arn:aws:iam::{account_id}:role/AirflowEC2Role"},
            "Resource": {"Database": {"Name": "gold"}},
            "Permissions": ["ALL"],
            "PermissionsWithGrantOption": [],
        },
    ]

    logger.info("\n--- Configurando permisos en Lake Formation ---")
    for perm in permissions_config:
        role_name = perm["Principal"]["DataLakePrincipalIdentifier"].split("/")[-1]
        db_name = perm["Resource"]["Database"]["Name"]
        perms = perm["Permissions"]

        if dry_run:
            logger.info(f"  [DRY-RUN] {role_name} → {perms} en DB '{db_name}'")
            continue

        try:
            lf_client.grant_permissions(**perm)
            logger.info(f"  ✅ {role_name} → {perms} en DB '{db_name}'")
        except ClientError as e:
            if e.response["Error"]["Code"] == "AlreadyExistsException":
                logger.warning(f"  ⚠️  Permiso ya existente para {role_name} en '{db_name}'.")
            else:
                logger.error(f"  ❌ Error al otorgar permiso: {e}")


def main() -> None:
    args = parse_args()
    config = get_config()
    region = args.region
    dry_run = args.dry_run
    bucket_name = config["bucket_name"]

    if dry_run:
        logger.info("🔍 MODO DRY-RUN: no se realizarán cambios en AWS.")

    session = boto3.Session(region_name=region)

    lf_client = session.client("lakeformation")
    glue_client = session.client("glue")

    logger.info(f"\n{'='*60}")
    logger.info(f"CONFIGURACIÓN DE AWS LAKE FORMATION")
    logger.info(f"Bucket: s3://{bucket_name}")
    logger.info(f"Región: {region}")
    logger.info(f"{'='*60}")

    try:
        register_data_lake_location(
            lf_client,
            bucket_name,
            config.get("lf_admin_role_arn", ""),
            dry_run,
        )
        create_glue_databases(glue_client, bucket_name, dry_run)
        grant_lake_formation_permissions(lf_client, config["account_id"], bucket_name, dry_run)

        logger.info("\n🎉 Lake Formation configurado correctamente.")
        logger.info("   Siguiente paso: Configurar conectores en Airbyte Cloud.")

    except ClientError as e:
        logger.error(f"❌ Error de AWS: {e.response['Error']['Message']}")
        logger.error("   Verificá que el IAM Role tenga permisos de DataLakeAdmin en Lake Formation.")
        sys.exit(1)


if __name__ == "__main__":
    main()
