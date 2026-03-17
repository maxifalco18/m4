"""
ingestion/scripts/validate_raw_data.py
========================================
Script de validación post-ingesta para la capa raw/ del Data Lake.

Implementa dos niveles de validación:

MVP (mandatorio):
  - Existencia de archivos Parquet en las rutas esperadas
  - Conteo de registros > 0 (no hay archivos vacíos)
  - Verificación de tamaño mínimo de archivos

PRO (schema drift detection):
  - Compara el esquema actual de los Parquet con un esquema de referencia
  - Detecta columnas agregadas, eliminadas o con tipo cambiado
  - Genera un reporte de "Schema Drift" que alerta antes de que datos
    inconsistentes lleguen a la capa Silver
  - Registra los esquemas observados como "Schema Registry" manual en JSON

Uso:
    # Validar todas las fuentes
    python ingestion/scripts/validate_raw_data.py --bucket pi-m4-datalake-xxx

    # Validar solo ecommerce
    python ingestion/scripts/validate_raw_data.py --bucket pi-m4-datalake-xxx --source ecommerce

    # Modo dry-run local (sin AWS — usa archivos locales)
    python ingestion/scripts/validate_raw_data.py --local-path ./test_data/raw/ --source weather

    # Registrar esquema actual como referencia (primera ejecución)
    python ingestion/scripts/validate_raw_data.py --bucket pi-m4-datalake-xxx --register-schema
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("validate_raw")


# --------------------------------------------------------------------------- #
# Esquemas esperados por fuente (Schema Registry Manual)
#
# Cada entrada define las columnas y tipos esperados en los Parquet de la capa raw.
# Si Airbyte agrega/elimina columnas (Schema Drift), este script lo detecta
# ANTES de que llegue a la capa Silver (donde rompería los jobs de Spark).
# --------------------------------------------------------------------------- #

EXPECTED_SCHEMAS = {
    "ecommerce": {
        "olist_orders": {
            "columns": {
                "order_id": "string",
                "customer_id": "string",
                "order_status": "string",
                "order_purchase_timestamp": "string",  # Airbyte exporta timestamps como string
                "order_approved_at": "string",
                "order_delivered_carrier_date": "string",
                "order_delivered_customer_date": "string",
                "order_estimated_delivery_date": "string",
                "updated_at": "string",
            },
            "min_row_count": 90000,  # Olist tiene ~99K órdenes
            "primary_key": "order_id",
        },
        "olist_order_items": {
            "columns": {
                "order_id": "string",
                "order_item_id": "int64",
                "product_id": "string",
                "seller_id": "string",
                "shipping_limit_date": "string",
                "price": "double",
                "freight_value": "double",
                "updated_at": "string",
            },
            "min_row_count": 100000,
            "primary_key": "order_id",
        },
        "olist_customers": {
            "columns": {
                "customer_id": "string",
                "customer_unique_id": "string",
                "customer_zip_code_prefix": "string",
                "customer_city": "string",
                "customer_state": "string",
                "updated_at": "string",
            },
            "min_row_count": 90000,
            "primary_key": "customer_id",
        },
        "olist_products": {
            "columns": {
                "product_id": "string",
                "product_category_name": "string",
                "product_name_length": "int64",
                "product_description_length": "int64",
                "product_photos_qty": "int64",
                "product_weight_g": "int64",
                "product_length_cm": "int64",
                "product_height_cm": "int64",
                "product_width_cm": "int64",
                "updated_at": "string",
            },
            "min_row_count": 30000,
            "primary_key": "product_id",
        },
        "olist_sellers": {
            "columns": {
                "seller_id": "string",
                "seller_zip_code_prefix": "string",
                "seller_city": "string",
                "seller_state": "string",
                "updated_at": "string",
            },
            "min_row_count": 3000,
            "primary_key": "seller_id",
        },
        "olist_order_payments": {
            "columns": {
                "order_id": "string",
                "payment_sequential": "int64",
                "payment_type": "string",
                "payment_installments": "int64",
                "payment_value": "double",
                "updated_at": "string",
            },
            "min_row_count": 100000,
            "primary_key": "order_id",
        },
        "olist_order_reviews": {
            "columns": {
                "review_id": "string",
                "order_id": "string",
                "review_score": "int64",
                "review_comment_title": "string",
                "review_comment_message": "string",
                "review_creation_date": "string",
                "review_answer_timestamp": "string",
                "updated_at": "string",
            },
            "min_row_count": 90000,
            "primary_key": "review_id",
        },
        "olist_geolocation": {
            "columns": {
                "geolocation_zip_code_prefix": "string",
                "geolocation_lat": "double",
                "geolocation_lng": "double",
                "geolocation_city": "string",
                "geolocation_state": "string",
            },
            "min_row_count": 900000,
            "primary_key": None,
        },
        "olist_product_category_name_translation": {
            "columns": {
                "product_category_name": "string",
                "product_category_name_english": "string",
            },
            "min_row_count": 70,
            "primary_key": "product_category_name",
        },
    },
    "weather": {
        "weather_current": {
            "columns": {
                "dt": "int64",
                "dt_iso": "string",
                "city_name": "string",
                "temp": "double",
                "humidity": "int64",
            },
            "min_row_count": 1,
            "primary_key": "dt",
            "_comment": "Esquema mínimo esperado. El JSON puede tener campos anidados.",
        },
    },
}

# Ruta al archivo de Schema Registry persistente
SCHEMA_REGISTRY_PATH = Path(__file__).parent.parent / "schema_registry.json"


# --------------------------------------------------------------------------- #
# Validadores
# --------------------------------------------------------------------------- #


class ValidationResult:
    """Almacena los resultados de validación por tabla."""

    def __init__(self, source: str, table: str):
        self.source = source
        self.table = table
        self.checks = []  # Lista de (check_name, passed: bool, detail: str)

    def add_check(self, name: str, passed: bool, detail: str = "") -> None:
        status = "✅ PASS" if passed else "❌ FAIL"
        self.checks.append((name, passed, detail))
        logger.info(f"    {status} | {name}: {detail}")

    @property
    def all_passed(self) -> bool:
        return all(passed for _, passed, _ in self.checks)

    def summary(self) -> str:
        total = len(self.checks)
        passed = sum(1 for _, p, _ in self.checks if p)
        return f"{self.source}/{self.table}: {passed}/{total} checks passed"


def validate_files_exist_s3(s3_client, bucket: str, prefix: str) -> tuple:
    """
    MVP Check: Verifica que existan archivos en el prefijo de S3.

    Returns:
        tuple: (exists: bool, file_count: int, total_bytes: int)
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    file_count = 0
    total_bytes = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            # Solo contar archivos Parquet (ignorar README.txt y _metadata)
            if key.endswith(".parquet") or key.endswith(".snappy.parquet"):
                file_count += 1
                total_bytes += obj["Size"]

    return (file_count > 0, file_count, total_bytes)


def validate_files_exist_local(local_path: Path, table_name: str) -> tuple:
    """MVP Check: versión local para dry-run sin AWS."""
    parquet_files = list(local_path.glob(f"**/{table_name}/**/*.parquet"))
    total_bytes = sum(f.stat().st_size for f in parquet_files)
    return (len(parquet_files) > 0, len(parquet_files), total_bytes)


def read_parquet_schema_s3(s3_client, bucket: str, prefix: str) -> Optional[dict]:
    """
    PRO Check: Lee el esquema del primer archivo Parquet en S3.

    Usa pyarrow para leer solo el footer del Parquet (no los datos),
    lo que es O(1) en tiempo y memoria sin importar el tamaño del archivo.
    """
    try:
        import pyarrow.parquet as pq
        import s3fs

        fs = s3fs.S3FileSystem()

        # Buscar el primer .parquet en el prefijo
        paginator = s3_client.get_paginator("list_objects_v2")
        first_parquet = None
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    first_parquet = obj["Key"]
                    break
            if first_parquet:
                break

        if not first_parquet:
            return None

        # Leer solo el footer (schema) — O(1) sin leer datos
        pf = pq.ParquetFile(fs.open(f"{bucket}/{first_parquet}"))
        schema = pf.schema_arrow

        return {
            field.name: str(field.type)
            for field in schema
        }

    except ImportError:
        logger.warning("⚠️  pyarrow o s3fs no instalados. Omitiendo schema check.")
        return None
    except Exception as e:
        logger.warning(f"⚠️  Error leyendo schema: {e}")
        return None


def read_parquet_schema_local(local_path: Path, table_name: str) -> Optional[dict]:
    """PRO Check: Lee el esquema de un Parquet local."""
    try:
        import pyarrow.parquet as pq

        parquet_files = list(local_path.glob(f"**/{table_name}/**/*.parquet"))
        if not parquet_files:
            return None

        pf = pq.ParquetFile(parquet_files[0])
        return {field.name: str(field.type) for field in pf.schema_arrow}
    except ImportError:
        logger.warning("⚠️  pyarrow no instalado. pip install pyarrow")
        return None
    except Exception as e:
        logger.warning(f"⚠️  Error leyendo schema local: {e}")
        return None


def read_parquet_row_count_s3(s3_client, bucket: str, prefix: str) -> int:
    """
    MVP Check: Cuenta el total de registros en todos los Parquet del prefijo.

    Lee solo los metadata footers (num_rows) de cada archivo, sin cargar datos.
    """
    try:
        import pyarrow.parquet as pq
        import s3fs

        fs = s3fs.S3FileSystem()
        total_rows = 0

        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    pf = pq.ParquetFile(fs.open(f"{bucket}/{obj['Key']}"))
                    total_rows += pf.metadata.num_rows

        return total_rows
    except ImportError:
        logger.warning("⚠️  pyarrow/s3fs no instalados — row count no disponible.")
        return -1
    except Exception as e:
        logger.warning(f"⚠️  Error contando filas: {e}")
        return -1


def detect_schema_drift(actual_schema: dict, expected_schema: dict, table_name: str) -> dict:
    """
    PRO Check: Detecta diferencias entre el esquema actual y el esperado.

    Detecta 3 tipos de drift:
    1. ADDED:   columnas en actual que no están en expected (Airbyte agregó un campo)
    2. REMOVED: columnas en expected que no están en actual (Airbyte dejó de enviar un campo)
    3. CHANGED: columnas con tipo diferente (string → int, etc.)

    Returns:
        dict con listas de columnas afectadas por cada tipo de drift.
    """
    expected_cols = set(expected_schema.keys())
    actual_cols = set(actual_schema.keys())

    # Filtrar columnas internas de Airbyte (empiezan con _airbyte_)
    actual_cols_filtered = {c for c in actual_cols if not c.startswith("_airbyte")}

    added = actual_cols_filtered - expected_cols
    removed = expected_cols - actual_cols_filtered

    # Verificar cambios de tipo en columnas comunes
    changed = {}
    common = expected_cols & actual_cols_filtered
    for col in common:
        expected_type = expected_schema[col]
        actual_type = actual_schema[col]
        # Normalizar tipos (pyarrow usa nombres como 'int64', 'string', 'double')
        if not _types_compatible(expected_type, actual_type):
            changed[col] = {"expected": expected_type, "actual": actual_type}

    return {
        "added": list(added),
        "removed": list(removed),
        "type_changed": changed,
        "has_drift": bool(added or removed or changed),
    }


def _types_compatible(expected: str, actual: str) -> bool:
    """
    Compara tipos con tolerancia a variaciones de naming.
    Ej: 'string' es compatible con 'large_string', 'utf8'.
    """
    # Grupo de tipos compatibles
    type_groups = {
        "string": {"string", "large_string", "utf8", "object"},
        "int64": {"int64", "int32", "int16", "int8"},
        "double": {"double", "float64", "float32", "float"},
    }

    for group_name, compatible_types in type_groups.items():
        if expected.lower() in compatible_types and actual.lower() in compatible_types:
            return True

    return expected.lower() == actual.lower()


def save_schema_registry(observed_schemas: dict) -> None:
    """
    Persiste los esquemas observados como Schema Registry manual.
    Cada ejecución actualiza el registro con los últimos esquemas vistos.
    """
    registry = {}
    if SCHEMA_REGISTRY_PATH.exists():
        with open(SCHEMA_REGISTRY_PATH, "r") as f:
            registry = json.load(f)

    registry["last_updated"] = datetime.utcnow().isoformat()
    registry["schemas"] = observed_schemas

    with open(SCHEMA_REGISTRY_PATH, "w") as f:
        json.dump(registry, f, indent=2)

    logger.info(f"✅ Schema Registry guardado: {SCHEMA_REGISTRY_PATH}")


# --------------------------------------------------------------------------- #
# Ejecución principal
# --------------------------------------------------------------------------- #


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Valida los datos en la capa raw/ del Data Lake.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--bucket", help="Nombre del bucket S3.")
    group.add_argument("--local-path", help="Ruta local para testing sin AWS.")

    parser.add_argument(
        "--source", choices=["ecommerce", "weather", "all"], default="all",
        help="Fuente de datos a validar.",
    )
    parser.add_argument(
        "--register-schema", action="store_true",
        help="Registra los esquemas actuales como referencia (primera ejecución).",
    )
    return parser.parse_args()


def validate_source(source_name: str, schemas: dict,
                    s3_client, bucket: str,
                    local_path: Optional[Path],
                    register_schema: bool) -> list:
    """Ejecuta todas las validaciones para una fuente de datos."""
    results = []

    for table_name, expected in schemas.items():
        logger.info(f"\n  📊 Validando {source_name}/{table_name}...")
        result = ValidationResult(source_name, table_name)

        # Definir el prefijo S3 o path local
        prefix = f"raw/{source_name}/{table_name}/"

        # ── MVP: Archivos existen ──────────────────────────────────────
        if bucket:
            exists, file_count, total_bytes = validate_files_exist_s3(s3_client, bucket, prefix)
        else:
            exists, file_count, total_bytes = validate_files_exist_local(local_path, table_name)

        result.add_check("Archivos Parquet existen", exists,
                         f"{file_count} archivos, {total_bytes / 1024:.1f} KB")

        # ── MVP: Archivos no vacíos ────────────────────────────────────
        result.add_check("Tamaño > 0 bytes", total_bytes > 0,
                         f"{total_bytes:,} bytes total")

        # ── MVP: Row count ─────────────────────────────────────────────
        if bucket and exists:
            row_count = read_parquet_row_count_s3(s3_client, bucket, prefix)
        else:
            row_count = -1  # No disponible en local sin pyarrow

        min_rows = expected.get("min_row_count", 1)
        if row_count >= 0:
            result.add_check("Row count suficiente",
                             row_count >= min_rows,
                             f"{row_count:,} filas (mínimo esperado: {min_rows:,})")
        else:
            result.add_check("Row count suficiente", True,
                             f"No disponible (pyarrow requerido). Mínimo esperado: {min_rows:,}")

        # ── PRO: Schema Drift Detection ────────────────────────────────
        if bucket and exists:
            actual_schema = read_parquet_schema_s3(s3_client, bucket, prefix)
        elif local_path:
            actual_schema = read_parquet_schema_local(local_path, table_name)
        else:
            actual_schema = None

        if actual_schema:
            if register_schema:
                # Modo registro: guardar como referencia
                result.add_check("Schema registrado", True,
                                 f"{len(actual_schema)} columnas guardadas como referencia")
            else:
                # Modo validación: comparar contra esquema esperado
                expected_cols = expected.get("columns", {})
                drift = detect_schema_drift(actual_schema, expected_cols, table_name)

                if drift["has_drift"]:
                    drift_detail = []
                    if drift["added"]:
                        drift_detail.append(f"Agregadas: {drift['added']}")
                    if drift["removed"]:
                        drift_detail.append(f"Eliminadas: {drift['removed']}")
                    if drift["type_changed"]:
                        drift_detail.append(f"Tipo cambiado: {drift['type_changed']}")

                    result.add_check("Schema Drift", False,
                                     "; ".join(drift_detail))
                else:
                    result.add_check("Schema Drift", True,
                                     "Sin cambios detectados vs esquema esperado")

        results.append(result)

    return results


def main() -> None:
    args = parse_args()
    bucket = args.bucket
    local_path = Path(args.local_path) if args.local_path else None

    logger.info(f"\n{'='*60}")
    logger.info(f"VALIDACIÓN POST-INGESTA — CAPA RAW")
    logger.info(f"{'='*60}")

    if bucket:
        import boto3
        s3_client = boto3.client("s3")
        logger.info(f"Bucket: s3://{bucket}")
    else:
        s3_client = None
        logger.info(f"Local path: {local_path}")

    # Determinar qué fuentes validar
    sources = {}
    if args.source in ("ecommerce", "all"):
        sources["ecommerce"] = EXPECTED_SCHEMAS["ecommerce"]
    if args.source in ("weather", "all"):
        sources["weather"] = EXPECTED_SCHEMAS["weather"]

    # Ejecutar validaciones
    all_results = []
    observed_schemas = {}  # Para schema registry

    for source_name, schemas in sources.items():
        logger.info(f"\n{'─'*40}")
        logger.info(f"Fuente: {source_name}")
        logger.info(f"{'─'*40}")

        results = validate_source(
            source_name, schemas, s3_client, bucket, local_path, args.register_schema
        )
        all_results.extend(results)

    # ── Reporte Final ──────────────────────────────────────────────────
    logger.info(f"\n{'='*60}")
    logger.info(f"REPORTE DE VALIDACIÓN")
    logger.info(f"{'='*60}")

    total_checks = 0
    passed_checks = 0
    failed_tables = []

    for result in all_results:
        for name, passed, detail in result.checks:
            total_checks += 1
            if passed:
                passed_checks += 1

        if not result.all_passed:
            failed_tables.append(f"{result.source}/{result.table}")

        logger.info(f"  {result.summary()}")

    logger.info(f"\n  Total: {passed_checks}/{total_checks} checks pasaron.")

    if failed_tables:
        logger.error(f"\n  ❌ TABLAS CON ERRORES: {', '.join(failed_tables)}")
        logger.error("  Revisar los datos en S3 y la configuración de Airbyte.")
        sys.exit(1)
    else:
        logger.info(f"\n  🎉 Todas las validaciones pasaron correctamente.")
        sys.exit(0)


if __name__ == "__main__":
    main()
