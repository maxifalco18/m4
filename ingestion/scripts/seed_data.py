"""
ingestion/scripts/seed_data.py
================================
Script de carga masiva del dataset Olist en PostgreSQL (Supabase/Neon).

Pre-requisitos:
    1. Descargar el dataset de Kaggle:
       https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
    2. Extraer los CSVs en una carpeta local (ej: ./data/olist/)
    3. Ejecutar primero el DDL: psql -f ingestion/scripts/ddl_olist.sql
    4. Copiar .env.example a .env y completar las credenciales de Supabase

Uso:
    python ingestion/scripts/seed_data.py --data-dir ./data/olist/
    python ingestion/scripts/seed_data.py --data-dir ./data/olist/ --dry-run

El script carga los CSVs en el orden correcto de dependencias FK.
"""

import argparse
import csv
import logging
import os
import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("seed_data")


# --------------------------------------------------------------------------- #
# Mapeo: nombre del CSV de Kaggle → tabla SQL (en orden de carga por FK)
# --------------------------------------------------------------------------- #
CSV_TO_TABLE_MAP = [
    # Primero: tablas sin dependencias (lookup / dimensiones)
    {
        "csv_file": "olist_geolocation_dataset.csv",
        "table_name": "olist_geolocation",
        "columns": [
            "geolocation_zip_code_prefix", "geolocation_lat",
            "geolocation_lng", "geolocation_city", "geolocation_state",
        ],
        "has_updated_at": False,
    },
    {
        "csv_file": "product_category_name_translation.csv",
        "table_name": "olist_product_category_name_translation",
        "columns": ["product_category_name", "product_category_name_english"],
        "has_updated_at": False,
    },
    {
        "csv_file": "olist_customers_dataset.csv",
        "table_name": "olist_customers",
        "columns": [
            "customer_id", "customer_unique_id", "customer_zip_code_prefix",
            "customer_city", "customer_state",
        ],
        "has_updated_at": True,
    },
    {
        "csv_file": "olist_sellers_dataset.csv",
        "table_name": "olist_sellers",
        "columns": [
            "seller_id", "seller_zip_code_prefix",
            "seller_city", "seller_state",
        ],
        "has_updated_at": True,
    },
    {
        "csv_file": "olist_products_dataset.csv",
        "table_name": "olist_products",
        "columns": [
            "product_id", "product_category_name", "product_name_length",
            "product_description_length", "product_photos_qty",
            "product_weight_g", "product_length_cm",
            "product_height_cm", "product_width_cm",
        ],
        "has_updated_at": True,
    },
    # Después: tablas con FK a las anteriores
    {
        "csv_file": "olist_orders_dataset.csv",
        "table_name": "olist_orders",
        "columns": [
            "order_id", "customer_id", "order_status",
            "order_purchase_timestamp", "order_approved_at",
            "order_delivered_carrier_date", "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
        "has_updated_at": True,
    },
    {
        "csv_file": "olist_order_items_dataset.csv",
        "table_name": "olist_order_items",
        "columns": [
            "order_id", "order_item_id", "product_id", "seller_id",
            "shipping_limit_date", "price", "freight_value",
        ],
        "has_updated_at": True,
    },
    {
        "csv_file": "olist_order_payments_dataset.csv",
        "table_name": "olist_order_payments",
        "columns": [
            "order_id", "payment_sequential", "payment_type",
            "payment_installments", "payment_value",
        ],
        "has_updated_at": True,
    },
    {
        "csv_file": "olist_order_reviews_dataset.csv",
        "table_name": "olist_order_reviews",
        "columns": [
            "review_id", "order_id", "review_score",
            "review_comment_title", "review_comment_message",
            "review_creation_date", "review_answer_timestamp",
        ],
        "has_updated_at": True,
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Carga los CSVs de Olist en PostgreSQL.")
    parser.add_argument(
        "--data-dir", required=True,
        help="Ruta a la carpeta con los CSVs de Kaggle descomprimidos.",
    )
    parser.add_argument("--batch-size", type=int, default=5000, help="Filas por batch INSERT.")
    parser.add_argument("--dry-run", action="store_true", help="Solo valida sin insertar.")
    return parser.parse_args()


def get_db_connection():
    """Crea conexión a PostgreSQL usando variables de entorno."""
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "postgres"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            sslmode="require",  # Supabase requiere SSL
        )
        conn.autocommit = False
        logger.info(f"✅ Conexión a PostgreSQL establecida ({os.getenv('POSTGRES_HOST')})")
        return conn
    except psycopg2.Error as e:
        logger.error(f"❌ Error de conexión: {e}")
        sys.exit(1)


def clean_value(val: str, col_name: str):
    """Limpia valores del CSV. Convierte strings vacíos a None (NULL en SQL)."""
    if val == "" or val is None:
        return None
    return val.strip()


def load_csv_to_table(conn, csv_path: Path, table_config: dict, batch_size: int, dry_run: bool) -> int:
    """
    Carga un CSV en la tabla PostgreSQL usando bulk INSERT con execute_values.

    execute_values() es ~10x más rápido que INSERT fila por fila.
    Se procesan en batches para controlar la memoria en datasets grandes.

    Returns:
        int: Cantidad de filas insertadas.
    """
    table_name = table_config["table_name"]
    columns = table_config["columns"]
    has_updated_at = table_config["has_updated_at"]

    if not csv_path.exists():
        logger.warning(f"⚠️  Archivo no encontrado: {csv_path}. Omitiendo {table_name}.")
        return 0

    # Leer CSV
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        rows = []
        skipped = 0
        for row in reader:
            # Extraer solo las columnas que necesitamos, en el orden correcto
            try:
                values = tuple(clean_value(row.get(col, ""), col) for col in columns)
                rows.append(values)
            except Exception as e:
                skipped += 1
                continue

    total_rows = len(rows)
    logger.info(f"  📄 {csv_path.name}: {total_rows:,} filas leídas ({skipped} omitidas).")

    if dry_run:
        logger.info(f"  [DRY-RUN] Se insertarían {total_rows:,} filas en {table_name}.")
        return total_rows

    # Insertar en batches
    cursor = conn.cursor()

    # Columnas SQL (sin updated_at — se usa DEFAULT NOW())
    col_list = ", ".join(columns)
    placeholder = "(" + ", ".join(["%s"] * len(columns)) + ")"

    insert_sql = f"INSERT INTO {table_name} ({col_list}) VALUES %s ON CONFLICT DO NOTHING"

    inserted = 0
    for i in range(0, total_rows, batch_size):
        batch = rows[i : i + batch_size]
        try:
            execute_values(cursor, insert_sql, batch, template=placeholder, page_size=batch_size)
            inserted += len(batch)
        except psycopg2.Error as e:
            conn.rollback()
            logger.error(f"  ❌ Error en batch {i//batch_size + 1} de {table_name}: {e}")
            # Re-intentar fila por fila para identificar la fila problemática
            for j, row in enumerate(batch):
                try:
                    cursor.execute(
                        f"INSERT INTO {table_name} ({col_list}) VALUES ({', '.join(['%s']*len(columns))}) ON CONFLICT DO NOTHING",
                        row,
                    )
                except psycopg2.Error:
                    logger.warning(f"    Fila {i + j} omitida por error de constraint.")
                    conn.rollback()
                    continue

    conn.commit()
    logger.info(f"  ✅ {table_name}: {inserted:,} filas insertadas.")
    return inserted


def main() -> None:
    args = parse_args()
    data_dir = Path(args.data_dir)

    if not data_dir.exists():
        logger.error(f"❌ Directorio no encontrado: {data_dir}")
        logger.error("   Descargar de: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce")
        sys.exit(1)

    logger.info(f"\n{'='*60}")
    logger.info(f"CARGA DE DATOS OLIST EN POSTGRESQL")
    logger.info(f"Directorio: {data_dir}")
    logger.info(f"{'='*60}\n")

    if args.dry_run:
        logger.info("🔍 MODO DRY-RUN: no se insertarán datos.\n")
        conn = None
    else:
        conn = get_db_connection()

    total_inserted = 0
    for config in CSV_TO_TABLE_MAP:
        csv_path = data_dir / config["csv_file"]
        count = load_csv_to_table(conn, csv_path, config, args.batch_size, args.dry_run)
        total_inserted += count

    if conn:
        conn.close()

    logger.info(f"\n{'='*60}")
    logger.info(f"🎉 Carga completada: {total_inserted:,} filas totales.")
    logger.info(f"   Siguiente paso: Configurar conectores en Airbyte Cloud.")
    logger.info(f"{'='*60}")


if __name__ == "__main__":
    main()
