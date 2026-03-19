"""
quality/tests/test_transformations.py
=======================================
Tests unitarios para las funciones de transformación de PySpark.

Usa moto para mockear S3 y PySpark en modo local.
Ejecutar con: pytest quality/tests/ -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType, TimestampType
)

import sys
import os
# Agregar el directorio raíz del proyecto al PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from processing.jobs.utils.transformations import (
    drop_duplicates_by_key,
    normalize_string_columns,
    cast_timestamp_columns,
    broadcast_join,
)


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #

@pytest.fixture(scope="session")
def spark():
    """Crea una SparkSession local para tests. Se usa una sola por sesión de pytest."""
    import sys
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    return (
        SparkSession.builder
        .appName("unit-tests")
        .master("local[1]")  # Un solo core — suficiente para tests
        .config("spark.sql.shuffle.partitions", "1")  # Reduce overhead en local
        .config("spark.ui.enabled", "false")  # Desactiva la UI para evitar conflictos de puerto
        .getOrCreate()
    )


# --------------------------------------------------------------------------- #
# Esquemas de prueba (mirror de los datos reales)
# --------------------------------------------------------------------------- #

ORDER_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=True),
    StructField("order_status", StringType(), nullable=True),
    StructField("order_purchase_timestamp", TimestampType(), nullable=True),
    StructField("order_delivered_customer_date", TimestampType(), nullable=True),
    StructField("order_estimated_delivery_date", TimestampType(), nullable=True),
])

WEATHER_SCHEMA = StructType([
    StructField("dt", IntegerType(), nullable=False),
    StructField("dt_iso", StringType(), nullable=False),
    StructField("city_name", StringType(), nullable=True),
    StructField("temp", DoubleType(), nullable=True),
    StructField("humidity", IntegerType(), nullable=True),
    StructField("wind_speed", DoubleType(), nullable=True),
])


# --------------------------------------------------------------------------- #
# Tests de transformaciones básicas
# (Los tests reales se completarán en el Avance 3 junto con los jobs de Spark)
# --------------------------------------------------------------------------- #

class TestBasicTransformations:
    """Tests para transformaciones elementales de datos."""

    def test_spark_session_is_created(self, spark):
        """Verifica que la SparkSession de tests se inicialice correctamente."""
        assert spark is not None
        assert spark.version is not None

    def test_deduplication(self, spark):
        """Verifica que la deduplicación por clave primaria funciona."""
        data = [
            ("order_001", "customer_A", "delivered"),
            ("order_001", "customer_A", "delivered"),  # duplicado
            ("order_002", "customer_B", "shipped"),
        ]
        df = spark.createDataFrame(data, ["order_id", "customer_id", "status"])
        deduped = df.dropDuplicates(["order_id"])
        assert deduped.count() == 2

    def test_null_filter_on_primary_key(self, spark):
        """Verifica que se eliminan filas con PK nula."""
        data = [
            ("order_001", "customer_A"),
            (None, "customer_B"),  # PK nula — debe eliminarse
            ("order_003", "customer_C"),
        ]
        df = spark.createDataFrame(data, ["order_id", "customer_id"])
        filtered = df.filter(df.order_id.isNotNull())
        assert filtered.count() == 2

    def test_column_rename(self, spark):
        """Verifica que el renombrado de columnas (raw → processed naming) funciona."""
        data = [("2024-08-01 00:00:00", 8.16, 63)]
        df = spark.createDataFrame(data, ["dt_iso", "temp", "humidity"])
        renamed = df.withColumnRenamed("dt_iso", "datetime_utc") \
                    .withColumnRenamed("temp", "temperature_celsius")
        assert "datetime_utc" in renamed.columns
        assert "temperature_celsius" in renamed.columns
        assert "temp" not in renamed.columns

    def test_weather_data_valid_temperature_range(self, spark):
        """
        Valida que los registros con temperaturas fuera de rango sean detectados.
        En Patagonia: temperaturas entre -50°C y 50°C son válidas.
        """
        data = [
            ("2024-08-01", 8.16, True),   # válida
            ("2024-08-02", -120.0, False), # inválida: por debajo del mínimo
            ("2024-08-03", 99.0, False),   # inválida: por encima del máximo
        ]
        df = spark.createDataFrame(data, ["date", "temperature", "expected_valid"])
        valid_df = df.filter((df.temperature >= -50) & (df.temperature <= 50))
        assert valid_df.count() == 1


class TestDataQualityChecks:
    """Tests de calidad de datos — se integran con Great Expectations en el Avance 4."""

    def test_no_negative_order_values(self, spark):
        """Los montos de órdenes no pueden ser negativos."""
        data = [
            ("order_001", 150.50),
            ("order_002", -10.00),  # inválido
            ("order_003", 0.0),     # borde — aceptable
        ]
        df = spark.createDataFrame(data, ["order_id", "order_value"])
        invalid = df.filter(df.order_value < 0)
        assert invalid.count() == 1, "Debe detectarse exactamente 1 valor negativo"

    def test_customer_id_not_empty_string(self, spark):
        """customer_id no puede ser una cadena vacía."""
        data = [
            ("order_001", "customer_A"),
            ("order_002", ""),          # inválido
            ("order_003", "customer_C"),
        ]
        df = spark.createDataFrame(data, ["order_id", "customer_id"])
        invalid = df.filter(df.customer_id == "")
        assert invalid.count() == 1


class TestRealTransformations:
    """Tests que importan y usan las funciones reales de transformations.py."""

    def test_drop_duplicates_by_key_real(self, spark):
        """Usa la función real drop_duplicates_by_key del proyecto."""
        data = [
            ("order_001", "customer_A", "delivered"),
            ("order_001", "customer_A", "delivered"),
            ("order_002", "customer_B", "shipped"),
        ]
        df = spark.createDataFrame(data, ["order_id", "customer_id", "status"])
        result = drop_duplicates_by_key(df, ["order_id"])
        assert result.count() == 2

    def test_normalize_string_columns_real(self, spark):
        """Usa la función real normalize_string_columns."""
        data = [
            ("  São Paulo  ",),
            ("RIO DE JANEIRO",),
            ("",),
        ]
        df = spark.createDataFrame(data, ["city"])
        result = normalize_string_columns(df, ["city"])
        rows = result.collect()
        assert rows[0]["city"] == "são paulo"
        assert rows[1]["city"] == "rio de janeiro"
        assert rows[2]["city"] is None  # vacío → null
