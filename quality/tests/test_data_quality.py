"""
quality/tests/test_data_quality.py
====================================
Tests unitarios para las utilidades de calidad de datos.
"""

import pytest
from pyspark.sql import SparkSession
import sys
import os

# Agregar el directorio raíz del proyecto al PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from processing.jobs.utils.data_quality import (
    check_nulls,
    check_unique,
    check_range,
    check_allowed_values
)

@pytest.fixture(scope="session")
def spark():
    import sys
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    return (
        SparkSession.builder
        .appName("data-quality-tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

class TestDataQualityUtils:
    
    def test_check_nulls(self, spark):
        data = [("A", 1), ("B", None), ("C", 2)]
        df = spark.createDataFrame(data, ["name", "val"])
        
        # Con umbral 0.4 (1/3 = 33% nulos) -> debe pasar
        assert check_nulls(df, ["val"], threshold=0.4) is True
        # Con umbral 0.2 (33% > 20%) -> debe fallar
        assert check_nulls(df, ["val"], threshold=0.2) is False

    def test_check_unique(self, spark):
        data = [("key1", 10), ("key2", 20), ("key1", 30)] # duplicado key1
        df = spark.createDataFrame(data, ["pk", "val"])
        assert check_unique(df, ["pk"]) is False
        
        data_ok = [("key1", 10), ("key2", 20)]
        df_ok = spark.createDataFrame(data_ok, ["pk", "val"])
        assert check_unique(df_ok, ["pk"]) is True

    def test_check_range(self, spark):
        data = [(10,), (50,), (110,)] # 110 fuera de (0, 100)
        df = spark.createDataFrame(data, ["val"])
        assert check_range(df, "val", 0, 100) is False
        assert check_range(df, "val", 0, 120) is True

    def test_check_allowed_values(self, spark):
        data = [("delivered",), ("shipped",), ("unknown",)]
        df = spark.createDataFrame(data, ["status"])
        allowed = ["delivered", "shipped", "canceled"]
        assert check_allowed_values(df, "status", allowed) is False
        
        df_ok = df.filter(df.status != "unknown")
        assert check_allowed_values(df_ok, "status", allowed) is True
