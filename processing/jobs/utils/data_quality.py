"""
processing/jobs/utils/data_quality.py
======================================
Utilidades para validación de calidad de datos en jobs de PySpark.
Proporciona verificaciones reusables para las capas Processed y Gold.
"""

import logging
from typing import List, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger("data_quality")

def check_nulls(df: DataFrame, columns: List[str], threshold: float = 0.0) -> bool:
    """
    Verifica si el porcentaje de nulos en las columnas excede el umbral.
    threshold: 0.0 significa que no se permiten nulos.
    """
    total_rows = df.count()
    if total_rows == 0:
        return True
    
    passed = True
    for col in columns:
        null_count = df.filter(F.col(col).isNull()).count()
        null_pct = null_count / total_rows
        if null_pct > threshold:
            logger.error(f"❌ DQ Fail | Columna '{col}' tiene {null_pct:.2%} nulos (umbral: {threshold:.2%})")
            passed = False
        else:
            logger.info(f"✅ DQ Pass | Columna '{col}' tiene {null_pct:.2%} nulos")
            
    return passed

def check_unique(df: DataFrame, columns: List[str]) -> bool:
    """Verifica que la combinación de columnas sea única (clave primaria)."""
    total_rows = df.count()
    unique_rows = df.select(columns).distinct().count()
    
    if total_rows != unique_rows:
        diff = total_rows - unique_rows
        logger.error(f"❌ DQ Fail | Se detectaron {diff} registros duplicados para PK {columns}")
        return False
    
    logger.info(f"✅ DQ Pass | Unicidad garantizada para PK {columns}")
    return True

def check_range(df: DataFrame, column: str, min_val: float, max_val: float) -> bool:
    """Verifica que los valores de una columna numérica estén en el rango esperado."""
    out_of_range = df.filter((F.col(column) < min_val) | (F.col(column) > max_val)).count()
    
    if out_of_range > 0:
        logger.error(f"❌ DQ Fail | Columna '{column}' tiene {out_of_range} valores fuera de rango [{min_val}, {max_val}]")
        return False
    
    logger.info(f"✅ DQ Pass | Columna '{column}' valores dentro de rango [{min_val}, {max_val}]")
    return True

def check_allowed_values(df: DataFrame, column: str, allowed_set: List) -> bool:
    """Verifica que todos los valores de una columna pertenezcan a un conjunto permitido."""
    invalid_count = df.filter(~F.col(column).isin(allowed_set)).count()
    
    if invalid_count > 0:
        logger.error(f"❌ DQ Fail | Columna '{column}' tiene {invalid_count} valores no permitidos")
        # Mostrar algunos ejemplos de valores inválidos
        df.filter(~F.col(column).isin(allowed_set)).select(column).distinct().show(5)
        return False
    
    logger.info(f"✅ DQ Pass | Columna '{column}' todos los valores están en el conjunto permitido")
    return True
