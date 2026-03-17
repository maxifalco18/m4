"""
orchestration/dags/s3_compaction_dag.py
=======================================
Solución al problema de los "Pequeños Archivos" (Small File Problem) en S3.

Los procesos de streaming como el que guarda en `raw-streaming` 
escriben pequeños archivos Parquet cada vez que se dispara un micro-batch
(en nuestro caso, cada 30 segundos), generando miles de KB files de pocos bytes.

Este proceso corre 1 vez al mes y compacta / reescribe 
los logs de streaming históricos crudos en archivos eficientes de ~128MB 
agrupados por fecha.
"""

from datetime import datetime
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from utils.slack_alerts import on_failure_callback, on_success_callback

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "on_failure_callback": on_failure_callback,
}

S3_BUCKET = os.getenv("S3_BUCKET_NAME", "pi-m4-datalake")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

with DAG(
    dag_id="s3_maintenance_compaction",
    description="Reparticiona y compacta el log histórico de raw-streaming en S3.",
    default_args=DEFAULT_ARGS,
    schedule_interval="@monthly",  # Se ejectuta una vez al mes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["maintenance", "s3", "compaction"],
    on_success_callback=on_success_callback,
) as dag:

    dag.doc_md = """
    ## Job de Mantenimiento de S3: Small File Compactor

    Al usar `outputMode("append")` en streaming crudo, creamos un Small File Problem 
    severo (1 archivo cada 30 segundos). Este DAG lee los logs del último mes
    y los consolida mediante `df.coalesce()` en Spark.
    """

    # En este proyecto usamos un BashOperator que llama a un PySpark script dedicado.
    # Acá usamos bash con el codigo heredado (inline) como ejemplo rapido para la demo,
    # aunque en pod es un job .py entero.
    
    compact_raw_streaming = BashOperator(
        task_id="compact_raw_streaming_files",
        bash_command=f"""
            echo "🔥 Iniciando Compactación Mensual de raw-streaming..."
            export S3_BUCKET_NAME={S3_BUCKET}
            export AWS_REGION={AWS_REGION}
            
            cat << 'EOF' > /tmp/compactor_job.py
import os, sys
from pyspark.sql import SparkSession

bucket = os.getenv("S3_BUCKET_NAME")
raw_path = f"s3a://{{bucket}}/raw-streaming/weather_events"
compacted_path = f"s3a://{{bucket}}/raw-streaming/weather_events_compacted"

spark = SparkSession.builder \\
    .appName("s3-compactor") \\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \\
    .config("spark.hadoop.fs.s3a.endpoint", f"s3.{{os.getenv('AWS_REGION')}}.amazonaws.com") \\
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \\
    .getOrCreate()

print(f"Leyendo miles de micro-archivos de {{raw_path}}...")
df = spark.read.parquet(raw_path)

# Coalesce junta todas las particiones en un número menor de archivos más grandes
print("Compactando...")
df.coalesce(5).write.mode("overwrite").partitionBy("ingestion_year", "ingestion_month").parquet(compacted_path)

print("✅ Data compactada exitosamente. En un entorno real se haria un swap de carpetas (o limpieza via S3 Lifecycle).")
spark.stop()
EOF
            cd /opt/airflow/processing
            spark-submit /tmp/compactor_job.py
        """,
        doc_md="Dispara un sub-job spark para compactar la subcarpeta raw-streaming reduciendo el overhead en S3",
    )
    
    compact_raw_streaming
