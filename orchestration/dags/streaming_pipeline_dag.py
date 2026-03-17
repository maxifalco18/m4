"""
orchestration/dags/streaming_pipeline_dag.py
==============================================
DAG de Airflow para gestionar el ciclo de vida del consumidor de Spark Streaming.

A diferencia del DAG ETLT (batch), un job de streaming idealmente
corre 24/7. Airflow no está diseñado para mantener tareas corriendo
perpetuamente, pero se usa frecuentemente como "supervisor" (watchdog)
para reiniciar jobs de streaming caídos o desplegarlos en clústeres EMR.

Flujo:
  1. Verifica si el job ya está corriendo.
  2. Si no lo está, lanza el spark-submit con el consumer en un EMR o EC2.
  3. Tiene sensores o timeouts para fallar y enviar alertas de caída.
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from utils.slack_alerts import on_failure_callback

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": on_failure_callback,
}

S3_BUCKET = os.getenv("S3_BUCKET_NAME", "pi-m4-datalake")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


with DAG(
    dag_id="streaming_watchdog_v1",
    description="Supervisa y reinicia el job de Spark Structured Streaming",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/5 * * * *",  # Ejecuta cada 5 mins para checkear estado
    catchup=False,
    max_active_runs=1,
    tags=["streaming", "weather", "avance-5"],
) as dag:

    dag.doc_md = """
    ## Watchdog de Streaming

    Este DAG se ejecuta cada 5 minutos.
    - Observa si el proceso `spark_streaming_consumer.py` sigue vivo.
    - Si murió (OOM, fallo de red, etc), lo reinicia automáticamente.
    - Envía alertas a Slack en caso de fallos recurrentes completos.
    """

    # Nota: Este BashOperator es una simplificación para EC2 local.
    # En producción AWS, usaríamos `EmrAddStepsOperator` o enviaríamos
    # un job a ECS/EKS, usando sensores para monitorear el Job ID.

    check_and_start_streaming = BashOperator(
        task_id="check_and_start_consumer",
        bash_command=f"""
            # Verificar si ya está corriendo localmente
            if pgrep -f "spark_streaming_consumer.py" > /dev/null; then
                echo "✅ El consumidor de streaming está corriendo. Saliendo..."
                exit 0
            else
                echo "⚠️  Consumidor caído o no iniciado. Lanzando spark-submit..."
                
                # Lanzar en background con nohup para que Airflow no se quede bloqueado
                cd /opt/airflow/streaming/consumer
                export S3_BUCKET_NAME={S3_BUCKET}
                export AWS_REGION={AWS_REGION}
                
                nohup spark-submit \
                    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
                    spark_streaming_consumer.py > consumer.log 2>&1 &
                
                echo "🚀 Consumidor lanzado. PID: $!"
            fi
        """,
        doc_md="Comprueba si el proceso existe; si no, lanza un nuevo spark-submit nohup",
    )

    check_and_start_streaming
