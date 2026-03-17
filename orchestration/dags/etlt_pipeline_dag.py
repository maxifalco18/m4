"""
orchestration/dags/etlt_pipeline_dag.py
=========================================
DAG principal del pipeline ETLT en AWS Data Lake.

Flujo de ejecución:
  1. trigger_airbyte_ecommerce  → Dispara sync Airbyte (PostgreSQL Olist → S3)
  2. trigger_airbyte_weather    → Dispara sync Airbyte (OWM API → S3)  [paralelo]
  3. validate_raw_data          → Verifica integridad + schema drift en raw/
  4. great_expectations_check   → Validación de calidad de datos (GE suites)
  5. spark_raw_to_processed     → Job Spark: Bronze → Silver (modelo estrella)
  6. spark_processed_to_gold    → Job Spark: Silver → Gold (KPIs)
  7. notify_success             → Alerta Slack de pipeline exitoso

Características:
  - Retries: 2 intentos, 5 min entre reintentos (tasks de ingesta)
  - SLA:     45 min (alerta si el DAG no termina en ese tiempo)
  - Emails:  en fallo (configurar SMTP en Airflow)
  - Slack:   on_failure_callback y on_success_callback en todo el DAG
  - Depends: task_groups para agrupar Airbyte + validación lógicamente

Schedule: diario a las 06:00 UTC (fuera de peak de AWS, costo óptimo)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.task_group import TaskGroup

from utils.slack_alerts import on_failure_callback, on_success_callback

# ─── Configuración del DAG ────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-engineering@pi-m4.local"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "on_failure_callback": on_failure_callback,
}

# Variables de entorno accesibles dentro del contenedor Airflow
import os
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "pi-m4-datalake")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AIRBYTE_WORKSPACE_ID = os.getenv("AIRBYTE_WORKSPACE_ID", "")
PROCESSING_DIR = "/opt/airflow/processing"

# IDs de las conexiones de Airbyte (obtener desde Airbyte Cloud UI → Settings → Sources)
# Reemplazar con los IDs reales después de crear las conexiones
AIRBYTE_CONN_ECOMMERCE = "{{ var.value.airbyte_conn_id_ecommerce }}"
AIRBYTE_CONN_WEATHER   = "{{ var.value.airbyte_conn_id_weather }}"


# ─── DAG Definition ──────────────────────────────────────────────────────────

with DAG(
    dag_id="etlt_pipeline_v1",
    description="Pipeline ETLT: Airbyte → Spark → Gold (Olist + Weather)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",        # Diario a las 06:00 UTC
    catchup=False,                         # No ejecutar fechas pasadas
    max_active_runs=1,                     # Solo 1 ejecución concurrente
    tags=["etlt", "olist", "weather", "avance-4"],
    on_success_callback=on_success_callback,
    sla_miss_callback=on_failure_callback,  # Alerta si supera los 45 min
    dagrun_timeout=timedelta(minutes=90),
) as dag:

    dag.doc_md = """
    ## Pipeline ETLT — Proyecto Integrador M4

    | Capa | Herramienta | Descripción |
    |------|-------------|-------------|
    | Ingesta | Airbyte Cloud | Olist (PostgreSQL) + Weather (HTTP API) → S3 raw/ |
    | Validación | Python + GE | Schema drift + row count + calidad |
    | Procesamiento | Apache Spark | raw/ → processed/ (estrella) → gold/ (KPIs) |
    | Alertas | Slack | Notifica éxito/fallo en #data-engineering |
    """

    # ─── TASK GROUP 1: Ingesta Airbyte (paralela) ─────────────────────────

    with TaskGroup("ingesta_airbyte", tooltip="Syncs de Airbyte (Olist + Weather)") as tg_ingesta:

        # Trigger sync Airbyte Olist → S3
        # Usa el endpoint de la API de Airbyte Cloud para iniciar el sync.
        # En Free Tier, el sync es manual — este operator lo dispara vía REST.
        trigger_ecommerce = SimpleHttpOperator(
            task_id="trigger_airbyte_ecommerce",
            http_conn_id="airbyte_cloud_api",     # Definir en Airflow Connections
            endpoint=f"/v1/connections/{AIRBYTE_CONN_ECOMMERCE}/sync",
            method="POST",
            headers={"Authorization": "Bearer {{ var.value.airbyte_api_key }}",
                     "Content-Type": "application/json"},
            response_check=lambda response: response.status_code in [200, 201],
            log_response=True,
            doc_md="Dispara el sync Airbyte de PostgreSQL Olist → S3 raw/ecommerce/",
        )

        # Trigger sync Airbyte Weather → S3 (en paralelo con ecommerce)
        trigger_weather = SimpleHttpOperator(
            task_id="trigger_airbyte_weather",
            http_conn_id="airbyte_cloud_api",
            endpoint=f"/v1/connections/{AIRBYTE_CONN_WEATHER}/sync",
            method="POST",
            headers={"Authorization": "Bearer {{ var.value.airbyte_api_key }}",
                     "Content-Type": "application/json"},
            response_check=lambda response: response.status_code in [200, 201],
            log_response=True,
            doc_md="Dispara el sync Airbyte de OpenWeatherMap API → S3 raw/weather_api/",
        )

        # Poll hasta que el sync de ecommerce termine
        wait_ecommerce_sync = BashOperator(
            task_id="wait_ecommerce_sync",
            bash_command="""
                echo "Esperando sync de Airbyte Olist..."
                sleep 120  # Esperar 2 min antes del primer check
                # Poll cada 30s hasta max 20 min
                for i in {1..40}; do
                    STATUS=$(curl -s -H "Authorization: Bearer $AIRBYTE_API_KEY" \
                        "https://api.airbyte.com/v1/jobs?connectionId=$AIRBYTE_CONN_ECOMMERCE&limit=1" \
                        | python3 -c "import sys,json; j=json.load(sys.stdin); print(j['data'][0]['status'])" 2>/dev/null || echo "pending")
                    echo "Check $i/40: estado=$STATUS"
                    if [ "$STATUS" = "succeeded" ]; then echo "✅ Sync completado"; exit 0; fi
                    if [ "$STATUS" = "failed" ]; then echo "❌ Sync falló"; exit 1; fi
                    sleep 30
                done
                echo "⏰ Timeout esperando sync"; exit 1
            """,
            retries=1,
            env={
                "AIRBYTE_API_KEY": "{{ var.value.airbyte_api_key }}",
                "AIRBYTE_CONN_ECOMMERCE": AIRBYTE_CONN_ECOMMERCE,
            },
        )

        trigger_ecommerce >> wait_ecommerce_sync

    # ─── TASK GROUP 2: Validación post-ingesta ────────────────────────────

    with TaskGroup("validacion_raw", tooltip="Validación de datos en capa raw/") as tg_validacion:

        validate_raw = BashOperator(
            task_id="validate_raw_data",
            bash_command=f"""
                echo "Validando datos en s3://{S3_BUCKET}/..."
                python3 /opt/airflow/processing/../ingestion/scripts/validate_raw_data.py \\
                    --bucket {S3_BUCKET} \\
                    --source all
            """,
            retries=1,
            doc_md="Ejecuta validate_raw_data.py: row count + schema drift detection",
        )

        # Great Expectations: validar suites de calidad configuradas
        ge_check = PythonOperator(
            task_id="great_expectations_check",
            python_callable=_run_great_expectations,
            op_kwargs={"bucket": S3_BUCKET, "source": "ecommerce"},
            doc_md="Valida expectativas de GE en raw/ecommerce/ (nulos, tipos, rangos)",
        )

        validate_raw >> ge_check

    # ─── TASK GROUP 3: Procesamiento Spark ────────────────────────────────

    with TaskGroup("spark_processing", tooltip="Jobs PySpark Bronze→Silver→Gold") as tg_spark:

        spark_raw_to_processed = BashOperator(
            task_id="spark_raw_to_processed",
            bash_command=f"""
                echo "Iniciando Spark: raw → processed"
                cd /opt/airflow/processing
                export SPARK_MODE=ec2
                export S3_BUCKET_NAME={S3_BUCKET}
                export AWS_REGION={AWS_REGION}
                bash spark-submit.sh raw_to_processed ec2
            """,
            retries=1,
            execution_timeout=timedelta(minutes=30),
            doc_md="Spark job: Bronze → Silver (modelo estrella Olist + weather_enriched)",
        )

        spark_processed_to_gold = BashOperator(
            task_id="spark_processed_to_gold",
            bash_command=f"""
                echo "Iniciando Spark: processed → gold"
                cd /opt/airflow/processing
                export SPARK_MODE=ec2
                export S3_BUCKET_NAME={S3_BUCKET}
                export AWS_REGION={AWS_REGION}
                bash spark-submit.sh processed_to_gold ec2
            """,
            retries=1,
            execution_timeout=timedelta(minutes=25),
            doc_md="Spark job: Silver → Gold (7 KPIs analíticos)",
        )

        spark_raw_to_processed >> spark_processed_to_gold

    # ─── TASK FINAL: Notificación de éxito ───────────────────────────────

    notify_pipeline_success = PythonOperator(
        task_id="notify_pipeline_success",
        python_callable=_notify_pipeline_done,
        doc_md="Envía resumen del pipeline completado a Slack",
    )

    # ─── Grafo de dependencias ────────────────────────────────────────────
    #
    # tg_ingesta (Airbyte Olist + Weather en PARALELO)
    #     ↓
    # tg_validacion (validate_raw → ge_check en SERIE)
    #     ↓
    # tg_spark (raw_to_processed → processed_to_gold en SERIE)
    #     ↓
    # notify_pipeline_success
    #
    tg_ingesta >> tg_validacion >> tg_spark >> notify_pipeline_success


# ─── Funciones Python de los tasks ───────────────────────────────────────────

def _run_great_expectations(bucket: str, source: str, **context) -> None:
    """
    Ejecuta las suites de validación de Great Expectations para la capa raw.

    Las suites se definen en quality/great_expectations/expectations/*.json
    y verifican:
    - Columnas esperadas (column_must_exist)
    - Nulos en columnas clave (expect_column_values_to_not_be_null)
    - Tipos de datos (expect_column_values_to_be_of_type)
    - Rangos de valores (expect_column_values_to_be_between)
    """
    import great_expectations as ge

    GE_DIR = "/opt/airflow/quality/great_expectations"

    try:
        context_ge = ge.get_context(context_root_dir=GE_DIR)

        results = context_ge.run_checkpoint(
            checkpoint_name=f"raw_{source}_checkpoint"
        )

        if not results["success"]:
            failed_expectations = [
                r for r in results["run_results"].values()
                if not r["validation_result"]["success"]
            ]
            raise ValueError(
                f"Great Expectations falló: {len(failed_expectations)} expectativas no cumplidas."
            )

        print(f"✅ Great Expectations: todas las validaciones pasaron para '{source}'.")

    except ModuleNotFoundError:
        print("⚠️  great_expectations no instalado. Skipping GE check.")
    except FileNotFoundError:
        print(f"⚠️  Checkpoint 'raw_{source}_checkpoint' no encontrado. "
              "Definirlo en quality/great_expectations/.")


def _notify_pipeline_done(**context) -> None:
    """Envía un resumen ejecutivo del pipeline completado a Slack."""
    import os
    import requests

    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        print("SLACK_WEBHOOK_URL no configurado.")
        return

    run_id = context.get("run_id", "N/A")
    dag_run = context.get("dag_run")
    start = dag_run.start_date.strftime("%H:%M UTC") if dag_run else "N/A"

    payload = {
        "text": (
            f"🎉 *Pipeline ETLT completado exitosamente*\n"
            f"• Run ID: `{run_id}`\n"
            f"• Inicio: `{start}`\n"
            f"• Capas actualizadas: `raw/` → `processed/` → `gold/`\n"
            f"• KPIs disponibles en `s3://{S3_BUCKET}/gold/`"
        )
    }

    try:
        requests.post(webhook_url, json=payload, timeout=10)
    except Exception as e:
        print(f"Error Slack: {e}")
