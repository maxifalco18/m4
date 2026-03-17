#!/usr/bin/env bash
# processing/spark-submit.sh
# ===========================
# Script de ejecución de jobs Spark via spark-submit.
#
# Soporta tres modos:
#   local: desarrollo en laptop (sin cluster)
#   ec2:   instancia EC2 única (Free Tier t2.large o similar)
#   emr:   cluster Amazon EMR (para volúmenes > 10 GB)
#
# Uso:
#   bash processing/spark-submit.sh raw_to_processed [local|ec2|emr]
#   bash processing/spark-submit.sh processed_to_gold [local|ec2|emr]
#
# Variables de entorno requeridas (definidas en .env):
#   S3_BUCKET_NAME, AWS_REGION, SPARK_MODE

set -euo pipefail

# ─── Args ────────────────────────────────────────────────────────────────────
JOB="${1:-raw_to_processed}"          # Job a ejecutar
MODE="${2:-${SPARK_MODE:-local}}"     # Modo de ejecución

# ─── Configuración de directorios ────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
JOBS_DIR="${SCRIPT_DIR}/jobs"
UTILS_ZIP="${SCRIPT_DIR}/utils.zip"

# ─── Configuración de recursos por modo ──────────────────────────────────────
case "$MODE" in
  local)
    MASTER="local[*]"
    DRIVER_MEMORY="2g"
    EXECUTOR_MEMORY="2g"
    SHUFFLE_PARTITIONS="8"
    ;;
  ec2)
    # EC2 t2.large: 2 vCPU, 8 GB RAM. Spark corre en modo standalone.
    # Se reserva 1 GB para el OS y el driver.
    MASTER="local[2]"   # local[vCPUs] en instancia single-node
    DRIVER_MEMORY="4g"
    EXECUTOR_MEMORY="4g"
    SHUFFLE_PARTITIONS="32"
    ;;
  emr)
    # En EMR, spark-submit usa el cluster YARN automáticamente.
    # Los recursos los define el cluster (instancias m5.xlarge o similar).
    MASTER="yarn"
    DRIVER_MEMORY="4g"
    EXECUTOR_MEMORY="8g"
    SHUFFLE_PARTITIONS="200"
    ;;
  *)
    echo "❌ Modo inválido: '$MODE'. Usar: local | ec2 | emr"
    exit 1
    ;;
esac

# ─── Crear zip de utilidades (necesario para distribuirlas en el cluster) ────
echo "📦 Empaquetando utilidades en utils.zip..."
cd "${SCRIPT_DIR}"
zip -r utils.zip jobs/utils/ -x "*.pyc" "__pycache__/*" > /dev/null
cd -

# ─── Validar que el job existe ───────────────────────────────────────────────
JOB_FILE="${JOBS_DIR}/${JOB}.py"
if [ ! -f "$JOB_FILE" ]; then
  echo "❌ Job no encontrado: ${JOB_FILE}"
  echo "   Jobs disponibles: raw_to_processed, processed_to_gold"
  exit 1
fi

# ─── Ejecución ───────────────────────────────────────────────────────────────
echo ""
echo "============================================================"
echo " SPARK SUBMIT"
echo " Job:       ${JOB}"
echo " Modo:      ${MODE}"
echo " Master:    ${MASTER}"
echo " Driver:    ${DRIVER_MEMORY}"
echo " Executor:  ${EXECUTOR_MEMORY}"
echo " Shuffles:  ${SHUFFLE_PARTITIONS} particiones"
echo " Bucket:    s3a://${S3_BUCKET_NAME:-NOT_SET}"
echo "============================================================"
echo ""

spark-submit \
  --master "${MASTER}" \
  --driver-memory "${DRIVER_MEMORY}" \
  --executor-memory "${EXECUTOR_MEMORY}" \
  \
  --py-files "${UTILS_ZIP}" \
  \
  --packages \
    "org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.261" \
  \
  --conf "spark.sql.adaptive.enabled=true" \
  --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
  --conf "spark.sql.shuffle.partitions=${SHUFFLE_PARTITIONS}" \
  --conf "spark.sql.parquet.compression.codec=snappy" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain" \
  --conf "spark.hadoop.fs.s3a.fast.upload=true" \
  --conf "spark.hadoop.fs.s3a.endpoint=s3.${AWS_REGION:-us-east-1}.amazonaws.com" \
  \
  --conf "spark.sql.autoBroadcastJoinThreshold=52428800" \
  --conf "spark.sql.adaptive.skewJoin.enabled=true" \
  \
  --conf "spark.eventLog.enabled=false" \
  \
  --name "pi-m4-${JOB}" \
  \
  "${JOB_FILE}"

EXIT_CODE=$?
if [ $EXIT_CODE -eq 0 ]; then
  echo ""
  echo "✅ Job '${JOB}' completado exitosamente."
else
  echo ""
  echo "❌ Job '${JOB}' falló con código ${EXIT_CODE}."
  exit $EXIT_CODE
fi
