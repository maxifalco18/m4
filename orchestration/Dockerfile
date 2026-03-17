# orchestration/Dockerfile
# =========================
# Imagen custom de Airflow con todas las dependencias del proyecto.
# Extiende la imagen oficial de Apache Airflow.

FROM apache/airflow:2.9.0-python3.11

USER root

# Instalar Java (requerido para PySpark)
RUN apt-get update && apt-get install -yq \
    default-jdk-headless \
    curl \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

# Instalar providers y dependencias Python
RUN pip install --no-cache-dir \
    # Providers de Airflow
    apache-airflow-providers-amazon==8.20.0 \
    apache-airflow-providers-slack==8.6.0 \
    apache-airflow-providers-http==4.10.0 \
    # PySpark (misma versión que en processing/requirements.txt)
    pyspark==3.5.0 \
    # AWS
    boto3==1.34.0 \
    # Calidad de datos
    great-expectations==0.18.12 \
    # PostgreSQL
    psycopg2-binary==2.9.9 \
    # Utilities
    python-dotenv==1.0.0 \
    pyarrow==15.0.0 \
    s3fs==2024.1.0
