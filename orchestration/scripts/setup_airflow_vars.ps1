# orchestration/scripts/setup_airflow_vars.ps1
# Setea las variables de Airflow necesarias para el DAG etlt_pipeline_v1

$AIRBYTE_API_KEY = Read-Host "Ingresa tu Airbyte Cloud API Key"
$CONN_ECOMMERCE = Read-Host "Ingresa el Connection ID de eCommerce (Olist)"
$CONN_WEATHER = Read-Host "Ingresa el Connection ID de Weather API"

Write-Host "Configurando variables en Airflow..." -ForegroundColor Cyan

docker exec -it airflow-scheduler airflow variables set airbyte_api_key $AIRBYTE_API_KEY
docker exec -it airflow-scheduler airflow variables set airbyte_conn_id_ecommerce $CONN_ECOMMERCE
docker exec -it airflow-scheduler airflow variables set airbyte_conn_id_weather $CONN_WEATHER

Write-Host "✅ Variables configuradas exitosamente." -ForegroundColor Green
Write-Host "Nota: Asegúrate de que la conexión HTTP 'airbyte_cloud_api' esté creada en Airflow UI." -ForegroundColor Yellow
