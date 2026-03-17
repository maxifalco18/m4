#!/usr/bin/env bash
# run_demo.sh
# ==================================
# Script interactivo 'idiot-proof' para correr la demostración de la
# Arquitectura Lambda y el Procesamiento Streaming localmente para la defensa.

set -euo pipefail

# Colores para mejor legibilidad
G="\033[0;32m"
C="\033[0;36m"
R="\033[0;31m"
NC="\033[0m"

echo -e "${C}====================================================${NC}"
echo -e "${C} Proyecto Integrador M4 - Arquitectura LAMBDA DEMO  ${NC}"
echo -e "${C}====================================================${NC}"

# 1. Chequear estado previo
echo -e "\n${G}[1/4] Inicializando y verificando Stack de Kafka...${NC}"
cd streaming
docker compose up -d
sleep 10  # Dar algo de tiempo a Zookeeper/Kafka
cd ..

# Crear el topic (silencio si ya existe)
bash streaming/scripts/create_topics.sh > /dev/null 2>&1

echo -e "Kafka UI disponible en: http://localhost:8090"

# Abrir ventana separada para el productor de eventos (Depende de si en GitBash hay terminal windows / Linux tmux)
echo -e "\n${G}[2/4] Lanzando Productor de Eventos Meteorologicos...${NC}"
echo -e "El productor enviará 1 evento JSON por segundo (Delay configurable)."

# Usamos nohup para la demo para no trabar la bash (o tmux si se estuviera usando)
nohup python streaming/producer/weather_producer.py > producer_demo.log 2>&1 &
PRODUCER_PID=$!
echo "Productor iniciado en background (PID: $PRODUCER_PID). Escribiendo en 'producer_demo.log'"

echo -e "\n${G}[3/4] Lanzando el Consumidor Spark Structured Streaming...${NC}"
echo -e "👉 Este script implementa el micro-batching (30s) y unificara con la data historica BATCH (Lambda)."
echo -e "${R}NOTA: En esta terminal se verá el log de Spark. Presionar Ctrl+C para finalizar la demostración.${NC}"
echo -e "En 10 segundos arrancará Spark...\n"
sleep 10

# Lanzar Consumidor Foreground (Para ver la salida de Batch y como une la Data)
export SPARK_MODE=local
bash processing/spark-submit.sh consumer/spark_streaming_consumer.py

# ================================
# Si el usuario mató Spark (Ctrl+C), cerramos el productor
echo -e "\n${C}====================================================${NC}"
echo -e "${G}[4/4] Limpiando procesos...${NC}"
kill -9 $PRODUCER_PID 2>/dev/null || true
echo -e "Demostración finalizada. Puedes hacer 'docker compose down' en /streaming si deseas apagar Kafka."
echo -e "${C}====================================================${NC}"
