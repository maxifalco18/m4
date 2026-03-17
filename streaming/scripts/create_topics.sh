#!/usr/bin/env bash
# streaming/scripts/create_topics.sh
# ==================================
# Script para crear los topics necesarios en Kafka una vez que
# los brokers están levantados vía docker-compose.
#
# Uso: bash streaming/scripts/create_topics.sh

set -euo pipefail

echo "⏳ Esperando a que Kafka inicie..."
sleep 10

# Crear topic para eventos de clima en tiempo real
# Refactor: replication-factor 1 porque solo tenemos 1 broker local.
echo "🛠️  Creando topic 'weather_events'..."
docker exec pi-m4-kafka kafka-topics \
    --create \
    --if-not-exists \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=86400000 \
    --topic weather_events

# Listar topics para verificar
echo "✅ Topics disponibles en Kafka:"
docker exec pi-m4-kafka kafka-topics \
    --list \
    --bootstrap-server localhost:9092
