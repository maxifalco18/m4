"""
streaming/producer/weather_producer.py
======================================
Simulador de eventos meteorológicos en tiempo real para Apache Kafka.

Lee el archivo histórico Patagonia_-41.json y envía cada registro
como un evento JSON al topic 'weather_events' con un pequeño delay,
simulando un flujo constante de datos en tiempo real.

Uso:
    python streaming/producer/weather_producer.py
"""

import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("weather_producer")

try:
    from kafka import KafkaProducer
except ImportError:
    print("❌ Falta la librería kafka-python. Instalar con: pip install kafka-python")
    sys.exit(1)

# Configuración
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPIC_NAME = "weather_events"
DATA_FILE = Path(__file__).parent.parent.parent / "0. Data" / "Patagonia_-41.json"


def create_producer() -> KafkaProducer:
    """Instancia y retorna un KafkaProducer con serialización JSON."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            retries=5,
        )
        logger.info(f"✅ Conectado a Kafka broker: {KAFKA_BROKER}")
        return producer
    except Exception as e:
        logger.error(f"❌ Error conectando a Kafka: {e}")
        sys.exit(1)


def simulate_stream(producer: KafkaProducer, file_path: Path, delay_seconds: float = 0.5):
    """Lee el CSV/JSON local y emite eventos uno a uno con un delay."""
    if not file_path.exists():
        logger.error(f"❌ Archivo de datos no encontrado: {file_path}")
        sys.exit(1)

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # El formato del JSON original de OpenWeatherMap tiene una clave 'list' con el array de datos
    # Si es plano, asumimos que iteramos la lista directamente.
    events = data.get("list", data) if isinstance(data, dict) else data

    logger.info(f"🚀 Iniciando simulación: enviando {len(events)} eventos al topic '{TOPIC_NAME}'")

    count = 0
    try:
        for event in events:
            # Modificar el timestamp para que parezca "en tiempo real" (opcional para pruebas de windowing)
            # Aquí mantenemos el original para poder hacer joins limpios con la tabla fact_orders
            
            # Usar la ciudad o ID de estación como partition key (asegura orden por estación)
            key = str(event.get("city", {}).get("id", "patagonia"))
            
            # Formatear payload
            payload = {
                "dt": event.get("dt"),
                "dt_iso": event.get("dt_iso", datetime.utcfromtimestamp(event.get("dt")).isoformat() + "Z"),
                "city_name": event.get("city_name", "Patagonia"),
                "lat": event.get("lat", -41.81),
                "lon": event.get("lon", -68.91),
                "temp": event.get("main", {}).get("temp", event.get("temp")),
                "humidity": event.get("main", {}).get("humidity", event.get("humidity")),
                "wind_speed": event.get("wind", {}).get("speed", event.get("wind_speed")),
                "weather_main": event.get("weather", [{}])[0].get("main"),
                "weather_description": event.get("weather", [{}])[0].get("description"),
                "ingestion_timestamp": datetime.utcnow().isoformat() + "Z"  # Metadato de Kafka
            }

            future = producer.send(topic=TOPIC_NAME, key=key, value=payload)
            record_metadata = future.get(timeout=10)
            
            count += 1
            if count % 10 == 0:
                logger.info(f"  → Enviados {count} eventos... (Último offset: {record_metadata.offset})")

            time.sleep(delay_seconds)

    except KeyboardInterrupt:
        logger.info("\n🛑 Simulación detenida por el usuario.")
    except Exception as e:
        logger.error(f"❌ Error durante el streaming: {e}")
    finally:
        producer.flush()
        producer.close()
        logger.info(f"✅ Productor cerrado. Total eventos enviados: {count}")


if __name__ == "__main__":
    producer = create_producer()
    # Enviamos 2 eventos por segundo para ver flujo en tiempo real sin saturar
    simulate_stream(producer, DATA_FILE, delay_seconds=0.5)
