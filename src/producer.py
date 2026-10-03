import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

TOPIC_NAME = "eventos"
DIRECCIONES_VIENTO = ["N", "NE", "E", "SE", "S", "SO", "O", "NO"]
RADARES = ["RADAR_NORTE", "RADAR_CENTRO", "RADAR_SUR"]

def serializador_json(dato):
    return json.dumps(dato).encode("utf-8")

def iniciar_productor():
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=serializador_json
    )
    print(f"[*] Productor iniciado. Transmitiendo eventos a topic '{TOPIC_NAME}' cada 1s...")

    try:
        while True:
            evento = {
                "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "radar_id": random.choice(RADARES),
                "radar_azimuth_deg": round(random.uniform(0.0, 359.9), 1),
                "co2_ppm": round(random.gauss(420, 30), 2),
                "wind_speed_ms": round(random.uniform(1.0, 25.0), 2),
                "wind_direction": random.choice(DIRECCIONES_VIENTO)
            }
            producer.send(TOPIC_NAME, value=evento)
            producer.flush()
            print(f"[ENVIADO] {evento}")
            time.sleep(1)
    except KeyboardInterrupt:
        print("Productor detenido por el usuario.")
    finally:
        producer.close()

if __name__ == "__main__":
    iniciar_productor()
