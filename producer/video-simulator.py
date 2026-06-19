"""
Productor simulado de datos de video surveillance.
Genera metadatos de video como detecciones, movimiento, FPS, etc.
"""

import json
import time
import os
import logging
import random
import uuid

from datadog import initialize, statsd
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_VIDEO", "video-stream")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "30"))
DATADOG_HOST = os.getenv("DATADOG_HOST", "datadog")
DATADOG_PORT = int(os.getenv("DATADOG_PORT", "8125"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

initialize(
    statsd_host=DATADOG_HOST,
    statsd_port=DATADOG_PORT
)

while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        logging.info("✅ Kafka listo (Video Simulator)")
        break
    except Exception as exc:
        logging.warning("Esperando Kafka... %s", exc)
        time.sleep(5)

logging.info("Video Simulator iniciado")

# Simular cámaras distribuidas
cameras = {
    "CAM-001": {"location": "Entrada Principal", "zone": "A"},
    "CAM-002": {"location": "Perímetro Este", "zone": "B"},
    "CAM-003": {"location": "Depósito", "zone": "C"},
    "CAM-004": {"location": "Salida", "zone": "A"},
}

while True:
    try:
        camera_id = random.choice(list(cameras.keys()))
        camera_info = cameras[camera_id]
        
        # Simular detecciones con probabilidad variable
        has_motion = random.random() > 0.6
        detection_confidence = random.uniform(0.5, 0.99) if has_motion else 0
        num_objects = random.randint(0, 5) if has_motion else 0
        
        # Simular tipos de objetos detectados
        object_types = []
        if num_objects > 0:
            possible_types = ["person", "vehicle", "animal", "debris"]
            object_types = random.sample(possible_types, min(num_objects, len(possible_types)))
        
        frame_id = str(uuid.uuid4())[:8]
        event_id = uuid.uuid4().hex
        
        data = {
            "event_id": event_id,
            "camera_id": camera_id,
            "camera_location": camera_info["location"],
            "zone": camera_info["zone"],
            "frame_id": frame_id,
            "frame_number": random.randint(1000, 999999),
            "fps": round(random.uniform(24, 30), 2),
            "timestamp": time.time(),
            "has_motion": has_motion,
            "motion_percentage": round(random.uniform(0, 100) if has_motion else 0, 2),
            "detection_count": num_objects,
            "detected_objects": object_types,
            "detection_confidence": round(detection_confidence, 3),
            "frame_size": f"{1920}x{1080}",
            "encoding": "h264",
            "source": "video-simulator",
            "source_topic": KAFKA_TOPIC
        }

        producer.send(KAFKA_TOPIC, data)
        statsd.increment("produccion.video.enviados")
        statsd.gauge("video.fps", data["fps"])
        statsd.gauge("video.detecciones", data["detection_count"])
        statsd.gauge("video.confianza_deteccion", data["detection_confidence"])
        statsd.increment("video.movimiento.detectado", int(has_motion))
        
        motion_str = "MOVIMIENTO" if has_motion else "Sin actividad"
        logging.info("📹 CAM-%s event_id=%s [%s] %s - Objetos: %d, FPS: %.1f",
                 camera_id, event_id, camera_info["location"], motion_str, num_objects, data["fps"])

    except Exception as exc:
        logging.error("Error obteniendo datos de video: %s", exc, exc_info=True)

    time.sleep(POLL_SECONDS)
