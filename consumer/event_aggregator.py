"""
Event Aggregator - Coordina eventos desde múltiples fuentes de Kafka.
Unifica datos de Open-Meteo, sensores IoT y video en un único flujo.
"""

import json
import logging
import os
import time
from kafka import KafkaConsumer, KafkaProducer
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from datadog import initialize, statsd


class EventAggregator:
    """
    Agrega eventos desde múltiples tópicos de Kafka.
    Coordina y enriquece eventos antes de enviarlos a un tópico unificado.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "kafka:9092",
        input_topics: list = None,
        output_topic: str = "aggregated-events",
        group_id: str = "event-aggregator"
    ):
        self.bootstrap_servers = bootstrap_servers
        self.input_topics = input_topics or ["sensores", "sensores-iot", "video-stream"]
        self.output_topic = output_topic
        self.group_id = group_id
        
        self.logger = logging.getLogger(__name__)
        self.event_counters = {topic: 0 for topic in self.input_topics}
        self.counter_lock = Lock()
        
        DATADOG_HOST = os.getenv("DATADOG_HOST", "datadog")
        DATADOG_PORT = int(os.getenv("DATADOG_PORT", "8125"))
        initialize(
            statsd_host=DATADOG_HOST,
            statsd_port=DATADOG_PORT
        )
        
        # Inicializar producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
    
    def start(self):
        """Inicia la agregación de eventos desde múltiples fuentes."""
        self.logger.info("🚀 Event Aggregator iniciado")
        self.logger.info("Tópicos de entrada: %s", self.input_topics)
        self.logger.info("Tópico de salida: %s", self.output_topic)
        
        # Crear un consumer para cada tópico
        with ThreadPoolExecutor(max_workers=len(self.input_topics)) as executor:
            for topic in self.input_topics:
                executor.submit(self._consume_topic, topic)
    
    def _consume_topic(self, topic: str):
        """Consume eventos de un tópico específico."""
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=f"{self.group_id}-{topic}",
            auto_offset_reset="latest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            session_timeout_ms=30000
        )
        
        self.logger.info("✅ Consumidor conectado a tópico: %s", topic)
        
        try:
            for message in consumer:
                    event = message.value
                    enriched_event = self._enrich_event(event, topic)
                    self._publish_event(enriched_event)
                    
                    if topic == "sensores":
                        statsd.increment("agregador.eventos.clima")
                    elif topic == "sensores-iot":
                        statsd.increment("agregador.eventos.sensores")
                    elif topic == "video-stream":
                        statsd.increment("agregador.eventos.video")
                    
                    with self.counter_lock:
                        self.event_counters[topic] += 1
                        total = sum(self.event_counters.values())
                        
                        if total % 100 == 0:
                            self.logger.info(
                                "📊 Eventos procesados: %s (Total: %d)",
                                self.event_counters, total
                            )
        except Exception as exc:
            self.logger.error("❌ Error en consumidor de %s: %s", topic, exc, exc_info=True)
    
    def _enrich_event(self, event: dict, source_topic: str) -> dict:
        """
        Enriquece un evento con metadatos de correlación.
        
        Args:
            event: Evento original
            source_topic: Tópico de origen
        
        Returns:
            Evento enriquecido
        """
        enriched = {
            **event,
            "aggregated_at": time.time(),
            "source_topic": source_topic,
            "event_type": self._classify_event(source_topic)
        }
        
        return enriched
    
    def _classify_event(self, topic: str) -> str:
        """Clasifica el tipo de evento según el tópico."""
        if topic == "sensores":
            return "weather"
        elif topic == "sensores-iot":
            return "sensor"
        elif topic == "video-stream":
            return "video"
        else:
            return "unknown"
    
    def _publish_event(self, event: dict):
        """Publica el evento enriquecido al tópico de salida."""
        try:
            self.producer.send(self.output_topic, event)
        except Exception as exc:
            self.logger.error("❌ Error publicando evento: %s", exc, exc_info=True)


def run_aggregator():
    """Función principal para ejecutar el aggregator."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )
    
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    
    aggregator = EventAggregator(
        bootstrap_servers=kafka_bootstrap,
        input_topics=[
            os.getenv("KAFKA_TOPIC", "sensores"),
            os.getenv("KAFKA_TOPIC_SENSORS", "sensores-iot"),
            os.getenv("KAFKA_TOPIC_VIDEO", "video-stream")
        ],
        output_topic=os.getenv("KAFKA_AGGREGATED_TOPIC", "aggregated-events")
    )
    
    try:
        aggregator.start()
    except KeyboardInterrupt:
        logging.info("Event Aggregator detenido")


if __name__ == "__main__":
    run_aggregator()
