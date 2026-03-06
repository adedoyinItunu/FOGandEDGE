"""
Base sensor class for all IoT sensor simulators.
All sensors inherit from this class.
"""
import json
import time
import random
import logging
from datetime import datetime, timezone
from abc import ABC, abstractmethod
import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")


class BaseSensor(ABC):
    """Abstract base class for all sensor simulators."""

    def __init__(self, sensor_id: str, sensor_type: str, unit: str,
                 broker_host: str, broker_port: int = 1883,
                 publish_interval: float = 2.0,
                 use_tls: bool = False, ca_cert: str = None,
                 certfile: str = None, keyfile: str = None):
        self.sensor_id = sensor_id
        self.sensor_type = sensor_type
        self.unit = unit
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.publish_interval = publish_interval
        self.topic = f"sensors/{sensor_type}/{sensor_id}"
        self.logger = logging.getLogger(f"Sensor.{sensor_id}")

        self.client = mqtt.Client(client_id=sensor_id)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect

        if use_tls and ca_cert:
            self.client.tls_set(ca_certs=ca_cert, certfile=certfile, keyfile=keyfile)

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.logger.info(f"Connected to broker at {self.broker_host}:{self.broker_port}")
        else:
            self.logger.error(f"Connection failed with code {rc}")

    def _on_disconnect(self, client, userdata, rc):
        self.logger.warning(f"Disconnected from broker (rc={rc})")

    @abstractmethod
    def generate_reading(self) -> float:
        """Generate a realistic sensor reading with Gaussian noise."""
        pass

    def build_payload(self, value: float) -> dict:
        """Build the standard JSON payload for a sensor reading."""
        return {
            "sensor_id": self.sensor_id,
            "sensor_type": self.sensor_type,
            "value": round(value, 3),
            "unit": self.unit,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "topic": self.topic
        }

    def connect(self):
        """Connect to MQTT broker."""
        self.client.connect(self.broker_host, self.broker_port, keepalive=60)
        self.client.loop_start()

    def disconnect(self):
        """Disconnect from MQTT broker."""
        self.client.loop_stop()
        self.client.disconnect()

    def run(self):
        """Main sensor loop: generate and publish readings continuously."""
        self.connect()
        self.logger.info(f"Starting sensor loop (interval={self.publish_interval}s, topic={self.topic})")
        try:
            while True:
                value = self.generate_reading()
                payload = self.build_payload(value)
                result = self.client.publish(self.topic, json.dumps(payload), qos=1)
                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    self.logger.info(f"Published: {self.sensor_type}={value} {self.unit}")
                else:
                    self.logger.warning(f"Publish failed (rc={result.rc})")
                time.sleep(self.publish_interval)
        except KeyboardInterrupt:
            self.logger.info("Sensor stopped by user.")
        finally:
            self.disconnect()
