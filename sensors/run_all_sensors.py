"""
run_all_sensors.py
Launches all 5 sensor simulators as separate threads.
Each sensor reads its configuration from environment variables.

Usage:
    python run_all_sensors.py

Environment variables (all optional, have defaults):
    MQTT_HOST           MQTT broker hostname (default: localhost)
    MQTT_PORT           MQTT broker port (default: 1883)
    PUBLISH_INTERVAL    Seconds between readings (default: 2.0)
    USE_TLS             Use TLS for AWS IoT Core (default: false)
    CA_CERT_PATH        Path to root CA certificate (for TLS)
    CERT_PATH           Path to device certificate (for TLS)
    KEY_PATH            Path to private key (for TLS)
"""
import os
import sys
import threading
import logging

# Add sensors directory to path so relative imports work
sys.path.insert(0, os.path.dirname(__file__))

from temperature_sensor import TemperatureSensor
from humidity_sensor import HumiditySensor
from co2_sensor import CO2Sensor
from pm25_sensor import PM25Sensor
from uv_index_sensor import UVIndexSensor

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
logger = logging.getLogger("SensorLauncher")


def build_common_kwargs() -> dict:
    """Read shared configuration from environment variables."""
    return {
        "broker_host": os.getenv("MQTT_HOST", "localhost"),
        "broker_port": int(os.getenv("MQTT_PORT", "1883")),
        "publish_interval": float(os.getenv("PUBLISH_INTERVAL", "2.0")),
        "use_tls": os.getenv("USE_TLS", "false").lower() == "true",
        "ca_cert": os.getenv("CA_CERT_PATH"),
        "certfile": os.getenv("CERT_PATH"),
        "keyfile": os.getenv("KEY_PATH"),
    }


def launch_sensor(sensor_class, sensor_id: str, kwargs: dict):
    """Thread target: instantiate and run a sensor."""
    try:
        sensor = sensor_class(sensor_id=sensor_id, **kwargs)
        sensor.run()
    except Exception as e:
        logger.error(f"Sensor {sensor_id} crashed: {e}", exc_info=True)


def main():
    common = build_common_kwargs()

    sensor_configs = [
        (TemperatureSensor, "temp_01"),
        (HumiditySensor,    "hum_01"),
        (CO2Sensor,         "co2_01"),
        (PM25Sensor,        "pm25_01"),
        (UVIndexSensor,     "uv_01"),
    ]

    threads = []
    for sensor_class, sensor_id in sensor_configs:
        t = threading.Thread(
            target=launch_sensor,
            args=(sensor_class, sensor_id, common),
            name=f"Thread-{sensor_id}",
            daemon=True
        )
        threads.append(t)

    logger.info(f"Launching {len(threads)} sensor simulators...")
    logger.info(f"MQTT broker: {common['broker_host']}:{common['broker_port']}")
    logger.info(f"Publish interval: {common['publish_interval']}s")

    for t in threads:
        t.start()

    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        logger.info("Shutting down all sensors...")


if __name__ == "__main__":
    main()
