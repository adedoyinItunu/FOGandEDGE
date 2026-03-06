"""
Humidity Sensor Simulator
Simulates relative humidity (%) with negative correlation to temperature.
"""
import os
import math
import random
import time
from base_sensor import BaseSensor


class HumiditySensor(BaseSensor):
    """
    Simulates relative humidity with inverse diurnal cycle (higher at night/morning)
    and Gaussian noise.
    Mean: 65%  |  Std Dev: 5%  |  Range: 10% to 100%
    """

    BASE_MEAN = 65.0
    DIURNAL_AMP = 12.0     # Humidity drops during warm afternoons
    NOISE_STD = 2.0

    def __init__(self, sensor_id: str, broker_host: str, **kwargs):
        super().__init__(
            sensor_id=sensor_id,
            sensor_type="humidity",
            unit="percent",
            broker_host=broker_host,
            **kwargs
        )

    def generate_reading(self) -> float:
        """
        Humidity is inversely correlated with temperature diurnal cycle.
        Phase-shifted by π to peak at night/morning.
        """
        hour_fraction = (time.time() % 86400) / 86400
        diurnal = self.DIURNAL_AMP * math.sin(2 * math.pi * (hour_fraction - 0.75))
        noise = random.gauss(0, self.NOISE_STD)
        value = self.BASE_MEAN + diurnal + noise
        return max(10.0, min(100.0, value))


if __name__ == "__main__":
    sensor = HumiditySensor(
        sensor_id=os.getenv("SENSOR_ID", "hum_01"),
        broker_host=os.getenv("MQTT_HOST", "localhost"),
        broker_port=int(os.getenv("MQTT_PORT", "1883")),
        publish_interval=float(os.getenv("PUBLISH_INTERVAL", "2.0")),
        use_tls=os.getenv("USE_TLS", "false").lower() == "true",
        ca_cert=os.getenv("CA_CERT_PATH"),
        certfile=os.getenv("CERT_PATH"),
        keyfile=os.getenv("KEY_PATH"),
    )
    sensor.run()
