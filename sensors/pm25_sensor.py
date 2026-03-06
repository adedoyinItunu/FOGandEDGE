"""
PM2.5 Particulate Matter Sensor Simulator
Simulates fine particulate matter concentration (µg/m³).
WHO guideline: < 15 µg/m³ (24-hour mean). Alert: > 35 µg/m³.
"""
import os
import random
from base_sensor import BaseSensor


class PM25Sensor(BaseSensor):
    """
    Simulates PM2.5 fine particulate matter (µg/m³).
    Uses a log-normal distribution to simulate realistic skewed aerosol data.
    Normal: 8–20 µg/m³  |  Alert threshold: > 35 µg/m³
    """

    LOG_MEAN = 2.5      # ln(~12 µg/m³) — typical clean urban air
    LOG_STD = 0.4       # Log-normal std dev
    POLLUTION_PROB = 0.04   # Probability of a pollution episode per reading
    POLLUTION_BOOST = 40.0  # Additional µg/m³ during episode

    def __init__(self, sensor_id: str, broker_host: str, **kwargs):
        super().__init__(
            sensor_id=sensor_id,
            sensor_type="pm25",
            unit="ug_m3",
            broker_host=broker_host,
            **kwargs
        )
        self._episode_remaining = 0

    def generate_reading(self) -> float:
        """
        Log-normal base distribution (realistic for aerosols) with
        occasional pollution episodes.
        """
        base = random.lognormvariate(self.LOG_MEAN, self.LOG_STD)

        if self._episode_remaining > 0:
            self._episode_remaining -= 1
            boost = self.POLLUTION_BOOST * (self._episode_remaining / 10.0)
        elif random.random() < self.POLLUTION_PROB:
            self._episode_remaining = random.randint(5, 10)
            boost = self.POLLUTION_BOOST
        else:
            boost = 0.0

        value = base + boost
        return max(0.0, min(500.0, value))


if __name__ == "__main__":
    sensor = PM25Sensor(
        sensor_id=os.getenv("SENSOR_ID", "pm25_01"),
        broker_host=os.getenv("MQTT_HOST", "localhost"),
        broker_port=int(os.getenv("MQTT_PORT", "1883")),
        publish_interval=float(os.getenv("PUBLISH_INTERVAL", "2.0")),
        use_tls=os.getenv("USE_TLS", "false").lower() == "true",
        ca_cert=os.getenv("CA_CERT_PATH"),
        certfile=os.getenv("CERT_PATH"),
        keyfile=os.getenv("KEY_PATH"),
    )
    sensor.run()
