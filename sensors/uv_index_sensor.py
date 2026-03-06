"""
UV Index Sensor Simulator
Simulates the UV Index (dimensionless, 0–11+) with solar elevation modelling.
WHO scale: 0-2 Low, 3-5 Moderate, 6-7 High, 8-10 Very High, 11+ Extreme.
"""
import os
import math
import random
import time
from base_sensor import BaseSensor


class UVIndexSensor(BaseSensor):
    """
    Simulates UV Index using a solar-arc model: zero at night,
    peaks around solar noon (13:00 local), with cloud noise.
    Peak UV: ~7 (typical mid-latitude summer)  |  Alert: > 6
    """

    PEAK_UV = 7.0          # Maximum UV index at solar noon
    CLOUD_NOISE_STD = 0.3  # Gaussian noise from cloud cover variation

    def __init__(self, sensor_id: str, broker_host: str, **kwargs):
        super().__init__(
            sensor_id=sensor_id,
            sensor_type="uv_index",
            unit="uvi",
            broker_host=broker_host,
            **kwargs
        )

    def generate_reading(self) -> float:
        """
        UV index follows a half-sine curve peaking at 13:00 (solar noon).
        Zero during nighttime hours (before 6am and after 20:00).
        """
        hour_fraction = (time.time() % 86400) / 86400  # 0.0–1.0
        hour = hour_fraction * 24.0  # Actual hour (0–24)

        # Solar window: 6am to 20pm (0.25 to 0.833 of day)
        solar_start = 6.0
        solar_end = 20.0
        solar_noon = 13.0

        if solar_start <= hour <= solar_end:
            # Half-sine peaking at solar noon
            solar_angle = math.pi * (hour - solar_start) / (solar_end - solar_start)
            uv = self.PEAK_UV * math.sin(solar_angle)
        else:
            uv = 0.0

        noise = random.gauss(0, self.CLOUD_NOISE_STD) if uv > 0 else 0
        value = uv + noise
        return max(0.0, min(11.0, value))


if __name__ == "__main__":
    sensor = UVIndexSensor(
        sensor_id=os.getenv("SENSOR_ID", "uv_01"),
        broker_host=os.getenv("MQTT_HOST", "localhost"),
        broker_port=int(os.getenv("MQTT_PORT", "1883")),
        publish_interval=float(os.getenv("PUBLISH_INTERVAL", "2.0")),
        use_tls=os.getenv("USE_TLS", "false").lower() == "true",
        ca_cert=os.getenv("CA_CERT_PATH"),
        certfile=os.getenv("CERT_PATH"),
        keyfile=os.getenv("KEY_PATH"),
    )
    sensor.run()
