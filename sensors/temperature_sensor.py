"""
Temperature Sensor Simulator
Simulates ambient air temperature in Celsius (realistic urban outdoor range).
"""
import os
import math
import random
from base_sensor import BaseSensor


class TemperatureSensor(BaseSensor):
    """
    Simulates outdoor ambient temperature with diurnal (day/night) variation
    and Gaussian noise.
    Mean: 15°C  |  Std Dev: 1.5°C  |  Range: -5°C to 40°C
    """

    BASE_MEAN = 15.0       # Baseline mean temperature (°C)
    DIURNAL_AMP = 5.0      # Amplitude of diurnal cycle (°C)
    NOISE_STD = 0.8        # Gaussian noise standard deviation

    def __init__(self, sensor_id: str, broker_host: str, **kwargs):
        super().__init__(
            sensor_id=sensor_id,
            sensor_type="temperature",
            unit="celsius",
            broker_host=broker_host,
            **kwargs
        )

    def generate_reading(self) -> float:
        """
        Generate temperature with sinusoidal diurnal cycle + Gaussian noise.
        Simulates warmer afternoons and cooler nights.
        """
        import time
        hour_fraction = (time.time() % 86400) / 86400  # 0.0 to 1.0 over 24h
        diurnal = self.DIURNAL_AMP * math.sin(2 * math.pi * (hour_fraction - 0.25))
        noise = random.gauss(0, self.NOISE_STD)
        value = self.BASE_MEAN + diurnal + noise
        return max(-5.0, min(45.0, value))  # Clamp to realistic range


if __name__ == "__main__":
    sensor = TemperatureSensor(
        sensor_id=os.getenv("SENSOR_ID", "temp_01"),
        broker_host=os.getenv("MQTT_HOST", "localhost"),
        broker_port=int(os.getenv("MQTT_PORT", "1883")),
        publish_interval=float(os.getenv("PUBLISH_INTERVAL", "2.0")),
        use_tls=os.getenv("USE_TLS", "false").lower() == "true",
        ca_cert=os.getenv("CA_CERT_PATH"),
        certfile=os.getenv("CERT_PATH"),
        keyfile=os.getenv("KEY_PATH"),
    )
    sensor.run()
