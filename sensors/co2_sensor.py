"""
CO2 Sensor Simulator
Simulates carbon dioxide concentration in parts per million (ppm).
Baseline outdoor ~415 ppm; indoor/urban can spike to 1000+ ppm.
"""
import os
import random
import time
from base_sensor import BaseSensor


class CO2Sensor(BaseSensor):
    """
    Simulates CO2 concentration (ppm) with random traffic-peak spikes.
    Normal range: 400–600 ppm  |  Alert threshold: > 800 ppm
    """

    BASE_MEAN = 420.0
    NOISE_STD = 15.0
    SPIKE_PROBABILITY = 0.05   # 5% chance of a traffic/event spike per reading
    SPIKE_MAGNITUDE = 350.0    # Additional ppm during spike

    def __init__(self, sensor_id: str, broker_host: str, **kwargs):
        super().__init__(
            sensor_id=sensor_id,
            sensor_type="co2",
            unit="ppm",
            broker_host=broker_host,
            **kwargs
        )
        self._spike_remaining = 0  # Persistence of a spike over N readings

    def generate_reading(self) -> float:
        """
        Generate CO2 reading. Spikes persist for 3–8 readings to simulate
        a real traffic event or industrial emission.
        """
        if self._spike_remaining > 0:
            self._spike_remaining -= 1
            spike = self.SPIKE_MAGNITUDE * (self._spike_remaining / 8.0)
        elif random.random() < self.SPIKE_PROBABILITY:
            self._spike_remaining = random.randint(3, 8)
            spike = self.SPIKE_MAGNITUDE
        else:
            spike = 0.0

        noise = random.gauss(0, self.NOISE_STD)
        value = self.BASE_MEAN + spike + noise
        return max(350.0, min(2000.0, value))


if __name__ == "__main__":
    sensor = CO2Sensor(
        sensor_id=os.getenv("SENSOR_ID", "co2_01"),
        broker_host=os.getenv("MQTT_HOST", "localhost"),
        broker_port=int(os.getenv("MQTT_PORT", "1883")),
        publish_interval=float(os.getenv("PUBLISH_INTERVAL", "2.0")),
        use_tls=os.getenv("USE_TLS", "false").lower() == "true",
        ca_cert=os.getenv("CA_CERT_PATH"),
        certfile=os.getenv("CERT_PATH"),
        keyfile=os.getenv("KEY_PATH"),
    )
    sensor.run()
