"""
test_fog_node.py — Unit Tests for Fog Node Processing Logic
=============================================================
Tests the core fog node functions: windowing, statistics, anomaly detection,
payload building, and SQS dispatch (mocked).

Run with:
    pip install pytest moto boto3
    pytest tests/test_fog_node.py -v
"""
import sys
import os
import json
import pytest
from collections import deque
from unittest.mock import patch, MagicMock

# Add fog directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "fog"))

# Patch boto3 before importing fog_node to prevent real AWS calls
with patch.dict(os.environ, {
    "SQS_QUEUE_URL": "https://sqs.eu-west-1.amazonaws.com/123456789/fec-iot-queue",
    "AWS_REGION": "eu-west-1",
    "FOG_NODE_ID": "test_fog_node",
    "DISPATCH_INTERVAL": "30",
    "WINDOW_SIZE": "10",
    "ANOMALY_SIGMA": "2.0",
}):
    with patch("boto3.client", return_value=MagicMock()):
        import fog_node


# ─── Tests: compute_window_stats ─────────────────────────────────────────────

class TestComputeWindowStats:

    def test_empty_window_returns_none_mean(self):
        result = fog_node.compute_window_stats([])
        assert result["mean"] is None
        assert result["count"] == 0

    def test_single_value_returns_correct_mean(self):
        result = fog_node.compute_window_stats([42.0])
        assert result["mean"] == 42.0
        assert result["count"] == 1
        assert result["std_dev"] == 0.0

    def test_multiple_values_correct_statistics(self):
        values = [10.0, 20.0, 30.0]
        result = fog_node.compute_window_stats(values)
        assert result["mean"] == 20.0
        assert result["min"] == 10.0
        assert result["max"] == 30.0
        assert result["count"] == 3
        assert result["std_dev"] > 0

    def test_values_are_rounded_to_3dp(self):
        values = [1.11111, 2.22222, 3.33333]
        result = fog_node.compute_window_stats(values)
        # Mean should be rounded to 3 decimal places
        assert len(str(result["mean"]).split(".")[-1]) <= 3

    def test_single_spike_in_stable_window(self):
        # Most values around 20, one spike
        values = [20.0] * 9 + [100.0]
        result = fog_node.compute_window_stats(values)
        assert result["max"] == 100.0
        assert result["min"] == 20.0
        assert result["count"] == 10


# ─── Tests: detect_anomalies ──────────────────────────────────────────────────

class TestDetectAnomalies:

    def test_no_anomalies_in_stable_window(self):
        values = [20.0, 20.1, 19.9, 20.2, 20.0, 20.1, 19.8, 20.3, 20.0, 20.1]
        ts     = [f"2026-03-01T{i:02d}:00:00Z" for i in range(len(values))]
        import statistics
        mean    = statistics.mean(values)
        std_dev = statistics.stdev(values)
        result = fog_node.detect_anomalies(values, ts, mean, std_dev, sigma_threshold=2.0)
        assert result == []

    def test_spike_detected_as_anomaly(self):
        # 9 stable values around 20 + one major spike
        values = [20.0] * 9 + [80.0]
        ts     = [f"2026-03-01T{i:02d}:00:00Z" for i in range(10)]
        import statistics
        mean    = statistics.mean(values)
        std_dev = statistics.stdev(values)
        anomalies = fog_node.detect_anomalies(values, ts, mean, std_dev, sigma_threshold=2.0)
        assert len(anomalies) == 1
        assert anomalies[0]["direction"] == "high"
        assert anomalies[0]["z_score"] > 2.0

    def test_low_anomaly_detected(self):
        values = [20.0] * 9 + [-40.0]
        ts     = [f"2026-03-01T{i:02d}:00:00Z" for i in range(10)]
        import statistics
        mean    = statistics.mean(values)
        std_dev = statistics.stdev(values)
        anomalies = fog_node.detect_anomalies(values, ts, mean, std_dev, sigma_threshold=2.0)
        assert len(anomalies) == 1
        assert anomalies[0]["direction"] == "low"

    def test_zero_std_dev_returns_no_anomalies(self):
        # All identical values → std_dev = 0, cannot compute z-score
        values = [15.0] * 10
        ts     = [f"t{i}" for i in range(10)]
        result = fog_node.detect_anomalies(values, ts, mean=15.0, std_dev=0.0, sigma_threshold=2.0)
        assert result == []

    def test_sigma_threshold_controls_sensitivity(self):
        values = [20.0] * 9 + [30.0]
        ts     = [f"t{i}" for i in range(10)]
        import statistics
        mean    = statistics.mean(values)
        std_dev = statistics.stdev(values)

        # With high sigma threshold, may not detect
        strict_anomalies = fog_node.detect_anomalies(values, ts, mean, std_dev, sigma_threshold=5.0)
        # With low sigma threshold, should detect
        loose_anomalies  = fog_node.detect_anomalies(values, ts, mean, std_dev, sigma_threshold=1.0)
        assert len(loose_anomalies) >= len(strict_anomalies)


# ─── Tests: build_batch_payload ──────────────────────────────────────────────

class TestBuildBatchPayload:

    def _mock_processed_sensors(self):
        return [
            {
                "sensor_id": "co2_01",
                "sensor_type": "co2",
                "unit": "ppm",
                "statistics": {"mean": 420.0, "std_dev": 10.0, "min": 400.0, "max": 450.0, "count": 10},
                "anomalies": [],
                "anomaly_count": 0,
                "latest_value": 425.0,
                "latest_timestamp": "2026-03-01T12:00:00Z",
            }
        ]

    def test_payload_has_required_fields(self):
        sensors = self._mock_processed_sensors()
        payload = fog_node.build_batch_payload(sensors)
        for field in ["fog_node_id", "processed_at", "sensor_count", "sensors", "total_anomalies"]:
            assert field in payload, f"Missing field: {field}"

    def test_sensor_count_correct(self):
        sensors = self._mock_processed_sensors()
        payload = fog_node.build_batch_payload(sensors)
        assert payload["sensor_count"] == 1

    def test_total_anomalies_summed_correctly(self):
        sensors = self._mock_processed_sensors()
        sensors[0]["anomaly_count"] = 3
        payload = fog_node.build_batch_payload(sensors)
        assert payload["total_anomalies"] == 3

    def test_fog_node_id_matches_env(self):
        payload = fog_node.build_batch_payload([])
        assert payload["fog_node_id"] == "test_fog_node"


# ─── Tests: on_message (MQTT handler) ────────────────────────────────────────

class TestOnMessage:

    def setup_method(self):
        """Clear sensor windows before each test."""
        fog_node.sensor_windows.clear()
        fog_node.sensor_metadata.clear()

    def _make_mqtt_message(self, payload: dict):
        """Create a mock MQTT message."""
        msg = MagicMock()
        msg.payload = json.dumps(payload).encode("utf-8")
        msg.topic   = f"sensors/{payload['sensor_type']}/{payload['sensor_id']}"
        return msg

    def test_valid_message_added_to_window(self):
        msg = self._make_mqtt_message({
            "sensor_id": "temp_01", "sensor_type": "temperature",
            "value": 18.5, "unit": "celsius",
            "timestamp": "2026-03-01T12:00:00Z", "topic": "sensors/temperature/temp_01"
        })
        fog_node.on_message(None, None, msg)
        assert "temp_01" in fog_node.sensor_windows
        assert len(fog_node.sensor_windows["temp_01"]) == 1
        assert fog_node.sensor_windows["temp_01"][0][1] == 18.5

    def test_multiple_messages_build_window(self):
        for i in range(5):
            msg = self._make_mqtt_message({
                "sensor_id": "co2_01", "sensor_type": "co2",
                "value": 400.0 + i, "unit": "ppm",
                "timestamp": f"2026-03-01T12:0{i}:00Z", "topic": "sensors/co2/co2_01"
            })
            fog_node.on_message(None, None, msg)
        assert len(fog_node.sensor_windows["co2_01"]) == 5

    def test_malformed_json_does_not_crash(self):
        msg = MagicMock()
        msg.payload = b"not valid json {"
        msg.topic   = "sensors/temperature/temp_01"
        # Should log warning but not raise
        fog_node.on_message(None, None, msg)

    def test_window_maxlen_enforced(self):
        """Verify deque maxlen (WINDOW_SIZE=10) is respected."""
        for i in range(25):
            msg = self._make_mqtt_message({
                "sensor_id": "pm25_01", "sensor_type": "pm25",
                "value": float(i), "unit": "ug_m3",
                "timestamp": f"2026-03-01T{i:02d}:00:00Z", "topic": "sensors/pm25/pm25_01"
            })
            fog_node.on_message(None, None, msg)
        # Window should not exceed WINDOW_SIZE (10)
        assert len(fog_node.sensor_windows["pm25_01"]) <= fog_node.WINDOW_SIZE


# ─── Tests: Sensor generators ────────────────────────────────────────────────

class TestSensorGenerators:

    def _import_sensors(self):
        sensors_dir = os.path.join(os.path.dirname(__file__), "..", "sensors")
        sys.path.insert(0, sensors_dir)

    def test_co2_sensor_value_in_range(self):
        self._import_sensors()
        from co2_sensor import CO2Sensor
        with patch("paho.mqtt.client.Client"):
            s = CO2Sensor("co2_test", broker_host="localhost")
            for _ in range(50):
                val = s.generate_reading()
                assert 350.0 <= val <= 2000.0, f"CO2 out of range: {val}"

    def test_humidity_sensor_value_in_range(self):
        self._import_sensors()
        from humidity_sensor import HumiditySensor
        with patch("paho.mqtt.client.Client"):
            s = HumiditySensor("hum_test", broker_host="localhost")
            for _ in range(50):
                val = s.generate_reading()
                assert 10.0 <= val <= 100.0, f"Humidity out of range: {val}"

    def test_pm25_sensor_value_non_negative(self):
        self._import_sensors()
        from pm25_sensor import PM25Sensor
        with patch("paho.mqtt.client.Client"):
            s = PM25Sensor("pm25_test", broker_host="localhost")
            for _ in range(50):
                val = s.generate_reading()
                assert val >= 0.0, f"PM2.5 negative: {val}"

    def test_uv_sensor_value_in_range(self):
        self._import_sensors()
        from uv_index_sensor import UVIndexSensor
        with patch("paho.mqtt.client.Client"):
            s = UVIndexSensor("uv_test", broker_host="localhost")
            for _ in range(50):
                val = s.generate_reading()
                assert 0.0 <= val <= 11.0, f"UV index out of range: {val}"

    def test_temperature_sensor_value_in_range(self):
        self._import_sensors()
        from temperature_sensor import TemperatureSensor
        with patch("paho.mqtt.client.Client"):
            s = TemperatureSensor("temp_test", broker_host="localhost")
            for _ in range(50):
                val = s.generate_reading()
                assert -5.0 <= val <= 45.0, f"Temperature out of range: {val}"
