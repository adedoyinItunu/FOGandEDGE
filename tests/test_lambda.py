"""
test_lambda.py — Unit Tests for Lambda Functions
==================================================
Tests Lambda handlers with mocked DynamoDB and CloudWatch clients.

Run with:
    pip install pytest moto boto3
    pytest tests/test_lambda.py -v
"""
import sys
import os
import json
import pytest
from decimal import Decimal
from unittest.mock import patch, MagicMock, call
from datetime import datetime, timezone

# Add lambda_functions directory to path
LAMBDA_DIR = os.path.join(os.path.dirname(__file__), "..", "backend", "lambda_functions")
sys.path.insert(0, LAMBDA_DIR)


# ─── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_fog_payload():
    """A realistic fog node batch payload for testing."""
    return {
        "fog_node_id": "fog_node_01",
        "processed_at": "2026-03-17T12:00:00+00:00",
        "dispatch_window_seconds": 30,
        "window_size": 20,
        "anomaly_sigma_threshold": 2.0,
        "sensor_count": 2,
        "total_anomalies": 1,
        "sensors": [
            {
                "sensor_id": "co2_01",
                "sensor_type": "co2",
                "unit": "ppm",
                "statistics": {"mean": 420.0, "std_dev": 10.0, "min": 400.0, "max": 750.0, "count": 15},
                "anomalies": [
                    {"timestamp": "2026-03-17T11:59:00Z", "value": 750.0, "z_score": 3.3, "direction": "high"}
                ],
                "anomaly_count": 1,
                "latest_value": 425.0,
                "latest_timestamp": "2026-03-17T11:59:30Z",
            },
            {
                "sensor_id": "temp_01",
                "sensor_type": "temperature",
                "unit": "celsius",
                "statistics": {"mean": 18.0, "std_dev": 1.0, "min": 16.0, "max": 20.0, "count": 15},
                "anomalies": [],
                "anomaly_count": 0,
                "latest_value": 18.5,
                "latest_timestamp": "2026-03-17T11:59:30Z",
            }
        ]
    }


@pytest.fixture
def sqs_event(sample_fog_payload):
    """Mock SQS event wrapping the fog payload."""
    return {
        "Records": [
            {
                "messageId": "abc-123",
                "receiptHandle": "handle-001",
                "body": json.dumps(sample_fog_payload),
                "attributes": {},
                "messageAttributes": {},
                "md5OfBody": "fakeMD5",
                "eventSource": "aws:sqs",
                "awsRegion": "eu-west-1",
            }
        ]
    }


# ─── Tests: process_iot_data Lambda ──────────────────────────────────────────

class TestProcessIoTDataLambda:

    def test_successful_processing_returns_no_failures(self, sqs_event):
        mock_table    = MagicMock()
        mock_cw       = MagicMock()

        with patch.dict(os.environ, {
            "AWS_REGION": "eu-west-1",
            "READINGS_TABLE": "iot_readings",
            "ALERTS_TABLE": "iot_alerts",
            "CW_NAMESPACE": "FogEdgeIoT",
        }):
            with patch("boto3.resource") as mock_resource, \
                 patch("boto3.client") as mock_client:
                mock_resource.return_value.Table.return_value = mock_table
                mock_client.return_value = mock_cw

                import importlib
                import process_iot_data
                importlib.reload(process_iot_data)

                result = process_iot_data.lambda_handler(sqs_event, None)

        assert result["batchItemFailures"] == []

    def test_malformed_record_returned_as_failure(self):
        bad_event = {
            "Records": [{
                "messageId": "bad-msg-001",
                "receiptHandle": "handle",
                "body": "this is not valid json {{{",
                "attributes": {}, "messageAttributes": {},
                "md5OfBody": "", "eventSource": "aws:sqs", "awsRegion": "eu-west-1"
            }]
        }
        with patch.dict(os.environ, {
            "AWS_REGION": "eu-west-1",
            "READINGS_TABLE": "iot_readings",
            "ALERTS_TABLE": "iot_alerts",
            "CW_NAMESPACE": "FogEdgeIoT",
        }):
            with patch("boto3.resource"), patch("boto3.client"):
                import importlib
                import process_iot_data
                importlib.reload(process_iot_data)
                result = process_iot_data.lambda_handler(bad_event, None)

        assert len(result["batchItemFailures"]) == 1
        assert result["batchItemFailures"][0]["itemIdentifier"] == "bad-msg-001"

    def test_floats_to_decimal_conversion(self):
        """Ensure float→Decimal conversion works for DynamoDB."""
        with patch.dict(os.environ, {"AWS_REGION": "eu-west-1",
                                      "READINGS_TABLE": "t", "ALERTS_TABLE": "t",
                                      "CW_NAMESPACE": "ns"}):
            with patch("boto3.resource"), patch("boto3.client"):
                import importlib
                import process_iot_data
                importlib.reload(process_iot_data)

        result = process_iot_data.floats_to_decimal({"value": 3.14, "nested": {"x": 2.71}})
        assert isinstance(result["value"], Decimal)
        assert isinstance(result["nested"]["x"], Decimal)
        assert float(result["value"]) == pytest.approx(3.14)


# ─── Tests: get_readings Lambda ──────────────────────────────────────────────

class TestGetReadingsLambda:

    def test_returns_200_with_items(self):
        api_event = {
            "httpMethod": "GET",
            "queryStringParameters": {"sensor_type": "co2"},
            "headers": {}
        }
        mock_table = MagicMock()
        mock_table.scan.return_value = {
            "Items": [
                {"sensor_id": "co2_01", "timestamp": "2026-03-17T12:00:00Z",
                 "sensor_type": "co2", "latest_value": Decimal("420.0"), "unit": "ppm"}
            ]
        }

        with patch.dict(os.environ, {"AWS_REGION": "eu-west-1",
                                      "READINGS_TABLE": "iot_readings",
                                      "ALERTS_TABLE": "iot_alerts"}):
            with patch("boto3.resource") as mock_res:
                mock_res.return_value.Table.return_value = mock_table
                import importlib
                import get_readings
                importlib.reload(get_readings)
                result = get_readings.lambda_handler(api_event, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert "readings" in body
        assert body["count"] >= 0

    def test_options_request_returns_200_cors(self):
        event = {"httpMethod": "OPTIONS", "queryStringParameters": {}}
        with patch.dict(os.environ, {"AWS_REGION": "eu-west-1",
                                      "READINGS_TABLE": "t", "ALERTS_TABLE": "t"}):
            with patch("boto3.resource"):
                import importlib
                import get_readings
                importlib.reload(get_readings)
                result = get_readings.lambda_handler(event, None)
        assert result["statusCode"] == 200
        assert "Access-Control-Allow-Origin" in result["headers"]


# ─── Tests: parse_query_params ────────────────────────────────────────────────

class TestParseQueryParams:

    def test_defaults_set_when_no_params(self):
        with patch.dict(os.environ, {"AWS_REGION": "eu-west-1",
                                      "READINGS_TABLE": "t", "ALERTS_TABLE": "t"}):
            with patch("boto3.resource"):
                import importlib
                import get_readings
                importlib.reload(get_readings)

        result = get_readings.parse_query_params({})
        assert result["sensor_id"] is None
        assert result["sensor_type"] is None
        assert result["limit"] == 100

    def test_limit_capped_at_500(self):
        with patch.dict(os.environ, {"AWS_REGION": "eu-west-1",
                                      "READINGS_TABLE": "t", "ALERTS_TABLE": "t"}):
            with patch("boto3.resource"):
                import importlib
                import get_readings
                importlib.reload(get_readings)

        result = get_readings.parse_query_params({"limit": "9999"})
        assert result["limit"] == 500
