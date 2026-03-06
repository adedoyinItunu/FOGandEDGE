"""
process_iot_data.py — Lambda Function
=======================================
Triggered by SQS event source mapping.
Parses fog node batch payloads, writes readings and alerts to DynamoDB,
and pushes custom metrics to CloudWatch for dashboarding.

DynamoDB Tables:
    iot_readings  — partition key: sensor_id (S), sort key: timestamp (S)
    iot_alerts    — partition key: alert_id (S), sort key: timestamp (S)

CloudWatch Metrics (namespace: FogEdgeIoT):
    SensorReading   — gauge value per sensor_type
    AnomalyCount    — count of anomalies per fog node dispatch
    FogDispatchLatency — seconds between reading and fog dispatch
"""
import json
import os
import uuid
import logging
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ─── AWS Clients ──────────────────────────────────────────────────────────────
AWS_REGION     = os.getenv("AWS_REGION", "eu-west-1")
READINGS_TABLE = os.getenv("READINGS_TABLE", "iot_readings")
ALERTS_TABLE   = os.getenv("ALERTS_TABLE", "iot_alerts")
CW_NAMESPACE   = os.getenv("CW_NAMESPACE", "FogEdgeIoT")

dynamodb   = boto3.resource("dynamodb", region_name=AWS_REGION)
cloudwatch = boto3.client("cloudwatch", region_name=AWS_REGION)

readings_table = dynamodb.Table(READINGS_TABLE)
alerts_table   = dynamodb.Table(ALERTS_TABLE)


# ─── Helper: DynamoDB float → Decimal conversion ──────────────────────────────

def floats_to_decimal(obj):
    """Recursively convert float values to Decimal for DynamoDB compatibility."""
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: floats_to_decimal(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [floats_to_decimal(i) for i in obj]
    return obj


# ─── Write to DynamoDB ────────────────────────────────────────────────────────

def write_reading(sensor: dict, fog_node_id: str, processed_at: str):
    """Write a sensor summary record to the iot_readings DynamoDB table."""
    item = {
        "sensor_id":      sensor["sensor_id"],
        "timestamp":      processed_at,
        "fog_node_id":    fog_node_id,
        "sensor_type":    sensor["sensor_type"],
        "unit":           sensor["unit"],
        "latest_value":   Decimal(str(sensor["latest_value"])) if sensor["latest_value"] is not None else Decimal("0"),
        "mean":           Decimal(str(sensor["statistics"]["mean"])) if sensor["statistics"]["mean"] else Decimal("0"),
        "std_dev":        Decimal(str(sensor["statistics"]["std_dev"])) if sensor["statistics"]["std_dev"] else Decimal("0"),
        "min_value":      Decimal(str(sensor["statistics"]["min"])) if sensor["statistics"]["min"] else Decimal("0"),
        "max_value":      Decimal(str(sensor["statistics"]["max"])) if sensor["statistics"]["max"] else Decimal("0"),
        "sample_count":   sensor["statistics"]["count"],
        "anomaly_count":  sensor["anomaly_count"],
        "ingested_at":    datetime.now(timezone.utc).isoformat(),
    }
    try:
        readings_table.put_item(Item=item)
        logger.info(f"Wrote reading: {sensor['sensor_id']} @ {processed_at}")
    except ClientError as e:
        logger.error(f"DynamoDB write_reading failed: {e}")
        raise


def write_alert(anomaly: dict, sensor: dict, fog_node_id: str):
    """Write an anomaly alert record to the iot_alerts DynamoDB table."""
    alert_id = f"{sensor['sensor_id']}_{uuid.uuid4().hex[:8]}"
    item = {
        "alert_id":      alert_id,
        "timestamp":     anomaly["timestamp"],
        "sensor_id":     sensor["sensor_id"],
        "sensor_type":   sensor["sensor_type"],
        "fog_node_id":   fog_node_id,
        "unit":          sensor["unit"],
        "anomaly_value": Decimal(str(anomaly["value"])),
        "z_score":       Decimal(str(anomaly["z_score"])),
        "direction":     anomaly["direction"],
        "window_mean":   Decimal(str(sensor["statistics"]["mean"])) if sensor["statistics"]["mean"] else Decimal("0"),
        "ingested_at":   datetime.now(timezone.utc).isoformat(),
    }
    try:
        alerts_table.put_item(Item=item)
        logger.info(f"Wrote alert: {alert_id} ({sensor['sensor_type']}, z={anomaly['z_score']})")
    except ClientError as e:
        logger.error(f"DynamoDB write_alert failed: {e}")
        raise


# ─── Push CloudWatch Metrics ──────────────────────────────────────────────────

def push_cloudwatch_metrics(sensor: dict, fog_node_id: str):
    """
    Push CloudWatch custom metrics for a sensor reading.
    These are what power the CloudWatch dashboard panels.
    """
    ts = datetime.now(timezone.utc)
    metric_data = []

    # Latest value gauge per sensor type
    if sensor["latest_value"] is not None:
        metric_data.append({
            "MetricName": "SensorReading",
            "Dimensions": [
                {"Name": "SensorType", "Value": sensor["sensor_type"]},
                {"Name": "SensorId",   "Value": sensor["sensor_id"]},
                {"Name": "FogNode",    "Value": fog_node_id},
            ],
            "Timestamp": ts,
            "Value": float(sensor["latest_value"]),
            "Unit": "None",
        })

    # Rolling mean
    if sensor["statistics"]["mean"] is not None:
        metric_data.append({
            "MetricName": "SensorRollingMean",
            "Dimensions": [
                {"Name": "SensorType", "Value": sensor["sensor_type"]},
                {"Name": "SensorId",   "Value": sensor["sensor_id"]},
            ],
            "Timestamp": ts,
            "Value": float(sensor["statistics"]["mean"]),
            "Unit": "None",
        })

    # Anomaly count per sensor
    metric_data.append({
        "MetricName": "AnomalyCount",
        "Dimensions": [
            {"Name": "SensorType", "Value": sensor["sensor_type"]},
            {"Name": "SensorId",   "Value": sensor["sensor_id"]},
            {"Name": "FogNode",    "Value": fog_node_id},
        ],
        "Timestamp": ts,
        "Value": float(sensor["anomaly_count"]),
        "Unit": "Count",
    })

    try:
        cloudwatch.put_metric_data(
            Namespace=CW_NAMESPACE,
            MetricData=metric_data
        )
    except ClientError as e:
        logger.warning(f"CloudWatch metric push failed (non-fatal): {e}")


# ─── Lambda Handler ───────────────────────────────────────────────────────────

def lambda_handler(event, context):
    """
    Main Lambda entry point. Processes a batch of SQS records.
    Each SQS record body is a fog node dispatch payload.
    Returns batch item failures so SQS can retry failed messages.
    """
    batch_item_failures = []

    for record in event.get("Records", []):
        message_id = record["messageId"]
        try:
            body = json.loads(record["body"])
            fog_node_id  = body["fog_node_id"]
            processed_at = body["processed_at"]
            sensors      = body["sensors"]

            logger.info(
                f"Processing fog dispatch from {fog_node_id}: "
                f"{len(sensors)} sensors, {body['total_anomalies']} anomalies"
            )

            for sensor in sensors:
                # Write aggregated reading to DynamoDB
                write_reading(sensor, fog_node_id, processed_at)

                # Write any anomaly alerts to DynamoDB
                for anomaly in sensor.get("anomalies", []):
                    write_alert(anomaly, sensor, fog_node_id)

                # Push metrics to CloudWatch for dashboard
                push_cloudwatch_metrics(sensor, fog_node_id)

        except Exception as e:
            logger.error(f"Failed to process record {message_id}: {e}", exc_info=True)
            batch_item_failures.append({"itemIdentifier": message_id})

    successful = len(event.get("Records", [])) - len(batch_item_failures)
    logger.info(f"Processed {successful} records successfully, {len(batch_item_failures)} failures.")

    # Return partial batch failures so SQS retries only failed messages
    return {"batchItemFailures": batch_item_failures}
