"""
get_latest.py — Lambda Function (API Gateway backed)
======================================================
Handles GET /latest — returns the most recent reading for each sensor.
Used by dashboards that need current values (e.g. gauge panels).

Response: JSON object keyed by sensor_type with latest reading.
"""
import json
import os
import logging
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

AWS_REGION     = os.getenv("AWS_REGION", "eu-west-1")
READINGS_TABLE = os.getenv("READINGS_TABLE", "iot_readings")

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table    = dynamodb.Table(READINGS_TABLE)

CORS_HEADERS = {
    "Content-Type":                "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}

SENSOR_IDS = ["temp_01", "hum_01", "co2_01", "pm25_01", "uv_01"]


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def get_latest_for_sensor(sensor_id: str) -> dict | None:
    """Get the most recent reading for a given sensor_id."""
    try:
        from boto3.dynamodb.conditions import Key
        response = table.query(
            KeyConditionExpression=Key("sensor_id").eq(sensor_id),
            ScanIndexForward=False,   # Most recent first
            Limit=1
        )
        items = response.get("Items", [])
        return items[0] if items else None
    except ClientError as e:
        logger.error(f"Failed to query latest for {sensor_id}: {e}")
        return None


def lambda_handler(event, context):
    """API Gateway Lambda handler for GET /latest."""
    if event.get("httpMethod") == "OPTIONS":
        return {"statusCode": 200, "headers": CORS_HEADERS, "body": ""}

    latest = {}
    for sensor_id in SENSOR_IDS:
        record = get_latest_for_sensor(sensor_id)
        if record:
            latest[record.get("sensor_type", sensor_id)] = record

    logger.info(f"Latest readings returned for {len(latest)} sensor types")
    return {
        "statusCode": 200,
        "headers": CORS_HEADERS,
        "body": json.dumps({"latest": latest, "sensor_count": len(latest)}, cls=DecimalEncoder),
    }
