"""
get_alerts.py — Lambda Function (API Gateway backed)
======================================================
Handles GET /alerts — returns anomaly alerts from DynamoDB.

Query parameters (all optional):
    sensor_type Filter by sensor type
    direction   Filter by anomaly direction: 'high' or 'low'
    from        ISO 8601 start timestamp (default: last 24h)
    limit       Maximum records to return (default: 50, max: 200)

Response: JSON array of alert records, sorted by timestamp descending.
"""
import json
import os
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

AWS_REGION   = os.getenv("AWS_REGION", "eu-west-1")
ALERTS_TABLE = os.getenv("ALERTS_TABLE", "iot_alerts")

dynamodb     = boto3.resource("dynamodb", region_name=AWS_REGION)
alerts_table = dynamodb.Table(ALERTS_TABLE)

CORS_HEADERS = {
    "Content-Type":                "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def lambda_handler(event, context):
    """API Gateway Lambda handler for GET /alerts."""
    if event.get("httpMethod") == "OPTIONS":
        return {"statusCode": 200, "headers": CORS_HEADERS, "body": ""}

    params = event.get("queryStringParameters") or {}
    now = datetime.now(timezone.utc)
    from_ts     = params.get("from", (now - timedelta(hours=24)).isoformat())
    sensor_type = params.get("sensor_type")
    direction   = params.get("direction")
    limit       = min(int(params.get("limit", "50")), 200)

    try:
        filter_expr = Attr("timestamp").gte(from_ts)
        if sensor_type:
            filter_expr = filter_expr & Attr("sensor_type").eq(sensor_type)
        if direction:
            filter_expr = filter_expr & Attr("direction").eq(direction)

        response = alerts_table.scan(
            FilterExpression=filter_expr,
            Limit=limit
        )
        items = response.get("Items", [])
        items.sort(key=lambda x: x.get("timestamp", ""), reverse=True)

        logger.info(f"Returning {len(items)} alerts")
        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps({"count": len(items), "alerts": items}, cls=DecimalEncoder),
        }
    except ClientError as e:
        logger.error(f"DynamoDB scan failed: {e}")
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": "Database error"}),
        }
