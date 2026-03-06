"""
get_readings.py — Lambda Function (API Gateway backed)
========================================================
Handles GET /readings — returns sensor readings from DynamoDB.

Query parameters (all optional):
    sensor_id   Filter by specific sensor ID
    sensor_type Filter by sensor type (e.g. co2, temperature)
    from        ISO 8601 start timestamp (default: last 24h)
    to          ISO 8601 end timestamp (default: now)
    limit       Maximum records to return (default: 100, max: 500)

Response: JSON array of reading records, sorted by timestamp descending.
"""
import json
import os
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key, Attr
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


class DecimalEncoder(json.JSONEncoder):
    """JSON encoder that handles Decimal values from DynamoDB."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def parse_query_params(params: dict) -> dict:
    """Extract and validate query parameters."""
    now = datetime.now(timezone.utc)
    default_from = (now - timedelta(hours=24)).isoformat()
    default_to   = now.isoformat()

    return {
        "sensor_id":   params.get("sensor_id"),
        "sensor_type": params.get("sensor_type"),
        "from_ts":     params.get("from", default_from),
        "to_ts":       params.get("to", default_to),
        "limit":       min(int(params.get("limit", "100")), 500),
    }


def query_readings(filters: dict) -> list:
    """Query DynamoDB for sensor readings based on filters."""
    items = []

    try:
        if filters["sensor_id"]:
            # Query by partition key (sensor_id) + timestamp range
            response = table.query(
                KeyConditionExpression=(
                    Key("sensor_id").eq(filters["sensor_id"]) &
                    Key("timestamp").between(filters["from_ts"], filters["to_ts"])
                ),
                ScanIndexForward=False,  # descending timestamp
                Limit=filters["limit"]
            )
            items = response.get("Items", [])
        else:
            # Scan with filter expression (less efficient — acceptable for demo scale)
            filter_expr = (
                Attr("timestamp").between(filters["from_ts"], filters["to_ts"])
            )
            if filters["sensor_type"]:
                filter_expr = filter_expr & Attr("sensor_type").eq(filters["sensor_type"])

            response = table.scan(
                FilterExpression=filter_expr,
                Limit=filters["limit"]
            )
            items = response.get("Items", [])
            # Sort by timestamp descending
            items.sort(key=lambda x: x.get("timestamp", ""), reverse=True)

    except ClientError as e:
        logger.error(f"DynamoDB query failed: {e}")
        raise

    return items


def lambda_handler(event, context):
    """API Gateway Lambda handler for GET /readings."""
    if event.get("httpMethod") == "OPTIONS":
        return {"statusCode": 200, "headers": CORS_HEADERS, "body": ""}

    params = event.get("queryStringParameters") or {}

    try:
        filters = parse_query_params(params)
        items = query_readings(filters)
        logger.info(f"Returning {len(items)} readings (filters={filters})")
        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "count": len(items),
                "filters": filters,
                "readings": items
            }, cls=DecimalEncoder),
        }
    except ValueError as e:
        return {
            "statusCode": 400,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": f"Invalid parameter: {e}"}),
        }
    except Exception as e:
        logger.error(f"Internal error: {e}", exc_info=True)
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": "Internal server error"}),
        }
