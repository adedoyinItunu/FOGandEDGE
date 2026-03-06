#!/usr/bin/env python3
"""
setup_aws_infra.py — AWS Infrastructure Provisioning Script
=============================================================
Creates all required AWS resources for the FEC IoT project.
Run this ONCE before deploying the application.

Resources created:
    - DynamoDB tables: iot_readings, iot_alerts
    - SQS queue: fec-iot-queue (with Dead Letter Queue)
    - Lambda execution IAM role
    - Lambda functions: process_iot_data, get_readings, get_alerts, get_latest
    - SQS → Lambda event source mapping
    - API Gateway REST API with /readings, /alerts, /latest endpoints
    - CloudWatch Dashboard: FogEdgeIoT-Dashboard

Usage:
    pip install boto3
    export AWS_REGION=eu-west-1
    python setup_aws_infra.py

Required AWS permissions for the executing IAM user:
    dynamodb:CreateTable, sqs:CreateQueue, iam:CreateRole, iam:AttachRolePolicy,
    lambda:CreateFunction, lambda:CreateEventSourceMapping,
    apigateway:*, cloudwatch:PutDashboard, logs:CreateLogGroup
"""
import os
import sys
import json
import time
import zipfile
import logging
import io

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger("SetupInfra")

REGION         = os.getenv("AWS_REGION", "eu-west-1")
PROJECT_PREFIX = "fec-iot"
CW_NAMESPACE   = "FogEdgeIoT"

dynamodb = boto3.client("dynamodb",    region_name=REGION)
sqs      = boto3.client("sqs",         region_name=REGION)
iam      = boto3.client("iam",         region_name=REGION)
lam      = boto3.client("lambda",      region_name=REGION)
apigw    = boto3.client("apigateway",  region_name=REGION)
cw       = boto3.client("cloudwatch",  region_name=REGION)
logs     = boto3.client("logs",        region_name=REGION)
sts      = boto3.client("sts",         region_name=REGION)

ACCOUNT_ID = sts.get_caller_identity()["Account"]


# ─── DynamoDB Tables ──────────────────────────────────────────────────────────

def create_dynamodb_table(table_name: str, partition_key: str, sort_key: str):
    """Create a DynamoDB table with on-demand billing."""
    try:
        dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": partition_key, "KeyType": "HASH"},
                {"AttributeName": sort_key,      "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": partition_key, "AttributeType": "S"},
                {"AttributeName": sort_key,      "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
            Tags=[{"Key": "Project", "Value": PROJECT_PREFIX}]
        )
        logger.info(f"✓ Created DynamoDB table: {table_name}")
        # Wait for table to be active
        waiter = boto3.client("dynamodb", region_name=REGION).get_waiter("table_exists")
        waiter.wait(TableName=table_name)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceInUseException":
            logger.info(f"  Table {table_name} already exists — skipping.")
        else:
            raise


# ─── SQS Queues ───────────────────────────────────────────────────────────────

def create_sqs_queues() -> tuple[str, str]:
    """Create main SQS queue and Dead Letter Queue. Returns (main_url, dlq_url)."""
    # Create DLQ first
    dlq_name = f"{PROJECT_PREFIX}-dlq"
    try:
        dlq = sqs.create_queue(
            QueueName=dlq_name,
            Attributes={
                "MessageRetentionPeriod": "1209600",  # 14 days
                "Tags": json.dumps({"Project": PROJECT_PREFIX}),
            }
        )
        dlq_url = dlq["QueueUrl"]
        dlq_arn = sqs.get_queue_attributes(
            QueueUrl=dlq_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]
        logger.info(f"✓ Created DLQ: {dlq_name}")
    except ClientError as e:
        if "QueueAlreadyExists" in str(e):
            dlq_url = sqs.get_queue_url(QueueName=dlq_name)["QueueUrl"]
            dlq_arn = sqs.get_queue_attributes(
                QueueUrl=dlq_url, AttributeNames=["QueueArn"]
            )["Attributes"]["QueueArn"]
            logger.info(f"  DLQ {dlq_name} already exists — reusing.")
        else:
            raise

    # Create main queue with redrive policy
    main_name = f"{PROJECT_PREFIX}-queue"
    try:
        main = sqs.create_queue(
            QueueName=main_name,
            Attributes={
                "VisibilityTimeout":      "60",
                "MessageRetentionPeriod": "86400",  # 1 day
                "RedrivePolicy": json.dumps({
                    "deadLetterTargetArn": dlq_arn,
                    "maxReceiveCount": "3"
                }),
            }
        )
        main_url = main["QueueUrl"]
        logger.info(f"✓ Created SQS queue: {main_name}")
    except ClientError as e:
        if "QueueAlreadyExists" in str(e):
            main_url = sqs.get_queue_url(QueueName=main_name)["QueueUrl"]
            logger.info(f"  Queue {main_name} already exists — reusing.")
        else:
            raise

    return main_url, dlq_url


# ─── IAM Role ─────────────────────────────────────────────────────────────────

def create_lambda_role() -> str:
    """Create IAM role for Lambda with DynamoDB, SQS, CloudWatch permissions."""
    role_name = f"{PROJECT_PREFIX}-lambda-role"
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }
    try:
        response = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Lambda execution role for FEC IoT project",
            Tags=[{"Key": "Project", "Value": PROJECT_PREFIX}]
        )
        role_arn = response["Role"]["Arn"]
        logger.info(f"✓ Created IAM role: {role_name}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityAlreadyExists":
            role_arn = iam.get_role(RoleName=role_name)["Role"]["Arn"]
            logger.info(f"  Role {role_name} already exists — reusing.")
        else:
            raise

    # Attach managed policies
    managed_policies = [
        "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
        "arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess",
        "arn:aws:iam::aws:policy/AmazonSQSFullAccess",
        "arn:aws:iam::aws:policy/CloudWatchFullAccess",
    ]
    for policy_arn in managed_policies:
        try:
            iam.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
        except ClientError:
            pass  # Already attached

    logger.info(f"  Attached {len(managed_policies)} policies to {role_name}")
    time.sleep(10)  # Allow IAM to propagate
    return role_arn


# ─── Lambda Functions ─────────────────────────────────────────────────────────

def zip_lambda_file(file_path: str) -> bytes:
    """Create an in-memory ZIP archive of a Lambda function file."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(file_path, arcname=os.path.basename(file_path))
    return buffer.getvalue()


def deploy_lambda(function_name: str, file_path: str, role_arn: str,
                  handler: str, env_vars: dict) -> str:
    """Deploy or update a Lambda function. Returns function ARN."""
    zip_bytes = zip_lambda_file(file_path)
    try:
        response = lam.create_function(
            FunctionName=function_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler=handler,
            Code={"ZipFile": zip_bytes},
            Timeout=60,
            MemorySize=256,
            Environment={"Variables": env_vars},
            Tags={"Project": PROJECT_PREFIX},
        )
        arn = response["FunctionArn"]
        logger.info(f"✓ Created Lambda: {function_name}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceConflictException":
            response = lam.update_function_code(
                FunctionName=function_name,
                ZipFile=zip_bytes
            )
            lam.update_function_configuration(
                FunctionName=function_name,
                Environment={"Variables": env_vars}
            )
            arn = response["FunctionArn"]
            logger.info(f"  Updated Lambda: {function_name}")
        else:
            raise
    return arn


# ─── SQS → Lambda Trigger ─────────────────────────────────────────────────────

def create_event_source_mapping(function_name: str, queue_url: str):
    """Create SQS event source mapping for the processor Lambda."""
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    try:
        lam.create_event_source_mapping(
            EventSourceArn=queue_arn,
            FunctionName=function_name,
            BatchSize=10,
            FunctionResponseTypes=["ReportBatchItemFailures"]
        )
        logger.info(f"✓ SQS→Lambda trigger: {function_name}")
    except ClientError as e:
        if "ResourceConflictException" in str(e):
            logger.info(f"  Event source mapping already exists — skipping.")
        else:
            raise


# ─── CloudWatch Dashboard ─────────────────────────────────────────────────────

def create_cloudwatch_dashboard():
    """Create a CloudWatch dashboard with panels for all 5 sensors."""
    dashboard_body = {
        "widgets": [
            # Row 1: Current value metrics
            {
                "type": "metric", "x": 0, "y": 0, "width": 4, "height": 4,
                "properties": {
                    "title": "Temperature (°C)",
                    "metrics": [[CW_NAMESPACE, "SensorReading", "SensorType", "temperature", "SensorId", "temp_01", "FogNode", "fog_node_01"]],
                    "view": "singleValue", "stat": "Average", "period": 60,
                    "region": REGION
                }
            },
            {
                "type": "metric", "x": 4, "y": 0, "width": 4, "height": 4,
                "properties": {
                    "title": "Humidity (%)",
                    "metrics": [[CW_NAMESPACE, "SensorReading", "SensorType", "humidity", "SensorId", "hum_01", "FogNode", "fog_node_01"]],
                    "view": "singleValue", "stat": "Average", "period": 60,
                    "region": REGION
                }
            },
            {
                "type": "metric", "x": 8, "y": 0, "width": 4, "height": 4,
                "properties": {
                    "title": "CO₂ (ppm)",
                    "metrics": [[CW_NAMESPACE, "SensorReading", "SensorType", "co2", "SensorId", "co2_01", "FogNode", "fog_node_01"]],
                    "view": "singleValue", "stat": "Average", "period": 60,
                    "region": REGION
                }
            },
            {
                "type": "metric", "x": 12, "y": 0, "width": 4, "height": 4,
                "properties": {
                    "title": "PM2.5 (µg/m³)",
                    "metrics": [[CW_NAMESPACE, "SensorReading", "SensorType", "pm25", "SensorId", "pm25_01", "FogNode", "fog_node_01"]],
                    "view": "singleValue", "stat": "Average", "period": 60,
                    "region": REGION
                }
            },
            {
                "type": "metric", "x": 16, "y": 0, "width": 4, "height": 4,
                "properties": {
                    "title": "UV Index",
                    "metrics": [[CW_NAMESPACE, "SensorReading", "SensorType", "uv_index", "SensorId", "uv_01", "FogNode", "fog_node_01"]],
                    "view": "singleValue", "stat": "Average", "period": 60,
                    "region": REGION
                }
            },
            # Row 2: Time-series charts
            {
                "type": "metric", "x": 0, "y": 4, "width": 12, "height": 6,
                "properties": {
                    "title": "Temperature & Humidity — Rolling Mean (1h)",
                    "metrics": [
                        [CW_NAMESPACE, "SensorRollingMean", "SensorType", "temperature", "SensorId", "temp_01"],
                        [CW_NAMESPACE, "SensorRollingMean", "SensorType", "humidity",    "SensorId", "hum_01"],
                    ],
                    "view": "timeSeries", "stat": "Average", "period": 300,
                    "region": REGION, "yAxis": {"left": {"min": 0}}
                }
            },
            {
                "type": "metric", "x": 12, "y": 4, "width": 12, "height": 6,
                "properties": {
                    "title": "CO₂ & PM2.5 — Rolling Mean (1h)",
                    "metrics": [
                        [CW_NAMESPACE, "SensorRollingMean", "SensorType", "co2",  "SensorId", "co2_01"],
                        [CW_NAMESPACE, "SensorRollingMean", "SensorType", "pm25", "SensorId", "pm25_01"],
                    ],
                    "view": "timeSeries", "stat": "Average", "period": 300,
                    "region": REGION, "yAxis": {"left": {"min": 0}}
                }
            },
            # Row 3: Anomaly count chart
            {
                "type": "metric", "x": 0, "y": 10, "width": 24, "height": 6,
                "properties": {
                    "title": "Anomaly Count per Sensor (per fog dispatch)",
                    "metrics": [
                        [CW_NAMESPACE, "AnomalyCount", "SensorType", "temperature", "SensorId", "temp_01", "FogNode", "fog_node_01"],
                        [CW_NAMESPACE, "AnomalyCount", "SensorType", "humidity",    "SensorId", "hum_01",  "FogNode", "fog_node_01"],
                        [CW_NAMESPACE, "AnomalyCount", "SensorType", "co2",         "SensorId", "co2_01",  "FogNode", "fog_node_01"],
                        [CW_NAMESPACE, "AnomalyCount", "SensorType", "pm25",        "SensorId", "pm25_01", "FogNode", "fog_node_01"],
                        [CW_NAMESPACE, "AnomalyCount", "SensorType", "uv_index",    "SensorId", "uv_01",   "FogNode", "fog_node_01"],
                    ],
                    "view": "timeSeries", "stat": "Sum", "period": 300,
                    "region": REGION, "yAxis": {"left": {"min": 0}}
                }
            },
        ]
    }

    cw.put_dashboard(
        DashboardName="FogEdgeIoT-Dashboard",
        DashboardBody=json.dumps(dashboard_body)
    )
    logger.info("✓ Created CloudWatch Dashboard: FogEdgeIoT-Dashboard")


# ─── Main Setup Flow ──────────────────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("FEC IoT Project — AWS Infrastructure Setup")
    logger.info(f"Region: {REGION}  |  Account: {ACCOUNT_ID}")
    logger.info("=" * 60)

    # 1. DynamoDB
    create_dynamodb_table("iot_readings", "sensor_id",  "timestamp")
    create_dynamodb_table("iot_alerts",   "alert_id",   "timestamp")

    # 2. SQS
    main_queue_url, _ = create_sqs_queues()

    # 3. IAM role
    role_arn = create_lambda_role()

    # 4. Common Lambda environment variables
    common_env = {
        "AWS_REGION":     REGION,
        "READINGS_TABLE": "iot_readings",
        "ALERTS_TABLE":   "iot_alerts",
        "CW_NAMESPACE":   CW_NAMESPACE,
    }

    # Base path for Lambda source files
    base = os.path.join(os.path.dirname(__file__), "..", "backend", "lambda_functions")

    # 5. Deploy Lambda functions
    processor_arn = deploy_lambda(
        "fec-iot-process",
        os.path.join(base, "process_iot_data.py"),
        role_arn,
        "process_iot_data.lambda_handler",
        common_env
    )

    deploy_lambda(
        "fec-iot-get-readings",
        os.path.join(base, "get_readings.py"),
        role_arn,
        "get_readings.lambda_handler",
        common_env
    )

    deploy_lambda(
        "fec-iot-get-alerts",
        os.path.join(base, "get_alerts.py"),
        role_arn,
        "get_alerts.lambda_handler",
        common_env
    )

    deploy_lambda(
        "fec-iot-get-latest",
        os.path.join(base, "get_latest.py"),
        role_arn,
        "get_latest.lambda_handler",
        common_env
    )

    # 6. SQS → Lambda trigger
    create_event_source_mapping("fec-iot-process", main_queue_url)

    # 7. CloudWatch Dashboard
    create_cloudwatch_dashboard()

    logger.info("=" * 60)
    logger.info("✅  Infrastructure setup complete!")
    logger.info(f"    SQS Queue URL:  {main_queue_url}")
    logger.info(f"    Set this as:   export SQS_QUEUE_URL='{main_queue_url}'")
    logger.info(f"    on your fog node EC2 instance.")
    logger.info("=" * 60)

    return main_queue_url


if __name__ == "__main__":
    main()
