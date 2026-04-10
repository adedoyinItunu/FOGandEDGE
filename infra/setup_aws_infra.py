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
    export AWS_REGION=us-east-1
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

REGION         = os.getenv("AWS_REGION", "us-east-1")
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
            Tags=[{"Key": "Project", "Value": PROJECT_PREFIX}],
        )
        logger.info(f"✓ Created DynamoDB table: {table_name}")
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
                "MessageRetentionPeriod": "1209600"  # 14 days
            },
            tags={
                "Project": PROJECT_PREFIX
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
            },
            tags={
                "Project": PROJECT_PREFIX
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
